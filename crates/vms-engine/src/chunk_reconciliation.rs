//! Startup reconciliation for `recordings` rows left with `end_time IS
//! NULL` by a previous process lifetime. Two distinct bugs can produce such
//! a row: a hard kill mid-chunk (the file itself is genuinely corrupt — no
//! readable `moov` atom), or the now-fixed clean-shutdown DB-write race (the
//! file finalized fine, only the bookkeeping was lost). This sweep can't
//! tell which bug produced a given row, and doesn't need to — it probes the
//! file itself and either heals the row (file is valid) or discards it
//! (file is missing/corrupt), regardless of cause.

use gstreamer_pbutils::prelude::*;
use vms_db::RecordingRepo;

/// Runs once at daemon startup, before any camera in this process opens a
/// fresh chunk of its own — every row this finds is unconditionally left
/// over from a previous process lifetime, never a chunk currently being
/// written. Best-effort throughout: a probe or DB failure is logged and
/// skipped, never fatal to startup.
pub async fn reconcile_orphaned_chunks(recording_repo: &RecordingRepo) {
    let open_rows = match recording_repo.list_open_chunks().await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, "Startup chunk reconciliation: failed to list open chunks");
            return;
        }
    };

    if open_rows.is_empty() {
        return;
    }

    tracing::info!(
        count = open_rows.len(),
        "Reconciling orphaned recording chunks left by a previous process lifetime"
    );

    for row in open_rows {
        let probe_path = row.file_path.clone();
        let probe = match tokio::task::spawn_blocking(move || probe_chunk_file(&probe_path)).await {
            Ok(probe) => probe,
            Err(e) => {
                tracing::warn!(recording_id = %row.id, error = %e, "Startup chunk reconciliation: probe task panicked");
                continue;
            }
        };

        match probe {
            ChunkProbe::Valid {
                end_time,
                size_bytes,
            } => {
                let recording_id = row.id;
                let file_path = row.file_path.clone();
                if let Err(e) = recording_repo
                    .backfill_end_time(row, end_time, size_bytes)
                    .await
                {
                    tracing::warn!(recording_id = %recording_id, error = %e, "Startup chunk reconciliation: failed to backfill healed chunk");
                } else {
                    tracing::info!(recording_id = %recording_id, file_path, "Healed orphaned recording chunk left by a previous process");
                }
            }
            ChunkProbe::Invalid => {
                std::fs::remove_file(&row.file_path).ok();
                let recording_id = row.id;
                let file_path = row.file_path.clone();
                if let Err(e) = recording_repo.delete(row.id).await {
                    tracing::warn!(recording_id = %recording_id, error = %e, "Startup chunk reconciliation: failed to discard corrupt chunk row");
                } else {
                    tracing::warn!(recording_id = %recording_id, file_path, "Discarded corrupt/missing orphaned recording chunk");
                }
            }
        }
    }
}

enum ChunkProbe {
    // `sea_orm::prelude::DateTimeWithTimeZone` is a type alias for exactly
    // this — spelled out here rather than adding a real (non-dev) `sea-orm`
    // dependency just for one alias.
    Valid {
        end_time: chrono::DateTime<chrono::FixedOffset>,
        size_bytes: i64,
    },
    Invalid,
}

/// Runs on a blocking thread — `Discoverer` does synchronous I/O and
/// decoding, and can take real wall-clock time on a large file. A missing
/// file, or one `Discoverer` can't find a readable video stream in, is
/// treated as a hard-kill casualty; anything else is healable, using the
/// file's own mtime/size as the backfilled `end_time`/`size_bytes` — the
/// process that actually closed it is long gone, so this is the best
/// available substitute for the real close instant.
fn probe_chunk_file(path: &str) -> ChunkProbe {
    let metadata = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => return ChunkProbe::Invalid,
    };

    // `file_path` in the DB is relative to the daemon's working directory
    // (e.g. `./recordings/...`), but `g_filename_to_uri` (and thus a bare
    // `file://` string) requires an absolute path — canonicalize first.
    let uri = match std::fs::canonicalize(path)
        .ok()
        .and_then(|abs| gstreamer::glib::filename_to_uri(abs, None).ok())
    {
        Some(uri) => uri,
        None => return ChunkProbe::Invalid,
    };
    let has_video_stream =
        gstreamer_pbutils::Discoverer::new(gstreamer::ClockTime::from_seconds(5))
            .and_then(|d| d.discover_uri(&uri))
            .map(|info| {
                info.stream_list().into_iter().any(|s| {
                    s.downcast::<gstreamer_pbutils::DiscovererVideoInfo>()
                        .is_ok()
                })
            })
            .unwrap_or(false);

    if !has_video_stream {
        return ChunkProbe::Invalid;
    }

    let end_time = metadata
        .modified()
        .map(|t| chrono::DateTime::<chrono::Utc>::from(t).fixed_offset())
        .unwrap_or_else(|_| chrono::Utc::now().fixed_offset());

    ChunkProbe::Valid {
        end_time,
        size_bytes: metadata.len() as i64,
    }
}

#[cfg(test)]
mod tests {
    use super::{probe_chunk_file, ChunkProbe};
    use gstreamer::prelude::*;

    fn scratch_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("vms-engine-test-{name}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn missing_file_is_invalid() {
        let path = scratch_path("missing");
        assert!(matches!(
            probe_chunk_file(path.to_str().unwrap()),
            ChunkProbe::Invalid
        ));
    }

    #[test]
    fn garbage_file_is_invalid() {
        gstreamer::init().unwrap();
        let path = scratch_path("garbage.mp4");
        std::fs::write(&path, b"not an mp4 file").unwrap();

        let result = probe_chunk_file(path.to_str().unwrap());

        std::fs::remove_file(&path).ok();
        assert!(matches!(result, ChunkProbe::Invalid));
    }

    #[test]
    fn valid_mp4_is_healed_with_size_and_mtime() {
        gstreamer::init().unwrap();
        let path = scratch_path("valid.mp4");

        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc num-buffers=5 ! video/x-raw,width=64,height=64 ! x264enc ! mp4mux ! filesink location={}",
            path.display()
        ))
        .unwrap();
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let bus = pipeline.bus().unwrap();
        bus.timed_pop_filtered(
            gstreamer::ClockTime::from_seconds(10),
            &[gstreamer::MessageType::Eos, gstreamer::MessageType::Error],
        );
        pipeline.set_state(gstreamer::State::Null).unwrap();

        let on_disk_size = std::fs::metadata(&path).unwrap().len();
        let result = probe_chunk_file(path.to_str().unwrap());
        std::fs::remove_file(&path).ok();

        match result {
            ChunkProbe::Valid { size_bytes, .. } => assert_eq!(size_bytes as u64, on_disk_size),
            ChunkProbe::Invalid => panic!("expected a valid mp4 to probe as Valid"),
        }
    }

    /// Regression test: `recordings.file_path` is relative to the daemon's
    /// working directory (e.g. `./recordings/...`), not absolute. A bare
    /// `file://<relative path>` string is not a valid URI, which used to
    /// make `Discoverer` fail on every real chunk and mark it corrupt.
    #[test]
    fn relative_path_valid_mp4_is_healed() {
        gstreamer::init().unwrap();
        let rel_path = format!("./vms-engine-test-relative-{}.mp4", uuid::Uuid::new_v4());

        let pipeline = gstreamer::parse::launch(&format!(
            "videotestsrc num-buffers=5 ! video/x-raw,width=64,height=64 ! x264enc ! mp4mux ! filesink location={rel_path}"
        ))
        .unwrap();
        pipeline.set_state(gstreamer::State::Playing).unwrap();
        let bus = pipeline.bus().unwrap();
        bus.timed_pop_filtered(
            gstreamer::ClockTime::from_seconds(10),
            &[gstreamer::MessageType::Eos, gstreamer::MessageType::Error],
        );
        pipeline.set_state(gstreamer::State::Null).unwrap();

        let result = probe_chunk_file(&rel_path);
        std::fs::remove_file(&rel_path).ok();

        assert!(matches!(result, ChunkProbe::Valid { .. }));
    }
}
