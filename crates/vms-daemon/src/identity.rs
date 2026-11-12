//! The site identity: this backend's Ed25519 key, the `be_id` a Coordinator
//! assigned at enrollment, and that Coordinator's URL. Kept in one file that
//! only the service user can read, so key and `be_id` never drift apart.

use std::fs::{self, OpenOptions, Permissions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::Context;
use base64::Engine;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// On-disk format of the identity file.
#[derive(Debug, Serialize, Deserialize)]
pub struct SiteIdentity {
    pub be_id: Uuid,
    pub coordinator_url: String,
    /// Base64 (standard) of the 32-byte Ed25519 seed.
    seed_b64: String,
}

impl SiteIdentity {
    /// Builds an identity for `key`, enrolled as `be_id` at `coordinator_url`.
    pub fn new(be_id: Uuid, coordinator_url: String, key: &SigningKey) -> Self {
        Self {
            be_id,
            coordinator_url,
            seed_b64: base64::engine::general_purpose::STANDARD.encode(key.to_bytes()),
        }
    }

    /// The private key stored in this identity.
    pub fn signing_key(&self) -> anyhow::Result<SigningKey> {
        let seed: [u8; 32] = base64::engine::general_purpose::STANDARD
            .decode(&self.seed_b64)
            .ok()
            .and_then(|bytes| bytes.try_into().ok())
            .context("seed_b64 must be 32 bytes, base64 encoded")?;
        Ok(SigningKey::from_bytes(&seed))
    }
}

/// Reads the identity at `path`. Returns `None` if the file does not exist,
/// and an error if it exists but is unreadable or malformed.
pub fn load(path: &Path) -> anyhow::Result<Option<SiteIdentity>> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("cannot read {}", path.display())),
    };
    let identity: SiteIdentity = serde_json::from_str(&text)
        .with_context(|| format!("{} is not a valid site identity", path.display()))?;
    identity
        .signing_key()
        .with_context(|| format!("{} holds an invalid key", path.display()))?;
    Ok(Some(identity))
}

/// Writes `identity` to `path` with mode 0600. The file is written next to
/// `path` and renamed into place, so a reader never sees a partial file and
/// a failed write leaves the old identity intact.
pub fn save(path: &Path, identity: &SiteIdentity) -> anyhow::Result<()> {
    let tmp = temp_path(path);
    let json = serde_json::to_vec_pretty(identity)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("cannot create {}", tmp.display()))?;
    // `mode` only applies when the file is created, not to a leftover temp file.
    fs::set_permissions(&tmp, Permissions::from_mode(0o600))?;
    file.write_all(&json)?;
    file.sync_all()?;
    fs::rename(&tmp, path)
        .with_context(|| format!("cannot move {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}
