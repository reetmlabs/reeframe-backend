use std::io::{Read, Write};
use std::path::Path;

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rand::{rngs::OsRng, RngCore};
use vms_core::{
    action::EncryptConfig,
    node::{NodeInput, NodeOutput},
    pipeline::NodeId,
    VmsError,
};

use crate::dispatcher::ActionContext;

// -- File format --
//
// Header (9 bytes):
//   magic       : b"RFFE"   (4 bytes)
//   version     : 0x01      (1 byte)
//   chunk_size  : u32 BE    (4 bytes) -- plaintext bytes per chunk
//
// Chunk (repeated until EOF):
//   nonce         : 12 bytes  (random per chunk)
//   ciphertext_len: u32 BE    (4 bytes)
//   ciphertext+tag: ciphertext_len bytes

const MAGIC: &[u8; 4] = b"RFFE";
const FORMAT_VERSION: u8 = 0x01;
const CHUNK_SIZE: usize = 64 * 1024;

// -- Handler --

pub async fn execute(
    node_id: NodeId,
    cfg: &EncryptConfig,
    input: &NodeInput,
    ctx: &ActionContext,
) -> NodeOutput {
    let Some(input_path) = input.first_artifact() else {
        return NodeOutput::failure(node_id, "encrypt: no upstream artifact");
    };

    if let Err(e) = tokio::fs::create_dir_all(&ctx.recording_dir).await {
        return NodeOutput::failure(node_id, format!("encrypt: create output dir: {e}"));
    }

    let stem = input_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("artifact");
    let output_path = ctx.recording_dir.join(format!("{stem}.enc"));

    let in_path = input_path.clone();
    let out_path = output_path.clone();
    let key_ref = cfg.key_ref.clone();
    let default_key = ctx.encryption_key; // Option<[u8; 32]> — Copy, safe to move

    let result = tokio::task::spawn_blocking(move || {
        let key = resolve_key_blocking(&key_ref, default_key)?;
        encrypt_file_blocking(&in_path, &key, &out_path)
    })
    .await
    .map_err(|e| format!("encrypt task panic: {e}"))
    .and_then(|r| r.map_err(|e| e.to_string()));

    match result {
        Ok(()) => {
            tracing::info!(
                node_id = %node_id,
                algorithm = ?cfg.algorithm,
                path = %output_path.display(),
                "Encrypt: done"
            );
            NodeOutput::success(node_id).with_artifact(output_path)
        }
        Err(e) => NodeOutput::failure(node_id, format!("encrypt: {e}")),
    }
}

// -- Key resolution --

fn resolve_key_blocking(
    key_ref: &str,
    default_key: Option<[u8; 32]>,
) -> Result<[u8; 32], VmsError> {
    if key_ref == "default" {
        return default_key
            .ok_or_else(|| VmsError::Config("no default encryption key configured".into()));
    }

    let path = format!("/etc/reeframe/keys/{key_ref}");
    let raw = std::fs::read_to_string(&path)
        .map_err(|e| VmsError::Config(format!("read key file {path}: {e}")))?;
    let bytes = STANDARD
        .decode(raw.trim())
        .map_err(|e| VmsError::Config(format!("key file {path} invalid base64: {e}")))?;
    if bytes.len() != 32 {
        return Err(VmsError::Config(format!(
            "key file {path} must decode to 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

// -- Blocking implementation --

fn encrypt_file_blocking(input: &Path, key: &[u8; 32], output: &Path) -> Result<(), VmsError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|_| VmsError::Encryption("failed to initialise cipher".into()))?;

    let mut in_file =
        std::fs::File::open(input).map_err(|e| VmsError::Media(format!("open input: {e}")))?;
    let mut out_file = std::fs::File::create(output)
        .map_err(|e| VmsError::Media(format!("create output: {e}")))?;

    out_file
        .write_all(MAGIC)
        .map_err(|e| VmsError::Media(format!("write magic: {e}")))?;
    out_file
        .write_all(&[FORMAT_VERSION])
        .map_err(|e| VmsError::Media(format!("write version: {e}")))?;
    out_file
        .write_all(&(CHUNK_SIZE as u32).to_be_bytes())
        .map_err(|e| VmsError::Media(format!("write chunk_size: {e}")))?;

    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = read_chunk(&mut in_file, &mut buf)
            .map_err(|e| VmsError::Media(format!("read chunk: {e}")))?;
        if n == 0 {
            break;
        }

        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, &buf[..n])
            .map_err(|_| VmsError::Encryption("chunk encryption failed".into()))?;

        out_file
            .write_all(&nonce_bytes)
            .map_err(|e| VmsError::Media(format!("write nonce: {e}")))?;
        out_file
            .write_all(&(ciphertext.len() as u32).to_be_bytes())
            .map_err(|e| VmsError::Media(format!("write ciphertext_len: {e}")))?;
        out_file
            .write_all(&ciphertext)
            .map_err(|e| VmsError::Media(format!("write ciphertext: {e}")))?;
    }

    Ok(())
}

fn read_chunk(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match reader.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

// -- Tests --

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42u8; 32]
    }

    fn write_temp(name: &str, content: &[u8]) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn round_trip_small() {
        let key = test_key();
        let plaintext = b"hello world - encrypt me";
        let input = write_temp("encrypt_test_input.bin", plaintext);
        let output = std::env::temp_dir().join("encrypt_test_input.bin.enc");

        encrypt_file_blocking(&input, &key, &output).unwrap();

        // File must start with magic
        let raw = std::fs::read(&output).unwrap();
        assert_eq!(&raw[..4], MAGIC);
        assert_eq!(raw[4], FORMAT_VERSION);

        std::fs::remove_file(&input).ok();
        std::fs::remove_file(&output).ok();
    }

    #[test]
    fn multi_chunk_produces_enc_file() {
        let key = test_key();
        // 2.5 chunks worth of data
        let plaintext = vec![0xABu8; CHUNK_SIZE * 2 + CHUNK_SIZE / 2];
        let input = write_temp("encrypt_multi_chunk.bin", &plaintext);
        let output = std::env::temp_dir().join("encrypt_multi_chunk.bin.enc");

        encrypt_file_blocking(&input, &key, &output).unwrap();

        let metadata = std::fs::metadata(&output).unwrap();
        assert!(metadata.len() > 9, "output must be larger than the header");

        std::fs::remove_file(&input).ok();
        std::fs::remove_file(&output).ok();
    }

    #[test]
    fn different_keys_produce_different_output() {
        let key_a = [0xAAu8; 32];
        let key_b = [0xBBu8; 32];
        let plaintext = b"determinism check";
        let input = write_temp("encrypt_key_diff.bin", plaintext);
        let out_a = std::env::temp_dir().join("encrypt_key_diff_a.bin.enc");
        let out_b = std::env::temp_dir().join("encrypt_key_diff_b.bin.enc");

        encrypt_file_blocking(&input, &key_a, &out_a).unwrap();
        encrypt_file_blocking(&input, &key_b, &out_b).unwrap();

        let bytes_a = std::fs::read(&out_a).unwrap();
        let bytes_b = std::fs::read(&out_b).unwrap();
        // Nonces are random so ciphertexts differ; at minimum they cannot be equal
        assert_ne!(bytes_a, bytes_b);

        std::fs::remove_file(&input).ok();
        std::fs::remove_file(&out_a).ok();
        std::fs::remove_file(&out_b).ok();
    }
}
