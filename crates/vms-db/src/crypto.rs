use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::{rngs::OsRng, RngCore};
use vms_core::VmsError;

const PREFIX: &str = "enc:v1:";
const NONCE_LEN: usize = 12;

/// AES-256-GCM encryption helper for credential fields stored in JSONB columns.
///
/// Encrypted values are stored as `enc:v1:<base64(nonce || ciphertext_with_tag)>`.
/// The 12-byte nonce is randomly generated per field per write; the GCM tag is
/// appended to the ciphertext by the AEAD implementation.
#[derive(Clone)]
pub struct Crypto {
    key: [u8; 32],
}

impl Crypto {
    /// Load the encryption key from the `VMS_ENCRYPTION_KEY` environment variable.
    ///
    /// The variable must be a base64-encoded 32-byte key.
    pub fn from_env() -> Result<Self, VmsError> {
        let raw = std::env::var("VMS_ENCRYPTION_KEY")
            .map_err(|_| VmsError::Config("VMS_ENCRYPTION_KEY not set".into()))?;
        let bytes = STANDARD
            .decode(raw.trim())
            .map_err(|e| VmsError::Config(format!("VMS_ENCRYPTION_KEY invalid base64: {e}")))?;
        if bytes.len() != 32 {
            return Err(VmsError::Config(format!(
                "VMS_ENCRYPTION_KEY must decode to 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(Self { key })
    }

    /// Decode a base64-encoded 32-byte key string (e.g. from a config file or env var).
    pub fn from_b64(b64: &str) -> Result<Self, VmsError> {
        let bytes = STANDARD
            .decode(b64.trim())
            .map_err(|e| VmsError::Config(format!("encryption key invalid base64: {e}")))?;
        if bytes.len() != 32 {
            return Err(VmsError::Config(format!(
                "encryption key must decode to 32 bytes, got {}",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(Self { key })
    }

    /// Construct directly from raw key material (useful in tests).
    pub fn from_key(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Encrypt `plaintext` and return the `enc:v1:…` encoded string.
    pub fn encrypt(&self, plaintext: &str) -> Result<String, VmsError> {
        let cipher = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|_| VmsError::Encryption("failed to initialise cipher".into()))?;
        let mut nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_bytes())
            .map_err(|_| VmsError::Encryption("encryption failed".into()))?;
        let mut combined = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        combined.extend_from_slice(&nonce_bytes);
        combined.extend_from_slice(&ciphertext);
        Ok(format!("{}{}", PREFIX, STANDARD.encode(&combined)))
    }

    /// Decrypt a value produced by [`Crypto::encrypt`].
    ///
    /// Returns `VmsError::Encryption` if the prefix is missing, the base64 is
    /// invalid, or the GCM tag does not match (tampered/wrong key).
    pub fn decrypt(&self, value: &str) -> Result<String, VmsError> {
        let encoded = value
            .strip_prefix(PREFIX)
            .ok_or_else(|| VmsError::Encryption("missing enc:v1: prefix".into()))?;
        let combined = STANDARD
            .decode(encoded)
            .map_err(|e| VmsError::Encryption(format!("base64 decode: {e}")))?;
        if combined.len() < NONCE_LEN {
            return Err(VmsError::Encryption("ciphertext too short".into()));
        }
        let (nonce_bytes, ct) = combined.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);
        let cipher = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|_| VmsError::Encryption("failed to initialise cipher".into()))?;
        let plaintext = cipher.decrypt(nonce, ct).map_err(|_| {
            VmsError::Encryption("decryption failed — invalid key or corrupted data".into())
        })?;
        String::from_utf8(plaintext)
            .map_err(|e| VmsError::Encryption(format!("plaintext is not valid UTF-8: {e}")))
    }

    /// Returns `true` if `value` is an encrypted string (has the `enc:v1:` prefix).
    pub fn is_encrypted(value: &str) -> bool {
        value.starts_with(PREFIX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42u8; 32]
    }

    #[test]
    fn round_trip() {
        let crypto = Crypto::from_key(test_key());
        let plaintext = "super-secret-password";
        let encrypted = crypto.encrypt(plaintext).unwrap();
        assert!(encrypted.starts_with("enc:v1:"));
        let decrypted = crypto.decrypt(&encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn different_nonces_each_call() {
        let crypto = Crypto::from_key(test_key());
        let a = crypto.encrypt("same").unwrap();
        let b = crypto.encrypt("same").unwrap();
        assert_ne!(a, b, "each encryption should produce a unique ciphertext");
    }

    #[test]
    fn wrong_key_fails() {
        let crypto_a = Crypto::from_key([0xAAu8; 32]);
        let crypto_b = Crypto::from_key([0xBBu8; 32]);
        let encrypted = crypto_a.encrypt("secret").unwrap();
        assert!(crypto_b.decrypt(&encrypted).is_err());
    }

    #[test]
    fn missing_prefix_fails() {
        let crypto = Crypto::from_key(test_key());
        assert!(crypto.decrypt("no-prefix-here").is_err());
    }
}
