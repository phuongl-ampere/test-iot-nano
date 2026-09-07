use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, OsRng, rand_core::RngCore},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};
use thiserror::Error;

const NONCE_BYTES: usize = 12;

#[derive(Clone)]
pub struct TokenVault {
    cipher: Aes256Gcm,
}

#[derive(Debug, Error)]
pub enum TokenVaultError {
    #[error("device token encryption failed")]
    Encrypt,
    #[error("stored device token ciphertext is invalid")]
    InvalidCiphertext,
    #[error("device token decryption failed")]
    Decrypt,
}

impl TokenVault {
    pub fn from_key_material(key_material: impl AsRef<[u8]>) -> Self {
        let key = Sha256::digest(key_material.as_ref());
        let cipher = Aes256Gcm::new_from_slice(&key).expect("SHA-256 output is 32 bytes");
        Self { cipher }
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String, TokenVaultError> {
        let mut nonce_bytes = [0_u8; NONCE_BYTES];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_bytes())
            .map_err(|_| TokenVaultError::Encrypt)?;
        let mut encoded = Vec::with_capacity(NONCE_BYTES + ciphertext.len());
        encoded.extend_from_slice(&nonce_bytes);
        encoded.extend_from_slice(&ciphertext);
        Ok(URL_SAFE_NO_PAD.encode(encoded))
    }

    pub fn decrypt(&self, ciphertext: &str) -> Result<String, TokenVaultError> {
        let encoded = URL_SAFE_NO_PAD
            .decode(ciphertext)
            .map_err(|_| TokenVaultError::InvalidCiphertext)?;
        if encoded.len() <= NONCE_BYTES {
            return Err(TokenVaultError::InvalidCiphertext);
        }
        let nonce = <[u8; NONCE_BYTES]>::try_from(&encoded[..NONCE_BYTES])
            .map_err(|_| TokenVaultError::InvalidCiphertext)?;
        let nonce = Nonce::from(nonce);
        let plaintext = self
            .cipher
            .decrypt(&nonce, &encoded[NONCE_BYTES..])
            .map_err(|_| TokenVaultError::Decrypt)?;
        String::from_utf8(plaintext).map_err(|_| TokenVaultError::InvalidCiphertext)
    }
}
