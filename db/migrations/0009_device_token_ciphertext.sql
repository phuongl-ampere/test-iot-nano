ALTER TABLE device_tokens
    ADD COLUMN IF NOT EXISTS token_ciphertext TEXT;
