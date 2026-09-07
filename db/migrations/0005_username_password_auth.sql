ALTER TABLE api_access_tokens
    ADD COLUMN IF NOT EXISTS username TEXT,
    ADD COLUMN IF NOT EXISTS password_hash TEXT;

UPDATE api_access_tokens
SET
    username = role,
    password_hash = token_hash
WHERE username IS NULL OR password_hash IS NULL;

ALTER TABLE api_access_tokens
    ALTER COLUMN username SET NOT NULL,
    ALTER COLUMN password_hash SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS api_access_tokens_username_index
    ON api_access_tokens (username);
