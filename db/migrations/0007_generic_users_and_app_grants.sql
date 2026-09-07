ALTER TABLE users
    DROP CONSTRAINT IF EXISTS users_role_key;

ALTER TABLE user_app_grants
    DROP CONSTRAINT IF EXISTS user_app_grants_app_key_check;
