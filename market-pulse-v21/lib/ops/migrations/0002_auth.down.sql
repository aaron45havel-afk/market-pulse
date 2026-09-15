-- Reverse of 0002_auth.
--
-- Dropping these columns loses the TOTP replay counters and the lockout
-- state. That is acceptable — both are recoverable by re-enrolling and by
-- waiting — and it is the honest reverse. A down migration that preserved
-- them somewhere would be inventing a second schema nobody tests.

DROP INDEX IF EXISTS mf_sessions_user_live_idx;

ALTER TABLE mf_sessions DROP COLUMN IF EXISTS revoked_reason;
ALTER TABLE mf_sessions DROP COLUMN IF EXISTS rotated_from;
ALTER TABLE mf_sessions DROP COLUMN IF EXISTS last_seen_at;

ALTER TABLE mf_users DROP COLUMN IF EXISTS locked_until;
ALTER TABLE mf_users DROP COLUMN IF EXISTS failed_login_count;
ALTER TABLE mf_users DROP COLUMN IF EXISTS mfa_last_counter;
