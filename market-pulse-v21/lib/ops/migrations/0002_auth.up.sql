-- 0002_auth — what 0001 left out because nothing had tried to log in yet.
--
-- Written as a second migration rather than as an edit to 0001, which is
-- the point of having a ledger: 0001 is pushed, and editing an applied
-- migration is the single most common way a staging database silently
-- diverges from production. runner.verify() refuses to run over that
-- drift, so the discipline is enforced rather than remembered.

-- ── TOTP replay ──
-- A six-digit code is valid for a 30-second step, and the verifier
-- accepts one step either side of it for clock skew. Without a record of
-- the last accepted step, a code observed over someone's shoulder — or
-- read off a phishing page — can be used a second time inside that
-- window. Storing the counter makes each code single-use.
ALTER TABLE mf_users ADD COLUMN mfa_last_counter BIGINT;

-- ── login throttling ──
-- Not rate limiting by IP, which an attacker chooses, but a lockout on
-- the account, which they do not. Cleared on a successful login.
ALTER TABLE mf_users ADD COLUMN failed_login_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE mf_users ADD COLUMN locked_until TIMESTAMPTZ;

-- ── session hygiene ──
-- rotated_from lets a chain of rotations be followed backwards. A stolen
-- token that gets used AFTER the legitimate holder rotated shows up as a
-- request against a revoked session whose successor is active, which is
-- the signature of theft rather than of an ordinary expiry.
ALTER TABLE mf_sessions ADD COLUMN last_seen_at TIMESTAMPTZ;
ALTER TABLE mf_sessions ADD COLUMN rotated_from BIGINT REFERENCES mf_sessions(id) ON DELETE SET NULL;
ALTER TABLE mf_sessions ADD COLUMN revoked_reason TEXT NOT NULL DEFAULT '';

-- Expiry sweeps and "show me this user's sessions" both want this.
CREATE INDEX mf_sessions_user_live_idx
    ON mf_sessions (user_id, expires_at DESC)
    WHERE revoked_at IS NULL;
