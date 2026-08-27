-- 0001_foundation — organizations, people, roles, jurisdictions, audit,
-- documents, and the job queue.
--
-- EVERY TABLE HERE IS PREFIXED mf_. That is the seam described in
-- ARCHITECTURE.md §2: this schema shares a database with the analysis
-- boards but shares none of their conventions. Money is BIGINT minor
-- units, never DOUBLE PRECISION. Instants are timestamptz. Legal dates
-- are DATE, resolved in the property's timezone by lib/ops/clock.py, not
-- by the server's.

-- ── the operating company ──
CREATE TABLE mf_organizations (
    id              BIGSERIAL PRIMARY KEY,
    legal_name      TEXT NOT NULL,
    dba             TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at     TIMESTAMPTZ
);

-- Business units. Self-referencing for sub-divisions.
CREATE TABLE mf_divisions (
    id                  BIGSERIAL PRIMARY KEY,
    organization_id     BIGINT NOT NULL REFERENCES mf_organizations(id) ON DELETE RESTRICT,
    name                TEXT NOT NULL,
    description         TEXT NOT NULL DEFAULT '',
    cost_center_code    TEXT,
    head_user_id        BIGINT,          -- FK added after mf_users exists
    parent_division_id  BIGINT REFERENCES mf_divisions(id) ON DELETE RESTRICT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at         TIMESTAMPTZ,
    UNIQUE (organization_id, name)
);

-- ── people ──
-- One identity table behind four portals. The PORTAL a session was opened
-- through is recorded on the session, not here, so a person who is both a
-- staff member and an owner cannot see owner data from a staff login by
-- accident.
CREATE TABLE mf_users (
    id                  BIGSERIAL PRIMARY KEY,
    organization_id     BIGINT NOT NULL REFERENCES mf_organizations(id) ON DELETE RESTRICT,
    email               TEXT NOT NULL,
    full_name           TEXT NOT NULL DEFAULT '',
    title               TEXT,
    division_id         BIGINT REFERENCES mf_divisions(id) ON DELETE SET NULL,
    password_hash       TEXT,            -- NULL until first credential set
    mfa_secret          TEXT,            -- TOTP shared secret, NULL = not enrolled
    mfa_enrolled_at     TIMESTAMPTZ,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    -- Bumped on any privilege change. Every live session carrying an older
    -- value is dead on its next request — this is the revocation path, and
    -- it works without a session store scan.
    privilege_epoch     INTEGER NOT NULL DEFAULT 1,
    last_login_at       TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at         TIMESTAMPTZ,
    UNIQUE (organization_id, email)
);

ALTER TABLE mf_divisions
    ADD CONSTRAINT mf_divisions_head_fk
    FOREIGN KEY (head_user_id) REFERENCES mf_users(id) ON DELETE SET NULL;

-- ── roles ──
CREATE TABLE mf_roles (
    id          BIGSERIAL PRIMARY KEY,
    key         TEXT NOT NULL UNIQUE,
    label       TEXT NOT NULL,
    rank        SMALLINT NOT NULL DEFAULT 0,   -- higher = broader
    requires_mfa BOOLEAN NOT NULL DEFAULT FALSE
);

-- A role assignment MAY be scoped. Both scope columns NULL means the role
-- applies organization-wide; that is only ever correct for platform_admin,
-- and mf_user_roles_scope_ck below enforces it rather than trusting the
-- caller. It is a TRIGGER and not a CHECK constraint because the rule
-- depends on mf_roles.key, and a CHECK cannot read another table.
CREATE TABLE mf_user_roles (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES mf_users(id) ON DELETE CASCADE,
    role_id         BIGINT NOT NULL REFERENCES mf_roles(id) ON DELETE RESTRICT,
    division_id     BIGINT REFERENCES mf_divisions(id) ON DELETE CASCADE,
    -- Property scope. mf_properties arrives in Phase 2; the column exists
    -- now so the scoping layer has a stable shape to compile against, and
    -- the FK is added by that migration rather than being forgotten.
    property_id     BIGINT,
    granted_by      BIGINT REFERENCES mf_users(id) ON DELETE SET NULL,
    granted_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at      TIMESTAMPTZ,
    UNIQUE (user_id, role_id, division_id, property_id)
);
CREATE INDEX mf_user_roles_user_idx ON mf_user_roles (user_id) WHERE revoked_at IS NULL;

-- An organization-wide grant is the single most dangerous row in this
-- schema: lib/ops/scope.py reads an unscoped grant as "no division
-- filter", so a division_manager row with both scope columns NULL would
-- silently see every division. That must be impossible to write, not
-- merely discouraged — the application layer already refuses it, and an
-- application-layer rule is one bug away from not applying.
CREATE OR REPLACE FUNCTION mf_user_roles_scope_check() RETURNS TRIGGER AS $$
DECLARE
    role_key TEXT;
BEGIN
    SELECT key INTO role_key FROM mf_roles WHERE id = NEW.role_id;
    IF role_key IS NULL THEN
        RAISE EXCEPTION 'mf_user_roles.role_id % does not name a role', NEW.role_id;
    END IF;
    IF role_key <> 'platform_admin'
       AND NEW.division_id IS NULL
       AND NEW.property_id IS NULL THEN
        RAISE EXCEPTION
            'an unscoped % grant is not permitted', role_key
            USING HINT = 'Only platform_admin may hold an organization-wide '
                         'role. Scope this grant to a division or a property.';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER mf_user_roles_scope_ck
    BEFORE INSERT OR UPDATE ON mf_user_roles
    FOR EACH ROW EXECUTE FUNCTION mf_user_roles_scope_check();

-- ── jurisdictions ──
CREATE TABLE mf_jurisdictions (
    id                      BIGSERIAL PRIMARY KEY,
    slug                    TEXT NOT NULL UNIQUE,
    display_name            TEXT NOT NULL,
    state                   TEXT,
    county                  TEXT,
    city                    TEXT,
    -- IANA name. lib/ops/clock.py refuses an invalid one rather than
    -- falling back to the server's zone, because a wrong timezone here
    -- silently shifts every legal date the jurisdiction touches.
    timezone                TEXT NOT NULL,
    parent_jurisdiction_id  BIGINT REFERENCES mf_jurisdictions(id) ON DELETE RESTRICT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Legal rules as DATA. CLAUDE.md: code asks "what is rule X for
-- jurisdiction Y on date Z" and never branches on a state code.
CREATE TABLE mf_jurisdiction_rules (
    id                  BIGSERIAL PRIMARY KEY,
    jurisdiction_id     BIGINT NOT NULL REFERENCES mf_jurisdictions(id) ON DELETE CASCADE,
    rule_key            TEXT NOT NULL,
    value               JSONB NOT NULL,
    effective_from      DATE NOT NULL,
    effective_to        DATE,
    authority           TEXT NOT NULL DEFAULT '',
    source_url          TEXT NOT NULL DEFAULT '',
    source_citation     TEXT NOT NULL DEFAULT '',
    last_verified_at    DATE,
    verified_by         TEXT NOT NULL DEFAULT '',
    notes               TEXT NOT NULL DEFAULT '',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX mf_jurisdiction_rules_lookup_idx
    ON mf_jurisdiction_rules (jurisdiction_id, rule_key, effective_from DESC);

-- ── audit ──
-- Append-only, enforced by trigger below rather than by convention.
CREATE TABLE mf_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    actor_user_id   BIGINT,          -- no FK: the log outlives the user row
    actor_label     TEXT NOT NULL DEFAULT '',
    action          TEXT NOT NULL,   -- read_pii | create | update | delete | login | ...
    target_type     TEXT NOT NULL,
    target_id       TEXT,
    detail          JSONB NOT NULL DEFAULT '{}'::jsonb,
    ip              TEXT,
    request_id      TEXT
);
CREATE INDEX mf_audit_log_target_idx ON mf_audit_log (target_type, target_id, occurred_at DESC);
CREATE INDEX mf_audit_log_actor_idx  ON mf_audit_log (actor_user_id, occurred_at DESC);

-- THE TRIGGER IS THE POINT. An application-level "we only ever insert"
-- is a promise; this is a guarantee. An UPDATE or DELETE raises, so a
-- future phase cannot quietly add a "clean up old audit rows" job.
CREATE OR REPLACE FUNCTION mf_audit_log_immutable() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'mf_audit_log is append-only: % is not permitted', TG_OP
        USING HINT = 'Corrections are new rows describing the correction.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER mf_audit_log_no_update
    BEFORE UPDATE ON mf_audit_log
    FOR EACH ROW EXECUTE FUNCTION mf_audit_log_immutable();

CREATE TRIGGER mf_audit_log_no_delete
    BEFORE DELETE ON mf_audit_log
    FOR EACH ROW EXECUTE FUNCTION mf_audit_log_immutable();

-- STATEMENT-LEVEL TRIGGERS TOO, and this is not belt-and-braces.
-- A row-level BEFORE DELETE only fires per row, so
-- `DELETE FROM mf_audit_log WHERE id = -999` matches nothing, fires
-- nothing, and RETURNS SUCCESS. The rows are safe, but "DELETE succeeded"
-- is the wrong answer from a table whose entire promise is that deletes
-- are impossible — anyone probing the guarantee gets told it does not
-- hold. Statement-level triggers make every DELETE and UPDATE statement
-- raise regardless of how many rows it would have touched.
CREATE TRIGGER mf_audit_log_no_delete_stmt
    BEFORE DELETE ON mf_audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION mf_audit_log_immutable();

CREATE TRIGGER mf_audit_log_no_update_stmt
    BEFORE UPDATE ON mf_audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION mf_audit_log_immutable();

CREATE TRIGGER mf_audit_log_no_truncate
    BEFORE TRUNCATE ON mf_audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION mf_audit_log_immutable();

-- NO GRANT HERE, DELIBERATELY. ARCHITECTURE.md §5.4 asks for "an
-- insert-only grant AND a trigger". Only the trigger is implemented, and
-- writing the grant anyway would be worse than omitting it: this app
-- connects with one DATABASE_URL as the table's owner, and an owner can
-- re-grant to itself at will while a superuser bypasses permission
-- checks entirely. `REVOKE UPDATE, DELETE ON mf_audit_log FROM PUBLIC`
-- would run without error, appear in this file as a second line of
-- defence, and stop nothing.
--
-- The grant becomes real when the application connects as a separate
-- non-owner role — infrastructure work (a second DATABASE_URL, a role
-- Railway does not create for you), tracked in BACKLOG.md. Until then
-- the trigger is the whole guarantee, which is why
-- tests/test_ops_schema.py attacks it through all five paths instead of
-- trusting that it exists.

-- ── documents ──
CREATE TABLE mf_documents (
    id              BIGSERIAL PRIMARY KEY,
    organization_id BIGINT NOT NULL REFERENCES mf_organizations(id) ON DELETE RESTRICT,
    owner_type      TEXT NOT NULL,       -- polymorphic: 'lease' | 'property' | ...
    owner_id        BIGINT NOT NULL,
    storage_key     TEXT NOT NULL UNIQUE,
    filename        TEXT NOT NULL,
    mime            TEXT NOT NULL DEFAULT 'application/octet-stream',
    size_bytes      BIGINT NOT NULL DEFAULT 0,
    -- Recorded at upload. Move-in photos decide deposit disputes; a hash
    -- is what makes "this is the photo we took that day" checkable rather
    -- than assertable.
    sha256          TEXT NOT NULL,
    uploaded_by     BIGINT REFERENCES mf_users(id) ON DELETE SET NULL,
    uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    visibility      TEXT NOT NULL DEFAULT 'internal',
    retention_until DATE,
    CONSTRAINT mf_documents_visibility_ck
        CHECK (visibility IN ('internal', 'owner', 'tenant', 'vendor'))
);
CREATE INDEX mf_documents_owner_idx ON mf_documents (owner_type, owner_id);

-- ── job queue ──
-- Postgres-backed rather than Redis: one fewer service, and enqueueing is
-- transactional with the write that caused it. SELECT ... FOR UPDATE SKIP
-- LOCKED is the standard claim pattern.
CREATE TABLE mf_jobs (
    id              BIGSERIAL PRIMARY KEY,
    kind            TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Set by the enqueuer for work that must happen at most once. A
    -- duplicate payment webhook enqueues the same key and the insert is a
    -- no-op instead of a second charge.
    idempotency_key TEXT UNIQUE,
    run_after       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 5,
    status          TEXT NOT NULL DEFAULT 'queued',
    locked_at       TIMESTAMPTZ,
    locked_by       TEXT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    CONSTRAINT mf_jobs_status_ck
        CHECK (status IN ('queued', 'running', 'done', 'failed', 'dead'))
);
CREATE INDEX mf_jobs_claim_idx ON mf_jobs (run_after)
    WHERE status = 'queued';

-- ── sessions ──
CREATE TABLE mf_sessions (
    id                  BIGSERIAL PRIMARY KEY,
    user_id             BIGINT NOT NULL REFERENCES mf_users(id) ON DELETE CASCADE,
    token_hash          TEXT NOT NULL UNIQUE,   -- never the token itself
    portal              TEXT NOT NULL,
    -- Copied from the user at issue. A mismatch on any later request means
    -- the user's privileges changed and this session is stale.
    privilege_epoch     INTEGER NOT NULL,
    mfa_satisfied       BOOLEAN NOT NULL DEFAULT FALSE,
    issued_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at          TIMESTAMPTZ NOT NULL,
    revoked_at          TIMESTAMPTZ,
    ip                  TEXT,
    user_agent          TEXT,
    CONSTRAINT mf_sessions_portal_ck
        CHECK (portal IN ('staff', 'tenant', 'owner', 'vendor'))
);
CREATE INDEX mf_sessions_user_idx ON mf_sessions (user_id) WHERE revoked_at IS NULL;

-- ── seed: roles ──
INSERT INTO mf_roles (key, label, rank, requires_mfa) VALUES
    ('platform_admin',   'Platform administrator', 100, TRUE),
    ('division_manager', 'Division manager',        80, TRUE),
    ('staff',            'Staff',                   60, FALSE),
    ('owner_client',     'Owner client',            40, FALSE),
    ('tenant',           'Tenant',                  20, FALSE),
    ('vendor',           'Vendor',                  20, FALSE);

-- ── seed: jurisdictions ──
INSERT INTO mf_jurisdictions (slug, display_name, state, county, city, timezone, parent_jurisdiction_id) VALUES
    ('us-ca', 'California', 'CA', NULL, NULL, 'America/Los_Angeles', NULL);
INSERT INTO mf_jurisdictions (slug, display_name, state, county, city, timezone, parent_jurisdiction_id)
    SELECT 'us-ca-alameda', 'Alameda County, CA', 'CA', 'Alameda', NULL, 'America/Los_Angeles', id
    FROM mf_jurisdictions WHERE slug = 'us-ca';
INSERT INTO mf_jurisdictions (slug, display_name, state, county, city, timezone, parent_jurisdiction_id)
    SELECT 'us-ca-san-leandro', 'San Leandro, CA', 'CA', 'Alameda', 'San Leandro', 'America/Los_Angeles', id
    FROM mf_jurisdictions WHERE slug = 'us-ca-alameda';
INSERT INTO mf_jurisdictions (slug, display_name, state, county, city, timezone, parent_jurisdiction_id) VALUES
    ('us-ri', 'Rhode Island', 'RI', NULL, NULL, 'America/New_York', NULL);
-- Placeholder for the specific RI municipality. The owner fills in the
-- city; the row exists so Phase 2 has something to attach a property to.
INSERT INTO mf_jurisdictions (slug, display_name, state, county, city, timezone, parent_jurisdiction_id)
    SELECT 'us-ri-city-placeholder', 'Rhode Island — municipality TBD', 'RI', NULL, NULL, 'America/New_York', id
    FROM mf_jurisdictions WHERE slug = 'us-ri';

-- ── seed: two obviously-fake rules ──
-- CLAUDE.md asks for these so the shape and the staleness warning are
-- visible before Phase 5 and 7 seed anything real. They are deliberately
-- absurd and deliberately stale so nobody mistakes them for research: the
-- authority is fictional and last_verified_at is old enough to trip the
-- 180-day warning on sight.
INSERT INTO mf_jurisdiction_rules
    (jurisdiction_id, rule_key, value, effective_from, authority, source_url,
     source_citation, last_verified_at, verified_by, notes)
SELECT id, 'PLACEHOLDER_notice_period_days', '{"days": 999}'::jsonb,
       DATE '2000-01-01', 'NOT A REAL AUTHORITY — placeholder',
       'https://example.invalid/placeholder',
       'Placeholder row. Not law. Delete before Phase 5.',
       DATE '2020-01-01', 'seed',
       'Fake on purpose: proves the rules table shape and the staleness warning.'
FROM mf_jurisdictions WHERE slug = 'us-ca-san-leandro';

INSERT INTO mf_jurisdiction_rules
    (jurisdiction_id, rule_key, value, effective_from, authority, source_url,
     source_citation, last_verified_at, verified_by, notes)
SELECT id, 'PLACEHOLDER_deposit_return_days', '{"days": 888}'::jsonb,
       DATE '2000-01-01', 'NOT A REAL AUTHORITY — placeholder',
       'https://example.invalid/placeholder',
       'Placeholder row. Not law. Delete before Phase 6.',
       DATE '2020-01-01', 'seed',
       'Fake on purpose: proves the rules table shape and the staleness warning.'
FROM mf_jurisdictions WHERE slug = 'us-ri';
