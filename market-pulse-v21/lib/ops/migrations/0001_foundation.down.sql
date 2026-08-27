-- Reverse of 0001_foundation.
--
-- Phase 0 flagged that this repo's existing idiom (CREATE TABLE IF NOT
-- EXISTS, called at boot) has no down path at all, and Phase 1's
-- acceptance requires "migrations run clean up and down". A down that has
-- never been executed is a comment, so tests/test_ops_schema.py runs the
-- full up/down/up cycle against a real database.
--
-- Order is the reverse of creation, and RESTRICT rather than CASCADE
-- throughout: if something outside this migration has taken a dependency,
-- the drop should fail loudly rather than quietly taking that thing with it.

DROP TABLE IF EXISTS mf_sessions;
DROP TABLE IF EXISTS mf_jobs;
DROP TABLE IF EXISTS mf_documents;

DROP TRIGGER IF EXISTS mf_audit_log_no_truncate    ON mf_audit_log;
DROP TRIGGER IF EXISTS mf_audit_log_no_update_stmt ON mf_audit_log;
DROP TRIGGER IF EXISTS mf_audit_log_no_delete_stmt ON mf_audit_log;
DROP TRIGGER IF EXISTS mf_audit_log_no_delete   ON mf_audit_log;
DROP TRIGGER IF EXISTS mf_audit_log_no_update   ON mf_audit_log;
-- The triggers must go before the table: the delete trigger would
-- otherwise be in force during the drop.
DROP TABLE IF EXISTS mf_audit_log;
DROP FUNCTION IF EXISTS mf_audit_log_immutable();

DROP TABLE IF EXISTS mf_jurisdiction_rules;
DROP TABLE IF EXISTS mf_jurisdictions;

DROP TRIGGER IF EXISTS mf_user_roles_scope_ck ON mf_user_roles;
DROP TABLE IF EXISTS mf_user_roles;
DROP FUNCTION IF EXISTS mf_user_roles_scope_check();
DROP TABLE IF EXISTS mf_roles;

-- mf_divisions.head_user_id points at mf_users and mf_users.division_id
-- points back, so the constraint has to come off before either can drop.
ALTER TABLE IF EXISTS mf_divisions DROP CONSTRAINT IF EXISTS mf_divisions_head_fk;
DROP TABLE IF EXISTS mf_users;
DROP TABLE IF EXISTS mf_divisions;
DROP TABLE IF EXISTS mf_organizations;
