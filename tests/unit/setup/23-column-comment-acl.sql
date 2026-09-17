--
-- Regression fixture: COMMENT ON COLUMN and column-level GRANT/REVOKE entries
-- in the pg_restore TOC use a composite tag whose object type is "COLUMN"
-- (e.g. "public COLUMN col_comment_acl.val owner"). ArchiveItemDesc /
-- pgRestoreDescriptionArray in pgcmd.c never had a COLUMN entry (only
-- top-level te->desc values were transcribed there), so the TOC parser
-- failed to recognize the object type and logged
-- "Failed to parse Archive TOC comment or acl: ..." for every such entry.
--
-- The failure was swallowed (non-fatal) and pg_restore --use-list still
-- restored the objects correctly by dumpId, so this is mostly a test that
-- the noisy error is gone -- but the column-level ACL bypassed pgcopydb's
-- name-based filtering while broken, so it is worth covering directly.
--
CREATE TABLE col_comment_acl (
    id  integer PRIMARY KEY,
    val text
);

INSERT INTO col_comment_acl SELECT g, 'v' || g FROM generate_series(1, 5) g;

COMMENT ON COLUMN col_comment_acl.val IS 'column comment, must survive clone';

-- A column-level GRANT produces an ACL TOC entry whose composite tag is also
-- "COLUMN <table>.<column>", exercising the ACL path (not just COMMENT).
GRANT SELECT (val) ON col_comment_acl TO PUBLIC;
