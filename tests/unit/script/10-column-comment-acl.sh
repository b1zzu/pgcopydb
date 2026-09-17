#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_TARGET_PGURI
#
# Regression test for the missing COLUMN object type in the archive TOC
# parser (pgcmd.c ArchiveItemDesc / pgRestoreDescriptionArray): every
# COMMENT ON COLUMN and column-level GRANT/REVOKE entry logged
# "Failed to parse Archive TOC comment or acl: ..." because "COLUMN" was
# never a recognized composite-tag object type, only ever appearing nested
# inside COMMENT/ACL entries (unlike TABLE, FUNCTION, CONSTRAINT, etc, which
# are also top-level te->desc values and so were already in the table).
#
# The setup (23-column-comment-acl.sql) creates col_comment_acl with a
# column COMMENT and a column-level GRANT to PUBLIC.
#
# The main pgcopydb fork (copydb.sh) already cloned source to target; here
# we verify that both were preserved:
#   - the column comment survived (col_description is not null)
#   - the column-level ACL survived (has_column_privilege for PUBLIC)

psql -At -d "${PGCOPYDB_TARGET_PGURI}" <<'EOF'
SELECT CASE
    WHEN col_description('col_comment_acl'::regclass, attnum) IS NOT NULL
    THEN 'column comment preserved'
    ELSE 'column comment missing'
END
FROM pg_attribute
WHERE attrelid = 'col_comment_acl'::regclass
  AND attname = 'val';

SELECT CASE
    WHEN has_column_privilege('public', 'col_comment_acl', 'val', 'SELECT')
    THEN 'column ACL preserved'
    ELSE 'column ACL missing'
END;
EOF
