#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# Regression test for the opt-in parallel two-phase FOREIGN KEY build
# (--fk-jobs):
#
#   1. "COMMENT ON CONSTRAINT ... does not exist" during the post-data
#      restore, because the FK CONSTRAINT was claimed and built AFTER
#      post-data instead of before it.
#
#   2. "database is locked" (SQLite SQLITE_BUSY_SNAPSHOT) in the VALIDATE
#      CONSTRAINT worker pool, from running the long ALTER TABLE and its
#      catalog writes while a read iterator over s_fk_constraint was still
#      open.
#
#   3. Comments on FOREIGN KEY constraints silently missing on the target
#      even without any error: pg_restore drops a COMMENT ON CONSTRAINT
#      post-data entry once the FK CONSTRAINT entry it depends on is
#      excluded from the --use-list file, with no error at all.
#
# Cloning with --fk-jobs 1 and --fk-jobs 4 separately also exercises the
# worker pool at different concurrency levels.

TARGET_BASE="${PGCOPYDB_TARGET_PGURI%/*}"

for jobs in 1 4
do
    TMPDB="pgcopydb_fk_jobs_${jobs}"
    TARGET_URI="${TARGET_BASE}/${TMPDB}"
    TMPDIR="/tmp/pgcopydb-fk-jobs-${jobs}"

    psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c "CREATE DATABASE ${TMPDB}"

    # The source schema contains a table using a custom collation.
    # Pre-create it on the target so that --skip-collations works correctly.
    psql -q -d "${TARGET_URI}" -c "
        CREATE COLLATION IF NOT EXISTS mycol
            (locale = 'fr-FR-x-icu', provider = 'icu');
    "

    pgcopydb clone \
        --source "${PGCOPYDB_SOURCE_PGURI}" \
        --target "${TARGET_URI}" \
        --fk-jobs "${jobs}" \
        --index-jobs 2 \
        --table-jobs 2 \
        --dir "${TMPDIR}" \
        --not-consistent \
        --skip-collations \
        --fail-fast > /dev/null

    # Verify every FOREIGN KEY constraint claimed by --fk-jobs landed on the
    # target with the same validated state and the same COMMENT as on the
    # source.
    psql -d "${TARGET_URI}" \
         --no-psqlrc \
         --expanded \
         --file ./sql/22-fk-constraints.sql

    psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c "DROP DATABASE ${TMPDB}"

    rm -rf "${TMPDIR}"
done
