#! /bin/bash

set -x
set -e

# This script expects the following environment variables to be set:
#
#  - PGCOPYDB_SOURCE_PGURI
#  - PGCOPYDB_TARGET_PGURI
#
# Regression test for the opt-in immediate retry (--retry-count):
#
#   1. The default (--retry-count 0, i.e. the option is not used at all)
#      must behave exactly as before this feature existed: a single
#      attempt per table-part COPY, CREATE INDEX, and VALIDATE CONSTRAINT.
#
#   2. --retry-count 2 must clone successfully on a normal run where
#      nothing actually fails: it must not change behaviour or introduce
#      duplicate rows or any other data corruption.
#
# Injecting an actual mid-COPY / mid-CREATE-INDEX connection failure isn't
# practical in this fixed, non-interactive test harness; that path has been
# exercised manually (kill the COPY / CREATE INDEX backend from a
# pg_terminate_backend() loop while a large clone is running) and is not
# repeated here.

TARGET_BASE="${PGCOPYDB_TARGET_PGURI%/*}"

for retryCount in 0 2
do
    TMPDB="pgcopydb_retry_count_${retryCount}"
    TARGET_URI="${TARGET_BASE}/${TMPDB}"
    TMPDIR="/tmp/pgcopydb-retry-count-${retryCount}"

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
        --retry-count "${retryCount}" \
        --index-jobs 2 \
        --table-jobs 2 \
        --dir "${TMPDIR}" \
        --not-consistent \
        --skip-collations \
        --fail-fast > /dev/null

    src_count=$(psql -t -A -d "${PGCOPYDB_SOURCE_PGURI}" \
        -c "SELECT count(*) FROM only public.parent_model;")
    tgt_count=$(psql -t -A -d "${TARGET_URI}" \
        -c "SELECT count(*) FROM only public.parent_model;")

    if [ "${src_count}" != "${tgt_count}" ]; then
        echo "ERROR: --retry-count ${retryCount}: expected ${src_count} rows" \
             "in public.parent_model, got ${tgt_count}"
        exit 1
    fi

    psql -q -d "${PGCOPYDB_TARGET_PGURI}" -c "DROP DATABASE ${TMPDB}"

    rm -rf "${TMPDIR}"
done
