-- Assert that FOREIGN KEY constraints claimed by --fk-jobs land on the
-- target exactly as they are on the source: same validated state, and same
-- COMMENT. The comments are the actual regression coverage here: pg_restore
-- silently drops a COMMENT ON CONSTRAINT post-data entry once the FK
-- CONSTRAINT entry it depends on is excluded from the --use-list file, with
-- no error, unless pgcopydb re-applies the comment itself (see fkeys.c,
-- copydb_apply_fk_constraint_comment).
   select conrelid::regclass::text as tbl,
          conname,
          convalidated,
          obj_description(oid, 'pg_constraint') as comment
     from pg_constraint
    where contype = 'f'
      and connamespace = 'public'::regnamespace
 order by 1, 2;
