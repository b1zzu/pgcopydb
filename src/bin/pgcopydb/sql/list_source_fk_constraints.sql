-- $1::oid[] : in-scope table OIDs from s_table (NULL means all tables)
--
-- Only foreign key constraints where BOTH the referencing and the referenced
-- table are in scope are returned. An FK whose parent (or child) is filtered
-- out is left untouched: it is not claimed here, so it stays in the
-- pg_restore --section=post-data script exactly as it does today.
--
-- conparentid <> 0 marks a constraint that a partition inherits from its
-- partitioned parent's own constraint; those are skipped here because they
-- are created implicitly when the parent's constraint recurses, and cannot
-- be added independently.
WITH filters AS (
    SELECT $1::oid[] AS table_oids
)
SELECT c.oid,
       format('%I', c.conname),
       c.conrelid,
       format('%I.%I', cn.nspname, ch.relname),
       ch.relkind,
       c.confrelid,
       format('%I.%I', pn.nspname, p.relname),
       pg_get_constraintdef(c.oid),
       c.condeferrable,
       c.condeferred,
       c.convalidated,
       format('%s %s %s',
              regexp_replace(cn.nspname, '[\n\r]', ' '),
              regexp_replace(c.conname, '[\n\r]', ' '),
              regexp_replace(auth.rolname, '[\n\r]', ' '))

  FROM pg_constraint c
  JOIN pg_class ch ON ch.oid = c.conrelid
  JOIN pg_namespace cn ON cn.oid = ch.relnamespace
  JOIN pg_class p ON p.oid = c.confrelid
  JOIN pg_namespace pn ON pn.oid = p.relnamespace
  JOIN pg_roles auth ON auth.oid = ch.relowner,
       filters f

 WHERE c.contype = 'f'
   AND c.conparentid = 0
   AND ch.relpersistence IN ('p', 'u')
   AND cn.nspname !~ '^pg_' AND cn.nspname <> 'information_schema'
   AND cn.nspname !~ 'pgcopydb'
   -- extension guard: FKs owned by an extension travel with it
   AND NOT EXISTS (
       SELECT 1 FROM pg_depend d
        WHERE d.classid = 'pg_constraint'::regclass
          AND d.objid = c.oid AND d.deptype = 'e'
   )
   -- both ends must be in scope
   AND (f.table_oids IS NULL
        OR (c.conrelid = ANY(f.table_oids) AND c.confrelid = ANY(f.table_oids)))

 ORDER BY c.confrelid, c.conrelid, c.oid;
