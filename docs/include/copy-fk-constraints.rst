::

   pgcopydb copy fk-constraints: Create all the FOREIGN KEY constraints found in the source database in the target
   usage: pgcopydb copy fk-constraints  --source ... --target ... [ --fk-jobs ... ]

     --source             Postgres URI to the source database
     --target             Postgres URI to the target database
     --dir                Work directory to use
     --fk-jobs            Number of concurrent VALIDATE CONSTRAINT jobs to run
     --filters <filename> Use the filters defined in <filename>
     --restart            Allow restarting when temp files exist already
     --resume             Allow resuming operations after a failure
     --not-consistent     Allow taking a new snapshot on the source database


