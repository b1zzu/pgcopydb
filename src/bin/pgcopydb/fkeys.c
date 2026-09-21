/*
 * src/bin/pgcopydb/fkeys.c
 *     Opt-in parallel two-phase build of FOREIGN KEY constraints claimed out
 *     of the pg_restore --section=post-data script.
 *
 * See the comment block in copydb.h just above the fkeys.c declarations for
 * the design rationale (why Phase A is sequential and Phase B is not, and why
 * Phase A runs before the post-data restore while Phase B runs after it).
 */

#include <errno.h>
#include <inttypes.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "catalog.h"
#include "cli_root.h"
#include "copydb.h"
#include "file_utils.h"
#include "log.h"
#include "pgsql.h"
#include "pqexpbuffer.h"
#include "queue_utils.h"
#include "signals.h"
#include "string_utils.h"
#include "summary.h"

/*
 * Phase A materializes the full list of claimed FK constraints into this
 * in-memory array before doing any DDL or catalog write, so that the SQLite
 * iterator over s_fk_constraint is closed (and its read snapshot released)
 * before we start deleting rows from that very table (un-claiming FKs whose
 * ADD CONSTRAINT failed).
 */
typedef struct FKConstraintAddItem
{
	uint32_t conOid;
	char conName[PG_NAMEDATALEN];
	char conRelQname[PG_NAMEDATALEN_FQ];
	bool conValidated;
	uint64_t addedTime;
	char *conDef;               /* malloc'ed, owned by this array entry */
	char *conComment;           /* malloc'ed, owned by this array entry, NULL ok */
} FKConstraintAddItem;

typedef struct FKConstraintAddArray
{
	int count;
	int capacity;
	FKConstraintAddItem *array; /* malloc'ed, grown with realloc */
} FKConstraintAddArray;

static bool copydb_collect_fk_constraint_add_hook(void *ctx,
												  SourceFKConstraint *fk);

static void copydb_free_fk_constraint_add_array(FKConstraintAddArray *array);

static bool copydb_add_one_fk_constraint_not_valid(CopyDataSpec *specs,
												   PGSQL *dst,
												   FKConstraintAddItem *item,
												   bool fallbackToPostData,
												   int *unclaimed);

static bool copydb_apply_fk_constraint_comment(PGSQL *dst,
											   FKConstraintAddItem *item);

static bool copydb_fk_phase_should_run(CopyDataSpec *specs,
									   bool *proceed,
									   int64_t *count);


typedef struct FKChildTableOIDArray
{
	int count;
	int capacity;
	uint32_t *array;            /* malloc'ed, grown with realloc */
} FKChildTableOIDArray;

static bool copydb_collect_fk_child_table_hook(void *ctx, uint32_t oid);


/*
 * Phase B materializes the pending constraints of a single child table into
 * this in-memory array before running any VALIDATE CONSTRAINT, so that the
 * SQLite iterator over s_fk_constraint (and its read snapshot) is released
 * before the multi-minute ALTER TABLE and the catalog writes that follow it.
 * Holding that iterator open across the long DDL is what caused "database is
 * locked" errors under concurrency: the worker's own snapshot went stale
 * while other workers committed, and its own later write could not upgrade
 * that stale read transaction.
 */
typedef struct FKConstraintValidateItem
{
	uint32_t conOid;
	char conName[PG_NAMEDATALEN];
	char conRelQname[PG_NAMEDATALEN_FQ];
	char confRelQname[PG_NAMEDATALEN_FQ];
	bool conValidated;
	uint64_t validatedTime;
} FKConstraintValidateItem;

typedef struct FKConstraintValidateArray
{
	int count;
	int capacity;
	FKConstraintValidateItem *array; /* malloc'ed, grown with realloc */
} FKConstraintValidateArray;

static bool copydb_collect_fk_constraint_validate_hook(void *ctx,
													   SourceFKConstraint *fk);

static bool copydb_validate_one_fk_constraint(CopyDataSpec *specs,
											  PGSQL *dst,
											  FKConstraintValidateItem *item,
											  int *errors);


/*
 * copydb_fk_phase_should_run implements the short-circuit checks shared by
 * both copydb_add_all_fk_constraints_not_valid and
 * copydb_validate_all_fk_constraints: the feature must be opted-in
 * (--fk-jobs), the whole FK phase must not already be fully done on a
 * previous run, and there must be at least one claimed FK constraint to work
 * on.
 *
 * Returns false only on an actual error (failed to query the catalogs).
 * Otherwise sets *proceed to true when the caller should continue with its
 * phase, or false when the caller should return true immediately. *count is
 * only meaningful when *proceed is true.
 */
static bool
copydb_fk_phase_should_run(CopyDataSpec *specs, bool *proceed, int64_t *count)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	*proceed = false;

	if (specs->fkJobs <= 0)
	{
		log_debug("Skipping parallel FOREIGN KEY build, --fk-jobs is not set");
		return true;
	}

	if (specs->runState.fkConstraintsAreDone)
	{
		log_info("Skipping FOREIGN KEY constraints, "
				 "already done on a previous run");
		return true;
	}

	CatalogCounts counts = { 0 };

	if (!catalog_count_objects(sourceDB, &counts))
	{
		log_error("Failed to count FOREIGN KEY constraints in our catalogs");
		return false;
	}

	if (counts.fkConstraints == 0)
	{
		log_info("No FOREIGN KEY constraints to build in parallel");
		return true;
	}

	*count = counts.fkConstraints;
	*proceed = true;

	return true;
}


/*
 * copydb_create_all_fk_constraints runs both phases of the opt-in parallel
 * FOREIGN KEY build back-to-back, with no post-data restore in between. Used
 * by the standalone `pgcopydb copy fk-constraints` command, which never
 * un-claims a constraint on failure (there is no post-data restore left to
 * fall back onto): a failed ADD CONSTRAINT is a hard error here.
 *
 * copydb_clone_database() does NOT call this: it calls
 * copydb_add_all_fk_constraints_not_valid() before the post-data restore and
 * copydb_validate_all_fk_constraints() after it, so that COMMENT ON
 * CONSTRAINT entries in post-data find the constraint already in place.
 */
bool
copydb_create_all_fk_constraints(CopyDataSpec *specs)
{
	if (!copydb_add_all_fk_constraints_not_valid(specs, false))
	{
		log_error("Failed to add FOREIGN KEY constraints, "
				  "see above for details");
		return false;
	}

	if (!copydb_validate_all_fk_constraints(specs))
	{
		log_error("Failed to validate FOREIGN KEY constraints, "
				  "see above for details");
		return false;
	}

	return true;
}


/*
 * copydb_add_all_fk_constraints_not_valid is the Phase A entry point: it
 * implements the shared short-circuits, the STEP banner, and the timing
 * section around copydb_add_fk_constraints_not_valid().
 *
 * When fallbackToPostData is true, a constraint whose ADD CONSTRAINT fails
 * (for a reason other than it already existing) is un-claimed rather than
 * failing the whole phase: it is deleted from s_fk_constraint so that the
 * post-data restore -- which has not run yet when this is called from
 * copydb_clone_database() -- builds it the ordinary way instead. Pass false
 * when no post-data restore will follow (the standalone `pgcopydb copy
 * fk-constraints` command, and any caller resuming a run whose post-data
 * restore already completed).
 */
bool
copydb_add_all_fk_constraints_not_valid(CopyDataSpec *specs,
										bool fallbackToPostData)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	bool proceed = false;
	int64_t count = 0;

	if (!copydb_fk_phase_should_run(specs, &proceed, &count))
	{
		/* errors have already been logged */
		return false;
	}

	if (!proceed)
	{
		return true;
	}

	log_info("STEP 10: creating %lld FOREIGN KEY constraint(s) as NOT VALID",
			 (long long) count);

	if (!summary_start_timing(sourceDB, TIMING_SECTION_FK_ADD))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_add_fk_constraints_not_valid(specs, fallbackToPostData))
	{
		log_error("Failed to add FOREIGN KEY constraints, "
				  "see above for details");
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_FK_ADD))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_validate_all_fk_constraints is the Phase B entry point: it
 * implements the shared short-circuits, the STEP banner, the worker pool
 * lifecycle (queue, fork, send, stop, wait), and the timing section, and only
 * marks the whole FK phase done once every claimed constraint has been both
 * added and (when applicable) validated.
 */
bool
copydb_validate_all_fk_constraints(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	bool proceed = false;
	int64_t count = 0;

	if (!copydb_fk_phase_should_run(specs, &proceed, &count))
	{
		/* errors have already been logged */
		return false;
	}

	if (!proceed)
	{
		return true;
	}

	log_info("STEP 12: validating %lld FOREIGN KEY constraint(s) "
			 "using %d processes",
			 (long long) count,
			 specs->fkJobs);

	if (!summary_start_timing(sourceDB, TIMING_SECTION_FK_VALIDATE))
	{
		/* errors have already been logged */
		return false;
	}

	if (!queue_create(&(specs->fkQueue), "validate fk"))
	{
		log_error("Failed to create the VALIDATE CONSTRAINT process queue");
		return false;
	}

	/*
	 * Collect the child table OIDs that still have Phase B work left into an
	 * in-memory array first, and only then send them to the queue, once the
	 * workers are up. This mirrors copydb_add_table_indexes(): keeping the
	 * iteration and the (retrying) queue_send() calls apart avoids holding
	 * the catalog open for reads any longer than necessary once workers are
	 * trying to read from it concurrently.
	 */
	FKChildTableOIDArray oidArray = { 0, 0, NULL };

	if (!catalog_iter_s_fk_constraint_child_tables(sourceDB,
												   &oidArray,
												   &copydb_collect_fk_child_table_hook))
	{
		log_error("Failed to list FOREIGN KEY constraint child tables, "
				  "see above for details");
		free(oidArray.array);
		return false;
	}

	/*
	 * Close the catalog before forking so that each worker process opens its
	 * own SQLite connection. Inheriting a forked sqlite3* handle causes WAL
	 * write-lock conflicts between sibling workers even when the SysV
	 * semaphore is held (issue #881).
	 */
	if (!catalog_close(sourceDB))
	{
		/* errors have already been logged */
		free(oidArray.array);
		return false;
	}

	if (!copydb_start_fk_workers(specs))
	{
		log_error("Failed to start FOREIGN KEY validate workers, "
				  "see above for details");
		free(oidArray.array);
		return false;
	}

	/* reopen in this process after the fork */
	if (!catalog_open(sourceDB))
	{
		/* errors have already been logged */
		free(oidArray.array);
		return false;
	}

	bool sendOk = true;

	for (int i = 0; i < oidArray.count && sendOk; i++)
	{
		QMessage mesg = {
			.type = QMSG_TYPE_TABLEOID,
			.data.tp.oid = oidArray.array[i]
		};

		if (!queue_send(&(specs->fkQueue), &mesg))
		{
			/* errors have already been logged */
			sendOk = false;
		}
	}

	free(oidArray.array);

	if (!sendOk)
	{
		log_error("Failed to queue FOREIGN KEY constraint child tables, "
				  "see above for details");
		return false;
	}

	if (!copydb_fk_workers_send_stop(specs))
	{
		log_fatal("Failed to send the STOP message to the "
				  "VALIDATE CONSTRAINT queue");
		(void) copydb_fatal_exit();
		return false;
	}

	if (!copydb_wait_for_subprocesses(specs->failFast))
	{
		log_error("Some VALIDATE CONSTRAINT worker process(es) have exited "
				  "with error, see above for details");

		if (specs->failFast)
		{
			(void) copydb_fatal_exit();
		}

		return false;
	}

	if (!queue_unlink(&(specs->fkQueue)))
	{
		log_warn("Failed to unlink the VALIDATE CONSTRAINT process queue, "
				 "see above for details");
	}

	/*
	 * Only mark the FK phase as fully done (and stop its timing section) when
	 * every claimed constraint has actually been added and, when applicable,
	 * validated. Otherwise a --resume must re-enter these functions and
	 * finish the remaining work, which is safe: both phases are incremental.
	 */
	int64_t left = 0;

	if (!catalog_count_fk_constraints_left(sourceDB, &left))
	{
		/* errors have already been logged */
		return false;
	}

	if (left > 0)
	{
		log_error("%lld FOREIGN KEY constraint(s) could not be built or "
				  "validated, see above for details. The constraint(s) "
				  "remain in place as NOT VALID: once the underlying data "
				  "is fixed, re-run `pgcopydb copy fk-constraints --resume "
				  "--not-consistent` to retry validation.",
				  (long long) left);
		return false;
	}

	if (!summary_stop_timing(sourceDB, TIMING_SECTION_FK_VALIDATE))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_add_fk_constraints_not_valid runs Phase A: ALTER TABLE ... ADD
 * CONSTRAINT ... [NOT VALID], sequentially, on a single target connection.
 *
 * This is deliberately NOT parallelized: the referencing table takes an
 * AccessExclusive lock and the referenced table takes a ShareRowExclusive
 * lock, both self-conflicting, so with many FKs pointing at the same hub
 * parent (the exact shape this feature targets) a worker pool here would
 * only add lock-wait time and a real risk of deadlock (40P01), for a phase
 * that is pure catalog work costing milliseconds per constraint even run
 * one at a time.
 *
 * The full list of claimed constraints is materialized into an in-memory
 * array before any DDL or catalog write happens: Phase A now needs to
 * un-claim (delete from s_fk_constraint) a constraint whose ADD CONSTRAINT
 * fails, and that delete must not happen while a read iterator over that very
 * table is still open.
 */
bool
copydb_add_fk_constraints_not_valid(CopyDataSpec *specs, bool fallbackToPostData)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		/* errors have already been logged */
		return false;
	}

	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		(void) pgsql_finish(&dst);
		return false;
	}

	FKConstraintAddArray fkArray = { 0, 0, NULL };

	bool iterOk =
		catalog_iter_s_fk_constraint(sourceDB,
									 &fkArray,
									 &copydb_collect_fk_constraint_add_hook);

	if (!iterOk)
	{
		/* errors have already been logged */
		(void) pgsql_finish(&dst);
		copydb_free_fk_constraint_add_array(&fkArray);
		return false;
	}

	int errors = 0;
	int unclaimed = 0;

	for (int i = 0; i < fkArray.count; i++)
	{
		if (!copydb_add_one_fk_constraint_not_valid(specs,
													&dst,
													&(fkArray.array[i]),
													fallbackToPostData,
													&unclaimed))
		{
			++errors;

			if (specs->failFast)
			{
				break;
			}
		}
	}

	copydb_free_fk_constraint_add_array(&fkArray);

	(void) pgsql_finish(&dst);

	if (unclaimed > 0)
	{
		log_warn("%d FOREIGN KEY constraint(s) could not be added and were "
				 "handed back to the post-data restore: they will be built "
				 "the ordinary way (validated immediately, under an "
				 "ACCESS EXCLUSIVE lock)",
				 unclaimed);
	}

	return errors == 0;
}


/*
 * copydb_collect_fk_constraint_add_hook is an iterator callback function that
 * copies the fields Phase A needs into an in-memory, realloc-grown array,
 * owning a copy of conDef (fk->conDef is freed when the iterator moves to the
 * next row or finishes).
 */
static bool
copydb_collect_fk_constraint_add_hook(void *ctx, SourceFKConstraint *fk)
{
	FKConstraintAddArray *array = (FKConstraintAddArray *) ctx;

	if (array->count == array->capacity)
	{
		int newCapacity = array->capacity == 0 ? 16 : array->capacity * 2;
		FKConstraintAddItem *newArray =
			(FKConstraintAddItem *) realloc(array->array,
											newCapacity *
											sizeof(FKConstraintAddItem));

		if (newArray == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		array->array = newArray;
		array->capacity = newCapacity;
	}

	FKConstraintAddItem *item = &(array->array[(array->count)++]);

	item->conOid = fk->conOid;

	strlcpy(item->conName, fk->conName, sizeof(item->conName));
	strlcpy(item->conRelQname, fk->conRelQname, sizeof(item->conRelQname));

	item->conValidated = fk->conValidated;
	item->addedTime = fk->addedTime;
	item->conDef = fk->conDef != NULL ? strdup(fk->conDef) : NULL;
	item->conComment = fk->conComment != NULL ? strdup(fk->conComment) : NULL;

	return true;
}


/*
 * copydb_free_fk_constraint_add_array releases the conDef/conComment copies
 * owned by each entry, then the array itself.
 */
static void
copydb_free_fk_constraint_add_array(FKConstraintAddArray *array)
{
	for (int i = 0; i < array->count; i++)
	{
		free(array->array[i].conDef);
		free(array->array[i].conComment);
	}

	free(array->array);
}


/*
 * copydb_add_one_fk_constraint_not_valid runs Phase A for a single FOREIGN
 * KEY constraint. Returns false only when the failure should count as an
 * error for the caller (a hard failure, or fallbackToPostData not set);
 * un-claiming a constraint via fallbackToPostData is not itself an error.
 */
static bool
copydb_add_one_fk_constraint_not_valid(CopyDataSpec *specs,
									   PGSQL *dst,
									   FKConstraintAddItem *item,
									   bool fallbackToPostData,
									   int *unclaimed)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (item->addedTime > 0)
	{
		log_debug("Skipping FOREIGN KEY %s: already added (done at %lld)",
				  item->conName,
				  (long long) item->addedTime);
		return true;
	}

	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd,
					  "ALTER TABLE %s ADD CONSTRAINT %s %s",
					  item->conRelQname,
					  item->conName,
					  item->conDef != NULL ? item->conDef : "");

	/*
	 * pg_get_constraintdef() already appends " NOT VALID" by itself when the
	 * constraint is NOT VALID on the source (item->conValidated == false): in
	 * that case we must NOT validate it later, and the command above already
	 * matches the source state as-is.
	 *
	 * When the source constraint is valid (the common case), we deliberately
	 * add it as NOT VALID here, and validate it in Phase B instead, so that
	 * this statement only ever takes a lock for milliseconds.
	 */
	if (item->conValidated)
	{
		appendPQExpBufferStr(cmd, " NOT VALID");
	}

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create query for FOREIGN KEY \"%s\": "
				  "out of memory",
				  item->conName);
		destroyPQExpBuffer(cmd);
		return false;
	}

	if (specs->datname[0] != '\0')
	{
		log_notice("%s: %s;", specs->datname, cmd->data);
	}
	else
	{
		log_notice("%s;", cmd->data);
	}

	uint64_t startTime = time(NULL);
	bool success = pgsql_execute(dst, cmd->data);
	uint64_t durationMs = (time(NULL) - startTime) * 1000;

	destroyPQExpBuffer(cmd);

	if (!success)
	{
		/*
		 * Edge case: a previous run may have already restored this very
		 * constraint via the ordinary pg_restore --section=post-data path,
		 * for instance when --fk-jobs is turned on for the first time on a
		 * --resume of a run whose post-data restore had already completed
		 * without it. Postgres has no ADD CONSTRAINT IF NOT EXISTS, so treat
		 * a duplicate_object (42710) error as success: the constraint is
		 * already there, we just did not know about it yet.
		 */
		bool alreadyExists = streq(dst->sqlstate, "42710");

		if (alreadyExists)
		{
			log_notice("FOREIGN KEY constraint %s on %s already exists on "
					   "the target, treating Phase A as already done for it",
					   item->conName,
					   item->conRelQname);
		}
		else if (fallbackToPostData)
		{
			/*
			 * Un-claim this constraint: delete it from s_fk_constraint so
			 * that the post-data restore (which has not run yet) leaves its
			 * FK CONSTRAINT entry uncommented and builds it the ordinary
			 * way, COMMENT ON CONSTRAINT included. This is not a hard
			 * failure for the phase as a whole.
			 */
			log_warn("Failed to add FOREIGN KEY constraint %s on %s (%s): "
					 "handing it back to the post-data restore",
					 item->conName,
					 item->conRelQname,
					 dst->sqlstate);

			if (!catalog_delete_s_fk_constraint(sourceDB, item->conOid))
			{
				log_error("Failed to un-claim FOREIGN KEY constraint %s, "
						  "see above for details",
						  item->conName);
				return false;
			}

			++(*unclaimed);

			return true;
		}
		else
		{
			log_error("Failed to add FOREIGN KEY constraint %s on %s, "
					  "see above for details",
					  item->conName,
					  item->conRelQname);

			return false;
		}
	}

	if (!catalog_s_fk_constraint_mark_added(sourceDB, item->conOid, durationMs))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_apply_fk_constraint_comment(dst, item))
	{
		/* errors have already been logged */
		return false;
	}

	return true;
}


/*
 * copydb_apply_fk_constraint_comment re-applies the constraint's own COMMENT
 * (captured by sql/list_source_fk_constraints.sql), right after ADD
 * CONSTRAINT, while the connection is already open. This is not optional:
 * the post-data script's COMMENT ON CONSTRAINT entry for this FK depends (in
 * the archive's own TOC dependency graph) on the FK CONSTRAINT entry that we
 * comment out of the --use-list file, and pg_restore silently drops a
 * dependent COMMENT/ACL entry whose dependency was excluded that way -- no
 * error, the comment is just never applied. Re-applying it ourselves here is
 * the only way it survives when --fk-jobs claims the constraint.
 *
 * A NULL or empty conComment means the source constraint has no comment;
 * nothing to do.
 */
static bool
copydb_apply_fk_constraint_comment(PGSQL *dst, FKConstraintAddItem *item)
{
	if (item->conComment == NULL || item->conComment[0] == '\0')
	{
		return true;
	}

	if (dst->connection == NULL)
	{
		log_error("BUG: copydb_apply_fk_constraint_comment: "
				  "no connection to the target database");
		return false;
	}

	char *literal = PQescapeLiteral(dst->connection,
									item->conComment,
									strlen(item->conComment));

	if (literal == NULL)
	{
		log_error("Failed to escape COMMENT text for FOREIGN KEY "
				  "constraint %s: %s",
				  item->conName,
				  PQerrorMessage(dst->connection));
		return false;
	}

	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd,
					  "COMMENT ON CONSTRAINT %s ON %s IS %s",
					  item->conName,
					  item->conRelQname,
					  literal);

	PQfreemem(literal);

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create COMMENT query for FOREIGN KEY "
				  "constraint \"%s\": out of memory",
				  item->conName);
		destroyPQExpBuffer(cmd);
		return false;
	}

	log_notice("%s;", cmd->data);

	bool success = pgsql_execute(dst, cmd->data);

	destroyPQExpBuffer(cmd);

	if (!success)
	{
		log_error("Failed to set comment on FOREIGN KEY constraint %s on %s, "
				  "see above for details",
				  item->conName,
				  item->conRelQname);
		return false;
	}

	return true;
}


/*
 * copydb_start_fk_workers creates as many sub-processes as --fk-jobs to
 * drain the VALIDATE CONSTRAINT queue.
 */
bool
copydb_start_fk_workers(CopyDataSpec *specs)
{
	for (int i = 0; i < specs->fkJobs; i++)
	{
		/*
		 * Flush stdio channels just before fork, to avoid double-output
		 * problems.
		 */
		fflush(stdout);
		fflush(stderr);

		int fpid = fork();

		switch (fpid)
		{
			case -1:
			{
				log_error("Failed to fork a VALIDATE CONSTRAINT "
						  "worker process: %m");
				return false;
			}

			case 0:
			{
				/* child process runs the command */
				(void) set_ps_title("pgcopydb: validate fk worker");

				if (!copydb_fk_worker(specs))
				{
					/* errors have already been logged */
					exit(EXIT_CODE_INTERNAL_ERROR);
				}

				exit(EXIT_CODE_QUIT);
			}

			default:
			{
				/* fork succeeded, in parent */
				break;
			}
		}
	}

	return true;
}


/*
 * copydb_fk_worker is a worker process that loops over messages received
 * from the FK queue, each message being the oid of a referencing (child)
 * table whose still-pending FOREIGN KEY constraints must be validated.
 *
 * Grouping Phase B work by child table, one message per table, ensures that
 * constraints on the same table (whose VALIDATE CONSTRAINT lock,
 * ShareUpdateExclusive, self-conflicts) are always validated one after the
 * other by the same worker, with no cross-worker coordination needed.
 */
bool
copydb_fk_worker(CopyDataSpec *specs)
{
	pid_t pid = getpid();

	log_notice("Started VALIDATE CONSTRAINT worker %d [%d]", pid, getppid());

	if (!catalog_init_from_specs(specs))
	{
		log_error("Failed to open internal catalogs in VALIDATE CONSTRAINT "
				  "worker, see above for details");
		return false;
	}

	PGSQL dst = { 0 };

	if (!pgsql_init(&dst, specs->connStrings.target_pguri, PGSQL_CONN_TARGET))
	{
		return false;
	}

	if (!pgsql_set_gucs(&dst, dstSettings))
	{
		log_fatal("Failed to set our GUC settings on the target connection, "
				  "see above for details");
		return false;
	}

	int errors = 0;
	bool stop = false;

	while (!stop)
	{
		QMessage mesg = { 0 };
		bool recv_ok = queue_receive(&(specs->fkQueue), &mesg);

		if (asked_to_stop || asked_to_stop_fast || asked_to_quit)
		{
			log_error("VALIDATE CONSTRAINT worker has been interrupted");
			(void) pgsql_finish(&dst);
			return false;
		}

		if (!recv_ok)
		{
			/* errors have already been logged */
			(void) pgsql_finish(&dst);
			return false;
		}

		switch (mesg.type)
		{
			case QMSG_TYPE_STOP:
			{
				stop = true;
				log_debug("Stop message received by validate fk worker");
				break;
			}

			case QMSG_TYPE_TABLEOID:
			{
				uint32_t conRelOid = mesg.data.tp.oid;

				if (!copydb_validate_fk_constraints_for_table(specs,
															  &dst,
															  conRelOid))
				{
					++errors;

					log_error("Failed to validate FOREIGN KEY constraints "
							  "for table oid %u, see above for details",
							  conRelOid);

					if (specs->failFast)
					{
						(void) pgsql_finish(&dst);
						return false;
					}
				}
				break;
			}

			default:
			{
				log_error("Received unknown message type %ld on "
						  "validate fk queue %d",
						  mesg.type,
						  specs->fkQueue.qId);
				break;
			}
		}
	}

	(void) pgsql_finish(&dst);

	if (!catalog_close_from_specs(specs))
	{
		/* errors have already been logged */
		return false;
	}

	bool success = (stop == true && errors == 0);

	if (errors > 0)
	{
		log_error("VALIDATE CONSTRAINT worker %d encountered %d errors, "
				  "see above for details",
				  pid,
				  errors);
	}

	return success;
}


/*
 * copydb_fk_workers_send_stop sends the STOP message to the VALIDATE
 * CONSTRAINT workers, one per worker started.
 */
bool
copydb_fk_workers_send_stop(CopyDataSpec *specs)
{
	for (int i = 0; i < specs->fkJobs; i++)
	{
		QMessage stop = { .type = QMSG_TYPE_STOP, .data.oid = 0 };

		log_debug("Send STOP message to VALIDATE CONSTRAINT queue %d",
				  specs->fkQueue.qId);

		if (!queue_send(&(specs->fkQueue), &stop))
		{
			/* errors have already been logged */
			continue;
		}
	}

	return true;
}


/*
 * copydb_collect_fk_child_table_hook is an iterator callback function that
 * appends a child table oid to an in-memory, realloc-grown array.
 */
static bool
copydb_collect_fk_child_table_hook(void *ctx, uint32_t oid)
{
	FKChildTableOIDArray *array = (FKChildTableOIDArray *) ctx;

	if (array->count == array->capacity)
	{
		int newCapacity = array->capacity == 0 ? 16 : array->capacity * 2;
		uint32_t *newArray =
			(uint32_t *) realloc(array->array, newCapacity * sizeof(uint32_t));

		if (newArray == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		array->array = newArray;
		array->capacity = newCapacity;
	}

	array->array[(array->count)++] = oid;

	return true;
}


/*
 * copydb_validate_fk_constraints_for_table runs Phase B for every claimed,
 * still-pending FOREIGN KEY constraint of a single referencing (child)
 * table, one ALTER TABLE ... VALIDATE CONSTRAINT statement at a time on the
 * given connection.
 *
 * The pending constraints are first materialized into an in-memory array,
 * closing the SQLite iterator (and releasing its WAL read snapshot) before
 * any long-running DDL or catalog write happens: see the comment on
 * FKConstraintValidateArray above for why this matters.
 *
 * Constraints are validated one at a time, rather than batched into a single
 * multi-clause ALTER TABLE, so that a failure on one constraint does not roll
 * back another constraint's already-successful validation, and so that each
 * constraint gets its own accurate per-constraint duration and done-time.
 */
bool
copydb_validate_fk_constraints_for_table(CopyDataSpec *specs,
										 PGSQL *dst,
										 uint32_t conRelOid)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	FKConstraintValidateArray fkArray = { 0, 0, NULL };

	bool iterOk =
		catalog_iter_s_fk_constraint_table(sourceDB,
										   conRelOid,
										   &fkArray,
										   &copydb_collect_fk_constraint_validate_hook);

	if (!iterOk)
	{
		/* errors have already been logged */
		free(fkArray.array);
		return false;
	}

	int errors = 0;

	for (int i = 0; i < fkArray.count; i++)
	{
		if (!copydb_validate_one_fk_constraint(specs,
											   dst,
											   &(fkArray.array[i]),
											   &errors))
		{
			if (specs->failFast)
			{
				break;
			}
		}
	}

	free(fkArray.array);

	return errors == 0;
}


/*
 * copydb_collect_fk_constraint_validate_hook is an iterator callback function
 * that copies the fields Phase B needs into an in-memory, realloc-grown
 * array. It deliberately does not copy fk->conDef: Phase B never needs it.
 */
static bool
copydb_collect_fk_constraint_validate_hook(void *ctx, SourceFKConstraint *fk)
{
	FKConstraintValidateArray *array = (FKConstraintValidateArray *) ctx;

	if (array->count == array->capacity)
	{
		int newCapacity = array->capacity == 0 ? 16 : array->capacity * 2;
		FKConstraintValidateItem *newArray =
			(FKConstraintValidateItem *) realloc(array->array,
												 newCapacity *
												 sizeof(FKConstraintValidateItem));

		if (newArray == NULL)
		{
			log_error(ALLOCATION_FAILED_ERROR);
			return false;
		}

		array->array = newArray;
		array->capacity = newCapacity;
	}

	FKConstraintValidateItem *item = &(array->array[(array->count)++]);

	item->conOid = fk->conOid;

	strlcpy(item->conName, fk->conName, sizeof(item->conName));
	strlcpy(item->conRelQname, fk->conRelQname, sizeof(item->conRelQname));
	strlcpy(item->confRelQname, fk->confRelQname, sizeof(item->confRelQname));

	item->conValidated = fk->conValidated;
	item->validatedTime = fk->validatedTime;

	return true;
}


/*
 * copydb_validate_one_fk_constraint runs Phase B for a single FOREIGN KEY
 * constraint. On failure it increments *errors and returns false, but never
 * drops the constraint: it stays in place as NOT VALID, still enforcing new
 * writes.
 */
static bool
copydb_validate_one_fk_constraint(CopyDataSpec *specs,
								  PGSQL *dst,
								  FKConstraintValidateItem *item,
								  int *errors)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	/*
	 * A constraint that was already NOT VALID on the source must stay that
	 * way on the target: never validate it.
	 */
	if (!item->conValidated)
	{
		return true;
	}

	if (item->validatedTime > 0)
	{
		log_debug("Skipping VALIDATE CONSTRAINT %s: already done (at %lld)",
				  item->conName,
				  (long long) item->validatedTime);
		return true;
	}

	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd,
					  "ALTER TABLE %s VALIDATE CONSTRAINT %s",
					  item->conRelQname,
					  item->conName);

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create query for VALIDATE CONSTRAINT \"%s\": "
				  "out of memory",
				  item->conName);
		destroyPQExpBuffer(cmd);
		++(*errors);
		return false;
	}

	if (specs->datname[0] != '\0')
	{
		log_notice("%s: %s;", specs->datname, cmd->data);
	}
	else
	{
		log_notice("%s;", cmd->data);
	}

	uint64_t startTime = time(NULL);
	bool success = false;

	/*
	 * On failure, and when the opt-in --retry-count is greater than 0, retry
	 * the same VALIDATE CONSTRAINT immediately (no backoff) up to that many
	 * extra times. VALIDATE CONSTRAINT is idempotent (re-validating an
	 * already-validated constraint is a no-op), so this is always safe; it
	 * only actually helps with infrastructure failures though, since a
	 * genuine FOREIGN KEY violation (sqlstate 23503) fails identically on
	 * every attempt.
	 */
	int attempts = 0;
	int maxAttempts = 1 + specs->retryCount;

	bool retry = true;

	while (!success && retry)
	{
		++attempts;

		success = pgsql_execute(dst, cmd->data);

		if (success)
		{
			if (attempts > 1)
			{
				log_info("VALIDATE CONSTRAINT %s succeeded after %d attempts",
						 item->conName,
						 attempts);
			}
			break;
		}

		retry = attempts < maxAttempts;

		if (asked_to_quit || asked_to_stop || asked_to_stop_fast)
		{
			break;
		}

		if (retry)
		{
			log_warn("Failed to validate FOREIGN KEY constraint %s "
					 "(attempt %d/%d), retrying immediately",
					 item->conName,
					 attempts,
					 maxAttempts);

			if (!copydb_reset_target_connection(dst))
			{
				/* errors have already been logged */
				break;
			}
		}
	}

	uint64_t durationMs = (time(NULL) - startTime) * 1000;

	destroyPQExpBuffer(cmd);

	if (!success)
	{
		/*
		 * This is expected to happen when the data genuinely violates the
		 * FOREIGN KEY (sqlstate 23503): the constraint stays in place as
		 * NOT VALID, enforcing new writes, and the operator can fix the
		 * data and re-run `pgcopydb copy fk-constraints --resume
		 * --not-consistent` to retry validation. We never drop the
		 * constraint here.
		 */
		log_error("Failed to validate FOREIGN KEY constraint %s on %s "
				  "referencing %s even after %d attempt(s): the constraint "
				  "remains in place as NOT VALID; fix the underlying data "
				  "and re-run `pgcopydb copy fk-constraints --resume "
				  "--not-consistent` to retry",
				  item->conName,
				  item->conRelQname,
				  item->confRelQname,
				  attempts);

		++(*errors);

		return false;
	}

	if (!catalog_s_fk_constraint_mark_validated(sourceDB, item->conOid, durationMs))
	{
		/* errors have already been logged */
		++(*errors);
		return false;
	}

	if (!summary_increment_timing(sourceDB,
								  TIMING_SECTION_FK_VALIDATE,
								  1, /* count */
								  0, /* bytes */
								  durationMs))
	{
		/* errors have already been logged */
		++(*errors);
		return false;
	}

	return true;
}
