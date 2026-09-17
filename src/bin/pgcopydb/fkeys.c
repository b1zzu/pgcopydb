/*
 * src/bin/pgcopydb/fkeys.c
 *     Opt-in parallel two-phase build of FOREIGN KEY constraints claimed out
 *     of the pg_restore --section=post-data script.
 *
 * See the comment block in copydb.h just above the fkeys.c declarations for
 * the design rationale (why Phase A is sequential and Phase B is not).
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

static bool copydb_add_fk_constraint_not_valid_hook(void *ctx,
													SourceFKConstraint *fk);

typedef struct AddFKConstraintsContext
{
	CopyDataSpec *specs;
	PGSQL *dst;
	int errors;
} AddFKConstraintsContext;


typedef struct FKChildTableOIDArray
{
	int count;
	int capacity;
	uint32_t *array;            /* malloc'ed, grown with realloc */
} FKChildTableOIDArray;

static bool copydb_collect_fk_child_table_hook(void *ctx, uint32_t oid);


typedef struct ValidateFKConstraintsContext
{
	CopyDataSpec *specs;
	PGSQL *dst;
	int errors;
} ValidateFKConstraintsContext;

static bool copydb_validate_fk_constraint_hook(void *ctx, SourceFKConstraint *fk);


/*
 * copydb_create_all_fk_constraints is the entry point for the opt-in
 * parallel two-phase FOREIGN KEY build. It is a no-op, returning true
 * immediately, unless --fk-jobs has been used (specs->fkJobs > 0).
 *
 * Unlike the CREATE INDEX and VACUUM phases, this does not fork a
 * "supervisor" process for itself: it is always the last schema-building
 * step (STEP 11, after the post-data restore), nothing else runs
 * concurrently with it in the calling process, so the extra indirection
 * would add nothing.
 */
bool
copydb_create_all_fk_constraints(CopyDataSpec *specs)
{
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

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

	CatalogCounts count = { 0 };

	if (!catalog_count_objects(sourceDB, &count))
	{
		log_error("Failed to count FOREIGN KEY constraints in our catalogs");
		return false;
	}

	if (count.fkConstraints == 0)
	{
		log_info("STEP 11: no FOREIGN KEY constraints to build in parallel");
		return true;
	}

	log_info("STEP 11: building %lld FOREIGN KEY constraints "
			 "using %d processes",
			 (long long) count.fkConstraints,
			 specs->fkJobs);

	/*
	 * Phase A: ALTER TABLE ... ADD CONSTRAINT ... NOT VALID, sequential, in
	 * this process. Pure catalog work, milliseconds each.
	 */
	if (!summary_start_timing(sourceDB, TIMING_SECTION_FK_ADD))
	{
		/* errors have already been logged */
		return false;
	}

	if (!copydb_add_fk_constraints_not_valid(specs))
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

	/*
	 * Phase B: ALTER TABLE ... VALIDATE CONSTRAINT, run by a worker pool
	 * sized by --fk-jobs, one child table per worker message.
	 */
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
	 * validated. Otherwise a --resume must re-enter this function and finish
	 * the remaining work, which is safe: both phases are incremental.
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
 */
bool
copydb_add_fk_constraints_not_valid(CopyDataSpec *specs)
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

	AddFKConstraintsContext context = {
		.specs = specs,
		.dst = &dst,
		.errors = 0
	};

	bool iterOk =
		catalog_iter_s_fk_constraint(sourceDB,
									 &context,
									 &copydb_add_fk_constraint_not_valid_hook);

	(void) pgsql_finish(&dst);

	if (!iterOk)
	{
		/* errors have already been logged */
		return false;
	}

	return context.errors == 0;
}


/*
 * copydb_add_fk_constraint_not_valid_hook is an iterator callback function.
 */
static bool
copydb_add_fk_constraint_not_valid_hook(void *ctx, SourceFKConstraint *fk)
{
	AddFKConstraintsContext *context = (AddFKConstraintsContext *) ctx;
	CopyDataSpec *specs = context->specs;
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	if (fk->addedTime > 0)
	{
		log_debug("Skipping FOREIGN KEY %s: already added (done at %lld)",
				  fk->conName,
				  (long long) fk->addedTime);
		return true;
	}

	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd,
					  "ALTER TABLE %s ADD CONSTRAINT %s %s",
					  fk->conRelQname,
					  fk->conName,
					  fk->conDef);

	/*
	 * pg_get_constraintdef() already appends " NOT VALID" by itself when the
	 * constraint is NOT VALID on the source (fk->conValidated == false): in
	 * that case we must NOT validate it later, and the command above already
	 * matches the source state as-is.
	 *
	 * When the source constraint is valid (the common case), we deliberately
	 * add it as NOT VALID here, and validate it in Phase B instead, so that
	 * this statement only ever takes a lock for milliseconds.
	 */
	if (fk->conValidated)
	{
		appendPQExpBufferStr(cmd, " NOT VALID");
	}

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create query for FOREIGN KEY \"%s\": "
				  "out of memory",
				  fk->conName);
		destroyPQExpBuffer(cmd);
		++context->errors;
		return !specs->failFast;
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
	bool success = pgsql_execute(context->dst, cmd->data);
	uint64_t durationMs = (time(NULL) - startTime) * 1000;

	destroyPQExpBuffer(cmd);

	if (!success)
	{
		/*
		 * Edge case: a previous run may have already restored this very
		 * constraint via the ordinary pg_restore --section=post-data path,
		 * for instance when --fk-jobs is turned on for the first time on a
		 * --resume of a run whose STEP 10 had already completed without it.
		 * Postgres has no ADD CONSTRAINT IF NOT EXISTS, so treat a
		 * duplicate_object (42710) error as success: the constraint is
		 * already there, we just did not know about it yet.
		 */
		bool alreadyExists = streq(context->dst->sqlstate, "42710");

		if (alreadyExists)
		{
			log_notice("FOREIGN KEY constraint %s on %s already exists on "
					   "the target, treating Phase A as already done for it",
					   fk->conName,
					   fk->conRelQname);
		}
		else
		{
			log_error("Failed to add FOREIGN KEY constraint %s on %s, "
					  "see above for details",
					  fk->conName,
					  fk->conRelQname);

			++context->errors;

			return !specs->failFast;
		}
	}

	if (!catalog_s_fk_constraint_mark_added(sourceDB, fk->conOid, durationMs))
	{
		/* errors have already been logged */
		++context->errors;
		return !specs->failFast;
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

	ValidateFKConstraintsContext context = {
		.specs = specs,
		.dst = dst,
		.errors = 0
	};

	bool iterOk =
		catalog_iter_s_fk_constraint_table(sourceDB,
										   conRelOid,
										   &context,
										   &copydb_validate_fk_constraint_hook);

	if (!iterOk)
	{
		/* errors have already been logged */
		return false;
	}

	return context.errors == 0;
}


/*
 * copydb_validate_fk_constraint_hook is an iterator callback function.
 */
static bool
copydb_validate_fk_constraint_hook(void *ctx, SourceFKConstraint *fk)
{
	ValidateFKConstraintsContext *context = (ValidateFKConstraintsContext *) ctx;
	CopyDataSpec *specs = context->specs;
	DatabaseCatalog *sourceDB = &(specs->catalogs.source);

	/*
	 * A constraint that was already NOT VALID on the source must stay that
	 * way on the target: never validate it.
	 */
	if (!fk->conValidated)
	{
		return true;
	}

	if (fk->validatedTime > 0)
	{
		log_debug("Skipping VALIDATE CONSTRAINT %s: already done (at %lld)",
				  fk->conName,
				  (long long) fk->validatedTime);
		return true;
	}

	PQExpBuffer cmd = createPQExpBuffer();

	appendPQExpBuffer(cmd,
					  "ALTER TABLE %s VALIDATE CONSTRAINT %s",
					  fk->conRelQname,
					  fk->conName);

	if (PQExpBufferBroken(cmd))
	{
		log_error("Failed to create query for VALIDATE CONSTRAINT \"%s\": "
				  "out of memory",
				  fk->conName);
		destroyPQExpBuffer(cmd);
		++context->errors;
		return !specs->failFast;
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
	bool success = pgsql_execute(context->dst, cmd->data);
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
				  "referencing %s: the constraint remains in place as "
				  "NOT VALID; fix the underlying data and re-run "
				  "`pgcopydb copy fk-constraints --resume --not-consistent` "
				  "to retry",
				  fk->conName,
				  fk->conRelQname,
				  fk->confRelQname);

		++context->errors;

		return !specs->failFast;
	}

	if (!catalog_s_fk_constraint_mark_validated(sourceDB, fk->conOid, durationMs))
	{
		/* errors have already been logged */
		++context->errors;
		return !specs->failFast;
	}

	if (!summary_increment_timing(sourceDB,
								  TIMING_SECTION_FK_VALIDATE,
								  1, /* count */
								  0, /* bytes */
								  durationMs))
	{
		/* errors have already been logged */
		++context->errors;
		return !specs->failFast;
	}

	return true;
}
