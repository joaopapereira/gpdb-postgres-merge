#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem_fake.h"
#include "storage/proc.h"
#include "elog_helper.h"
#include "utils/memutils.h"
#include "../../../../test/unit/helpers/elog_helper.h"


static PGPROC pg_process_information = {};

LWLockPadded *translate_memory_allocated_into_locks_structure();
void acquire_exclusive_lock_for_testing(int lock_type_to_be_held);
void clear_exclusive_lock_for_testing(int lock_type_to_be_held);
void reset_process_information(void);
void clear_waiting_process(void *none);
void setup_pg_process_information(void);

void
default_teardown(void)
{
	LWLockReleaseAll();
	free(shmem_fake_retrieve_allocated_memory());
}

void
test__LWLockAcquire_when_no_lock_was_acquired_before_and_try_to_acquire_it_does_so_succesfuly(
	void)
{
	CreateLWLocks();
	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	assert_true("Success aquiring the lock");
	LWLockRelease(ShmemIndexLock);
}

void
test__LWLockAcquire_when_trying_to_acquire_101_locks_it_elogs_error(void)
{
	CreateLWLocks();
	for (int i = 1; i < 101; i++)
		LWLockAcquire(&MainLWLockArray[i].lock, LW_EXCLUSIVE);
	assert_true("Success aquiring 100 the locks");

	expect_elog_with_message(ERROR, "too many LWLocks taken");
	PG_TRY();
	{
		LWLockAcquire(&MainLWLockArray[101].lock, LW_EXCLUSIVE);
		for (int i = 1; i < 101; i++)
			LWLockRelease(&MainLWLockArray[i].lock);
		fail_msg("elog Should have been called");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();

	assert_false(pg_process_information.lwWaiting);
	assert_int_equal(pg_process_information.lwWaitMode, 100);
	assert_true(pg_process_information.lwWaitLink == NULL);

	for (int i = 1; i < 101; i++)
		LWLockRelease(&MainLWLockArray[i].lock);
}

void
test__LWLockAcquire_when_trying_to_acquire_a_lock_twice_it_elogs_panic(void)
{
	setup_pg_process_information();
	CreateLWLocks();
	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	assert_true("Success aquiring the lock for the first time");

	expect_elog_with_message(PANIC, "Waiting on lock already held!");
	PG_TRY();
	{
		LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
		fail_msg("elog Should have been called");
	}
	PG_CATCH();
	{
		SpinLockRelease(&ShmemIndexLock->mutex);
	}
	PG_END_TRY();

	expect_any(PGSemaphoreUnlock, sema);
	will_be_called(PGSemaphoreUnlock);
	LWLockRelease(ShmemIndexLock);
}

static void
setup_outside_pg_process(void)
{
	MyProc = NULL;
}

static void
teardown_outside_pg_process(void)
{
	default_teardown();
	setup_pg_process_information();
}
void
test__LWLockAcquire_when_trying_to_acquire_a_lock_outside_of_a_pg_process_context_elogs_panic(
	void)
{
	CreateLWLocks();
	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	assert_true("Success aquiring the lock for the first time");

	expect_elog_with_message(PANIC, "cannot wait without a PGPROC structure");
	PG_TRY();
	{
		LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
		LWLockRelease(ShmemIndexLock);
		fail_msg("elog Should have been called");
	}
	PG_CATCH();
	{
		SpinLockRelease(&ShmemIndexLock->mutex);
	}
	PG_END_TRY();
}

void
test__LWLockAcquire_when_trying_to_acquire_a_lock_that_is_held_by_another_process_it_waits_for_other_process_to_release(
	void)
{
	setup_pg_process_information();
	CreateLWLocks();
	acquire_exclusive_lock_for_testing(1);
	expect_any(PGSemaphoreLock, sema);
	expect_any(PGSemaphoreLock, sema);
	expect_any(PGSemaphoreLock, interruptOK);
	expect_any(PGSemaphoreLock, interruptOK);
	will_be_called(PGSemaphoreLock);
	will_be_called_with_sideeffect(
		PGSemaphoreLock, clear_waiting_process, NULL);
	expect_any(PGSemaphoreUnlock, sema);
	will_be_called(PGSemaphoreUnlock);

	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);


	expect_any(PGSemaphoreUnlock, sema);
	will_be_called(PGSemaphoreUnlock);
	LWLockRelease(ShmemIndexLock);
}

void
test__LWLockRelease_when_did_not_acquire_a_lock_it_elogs(void)
{
	CreateLWLocks();
	expect_elog_with_message(ERROR, "lock %s %d is not held");
	PG_TRY();
	{
		LWLockRelease(ShmemIndexLock);
		fail_msg("elog Should have been called");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}

void
test__LWLockHeldByMe_when_process_hold_a_lock_it_return_true(void)
{
	CreateLWLocks();
	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	assert_true(LWLockHeldByMe(ShmemIndexLock));
	LWLockRelease(ShmemIndexLock);
}

void
test__LWLockHeldByMe_when_process_is_not_holding_a_lock_it_return_false(void)
{
	CreateLWLocks();
	assert_false(LWLockHeldByMe(ShmemIndexLock));
}

void
test__LWLockHeldExclusiveByMe_when_process_hold_an_exclusive_lock_it_return_true(
	void)
{
	CreateLWLocks();
	LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);
	assert_true(LWLockHeldExclusiveByMe(ShmemIndexLock));
	LWLockRelease(ShmemIndexLock);
}

void
test__LWLockHeldExclusiveByMe_when_process_hold_a_shared_lock_it_return_false(
	void)
{
	CreateLWLocks();
	LWLockAcquire(ShmemIndexLock, LW_SHARED);
	assert_false(LWLockHeldExclusiveByMe(ShmemIndexLock));
	LWLockRelease(ShmemIndexLock);
}

void
test__LWLockHeldExclusiveByMe_when_process_is_not_holding_a_lock_it_return_false(
	void)
{
	CreateLWLocks();
	assert_false(LWLockHeldByMe(ShmemIndexLock));
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test_setup_teardown(
			test__LWLockAcquire_when_no_lock_was_acquired_before_and_try_to_acquire_it_does_so_succesfuly,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockAcquire_when_trying_to_acquire_101_locks_it_elogs_error,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockAcquire_when_trying_to_acquire_a_lock_twice_it_elogs_panic,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockAcquire_when_trying_to_acquire_a_lock_that_is_held_by_another_process_it_waits_for_other_process_to_release,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockAcquire_when_trying_to_acquire_a_lock_outside_of_a_pg_process_context_elogs_panic,
			setup_outside_pg_process,
			teardown_outside_pg_process),
		unit_test_setup_teardown(
			test__LWLockRelease_when_did_not_acquire_a_lock_it_elogs,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockHeldByMe_when_process_hold_a_lock_it_return_true,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockHeldByMe_when_process_is_not_holding_a_lock_it_return_false,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockHeldExclusiveByMe_when_process_hold_an_exclusive_lock_it_return_true,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockHeldExclusiveByMe_when_process_hold_a_shared_lock_it_return_false,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockHeldExclusiveByMe_when_process_is_not_holding_a_lock_it_return_false,
			setup_outside_pg_process,
			teardown_outside_pg_process),
	};


    /*
     * The following setup is needed to ensure ShmemAlloc is able to allocate
     * some memory
     */
    MemoryContextInit();
    RequestAddinLWLocks(100);

	return run_tests(tests);
}


/*
 * Helper Methods
 */
void
setup_pg_process_information(void)
{
	MyProc = &pg_process_information;
}

/*
 * This function is very specific to our lwlock.c module. It ties the test
 * to the implementation, because we are assuming that we know how
 * the memory allocation is done in the function CreateLWLocks
 * To be fair the best options would be for us to some how have direct query
 * access to eventually update access to the lock.
 * This might create a problem in the real implementation. So we need to be
 * careful if we are going to do this
 */
LWLockPadded *
translate_memory_allocated_into_locks_structure()
{
//	void *memory_location = shmem_fake_retrieve_allocated_memory();
//	memory_location += 2 * sizeof(int);
//	memory_location +=
//		LWLOCK_PADDED_SIZE - ((uintptr_t) memory_location) % LWLOCK_PADDED_SIZE;
//	return (LWLockPadded *) memory_location;
    return MainLWLockArray;
}

/*
 * This function as a direct access to the part of the memory that
 * is storing the locks and updates the exclusive lock to the
 * lock passed in the argument `lock_type_to_be_held`.
 * This is a backdoor that we were talking in the function
 * `translate_memory_allocated_into_locks_structure` that would
 * help us do some better testing in this unit, but at the same
 * time it ties us to the implementation of the locks
 */
void
acquire_exclusive_lock_for_testing(int lock_type_to_be_held)
{
	LWLockPadded *all_locks =
		translate_memory_allocated_into_locks_structure();
	all_locks[lock_type_to_be_held].lock.exclusive++;
}

/*
 * This function as a direct access to the part of the memory that
 * is storing the locks and removes the exclusive lock on the
 * lock passed in the argument `lock_type_to_be_held`.
 * This is a backdoor that we were talking in the function
 * `translate_memory_allocated_into_locks_structure` that would
 * help us do some better testing in this unit, but at the same
 * time it ties us to the implementation of the locks
 */
void
clear_exclusive_lock_for_testing(int lock_type_to_be_held)
{
	LWLockPadded *all_locks =
		translate_memory_allocated_into_locks_structure();
	all_locks[lock_type_to_be_held].lock.exclusive--;
}

void
reset_process_information(void)
{
	pg_process_information.lwWaiting = false;
	pg_process_information.lwWaitMode = 100;
	pg_process_information.lwWaitLink = NULL;
}

void
clear_waiting_process(void *none)
{
	(void *) none;
	MyProc->lwWaiting = false;
	clear_exclusive_lock_for_testing(1);
}
