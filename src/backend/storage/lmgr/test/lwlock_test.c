#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem_fake.h"
#include "storage/proc.h"
#include "elog_helper.h"
#include "../../../../test/unit/helpers/elog_helper.h"

/*
 * The next structures are only present in this file because
 * they are internal state structures that are needed while testing.
 *
 * This was duplicated for now because
 * 1. These structures should not change very often
 * 2. We cannot move the upstream code into the .h
 *
 * TODO: The 2 points from above should be addressed in an upstream
 * commit that we need to do. This commit will add testing around
 * the lwlock, similar to what we have in this file and also move these
 * structure into the .h file to allow us to use them in tests
 */
typedef struct LWLockTest
{
	slock_t mutex;
	bool releaseOK;
	char exclusive;
	int shared;
	int exclusivePid;
	PGPROC *head;
	PGPROC *tail;

} LWLockTest;

#define LWLOCK_PADDED_SIZE (sizeof(LWLockTest) <= 16 ? 16 : 32)

typedef union LWLockPaddedTest {
	LWLockTest lock;
	char pad[LWLOCK_PADDED_SIZE];
} LWLockPaddedTest;

static PGPROC pg_process_information = {};

LWLockPaddedTest *translate_memory_allocated_into_locks_structure();
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
test__NumLWLocks_it_returns_maximum_number_of_different_locks_available_in_the_system(
	void)
{
	assert_int_equal(NumLWLocks(), 8560);
}

void
test__LWLockShmemSize__expect_273960b_to_be_used_on_shmem(void)
{
	assert_int_equal(LWLockShmemSize(), 273960);
}

void
test__CreateLWLocks_creates_all_locks(void)
{
	CreateLWLocks();
	/*
	 * The memory allocated should be the same as the LWLock Shared Memory Size
	 */
	assert_int_equal(shmem_fake_get_total_memory_allocated(), 273960);
}

void
test__LWLockAcquire_when_no_lock_was_acquired_before_and_try_to_acquire_it_does_so_succesfuly(
	void)
{
	CreateLWLocks();
	LWLockAcquire(3, LW_EXCLUSIVE);
	assert_true("Success aquiring the lock");
	LWLockRelease(3);
}

void
test__LWLockAcquire_when_trying_to_acquire_101_locks_it_elogs_error(void)
{
	CreateLWLocks();
	for (int i = 1; i < 101; i++)
		LWLockAcquire(i, LW_EXCLUSIVE);
	assert_true("Success aquiring 100 the locks");

	expect_elog_with_message(ERROR, "too many LWLocks taken");
	PG_TRY();
	{
		LWLockAcquire(101, LW_EXCLUSIVE);
		for (int i = 1; i < 101; i++)
			LWLockRelease(i);
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
		LWLockRelease(i);
}

void
test__LWLockAcquire_when_trying_to_acquire_a_lock_twice_it_elogs_panic(void)
{
	setup_pg_process_information();
	CreateLWLocks();
	LWLockAcquire(1, LW_EXCLUSIVE);
	assert_true("Success aquiring the lock for the first time");

	expect_elog_with_message(PANIC, "Waiting on lock already held!");
	PG_TRY();
	{
		LWLockAcquire(1, LW_EXCLUSIVE);
		fail_msg("elog Should have been called");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();

	expect_any(PGSemaphoreUnlock, sema);
	will_be_called(PGSemaphoreUnlock);
	LWLockRelease(1);
}

static void
setup_outside_pg_process(void)
{
	MyProc = NULL;
}

static void
teardown_outside_pg_process(void)
{
	setup_pg_process_information();
}
void
test__LWLockAcquire_when_trying_to_acquire_a_lock_outside_of_a_pg_process_context_elogs_panic(
	void)
{
	CreateLWLocks();
	LWLockAcquire(1, LW_EXCLUSIVE);
	assert_true("Success aquiring the lock for the first time");

	expect_elog_with_message(PANIC, "cannot wait without a PGPROC structure");
	PG_TRY();
	{
		LWLockAcquire(1, LW_EXCLUSIVE);
		fail_msg("elog Should have been called");
	}
	PG_CATCH();
	{
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

	LWLockAcquire(1, LW_EXCLUSIVE);


	expect_any(PGSemaphoreUnlock, sema);
	will_be_called(PGSemaphoreUnlock);
	LWLockRelease(1);
}

void
test__LWLockRelease_when_did_not_acquire_a_lock_it_elogs(void)
{
	CreateLWLocks();
	expect_elog_with_message(ERROR, "lock %d is not held");
	PG_TRY();
	{
		LWLockRelease(2);
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
	LWLockAcquire(1, LW_EXCLUSIVE);
	assert_true(LWLockHeldByMe(1));
	LWLockRelease(1);
}

void
test__LWLockHeldByMe_when_process_is_not_holding_a_lock_it_return_false(void)
{
	CreateLWLocks();
	assert_false(LWLockHeldByMe(5));
}

void
test__LWLockHeldExclusiveByMe_when_process_hold_an_exclusive_lock_it_return_true(
	void)
{
	CreateLWLocks();
	LWLockAcquire(1, LW_EXCLUSIVE);
	assert_true(LWLockHeldExclusiveByMe(1));
	LWLockRelease(1);
}

void
test__LWLockHeldExclusiveByMe_when_process_hold_a_shared_lock_it_return_false(
	void)
{
	CreateLWLocks();
	LWLockAcquire(1, LW_SHARED);
	assert_false(LWLockHeldExclusiveByMe(1));
	LWLockRelease(1);
}

void
test__LWLockHeldExclusiveByMe_when_process_is_not_holding_a_lock_it_return_false(
	void)
{
	CreateLWLocks();
	assert_false(LWLockHeldByMe(5));
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test_setup_teardown(
			test__NumLWLocks_it_returns_maximum_number_of_different_locks_available_in_the_system,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(
			test__LWLockShmemSize__expect_273960b_to_be_used_on_shmem,
			reset_process_information,
			default_teardown),
		unit_test_setup_teardown(test__CreateLWLocks_creates_all_locks,
								 reset_process_information,
								 default_teardown),
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
LWLockPaddedTest *
translate_memory_allocated_into_locks_structure()
{
	void *memory_location = shmem_fake_retrieve_allocated_memory();
	memory_location += 2 * sizeof(int);
	memory_location +=
		LWLOCK_PADDED_SIZE - ((uintptr_t) memory_location) % LWLOCK_PADDED_SIZE;
	return (LWLockPaddedTest *) memory_location;
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
	LWLockPaddedTest *all_locks =
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
	LWLockPaddedTest *all_locks =
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