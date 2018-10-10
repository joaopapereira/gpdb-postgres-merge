subdir=src/backend/storage/lmgr
top_builddir=../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=lock \
        lwlock

include $(top_builddir)/src/backend/mock.mk


lwlock.t: ../lwlock.o \
		$(TEST_DOUBLE_DIR)/backend/storage/ipc/shmem_fake.o \
		$(UNIT_TEST_HELPER_DIR)/elog_helper.o \
		$(MOCK_DIR)/backend/utils/error/elog_mock.o \
		$(MOCK_DIR)/backend/port/pg_sema_mock.o