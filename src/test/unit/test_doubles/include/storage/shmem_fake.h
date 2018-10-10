#ifndef GPDB_SHMEM_FAKE_H
#define GPDB_SHMEM_FAKE_H

/*
 * Function will return the total amount of shared memory
 * that was allocated since the beginning of the execution
 */
int64 shmem_fake_get_total_memory_allocated(void);

char * shmem_fake_retrieve_allocated_memory(void);

#endif //GPDB_SHMEM_FAKE_H
