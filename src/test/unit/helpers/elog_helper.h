#ifndef GPDB_ELOG_HELPER_H
#define GPDB_ELOG_HELPER_H

void expect_elog(int log_level);

void expect_elog_with_message(int log_level, char *message);

#endif  // GPDB_ELOG_HELPER_H
