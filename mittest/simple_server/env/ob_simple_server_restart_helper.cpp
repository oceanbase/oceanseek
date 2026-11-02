/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cerrno>
#include <cstdlib>
#include <sys/wait.h>

#define protected public
#define private public

#include "ob_simple_server_restart_helper.h"

#include "ob_simple_cluster_test_base.h"

#undef private
#undef protected

bool ObSimpleServerRestartHelper::need_restart_ = false;
bool ObSimpleServerRestartHelper::is_restart_ = false;
int ObSimpleServerRestartHelper::sleep_sec_ = 0;

// #define RUN_TEST_CASE(type)                                                                      \
//   sprintf(log_file_name, "%s_%s.log", test_file_name_, #type);                                   \
//   sprintf(rs_log_file_name, "%s_%s_rs.log", test_file_name_, #type);                             \
//   sprintf(election_log_file_name, "%s_%s_election.log", test_file_name_, #type);                 \
//   sprintf(filter_str, "%s*", type##_case_name_);                                                 \
//                                                                                                  \
//   OB_LOGGER.set_file_name(log_file_name, true, false, rs_log_file_name, election_log_file_name); \
//   OB_LOGGER.set_log_level(log_level_);                                                           \
//   ::testing::InitGoogleTest(&argc_, argv_);                                                      \
//                                                                                                  \
//   ::testing::GTEST_FLAG(filter) = filter_str;                                                    \
//   ret = RUN_ALL_TESTS();


int ObSimpleServerRestartHelper::run()
{
  need_restart_ = true;
  int ret = 0;
  char log_file_name[name_len];
  char rs_log_file_name[name_len];
  char election_log_file_name[name_len];
  char trace_log_file_name[name_len];
  char audit_log_file_name[name_len];
  char filter_str[name_len];
  memset(log_file_name, 0, name_len);
  memset(rs_log_file_name, 0, name_len);
  memset(election_log_file_name, 0, name_len);
  memset(filter_str, 0, name_len);

  GCONF._enable_defensive_check = false;
  GCONF._private_buffer_size = 1;
  auto child_pid = fork();

  if (child_pid < 0) {
    perror("fork");
    exit(EXIT_FAILURE);
  } else if (child_pid == 0) {

    sprintf(log_file_name, "%s_born.log", test_file_name_);
    sprintf(rs_log_file_name, "%s_born_rs.log", test_file_name_);
    sprintf(election_log_file_name, "%s_born_election.log", test_file_name_);
    sprintf(trace_log_file_name, "%s_born_trace.log", test_file_name_);
    sprintf(audit_log_file_name, "%s_born_audit.log", test_file_name_);
    sprintf(filter_str, "%s*", born_case_name_);

    OB_LOGGER.set_file_name(
        log_file_name, true, false, rs_log_file_name, election_log_file_name, trace_log_file_name, audit_log_file_name);
    OB_LOGGER.set_log_level(log_level_);
    ::testing::InitGoogleTest(&argc_, argv_);

    ::testing::GTEST_FLAG(filter) = filter_str;
    ret = RUN_ALL_TESTS();

    fprintf(stdout, "Child process exit ret = %d.\n", ret);

  } else {
    // parent process
    int status = 0;
    pid_t waited_pid;
    do {
      waited_pid = waitpid(child_pid, &status, 0);
    } while (waited_pid == -1 && errno == EINTR);
    if (waited_pid == -1) {
      perror("waitpid");
      return EXIT_FAILURE;
    } else if (!WIFEXITED(status)) {
      fprintf(stdout, "Child process terminated abnormally, wait status : %d\n", status);
      return EXIT_FAILURE;
    } else if (0 != WEXITSTATUS(status)) {
      ret = WEXITSTATUS(status);
      fprintf(stdout, "Child process exit with error code : %d\n", ret);
      return ret;
    } else {
      fprintf(stdout, "Child process run all test cases done. child ret = %d \n", status);
    }

    ObSimpleServerRestartHelper::is_restart_ = true;
    // RUN_TEST_CASE(restart);
    sprintf(log_file_name, "%s_restart.log", test_file_name_);
    sprintf(rs_log_file_name, "%s_restart_rs.log", test_file_name_);
    sprintf(election_log_file_name, "%s_restart_election.log", test_file_name_);
    sprintf(trace_log_file_name, "%s_restart_trace.log", test_file_name_);
    sprintf(audit_log_file_name, "%s_restart_audit.log", test_file_name_);
    sprintf(filter_str, "%s*", restart_case_name_);

    OB_LOGGER.set_file_name(
        log_file_name, true, false, rs_log_file_name, election_log_file_name, trace_log_file_name, audit_log_file_name);
    OB_LOGGER.set_log_level(log_level_);
    ::testing::InitGoogleTest(&argc_, argv_);

    ::testing::GTEST_FLAG(filter) = filter_str;
    ret = RUN_ALL_TESTS();

    fprintf(stdout, "Parent process exit ret = %d\n", ret);
  }

  return ret;
}
