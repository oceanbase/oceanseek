/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>
#include "env/ob_simple_server_restart_helper.h"

namespace
{
const char *scenario = nullptr;

TEST(RestartHelperBefore, result)
{
  if (0 == strcmp(scenario, "before_signal")) {
    raise(SIGTERM);
  }
  ASSERT_STRNE("before_failure", scenario);
}

TEST(RestartHelperAfter, result)
{
  ASSERT_STRNE("after_failure", scenario);
}

void check_exit_status(const char *test_scenario, int expected_status)
{
  const pid_t child = fork();
  ASSERT_NE(-1, child);
  if (0 == child) {
    // Intentional GTest failures belong to the subprocess, not this test's
    // result. Keep them out of the outer runner's console output.
    const int sink = open("/dev/null", O_WRONLY);
    if (sink < 0 || dup2(sink, STDOUT_FILENO) < 0 || dup2(sink, STDERR_FILENO) < 0) {
      _exit(126);
    }
    close(sink);
    execl("/proc/self/exe", "test_simple_server_restart_helper",
          "--restart-helper-scenario", test_scenario, nullptr);
    _exit(127);
  }
  int status = 0;
  pid_t waited;
  do {
    waited = waitpid(child, &status, 0);
  } while (waited == -1 && errno == EINTR);
  ASSERT_EQ(child, waited);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(expected_status, WEXITSTATUS(status));
}

TEST(RestartHelperExit, success)
{
  check_exit_status("success", 0);
}

TEST(RestartHelperExit, before_failure)
{
  check_exit_status("before_failure", 1);
}

TEST(RestartHelperExit, before_signal)
{
  check_exit_status("before_signal", 1);
}

TEST(RestartHelperExit, after_failure)
{
  check_exit_status("after_failure", 1);
}
} // namespace

int main(int argc, char **argv)
{
  if (argc == 3 && 0 == strcmp(argv[1], "--restart-helper-scenario")) {
    scenario = argv[2];
    argc = 1;
    ObSimpleServerRestartHelper helper(argc, argv, "test_restart_helper",
                                      "RestartHelperBefore", "RestartHelperAfter");
    return helper.run();
  }
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "RestartHelperExit.*";
  return RUN_ALL_TESTS();
}
