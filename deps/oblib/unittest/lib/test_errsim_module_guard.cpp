/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <stdexcept>
#include <type_traits>
#include <gtest/gtest.h>
#include "lib/worker.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

static_assert(!std::is_copy_constructible<ErrsimModuleGuard>::value, "guards must not be copied");
static_assert(!std::is_copy_assignable<ErrsimModuleGuard>::value, "guards must not be copied");
static_assert(!std::is_move_constructible<ErrsimModuleGuard>::value, "guards must stay in their scope");
static_assert(!std::is_move_assignable<ErrsimModuleGuard>::value, "guards must stay in their scope");

#ifdef ERRSIM
class TestErrsimModuleGuard : public ::testing::Test
{
public:
  void SetUp() override
  {
    saved_type_ = Worker::get_module_type();
    Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_NONE));
  }

  void TearDown() override
  {
    Worker::set_module_type(saved_type_);
  }

private:
  ObErrsimModuleType saved_type_;
};

TEST_F(TestErrsimModuleGuard, supports_every_module_and_restores_previous_module)
{
  for (int old = ObErrsimModuleType::ERRSIM_MODULE_NONE; old < ObErrsimModuleType::ERRSIM_MODULE_MAX; ++old) {
    const auto previous = static_cast<ObErrsimModuleType::TYPE>(old);
    Worker::set_module_type(ObErrsimModuleType(previous));
    for (int current = ObErrsimModuleType::ERRSIM_MODULE_NONE; current < ObErrsimModuleType::ERRSIM_MODULE_MAX; ++current) {
      const auto type = static_cast<ObErrsimModuleType::TYPE>(current);
      {
        ErrsimModuleGuard guard(type);
        EXPECT_EQ(type, Worker::get_module_type().type_);
      }
      EXPECT_EQ(previous, Worker::get_module_type().type_);
    }
  }
}

TEST_F(TestErrsimModuleGuard, nested_modules_and_temporary_disable)
{
  {
    ErrsimModuleGuard migration(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION);
    {
      ErrsimModuleGuard transfer(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
      {
        ErrsimModuleGuard disabled(ObErrsimModuleType::ERRSIM_MODULE_NONE);
        EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, Worker::get_module_type().type_);
      }
      EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, Worker::get_module_type().type_);
    }
    EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, Worker::get_module_type().type_);
  }
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, Worker::get_module_type().type_);
}

TEST_F(TestErrsimModuleGuard, nested_same_module_preserves_service_context_across_iterations)
{
  ErrsimModuleGuard service(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
  for (int i = 0; i < 3; ++i) {
    {
      ErrsimModuleGuard callback(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
      EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, Worker::get_module_type().type_);
    }
    EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, Worker::get_module_type().type_);
  }
}

TEST_F(TestErrsimModuleGuard, early_return_restores_caller)
{
  ErrsimModuleGuard caller(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION);
  const auto fail = []() {
    ErrsimModuleGuard transfer(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
    EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, Worker::get_module_type().type_);
    return OB_INVALID_ARGUMENT;
  };
  EXPECT_EQ(OB_INVALID_ARGUMENT, fail());
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, Worker::get_module_type().type_);
}

TEST_F(TestErrsimModuleGuard, exception_restores_caller)
{
  ErrsimModuleGuard caller(ObErrsimModuleType::ERRSIM_MODULE_ALL);
  const auto fail = []() {
    ErrsimModuleGuard transfer(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
    throw std::runtime_error("errsim scope failed");
  };
  EXPECT_THROW(fail(), std::runtime_error);
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_ALL, Worker::get_module_type().type_);
}
#else
TEST(TestErrsimModuleGuard, available_without_errsim)
{
  // The same call sites compile without ERRSIM and carry no saved context.
  EXPECT_TRUE(std::is_empty<ErrsimModuleGuard>::value);
  ErrsimModuleGuard migration(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION);
  ErrsimModuleGuard transfer(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
  ErrsimModuleGuard disabled(ObErrsimModuleType::ERRSIM_MODULE_NONE);
}
#endif

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}
