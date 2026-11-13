/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include "common/errsim_module/ob_errsim_module_interface.h"
#include "lib/alloc/alloc_func.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace common
{
namespace
{
ObTenantErrsimEvent recorded;
int record_calls = 0;
int record_error = OB_SUCCESS;
bool inject = false;
bool nested_injection = false;
ObErrsimModuleType::TYPE recording_module = ObErrsimModuleType::ERRSIM_MODULE_MAX;
}

// Replace the oblib weak sink with an observer-independent recording fixture.
int add_tenant_errsim_event(uint64_t tenant_id, const ObTenantErrsimEvent &event)
{
  UNUSED(tenant_id);
  ++record_calls;
  recorded = event;
  recording_module = lib::Worker::get_module_type().type_;
  // A nested allocation must not inject again while the recorder is active.
  nested_injection = lib::errsim_alloc(ObMemAttr(OB_SYS_TENANT_ID, "ErrsimTest"));
  return record_error;
}

bool is_errsim_module(uint64_t tenant_id, const ObErrsimModuleType::TYPE &type)
{
  UNUSED(tenant_id);
  return inject && type == ObErrsimModuleType::ERRSIM_MODULE_TRANSFER;
}

class TestErrsimEventRecord : public ::testing::Test
{
public:
  void SetUp() override
  {
    previous_ = lib::Worker::get_module_type();
    lib::Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_NONE));
    recorded.reset(); record_calls = 0; record_error = OB_SUCCESS; inject = false; nested_injection = false;
  }
  void TearDown() override { lib::Worker::set_module_type(previous_); }
private:
  ObErrsimModuleType previous_;
};

TEST_F(TestErrsimEventRecord, propagates_recording_error_and_restores_context)
{
  lib::ErrsimModuleGuard caller(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION);
  record_error = OB_ALLOCATE_MEMORY_FAILED;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
      record_tenant_errsim_event(OB_SYS_TENANT_ID, ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, OB_IO_ERROR));
  ASSERT_EQ(1, record_calls);
  EXPECT_EQ(OB_IO_ERROR, recorded.errsim_error_);
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, recorded.type_.type_);
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, recording_module);
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, lib::Worker::get_module_type().type_);
  EXPECT_FALSE(recorded.backtrace_.is_empty());
}

TEST_F(TestErrsimEventRecord, allocation_hit_records_once_without_recursing)
{
  lib::ErrsimModuleGuard caller(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
  inject = true;
  record_error = OB_NOT_INIT;
  EXPECT_TRUE(lib::errsim_alloc(ObMemAttr(OB_SYS_TENANT_ID, "ErrsimTest")));
  EXPECT_EQ(1, record_calls);
  EXPECT_FALSE(nested_injection);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, recorded.errsim_error_);
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, recorded.type_.type_);
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, lib::Worker::get_module_type().type_);
}

TEST_F(TestErrsimEventRecord, no_hit_has_no_record)
{
  lib::ErrsimModuleGuard caller(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER);
  EXPECT_FALSE(lib::errsim_alloc(ObMemAttr(OB_SYS_TENANT_ID, "ErrsimTest")));
  EXPECT_EQ(0, record_calls);
}

TEST_F(TestErrsimEventRecord, rejects_non_injection_events)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      record_tenant_errsim_event(OB_SYS_TENANT_ID, ObErrsimModuleType::ERRSIM_MODULE_NONE, OB_IO_ERROR));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      record_tenant_errsim_event(OB_SYS_TENANT_ID, ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, OB_SUCCESS));
  EXPECT_EQ(0, record_calls);
}
} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}
