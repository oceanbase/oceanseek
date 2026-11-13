/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cmath>
#include <limits>
#include "share/config/ob_config.h"
#include "share/config/ob_config_helper.h"
#include <gtest/gtest.h>
#include "share/errsim_module/ob_tenant_errsim_module_mgr.h"

namespace oceanbase
{
namespace share
{
using namespace common;

class TestTenantErrsimModuleMgr : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, mgr_.init(OB_SYS_TENANT_ID));
  }

  void TearDown() override
  {
    mgr_.destroy();
  }

  int configure(const char *module, const double percentage)
  {
    int ret = OB_SUCCESS;
    ObTenantErrsimModuleMgr::ModuleArray modules;
    ObTenantErrsimModuleMgr::ErrsimModuleString name;
    if (OB_FAIL(name.assign(module))) {
    } else if (OB_FAIL(modules.push_back(name))) {
    } else {
      ret = mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, ++config_version_, modules, percentage);
    }
    return ret;
  }

  int64_t count_hits(const ObErrsimModuleType::TYPE type, const int64_t count)
  {
    int64_t hits = 0;
    for (int64_t i = 0; i < count; ++i) {
      hits += mgr_.is_errsim_module(type) ? 1 : 0;
    }
    return hits;
  }

protected:
  ObTenantErrsimModuleMgr mgr_;
  int64_t config_version_ = 0;
};

TEST_F(TestTenantErrsimModuleMgr, zero_and_full_percentage)
{
  const int64_t count = 10000;
  for (const char *module : {"TRANSFER", "ALL"}) {
    ASSERT_EQ(OB_SUCCESS, configure(module, 100.0));
    ASSERT_EQ(count, count_hits(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, count));
    // Setting only the percentage to zero must disable an already enabled module.
    ASSERT_EQ(OB_SUCCESS, configure(module, 0.0));
    ASSERT_EQ(0, count_hits(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, count));
  }
}

TEST_F(TestTenantErrsimModuleMgr, fractional_percentage)
{
  const int64_t count = 1000000;
  for (const double percentage : {0.1, 0.5, 1.0, 12.5}) {
    SCOPED_TRACE(percentage);
    ASSERT_EQ(OB_SUCCESS, configure("TRANSFER", percentage));
    const int64_t hits = count_hits(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, count);
    const double probability = percentage / 100.0;
    const double expected = count * probability;
    // Eight standard deviations avoid flakiness while detecting truncation and inclusive bounds.
    const double tolerance = 8.0 * std::sqrt(count * probability * (1.0 - probability));
    ASSERT_NEAR(expected, static_cast<double>(hits), tolerance);
  }
}

TEST_F(TestTenantErrsimModuleMgr, module_filter)
{
  const int64_t count = 1000;
  ASSERT_EQ(OB_SUCCESS, configure("TRANSFER", 100.0));
  ASSERT_EQ(0, count_hits(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, count));
  ASSERT_EQ(OB_SUCCESS, configure("ALL", 100.0));
  ASSERT_EQ(count, count_hits(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, count));
  ObTenantErrsimModuleMgr::ModuleArray modules;
  ASSERT_EQ(OB_SUCCESS, mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, ++config_version_, modules, 100.0));
  ASSERT_EQ(0, count_hits(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, count));
}

TEST_F(TestTenantErrsimModuleMgr, case_insensitive_module_names)
{
  for (const char *name : {"transfer", "Transfer", "TRANSFER"}) {
    ASSERT_EQ(OB_SUCCESS, configure(name, 100));
    EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));
  }
  ASSERT_EQ(OB_SUCCESS, configure("aLl", 100));
  EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));
  EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_MAX, ObErrsimModuleTypeHelper::get_type(nullptr));
}

TEST_F(TestTenantErrsimModuleMgr, ss_gc_module_filter)
{
  EXPECT_STREQ("SS_GC", ObErrsimModuleTypeHelper::get_str(ObErrsimModuleType::ERRSIM_MODULE_SS_GC));
  for (const char *name : {"SS_GC", "ss_gc", "Ss_Gc"}) {
    ASSERT_EQ(OB_SUCCESS, configure(name, 100));
    EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_SS_GC));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_NONE));
  }
  ASSERT_EQ(OB_SUCCESS, configure("TRANSFER", 100));
  EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_SS_GC));
  ASSERT_EQ(OB_SUCCESS, configure("ALL", 100));
  EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_SS_GC));
  ASSERT_EQ(OB_SUCCESS, configure("SS_GC", 0));
  EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_SS_GC));
}

TEST_F(TestTenantErrsimModuleMgr, outtable_lob_module_filter)
{
  const auto type = ObErrsimModuleType::ERRSIM_MODULE_OUTTABLE_LOB;
  EXPECT_STREQ("OUTTABLE_LOB", ObErrsimModuleTypeHelper::get_str(type));
  for (const char *name : {"OUTTABLE_LOB", "outtable_lob", "OutTable_Lob"}) {
    ASSERT_EQ(OB_SUCCESS, configure(name, 100));
    EXPECT_TRUE(mgr_.is_errsim_module(type));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_SS_GC));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_NONE));
  }
  ASSERT_EQ(OB_SUCCESS, configure("SS_GC", 100));
  EXPECT_FALSE(mgr_.is_errsim_module(type));
  ASSERT_EQ(OB_SUCCESS, configure("ALL", 100));
  EXPECT_TRUE(mgr_.is_errsim_module(type));
  ASSERT_EQ(OB_SUCCESS, configure("OUTTABLE_LOB", 0));
  EXPECT_FALSE(mgr_.is_errsim_module(type));
}

TEST_F(TestTenantErrsimModuleMgr, failed_build_preserves_snapshot_and_allows_same_version_retry)
{
  for (const char *old_module : {"TRANSFER", "ALL"}) {
    ASSERT_EQ(OB_SUCCESS, configure(old_module, 100));
    ObTenantErrsimModuleMgr::ModuleArray modules;
    ObTenantErrsimModuleMgr::ErrsimModuleString name;
    for (const char *module : {"migration", "unknown"}) {
      ASSERT_EQ(OB_SUCCESS, name.assign(module));
      ASSERT_EQ(OB_SUCCESS, modules.push_back(name));
    }
    const int64_t version = ++config_version_;
    ASSERT_EQ(OB_INVALID_CONFIG, mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, version, modules, 0));
    EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
    EXPECT_EQ(0 == strcmp(old_module, "ALL"), mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));

    modules.pop_back();
    ASSERT_EQ(OB_SUCCESS, mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, version, modules, 100));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
    EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));
  }
}

TEST_F(TestTenantErrsimModuleMgr, ignores_older_and_duplicate_versions)
{
  ASSERT_EQ(OB_SUCCESS, configure("TRANSFER", 100));
  ObTenantErrsimModuleMgr::ModuleArray empty;
  ASSERT_EQ(OB_SUCCESS, mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, config_version_, empty, 0));
  ASSERT_EQ(OB_SUCCESS, mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, config_version_ - 1, empty, 0));
  EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
  ASSERT_EQ(OB_SUCCESS, mgr_.build_tenant_moulde(OB_SYS_TENANT_ID, ++config_version_, empty, 0));
  EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
}

TEST_F(TestTenantErrsimModuleMgr, invalid_percentage_preserves_snapshot)
{
  ASSERT_EQ(OB_SUCCESS, configure("TRANSFER", 100));
  for (const double percentage : {-1.0, 101.0, std::numeric_limits<double>::quiet_NaN()}) {
    ASSERT_EQ(OB_INVALID_CONFIG, configure("ALL", percentage));
    EXPECT_TRUE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
    EXPECT_FALSE(mgr_.is_errsim_module(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));
  }
}

TEST(TestErrsimModuleConfig, validates_complete_list)
{
  class ModuleList : public ObConfigStrListItem
  {
  public:
    explicit ModuleList(const char *value)
        : ObConfigStrListItem(nullptr, Scope::TENANT, "errsim_module_types", value, "") {}
    const char *value_default_ptr() const override { return ""; }
  };
  ObConfigErrsimModuleTypesChecker checker;
  for (const char *value : {"", "transfer", "TRANSFER;Migration", "all", "none",
                           "transfer;transfer", "SS_GC", "ss_gc;TRANSFER", "OUTTABLE_LOB",
                           "outtable_lob;SS_GC", "TRANSFER;OutTable_Lob"}) {
    ModuleList item(value);
    EXPECT_TRUE(checker.check(item)) << value;
  }
  for (const char *value : {"unknown", "migration;unknown", ";transfer", "transfer;", "transfer;;all",
                          "transfer,migration", "transferxxxxxxxxxxxxxxxx", "migration;transferxxxxxxxxxxxxxxxx"}) {
    ModuleList item(value);
    EXPECT_FALSE(checker.check(item)) << value;
  }
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}
