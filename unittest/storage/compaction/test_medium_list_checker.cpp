/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/compaction/ob_medium_list_checker.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction/ob_extra_medium_info.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace storage;

namespace unittest
{

class TestMediumListChecker : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    GCONF.cluster_id = INIT_CLUSTER_ID;
  }
  virtual void TearDown()
  {
    allocator_.reset();
  }
  void set_basic_info(ObMediumCompactionInfo &medium_info, const int64_t compat_version)
  {
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info.tenant_id_ = MTL_ID();
    medium_info.data_version_ = 100;
    medium_info.cluster_id_ = INIT_CLUSTER_ID;
    medium_info.medium_compat_version_ = compat_version;
  }
  int construct_array(
      const char *snapshot_list,
      ObMediumListChecker::MediumInfoArray &array,
      const int64_t medium_compat_version = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2,
      const int64_t last_medium_scn_of_first_medium_info = 1,
      const int64_t cluster_id = INIT_CLUSTER_ID);
  int construct_array(
      const char *snapshot_list,
      ObIArray<int64_t> &array);
  static const int64_t INIT_CLUSTER_ID = 1;
  static const int64_t OTHER_CLUSTER_ID = 2;
  static const int64_t ARRAY_SIZE = 10;
private:
  ObMediumCompactionInfo medium_info_array_[ARRAY_SIZE];
  ObSEArray<int64_t, ARRAY_SIZE> array_;
  ObArenaAllocator allocator_;
};

int TestMediumListChecker::construct_array(
    const char *snapshot_list,
    ObIArray<int64_t> &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(snapshot_list)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(snapshot_list));
  } else {
    array.reset();
    std::string copy(snapshot_list);
    char *org = const_cast<char *>(copy.c_str());
    static const char *delim = " ";
    char *s = std::strtok(org, delim);
    if (NULL != s) {
      array.push_back(atoi(s));
      while (NULL != (s= strtok(NULL, delim))) {
        array.push_back(atoi(s));
      }
    }
  }
  return ret;
}

int TestMediumListChecker::construct_array(
    const char *snapshot_list,
    ObMediumListChecker::MediumInfoArray &input_array,
    const int64_t medium_compat_version,
    const int64_t last_medium_scn_of_first_medium_info,
    const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  construct_array(snapshot_list, array_);
  OB_ASSERT(array_.count() <= ARRAY_SIZE);
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); ++i) {
    set_basic_info(medium_info_array_[i], medium_compat_version);
    medium_info_array_[i].medium_snapshot_ = array_.at(i);
    medium_info_array_[i].last_medium_snapshot_ = (i > 0 ? array_.at(i - 1) : last_medium_scn_of_first_medium_info);
    ret = input_array.push_back(&medium_info_array_[i]);
  }
  return ret;
}


TEST_F(TestMediumListChecker, test_validate_medium_info_list)
{
  int ret = OB_SUCCESS;
  int64_t medium_compat_version = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
  for ( ; medium_compat_version <= ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_LATEST; medium_compat_version++) {
    COMMON_LOG(INFO, "Start test for compat version", K(medium_compat_version));
    ObExtraMediumInfo extra_info;
    extra_info.last_medium_scn_ = 100;
    ObSEArray<compaction::ObMediumCompactionInfo*, 10> array;
    
    ASSERT_EQ(OB_SUCCESS, construct_array("300, 400, 500", array, medium_compat_version, 10/*last_medium_scn_of_first_medium_info*/));
    ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

    array.reset();
    ASSERT_EQ(OB_SUCCESS, construct_array("200, 400, 500", array, medium_compat_version, 100/*last_medium_scn_of_first_medium_info*/));
    ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 50/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
    
    ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    extra_info.last_medium_scn_ = 1000;
    ret = ObMediumListChecker::validate_medium_info_list(extra_info, &array, 1000/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    // push item without clear array
    ASSERT_EQ(OB_SUCCESS, construct_array("900", array, medium_compat_version, 700/*last_medium_scn_of_first_medium_info*/));
    ret = ObMediumListChecker::check_continue(array);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  }
}

TEST_F(TestMediumListChecker, test_check_extra_info)
{
  int ret = OB_SUCCESS;
  ObExtraMediumInfo extra_info;

  extra_info.last_medium_scn_ = 0;
  ret = ObMediumListChecker::check_extra_info(extra_info, 50/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  extra_info.last_medium_scn_ = 100;
  ret = ObMediumListChecker::check_extra_info(extra_info, 0/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  extra_info.last_medium_scn_ = 100;
  ret = ObMediumListChecker::check_extra_info(extra_info, 50/*last_major_snapshot*/);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

  extra_info.last_medium_scn_ = 100;
  ret = ObMediumListChecker::check_extra_info(extra_info, 100/*last_major_snapshot*/);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMediumListChecker, test_check_next_schedule_medium)
{
  int ret = OB_SUCCESS;
  int64_t medium_compat_version = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
  for ( ; medium_compat_version <= ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_LATEST; medium_compat_version++) {
    COMMON_LOG(INFO, "Start test for compat version", K(medium_compat_version));
    ObMediumCompactionInfo medium_info;
    set_basic_info(medium_info, medium_compat_version);
    medium_info.medium_snapshot_ = 130;
    medium_info.last_medium_snapshot_ = 100;

    ret = ObMediumListChecker::check_next_schedule_medium(medium_info, 100/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        true/*force_check*/, false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = ObMediumListChecker::check_next_schedule_medium(medium_info, 120/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        true/*force_check*/, false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

    // medium from different cluster
    medium_info.cluster_id_ = OTHER_CLUSTER_ID;
    ret = ObMediumListChecker::check_next_schedule_medium(medium_info, 50/*last_major_snapshot*/, 0/*last_inc_major_snapshot*/,
        true/*force_check*/, false/*allow_predecessor_mismatch*/);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestMediumListChecker, test_same_origin_predecessor_mismatch_remains_strict)
{
  ObExtraMediumInfo extra_info;
  extra_info.last_medium_scn_ = 100;
  ObSEArray<ObMediumCompactionInfo *, 10> array;
  ASSERT_EQ(OB_SUCCESS, construct_array("300 400", array,
      ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2, 200));
  const ObMediumCompactionInfo::ObCompactionType types[] = {
      ObMediumCompactionInfo::MAJOR_COMPACTION, ObMediumCompactionInfo::MEDIUM_COMPACTION};
  for (const auto type : types) {
    SCOPED_TRACE(static_cast<int>(type));
    for (int64_t i = 0; i < array.count(); ++i) {
      array.at(i)->compaction_type_ = type;
    }
    array.at(0)->last_medium_snapshot_ = 200;
    // The exception does not relax same-origin Major or Medium predecessor checks.
    ASSERT_EQ(OB_ERR_UNEXPECTED,
        ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, false));
    ASSERT_EQ(OB_ERR_UNEXPECTED,
        ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, true));

    // Repair the predecessor so subsequent failures isolate extra_info and internal continuity checks.
    array.at(0)->last_medium_snapshot_ = 100;
    ASSERT_EQ(OB_SUCCESS,
        ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, false));
    ASSERT_EQ(OB_SUCCESS,
        ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, true));
    extra_info.last_medium_scn_ = 200;
    ASSERT_EQ(OB_ERR_UNEXPECTED,
        ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, true));
    extra_info.last_medium_scn_ = 100;
    array.at(1)->last_medium_snapshot_ = 250;
    ASSERT_EQ(OB_ERR_UNEXPECTED,
        ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, true));
    array.at(1)->last_medium_snapshot_ = 300;
  }
}

// Install a deterministic freeze cache without starting tenant background services.
class TestMediumListCheckerWithFreezeInfo : public TestMediumListChecker
{
public:
  TestMediumListCheckerWithFreezeInfo() : tenant_(1006), previous_tenant_(nullptr) {}
  void SetUp() override
  {
    TestMediumListChecker::SetUp();
    previous_tenant_ = share::ObTenantEnv::get_tenant();
    freeze_mgr_.init_for_test();
    freeze_mgr_.freeze_info_mgr_.is_inited_ = true;
    tenant_.set(&freeze_mgr_);
    share::ObTenantEnv::set_tenant(&tenant_);
    ASSERT_EQ(&freeze_mgr_, MTL(ObTenantFreezeInfoMgr *));
  }
  void TearDown() override
  {
    share::ObTenantEnv::set_tenant(previous_tenant_);
    TestMediumListChecker::TearDown();
  }
  int add_freeze(const int64_t snapshot)
  {
    share::SCN scn;
    scn.convert_for_tx(snapshot);
    return freeze_mgr_.freeze_info_mgr_.freeze_info_.frozen_statuses_.push_back(
        share::ObFreezeInfo(scn, 1, DATA_VERSION_4_3_3_0));
  }
protected:
  share::ObTenantBase tenant_;
  share::ObTenantBase *previous_tenant_;
  ObTenantFreezeInfoMgr freeze_mgr_;
};

TEST_F(TestMediumListCheckerWithFreezeInfo, test_cross_origin_major_predecessor_mismatch)
{
  ASSERT_EQ(OB_SUCCESS, add_freeze(100));
  ASSERT_EQ(OB_SUCCESS, add_freeze(200));
  ASSERT_EQ(OB_SUCCESS, add_freeze(300));
  ObExtraMediumInfo extra_info;
  extra_info.last_medium_scn_ = 100;
  for (int64_t compat = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
       compat <= ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_LATEST; ++compat) {
    for (const bool different_tenant : {true, false}) {
      SCOPED_TRACE(compat);
      SCOPED_TRACE(different_tenant);
      ObMediumCompactionInfo medium_info;
      set_basic_info(medium_info, compat);
      medium_info.compaction_type_ = ObMediumCompactionInfo::MAJOR_COMPACTION;
      medium_info.medium_snapshot_ = 300;
      medium_info.last_medium_snapshot_ = 200;
      medium_info.tenant_id_ = different_tenant ? 1002 : MTL_ID();
      medium_info.cluster_id_ = different_tenant ? INIT_CLUSTER_ID : OTHER_CLUSTER_ID;
      ASSERT_FALSE(medium_info.from_cur_cluster());
      ASSERT_FALSE(medium_info.is_skip_tenant_major_);
      ObSEArray<share::ObFreezeInfo, 4> freezes;
      ASSERT_EQ(OB_SUCCESS, freeze_mgr_.get_freeze_info_behind_major_snapshot(100, false, freezes));
      ASSERT_FALSE(freezes.empty());
      ASSERT_EQ(200, freezes.at(0).frozen_scn_.get_val_for_tx());
      ObSEArray<ObMediumCompactionInfo *, 1> array;
      ASSERT_EQ(OB_SUCCESS, array.push_back(&medium_info));

      // The same input must change its result only when the exception is enabled.
      // Exercise validate -> inner_check -> check_next so dropped arguments also fail this test.
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, false));
      ASSERT_EQ(OB_SUCCESS,
          ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, true));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          ObMediumListChecker::check_next_schedule_medium(medium_info, 100, 0, true, false));
      ASSERT_EQ(OB_SUCCESS,
          ObMediumListChecker::check_next_schedule_medium(medium_info, 100, 0, true, true));

      // The exception must not bypass an unrelated extra_info mismatch.
      extra_info.last_medium_scn_ = 200;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          ObMediumListChecker::validate_medium_info_list(extra_info, &array, 100, 0, true));
      extra_info.last_medium_scn_ = 100;
    }
  }
}

TEST_F(TestMediumListCheckerWithFreezeInfo, test_cross_origin_freeze_lookup_boundaries)
{
  ObMediumCompactionInfo medium_info;
  set_basic_info(medium_info, ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2);
  medium_info.compaction_type_ = ObMediumCompactionInfo::MAJOR_COMPACTION;
  medium_info.medium_snapshot_ = 300;
  medium_info.last_medium_snapshot_ = 200;
  medium_info.tenant_id_ = 1002;
  ASSERT_FALSE(medium_info.from_cur_cluster());
  for (const bool allow : {false, true}) {
    SCOPED_TRACE(allow);
    // An empty cache preserves the existing deferred-check / force-check behavior.
    ASSERT_EQ(OB_SUCCESS,
        ObMediumListChecker::check_next_schedule_medium(medium_info, 100, 0, false, allow));
    ASSERT_EQ(OB_EAGAIN,
        ObMediumListChecker::check_next_schedule_medium(medium_info, 100, 0, true, allow));
    freeze_mgr_.freeze_info_mgr_.is_inited_ = false;
    EXPECT_EQ(OB_NOT_INIT,
        ObMediumListChecker::check_next_schedule_medium(medium_info, 100, 0, true, allow));
    freeze_mgr_.freeze_info_mgr_.is_inited_ = true;
  }
  ASSERT_EQ(OB_SUCCESS, add_freeze(300));
  for (const bool allow : {false, true}) {
    // No missing freeze predecessor: strict validation must already succeed.
    ASSERT_EQ(OB_SUCCESS,
        ObMediumListChecker::check_next_schedule_medium(medium_info, 100, 0, true, allow));
  }
}

TEST_F(TestMediumListChecker, test_filter_finish_medium_info)
{
  int ret = OB_SUCCESS;
  int64_t medium_compat_version = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
  for ( ; medium_compat_version <= ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_LATEST; medium_compat_version++) {
    COMMON_LOG(INFO, "Start test for compat version", K(medium_compat_version));
    ObSEArray<compaction::ObMediumCompactionInfo*, 10> array;
    ASSERT_EQ(OB_SUCCESS, construct_array("300, 400, 500", array, medium_compat_version));
    int64_t next_medium_info_idx = 0;
    ret = ObMediumListChecker::filter_finish_medium_info(array, 100, 0/*last_inc_major_snapshot*/, next_medium_info_idx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(next_medium_info_idx, 0);

    ret = ObMediumListChecker::filter_finish_medium_info(array, 500, 0/*last_inc_major_snapshot*/, next_medium_info_idx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(next_medium_info_idx, 3);

    ret = ObMediumListChecker::filter_finish_medium_info(array, 400, 0/*last_inc_major_snapshot*/, next_medium_info_idx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(next_medium_info_idx, 2);
  }
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_list_checker.log*");
  OB_LOGGER.set_file_name("test_medium_list_checker.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
