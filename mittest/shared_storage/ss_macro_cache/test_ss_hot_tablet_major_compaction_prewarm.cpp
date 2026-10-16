/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#include <gtest/gtest.h>
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{


} // namespace sslog
} // namespace oceanbase


using namespace oceanbase::transaction;
using namespace oceanbase::share;
using namespace oceanbase::storage;

const char *get_per_file_test_name() 
{
  return "test_hot_tablet_major_compaction_prewarm_";
}

namespace oceanbase
{
namespace unittest
{

class ObHotTabletMajorCompactionPrewarmTest : public ObStorageCachePolicyPrewarmerTest
{
public:
  ObHotTabletMajorCompactionPrewarmTest() : ObStorageCachePolicyPrewarmerTest() {}
  virtual ~ObHotTabletMajorCompactionPrewarmTest() = default;
};

TEST_F(ObHotTabletMajorCompactionPrewarmTest, test_compaction)
{
  ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_compaction");
  OK(exe_sql("create table test_compaction (a varchar(1000)) storage_cache_policy (global = 'hot')"));
  OK(exe_sql("insert into test_compaction select randstr(1000, random(10)) from table(generator(16000))"));
  set_ls_and_tablet_id_for_run_ctx("test_compaction");
  // wait policy refresh finish before major compaction, to ensure the tablet is HOT.
  OK(TestCompactionUtil::wait_policy_refresh_finish(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), "HOT"/*storage_cache_policy*/));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  check_macro_cache_exist();
  FLOG_INFO("[TEST] finish test_compaction");
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
