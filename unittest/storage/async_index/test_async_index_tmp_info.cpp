/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/async_index/ob_async_index_service.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/px/ob_px_sub_coord.h"
#include "observer/ob_server_struct.h"
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace storage;
using rootserver::ObDDLSliceInfo;

// Exercise the real registry APIs on two isolated tenant-local components.
// This does not start LS services, schedulers or an observer.
struct ObTmpInfoNode
{
  ObTmpInfoNode() : tenant_(OB_SYS_TENANT_ID) {}

  void enter() { ObTenantEnv::set_tenant(&tenant_); }
  void init()
  {
    tenant_.set<ObAsyncIndexService *>(&service_);
    enter();
    ASSERT_EQ(OB_SUCCESS, service_.tmp_info_allocator_.init(nullptr,
        OB_MALLOC_MIDDLE_BLOCK_SIZE, lib::ObMemAttr(OB_SYS_TENANT_ID, "TmpInfoTest")));
    ASSERT_EQ(OB_SUCCESS, service_.tmp_info_map_.create(
        64, "TmpInfoTest", "TmpInfoTest", OB_SYS_TENANT_ID));
    service_.tenant_id_ = OB_SYS_TENANT_ID;
    service_.is_inited_ = true;
  }
  void cleanup()
  {
    enter();
    for (int64_t id = 101; id <= 103; ++id) {
      EXPECT_EQ(OB_SUCCESS, service_.release_refresh_versions(id));
    }
    service_.tmp_info_map_.destroy();
    service_.tmp_info_allocator_.reset();
    service_.is_inited_ = false;
  }

  ObTenantBase tenant_;
  ObAsyncIndexService service_;
};

class TestAsyncIndexTmpInfo : public testing::Test
{
public:
  void SetUp() override
  {
    a_.init();
    b_.init();
  }
  void TearDown() override
  {
    a_.cleanup();
    b_.cleanup();
    ObTenantEnv::set_tenant(nullptr);
  }
  static SCN scn(const int64_t value)
  {
    SCN result;
    EXPECT_EQ(OB_SUCCESS, result.convert_for_tx(value));
    return result;
  }
  void register_context(ObTmpInfoNode &node, const int64_t id = 101,
      const int64_t start = 100, const int64_t target = 200, const int64_t build_id = 1001)
  {
    node.enter();
    ASSERT_EQ(OB_SUCCESS, node.service_.register_refresh_versions(
        id, scn(start), target, 300, build_id));
  }
  void make_slice(ObDDLSliceInfo &slice)
  {
    sql::ObPxTabletRange range;
    range.tablet_id_ = 200001;
    ASSERT_EQ(OB_SUCCESS, slice.part_ranges_.push_back(range));
    ASSERT_TRUE(slice.is_valid());
  }

  ObTmpInfoNode a_;
  ObTmpInfoNode b_;
};

TEST_F(TestAsyncIndexTmpInfo, owner_reads_registered_versions_and_build_id)
{
  register_context(a_);
  SCN start;
  int64_t version = 0;
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_start_refresh_scn(101, start));
  EXPECT_EQ(scn(100), start);
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_snapshot_version(101, version));
  EXPECT_EQ(200, version);
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_schema_version(101, version));
  EXPECT_EQ(300, version);
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_ddl_build_id(101, version));
  EXPECT_EQ(1001, version);
}

TEST_F(TestAsyncIndexTmpInfo, remote_version_miss_is_retryable_and_does_not_create_context)
{
  register_context(a_);
  b_.enter();
  SCN start;
  int64_t version = 0;
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.get_inner_sql_start_refresh_scn(101, start));
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.get_inner_sql_snapshot_version(101, version));
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.get_inner_sql_schema_version(101, version));
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.get_inner_sql_ddl_build_id(101, version));
  EXPECT_EQ(0, b_.service_.tmp_info_map_.size());
}

TEST_F(TestAsyncIndexTmpInfo, missing_slice_producer_and_consumer_are_retryable)
{
  register_context(a_);
  ObDDLSliceInfo input;
  ObDDLSliceInfo output;
  make_slice(input);
  ASSERT_EQ(OB_SUCCESS, a_.service_.set_inner_sql_slice_info(101, input));
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_slice_info(101, allocator, output));
  ASSERT_EQ(1, output.part_ranges_.count());
  EXPECT_EQ(200001, output.part_ranges_.at(0).tablet_id_);
  b_.enter();
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.set_inner_sql_slice_info(101, input));
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.get_inner_sql_slice_info(101, allocator, output));
  EXPECT_EQ(0, b_.service_.tmp_info_map_.size());
}

TEST_F(TestAsyncIndexTmpInfo, release_is_idempotent_and_reregister_has_fresh_versions)
{
  register_context(a_);
  EXPECT_EQ(OB_SUCCESS, a_.service_.release_refresh_versions(101));
  EXPECT_EQ(OB_SUCCESS, a_.service_.release_refresh_versions(101));
  int64_t version = 0;
  EXPECT_EQ(OB_NEED_RETRY, a_.service_.get_inner_sql_ddl_build_id(101, version));
  register_context(a_, 101, 200, 400, 1002);
  SCN start;
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_start_refresh_scn(101, start));
  EXPECT_EQ(scn(200), start);
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_snapshot_version(101, version));
  EXPECT_EQ(400, version);
  ASSERT_EQ(OB_SUCCESS, a_.service_.get_inner_sql_ddl_build_id(101, version));
  EXPECT_EQ(1002, version);
}

TEST_F(TestAsyncIndexTmpInfo, new_owner_registers_new_attempt_independently)
{
  register_context(a_);
  register_context(b_, 102, 200, 400, 1002);
  int64_t version = 0;
  EXPECT_EQ(OB_NEED_RETRY, b_.service_.get_inner_sql_snapshot_version(101, version));
  a_.enter();
  EXPECT_EQ(OB_SUCCESS, a_.service_.release_refresh_versions(101));
  EXPECT_EQ(OB_NEED_RETRY, a_.service_.get_inner_sql_snapshot_version(102, version));
  b_.enter();
  ASSERT_EQ(OB_SUCCESS, b_.service_.get_inner_sql_snapshot_version(102, version));
  EXPECT_EQ(400, version);
  ASSERT_EQ(OB_SUCCESS, b_.service_.get_inner_sql_ddl_build_id(102, version));
  EXPECT_EQ(1002, version);
}

TEST_F(TestAsyncIndexTmpInfo, missing_context_retry_survives_px_ddl_error_rewrite)
{
  register_context(a_);
  b_.enter();
  sql::ObPxRpcInitSqcArgs args;
  sql::ObPxSubCoord sqc(GCTX, args);
  SCN start;
  int ret = b_.service_.get_inner_sql_start_refresh_scn(101, start);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  sqc.ddl_rewrite_ret_code(ret);
  EXPECT_EQ(OB_NEED_RETRY, ret);

  ObDDLSliceInfo slice;
  make_slice(slice);
  ret = b_.service_.set_inner_sql_slice_info(101, slice);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  sqc.ddl_rewrite_ret_code(ret);
  EXPECT_EQ(OB_NEED_RETRY, ret);
  ObArenaAllocator allocator;
  ret = b_.service_.get_inner_sql_slice_info(101, allocator, slice);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  sqc.ddl_rewrite_ret_code(ret);
  EXPECT_EQ(OB_NEED_RETRY, ret);

  // The shared PX protection remains unchanged for unrelated EAGAIN failures.
  ret = OB_EAGAIN;
  sqc.ddl_rewrite_ret_code(ret);
  EXPECT_EQ(OB_STATE_NOT_MATCH, ret);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name(
      "test_async_index_tmp_info.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
