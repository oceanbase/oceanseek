/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_log_brief.h"
#include "share/config/ob_server_config.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "src/storage/compaction/ob_medium_list_checker.h"
#include "src/storage/multi_data_source/ob_abort_transfer_in_mds_ctx.h"
#include "src/storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h"
#include "src/storage/multi_data_source/ob_start_transfer_in_mds_ctx.h"
#include "storage/high_availability/ob_transfer_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::unittest;
using namespace oceanbase::compaction;
using namespace oceanbase::storage::mds;

#define USING_LOG_PREFIX STORAGE


namespace oceanbase
{
namespace unittest
{
class TestTransferMdsCtx : public ::testing::Test
{
public:
  TestTransferMdsCtx() = default;
  virtual ~TestTransferMdsCtx() = default;
};

TEST_F(TestTransferMdsCtx, start_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObStartTransferInMdsCtx start_ctx;
  start_ctx.version_ = ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = start_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStartTransferInMdsCtx de_start_ctx1;
  pos = 0;
  ret = de_start_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1, de_start_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStartTransferInMdsCtx de_start_ctx2;
  pos = 0;
  ret = de_start_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1, de_start_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObStartTransferInMdsCtx start_ctx2;
  ret = start_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStartTransferInMdsCtx de_start_ctx3;
  pos = 0;
  ret = de_start_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3, de_start_ctx3.version_);
}


TEST_F(TestTransferMdsCtx, finish_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObFinishTransferInMdsCtx finish_ctx;
  finish_ctx.version_ = ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = finish_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObFinishTransferInMdsCtx de_finish_ctx1;
  pos = 0;
  ret = de_finish_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1, de_finish_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFinishTransferInMdsCtx de_finish_ctx2;
  pos = 0;
  ret = de_finish_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1, de_finish_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObFinishTransferInMdsCtx finish_ctx2;
  ret = finish_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObFinishTransferInMdsCtx de_finish_ctx3;
  pos = 0;
  ret = de_finish_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V2, de_finish_ctx3.version_);
}

TEST_F(TestTransferMdsCtx, abort_transfer_mds_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 4096;
  int64_t pos = 0;
  char buf[buf_len] = {0};

  ObAbortTransferInMdsCtx abort_ctx;
  abort_ctx.version_ = ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1;
  ret = abort_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObAbortTransferInMdsCtx de_abort_ctx1;
  pos = 0;
  ret = de_abort_ctx1.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1, de_abort_ctx1.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  MdsCtx mds_ctx;
  ret = mds_ctx.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAbortTransferInMdsCtx de_abort_ctx2;
  pos = 0;
  ret = de_abort_ctx2.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V1, de_abort_ctx2.version_);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObAbortTransferInMdsCtx abort_ctx2;
  ret = abort_ctx2.serialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAbortTransferInMdsCtx de_abort_ctx3;
  pos = 0;
  ret = de_abort_ctx3.deserialize(buf, buf_len, pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObAbortTransferInMdsCtxVersion::ABORT_TRANSFER_IN_MDS_CTX_VERSION_V2, de_abort_ctx3.version_);
}

TEST_F(TestTransferMdsCtx, wide_tablet_log_summary)
{
  storage::ObMigrationTabletParam param;
  param.ls_id_ = ObLSID(1003);
  param.tablet_id_ = ObTabletID(200001);
  param.data_tablet_id_ = ObTabletID(200001);
  param.transfer_info_.ls_id_ = ObLSID(1001);
  param.transfer_info_.transfer_seq_ = 42;
  ASSERT_EQ(OB_SUCCESS, param.transfer_info_.transfer_start_scn_.convert_for_tx(1234567));
  param.transfer_src_tablet_status_.tablet_status_ = storage::ObTabletStatus::TRANSFER_OUT;
  param.snapshot_version_ = 123456;
  param.multi_version_start_ = 123000;
  ASSERT_EQ(OB_SUCCESS, param.clog_checkpoint_scn_.convert_for_tx(1234567));
  param.storage_schema_.allocator_ = &param.allocator_;
  param.storage_schema_.schema_version_ = 1024;
  param.storage_schema_.column_cnt_ = 4097;
  param.storage_schema_.column_array_.set_allocator(&param.allocator_);
  ASSERT_EQ(OB_SUCCESS, param.storage_schema_.column_array_.prepare_allocate(4097));

  char summary_buf[2048] = {0};
  const int64_t len = brief(param).to_string(summary_buf, sizeof(summary_buf));
  ASSERT_LT(len, sizeof(summary_buf) - 1);
  EXPECT_NE(nullptr, strstr(summary_buf, "tablet_id:{id:200001}"));
  EXPECT_NE(nullptr, strstr(summary_buf, "transfer_seq:42"));
  EXPECT_NE(nullptr, strstr(summary_buf, "transfer_start_scn"));
  EXPECT_NE(nullptr, strstr(summary_buf, "param_tablet_status"));
  EXPECT_NE(nullptr, strstr(summary_buf, "clog_checkpoint_scn"));
  EXPECT_NE(nullptr, strstr(summary_buf, "snapshot_version:123456"));
  EXPECT_NE(nullptr, strstr(summary_buf, "multi_version_start:123000"));
  EXPECT_NE(nullptr, strstr(summary_buf, "schema_version:1024"));
  EXPECT_NE(nullptr, strstr(summary_buf, "column_cnt:4097"));
  EXPECT_NE(nullptr, strstr(summary_buf, "column_group_cnt:0"));
  EXPECT_EQ(nullptr, strstr(summary_buf, "column_array"));

  char pointer_buf[2048] = {0};
  storage::ObMigrationTabletParam *param_ptr = &param;
  const storage::ObMigrationTabletParam *const_param_ptr = &param;
  EXPECT_EQ(len, brief(param_ptr).to_string(pointer_buf, sizeof(pointer_buf)));
  EXPECT_STREQ(summary_buf, pointer_buf);
  EXPECT_EQ(len, brief(const_param_ptr).to_string(pointer_buf, sizeof(pointer_buf)));
  EXPECT_STREQ(summary_buf, pointer_buf);
  param_ptr = nullptr;
  EXPECT_EQ(4, brief(param_ptr).to_string(pointer_buf, sizeof(pointer_buf)));
  EXPECT_STREQ("NULL", pointer_buf);

  int64_t pointer_pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_print_kv(pointer_buf, sizeof(pointer_buf), pointer_pos, KB(param_ptr)));
  EXPECT_STREQ("param_ptr:NULL", pointer_buf);

  char error_buf[64 * 1024] = {0};
  int64_t error_pos = 0;
  databuff_print_kv(error_buf, sizeof(error_buf), error_pos, "ret", OB_EAGAIN, "tablet_meta", param);
  EXPECT_GE(error_pos, sizeof(error_buf) - 2);
  const char *full_meta = strstr(error_buf, "tablet_meta:");
  ASSERT_NE(nullptr, full_meta);
  const char *large_field = strstr(full_meta, "storage_schema:");
  ASSERT_NE(nullptr, large_field);
  for (const char *key : {"transfer_info:", "transfer_src_tablet_status:",
                         "ddl_replay_status:", "fork_info:"}) {
    const char *field = strstr(full_meta, key);
    ASSERT_NE(nullptr, field);
    EXPECT_LT(field, large_field);
  }
  EXPECT_NE(nullptr, strstr(large_field, "column_array"));
}

TEST_F(TestTransferMdsCtx, empty_shell_and_transfer_batch_log_summary)
{
  storage::ObMigrationTabletParam empty_shell;
  empty_shell.is_empty_shell_ = true;
  char buf[2048] = {0};
  const int64_t len = brief(empty_shell).to_string(buf, sizeof(buf));
  ASSERT_LT(len, sizeof(buf) - 1);
  EXPECT_NE(nullptr, strstr(buf, "is_empty_shell:true"));
  EXPECT_NE(nullptr, strstr(buf, "schema_inited:false"));

  storage::ObTXStartTransferInInfo info;
  info.task_id_ = ObTransferTaskID(741);
  info.data_version_ = DATA_VERSION_4_3_5_2;
  info.src_ls_id_ = ObLSID(1001);
  info.dest_ls_id_ = ObLSID(1003);
  ASSERT_EQ(OB_SUCCESS, info.start_scn_.convert_for_tx(1234567));
  ASSERT_EQ(OB_SUCCESS, info.tablet_meta_list_.prepare_allocate(98));
  MEMSET(buf, 0, sizeof(buf));
  const int64_t batch_len = brief(info).to_string(buf, sizeof(buf));
  ASSERT_LT(batch_len, sizeof(buf) - 1);
  EXPECT_NE(nullptr, strstr(buf, "task_id:{id:741}"));
  EXPECT_NE(nullptr, strstr(buf, "src_ls_id:{id:1001}"));
  EXPECT_NE(nullptr, strstr(buf, "dest_ls_id:{id:1003}"));
  EXPECT_NE(nullptr, strstr(buf, "start_scn"));
  EXPECT_NE(nullptr, strstr(buf, "data_version"));
  EXPECT_NE(nullptr, strstr(buf, "tablet_count:98"));
  EXPECT_EQ(nullptr, strstr(buf, "tablet_meta_list"));

  char error_buf[64 * 1024] = {0};
  int64_t error_pos = 0;
  databuff_print_kv(error_buf, sizeof(error_buf), error_pos, "ret", OB_EAGAIN,
      "tx_start_transfer_in_info", info);
  EXPECT_GE(error_pos, sizeof(error_buf) - 2);
  const char *tablet_list = strstr(error_buf, "tablet_meta_list:");
  ASSERT_NE(nullptr, tablet_list);
  for (const char *key : {"task_id:{id:741}", "src_ls_id:{id:1001}", "dest_ls_id:{id:1003}",
                         "start_scn:", "data_version:"}) {
    const char *field = strstr(error_buf, key);
    ASSERT_NE(nullptr, field);
    EXPECT_LT(field, tablet_list);
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_transfer_mds_ctx.log*");
  OB_LOGGER.set_file_name("test_transfer_mds_ctx.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
