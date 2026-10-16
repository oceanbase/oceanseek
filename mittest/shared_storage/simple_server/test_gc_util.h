/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef TEST_GC_UTIL_H_
#define TEST_GC_UTIL_H_
#endif

#define protected public
#define private public

#include <string.h>
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_persist_common.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "src/storage/shared_storage/ob_ss_object_access_util.h"
#include "src/storage/incremental/sslog/ob_sslog_define.h"


namespace oceanbase
{

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
  ObLS *ls_;
};

TestRunCtx RunCtx;
static void gen_block_id(
    const int64_t op_id,
    const ObStorageObjectType type,
    const int64_t seq,
    MacroBlockId &block_id,
    const int64_t cg_id = 0)
{
  blocksstable::ObStorageObjectOpt opt;
  if (ObStorageObjectType::SHARED_TABLET_SUB_META == type) {
    opt.set_ss_tablet_sub_meta_opt(RunCtx.ls_id_.id(), RunCtx.tablet_id_.id(), op_id,
        seq /* start_seq */, RunCtx.tablet_id_.is_ls_inner_tablet(), 0 /* reorganization_scn */, true /* is_object_storage */);
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
  } else {
    const int64_t macro_op_id = ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == type ? 0 : op_id;
    opt.set_ss_share_object_opt(type, RunCtx.tablet_id_.is_ls_inner_tablet(), RunCtx.ls_id_.id(),
        RunCtx.tablet_id_.id(), macro_op_id, seq, cg_id /* column_group_id */, 0);
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, block_id));
  }
  ASSERT_TRUE(block_id.is_valid());
  SERVER_LOG(INFO, "get block_id", K(block_id), K(op_id), K(type), K(seq));
}

static void write_block(
    const MacroBlockId &block_id)
{

  ObStorageObjectWriteInfo write_info;

  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(block_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();
}

void wait_minor_finish(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "wait minor begin");

  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
          RunCtx.tenant_id_, RunCtx.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    SERVER_LOG(INFO, "minor result", K(row_cnt));
  } while (row_cnt > 0);
  SERVER_LOG(INFO, "minor finished", K(row_cnt));
}

void wait_major_finish(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "wait minor begin");

  ObSqlString sql;
  int64_t affected_rows = 0;
  static int64_t old_major_scn = 1;
  int64_t new_major_scn = 1;
  int64_t scn = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select frozen_scn, (frozen_scn - last_scn) as result from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu;", 
          RunCtx.tenant_id_));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("result", scn));
      ASSERT_EQ(OB_SUCCESS, result->get_int("frozen_scn", new_major_scn));
    }
    SERVER_LOG(INFO, "shared result", K(scn), K(new_major_scn));
    usleep(100 * 1000); // 100_ms
  } while (0 != scn || old_major_scn == new_major_scn);
  old_major_scn = new_major_scn;
  SERVER_LOG(INFO, "major finished", K(new_major_scn));
}

void get_ss_checkpoint_scn(common::ObMySQLProxy &sql_proxy, int64_t &ss_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ss_checkpoint_scn = 0;
  ObSqlString sql;
  // common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ss_checkpoint_scn from oceanbase.__all_virtual_shared_ls_meta where tenant_id=%lu and ls_id=%ld;",
        RunCtx.tenant_id_, RunCtx.ls_id_.id()));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ss_checkpoint_scn", ss_checkpoint_scn));
  }
}

void wait_upload_sstable(common::ObMySQLProxy &sql_proxy, const int64_t ss_checkpoint_scn)
{
  int64_t new_ss_checkpoint_scn = 0;
  while (new_ss_checkpoint_scn <= ss_checkpoint_scn) {
    get_ss_checkpoint_scn(sql_proxy, new_ss_checkpoint_scn);
    SERVER_LOG(INFO, "wait upload sstable", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_), K(new_ss_checkpoint_scn), K(ss_checkpoint_scn));
    usleep(100 * 1000);
  }
}

}
