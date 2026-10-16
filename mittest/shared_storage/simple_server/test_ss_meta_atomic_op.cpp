// owner: donglou.zl
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace oceanbase::common;
using namespace oceanbase::storage;

static const uint64_t TEST_TENANT_ID = 1;
static const ObLSID TEST_LS_ID(1);
static const ObTabletID TEST_TABLET_ID(200001);

class TestSSMetaAtomicOp : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSMetaAtomicOp() : unittest::ObSimpleClusterTestBase("test_ss_meta_atomic_op") {}

  common::ObISQLClient &get_sql_client()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  static share::SCN make_scn(const uint64_t val)
  {
    share::SCN scn;
    scn.convert_for_logservice(val);
    return scn;
  }

  static ObSSMetaAtomicOpInitParam make_ls_param(
      const int64_t op_id,
      const SCN &meta_scn,
      const ObLSID &ls_id)
  {
    ObSSMetaAtomicOpInitParam param;
    param.op_id_ = op_id;
    param.meta_scn_ = meta_scn;
    param.ls_id_ = ls_id;
    param.tablet_id_.reset();
    param.reorgan_scn_.reset();
    param.protocol_type_.type_ = ObAtomicProtocolTypeEnum::LS_META;
    return param;
  }

  static ObSSMetaAtomicOpInitParam make_tablet_param(
      const int64_t op_id,
      const SCN &meta_scn,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const SCN &reorgan_scn)
  {
    ObSSMetaAtomicOpInitParam param;
    param.op_id_ = op_id;
    param.meta_scn_ = meta_scn;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet_id;
    param.reorgan_scn_ = reorgan_scn;
    param.protocol_type_.type_ = ObAtomicProtocolTypeEnum::TABLET_META;
    return param;
  }
};

// ===========================================================================
// Test ObSSMetaAtomicBaseOp default construction
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, default_construction)
{
  ObSSMetaAtomicBaseOp op;
  ASSERT_EQ(-1, op.get_op_id());
  ASSERT_EQ(ObSSMetaAtomicOpType::INVALID_OP, op.get_op_type());
  ASSERT_FALSE(op.get_protocol_type().is_valid());
}

// ===========================================================================
// Test subclass op_type initialization
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, subclass_op_type)
{
  {
    ObSSMetaAtomicLSOp op;
    ASSERT_EQ(ObSSMetaAtomicOpType::LS_OP, op.get_op_type());
  }
  {
    ObSSMetaAtomicTabletOp op;
    ASSERT_EQ(ObSSMetaAtomicOpType::TABLET_OP, op.get_op_type());
  }
  {
    ObSSMetaAtomicSSTableOp op;
    ASSERT_EQ(ObSSMetaAtomicOpType::SSTABLE_OP, op.get_op_type());
  }
}

// ===========================================================================
// Test set_op_id / get_op_id
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, set_and_get_op_id)
{
  ObSSMetaAtomicBaseOp op;
  ASSERT_EQ(-1, op.get_op_id());
  op.set_op_id(42);
  ASSERT_EQ(42, op.get_op_id());
  op.set_op_id(0);
  ASSERT_EQ(0, op.get_op_id());
}

// ===========================================================================
// Test reset clears all fields
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, reset_clears_fields)
{
  ObSSMetaAtomicLSOp op;
  op.set_op_id(100);
  op.ls_id_ = TEST_LS_ID;
  op.tablet_id_ = TEST_TABLET_ID;

  op.reset();
  ASSERT_EQ(0, op.op_id_);
  ASSERT_FALSE(op.ls_id_.is_valid());
  ASSERT_FALSE(op.tablet_id_.is_valid());
  ASSERT_EQ(ObSSMetaAtomicOpType::INVALID_OP, op.get_op_type());
  ASSERT_FALSE(op.get_protocol_type().is_valid());
}

// ===========================================================================
// Test write_meta_info / read_meta_info round-trip with ObSSMetaHeadValue
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, write_read_meta_info_round_trip)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaAtomicLSOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::LS_META;

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = true;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;

  ObSSMetaHeadValue orig_value;
  orig_value.head_info_ = head_info;

  int ret = op.write_meta_info(orig_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(op.cached_buffer_.has_data());

  // simulate read by copying cached_buffer_ into cached_meta_
  ObSSMetaOpBufferGuard read_guard;
  const int64_t data_len = op.cached_buffer_.data_len_;
  char *read_buf = static_cast<char *>(share::mtl_malloc(data_len, "TestBuf"));
  ASSERT_NE(nullptr, read_buf);
  MEMCPY(read_buf, op.cached_buffer_.buf_, data_len);
  ObSSMetaOpBuffer read_buffer(read_buf, data_len);
  read_buffer.data_len_ = data_len;
  op.cached_meta_.set_buffer(read_buffer);

  ObSSMetaHeadValue read_value;
  ret = op.read_meta_info(read_value);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(head_info.mark_delete_, read_value.head_info_.mark_delete_);
  ASSERT_EQ(head_info.update_reason_, read_value.head_info_.update_reason_);
  ASSERT_TRUE(head_info == read_value.head_info_);
}

// ===========================================================================
// Test read_meta_info returns OB_OBJECT_NOT_EXIST when no cached meta
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, read_meta_info_no_cached_meta)
{
  ObSSMetaAtomicTabletOp op;

  ObSSMetaHeadValue value;
  int ret = op.read_meta_info(value);
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, ret);
}

// ===========================================================================
// Test serialize / deserialize static helpers directly
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, serialize_deserialize_static_helpers)
{
  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;

  ObSSMetaHeadValue orig_value;
  orig_value.head_info_ = head_info;

  const int64_t ser_size = orig_value.get_serialize_size();
  ASSERT_GT(ser_size, 0);

  char *buf = static_cast<char *>(ob_malloc(ser_size, "TestSer"));
  ASSERT_NE(nullptr, buf);
  ObSSMetaOpBuffer content_buf(buf, ser_size);

  int ret = ObSSMetaAtomicBaseOp::serialize_meta_info(orig_value, content_buf);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ser_size, content_buf.data_len_);

  ObSSMetaHeadValue deser_value;
  ret = ObSSMetaAtomicBaseOp::deserialize_meta_info(content_buf, deser_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(orig_value.head_info_ == deser_value.head_info_);

  ob_free(buf);
}

// ===========================================================================
// Test process fails when cached_buffer has no data
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, process_no_data)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaAtomicLSOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::LS_META;
  op.ls_id_ = ObLSID(7001);
  op.meta_scn_ = make_scn(100);

  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&get_sql_client(), TEST_TENANT_ID));
  int ret = op.process(trans, make_scn(100));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  trans.end(false);
}

// ===========================================================================
// Test ObSSMetaAtomicLSOp process: write and verify
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, ls_op_process_write_and_verify)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(7010);
  const SCN meta_scn = make_scn(500);

  // prepare op with serialized meta in cached_buffer_
  ObSSMetaAtomicLSOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::LS_META;
  op.ls_id_ = ls_id;
  op.meta_scn_ = meta_scn;

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;
  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;

  ASSERT_EQ(OB_SUCCESS, op.write_meta_info(head_value));
  ASSERT_TRUE(op.cached_buffer_.has_data());

  // write via process inside a transaction
  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&get_sql_client(), TEST_TENANT_ID));
  int ret = op.process(trans, meta_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, trans.end(true/*commit*/));

  // verify the row was written by reading it back
  ObArenaAllocator alloc;
  ObSSMetaBaseOperator read_op;
  ASSERT_EQ(OB_SUCCESS, read_op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  // construct the same meta_key that process would have used
  ObSSMetaLSKey ls_key;
  ls_key.ls_id_ = ls_id;
  ls_key.meta_scn_ = meta_scn;
  blocksstable::MacroBlockId macro_id = ls_key.convert_to_macro_id(true);
  char path_buf[SSMETA_PATH_MAX_LEN] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, path_buf, sizeof(path_buf), pos));
  ObString meta_key(pos, path_buf);

  ObString read_value;
  ret = read_op.read_row(TEST_TENANT_ID, ls_id, meta_type, meta_key, alloc, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(op.cached_buffer_.data_len_, read_value.length());
  ASSERT_EQ(0, MEMCMP(op.cached_buffer_.buf_, read_value.ptr(), read_value.length()));

  // cleanup
  int64_t affected_rows = 0;
  read_op.delete_row(TEST_TENANT_ID, ls_id, meta_type, meta_key, false, affected_rows);
}

// ===========================================================================
// CHECKPOINT 必须使用 SHARED_LS_CKPT（.T30）作为 meta_key
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, ls_ckpt_op_process_writes_t30_key)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(7013);
  const SCN meta_scn = make_scn(900);
  ObSSMetaAtomicLSCkptOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::CHECKPOINT;
  op.ls_id_ = ls_id;
  op.meta_scn_ = meta_scn;

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::CREATE_LS_CKPT;
  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;
  ASSERT_EQ(OB_SUCCESS, op.write_meta_info(head_value));

  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&get_sql_client(), TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, op.process(trans, meta_scn));
  ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit*/));

  ObArenaAllocator alloc;
  ObSSMetaBaseOperator read_op;
  ASSERT_EQ(OB_SUCCESS, read_op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);

  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = ls_id;
  ckpt_key.meta_scn_ = meta_scn;
  blocksstable::MacroBlockId ckpt_macro_id = ckpt_key.convert_to_macro_id(true);
  char ckpt_path_buf[SSMETA_PATH_MAX_LEN] = {0};
  int64_t ckpt_path_len = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      ckpt_macro_id, ckpt_path_buf, sizeof(ckpt_path_buf), ckpt_path_len));
  const ObString ckpt_meta_key(ckpt_path_len, ckpt_path_buf);

  ObString read_value;
  ASSERT_EQ(OB_SUCCESS, read_op.read_row(
      TEST_TENANT_ID, ls_id, meta_type, ckpt_meta_key, alloc, read_value));
  ASSERT_EQ(op.cached_buffer_.data_len_, read_value.length());
  ASSERT_EQ(0, MEMCMP(op.cached_buffer_.buf_, read_value.ptr(), read_value.length()));

  // 相同 LS/SCN 的 LS_META key（.T31）不应存在。
  ObSSMetaLSKey ls_key;
  ls_key.ls_id_ = ls_id;
  ls_key.meta_scn_ = meta_scn;
  blocksstable::MacroBlockId ls_macro_id = ls_key.convert_to_macro_id(true);
  char ls_path_buf[SSMETA_PATH_MAX_LEN] = {0};
  int64_t ls_path_len = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      ls_macro_id, ls_path_buf, sizeof(ls_path_buf), ls_path_len));
  const ObString ls_meta_key(ls_path_len, ls_path_buf);
  ObString unexpected_value;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, read_op.read_row(
      TEST_TENANT_ID, ls_id, meta_type, ls_meta_key, alloc, unexpected_value));

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, read_op.delete_row(
      TEST_TENANT_ID, ls_id, meta_type, ckpt_meta_key,
      false /*prefix_delete*/, affected_rows));
  ASSERT_EQ(1, affected_rows);
}

// ===========================================================================
// Test ObSSMetaAtomicTabletOp process: write and verify
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, tablet_op_process_write_and_verify)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(7011);
  const ObTabletID tablet_id(200011);
  const SCN meta_scn = make_scn(800);
  const SCN reorgan_scn = make_scn(0);

  ObSSMetaAtomicTabletOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::TABLET_META;
  op.ls_id_ = ls_id;
  op.tablet_id_ = tablet_id;
  op.meta_scn_ = meta_scn;
  op.reorgan_scn_ = reorgan_scn;

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::CREATE_TABLET;
  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;

  ASSERT_EQ(OB_SUCCESS, op.write_meta_info(head_value));
  ASSERT_TRUE(op.cached_buffer_.has_data());

  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&get_sql_client(), TEST_TENANT_ID));
  int ret = op.process(trans, meta_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, trans.end(true));

  // verify
  ObArenaAllocator alloc;
  ObSSMetaBaseOperator read_op;
  ASSERT_EQ(OB_SUCCESS, read_op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObSSMetaTabletKey tablet_key;
  tablet_key.ls_id_ = ls_id;
  tablet_key.tablet_id_ = tablet_id;
  tablet_key.reorganization_scn_ = reorgan_scn;
  tablet_key.meta_scn_ = meta_scn;
  blocksstable::MacroBlockId macro_id = tablet_key.convert_to_macro_id(true);
  char path_buf[SSMETA_PATH_MAX_LEN] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, path_buf, sizeof(path_buf), pos));
  ObString meta_key(pos, path_buf);

  ObString read_value;
  ret = read_op.read_row(TEST_TENANT_ID, ls_id, meta_type, meta_key, alloc, read_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(op.cached_buffer_.data_len_, read_value.length());
  ASSERT_EQ(0, MEMCMP(op.cached_buffer_.buf_, read_value.ptr(), read_value.length()));

  // cleanup
  int64_t affected_rows = 0;
  read_op.delete_row(TEST_TENANT_ID, ls_id, meta_type, meta_key, false, affected_rows);
}

// ===========================================================================
// Test process rollback: data should not persist
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, process_rollback)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls_id(7012);
  const SCN meta_scn = make_scn(300);

  ObSSMetaAtomicLSOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::LS_META;
  op.ls_id_ = ls_id;
  op.meta_scn_ = meta_scn;

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;
  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;
  ASSERT_EQ(OB_SUCCESS, op.write_meta_info(head_value));

  // process then rollback
  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&get_sql_client(), TEST_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, op.process(trans, meta_scn));
  ASSERT_EQ(OB_SUCCESS, trans.end(false/*rollback*/));

  // row should not exist
  ObArenaAllocator alloc;
  ObSSMetaBaseOperator read_op;
  ASSERT_EQ(OB_SUCCESS, read_op.init(get_sql_client(), OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObSSMetaLSKey ls_key;
  ls_key.ls_id_ = ls_id;
  ls_key.meta_scn_ = meta_scn;
  blocksstable::MacroBlockId macro_id = ls_key.convert_to_macro_id(true);
  char path_buf[SSMETA_PATH_MAX_LEN] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, path_buf, sizeof(path_buf), pos));
  ObString meta_key(pos, path_buf);

  ObString read_value;
  int ret = read_op.read_row(TEST_TENANT_ID, ls_id, meta_type, meta_key, alloc, read_value);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// Test ObSSMetaAtomicSSTableOp process (override, no-op)
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, sstable_op_process)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaAtomicSSTableOp op;
  common::ObMySQLTransaction trans;
  int ret = op.process(trans, SCN::min_scn());
  ASSERT_EQ(OB_SUCCESS, ret);
}

// ===========================================================================
// Test ObSSMetaOpBuffer basic operations
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, op_buffer_basic)
{
  ObSSMetaOpBuffer buf;
  ASSERT_FALSE(buf.is_valid());
  ASSERT_FALSE(buf.has_data());

  char data[64];
  ObSSMetaOpBuffer buf2(data, sizeof(data));
  ASSERT_TRUE(buf2.is_valid());
  ASSERT_FALSE(buf2.has_data());

  buf2.data_len_ = 10;
  ASSERT_TRUE(buf2.has_data());

  buf2.set_op_id(99);
  ASSERT_EQ(99, buf2.op_id_);
}

// ===========================================================================
// Test ObSSMetaOpBuffer assign (move semantics)
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, op_buffer_assign)
{
  char *src_buf = static_cast<char *>(ob_malloc(128, "TestAssign"));
  ASSERT_NE(nullptr, src_buf);
  MEMSET(src_buf, 'X', 128);

  ObSSMetaOpBuffer src(src_buf, 128);
  src.data_len_ = 64;
  src.set_op_id(7);

  ObSSMetaOpBuffer dst;
  dst.assign(src);

  ASSERT_EQ(src_buf, dst.buf_);
  ASSERT_EQ(128, dst.size_);
  ASSERT_EQ(64, dst.data_len_);

  ASSERT_EQ(nullptr, src.buf_);
  ASSERT_EQ(0, src.size_);

  ob_free(src_buf);
}

// ===========================================================================
// Test ObSSMetaOpBufferGuard lifecycle
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, op_buffer_guard_lifecycle)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  {
    ObSSMetaOpBufferGuard guard;
    ASSERT_FALSE(guard.is_valid());

    char *buf = static_cast<char *>(share::mtl_malloc(256, "TestGuard"));
    ASSERT_NE(nullptr, buf);
    ObSSMetaOpBuffer buffer(buf, 256);
    guard.set_buffer(buffer);

    ASSERT_TRUE(guard.is_valid());
    ASSERT_EQ(256, guard.get_buffer().size_);
  }
  // guard destructor should free the buffer without crash
}

// ===========================================================================
// Test write_meta_info with custom serialize function
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, write_meta_info_custom_serialize)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaAtomicTabletOp op;
  op.protocol_type_.type_ = ObAtomicProtocolTypeEnum::TABLET_META;

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;

  ObSSMetaHeadValue value;
  value.head_info_ = head_info;

  auto custom_serialize = [](const ObSSMetaHeadValue &meta_info, ObSSMetaOpBuffer &content_buf) -> int {
    return ObSSMetaAtomicBaseOp::serialize_meta_info(meta_info, content_buf);
  };

  int ret = op.write_meta_info(value, ObFunction<int(const ObSSMetaHeadValue&, ObSSMetaOpBuffer&)>(custom_serialize));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(op.cached_buffer_.has_data());
}

// ===========================================================================
// Test init param construction helpers
// ===========================================================================

TEST_F(TestSSMetaAtomicOp, init_param_construction)
{
  SCN meta_scn = SCN::min_scn();
  SCN reorgan_scn = SCN::min_scn();

  {
    ObSSMetaAtomicOpInitParam param = make_ls_param(1, meta_scn, TEST_LS_ID);
    ASSERT_EQ(1, param.op_id_);
    ASSERT_EQ(ObAtomicProtocolTypeEnum::LS_META, param.protocol_type_.type_);
    ASSERT_EQ(TEST_LS_ID, param.ls_id_);
  }

  {
    ObSSMetaAtomicOpInitParam param = make_tablet_param(2, meta_scn, TEST_LS_ID, TEST_TABLET_ID, reorgan_scn);
    ASSERT_EQ(2, param.op_id_);
    ASSERT_EQ(ObAtomicProtocolTypeEnum::TABLET_META, param.protocol_type_.type_);
    ASSERT_EQ(TEST_LS_ID, param.ls_id_);
    ASSERT_EQ(TEST_TABLET_ID, param.tablet_id_);
  }
}

} // share
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
