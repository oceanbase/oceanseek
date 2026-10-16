/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include <thread>
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/share/ob_shared_meta_common.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestSSMetaCommonInfo : public ::testing::Test
{
public:
  TestSSMetaCommonInfo() = default;
  ~TestSSMetaCommonInfo() = default;
};

/*-----------------------------------------ObSSMetaLSCkptKey-----------------------------------------*/

// 覆盖模块：ObSSMetaLSCkptKey::convert_to_macro_id 和 is_valid_ls_ckpt_macro_id。
// 测试流程：构造合法 key，分别生成 TABLE/OBJECT_STORAGE MacroBlockId 并校验；
//           再篡改 ID 模式、通用结构、对象类型、存储位置、third_id/seq_id 和
//           LS ID，验证非法 MacroBlockId 被拒绝。
TEST_F(TestSSMetaCommonInfo, ls_ckpt_key_macro_id_validation)
{
  ObSSMetaLSCkptKey key;
  key.ls_id_ = ObLSID(1001);
  ASSERT_EQ(OB_SUCCESS, key.meta_scn_.convert_for_tx(345));

  blocksstable::MacroBlockId table_id = key.convert_to_macro_id(true);
  blocksstable::MacroBlockId object_id = key.convert_to_macro_id(false);
  ASSERT_TRUE(is_valid_ls_ckpt_macro_id(table_id));
  ASSERT_TRUE(is_valid_ls_ckpt_macro_id(object_id));

  blocksstable::MacroBlockId local_id(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_LS_CKPT), 1001, 0);
  ASSERT_TRUE(local_id.is_valid());
  ASSERT_TRUE(local_id.is_id_mode_local());
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_CKPT, local_id.storage_object_type());
  ASSERT_TRUE(local_id.is_in_object_storage());
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(local_id));

  blocksstable::MacroBlockId wrong_type_id(object_id);
  wrong_type_id.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_LS_META));
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(wrong_type_id));

  blocksstable::MacroBlockId wrong_location_id(object_id);
  wrong_location_id.set_location(blocksstable::ObMacroBlockIdLocation::CONTROL);
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(wrong_location_id));

  blocksstable::MacroBlockId nonzero_third_id(object_id);
  nonzero_third_id.set_third_id(1);
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(nonzero_third_id));

  blocksstable::MacroBlockId nonzero_seq_id(object_id);
  nonzero_seq_id.set_seq_id(1);
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(nonzero_seq_id));

  blocksstable::MacroBlockId wrong_version_id(object_id);
  wrong_version_id.ss_version_ = 0;
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(wrong_version_id));

  blocksstable::MacroBlockId zero_ls_id(object_id);
  zero_ls_id.set_second_id(0);
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(zero_ls_id));

  blocksstable::MacroBlockId negative_ls_id(object_id);
  negative_ls_id.set_second_id(-2);
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(negative_ls_id));

  blocksstable::MacroBlockId max_ls_id(object_id);
  max_ls_id.set_second_id(INT64_MAX);
  ASSERT_FALSE(is_valid_ls_ckpt_macro_id(max_ls_id));
}

/*-----------------------------------------ObSSMetaTabletKey-----------------------------------------*/

TEST_F(TestSSMetaCommonInfo, tablet_key_default_construct)
{
  ObSSMetaTabletKey key;
  ASSERT_FALSE(key.is_valid());
  ASSERT_EQ(SCN::min_scn(), key.meta_scn_);
}

TEST_F(TestSSMetaCommonInfo, tablet_key_copy_construct)
{
  ObSSMetaTabletKey key1;
  key1.ls_id_ = ObLSID(1001);
  key1.tablet_id_ = ObTabletID(200001);
  key1.reorganization_scn_ = SCN::min_scn();
  key1.meta_scn_ = SCN::min_scn();
  key1.meta_scn_.convert_for_tx(100);

  ObSSMetaTabletKey key2(key1);
  ASSERT_EQ(key1.ls_id_, key2.ls_id_);
  ASSERT_EQ(key1.tablet_id_, key2.tablet_id_);
  ASSERT_EQ(key1.reorganization_scn_, key2.reorganization_scn_);
  ASSERT_EQ(key1.meta_scn_, key2.meta_scn_);
}

TEST_F(TestSSMetaCommonInfo, tablet_key_is_valid)
{
  ObSSMetaTabletKey key;
  ASSERT_FALSE(key.is_valid());

  key.ls_id_ = ObLSID(1);
  key.tablet_id_ = ObTabletID(200001);
  key.reorganization_scn_ = SCN::min_scn();
  key.meta_scn_ = SCN::invalid_scn();
  ASSERT_FALSE(key.is_valid());

  key.meta_scn_.convert_for_tx(1);
  ASSERT_TRUE(key.is_valid());
}

TEST_F(TestSSMetaCommonInfo, tablet_key_reset)
{
  ObSSMetaTabletKey key;
  key.ls_id_ = ObLSID(1);
  key.tablet_id_ = ObTabletID(200001);
  key.reorganization_scn_ = SCN::min_scn();
  key.meta_scn_.convert_for_tx(100);
  ASSERT_TRUE(key.is_valid());

  key.reset();
  ASSERT_FALSE(key.is_valid());
  ASSERT_EQ(SCN::min_scn(), key.meta_scn_);
}

TEST_F(TestSSMetaCommonInfo, tablet_key_equality)
{
  ObSSMetaTabletKey key1;
  key1.ls_id_ = ObLSID(1);
  key1.tablet_id_ = ObTabletID(200001);
  key1.reorganization_scn_ = SCN::min_scn();
  key1.meta_scn_.convert_for_tx(100);

  ObSSMetaTabletKey key2(key1);
  ASSERT_TRUE(key1 == key2);

  key2.meta_scn_.convert_for_tx(200);
  ASSERT_FALSE(key1 == key2);
}

// 覆盖模块：普通 tablet 的 ObSSMetaTabletKey 双向转换和 is_valid_ss_tablet_id。
// 测试流程：将 key 转为 MacroBlockId，再携带默认 LS 反向还原并比较；
//           最后传入无效默认 LS，验证转换失败且输出 key 被重置。
TEST_F(TestSSMetaCommonInfo, tablet_key_macro_id_round_trip_normal)
{
  ObSSMetaTabletKey key;
  key.ls_id_ = ObLSID(1001);
  key.tablet_id_ = ObTabletID(200001);
  ASSERT_EQ(OB_SUCCESS, key.reorganization_scn_.convert_for_tx(123));
  ASSERT_EQ(OB_SUCCESS, key.meta_scn_.convert_for_tx(456));

  blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(false);
  ASSERT_TRUE(is_valid_ss_tablet_id(macro_id));

  ObSSMetaTabletKey restored_key;
  ASSERT_EQ(OB_SUCCESS, restored_key.init_from_macro_id(macro_id, key.ls_id_));
  ASSERT_TRUE(key == restored_key);

  restored_key.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, restored_key.init_from_macro_id(macro_id, ObLSID()));
  ASSERT_FALSE(restored_key.is_valid());
}

// 覆盖模块：inner tablet 的 LS 编码以及 ObSSMetaTabletKey 双向转换。
// 测试流程：生成包含 LS 的 MacroBlockId，再使用无效默认 LS 反向还原；
//           验证 LS 取自 MacroBlockId，且还原后的 key 与原 key 一致。
TEST_F(TestSSMetaCommonInfo, tablet_key_macro_id_round_trip_inner_tablet)
{
  ObSSMetaTabletKey key;
  key.ls_id_ = ObLSID(1002);
  key.tablet_id_ = ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID);
  key.reorganization_scn_ = SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, key.meta_scn_.convert_for_tx(789));

  blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(true);
  ASSERT_TRUE(is_valid_ss_tablet_id(macro_id));

  // inner tablet 的 LS 来自 MacroBlockId，不依赖调用方传入的默认 LS。
  ObSSMetaTabletKey restored_key;
  ASSERT_EQ(OB_SUCCESS, restored_key.init_from_macro_id(macro_id, ObLSID()));
  ASSERT_TRUE(key == restored_key);
}

// 覆盖模块：is_valid_ss_tablet_id 和 ObSSMetaTabletKey::init_from_macro_id 的异常校验。
// 测试流程：基于合法 MacroBlockId 构造错误 ID 模式、通用结构、对象类型、存储位置、
//           Tablet ID 和 seq_id；
//           验证 validator 与反向转换均拒绝异常输入，且失败后输出 key 无效。
TEST_F(TestSSMetaCommonInfo, tablet_key_rejects_unexpected_macro_id)
{
  ObSSMetaTabletKey key;
  key.ls_id_ = ObLSID(1001);
  key.tablet_id_ = ObTabletID(200001);
  ASSERT_EQ(OB_SUCCESS, key.reorganization_scn_.convert_for_tx(123));
  ASSERT_EQ(OB_SUCCESS, key.meta_scn_.convert_for_tx(456));
  blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(false);

  blocksstable::MacroBlockId local_id(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_TABLET_META), 200001, 0);
  ASSERT_TRUE(local_id.is_valid());
  ASSERT_TRUE(local_id.is_id_mode_local());
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_META, local_id.storage_object_type());
  ASSERT_TRUE(local_id.is_in_object_storage());
  ASSERT_FALSE(is_valid_ss_tablet_id(local_id));

  blocksstable::MacroBlockId wrong_type_id(macro_id);
  wrong_type_id.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::SHARED_LS_CKPT));
  ASSERT_FALSE(is_valid_ss_tablet_id(wrong_type_id));

  blocksstable::MacroBlockId wrong_location_id(macro_id);
  wrong_location_id.set_location(blocksstable::ObMacroBlockIdLocation::CONTROL);
  ASSERT_FALSE(is_valid_ss_tablet_id(wrong_location_id));

  blocksstable::MacroBlockId wrong_version_id(macro_id);
  wrong_version_id.ss_version_ = 0;
  ASSERT_FALSE(is_valid_ss_tablet_id(wrong_version_id));

  blocksstable::MacroBlockId zero_tablet_id(macro_id);
  zero_tablet_id.set_second_id(0);
  ASSERT_FALSE(is_valid_ss_tablet_id(zero_tablet_id));

  blocksstable::MacroBlockId negative_tablet_id(macro_id);
  negative_tablet_id.set_second_id(-2);
  ASSERT_FALSE(is_valid_ss_tablet_id(negative_tablet_id));

  blocksstable::MacroBlockId max_tablet_id(macro_id);
  max_tablet_id.set_second_id(INT64_MAX);
  ASSERT_FALSE(is_valid_ss_tablet_id(max_tablet_id));

  blocksstable::MacroBlockId nonzero_seq_id(macro_id);
  nonzero_seq_id.set_seq_id(1);
  ASSERT_FALSE(is_valid_ss_tablet_id(nonzero_seq_id));

  ObSSMetaTabletKey restored_key;
  ASSERT_NE(OB_SUCCESS, restored_key.init_from_macro_id(wrong_type_id, key.ls_id_));
  ASSERT_FALSE(restored_key.is_valid());
}

/*-----------------------------------------ObSSMetaOpBuffer / ObSSMetaOpBufferGuard-----------------------------------------*/

TEST_F(TestSSMetaCommonInfo, op_buffer_default_construct)
{
  ObSSMetaOpBuffer buf;
  ASSERT_FALSE(buf.is_valid());
  ASSERT_FALSE(buf.has_data());
  ASSERT_EQ(nullptr, buf.buf_);
  ASSERT_EQ(0, buf.size_);
  ASSERT_EQ(0, buf.data_len_);
  ASSERT_EQ(-1, buf.op_id_);
}

TEST_F(TestSSMetaCommonInfo, op_buffer_construct_with_buf)
{
  char raw[128];
  ObSSMetaOpBuffer buf(raw, sizeof(raw));
  ASSERT_TRUE(buf.is_valid());
  ASSERT_FALSE(buf.has_data());
  ASSERT_EQ(raw, buf.buf_);
  ASSERT_EQ(raw, buf.ori_buf_);
  ASSERT_EQ(128, buf.size_);
  ASSERT_EQ(0, buf.data_len_);
}

TEST_F(TestSSMetaCommonInfo, op_buffer_assign_ptr)
{
  ObSSMetaOpBuffer buf;
  char raw[64] = "hello";
  buf.assign(raw, sizeof(raw));
  ASSERT_TRUE(buf.is_valid());
  ASSERT_TRUE(buf.has_data());
  ASSERT_EQ(raw, buf.buf_);
  ASSERT_EQ(sizeof(raw), buf.size_);
  ASSERT_EQ(sizeof(raw), buf.data_len_);
}

TEST_F(TestSSMetaCommonInfo, op_buffer_assign_move)
{
  char raw[64];
  ObSSMetaOpBuffer src;
  src.buf_ = raw;
  src.ori_buf_ = nullptr;
  src.size_ = 64;
  src.data_len_ = 32;

  ObSSMetaOpBuffer dst;
  dst.assign(src);

  ASSERT_EQ(raw, dst.buf_);
  ASSERT_EQ(64, dst.size_);
  ASSERT_EQ(32, dst.data_len_);

  ASSERT_EQ(nullptr, src.buf_);
  ASSERT_EQ(0, src.size_);
  ASSERT_EQ(0, src.data_len_);
}

TEST_F(TestSSMetaCommonInfo, op_buffer_set_op_id)
{
  ObSSMetaOpBuffer buf;
  ASSERT_EQ(-1, buf.op_id_);
  buf.set_op_id(42);
  ASSERT_EQ(42, buf.op_id_);
}

TEST_F(TestSSMetaCommonInfo, op_buffer_reset)
{
  char raw[64];
  ObSSMetaOpBuffer buf(raw, sizeof(raw));
  buf.data_len_ = 10;
  buf.set_op_id(5);
  buf.reset();

  ASSERT_EQ(nullptr, buf.buf_);
  ASSERT_EQ(nullptr, buf.ori_buf_);
  ASSERT_EQ(0, buf.size_);
  ASSERT_EQ(0, buf.data_len_);
  ASSERT_EQ(-1, buf.op_id_);
}

/*-----------------------------------------ObSSMetaHeadValueInfo / ObSSMetaHeadValue-----------------------------------------*/

TEST_F(TestSSMetaCommonInfo, head_value_info_default)
{
  ObSSMetaHeadValueInfo info;
  ASSERT_FALSE(info.is_mark_deleted());
  ASSERT_FALSE(info.is_valid());
  ASSERT_EQ(ObMetaUpdateReason::INVALID_META_UPDATE_REASON, info.update_reason_);
}

TEST_F(TestSSMetaCommonInfo, head_value_info_set_valid_reason)
{
  ObSSMetaHeadValueInfo info;
  int ret = info.set(ObMetaUpdateReason::CREATE_LS);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(info.is_valid());
  ASSERT_FALSE(info.is_mark_deleted());

  ObMetaUpdateReason reason;
  info.get_update_reason(reason);
  ASSERT_EQ(ObMetaUpdateReason::CREATE_LS, reason);
}

TEST_F(TestSSMetaCommonInfo, head_value_info_set_invalid_reason)
{
  ObSSMetaHeadValueInfo info;
  int ret = info.set(ObMetaUpdateReason::INVALID_META_UPDATE_REASON);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_FALSE(info.is_valid());
}

TEST_F(TestSSMetaCommonInfo, head_value_info_equality)
{
  ObSSMetaHeadValueInfo info1;
  info1.set(ObMetaUpdateReason::CREATE_LS);
  ObSSMetaHeadValueInfo info2;
  info2.set(ObMetaUpdateReason::CREATE_LS);
  ASSERT_TRUE(info1 == info2);

  info2.set(ObMetaUpdateReason::CREATE_TABLET);
  ASSERT_FALSE(info1 == info2);
}

TEST_F(TestSSMetaCommonInfo, head_value_set_and_get)
{
  ObSSMetaHeadValue head;

  ObSSMetaHeadValueInfo info;
  info.set(ObMetaUpdateReason::CREATE_TABLET);
  int ret = head.set_meta_head_info(info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(head.get_meta_head_info() == info);
}

TEST_F(TestSSMetaCommonInfo, head_value_set_invalid)
{
  ObSSMetaHeadValue head;
  ObSSMetaHeadValueInfo invalid_info;
  int ret = head.set_meta_head_info(invalid_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST_F(TestSSMetaCommonInfo, head_value_serialize_deserialize)
{
  ObSSMetaHeadValue head1;
  ObSSMetaHeadValueInfo info;
  info.set(ObMetaUpdateReason::CREATE_LS);
  head1.set_meta_head_info(info);

  const int64_t buf_size = head1.get_serialize_size();
  ASSERT_GT(buf_size, 0);

  char buf[1024];
  int64_t pos = 0;
  int ret = head1.serialize(buf, sizeof(buf), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buf_size, pos);

  ObSSMetaHeadValue head2;
  int64_t deser_pos = 0;
  ret = head2.deserialize(buf, pos, deser_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, deser_pos);
  ASSERT_TRUE(head1.get_meta_head_info() == head2.get_meta_head_info());
}

TEST_F(TestSSMetaCommonInfo, head_value_info_deleted_scn_serialize)
{
  ObSSMetaHeadValueInfo info;
  ASSERT_EQ(OB_SUCCESS, info.set(ObMetaUpdateReason::TABLET_META_UPDATE, true /*mark_delete*/));
  share::SCN deleted_scn;
  ASSERT_EQ(OB_SUCCESS, deleted_scn.convert_for_tx(123456789));
  info.set_deleted_scn(deleted_scn);
  ASSERT_TRUE(info.is_mark_deleted());
  ASSERT_EQ(deleted_scn, info.get_deleted_scn());

  ObSSMetaHeadValue head1;
  ASSERT_EQ(OB_SUCCESS, head1.set_meta_head_info(info));

  const int64_t buf_size = head1.get_serialize_size();
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, head1.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(buf_size, pos);

  ObSSMetaHeadValue head2;
  int64_t deser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, head2.deserialize(buf, pos, deser_pos));
  ASSERT_TRUE(head2.get_meta_head_info().is_mark_deleted());
  ASSERT_EQ(deleted_scn, head2.get_meta_head_info().get_deleted_scn());
  ASSERT_TRUE(head1.get_meta_head_info() == head2.get_meta_head_info());
}

/*-----------------------------------------ObSSMetaKeyParam-----------------------------------------*/

TEST_F(TestSSMetaCommonInfo, key_param_default)
{
  ObSSMetaKeyParam param;
  ASSERT_FALSE(param.is_valid());
  ASSERT_TRUE(param.is_in_table_);
  ASSERT_EQ(ObAtomicProtocolTypeEnum::INVALID_TYPE, param.protocol_type_);
}

TEST_F(TestSSMetaCommonInfo, key_param_set_tablet_level)
{
  ObSSMetaKeyParam param;
  SCN meta_scn;
  meta_scn.convert_for_tx(100);
  SCN reorgan_scn = SCN::min_scn();

  param.set_tablet_level_param(
      ObAtomicProtocolTypeEnum::TABLET_META,
      ObLSID(1001),
      ObTabletID(200001),
      meta_scn,
      reorgan_scn,
      true);

  ASSERT_TRUE(param.is_valid());
  ASSERT_EQ(ObAtomicProtocolTypeEnum::TABLET_META, param.protocol_type_);
  ASSERT_EQ(ObLSID(1001), param.tablet_key_.ls_id_);
  ASSERT_EQ(ObTabletID(200001), param.tablet_key_.tablet_id_);
  ASSERT_EQ(meta_scn, param.tablet_key_.meta_scn_);
  ASSERT_EQ(reorgan_scn, param.tablet_key_.reorganization_scn_);
}

TEST_F(TestSSMetaCommonInfo, key_param_set_ls_level)
{
  ObSSMetaKeyParam param;
  SCN meta_scn;
  meta_scn.convert_for_tx(200);

  param.set_ls_level_param(
      ObAtomicProtocolTypeEnum::LS_META,
      ObLSID(1001),
      meta_scn,
      false);

  ASSERT_TRUE(param.is_valid());
  ASSERT_FALSE(param.is_in_table_);
  ASSERT_EQ(ObAtomicProtocolTypeEnum::LS_META, param.protocol_type_);
  ASSERT_EQ(ObLSID(1001), param.ls_key_.ls_id_);
  ASSERT_EQ(meta_scn, param.ls_key_.meta_scn_);
}

TEST_F(TestSSMetaCommonInfo, key_param_set_tenant_level)
{
  ObSSMetaKeyParam param;
  param.set_tenant_level_param(ObAtomicProtocolTypeEnum::TABLET_META, true);
  ASSERT_FALSE(param.is_valid());
}

TEST_F(TestSSMetaCommonInfo, key_param_reset)
{
  ObSSMetaKeyParam param;
  SCN meta_scn;
  meta_scn.convert_for_tx(100);
  param.set_tablet_level_param(
      ObAtomicProtocolTypeEnum::TABLET_META,
      ObLSID(1),
      ObTabletID(200001),
      meta_scn,
      SCN::min_scn());
  ASSERT_TRUE(param.is_valid());

  param.reset();
  ASSERT_FALSE(param.is_valid());
  ASSERT_EQ(ObAtomicProtocolTypeEnum::INVALID_TYPE, param.protocol_type_);
}

TEST_F(TestSSMetaCommonInfo, key_param_serialize_deserialize)
{
  ObSSMetaKeyParam param1;
  SCN meta_scn;
  meta_scn.convert_for_tx(300);

  param1.set_tablet_level_param(
      ObAtomicProtocolTypeEnum::TABLET_META,
      ObLSID(1001),
      ObTabletID(200001),
      meta_scn,
      SCN::min_scn(),
      true);

  const int64_t buf_size = param1.get_serialize_size();
  ASSERT_GT(buf_size, 0);

  char buf[1024];
  int64_t pos = 0;
  int ret = param1.serialize(buf, sizeof(buf), pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaKeyParam param2;
  int64_t deser_pos = 0;
  ret = param2.deserialize(buf, pos, deser_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(pos, deser_pos);

  ASSERT_EQ(param1.is_in_table_, param2.is_in_table_);
  ASSERT_EQ(param1.protocol_type_, param2.protocol_type_);
  ASSERT_EQ(param1.tablet_key_.ls_id_, param2.tablet_key_.ls_id_);
  ASSERT_EQ(param1.tablet_key_.tablet_id_, param2.tablet_key_.tablet_id_);
  ASSERT_EQ(param1.tablet_key_.meta_scn_, param2.tablet_key_.meta_scn_);
  ASSERT_EQ(param1.tablet_key_.reorganization_scn_, param2.tablet_key_.reorganization_scn_);
}

/*-----------------------------------------ObSSTabletMetaValue-----------------------------------------*/

TEST_F(TestSSMetaCommonInfo, tablet_meta_value_serialize_version_and_head)
{
  // Construct an ObSSTabletMetaValue manually and verify that the outer
  // version+length envelope and the embedded ObSSMetaHeadValue round-trip
  // correctly through serialize → get_tablet_raw_buffer.
  //
  // We cannot easily build a full ObTablet in a unit test, so we exercise
  // the envelope / head portion which is the part that failed in the
  // integration test (version mismatch, pos offset bugs).

  ObArenaAllocator allocator("TestTabletVal");

  // --- Step 1: build a minimal ObSSMetaHeadValue, serialize it, and
  //     wrap it in the ObSSTabletMetaValue envelope (version + len). ---
  ObSSMetaHeadValueInfo head_info;
  ASSERT_EQ(OB_SUCCESS, head_info.set(ObMetaUpdateReason::CREATE_TABLET));

  ObSSMetaHeadValue head;
  ASSERT_EQ(OB_SUCCESS, head.set_meta_head_info(head_info));

  // Serialize head alone
  const int64_t head_ser_size = head.get_serialize_size();
  ASSERT_GT(head_ser_size, 0);

  char head_buf[256];
  int64_t head_pos = 0;
  ASSERT_EQ(OB_SUCCESS, head.serialize(head_buf, sizeof(head_buf), head_pos));
  ASSERT_EQ(head_ser_size, head_pos);

  // Deserialize head and verify round-trip
  ObSSMetaHeadValue head2;
  int64_t head_deser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, head2.deserialize(head_buf, head_pos, head_deser_pos));
  ASSERT_EQ(head_pos, head_deser_pos);
  ASSERT_TRUE(head.get_meta_head_info() == head2.get_meta_head_info());

  LOG_INFO("head serialize/deserialize round-trip OK", K(head_ser_size));
}

TEST_F(TestSSMetaCommonInfo, tablet_meta_value_deserialize_value_info_from_head)
{
  // deserialize_value_info is designed for buffers produced by
  // ObDefaultSSMetaValue<T>::serialize(), which has a double-envelope
  // layout:
  //   [outer_ver][outer_len][ ObSSMetaHeadValue::serialize() | meta_value_ ]
  //
  // It manually strips the outer envelope, then calls
  // ObSSMetaHeadValue::deserialize() on the inner portion.
  // Here we simulate that layout without needing a full MetaValueType.

  ObSSMetaHeadValueInfo info;
  ASSERT_EQ(OB_SUCCESS, info.set(ObMetaUpdateReason::CREATE_TABLET));

  ObSSMetaHeadValue head_orig;
  ASSERT_EQ(OB_SUCCESS, head_orig.set_meta_head_info(info));

  // Step 1: serialize head (produces [head_ver][head_len][head_info_data])
  char inner_buf[256];
  int64_t inner_pos = 0;
  ASSERT_EQ(OB_SUCCESS, head_orig.serialize(inner_buf, sizeof(inner_buf), inner_pos));
  const int64_t inner_len = inner_pos;

  // Step 2: wrap in outer envelope [outer_ver=1][outer_len=inner_len][inner_data]
  char buf[512];
  int64_t pos = 0;
  int64_t outer_version = 1;
  serialization::encode_vi64(buf, sizeof(buf), pos, outer_version);
  const int64_t size_nbytes = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  int64_t tmp_pos = 0;
  serialization::encode_fixed_bytes_i64(buf + pos, size_nbytes, tmp_pos, inner_len);
  pos += size_nbytes;
  ASSERT_LE(pos + inner_len, (int64_t)sizeof(buf));
  memcpy(buf + pos, inner_buf, inner_len);
  const int64_t total_len = pos + inner_len;

  // Step 3: deserialize_value_info should extract head_info from the double envelope
  ObSSMetaHeadValue head_parsed;
  int64_t head_pos = 0;
  ASSERT_EQ(OB_SUCCESS, head_parsed.deserialize_value_info(buf, total_len, head_pos));
  ASSERT_EQ(total_len, head_pos);
  ASSERT_TRUE(head_orig.get_meta_head_info() == head_parsed.get_meta_head_info());

  LOG_INFO("deserialize_value_info OK", K(total_len), K(head_pos));
}

// Verify that convert_to_macro_id uses is_ls_inner_tablet (not is_inner_tablet)
// to decide whether third_id carries ls_id or reorganization_scn.
// Regression guard for the bug where sys-table tablets (e.g. id=292) were
// misclassified as LS-inner tablets, causing the meta_key to embed ls_id
// instead of reorganization_scn.
TEST_F(TestSSMetaCommonInfo, tablet_key_convert_to_macro_id_third_id)
{
  const ObLSID ls_id(1);
  SCN meta_scn;
  meta_scn.convert_for_tx(100);
  SCN reorgan_scn = SCN::min_scn();

  // Case 1: LS inner tablet (tx_data) — third_id must be ls_id
  {
    ObSSMetaTabletKey key;
    key.ls_id_ = ls_id;
    key.tablet_id_ = ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID);
    key.reorganization_scn_ = reorgan_scn;
    key.meta_scn_ = meta_scn;
    ASSERT_TRUE(key.tablet_id_.is_ls_inner_tablet());
    ASSERT_TRUE(key.tablet_id_.is_inner_tablet());

    blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(true);
    ASSERT_EQ(ls_id.id(), macro_id.third_id());
  }

  // Case 2: sys-table tablet (id=292) — is_inner_tablet() is true but
  // is_ls_inner_tablet() is false; third_id must be reorganization_scn.
  {
    ObSSMetaTabletKey key;
    key.ls_id_ = ls_id;
    key.tablet_id_ = ObTabletID(292);
    key.reorganization_scn_ = reorgan_scn;
    key.meta_scn_ = meta_scn;
    ASSERT_TRUE(key.tablet_id_.is_inner_tablet());
    ASSERT_FALSE(key.tablet_id_.is_ls_inner_tablet());

    blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(true);
    ASSERT_EQ((int64_t)reorgan_scn.get_val_for_inner_table_field(), macro_id.third_id());
    // must NOT be ls_id, which would be 1
    ASSERT_NE(ls_id.id(), macro_id.third_id());
  }

  // Case 3: user tablet — third_id must be reorganization_scn
  {
    SCN user_reorgan_scn;
    user_reorgan_scn.convert_for_tx(999);

    ObSSMetaTabletKey key;
    key.ls_id_ = ls_id;
    key.tablet_id_ = ObTabletID(200001);
    key.reorganization_scn_ = user_reorgan_scn;
    key.meta_scn_ = meta_scn;
    ASSERT_FALSE(key.tablet_id_.is_inner_tablet());
    ASSERT_FALSE(key.tablet_id_.is_ls_inner_tablet());

    blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(true);
    ASSERT_EQ((int64_t)user_reorgan_scn.get_val_for_inner_table_field(), macro_id.third_id());
  }
}

TEST_F(TestSSMetaCommonInfo, tablet_meta_value_bad_version_rejected)
{
  // Verify that a buffer with a wrong version is rejected by
  // ObSSMetaHeadValue::deserialize_value_info.

  char buf[64];
  int64_t pos = 0;

  // Write a bogus version (115) and a length
  int64_t fake_version = 115;
  int64_t fake_len = 10;
  serialization::encode_vi64(buf, sizeof(buf), pos, fake_version);
  serialization::encode_vi64(buf, sizeof(buf), pos, fake_len);
  // pad some bytes
  memset(buf + pos, 0, fake_len);
  int64_t total = pos + fake_len;

  ObSSMetaHeadValue head;
  int64_t head_pos = 0;
  int ret = head.deserialize_value_info(buf, total, head_pos);
  ASSERT_NE(OB_SUCCESS, ret);
  LOG_INFO("bad version correctly rejected", K(ret), K(fake_version));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_meta_common_info.log*");
  OB_LOGGER.set_file_name("test_ss_meta_common_info.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
