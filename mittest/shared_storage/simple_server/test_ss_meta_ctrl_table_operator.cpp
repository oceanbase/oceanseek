// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;
using namespace storage;

class TestSSMetaCtrlTableOperator : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t SQL_TIMEOUT_US = 10L * 1000L * 1000L; // 10s
  TestSSMetaCtrlTableOperator() : unittest::ObSimpleClusterTestBase("test_ss_meta_ctrl_table_operator") {}

  // helpers
  int prepare_ctrl_key_and_value(const int64_t tablet_id_int = 1);
  int prepare_lock_key(const int64_t tablet_id_int, ObSSMetaCtrlKey &lock_key);
  int build_meta_key_str(const ObSSMetaCtrlKey &ctrl_key,
                         common::ObIAllocator &allocator,
                         common::ObString &meta_key);
  int build_meta_value_str(const ObSSMetaCtrlValue &ctrl_value,
                           common::ObIAllocator &allocator,
                           common::ObString &meta_value);
  int build_lock_row_value_str(const ObSSMetaCtrlLockRowValue &row_value,
                               common::ObIAllocator &allocator,
                               common::ObString &meta_value);

public:
  ObSSMetaCtrlKey ctrl_key_;
  ObSSMetaCtrlValue ctrl_value_;
};

int TestSSMetaCtrlTableOperator::prepare_ctrl_key_and_value(const int64_t tablet_id_int)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id = ObTabletID(tablet_id_int);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const share::SCN meta_scn(share::SCN::min_scn());
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  ctrl_key_.reset();
  ctrl_value_.reset();
  ctrl_key_.assign_type(type);
  ctrl_key_.tablet_key_.tablet_id_ = tablet_id;
  ctrl_key_.tablet_key_.reorg_scn_ = reorg_scn;
  ctrl_key_.tablet_key_.control_scn_ = meta_scn;

  ObSSMetaCtrlInfo ctrl_info;
  ctrl_info.lease_epoch_ = 1;
  ctrl_info.op_id_ = 1;

  ObSSMetaCtrlObjectInfo object_info;
  if (OB_FAIL(ctrl_info.status_.set_status_start())) {
    LOG_WARN("failed to set status start", K(ret), K(ctrl_info));
  } else if (OB_FAIL(object_info.init_by_major(
      share::SCN::base_scn(), 1 /*seq_step*/, 1 /*parallel_cnt*/, 1 /*cg_cnt*/))) {
    LOG_WARN("failed to init object info", K(ret), K(object_info));
  }
  if (OB_SUCC(ret)) {
    ctrl_value_.ctrl_info_ = ctrl_info;
    ctrl_value_.object_info_ = object_info;
    if (!ctrl_key_.is_valid() || !ctrl_value_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prepare ctrl key and value is invalid, unexpected", K(ret),
          K(ctrl_key_),
          K(ctrl_value_));
    }
  }
  return ret;
}

int TestSSMetaCtrlTableOperator::prepare_lock_key(const int64_t tablet_id_int, ObSSMetaCtrlKey &lock_key)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlType type;
  type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  lock_key.reset();
  lock_key.assign_type(type);
  lock_key.tablet_key_.tablet_id_ = ObTabletID(tablet_id_int);
  lock_key.tablet_key_.reorg_scn_ = share::SCN::min_scn();
  lock_key.tablet_key_.control_scn_ = share::SCN::min_scn();
  if (!lock_key.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prepare lock key is invalid, unexpected", K(ret), K(lock_key));
  }
  return ret;
}

int TestSSMetaCtrlTableOperator::build_meta_key_str(
    const ObSSMetaCtrlKey &ctrl_key,
    common::ObIAllocator &allocator,
    common::ObString &meta_key)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(size))) {
    LOG_WARN("failed to get meta key serialize size", K(ret), K(ctrl_key));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(ctrl_key.serialize_meta_key(buf, size, pos))) {
    LOG_WARN("failed to serialize meta key", K(ret), K(ctrl_key));
  } else {
    meta_key.assign(buf, pos);
  }
  return ret;
}

int TestSSMetaCtrlTableOperator::build_meta_value_str(
    const ObSSMetaCtrlValue &ctrl_value,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t size = ctrl_value.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(ctrl_value.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize meta value", K(ret), K(ctrl_value));
  } else {
    meta_value.assign(buf, size);
  }
  return ret;
}

int TestSSMetaCtrlTableOperator::build_lock_row_value_str(
    const ObSSMetaCtrlLockRowValue &row_value,
    common::ObIAllocator &allocator,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t size = row_value.get_serialize_size();
  int64_t pos = 0;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(size));
  } else if (OB_FAIL(row_value.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize lock row value", K(ret), K(row_value));
  } else {
    meta_value.assign(buf, size);
  }
  return ret;
}

TEST_F(TestSSMetaCtrlTableOperator, inner_tablet_meta_key_round_trip)
{
  const ObLSID ls_id(7);
  const SCN min_scn(SCN::min_scn());
  ObSSMetaCtrlKey key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID),
      min_scn,
      min_scn,
      ls_id);
  ASSERT_TRUE(key.is_valid());
  ASSERT_EQ(ls_id.id(), key.fourth_id_);
  ASSERT_TRUE(is_ss_meta_ctrl_ls_id_consistent(ls_id, key));
  ASSERT_FALSE(is_ss_meta_ctrl_ls_id_consistent(ObLSID(8), key));
  ASSERT_EQ(OB_SUCCESS, validate_ss_meta_ctrl_row_key(ls_id, key.type_, key));
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      validate_ss_meta_ctrl_row_key(ObLSID(8), key.type_, key));

  const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id();
  ASSERT_TRUE(macro_id.is_in_control());
  ASSERT_FALSE(macro_id.is_in_table());
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_TRUE(blocksstable::SSObjUtil::is_store_in_table(macro_id));
  ASSERT_TRUE(macro_id.meta_is_inner_tablet());
  ASSERT_EQ(ls_id.id(), macro_id.ls_id());

  char meta_key_buf[1024] = {0};
  int64_t meta_key_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      key.serialize_meta_key(meta_key_buf, sizeof(meta_key_buf), meta_key_pos));
  ASSERT_STREQ("ls/7/TX_DATA/meta/scn00000000000000000000.T20", meta_key_buf);

  ObSSMetaCtrlKey round_trip;
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      round_trip.deserialize_meta_key(meta_key_buf, meta_key_pos, deserialize_pos));
  ASSERT_EQ(meta_key_pos, deserialize_pos);
  ASSERT_EQ(key.type_, round_trip.type_);
  ASSERT_EQ(key.tablet_key_.tablet_id_, round_trip.tablet_key_.tablet_id_);
  ASSERT_EQ(key.tablet_key_.control_scn_, round_trip.tablet_key_.control_scn_);
  ASSERT_EQ(ls_id, round_trip.tablet_key_.ls_id_);
  ASSERT_EQ(ls_id.id(), round_trip.fourth_id_);

  char prefix_buf[1024] = {0};
  int64_t prefix_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      key.serialize_prefix_meta_key(prefix_buf, sizeof(prefix_buf), prefix_pos));
  ASSERT_STREQ("ls/7/TX_DATA/", prefix_buf);

  ObSSMetaCtrlKey lock_key;
  ASSERT_EQ(OB_SUCCESS, key.get_lock_row_key(lock_key));
  ASSERT_TRUE(lock_key.is_valid());
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
      lock_key.type_);
  ASSERT_EQ(ls_id, lock_key.tablet_key_.ls_id_);
  ASSERT_EQ(ls_id.id(), lock_key.fourth_id_);
  ASSERT_TRUE(is_ss_meta_ctrl_ls_id_consistent(ls_id, lock_key));
  ASSERT_FALSE(is_ss_meta_ctrl_ls_id_consistent(ObLSID(8), lock_key));

  char lock_key_buf[1024] = {0};
  int64_t lock_key_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      lock_key.serialize_meta_key(lock_key_buf, sizeof(lock_key_buf), lock_key_pos));
  ASSERT_STREQ("ls/7/TX_DATA/lock/scn00000000000000000000.T37", lock_key_buf);

  ObSSMetaCtrlKey lock_round_trip;
  deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      lock_round_trip.deserialize_meta_key(
          lock_key_buf, lock_key_pos, deserialize_pos));
  ASSERT_EQ(lock_key_pos, deserialize_pos);
  ASSERT_EQ(lock_key.type_, lock_round_trip.type_);
  ASSERT_EQ(lock_key.tablet_key_.tablet_id_,
      lock_round_trip.tablet_key_.tablet_id_);
  ASSERT_EQ(ls_id, lock_round_trip.tablet_key_.ls_id_);

  MEMSET(prefix_buf, 0, sizeof(prefix_buf));
  prefix_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      lock_key.serialize_prefix_meta_key(
          prefix_buf, sizeof(prefix_buf), prefix_pos));
  ASSERT_STREQ("ls/7/TX_DATA/", prefix_buf);
}

TEST_F(TestSSMetaCtrlTableOperator, inner_tablet_sstable_key_round_trip)
{
  const ObLSID ls_id(7);
  const SCN min_scn(SCN::min_scn());
  SCN reorg_scn;
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(42));
  ObSSMetaCtrlKey key;
  key.type_ = blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL;
  key.sstable_key_.tablet_id_ =
      ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID);
  key.sstable_key_.reorg_scn_ = reorg_scn;
  key.sstable_key_.control_scn_ = min_scn;
  key.sstable_key_.ls_id_ = ls_id;
  ASSERT_TRUE(key.is_valid());
  ASSERT_TRUE(is_ss_meta_ctrl_ls_id_consistent(ls_id, key));
  ASSERT_FALSE(is_ss_meta_ctrl_ls_id_consistent(ObLSID(8), key));

  char key_buf[1024] = {0};
  int64_t key_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
  ASSERT_STREQ(
      "ls/7/TX_DATA/meta/scn00000000000000000000.T34", key_buf);

  ObSSMetaCtrlKey round_trip;
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      round_trip.deserialize_meta_key(key_buf, key_pos, deserialize_pos));
  ASSERT_EQ(key_pos, deserialize_pos);
  ASSERT_EQ(key.type_, round_trip.type_);
  ASSERT_EQ(key.sstable_key_.tablet_id_,
      round_trip.sstable_key_.tablet_id_);
  ASSERT_EQ(key.sstable_key_.control_scn_,
      round_trip.sstable_key_.control_scn_);
  ASSERT_TRUE(round_trip.sstable_key_.reorg_scn_.is_min());
  ASSERT_EQ(ls_id, round_trip.sstable_key_.ls_id_);

  ObSSMetaCtrlKey lock_key;
  ASSERT_EQ(OB_SUCCESS, key.get_lock_row_key(lock_key));
  ASSERT_TRUE(lock_key.is_valid());
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_MAJOR_LOCK,
      lock_key.type_);
  ASSERT_EQ(ls_id, lock_key.sstable_key_.ls_id_);

  MEMSET(key_buf, 0, sizeof(key_buf));
  key_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      lock_key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
  ASSERT_STREQ(
      "ls/7/TX_DATA/lock/scn00000000000000000000.T40", key_buf);

  ObSSMetaCtrlKey lock_round_trip;
  int64_t lock_deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      lock_round_trip.deserialize_meta_key(
          key_buf, key_pos, lock_deserialize_pos));
  ASSERT_EQ(key_pos, lock_deserialize_pos);
  ASSERT_EQ(lock_key.type_, lock_round_trip.type_);
  ASSERT_EQ(lock_key.sstable_key_.tablet_id_,
      lock_round_trip.sstable_key_.tablet_id_);
  ASSERT_EQ(ls_id, lock_round_trip.sstable_key_.ls_id_);

  MEMSET(key_buf, 0, sizeof(key_buf));
  key_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      lock_key.serialize_prefix_meta_key(key_buf, sizeof(key_buf), key_pos));
  ASSERT_STREQ("ls/7/TX_DATA/", key_buf);
}

// 覆盖 max SCN 边界下 user tablet 与 LS 两类 control key。
// 每组依次校验 MacroBlockId 合法性、无前导斜杠的完整 key、value/lock
// 公共扫描前缀，以及 value/lock key 的反序列化回环。
// value key 保留 max control SCN；lock key 的 control SCN 固定为 min。
TEST_F(TestSSMetaCtrlTableOperator, tx_scn_round_trip)
{
  const SCN max_scn = SCN::max_scn();
  const ObLSID ls_id(7);

  {
    // 场景一：用户 tablet 的 TABLET_META / TABLET_LOCK。
    ObSSMetaCtrlKey key(
        blocksstable::ObStorageObjectType::SHARED_TABLET_META,
        ObTabletID(1001),
        max_scn,
        max_scn,
        ls_id);
    ASSERT_TRUE(key.is_valid());

    const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id();
    ASSERT_TRUE(macro_id.is_in_control());
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(INT64_MAX, macro_id.third_id());
    ASSERT_EQ(INT64_MAX, macro_id.reorganization_scn());
    ASSERT_EQ(INT64_MAX, macro_id.meta_scn());

    char key_buf[1024] = {0};
    int64_t key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
    ASSERT_STREQ(
        "tablet/1001/9223372036854775807/meta/"
        "scn09223372036854775807.T20",
        key_buf);

    char prefix_buf[1024] = {0};
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_prefix_meta_key(
            prefix_buf, sizeof(prefix_buf), prefix_pos));
    ASSERT_STREQ("tablet/1001/9223372036854775807/", prefix_buf);
    ASSERT_EQ(0, MEMCMP(key_buf, prefix_buf, prefix_pos));

    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, key.get_lock_row_key(lock_key));
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
        lock_key.type_);
    MEMSET(prefix_buf, 0, sizeof(prefix_buf));
    prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_prefix_meta_key(
            prefix_buf, sizeof(prefix_buf), prefix_pos));
    ASSERT_STREQ("tablet/1001/9223372036854775807/", prefix_buf);
    ASSERT_EQ(0, MEMCMP(key_buf, prefix_buf, prefix_pos));

    ObSSMetaCtrlKey round_trip;
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        round_trip.deserialize_meta_key(key_buf, key_pos, deserialize_pos));
    ASSERT_EQ(key_pos, deserialize_pos);
    ASSERT_TRUE(round_trip.tablet_key_.reorg_scn_.is_max());
    ASSERT_TRUE(round_trip.tablet_key_.control_scn_.is_max());
  }

  {
    // 场景二：用户 tablet 的 MINOR_SSTABLE_CTRL / MINOR_LOCK。
    ObSSMetaCtrlKey key(
        blocksstable::ObStorageObjectType::SHARED_MINOR_SSTABLE_CTRL,
        ObTabletID(1002),
        max_scn,
        max_scn,
        ls_id);
    ASSERT_TRUE(key.is_valid());

    const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id();
    ASSERT_TRUE(macro_id.is_in_control());
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(INT64_MAX, macro_id.third_id());
    ASSERT_EQ(INT64_MAX, macro_id.reorganization_scn());
    ASSERT_EQ(INT64_MAX, macro_id.meta_scn());
    blocksstable::MacroBlockId table_macro_id = macro_id;
    table_macro_id.set_location(blocksstable::ObMacroBlockIdLocation::TABLE);
    ASSERT_FALSE(table_macro_id.is_valid());
    blocksstable::MacroBlockId non_ctrl_macro_id = macro_id;
    non_ctrl_macro_id.set_storage_object_type(
        static_cast<uint64_t>(
            blocksstable::ObStorageObjectType::SHARED_TABLET_SUB_META));
    ASSERT_FALSE(non_ctrl_macro_id.is_valid());

    char key_buf[1024] = {0};
    int64_t key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
    ASSERT_STREQ(
        "tablet/1002/9223372036854775807/meta/"
        "scn09223372036854775807.T33",
        key_buf);

    ObSSMetaCtrlKey round_trip;
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        round_trip.deserialize_meta_key(key_buf, key_pos, deserialize_pos));
    ASSERT_EQ(key_pos, deserialize_pos);
    ASSERT_TRUE(round_trip.sstable_key_.reorg_scn_.is_max());
    ASSERT_TRUE(round_trip.sstable_key_.control_scn_.is_max());

    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, key.get_lock_row_key(lock_key));
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_MINOR_LOCK,
        lock_key.type_);
    MEMSET(key_buf, 0, sizeof(key_buf));
    key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
    ASSERT_STREQ(
        "tablet/1002/9223372036854775807/lock/"
        "scn00000000000000000000.T39",
        key_buf);

    ObSSMetaCtrlKey lock_round_trip;
    int64_t lock_deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_round_trip.deserialize_meta_key(
            key_buf, key_pos, lock_deserialize_pos));
    ASSERT_EQ(key_pos, lock_deserialize_pos);
    ASSERT_EQ(lock_key.type_, lock_round_trip.type_);
    ASSERT_TRUE(lock_round_trip.sstable_key_.reorg_scn_.is_max());
    ASSERT_TRUE(lock_round_trip.sstable_key_.control_scn_.is_min());

    MEMSET(key_buf, 0, sizeof(key_buf));
    key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_prefix_meta_key(
            key_buf, sizeof(key_buf), key_pos));
    ASSERT_STREQ("tablet/1002/9223372036854775807/", key_buf);
  }

  {
    // 场景三：LS checkpoint 的 SHARED_LS_CKPT / SHARED_LS_CKPT_LOCK。
    ObSSMetaCtrlKey key;
    key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_CKPT;
    key.ls_key_.first_id_ = ls_id.id();
    key.ls_key_.second_id_ = 0;
    key.ls_key_.control_scn_ = max_scn;
    ASSERT_TRUE(key.is_valid());

    const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id();
    ASSERT_TRUE(macro_id.is_in_control());
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(0, macro_id.third_id());
    ASSERT_EQ(INT64_MAX, macro_id.meta_scn());

    char key_buf[1024] = {0};
    int64_t key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
    ASSERT_STREQ(
        "ls/7/meta/scn09223372036854775807.T30",
        key_buf);

    char prefix_buf[1024] = {0};
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_prefix_meta_key(
            prefix_buf, sizeof(prefix_buf), prefix_pos));
    ASSERT_STREQ("ls/7/", prefix_buf);
    ASSERT_EQ(0, MEMCMP(key_buf, prefix_buf, prefix_pos));

    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, key.get_lock_row_key(lock_key));
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_CKPT_LOCK,
        lock_key.type_);
    char lock_key_buf[1024] = {0};
    int64_t lock_key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_meta_key(
            lock_key_buf, sizeof(lock_key_buf), lock_key_pos));
    ASSERT_STREQ(
        "ls/7/lock/scn00000000000000000000.T38",
        lock_key_buf);
    ObSSMetaCtrlKey lock_round_trip;
    int64_t lock_deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_round_trip.deserialize_meta_key(
            lock_key_buf, lock_key_pos, lock_deserialize_pos));
    ASSERT_EQ(lock_key_pos, lock_deserialize_pos);
    ASSERT_EQ(lock_key.type_, lock_round_trip.type_);
    ASSERT_TRUE(lock_round_trip.ls_key_.control_scn_.is_min());

    MEMSET(prefix_buf, 0, sizeof(prefix_buf));
    prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_prefix_meta_key(
            prefix_buf, sizeof(prefix_buf), prefix_pos));
    ASSERT_STREQ("ls/7/", prefix_buf);
    ASSERT_EQ(0, MEMCMP(key_buf, prefix_buf, prefix_pos));
    ASSERT_EQ(0, MEMCMP(lock_key_buf, prefix_buf, prefix_pos));

    ObSSMetaCtrlKey round_trip;
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        round_trip.deserialize_meta_key(key_buf, key_pos, deserialize_pos));
    ASSERT_EQ(key_pos, deserialize_pos);
    ASSERT_EQ(key.type_, round_trip.type_);
    ASSERT_TRUE(round_trip.ls_key_.control_scn_.is_max());
  }

  {
    // 场景四：LS 元数据的 SHARED_LS_META / SHARED_LS_LOCK。
    ObSSMetaCtrlKey key;
    key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_META;
    key.ls_key_.first_id_ = ls_id.id();
    key.ls_key_.second_id_ = 0;
    key.ls_key_.control_scn_ = max_scn;
    ASSERT_TRUE(key.is_valid());

    const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id();
    ASSERT_TRUE(macro_id.is_in_control());
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(0, macro_id.third_id());
    ASSERT_EQ(INT64_MAX, macro_id.meta_scn());

    char key_buf[1024] = {0};
    int64_t key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_meta_key(key_buf, sizeof(key_buf), key_pos));
    ASSERT_STREQ(
        "ls/7/meta/scn09223372036854775807.T31",
        key_buf);

    char prefix_buf[1024] = {0};
    int64_t prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        key.serialize_prefix_meta_key(
            prefix_buf, sizeof(prefix_buf), prefix_pos));
    ASSERT_STREQ("ls/7/", prefix_buf);
    ASSERT_EQ(0, MEMCMP(key_buf, prefix_buf, prefix_pos));

    ObSSMetaCtrlKey lock_key;
    ASSERT_EQ(OB_SUCCESS, key.get_lock_row_key(lock_key));
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_LOCK,
        lock_key.type_);
    char lock_key_buf[1024] = {0};
    int64_t lock_key_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_meta_key(
            lock_key_buf, sizeof(lock_key_buf), lock_key_pos));
    ASSERT_STREQ(
        "ls/7/lock/scn00000000000000000000.T36",
        lock_key_buf);
    ObSSMetaCtrlKey lock_round_trip;
    int64_t lock_deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_round_trip.deserialize_meta_key(
            lock_key_buf, lock_key_pos, lock_deserialize_pos));
    ASSERT_EQ(lock_key_pos, lock_deserialize_pos);
    ASSERT_EQ(lock_key.type_, lock_round_trip.type_);
    ASSERT_TRUE(lock_round_trip.ls_key_.control_scn_.is_min());

    MEMSET(prefix_buf, 0, sizeof(prefix_buf));
    prefix_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        lock_key.serialize_prefix_meta_key(
            prefix_buf, sizeof(prefix_buf), prefix_pos));
    ASSERT_STREQ("ls/7/", prefix_buf);
    ASSERT_EQ(0, MEMCMP(key_buf, prefix_buf, prefix_pos));
    ASSERT_EQ(0, MEMCMP(lock_key_buf, prefix_buf, prefix_pos));

    ObSSMetaCtrlKey round_trip;
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        round_trip.deserialize_meta_key(key_buf, key_pos, deserialize_pos));
    ASSERT_EQ(key_pos, deserialize_pos);
    ASSERT_EQ(key.type_, round_trip.type_);
    ASSERT_TRUE(round_trip.ls_key_.control_scn_.is_max());
  }
}

TEST_F(TestSSMetaCtrlTableOperator, reject_oversized_inner_tablet_name)
{
  static constexpr int64_t OVERSIZED_TABLET_NAME_LEN = 600;
  const char *path_tails[] = {
      "/meta/scn00000000000000000000.T20",
      "/lock/scn00000000000000000000.T39",
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(path_tails); ++i) {
    char path[SSMETA_PATH_MAX_LEN] = {0};
    int64_t path_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        databuff_printf(path, sizeof(path), path_pos, "ls/7/"));
    ASSERT_LE(path_pos + OVERSIZED_TABLET_NAME_LEN, sizeof(path));
    MEMSET(path + path_pos, 'A', OVERSIZED_TABLET_NAME_LEN);
    path_pos += OVERSIZED_TABLET_NAME_LEN;
    ASSERT_EQ(OB_SUCCESS,
        databuff_printf(path, sizeof(path), path_pos, "%s", path_tails[i]));

    ObSSMetaCtrlKey key;
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_UNEXPECTED_MACRO_CACHE_FILE,
        key.deserialize_meta_key(path, path_pos, deserialize_pos));
    ASSERT_EQ(0, deserialize_pos);
  }
}

TEST_F(TestSSMetaCtrlTableOperator, insert_row)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value());

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  LOG_INFO("meta key is", K(meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  int64_t affected_rows = 0;
  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.insert_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);


  //get row
  int64_t pos = 0;
  ObSSMetaCtrlValue new_ctrl_value;
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_ctrl_value.deserialize(meta_value.ptr(), meta_value.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("meta value is", K(new_ctrl_value));
  ASSERT_EQ(new_ctrl_value, ctrl_value_);
}

TEST_F(TestSSMetaCtrlTableOperator, update_row)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value());

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  LOG_INFO("meta key is", K(meta_key));
  ASSERT_EQ(OB_SUCCESS, ctrl_value_.ctrl_info_.status_.set_status_complete());
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  int64_t affected_rows = 0;
  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.update_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);
  //get row
  int64_t pos = 0;
  ObSSMetaCtrlValue new_ctrl_value;
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_ctrl_value.deserialize(meta_value.ptr(), meta_value.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("meta value is", K(new_ctrl_value));
  ASSERT_EQ(new_ctrl_value, ctrl_value_);
}


TEST_F(TestSSMetaCtrlTableOperator, delete_row)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value());
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  LOG_INFO("meta key is", K(meta_key));
  ASSERT_EQ(OB_SUCCESS, ctrl_value_.ctrl_info_.status_.set_status_complete());
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  const bool prefix_delete = false;
  int64_t affected_rows = 0;
  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.delete_row(tenant_id, ls, ctrl_key_.type_, meta_key, prefix_delete, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  //get row
  ObSSMetaCtrlValue new_ctrl_value;
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_ITER_END, ret);
}

// read a row that does not exist => OB_ITER_END
TEST_F(TestSSMetaCtrlTableOperator, read_non_existing_row)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(101));

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));

  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_ITER_END, ret);
}

// update a row that does not exist => OB_SUCCESS, affected_rows = 0
TEST_F(TestSSMetaCtrlTableOperator, update_non_existing_row)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(102));

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  int64_t affected_rows = -1;
  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.update_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, affected_rows);
}

// delete a row that does not exist => OB_SUCCESS, affected_rows = 0
TEST_F(TestSSMetaCtrlTableOperator, delete_non_existing_row)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(103));

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));

  int64_t affected_rows = -1;
  const bool prefix_delete = false;
  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.delete_row(tenant_id, ls, ctrl_key_.type_, meta_key, prefix_delete, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, affected_rows);
}

// insert multiple rows that share a key prefix, then prefix-delete should remove them all
TEST_F(TestSSMetaCtrlTableOperator, delete_with_prefix)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;

  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);

  // insert several rows under the same (tablet_id, reorg_scn) with different meta_scn.
  // append_meta_key_like_prefix_ matches meta_key LIKE '<prefix>%' (anchored at start),
  // so the prefix must be a leading substring of the serialized key, not e.g. id_4:...
  const int64_t tablet_id_int = 200;
  share::SCN reorg_scn;
  reorg_scn.set_min();
  share::SCN meta_scn_a;
  meta_scn_a.set_min();
  share::SCN meta_scn_b;
  meta_scn_b.set_base();
  share::SCN meta_scn_c;
  ASSERT_EQ(OB_SUCCESS, meta_scn_c.convert_for_tx(42));
  const share::SCN meta_scns[] = {meta_scn_a, meta_scn_b, meta_scn_c};
  ObSSMetaCtrlType meta_type;
  meta_type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ctrl_key_.tablet_key_.reorg_scn_ = reorg_scn;
    ctrl_key_.tablet_key_.control_scn_ = meta_scns[i];
    ObString meta_key;
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));
    int64_t affected_rows = 0;
    ret = table_operator.insert_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, affected_rows);
  }

  // prefix_delete via LIKE 'id_1:<tablet>;id_2:<reorg>;%' (serialize_prefix_meta_key).
  ObSSMetaCtrlKey prefix_key;
  prefix_key.assign_type(meta_type);
  prefix_key.tablet_key_.tablet_id_ = ObTabletID(tablet_id_int);
  prefix_key.tablet_key_.reorg_scn_ = reorg_scn;
  prefix_key.tablet_key_.control_scn_.set_min();
  int64_t prefix_buf_size = 0;
  int64_t prefix_pos = 0;
  char *prefix_buf = nullptr;
  ObString prefix_meta_key;
  ASSERT_EQ(OB_SUCCESS, prefix_key.get_prefix_meta_key_serialize_size(prefix_buf_size));
  prefix_buf = static_cast<char *>(allocator.alloc(prefix_buf_size));
  ASSERT_NE(nullptr, prefix_buf);
  ASSERT_EQ(OB_SUCCESS, prefix_key.serialize_prefix_meta_key(prefix_buf, prefix_buf_size, prefix_pos));
  prefix_meta_key.assign(prefix_buf, prefix_pos);

  int64_t affected_rows = 0;
  const bool prefix_delete = true;
  ret = table_operator.delete_row(tenant_id, ls, meta_type.type_, prefix_meta_key, prefix_delete, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_GE(affected_rows, 3);

  // verify each row no longer exists
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(tablet_id_int));
    ctrl_key_.tablet_key_.reorg_scn_ = reorg_scn;
    ctrl_key_.tablet_key_.control_scn_ = meta_scns[i];
    ObString meta_key;
    ObString meta_value;
    ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
    ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

// lock_row needs to run inside a transaction since it issues SELECT ... FOR UPDATE.
// Insert a TABLET_LOCK_ROW record first via mysql_proxy, then issue lock_row from
// inside an ObMySQLTransaction and verify the value can be deserialized back.
TEST_F(TestSSMetaCtrlTableOperator, lock_row)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;

  ObSSMetaCtrlKey lock_key;
  ASSERT_EQ(OB_SUCCESS, prepare_lock_key(300, lock_key));
  ObString meta_key;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(lock_key, allocator, meta_key));

  ObSSMetaCtrlLockRowValue lock_row_value;
  lock_row_value.is_mark_deleted_ = false;
  lock_row_value.op_id_ = 7;
  lock_row_value.meta_scn_.set_min();
  lock_row_value.deleted_scn_.reset();
  ASSERT_TRUE(lock_row_value.is_valid());
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_lock_row_value_str(lock_row_value, allocator, meta_value));

  // 1. insert lock row using the global mysql proxy
  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t affected_rows = 0;
  ret = table_operator.insert_row(tenant_id, ls, lock_key.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // 2. open a transaction and lock the row inside it
  ObMySQLTransaction trans;
  ret = trans.start(&get_curr_simple_server().get_observer().get_mysql_proxy(), tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaCtrlTableOperator trans_table_operator;
  ret = trans_table_operator.init(trans, SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObString locked_value;
  ret = trans_table_operator.lock_row(tenant_id, ls, lock_key.type_, meta_key, allocator, locked_value);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaCtrlLockRowValue read_back_value;
  int64_t pos = 0;
  ret = read_back_value.deserialize(locked_value.ptr(), locked_value.length(), pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_row_value.is_mark_deleted_, read_back_value.is_mark_deleted_);
  ASSERT_EQ(lock_row_value.op_id_, read_back_value.op_id_);
  ASSERT_EQ(lock_row_value.meta_scn_, read_back_value.meta_scn_);

  // 3. commit the transaction
  ret = trans.end(true /*commit*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. lock_row rejects non-lock-row meta types
  ObMySQLTransaction trans2;
  ret = trans2.start(&get_curr_simple_server().get_observer().get_mysql_proxy(), tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSSMetaCtrlTableOperator trans_op2;
  ASSERT_EQ(OB_SUCCESS, trans_op2.init(trans2, SQL_TIMEOUT_US));
  ObSSMetaCtrlType non_lock_type;
  non_lock_type.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  ObString tmp_value;
  ret = trans_op2.lock_row(tenant_id, ls, non_lock_type.type_, meta_key, allocator, tmp_value);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  (void)trans2.end(false /*rollback*/);

  // 5. cleanup
  ret = table_operator.delete_row(tenant_id, ls, lock_key.type_, meta_key, false /*prefix_delete*/, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
}

// Operations should fail before init() and init() must be idempotent only when not inited.
TEST_F(TestSSMetaCtrlTableOperator, not_init_and_init_twice)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(400));

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  ObSSMetaCtrlTableOperator table_operator;
  int64_t affected_rows = 0;
  ret = table_operator.insert_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = table_operator.update_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = table_operator.delete_row(tenant_id, ls, ctrl_key_.type_, meta_key, false, affected_rows);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_NOT_INIT, ret);

  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_INIT_TWICE, ret);
}

// All public DML APIs should reject obviously invalid arguments.
TEST_F(TestSSMetaCtrlTableOperator, invalid_argument)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(500));

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));
  ObString empty_key;

  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t affected_rows = 0;
  // invalid tenant_id
  ret = table_operator.insert_row(OB_INVALID_ID, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.update_row(OB_INVALID_ID, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.delete_row(OB_INVALID_ID, ls, ctrl_key_.type_, meta_key, false, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.read_row(OB_INVALID_ID, ls, ctrl_key_.type_, meta_key, allocator, meta_value);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // invalid meta_type (default constructed enum is INVALID_META)
  ObSSMetaCtrlType invalid_type;
  ret = table_operator.insert_row(tenant_id, ls, invalid_type.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // empty meta_key
  ret = table_operator.insert_row(tenant_id, ls, ctrl_key_.type_, empty_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.update_row(tenant_id, ls, ctrl_key_.type_, empty_key, meta_value, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.delete_row(tenant_id, ls, ctrl_key_.type_, empty_key, false, affected_rows);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, empty_key, allocator, meta_value);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = table_operator.lock_row(tenant_id, ls, ctrl_key_.type_, empty_key, allocator, meta_value);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// An out-of-range SQL value must be rejected before conversion to the
// uint8_t-backed enum. Without the range check, adding 256 to a valid enum
// value narrows back to that value and is incorrectly accepted.
TEST_F(TestSSMetaCtrlTableOperator, reject_out_of_range_meta_type_from_result)
{
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  const uint64_t tenant_id = 1;
  const ObLSID ls(700001);
  const int64_t out_of_range_type =
      static_cast<int64_t>(blocksstable::ObStorageObjectType::SHARED_TABLET_META) + 256;
  ObSqlString cleanup_sql;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;

  ASSERT_GE(out_of_range_type,
      static_cast<int64_t>(blocksstable::ObStorageObjectType::MAX));
  ASSERT_EQ(OB_SUCCESS, cleanup_sql.assign_fmt(
      "DELETE FROM %s WHERE tenant_id = %lu AND ls_id = %ld",
      share::OB_ALL_SHARED_META_CONTROL_TNAME, tenant_id, ls.id()));
  ASSERT_EQ(OB_SUCCESS, proxy.write(tenant_id, cleanup_sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, insert_sql.assign_fmt(
      "INSERT INTO %s (tenant_id, ls_id, meta_type, meta_key, meta_value) "
      "VALUES (%lu, %ld, %ld, 'out_of_range_meta_type', X'')",
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id,
      ls.id(),
      out_of_range_type));
  ASSERT_EQ(OB_SUCCESS, proxy.write(tenant_id, insert_sql.ptr(), affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObSSMetaCtrlTableIterator iter;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::WHOLE_VALUE);
  const int init_ret = iter.init(proxy, SQL_TIMEOUT_US, tenant_id, ls, iter_type, ObString());
  int parse_ret = init_ret;
  if (OB_SUCCESS == init_ret) {
    ObArenaAllocator allocator;
    uint64_t actual_tenant_id = OB_INVALID_ID;
    ObLSID actual_ls;
    blocksstable::ObStorageObjectType actual_type = blocksstable::ObStorageObjectType::MAX;
    ObString actual_key;
    ObString actual_value;
    parse_ret = iter.get_next(
        allocator, actual_tenant_id, actual_ls, actual_type, actual_key, actual_value);
  }

  const int cleanup_ret = proxy.write(tenant_id, cleanup_sql.ptr(), affected_rows);
  ASSERT_EQ(OB_SUCCESS, init_ret);
  ASSERT_EQ(OB_SUCCESS, cleanup_ret);
  ASSERT_EQ(OB_ERR_UNEXPECTED, parse_ret);
}

// One-shot CRUD lifecycle on a key dedicated to this test, with no cross-test dependency.
TEST_F(TestSSMetaCtrlTableOperator, full_lifecycle)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, prepare_ctrl_key_and_value(600));

  const uint64_t tenant_id = 1;
  const ObLSID ls = ObLSID(1);
  ObArenaAllocator allocator;
  ObString meta_key;
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_key_str(ctrl_key_, allocator, meta_key));
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, meta_value));

  ObSSMetaCtrlTableOperator table_operator;
  ret = table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(), SQL_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t affected_rows = 0;
  // insert
  ret = table_operator.insert_row(tenant_id, ls, ctrl_key_.type_, meta_key, meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // read back
  {
    ObString read_value;
    ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, read_value);
    ASSERT_EQ(OB_SUCCESS, ret);
    int64_t pos = 0;
    ObSSMetaCtrlValue v;
    ASSERT_EQ(OB_SUCCESS, v.deserialize(read_value.ptr(), read_value.length(), pos));
    ASSERT_EQ(v, ctrl_value_);
  }

  // update
  ASSERT_EQ(OB_SUCCESS, ctrl_value_.ctrl_info_.status_.set_status_complete());
  ctrl_value_.ctrl_info_.op_id_ = 99;
  ObString new_meta_value;
  ASSERT_EQ(OB_SUCCESS, build_meta_value_str(ctrl_value_, allocator, new_meta_value));
  ret = table_operator.update_row(tenant_id, ls, ctrl_key_.type_, meta_key, new_meta_value, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // read back updated value
  {
    ObString read_value;
    ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, read_value);
    ASSERT_EQ(OB_SUCCESS, ret);
    int64_t pos = 0;
    ObSSMetaCtrlValue v;
    ASSERT_EQ(OB_SUCCESS, v.deserialize(read_value.ptr(), read_value.length(), pos));
    ASSERT_EQ(v, ctrl_value_);
    ASSERT_EQ(99, v.ctrl_info_.op_id_);
    ASSERT_TRUE(v.ctrl_info_.status_.is_complete_status());
  }

  // delete
  ret = table_operator.delete_row(tenant_id, ls, ctrl_key_.type_, meta_key, false, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  // gone
  ObString gone_value;
  ret = table_operator.read_row(tenant_id, ls, ctrl_key_.type_, meta_key, allocator, gone_value);
  ASSERT_EQ(OB_ITER_END, ret);
}

TEST_F(TestSSMetaCtrlTableOperator, delete_rows_from_new_primary_key)
{
  const uint64_t tenant_id = 1;
  const ObLSID high_type_ls(910001);
  const ObLSID low_type_ls(910003);
  const blocksstable::ObStorageObjectType high_type =
      blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_LOCK;
  const blocksstable::ObStorageObjectType low_type =
      blocksstable::ObStorageObjectType::SHARED_TABLET_META;
  const ObString high_key = ObString::make_string("range-delete-high-type");
  const ObString low_key = ObString::make_string("range-delete-low-type");
  const ObString meta_value = ObString::make_string("range-delete-value");
  ObSSMetaCtrlTableOperator table_operator;
  ASSERT_EQ(OB_SUCCESS,
      table_operator.init(get_curr_simple_server().get_observer().get_mysql_proxy(),
                          SQL_TIMEOUT_US));

  // 清理上次异常退出可能留下的同名测试数据。
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
      table_operator.delete_row(tenant_id, high_type_ls, high_type, high_key,
                                false /*prefix_delete*/, affected_rows));
  ASSERT_EQ(OB_SUCCESS,
      table_operator.delete_row(tenant_id, low_type_ls, low_type, low_key,
                                false /*prefix_delete*/, affected_rows));

  ASSERT_EQ(OB_SUCCESS,
      table_operator.insert_row(tenant_id, high_type_ls, high_type,
                                high_key, meta_value, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS,
      table_operator.insert_row(tenant_id, low_type_ls, low_type,
                                low_key, meta_value, affected_rows));
  ASSERT_EQ(1, affected_rows);

  // high_type 是最大的有效 meta_type，因此新主键范围中只有 high_key；
  // low_key 的 ls_id 虽更大，但 meta_type 更小，不应被删除。
  ASSERT_EQ(OB_SUCCESS,
      table_operator.delete_rows_from_rowkey(tenant_id, high_type,
                                             high_type_ls, high_key,
                                             2 /*delete_limit_count*/,
                                             affected_rows));
  ASSERT_EQ(1, affected_rows);

  ObArenaAllocator allocator("MetaDeleteTest");
  ObString read_value;
  ASSERT_EQ(OB_ITER_END,
      table_operator.read_row(tenant_id, high_type_ls, high_type,
                              high_key, allocator, read_value));
  ASSERT_EQ(OB_SUCCESS,
      table_operator.read_row(tenant_id, low_type_ls, low_type,
                              low_key, allocator, read_value));
  ASSERT_EQ(meta_value, read_value);

  ASSERT_EQ(OB_SUCCESS,
      table_operator.delete_row(tenant_id, low_type_ls, low_type, low_key,
                                false /*prefix_delete*/, affected_rows));
  ASSERT_EQ(1, affected_rows);
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
