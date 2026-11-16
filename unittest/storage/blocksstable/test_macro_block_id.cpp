/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace unittest
{
namespace
{
constexpr uint64_t COLUMN_GROUP_ID_SHIFT = MacroBlockId::SF_BIT_STORAGE_OBJECT_TYPE;
constexpr uint64_t LOCATION_SHIFT = COLUMN_GROUP_ID_SHIFT + MacroBlockId::SF_BIT_COLUMN_GROUP_ID;
constexpr uint64_t ID_MODE_SHIFT =
    LOCATION_SHIFT + MacroBlockId::SF_BIT_LOCATION + MacroBlockId::SF_BIT_RESERVED;
constexpr uint64_t VERSION_SHIFT = ID_MODE_SHIFT + MacroBlockId::SF_BIT_ID_MODE;
}

class TestMacroBlockId : public ::testing::Test
{
public:
  TestMacroBlockId() = default;
  void SetUp() {}
  void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

TEST_F(TestMacroBlockId, first_id_layout_and_size)
{
  const uint64_t sf_bit_id_mode = MacroBlockId::SF_BIT_ID_MODE;
  const uint64_t sf_bit_location = MacroBlockId::SF_BIT_LOCATION;
  const uint64_t sf_bit_reserved = MacroBlockId::SF_BIT_RESERVED;
  const int64_t macro_block_id_version = MacroBlockId::MACRO_BLOCK_ID_VERSION_V1;
  ASSERT_EQ(40, sizeof(MacroBlockId));
  ASSERT_EQ(4, sf_bit_id_mode);
  ASSERT_EQ(2, sf_bit_location);
  ASSERT_EQ(30, sf_bit_reserved);
  ASSERT_EQ(8, COLUMN_GROUP_ID_SHIFT);
  ASSERT_EQ(24, LOCATION_SHIFT);
  ASSERT_EQ(56, ID_MODE_SHIFT);
  ASSERT_EQ(60, VERSION_SHIFT);

  MacroBlockId id;
  const uint64_t object_type = static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_META);
  const uint64_t column_group_id = 0x1234;
  id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(object_type);
  id.set_column_group_id(column_group_id);
  id.set_location(ObMacroBlockIdLocation::TABLE);
  id.set_second_id(1);

  const uint64_t expected_first_id = object_type
      | (column_group_id << COLUMN_GROUP_ID_SHIFT)
      | (static_cast<uint64_t>(ObMacroBlockIdLocation::TABLE) << LOCATION_SHIFT)
      | (static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE) << ID_MODE_SHIFT)
      | (static_cast<uint64_t>(macro_block_id_version) << VERSION_SHIFT);
  ASSERT_EQ(expected_first_id, static_cast<uint64_t>(id.first_id()));
  ASSERT_EQ(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE), id.id_mode());
  ASSERT_EQ(0, id.ss_reserved_);
  ASSERT_EQ(macro_block_id_version, id.version_);
  ASSERT_EQ(ObMacroBlockIdLocation::TABLE, id.location());
  ASSERT_TRUE(id.is_in_table());
  ASSERT_FALSE(id.is_in_control());
  ASSERT_FALSE(id.is_in_object_storage());
  ASSERT_TRUE(id.is_valid());

  id.set_location(ObMacroBlockIdLocation::CONTROL);
  const uint64_t expected_control_first_id = object_type
      | (column_group_id << COLUMN_GROUP_ID_SHIFT)
      | (static_cast<uint64_t>(ObMacroBlockIdLocation::CONTROL) << LOCATION_SHIFT)
      | (static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE) << ID_MODE_SHIFT)
      | (static_cast<uint64_t>(macro_block_id_version) << VERSION_SHIFT);
  ASSERT_EQ(expected_control_first_id, static_cast<uint64_t>(id.first_id()));
  ASSERT_EQ(ObMacroBlockIdLocation::CONTROL, id.location());
  ASSERT_FALSE(id.is_in_table());
  ASSERT_TRUE(id.is_in_control());
  ASSERT_FALSE(id.is_in_object_storage());
  ASSERT_TRUE(id.is_valid());

  id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  ASSERT_EQ(expected_first_id ^ (1UL << LOCATION_SHIFT), static_cast<uint64_t>(id.first_id()));
  ASSERT_EQ(ObMacroBlockIdLocation::OBJECT_STORAGE, id.location());
  ASSERT_FALSE(id.is_in_table());
  ASSERT_FALSE(id.is_in_control());
  ASSERT_TRUE(id.is_in_object_storage());
  ASSERT_TRUE(id.is_valid());
}

TEST_F(TestMacroBlockId, id_mode_classification)
{
  MacroBlockId local_id(7, 1001, 0);
  ASSERT_TRUE(local_id.is_local_id());
  ASSERT_FALSE(local_id.is_backup_id());
  ASSERT_FALSE(local_id.is_id_mode_share());
  ASSERT_TRUE(local_id.is_valid());
  local_id.set_seq_id(1);
  ASSERT_FALSE(local_id.is_valid());
  local_id.set_seq_id(0);
  ASSERT_TRUE(local_id.is_valid());
  ASSERT_EQ(7, local_id.write_seq());
  ASSERT_TRUE(SSObjUtil::is_support_sn(local_id));
  ASSERT_FALSE(SSObjUtil::is_private(local_id));
  ASSERT_FALSE(SSObjUtil::is_macro_data(local_id));

  char local_buf[MacroBlockId::SERIALIZED_SIZE] = {};
  int64_t local_pos = 0;
  ASSERT_EQ(OB_SUCCESS, local_id.serialize(local_buf, sizeof(local_buf), local_pos));
  ASSERT_EQ(MacroBlockId::SERIALIZED_SIZE, local_pos);
  MacroBlockId decoded_local_id;
  int64_t local_decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_local_id.deserialize(local_buf, local_pos, local_decode_pos));
  ASSERT_EQ(local_pos, local_decode_pos);
  ASSERT_EQ(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_LOCAL), decoded_local_id.id_mode());
  ASSERT_TRUE(decoded_local_id.is_local_id());
  ASSERT_EQ(7, decoded_local_id.write_seq());
  ASSERT_EQ(0, decoded_local_id.fifth_id());

  MacroBlockId shared_id;
  shared_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  shared_id.set_second_id(1001);
  shared_id.set_reorganization_scn(1002);
  shared_id.set_op_id(1003);
  shared_id.set_seq_id(1004);
  ASSERT_FALSE(shared_id.is_local_id());
  ASSERT_FALSE(shared_id.is_backup_id());
  ASSERT_TRUE(shared_id.is_id_mode_share());
  ASSERT_TRUE(shared_id.is_valid());
  ASSERT_TRUE(SSObjUtil::is_shared(shared_id));
  ASSERT_TRUE(SSObjUtil::is_macro_data(shared_id));

  shared_id.set_id_mode((1UL << MacroBlockId::SF_BIT_ID_MODE) - 1);
  ASSERT_FALSE(shared_id.is_valid());
}

TEST_F(TestMacroBlockId, shared_first_id_reinitializes_reserved_bits)
{
  MacroBlockId id(-1, 1001, 1002, 1003, 1004);
  const ObStorageObjectTypeBase &type =
      ObStorageObjectTypeInstance::get_instance(ObStorageObjectType::SHARED_MINOR_DATA_MACRO);
  type.set_ss_object_first_id_(0x1234, id);

  ASSERT_EQ(0, id.ss_reserved_);
  ASSERT_EQ(0x1234, id.column_group_id());
  ASSERT_EQ(ObMacroBlockIdLocation::OBJECT_STORAGE, id.location());
  ASSERT_EQ(1001, id.second_id());
  ASSERT_EQ(1002, id.third_id());
  ASSERT_EQ(1003, id.fourth_id());
  ASSERT_EQ(1004, id.fifth_id());
  ASSERT_TRUE(id.is_valid());
}

TEST_F(TestMacroBlockId, five_id_lifecycle)
{
  MacroBlockId id;
  id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  id.set_second_id(1001);
  id.set_reorganization_scn(1002);
  id.set_op_id(1003);
  id.set_seq_id(1004);
  ASSERT_TRUE(id.is_valid());
  ASSERT_EQ(1002, id.reorganization_scn());
  ASSERT_EQ(1003, id.op_id());
  ASSERT_EQ(1004, id.seq_id());

  MacroBlockId copied(id);
  ASSERT_EQ(id, copied);
  ASSERT_EQ(id.hash(), copied.hash());

  MacroBlockId assigned;
  assigned = id;
  ASSERT_EQ(id, assigned);

  MacroBlockId raw(id.first_id(), id.second_id(), id.third_id(), id.fourth_id(), id.fifth_id());
  ASSERT_EQ(id, raw);

  copied.set_seq_id(1005);
  ASSERT_NE(id, copied);
  ASSERT_TRUE(id < copied);
  ASSERT_FALSE(copied < id);
  ASSERT_NE(id.hash(), copied.hash());

  copied.reset();
  ASSERT_EQ(0, copied.fifth_id());
  ASSERT_FALSE(copied.is_valid());
}

TEST_F(TestMacroBlockId, verification)
{
  MacroBlockId id;
  id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  id.set_second_id(1);
  id.set_reorganization_scn(2);
  id.set_op_id(3);
  id.set_seq_id(4);
  ASSERT_TRUE(id.is_valid());

  id.ss_reserved_ = 1;
  ASSERT_FALSE(id.is_valid());
  id.ss_reserved_ = 0;
  id.set_location(ObMacroBlockIdLocation::CONTROL);
  ASSERT_FALSE(id.is_valid());
  id.set_location(ObMacroBlockIdLocation::MAX);
  ASSERT_FALSE(id.is_valid());
  id.set_location(ObMacroBlockIdLocation::TABLE);
  ASSERT_FALSE(id.is_valid());
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
  ASSERT_TRUE(id.is_valid());
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  id.version_ = 0;
  ASSERT_FALSE(id.is_valid());
  id.set_version_v1();

  id.set_reorganization_scn(-1);
  ASSERT_FALSE(id.is_valid());
  id.set_reorganization_scn(2);
  id.set_op_id(-1);
  ASSERT_FALSE(id.is_valid());
  id.set_op_id(3);
  id.set_seq_id(-1);
  ASSERT_FALSE(id.is_valid());
  id.set_seq_id(4);

  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::MAX));
  ASSERT_FALSE(id.is_valid());
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  id.set_second_id(INT64_MAX);
  ASSERT_FALSE(id.is_valid());
}

TEST_F(TestMacroBlockId, fifth_id_semantics)
{
  MacroBlockId tablet_meta;
  tablet_meta.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tablet_meta.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  tablet_meta.set_second_id(1001);
  tablet_meta.set_third_id(0);
  tablet_meta.set_fourth_id(200001);
  tablet_meta.set_fifth_id(-1);
  tablet_meta.set_meta_version_id((1UL << MacroBlockId::SF_BIT_META_VERSION_ID) - 1);
  ASSERT_EQ(-1, tablet_meta.fifth_id());
  ASSERT_FALSE(tablet_meta.is_valid());

  tablet_meta.set_fifth_id(0);
  tablet_meta.set_meta_version_id(-1);
  ASSERT_FALSE(tablet_meta.is_valid());
  tablet_meta.set_meta_version_id(100002);
  ASSERT_EQ(0, tablet_meta.fifth_id());
  ASSERT_EQ(100002, tablet_meta.meta_version_id());
  ASSERT_TRUE(tablet_meta.is_valid());

  MacroBlockId private_macro;
  private_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  private_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  private_macro.set_second_id(200001);
  private_macro.set_reorganization_scn(300001);
  private_macro.set_op_id(400001);
  private_macro.set_tenant_seq(0x123456789ABCDEF0UL);
  ASSERT_EQ(300001, private_macro.third_id());
  ASSERT_EQ(400001, private_macro.op_id());
  ASSERT_EQ(0x123456789ABCDEF0UL, private_macro.tenant_seq());
  ASSERT_EQ(static_cast<int64_t>(0x123456789ABCDEF0UL), private_macro.fifth_id());
  ASSERT_TRUE(private_macro.is_valid());

  MacroBlockId inner_tablet_macro;
  inner_tablet_macro.set_ss_third_id(true, 500001, 600001);
  ASSERT_EQ(500001, inner_tablet_macro.ls_id());
  inner_tablet_macro.set_ss_third_id(false, 500002, 600002);
  ASSERT_EQ(600002, inner_tablet_macro.reorganization_scn());
}

TEST_F(TestMacroBlockId, outtable_lob_fifth_id_path_round_trip)
{
  const ObStorageObjectType type = ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO;
  MacroBlockId macro_id;
  macro_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_storage_object_type(static_cast<uint64_t>(type));
  macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  macro_id.set_second_id(1001);
  macro_id.set_third_id(1002);
  macro_id.set_fourth_id(1003);

  const int64_t fifth_ids[] = {0, INT64_MAX, INT64_MIN, -1};
  const char *remote_paths[] = {
    "outtable_lob/1002/1001/1003_0.T42",
    "outtable_lob/1002/1001/1003_9223372036854775807.T42",
    "outtable_lob/1002/1001/1003_9223372036854775808.T42",
    "outtable_lob/1002/1001/1003_18446744073709551615.T42"
  };
  const char *local_names[] = {
    "tablet_1002_create_1001_seq_1003_hash_0.T42",
    "tablet_1002_create_1001_seq_1003_hash_9223372036854775807.T42",
    "tablet_1002_create_1001_seq_1003_hash_9223372036854775808.T42",
    "tablet_1002_create_1001_seq_1003_hash_18446744073709551615.T42"
  };
  MacroBlockId decoded;
  for (int64_t i = 0; i < ARRAYSIZEOF(fifth_ids); ++i) {
    SCOPED_TRACE(fifth_ids[i]);
    macro_id.set_fifth_id(fifth_ids[i]);
    ASSERT_TRUE(SSObjUtil::is_valid(macro_id));

    char encoded_id[64] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, macro_id.serialize(encoded_id, sizeof(encoded_id), pos));
    const int64_t encoded_len = pos;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(encoded_id, encoded_len, pos));
    ASSERT_EQ(macro_id, decoded);

    char remote_path[256] = {};
    pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        SSObjUtil::to_relative_remote_path_format(macro_id, remote_path, sizeof(remote_path), pos));
    ASSERT_STREQ(remote_paths[i], remote_path);
    ASSERT_EQ(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(remote_path, decoded));
    ASSERT_EQ(macro_id, decoded);

    char local_path[256] = {};
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, SSObjUtil::to_local_path_format(
        macro_id, local_path, sizeof(local_path), pos,
        1 /*tenant_id*/, 1 /*tenant_epoch_id*/, 0, 0, 0));
    const char *local_basename = ObString(local_path).reverse_find('/', 1);
    ASSERT_NE(nullptr, local_basename);
    ASSERT_STREQ(local_names[i], local_basename + 1);
    ASSERT_EQ(OB_SUCCESS, SSObjUtil::local_path_to_macro_id(type, local_path, decoded));
    ASSERT_EQ(macro_id, decoded);
  }

  ASSERT_EQ(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(
      type, "object_root/outtable_lob/1002/1001/1003_18446744073709551615.T42", decoded));
  ASSERT_EQ(macro_id, decoded);
  ASSERT_NE(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(
      type, "outtable_lob/1002/1001/1003_0.T042", decoded));
  ASSERT_NE(OB_SUCCESS, SSObjUtil::remote_path_to_macro_id(
      "outtable_lob/1002/1001/1003.T42", decoded));
  ASSERT_NE(OB_SUCCESS, SSObjUtil::local_path_to_macro_id(
      type, "/tablet_1002_create_1001_seq_1003.T42", decoded));

  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  macro_id.set_fifth_id(-1);
  ASSERT_FALSE(macro_id.is_valid());
}

TEST_F(TestMacroBlockId, shared_major_inner_tablet_uses_ls_id)
{
  const ObStorageObjectType types[] = {
    ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
    ObStorageObjectType::SHARED_MAJOR_META_MACRO,
    ObStorageObjectType::SHARED_INC_MAJOR_DATA_MACRO,
    ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
  };
  const int64_t first_ls_id = 1001;
  const int64_t second_ls_id = 1002;
  const int64_t op_id = 17;
  const int64_t data_seq = 23;
  const int64_t column_group_id = 3;
  const int64_t reorganization_scn = 0;

  for (const ObStorageObjectType type : types) {
    ObStorageObjectOpt first_opt;
    first_opt.set_ss_share_object_opt(type, true, first_ls_id,
        ObTabletID::LS_MDS_TABLET_ID, op_id, data_seq, column_group_id, reorganization_scn);
    ObStorageObjectOpt second_opt;
    second_opt.set_ss_share_object_opt(type, true, second_ls_id,
        ObTabletID::LS_MDS_TABLET_ID, op_id, data_seq, column_group_id, reorganization_scn);
    MacroBlockId first_id;
    MacroBlockId second_id;

    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(first_opt, first_id));
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(second_opt, second_id));
    ASSERT_TRUE(first_id.is_id_mode_share());
    ASSERT_TRUE(second_id.is_id_mode_share());
    ASSERT_TRUE(first_id.meta_is_inner_tablet());
    ASSERT_EQ(first_ls_id, first_id.ls_id());
    ASSERT_EQ(second_ls_id, second_id.ls_id());
    ASSERT_EQ(op_id, first_id.op_id());
    ASSERT_EQ(data_seq, first_id.seq_id());
    ASSERT_NE(first_id, second_id);
  }
}

TEST_F(TestMacroBlockId, shared_major_helper_preserves_tablet_namespace)
{
  const int64_t ls_id = 1001;
  const int64_t major_snapshot = 2001;
  const int64_t data_seq = 23;
  const int64_t column_group_id = 3;
  const int64_t reorganization_scn = 3001;
  const int64_t inc_major_snapshot = 4001;
  ObStorageObjectOpt inner_opts[3];
  inner_opts[0].set_ss_major_data_macro_object_opt(
      true, ls_id, ObTabletID::LS_MDS_TABLET_ID,
      major_snapshot, data_seq, column_group_id, reorganization_scn);
  inner_opts[1].set_ss_major_meta_macro_object_opt(
      true, ls_id, ObTabletID::LS_MDS_TABLET_ID,
      major_snapshot, data_seq, column_group_id, reorganization_scn);
  inner_opts[2].set_ss_inc_major_meta_macro_object_opt(
      true, ls_id, ObTabletID::LS_MDS_TABLET_ID,
      inc_major_snapshot,
      data_seq, column_group_id, reorganization_scn);
  const ObStorageObjectType types[] = {
    ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
    ObStorageObjectType::SHARED_MAJOR_META_MACRO,
    ObStorageObjectType::SHARED_INC_MAJOR_META_MACRO,
  };

  for (int64_t i = 0; i < ARRAYSIZEOF(inner_opts); ++i) {
    MacroBlockId macro_id;
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(inner_opts[i], macro_id));
    ASSERT_TRUE(macro_id.is_id_mode_share());
    ASSERT_EQ(types[i], macro_id.storage_object_type());
    ASSERT_TRUE(macro_id.meta_is_inner_tablet());
    ASSERT_EQ(ls_id, macro_id.ls_id());
    if (i < 2) {
      ASSERT_EQ(major_snapshot, macro_id.major_snapshot());
    } else {
      ASSERT_EQ(inc_major_snapshot, macro_id.op_id());
    }
  }

  ObStorageObjectOpt user_opt;
  user_opt.set_ss_major_meta_macro_object_opt(
      false, ls_id, 200001, major_snapshot, data_seq, column_group_id, reorganization_scn);
  MacroBlockId user_id;
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(user_opt, user_id));
  ASSERT_TRUE(user_id.is_id_mode_share());
  ASSERT_FALSE(user_id.meta_is_inner_tablet());
  ASSERT_EQ(reorganization_scn, user_id.reorganization_scn());
  ASSERT_EQ(major_snapshot, user_id.major_snapshot());
}

TEST_F(TestMacroBlockId, shared_major_snapshot_namespace)
{
  const int64_t ls_id = 1001;
  const int64_t tablet_id = 200001;
  const int64_t first_snapshot = 3001;
  const int64_t second_snapshot = 3002;
  const int64_t data_seq = 0;
  const int64_t column_group_id = 0;
  const int64_t reorganization_scn = 4001;
  ObStorageObjectOpt first_opt;
  ObStorageObjectOpt second_opt;
  first_opt.set_ss_major_data_macro_object_opt(
      false, ls_id, tablet_id, first_snapshot, data_seq,
      column_group_id, reorganization_scn);
  second_opt.set_ss_major_data_macro_object_opt(
      false, ls_id, tablet_id, second_snapshot, data_seq,
      column_group_id, reorganization_scn);

  MacroBlockId first_id;
  MacroBlockId second_id;
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(first_opt, first_id));
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(second_opt, second_id));
  ASSERT_TRUE(first_id.is_valid());
  ASSERT_TRUE(second_id.is_valid());
  ASSERT_EQ(first_snapshot, first_id.major_snapshot());
  ASSERT_EQ(second_snapshot, second_id.major_snapshot());
  ASSERT_EQ(data_seq, first_id.seq_id());
  ASSERT_EQ(data_seq, second_id.seq_id());
  ASSERT_NE(first_id, second_id);
  ASSERT_NE(first_id.hash(), second_id.hash());

  char buf[MacroBlockId::SERIALIZED_SIZE] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, first_id.serialize(buf, sizeof(buf), pos));
  MacroBlockId decoded_id;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_id.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(first_id, decoded_id);
  ASSERT_EQ(first_snapshot, decoded_id.major_snapshot());

  ObStorageObjectOpt legacy_opt;
  legacy_opt.set_ss_share_meta_macro_object_opt(
      false, ls_id, tablet_id, data_seq,
      column_group_id, reorganization_scn);
  MacroBlockId legacy_id;
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(legacy_opt, legacy_id));
  ASSERT_TRUE(legacy_id.is_valid());
  ASSERT_EQ(0, legacy_id.major_snapshot());
}

// 验证 Major、Medium 和 Inc-Major 两代均可从 sequence 0 重启，完整对象 ID 仍全局唯一。
TEST_F(TestMacroBlockId, shared_merge_generations_restart_sequence_without_collision)
{
  const int64_t ls_id = 1001;
  const int64_t tablet_id = 200001;
  const int64_t reorganization_scn = 6001;
  const int64_t generation_namespaces[] = {
      3001, 3002,  // 两代 Major snapshot
      4001, 4002,  // 两代 Medium snapshot
      5001, 5002   // 两代 Inc-Major op_id
  };
  ObSEArray<MacroBlockId, 12> macro_ids;

  for (int64_t generation = 0;
       generation < ARRAYSIZEOF(generation_namespaces);
       ++generation) {
    const bool is_inc_major = generation >= 4;
    const ObStorageObjectType object_types[] = {
        is_inc_major
            ? ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO
            : ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
        is_inc_major
            ? ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO
            : ObStorageObjectType::SHARED_MAJOR_META_MACRO
    };
    for (int64_t type_idx = 0; type_idx < ARRAYSIZEOF(object_types); ++type_idx) {
      ObStorageObjectOpt opt;
      opt.set_ss_share_object_opt(
          object_types[type_idx], false, ls_id, tablet_id,
          generation_namespaces[generation], 0 /*sequence*/, 0 /*column_group_id*/,
          reorganization_scn);
      MacroBlockId macro_id;
      ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
      ASSERT_TRUE(macro_id.is_valid());
      ASSERT_EQ(0, macro_id.seq_id());
      ASSERT_EQ(generation_namespaces[generation], macro_id.fourth_id());
      ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(macro_id));
    }
  }

  ASSERT_EQ(12, macro_ids.count());
  for (int64_t i = 0; i < macro_ids.count(); ++i) {
    for (int64_t j = i + 1; j < macro_ids.count(); ++j) {
      ASSERT_NE(macro_ids.at(i), macro_ids.at(j));
    }
  }
}

TEST_F(TestMacroBlockId, shared_mark_sweep_gc_diag_layout)
{
  static constexpr int64_t ROUND_ID = 20000;
  static constexpr int64_t MAX_BUCKET = (1L << 20) - 1;
  static constexpr int64_t BUCKET_SHIFT = 9;
  const int64_t groups[] = {0, 1};
  const int64_t types[] = {0, 1};
  const int64_t buckets[] = {0, MAX_BUCKET};

  for (const int64_t group : groups) {
    for (const int64_t type : types) {
      for (const int64_t bucket : buckets) {
        ObStorageObjectOpt opt;
        opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, group, type, bucket);
        MacroBlockId macro_id;
        ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
        const uint64_t expected = static_cast<uint64_t>(group)
            | (static_cast<uint64_t>(type) << 1)
            | (static_cast<uint64_t>(bucket) << BUCKET_SHIFT);
        ASSERT_EQ(expected, static_cast<uint64_t>(macro_id.fourth_id()));
        ASSERT_TRUE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
      }
    }
  }

  ObStorageObjectOpt path_opt;
  path_opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 1/*type*/, MAX_BUCKET);
  MacroBlockId path_macro_id;
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(path_opt, path_macro_id));
  char path[256] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      SSObjUtil::to_relative_remote_path_format(path_macro_id, path, sizeof(path), pos));
  ASSERT_STREQ(
      "mark_sweep_gc_diag/round20000/kept/tablet_meta_scns/bkt1048575.T32", path);
}

TEST_F(TestMacroBlockId, shared_mark_sweep_gc_diag_study_layout)
{
  static constexpr int64_t ROUND_ID = 20000;
  static constexpr int64_t MAX_BUCKET = (1L << 20) - 1;
  static constexpr int64_t KEPT_GROUP = 1;
  static constexpr int64_t STUDY_RESULTS_TYPE = 2;
  const int64_t buckets[] = {0, 1, MAX_BUCKET};
  const char *paths[] = {
    "mark_sweep_gc_diag/round20000/kept/study_results/bkt0.T32",
    "mark_sweep_gc_diag/round20000/kept/study_results/bkt1.T32",
    "mark_sweep_gc_diag/round20000/kept/study_results/bkt1048575.T32"
  };
  ObStorageObjectOpt opt;
  MacroBlockId macro_id;
  MacroBlockId decoded_id;
  char path[256] = {0};
  char id_buf[128] = {0};
  for (int64_t i = 0; i < ARRAYSIZEOF(buckets); ++i) {
    const int64_t bucket = buckets[i];
    const uint64_t expected = 1UL | (2UL << 1) | (static_cast<uint64_t>(bucket) << 9);
    int64_t pos = 0;
    int64_t decode_pos = 0;
    opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, KEPT_GROUP, STUDY_RESULTS_TYPE, bucket);
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
    ASSERT_EQ(expected, static_cast<uint64_t>(macro_id.fourth_id()));
    ASSERT_TRUE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
    ASSERT_EQ(OB_SUCCESS,
        SSObjUtil::to_relative_remote_path_format(macro_id, path, sizeof(path), pos));
    ASSERT_STREQ(paths[i], path);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, macro_id.serialize(id_buf, sizeof(id_buf), pos));
    ASSERT_EQ(OB_SUCCESS, decoded_id.deserialize(id_buf, pos, decode_pos));
    ASSERT_EQ(pos, decode_pos);
    ASSERT_EQ(macro_id, decoded_id);
    ASSERT_TRUE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(decoded_id));
  }
}

TEST_F(TestMacroBlockId, shared_mark_sweep_gc_diag_rejects_invalid_layout)
{
  static constexpr int64_t ROUND_ID = 20000;
  static constexpr int64_t MAX_BUCKET_CNT = 1L << 20;
  ObStorageObjectOpt opt;
  MacroBlockId macro_id;

  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, 6/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 2/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 2/*group*/, 0/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 2/*group*/, 1/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 3/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, -1/*group*/, 2/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 3/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, 0/*type*/, -1/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, 0/*type*/, MAX_BUCKET_CNT/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 2/*type*/, -1/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 1/*group*/, 2/*type*/, MAX_BUCKET_CNT/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));
  opt.set_shared_mark_sweep_gc_diag_opt(-1/*round_id*/, 0/*group*/, 0/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObObjectManager::ss_get_object_id(opt, macro_id));

  opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, 0/*type*/, 0/*bucket*/);
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
  macro_id.set_fourth_id(1L << 29);
  ASSERT_FALSE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
  macro_id.set_fourth_id(5/*kept_study_results*/);
  ASSERT_TRUE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
  macro_id.set_fourth_id((1L << 29) | 5/*reserved_bit*/);
  ASSERT_FALSE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
  macro_id.set_fourth_id((1L << 30) | 5/*reserved_bit*/);
  ASSERT_FALSE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
}

TEST_F(TestMacroBlockId, shared_lob_gc_diag_object_id_and_path)
{
  static constexpr int64_t ROUND_ID = 20000;
  static constexpr int64_t BUCKET = 17;
  const int64_t types[] = {3, 4, 5};
  const char *paths[] = {
    "mark_sweep_gc_diag/round20000/lob_user_gc/lob_meta_kept/bkt17.T32",
    "mark_sweep_gc_diag/round20000/lob_user_gc/lob_ctrl_candidate/bkt17.T32",
    "mark_sweep_gc_diag/round20000/lob_user_gc/lob_ctrl_diff/bkt17.T32"
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(types); ++i) {
    ObStorageObjectOpt opt;
    opt.set_shared_mark_sweep_gc_diag_opt(ROUND_ID, 0/*group*/, types[i], BUCKET);
    MacroBlockId macro_id;
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, macro_id));
    ASSERT_TRUE(STI(ObStorageObjectType::SHARED_MARK_SWEEP_GC_DIAG).is_valid(macro_id));
    char path[256] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        SSObjUtil::to_relative_remote_path_format(macro_id, path, sizeof(path), pos));
    ASSERT_STREQ(paths[i], path);
  }
}

TEST_F(TestMacroBlockId, serialization_round_trip)
{
  MacroBlockId first;
  first.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  first.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  first.set_second_id(1001);
  first.set_reorganization_scn(1002);
  first.set_op_id(1003);
  first.set_seq_id(1004);
  ASSERT_TRUE(first.is_valid());

  MacroBlockId second(first);
  second.set_seq_id(2004);
  ASSERT_TRUE(second.is_valid());

  const int64_t id_size = 5 * sizeof(int64_t);
  ASSERT_EQ(id_size, first.get_serialize_size());
  char buf[2 * 5 * sizeof(int64_t)] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, first.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(id_size, pos);
  ASSERT_EQ(OB_SUCCESS, second.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(2 * id_size, pos);

  MacroBlockId decoded_first;
  MacroBlockId decoded_second;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_first.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(id_size, decode_pos);
  ASSERT_EQ(OB_SUCCESS, decoded_second.deserialize(buf, pos, decode_pos));
  ASSERT_EQ(pos, decode_pos);
  ASSERT_EQ(first, decoded_first);
  ASSERT_EQ(second, decoded_second);
}

TEST_F(TestMacroBlockId, deserialize_rejects_incomplete_or_unknown_version)
{
  MacroBlockId id;
  id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  id.set_second_id(1001);
  id.set_reorganization_scn(1002);
  id.set_op_id(1003);
  id.set_seq_id(1004);
  ASSERT_TRUE(id.is_valid());

  char buf[5 * sizeof(int64_t)] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, id.serialize(buf, sizeof(buf), pos));

  MacroBlockId target(id);
  target.set_seq_id(2004);
  const MacroBlockId original_target(target);
  int64_t decode_pos = 0;
  ASSERT_NE(OB_SUCCESS, target.deserialize(buf, pos - sizeof(int64_t), decode_pos));
  ASSERT_EQ(0, decode_pos);
  ASSERT_EQ(original_target, target);

  MacroBlockId unknown(id);
  unknown.version_ = 0;
  int64_t first_id_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      serialization::encode_i64(buf, sizeof(buf), first_id_pos, unknown.first_id()));
  decode_pos = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, target.deserialize(buf, sizeof(buf), decode_pos));
  ASSERT_EQ(0, decode_pos);
  ASSERT_EQ(original_target, target);

  unknown.version_ = (1UL << MacroBlockId::SF_BIT_VERSION) - 1;
  first_id_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      serialization::encode_i64(buf, sizeof(buf), first_id_pos, unknown.first_id()));
  decode_pos = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, target.deserialize(buf, sizeof(buf), decode_pos));
  ASSERT_EQ(0, decode_pos);
  ASSERT_EQ(original_target, target);
}
}
}

int main(int argc, char **argv)
{
  system("rm -f test_macro_block_id.log*");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_macro_block_id");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
