/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <new>

#define private public
#define protected public
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/index_block/ob_sstable_meta_info.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/ob_super_block_struct.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_write_info.h"
#undef protected
#undef private

namespace oceanbase
{
namespace storage
{

// The production micro-cache implementation is intentionally excluded from
// ob_close_modules. Define this type's UNIS methods in the dedicated test
// binary so its MacroBlockId field remains covered without restoring the
// retired micro-cache sources to the product target.
OB_SERIALIZE_MEMBER(ObSSMicroBlockId, macro_id_, offset_, size_);

} // namespace storage

namespace unittest
{

using namespace common;
using namespace blocksstable;
using namespace storage;

namespace
{

constexpr int64_t FUTURE_MACRO_BLOCK_ID_VERSION = 2;
constexpr int64_t FUTURE_MACRO_BLOCK_ID_SIZE = 6 * sizeof(int64_t);
constexpr int64_t COLUMN_GROUP_ID_SHIFT = MacroBlockId::SF_BIT_STORAGE_OBJECT_TYPE;
constexpr int64_t LOCATION_SHIFT = COLUMN_GROUP_ID_SHIFT + MacroBlockId::SF_BIT_COLUMN_GROUP_ID;
constexpr int64_t VERSION_SHIFT = LOCATION_SHIFT + MacroBlockId::SF_BIT_LOCATION +
                                  MacroBlockId::SF_BIT_RESERVED + MacroBlockId::SF_BIT_ID_MODE;
constexpr uint64_t VERSION_MASK = ((1ULL << MacroBlockId::SF_BIT_VERSION) - 1) << VERSION_SHIFT;
constexpr int64_t TAIL_MAGIC = 0x123456789ABCDEF;

int64_t set_macro_id_version(const int64_t first_id, const int64_t version)
{
  uint64_t value = static_cast<uint64_t>(first_id);
  value &= ~VERSION_MASK;
  value |= static_cast<uint64_t>(version) << VERSION_SHIFT;
  return static_cast<int64_t>(value);
}

int64_t get_macro_id_version(const int64_t first_id)
{
  return static_cast<int64_t>((static_cast<uint64_t>(first_id) & VERSION_MASK) >> VERSION_SHIFT);
}

MacroBlockId make_macro_id(const int64_t seq_id)
{
  MacroBlockId id;
  id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  id.set_second_id(200001);
  id.set_reorganization_scn(300001);
  id.set_op_id(400001);
  id.set_seq_id(seq_id);
  return id;
}

MacroBlockId make_tiered_meta_id(const int64_t seq_id)
{
  MacroBlockId id;
  id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
  id.set_location(ObMacroBlockIdLocation::TABLE);
  id.set_second_id(200001);
  id.set_reorganization_scn(300001);
  id.set_op_id(400001);
  id.set_seq_id(seq_id);
  return id;
}

// Model the future wire contract without changing the production type: V1 consumes five
// int64_t fields and defaults the appended field, while V2 consumes all six fields.
class TestMacroBlockIdV2 final
{
public:
  TestMacroBlockIdV2() : first_id_(0), second_id_(0), third_id_(0), fourth_id_(0), fifth_id_(0), sixth_id_(0) {}

  explicit TestMacroBlockIdV2(const MacroBlockId &id, const int64_t sixth_id = 0)
      : first_id_(id.first_id()),
        second_id_(id.second_id()),
        third_id_(id.third_id()),
        fourth_id_(id.fourth_id()),
        fifth_id_(id.fifth_id()),
        sixth_id_(sixth_id)
  {
  }

  void set_version(const int64_t version)
  {
    first_id_ = set_macro_id_version(first_id_, version);
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    int64_t new_pos = pos;
    const int64_t version = get_macro_id_version(first_id_);
    int64_t field_count = 0;
    const int64_t fields[] = {first_id_, second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_};
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (MacroBlockId::MACRO_BLOCK_ID_VERSION_V1 == version) {
      field_count = 5;
    } else if (FUTURE_MACRO_BLOCK_ID_VERSION == version) {
      field_count = 6;
    } else {
      ret = OB_NOT_SUPPORTED;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < field_count; ++i) {
      if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, fields[i]))) {
      }
    }
    if (OB_SUCC(ret)) {
      pos = new_pos;
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t new_pos = pos;
    TestMacroBlockIdV2 tmp;
    int64_t *fields[] = {
        &tmp.first_id_, &tmp.second_id_, &tmp.third_id_, &tmp.fourth_id_, &tmp.fifth_id_, &tmp.sixth_id_};
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos >= data_len)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, fields[0]))) {
    } else {
      const int64_t version = get_macro_id_version(tmp.first_id_);
      int64_t field_count = 0;
      if (MacroBlockId::MACRO_BLOCK_ID_VERSION_V1 == version) {
        field_count = 5;
      } else if (FUTURE_MACRO_BLOCK_ID_VERSION == version) {
        field_count = 6;
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < field_count; ++i) {
        if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, fields[i]))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      *this = tmp;
      pos = new_pos;
    }
    return ret;
  }

  int64_t first_id_;
  int64_t second_id_;
  int64_t third_id_;
  int64_t fourth_id_;
  int64_t fifth_id_;
  int64_t sixth_id_;
};

// ObMetaDiskAddr flattens its address header and all MacroBlockId fields into one UNIS payload.
class TestMetaDiskAddrV2 final
{
public:
  static const int64_t VERSION_V1 = 1;
  static const int64_t VERSION_V2 = 2;

  TestMetaDiskAddrV2()
      : first_id_(0), second_id_(0), third_id_(0), fourth_id_(0), fifth_id_(0), sixth_id_(0), seventh_id_(0)
  {
  }

  explicit TestMetaDiskAddrV2(const ObMetaDiskAddr &addr, const int64_t seventh_id = 0)
      : first_id_(addr.first_id_),
        second_id_(set_macro_id_version(addr.second_id_, FUTURE_MACRO_BLOCK_ID_VERSION)),
        third_id_(addr.third_id_),
        fourth_id_(addr.fourth_id_),
        fifth_id_(addr.fifth_id_),
        sixth_id_(addr.sixth_id_),
        seventh_id_(seventh_id)
  {
  }

  int64_t get_serialize_size() const
  {
    int64_t payload_size = 0;
    const int64_t fields[] = {first_id_, second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_, seventh_id_};
    for (int64_t i = 0; i < ARRAYSIZEOF(fields); ++i) {
      payload_size += serialization::encoded_length(fields[i]);
    }
    return serialization::encoded_length(VERSION_V2) + serialization::OB_SERIALIZE_SIZE_NEED_BYTES + payload_size;
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    int64_t new_pos = pos;
    int64_t version = VERSION_V2;
    const int64_t fields[] = {first_id_, second_id_, third_id_, fourth_id_, fifth_id_, sixth_id_, seventh_id_};
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos > buf_len)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, version))) {
    } else if (OB_UNLIKELY(buf_len - new_pos < serialization::OB_SERIALIZE_SIZE_NEED_BYTES)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      const int64_t len_pos = new_pos;
      new_pos += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
      const int64_t payload_pos = new_pos;
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(fields); ++i) {
        if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, fields[i]))) {
        }
      }
      if (OB_SUCC(ret)) {
        int64_t rewrite_pos = 0;
        const int64_t payload_size = new_pos - payload_pos;
        if (OB_FAIL(serialization::encode_fixed_bytes_i64(
                buf + len_pos, serialization::OB_SERIALIZE_SIZE_NEED_BYTES, rewrite_pos, payload_size))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      pos = new_pos;
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t new_pos = pos;
    int64_t version = 0;
    int64_t payload_size = 0;
    TestMetaDiskAddrV2 tmp;
    int64_t *fields[] = {&tmp.first_id_,
                         &tmp.second_id_,
                         &tmp.third_id_,
                         &tmp.fourth_id_,
                         &tmp.fifth_id_,
                         &tmp.sixth_id_,
                         &tmp.seventh_id_};
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || pos >= data_len)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(serialization::decode(buf, data_len, new_pos, version))) {
    } else if (OB_FAIL(serialization::decode(buf, data_len, new_pos, payload_size))) {
    } else if (OB_UNLIKELY(version != VERSION_V1 && version != VERSION_V2)) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_UNLIKELY(payload_size < 0 || data_len - new_pos < payload_size)) {
      ret = OB_DESERIALIZE_ERROR;
    } else {
      const int64_t payload_end = new_pos + payload_size;
      const int64_t field_count = VERSION_V1 == version ? 6 : 7;
      for (int64_t i = 0; OB_SUCC(ret) && i < field_count; ++i) {
        if (OB_FAIL(serialization::decode(buf, payload_end, new_pos, *fields[i]))) {
        }
      }
      if (OB_SUCC(ret)) {
        new_pos = payload_end;
      }
    }
    if (OB_SUCC(ret)) {
      *this = tmp;
      pos = new_pos;
    }
    return ret;
  }

  int64_t first_id_;
  int64_t second_id_;
  int64_t third_id_;
  int64_t fourth_id_;
  int64_t fifth_id_;
  int64_t sixth_id_;
  int64_t seventh_id_;
};

// ObIndexBlockRowHeader is a raw-memory format, so a future field must be appended at the tail
// and selected by the header version instead of relying on the new sizeof(struct).
class TestIndexBlockRowHeaderV2 final
{
public:
  static const int64_t VERSION_V2 = 2;

  TestIndexBlockRowHeaderV2() : header_(), macro_id_sixth_id_(0) {}

  int serialize_v2(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    const int64_t serialize_size = sizeof(ObIndexBlockRowHeader) + sizeof(macro_id_sixth_id_);
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || buf_len - pos < serialize_size)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ObIndexBlockRowHeader header;
      MEMCPY(&header, &header_, sizeof(header));
      header.version_ = VERSION_V2;
      header.macro_id_first_id_ = set_macro_id_version(header.macro_id_first_id_, FUTURE_MACRO_BLOCK_ID_VERSION);
      MEMCPY(buf + pos, &header, sizeof(header));
      pos += sizeof(header);
      MEMCPY(buf + pos, &macro_id_sixth_id_, sizeof(macro_id_sixth_id_));
      pos += sizeof(macro_id_sixth_id_);
    }
    return ret;
  }

  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t new_pos = pos;
    TestIndexBlockRowHeaderV2 tmp;
    if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || data_len - pos < sizeof(ObIndexBlockRowHeader))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(&tmp.header_, buf + new_pos, sizeof(tmp.header_));
      new_pos += sizeof(tmp.header_);
      if (ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V1 == tmp.header_.version_) {
        tmp.macro_id_sixth_id_ = 0;
      } else if (VERSION_V2 == tmp.header_.version_) {
        if (OB_UNLIKELY(data_len - new_pos < sizeof(tmp.macro_id_sixth_id_))) {
          ret = OB_BUF_NOT_ENOUGH;
        } else {
          MEMCPY(&tmp.macro_id_sixth_id_, buf + new_pos, sizeof(tmp.macro_id_sixth_id_));
          new_pos += sizeof(tmp.macro_id_sixth_id_);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
      }
    }
    if (OB_SUCC(ret)) {
      *this = tmp;
      pos = new_pos;
    }
    return ret;
  }

  ObIndexBlockRowHeader header_;
  int64_t macro_id_sixth_id_;
};

void build_valid_index_header(const MacroBlockId &macro_id, ObIndexBlockRowHeader &header)
{
  header.reset();
  header.row_store_type_ = static_cast<uint8_t>(ObRowStoreType::FLAT_ROW_STORE);
  header.compressor_type_ = static_cast<uint8_t>(ObCompressorType::NONE_COMPRESSOR);
  header.is_data_index_ = 1;
  header.is_data_block_ = 1;
  header.is_leaf_block_ = 1;
  header.is_major_node_ = 1;
  header.all_lob_in_row_ = 1;
  header.unset_has_logic_micro_id();
  header.unset_has_shared_data_macro_id();
  header.block_offset_ = 100;
  header.block_size_ = 2000;
  header.row_count_ = 100;
  header.schema_version_ = 1;
  header.macro_block_count_ = 1;
  header.micro_block_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, header.set_macro_id(macro_id));
  ASSERT_TRUE(header.is_valid());
}

} // namespace

TEST(TestMacroBlockIdCompat, location_encoding_round_trip)
{
  const uint64_t object_type = static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META);
  const uint64_t column_group_id = 0x1234;
  const ObMacroBlockIdLocation locations[] = {
      ObMacroBlockIdLocation::OBJECT_STORAGE, ObMacroBlockIdLocation::TABLE};
  for (const ObMacroBlockIdLocation location : locations) {
    const uint64_t first_id = object_type
        | (column_group_id << COLUMN_GROUP_ID_SHIFT)
        | (static_cast<uint64_t>(location) << LOCATION_SHIFT)
        | (static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE)
            << (VERSION_SHIFT - MacroBlockId::SF_BIT_ID_MODE))
        | (static_cast<uint64_t>(MacroBlockId::MACRO_BLOCK_ID_VERSION_V1) << VERSION_SHIFT);
    const MacroBlockId id(static_cast<int64_t>(first_id), 200001, 300001, 400001, 500001);
    ASSERT_TRUE(id.is_valid());
    ASSERT_EQ(column_group_id, id.column_group_id());
    ASSERT_EQ(location, id.location());
    ASSERT_EQ(ObMacroBlockIdLocation::TABLE == location, id.is_in_table());
    ASSERT_FALSE(id.is_in_control());

    char buf[MacroBlockId::SERIALIZED_SIZE] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, id.serialize(buf, sizeof(buf), pos));
    MacroBlockId decoded;
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
    ASSERT_EQ(pos, decode_pos);
    ASSERT_EQ(id, decoded);
  }

  const uint64_t control_first_id =
      static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_META)
      | (column_group_id << COLUMN_GROUP_ID_SHIFT)
      | (static_cast<uint64_t>(ObMacroBlockIdLocation::CONTROL) << LOCATION_SHIFT)
      | (static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE)
          << (VERSION_SHIFT - MacroBlockId::SF_BIT_ID_MODE))
      | (static_cast<uint64_t>(MacroBlockId::MACRO_BLOCK_ID_VERSION_V1) << VERSION_SHIFT);
  const MacroBlockId control_id(
      static_cast<int64_t>(control_first_id), 200001, 300001, 400001, 0);
  ASSERT_TRUE(control_id.is_valid());
  ASSERT_TRUE(control_id.is_in_control());
  ASSERT_FALSE(control_id.is_in_table());
  char control_buf[MacroBlockId::SERIALIZED_SIZE] = {};
  int64_t control_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      control_id.serialize(control_buf, sizeof(control_buf), control_pos));
  MacroBlockId decoded_control_id;
  int64_t control_decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      decoded_control_id.deserialize(
          control_buf, control_pos, control_decode_pos));
  ASSERT_EQ(control_pos, control_decode_pos);
  ASSERT_EQ(control_id, decoded_control_id);
}

TEST(TestMacroBlockIdCompat, future_reader_reads_v1_and_preserves_tail)
{
  const MacroBlockId id = make_macro_id(500001);
  ASSERT_TRUE(id.is_valid());
  char buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, id.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(MacroBlockId::SERIALIZED_SIZE, pos);
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), pos, TAIL_MAGIC));

  TestMacroBlockIdV2 future_id;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, future_id.deserialize(buf, pos, read_pos));
  ASSERT_EQ(MacroBlockId::SERIALIZED_SIZE, read_pos);
  ASSERT_EQ(id.first_id(), future_id.first_id_);
  ASSERT_EQ(id.second_id(), future_id.second_id_);
  ASSERT_EQ(id.third_id(), future_id.third_id_);
  ASSERT_EQ(id.fourth_id(), future_id.fourth_id_);
  ASSERT_EQ(id.fifth_id(), future_id.fifth_id_);
  ASSERT_EQ(0, future_id.sixth_id_);

  int64_t tail = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buf, pos, read_pos, &tail));
  ASSERT_EQ(TAIL_MAGIC, tail);
  ASSERT_EQ(pos, read_pos);
}

TEST(TestMacroBlockIdCompat, current_reader_rejects_v2_atomically)
{
  const MacroBlockId id = make_macro_id(500001);
  TestMacroBlockIdV2 future_id(id, 600001);
  future_id.set_version(FUTURE_MACRO_BLOCK_ID_VERSION);
  char buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, future_id.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(FUTURE_MACRO_BLOCK_ID_SIZE, pos);

  MacroBlockId target = make_macro_id(700001);
  const MacroBlockId original(target);
  int64_t read_pos = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, target.deserialize(buf, pos, read_pos));
  ASSERT_EQ(0, read_pos);
  ASSERT_EQ(original, target);
}

TEST(TestMacroBlockIdCompat, mixed_v1_v2_sequence_keeps_boundaries)
{
  const MacroBlockId first = make_macro_id(500001);
  const MacroBlockId third = make_macro_id(500003);
  TestMacroBlockIdV2 second(make_macro_id(500002), 600002);
  second.set_version(FUTURE_MACRO_BLOCK_ID_VERSION);
  char buf[256] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, first.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, second.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, third.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), pos, TAIL_MAGIC));

  TestMacroBlockIdV2 decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  ASSERT_EQ(MacroBlockId::SERIALIZED_SIZE, read_pos);
  ASSERT_EQ(0, decoded.sixth_id_);
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  ASSERT_EQ(MacroBlockId::SERIALIZED_SIZE + FUTURE_MACRO_BLOCK_ID_SIZE, read_pos);
  ASSERT_EQ(FUTURE_MACRO_BLOCK_ID_VERSION, get_macro_id_version(decoded.first_id_));
  ASSERT_EQ(second.second_id_, decoded.second_id_);
  ASSERT_EQ(second.third_id_, decoded.third_id_);
  ASSERT_EQ(second.fourth_id_, decoded.fourth_id_);
  ASSERT_EQ(second.fifth_id_, decoded.fifth_id_);
  ASSERT_EQ(600002, decoded.sixth_id_);
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  ASSERT_EQ(2 * MacroBlockId::SERIALIZED_SIZE + FUTURE_MACRO_BLOCK_ID_SIZE, read_pos);
  ASSERT_EQ(0, decoded.sixth_id_);
  int64_t tail = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buf, pos, read_pos, &tail));
  ASSERT_EQ(TAIL_MAGIC, tail);
  ASSERT_EQ(pos, read_pos);
}

TEST(TestMacroBlockIdCompat, meta_disk_addr_flattened_layout)
{
  const MacroBlockId id = make_macro_id(500001);
  ObMetaDiskAddr addr;
  ASSERT_EQ(OB_SUCCESS, addr.set_block_addr(id, 128, 4096, ObMetaDiskAddr::DiskType::BLOCK));
  addr.set_seq(7);
  addr.set_epoch(8);

  char buf[256] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, addr.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(addr.get_serialize_size(), pos);

  TestMetaDiskAddrV2 future_addr;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, future_addr.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(addr.first_id_, future_addr.first_id_);
  ASSERT_EQ(id.first_id(), future_addr.second_id_);
  ASSERT_EQ(id.second_id(), future_addr.third_id_);
  ASSERT_EQ(id.third_id(), future_addr.fourth_id_);
  ASSERT_EQ(id.fourth_id(), future_addr.fifth_id_);
  ASSERT_EQ(id.fifth_id(), future_addr.sixth_id_);
  ASSERT_EQ(0, future_addr.seventh_id_);

  TestMetaDiskAddrV2 v2_addr(addr, 600001);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, v2_addr.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(v2_addr.get_serialize_size(), pos);
  TestMetaDiskAddrV2 decoded_v2_addr;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_v2_addr.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(FUTURE_MACRO_BLOCK_ID_VERSION, get_macro_id_version(decoded_v2_addr.second_id_));
  ASSERT_EQ(addr.first_id_, decoded_v2_addr.first_id_);
  ASSERT_EQ(id.second_id(), decoded_v2_addr.third_id_);
  ASSERT_EQ(id.third_id(), decoded_v2_addr.fourth_id_);
  ASSERT_EQ(id.fourth_id(), decoded_v2_addr.fifth_id_);
  ASSERT_EQ(id.fifth_id(), decoded_v2_addr.sixth_id_);
  ASSERT_EQ(600001, decoded_v2_addr.seventh_id_);

  ObMetaDiskAddr old_reader;
  ASSERT_EQ(OB_SUCCESS, old_reader.set_block_addr(make_macro_id(700001), 256, 8192, ObMetaDiskAddr::DiskType::BLOCK));
  const ObMetaDiskAddr original(old_reader);
  read_pos = 0;
  ASSERT_EQ(OB_NOT_SUPPORTED, old_reader.deserialize(buf, pos, read_pos));
  ASSERT_EQ(
      serialization::encoded_length(TestMetaDiskAddrV2::VERSION_V2) + serialization::OB_SERIALIZE_SIZE_NEED_BYTES,
      read_pos);
  ASSERT_TRUE(old_reader.is_equal_for_persistence(original));

  const int64_t raw_fields[] = {
      addr.first_id_, addr.second_id_, addr.third_id_, addr.fourth_id_, addr.fifth_id_, addr.sixth_id_};
  ObMetaDiskAddr memcpy_addr;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            memcpy_addr.memcpy_deserialize(
                reinterpret_cast<const char *>(raw_fields), sizeof(raw_fields), read_pos));
  ASSERT_EQ(sizeof(raw_fields), read_pos);
  ASSERT_TRUE(addr.is_equal_for_persistence(memcpy_addr));
  MacroBlockId decoded_id;
  int64_t offset = 0;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, memcpy_addr.get_block_addr(decoded_id, offset, size));
  ASSERT_EQ(id, decoded_id);
  ASSERT_EQ(128, offset);
  ASSERT_EQ(4096, size);
}

TEST(TestMacroBlockIdCompat, index_header_appends_sixth_id)
{
  const MacroBlockId id = make_macro_id(500001);
  ObIndexBlockRowHeader header;
  ASSERT_NO_FATAL_FAILURE(build_valid_index_header(id, header));
  char buf[sizeof(ObIndexBlockRowHeader) + 2 * sizeof(int64_t)] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), pos, TAIL_MAGIC));

  TestIndexBlockRowHeaderV2 future_header;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, future_header.deserialize(buf, pos, read_pos));
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader), read_pos);
  ASSERT_EQ(id, future_header.header_.get_macro_id());
  ASSERT_EQ(0, future_header.macro_id_sixth_id_);
  int64_t tail = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buf, pos, read_pos, &tail));
  ASSERT_EQ(TAIL_MAGIC, tail);

  MEMCPY(&future_header.header_, &header, sizeof(header));
  future_header.macro_id_sixth_id_ = 600001;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, future_header.serialize_v2(buf, sizeof(buf), pos));
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader) + sizeof(int64_t), pos);
  ObIndexBlockRowHeader old_reader;
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, old_reader.deserialize(buf, pos, read_pos));
  ASSERT_EQ(0, read_pos);

  TestIndexBlockRowHeaderV2 decoded_v2;
  ASSERT_EQ(OB_SUCCESS, decoded_v2.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(FUTURE_MACRO_BLOCK_ID_VERSION, get_macro_id_version(decoded_v2.header_.macro_id_first_id_));
  ASSERT_EQ(id.second_id(), decoded_v2.header_.macro_id_second_id_);
  ASSERT_EQ(id.third_id(), decoded_v2.header_.macro_id_third_id_);
  ASSERT_EQ(id.fourth_id(), decoded_v2.header_.macro_id_fourth_id_);
  ASSERT_EQ(id.fifth_id(), decoded_v2.header_.macro_id_fifth_id_);
  ASSERT_EQ(600001, decoded_v2.macro_id_sixth_id_);
}

TEST(TestMacroBlockIdCompat, index_header_nested_shared_macro_id)
{
  const MacroBlockId primary_id = make_macro_id(500001);
  const MacroBlockId shared_id = make_macro_id(500002);
  ObIndexBlockRowHeader header;
  ASSERT_NO_FATAL_FAILURE(build_valid_index_header(primary_id, header));
  header.is_data_block_ = 0;
  header.set_shared_data_macro_id(shared_id);
  ASSERT_TRUE(header.is_valid());

  char buf[sizeof(ObIndexBlockRowHeader) + sizeof(int64_t)] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), pos, TAIL_MAGIC));

  TestIndexBlockRowHeaderV2 future_header;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, future_header.deserialize(buf, pos, read_pos));
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader), read_pos);
  ASSERT_TRUE(future_header.header_.has_valid_shared_macro_id());
  ASSERT_EQ(shared_id, future_header.header_.get_shared_data_macro_id());
  int64_t tail = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode_i64(buf, pos, read_pos, &tail));
  ASSERT_EQ(TAIL_MAGIC, tail);
  ASSERT_EQ(pos, read_pos);
}

TEST(TestMacroBlockIdCompat, manual_nested_structures_round_trip)
{
  const MacroBlockId id = make_macro_id(500001);
  char buf[1024] = {};
  int64_t pos = 0;
  int64_t read_pos = 0;

  ObSharedBlockInfo shared_info(id, 4096);
  ASSERT_EQ(OB_SUCCESS, shared_info.serialize(buf, sizeof(buf), pos));
  ObSharedBlockInfo decoded_shared_info;
  ASSERT_EQ(OB_SUCCESS, decoded_shared_info.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(id, decoded_shared_info.shared_macro_id_);
  ASSERT_EQ(4096, decoded_shared_info.occupy_size_);

  ObLinkedMacroBlockHeader linked_header;
  linked_header.item_count_ = 2;
  linked_header.fragment_offset_ = 64;
  linked_header.set_previous_block_id(id);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, linked_header.serialize(buf, sizeof(buf), pos));
  ASSERT_EQ(linked_header.get_serialize_size(), pos);
  ObLinkedMacroBlockHeader decoded_linked_header;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_linked_header.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(id, decoded_linked_header.get_previous_block_id());
  ASSERT_EQ(2, decoded_linked_header.item_count_);
  ASSERT_EQ(64, decoded_linked_header.fragment_offset_);

  ObSharedObjectHeader object_header;
  object_header.total_block_cnt_ = 1;
  object_header.cur_block_idx_ = 0;
  object_header.data_size_ = 4096;
  ASSERT_EQ(OB_SUCCESS, object_header.prev_addr_.set_block_addr(id, 128, 4096, ObMetaDiskAddr::DiskType::BLOCK));
  object_header.header_size_ = object_header.get_serialize_size();
  ASSERT_TRUE(object_header.is_valid());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, object_header.serialize(buf, sizeof(buf), pos));
  ObSharedObjectHeader decoded_object_header;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_object_header.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(ObSharedObjectHeader::DEFAULT_MACRO_ID, decoded_object_header.next_macro_id_);
  MacroBlockId decoded_id;
  int64_t offset = 0;
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_object_header.prev_addr_.get_block_addr(decoded_id, offset, size));
  ASSERT_EQ(id, decoded_id);
  ASSERT_EQ(128, offset);
  ASSERT_EQ(4096, size);
}

TEST(TestMacroBlockIdCompat, sstable_macro_info_inline_arrays)
{
  ObArenaAllocator allocator;
  const MacroBlockId first = make_macro_id(500001);
  const MacroBlockId second = make_macro_id(500002);
  ObSSTableMacroInfo source;
  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  ObMicroBlockData block_data;
  block_data.type_ = ObMicroBlockData::INDEX_BLOCK;
  ASSERT_EQ(
      OB_SUCCESS,
      source.macro_meta_info_.init_root_block_info(allocator, none_addr, block_data, ObRowStoreType::FLAT_ROW_STORE));
  source.data_block_ids_ = static_cast<MacroBlockId *>(allocator.alloc(2 * sizeof(MacroBlockId)));
  source.other_block_ids_ = static_cast<MacroBlockId *>(allocator.alloc(sizeof(MacroBlockId)));
  ASSERT_NE(nullptr, source.data_block_ids_);
  ASSERT_NE(nullptr, source.other_block_ids_);
  new (source.data_block_ids_) MacroBlockId(first);
  new (source.data_block_ids_ + 1) MacroBlockId(second);
  new (source.other_block_ids_) MacroBlockId(second);
  source.data_block_count_ = 2;
  source.other_block_count_ = 1;
  source.linked_block_count_ = 0;
  source.entry_id_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  source.nested_size_ = 4096;
  ASSERT_TRUE(source.is_valid());

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObMicroBlockDesMeta des_meta;
  des_meta.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  des_meta.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  ObSSTableMacroInfo decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, des_meta, buf, buf_len, read_pos));
  ASSERT_EQ(buf_len, read_pos);
  ASSERT_EQ(2, decoded.data_block_count_);
  ASSERT_EQ(1, decoded.other_block_count_);
  ASSERT_EQ(first, decoded.data_block_ids_[0]);
  ASSERT_EQ(second, decoded.data_block_ids_[1]);
  ASSERT_EQ(second, decoded.other_block_ids_[0]);

  const int64_t deep_copy_size = source.get_variable_size();
  char *deep_copy_buf = static_cast<char *>(allocator.alloc(deep_copy_size));
  ASSERT_NE(nullptr, deep_copy_buf);
  ObSSTableMacroInfo copied;
  int64_t deep_copy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.deep_copy(deep_copy_buf, deep_copy_size, deep_copy_pos, copied));
  ASSERT_EQ(deep_copy_size, deep_copy_pos);
  ASSERT_EQ(first, copied.data_block_ids_[0]);
  ASSERT_EQ(second, copied.data_block_ids_[1]);
  ASSERT_EQ(second, copied.other_block_ids_[0]);
}

TEST(TestMacroBlockIdCompat, tablet_macro_info_all_arrays)
{
  ObArenaAllocator allocator;
  const MacroBlockId first = make_macro_id(500001);
  const MacroBlockId second = make_macro_id(500002);
  ObTabletMacroInfo source;
  source.entry_block_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  ASSERT_EQ(OB_SUCCESS, source.meta_block_info_arr_.reserve(1, allocator));
  ASSERT_EQ(OB_SUCCESS, source.data_block_info_arr_.reserve(1, allocator));
  ASSERT_EQ(OB_SUCCESS, source.shared_meta_block_info_arr_.reserve(1, allocator));
  ASSERT_EQ(OB_SUCCESS, source.shared_data_block_info_arr_.reserve(1, allocator));
  new (source.meta_block_info_arr_.arr_) MacroBlockId(first);
  new (source.data_block_info_arr_.arr_) MacroBlockId(second);
  new (source.shared_meta_block_info_arr_.arr_) MacroBlockId(first);
  new (source.shared_data_block_info_arr_.arr_) ObSharedBlockInfo(second, 4096);
  source.is_inited_ = true;
  source.is_owned_by_tablet_ = false;
  ASSERT_TRUE(source.is_valid());

  const int64_t buf_len = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObTabletMacroInfo decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(allocator, buf, buf_len, read_pos));
  ASSERT_EQ(buf_len, read_pos);
  ASSERT_EQ(first, decoded.meta_block_info_arr_.arr_[0]);
  ASSERT_EQ(second, decoded.data_block_info_arr_.arr_[0]);
  ASSERT_EQ(first, decoded.shared_meta_block_info_arr_.arr_[0]);
  ASSERT_EQ(second, decoded.shared_data_block_info_arr_.arr_[0].shared_macro_id_);
  ASSERT_EQ(4096, decoded.shared_data_block_info_arr_.arr_[0].occupy_size_);

  const int64_t deep_copy_size = source.get_deep_copy_size();
  char *deep_copy_buf = static_cast<char *>(allocator.alloc(deep_copy_size));
  ASSERT_NE(nullptr, deep_copy_buf);
  ObTabletMacroInfo *copied = nullptr;
  ASSERT_EQ(OB_SUCCESS, source.deep_copy(deep_copy_buf, deep_copy_size, copied));
  ASSERT_NE(nullptr, copied);
  ASSERT_EQ(first, copied->meta_block_info_arr_.arr_[0]);
  ASSERT_EQ(second, copied->data_block_info_arr_.arr_[0]);
  ASSERT_EQ(first, copied->shared_meta_block_info_arr_.arr_[0]);
  ASSERT_EQ(second, copied->shared_data_block_info_arr_.arr_[0].shared_macro_id_);
  ASSERT_EQ(4096, copied->shared_data_block_info_arr_.arr_[0].occupy_size_);
  copied->~ObTabletMacroInfo();
}

TEST(TestMacroBlockIdCompat, checkpoint_and_tenant_super_block)
{
  ObArenaAllocator allocator;
  const MacroBlockId first = make_macro_id(500001);
  const MacroBlockId second = make_macro_id(500002);

  ObLSCkptMember ckpt;
  ckpt.tablet_meta_entry_ = first;
  const int64_t ckpt_buf_len = ckpt.get_serialize_size();
  char *ckpt_buf = static_cast<char *>(allocator.alloc(ckpt_buf_len));
  ASSERT_NE(nullptr, ckpt_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ckpt.serialize(ckpt_buf, ckpt_buf_len, pos));
  ObLSCkptMember decoded_ckpt;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_ckpt.deserialize(ckpt_buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(first, decoded_ckpt.tablet_meta_entry_);

  ObTenantSuperBlock super_block(1001);
  super_block.ls_meta_entry_ = first;
  super_block.tablet_meta_entry_ = second;
  super_block.wait_gc_tablet_entry_ = make_macro_id(500003);
  super_block.snapshot_cnt_ = 1;
  super_block.tenant_snapshots_[0].ls_meta_entry_ = make_macro_id(500004);
  super_block.tenant_snapshots_[0].snapshot_id_ = share::ObTenantSnapshotID(1);
  const int64_t super_buf_len = super_block.get_serialize_size();
  char *super_buf = static_cast<char *>(allocator.alloc(super_buf_len));
  ASSERT_NE(nullptr, super_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, super_block.serialize(super_buf, super_buf_len, pos));
  ASSERT_EQ(super_buf_len, pos);
  ObTenantSuperBlock decoded_super_block;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_super_block.deserialize(super_buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(first, decoded_super_block.ls_meta_entry_);
  ASSERT_EQ(second, decoded_super_block.tablet_meta_entry_);
  ASSERT_EQ(make_macro_id(500003), decoded_super_block.wait_gc_tablet_entry_);
  ASSERT_EQ(make_macro_id(500004), decoded_super_block.tenant_snapshots_[0].ls_meta_entry_);
}

TEST(TestMacroBlockIdCompat, shared_storage_unis_wrappers)
{
  ObArenaAllocator allocator;
  const MacroBlockId first = make_macro_id(500001);
  const MacroBlockId second = make_macro_id(500002);

  ObSSMicroBlockId micro_id(first, 128, 4096);
  int64_t buf_len = micro_id.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_id.serialize(buf, buf_len, pos));
  ObSSMicroBlockId decoded_micro_id;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_micro_id.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(micro_id, decoded_micro_id);

  ObSharedGCMacroIdList gc_list;
  ASSERT_EQ(OB_SUCCESS, gc_list.add(first));
  ASSERT_EQ(OB_SUCCESS, gc_list.add(second));
  buf_len = gc_list.get_serialize_size();
  buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, gc_list.serialize(buf, buf_len, pos));
  ObSharedGCMacroIdList decoded_gc_list;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_gc_list.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(2, decoded_gc_list.count());
  ASSERT_EQ(first, decoded_gc_list.at(0));
  ASSERT_EQ(second, decoded_gc_list.at(1));

  const MacroBlockId tiered_id = make_tiered_meta_id(500003);
  ASSERT_TRUE(tiered_id.is_shared_tablet_sub_meta_in_table());
  const char payload[] = "macro-id-compat";
  ObTieredMetadataWriteInfo tiered_info;
  tiered_info.macro_block_id_ = tiered_id;
  tiered_info.size_ = sizeof(payload);
  tiered_info.buffer_ = payload;
  ASSERT_TRUE(tiered_info.is_valid());
  buf_len = tiered_info.get_serialize_size();
  buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tiered_info.serialize(buf, buf_len, pos));
  ObTieredMetadataWriteInfo decoded_tiered_info;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_tiered_info.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(tiered_id, decoded_tiered_info.macro_block_id_);
  ASSERT_EQ(sizeof(payload), decoded_tiered_info.size_);
  ASSERT_EQ(0, MEMCMP(payload, decoded_tiered_info.buffer_, sizeof(payload)));

  ObSSMacroCacheInfo cache_info(200001, 4096, ObSSMacroCacheType::META_FILE, false);
  ObSSMacroCacheMeta cache_meta(first, cache_info);
  buf_len = cache_meta.get_serialize_size();
  buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, cache_meta.serialize(buf, buf_len, pos));
  ObSSMacroCacheMeta decoded_cache_meta;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_cache_meta.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(first, decoded_cache_meta.get_macro_id());
  ASSERT_EQ(200001, decoded_cache_meta.get_effective_tablet_id());
  ASSERT_EQ(4096, decoded_cache_meta.get_size());

  ObSSMacroPrewarmMeta prewarm_meta;
  prewarm_meta.macro_id_ = second;
  prewarm_meta.effective_tablet_id_ = 200002;
  prewarm_meta.size_ = 8192;
  prewarm_meta.is_in_fifo_list_ = true;
  prewarm_meta.cache_type_ = ObSSMacroCacheType::META_FILE;
  ASSERT_TRUE(prewarm_meta.is_valid());
  buf_len = prewarm_meta.get_serialize_size();
  buf = static_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, prewarm_meta.serialize(buf, buf_len, pos));
  ObSSMacroPrewarmMeta decoded_prewarm_meta;
  read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_prewarm_meta.deserialize(buf, pos, read_pos));
  ASSERT_EQ(pos, read_pos);
  ASSERT_EQ(second, decoded_prewarm_meta.macro_id_);
  ASSERT_EQ(200002, decoded_prewarm_meta.effective_tablet_id_);
  ASSERT_EQ(8192, decoded_prewarm_meta.size_);
  ASSERT_TRUE(decoded_prewarm_meta.is_in_fifo_list_);
  ASSERT_EQ(ObSSMacroCacheType::META_FILE, decoded_prewarm_meta.cache_type_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_macro_block_id_compat.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
