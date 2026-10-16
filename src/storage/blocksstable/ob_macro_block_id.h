/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_
#define SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_

#include "share/ob_define.h"
#include "common/storage/ob_io_device.h"

// Forward declaration to avoid circular dependency
namespace oceanbase {
namespace blocksstable {
enum class ObStorageObjectType : uint8_t;
class ObStorageObjectTypeBase;
class SSObjUtil;
}
}

// Forward declaration for storage namespace types
namespace oceanbase {
namespace storage {
enum class ObSSMacroBlockType : uint8_t;
}
}

namespace oceanbase
{
namespace blocksstable
{
enum class ObMacroBlockIdMode : uint8_t
{
  ID_MODE_LOCAL = 0,
  ID_MODE_BACKUP = 1,
  ID_MODE_SHARE = 2,
  ID_MODE_MAX,
};

enum class ObMacroBlockIdLocation : uint8_t
{
  OBJECT_STORAGE = 0,
  TABLE = 1,
  CONTROL = 2,
  MAX,
};

class MacroBlockId final
{
public:
  MacroBlockId();
  // only for ID_MODE_LOCAL
  MacroBlockId(const uint64_t write_seq, const int64_t block_index, const int64_t third_id);
  MacroBlockId(const int64_t first_id, const int64_t second_id, const int64_t third_id, const int64_t fourth_id);
  MacroBlockId(const int64_t first_id, const int64_t second_id, const int64_t third_id, const int64_t fourth_id, const int64_t fifth_id);
  MacroBlockId(const MacroBlockId &id);
  ~MacroBlockId() = default;

  OB_INLINE void reset()
  {
    first_id_ = 0;
    second_id_ = INT64_MAX;
    third_id_ = 0;
    fourth_id_ = 0;
    fifth_id_ = 0;
    version_ = MACRO_BLOCK_ID_VERSION_V1;
  }
  bool is_valid() const;

  OB_INLINE bool is_local_id() const
  {
    return static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_LOCAL) == id_mode_;
  }
  OB_INLINE bool is_backup_id() const
  {
    return static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_BACKUP) == id_mode_;
  }
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  // General GETTER/SETTER
  int64_t first_id() const { return first_id_; }
  int64_t second_id() const { return second_id_; }
  int64_t third_id() const { return third_id_; }
  int64_t fourth_id() const { return fourth_id_; }
  int64_t fifth_id() const { return fifth_id_; }

  void set_first_id(const int64_t first_id) { first_id_ = first_id; }
  void set_second_id(const int64_t second_id) { second_id_ = second_id; }
  void set_third_id(const int64_t third_id) { third_id_ = third_id; }
  void set_fourth_id(const int64_t fourth_id) { fourth_id_ = fourth_id; }
  void set_fifth_id(const int64_t fifth_id) { fifth_id_ = fifth_id; }

  void set_version_v1() { version_ = MACRO_BLOCK_ID_VERSION_V1; }
  uint64_t id_mode() const { return id_mode_; }
  bool is_id_mode_local() const;
  bool is_id_mode_backup() const;
  bool is_id_mode_share() const;
  void set_id_mode(const uint64_t id_mode) { id_mode_ = id_mode; }
  bool is_shared_data_or_meta() const; // shared tablet macro block in ss mode
  bool is_shared_data_block_or_meta_block() const; // shared tablet meta or data macro block in ss mode
  bool is_transfer_out_non_gc_block() const; // macro blocks that should skip gc for transfer out tablet
  bool is_private_data_or_meta() const; // private tablet macro block in ss mode
  bool is_data() const; // shared data or private data
  bool is_meta() const; // shared meta or private meta
  bool is_tablet_local_cache_object() const; // shared data or private data or shared meta or private meta, which has tablet info
  bool is_private_macro() const; // private data/meta macro block in ss mode
  bool is_private() const; // private objects in ss mode, including private tablet meta, private data/meta macro...
  bool is_macro_write_cache_ctrl_obj_type() const; // macro write cache controlled by _ss_local_cache_control in ss mode
  // Local mode
  int64_t block_index() const { return second_id_; }
  void set_block_index(const int64_t block_index) { second_id_ = block_index; }
  uint64_t write_seq() const { return write_seq_; }
  void set_write_seq(const uint64_t write_seq) { write_seq_ = write_seq; }

  // Common object fields
  ObStorageObjectType storage_object_type() const { return static_cast<ObStorageObjectType>(storage_object_type_); }
  const ObStorageObjectTypeBase &get_type_instance() const;
  void set_storage_object_type(const uint64_t storage_object_type)
  {
    id_mode_ = static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE);
    storage_object_type_ = storage_object_type;
  }
  int64_t column_group_id() const { return column_group_id_; }
  void set_column_group_id(const uint64_t column_group_id) { column_group_id_ = column_group_id; }
  ObMacroBlockIdLocation location() const { return static_cast<ObMacroBlockIdLocation>(location_); }
  void set_location(const ObMacroBlockIdLocation location) { location_ = static_cast<uint64_t>(location); }
  bool is_in_table() const { return ObMacroBlockIdLocation::TABLE == location(); }
  bool is_in_control() const { return ObMacroBlockIdLocation::CONTROL == location(); }
  bool is_in_object_storage() const { return ObMacroBlockIdLocation::OBJECT_STORAGE == location(); }
  int64_t ls_id() const { return ls_id_; }
  void set_ls_id(const int64_t ls_id) { ls_id_ = ls_id; }
  int64_t reorganization_scn() const { return reorganization_scn_; }
  void set_reorganization_scn(const int64_t reorganization_scn) { reorganization_scn_ = reorganization_scn; }
  int64_t op_id() const { return fourth_id_; }
  void set_op_id(const int64_t op_id) { fourth_id_ = op_id; }
  int64_t major_snapshot() const { return fourth_id_; }
  void set_major_snapshot(const int64_t major_snapshot) { fourth_id_ = major_snapshot; }
  int64_t meta_scn() const { return fourth_id_; }
  void set_meta_scn(const int64_t meta_scn) { fourth_id_ = meta_scn; }
  int64_t seq_id() const { return fifth_id_; }
  void set_seq_id(const int64_t seq_id) { fifth_id_ = seq_id; }
  uint64_t tenant_seq() const { return tenant_seq_; }
  void set_tenant_seq(const uint64_t tenant_seq) { tenant_seq_ = tenant_seq; }

  int64_t meta_version_id() const { return fourth_id_; }
  void set_meta_version_id(const int64_t meta_version_id) { fourth_id_ = meta_version_id; }
  bool meta_is_inner_tablet() const;
  bool is_shared_tablet_sub_meta_in_table() const;
  bool is_shared_tablet_sub_meta() const;
  void set_ss_third_id(
      const bool meta_is_inner_tablet,
      const int64_t ls_id,
      const int64_t reorganization_scn)
  {
    if (meta_is_inner_tablet) {
      ls_id_ = ls_id;
    } else {
      reorganization_scn_ = reorganization_scn;
    }
  }

  // Deivce mode
  void set_from_io_fd(const common::ObIOFd &block_id)
  {
    first_id_ = block_id.first_id_;
    second_id_ = block_id.second_id_;
    third_id_ = block_id.third_id_;
    fourth_id_ = 0;
    fifth_id_ = 0;
  }

  MacroBlockId& operator =(const MacroBlockId &other)
  {
    first_id_ = other.first_id_;
    second_id_ = other.second_id_;
    third_id_ = other.third_id_;
    fourth_id_ = other.fourth_id_;
    fifth_id_ = other.fifth_id_;
    return *this;
  }
  bool operator ==(const MacroBlockId &other) const;
  bool operator !=(const MacroBlockId &other) const;
  bool operator <(const MacroBlockId &other) const;

  NEED_SERIALIZE_AND_DESERIALIZE;
  static MacroBlockId mock_valid_macro_id()
  {
    MacroBlockId id;
    id.set_second_id(AUTONOMIC_BLOCK_INDEX);
    return id;
  }
private:
  void first_id_to_string_(char *buf, const int64_t buf_len, int64_t &pos) const;

public:
  static constexpr int64_t MACRO_BLOCK_ID_VERSION_V1 = 1;
  static constexpr int64_t SERIALIZED_SIZE = 5 * sizeof(int64_t);

  static const int64_t EMPTY_ENTRY_BLOCK_INDEX = -1;
  static const int64_t AUTONOMIC_BLOCK_INDEX = -1;

  static const uint64_t SF_BIT_WRITE_SEQ = 52;
  static const uint64_t SF_BIT_LOCAL_RESERVED = 4;
  static const uint64_t SF_BIT_STORAGE_OBJECT_TYPE = 8;
  static const uint64_t SF_BIT_COLUMN_GROUP_ID = 16;
  // bits 24-25: the actual storage location of the object.
  static const uint64_t SF_BIT_LOCATION = 2;
  static constexpr uint64_t SF_BIT_RESERVED = 30;
  static constexpr uint64_t SF_BIT_ID_MODE = 4;
  static const uint64_t SF_BIT_VERSION = 4;
  static const uint64_t SF_BIT_TENANT_SEQ = 44;
  static constexpr uint64_t SF_BIT_META_VERSION_ID = 44;
  static const uint64_t MAX_WRITE_SEQ = (0x1UL << MacroBlockId::SF_BIT_WRITE_SEQ) - 1;
  static constexpr int64_t DEFAULT_FIRST_ID = static_cast<int64_t>(
      static_cast<uint64_t>(MACRO_BLOCK_ID_VERSION_V1)
      << (SF_BIT_WRITE_SEQ + SF_BIT_LOCAL_RESERVED + SF_BIT_ID_MODE));

private:
  static const uint64_t HASH_MAGIC_NUM = 2654435761;

private:
  union { // FARM COMPAT WHITELIST
    int64_t first_id_;
    struct {
      uint64_t write_seq_ : SF_BIT_WRITE_SEQ;
      uint64_t local_reserved_ : SF_BIT_LOCAL_RESERVED;
      uint64_t id_mode_ : SF_BIT_ID_MODE;
      uint64_t version_ : SF_BIT_VERSION;
    };
    struct {
      uint64_t storage_object_type_ : SF_BIT_STORAGE_OBJECT_TYPE;
      uint64_t column_group_id_ : SF_BIT_COLUMN_GROUP_ID;
      uint64_t location_ : SF_BIT_LOCATION;
      uint64_t ss_reserved_ : SF_BIT_RESERVED;
      uint64_t ss_id_mode_ : SF_BIT_ID_MODE;
      uint64_t ss_version_ : SF_BIT_VERSION;
    };
  };
  int64_t second_id_;
  union {
    int64_t third_id_;
    int64_t ls_id_;               // for ls inner tablet
    int64_t reorganization_scn_;  // for user tablet
  };
  int64_t fourth_id_;
  union {
    int64_t fifth_id_;
    uint64_t tenant_seq_; // for PRIVATE_DATA_MACRO and PRIVATE_META_MACRO
  };
};

static_assert(MacroBlockId::SF_BIT_WRITE_SEQ + MacroBlockId::SF_BIT_LOCAL_RESERVED
        + MacroBlockId::SF_BIT_ID_MODE + MacroBlockId::SF_BIT_VERSION == 64,
    "local macro block id first id must be 64 bits");
static_assert(MacroBlockId::SF_BIT_STORAGE_OBJECT_TYPE + MacroBlockId::SF_BIT_COLUMN_GROUP_ID
        + MacroBlockId::SF_BIT_LOCATION + MacroBlockId::SF_BIT_RESERVED
        + MacroBlockId::SF_BIT_ID_MODE
        + MacroBlockId::SF_BIT_VERSION == 64,
    "shared storage macro block id first id must be 64 bits");
static_assert(sizeof(MacroBlockId) == MacroBlockId::SERIALIZED_SIZE, "MacroBlockId must be 40 bytes");

OB_INLINE bool MacroBlockId::operator ==(const MacroBlockId &other) const
{
  return other.first_id_ == first_id_ && other.second_id_ == second_id_
      && other.third_id_ == third_id_ && other.fourth_id_ == fourth_id_
      && other.fifth_id_ == fifth_id_;
}

OB_INLINE bool MacroBlockId::operator !=(const MacroBlockId &other) const
{
  return !(other == *this);
}
#define SERIALIZE_MEMBER_WITH_MEMCPY(member)                                 \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_UNLIKELY(buf_len - pos < sizeof(member))) {                       \
      ret = OB_BUF_NOT_ENOUGH;                                               \
      LOG_WARN("buffer not enough", K(ret), KP(buf), K(buf_len), K(pos));    \
    } else {                                                                 \
      MEMCPY(buf + pos, &member, sizeof(member));                            \
      pos += sizeof(member);                                                 \
    }                                                                        \
  }

#define DESERIALIZE_MEMBER_WITH_MEMCPY(member)                               \
  if (OB_SUCC(ret)) {                                                        \
    if (OB_UNLIKELY(data_len - pos < sizeof(member))) {                      \
      ret = OB_DESERIALIZE_ERROR;                                            \
      LOG_WARN("buffer not enough", K(ret), KP(buf), K(data_len), K(pos));   \
    } else {                                                                 \
      MEMCPY(&member, buf + pos, sizeof(member));                            \
      pos += sizeof(member);                                                 \
    }                                                                        \
  }

} // namespace blocksstable
} // namespace oceanbase

#endif /* SRC_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_ID_H_ */
