/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_CTRL_DEFINE
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_CTRL_DEFINE

#include <cstdint>
#include "lib/container/ob_iarray.h"
#include "share/ob_ls_id.h"
#include "deps/oblib/src/common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_occam_time_guard.h"
#include "lib/profile/ob_trace_id.h"
#include "storage/blocksstable/ob_storage_object_type.h"

namespace oceanbase
{
namespace storage
{

inline bool is_valid_ss_meta_ctrl_type(const blocksstable::ObStorageObjectType type)
{
  return type < blocksstable::ObStorageObjectType::MAX
      && blocksstable::ObSSMetaCtrlFamily::NONE
          != blocksstable::ObStorageObjectTypeInstance::get_instance(type).ss_meta_ctrl_family();
}

inline const char *ss_meta_ctrl_to_string(const blocksstable::ObStorageObjectType type)
{
  return blocksstable::ObStorageObjectTypeInstance::get_instance(type).get_type_str();
}

inline bool should_backup_ss_ctrl_type(const blocksstable::ObStorageObjectType type)
{
  return blocksstable::ObStorageObjectTypeInstance::get_instance(type).ss_meta_ctrl_should_backup();
}

inline bool is_ss_meta_ctrl_lock_type(const blocksstable::ObStorageObjectType type)
{
  return blocksstable::ObStorageObjectTypeInstance::get_instance(type).is_ss_meta_ctrl_lock_row();
}

// Two orthogonal classifications live on this type:
//
//   1) lock-row vs data-row -- is_lock_row()
//      Driven by ObStorageObjectType cfg `ss_meta_ctrl_is_lock_row`.
//
//   2) ctrl_key sub-struct routing -- is_tablet_type / is_sstable_type /
//      is_ls_type. Tells which member of ObSSMetaCtrlKey's anonymous union
//      (`tablet_key_` / `sstable_key_` / `ls_key_`) the row keys itself
//      with, and is what ObSSMetaCtrlKey::is_valid() consults to decide
//      which sub-struct to validate.
//
// IMPORTANT: these two classifications are NOT mutually exclusive.
struct ObSSMetaCtrlType final
{
  ObSSMetaCtrlType();
  ~ObSSMetaCtrlType() = default;
  void reset();
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static bool should_backup(const blocksstable::ObStorageObjectType meta_type);
  bool is_lock_row() const;
  int get_lock_type(ObSSMetaCtrlType &type) const;
  bool is_tablet_type() const;
  bool is_sstable_type() const;
  bool is_ls_type() const;
  bool is_major_sstable_type() const;
  bool is_checkpoint_type() const;
  static int get_all_lock_types(common::ObIArray<blocksstable::ObStorageObjectType> &types);
  static int get_all_ctrl_value_types(common::ObIArray<blocksstable::ObStorageObjectType> &types);
  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;
  bool operator==(const ObSSMetaCtrlType &other) const { return type_ == other.type_; }
  operator int64_t() const { return static_cast<int64_t>(type_); }
  blocksstable::ObStorageObjectType type_;
};

struct ObSSMetaCtrlKey final
{
public:
  ObSSMetaCtrlKey();
  ObSSMetaCtrlKey(const blocksstable::ObStorageObjectType type,
                  const int64_t first_id,
                  const int64_t second_id,
                  const int64_t third_id);
  ObSSMetaCtrlKey(const blocksstable::ObStorageObjectType type,
                  const common::ObTabletID &tablet_id,
                  const share::SCN &reorg_scn,
                  const share::SCN &control_scn,
                  const share::ObLSID &ls_id);
  ~ObSSMetaCtrlKey() {}
  void reset();
  void assign_type(const ObSSMetaCtrlType &meta_type);
  void assign_type(const blocksstable::ObStorageObjectType type);
  ObSSMetaCtrlType get_meta_type() const;
  int get_lock_meta_type(ObSSMetaCtrlType &type) const;
  bool is_valid_type() const;
  bool is_lock_row() const;
  bool is_tablet_type() const;
  bool is_sstable_type() const;
  bool is_ls_type() const;
  bool is_major_sstable_type() const;
  bool is_checkpoint_type() const;
  bool is_valid() const;
  blocksstable::MacroBlockId convert_to_macro_id() const;
  int init_from_macro_id(const blocksstable::MacroBlockId &macro_id);
  int serialize_meta_key(char* buf,
                         const int64_t buf_len,
                         int64_t& pos) const;
  int deserialize_meta_key(const char* buf,
                           const int64_t buf_len,
                           int64_t& pos);
  int get_meta_key_serialize_size(int64_t &size) const;
  int assign(const ObSSMetaCtrlKey &meta_key);
  // Return the control_scn stored in this key. The slot differs across key
  // families (tablet_key_.control_scn_ / sstable_key_.control_scn_ /
  // ls_key_.control_scn_), so we dispatch on type_. Returns OB_ERR_UNEXPECTED if
  // type_ does not map to any of the known families.
  int get_control_scn(share::SCN &control_scn) const;
  int get_lock_row_key(ObSSMetaCtrlKey &lock_key) const;
  int get_prefix_meta_key_serialize_size(int64_t &size) const;
  int serialize_prefix_meta_key(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const;
  TO_STRING_KV(K_(type), K_(first_id), K_(second_id), K_(third_id), K_(fourth_id));

  blocksstable::ObStorageObjectType type_;
  union {
    struct {
      int64_t first_id_;
      int64_t second_id_;
      int64_t third_id_;
      int64_t fourth_id_;
    };

    struct {
      ObTabletID tablet_id_;
      share::SCN reorg_scn_;
      share::SCN control_scn_;
      share::ObLSID ls_id_;
    } tablet_key_;

    struct {
      ObTabletID tablet_id_;
      share::SCN reorg_scn_;
      share::SCN control_scn_;
      share::ObLSID ls_id_;
    } sstable_key_;

    struct {
      int64_t first_id_;
      int64_t second_id_;
      share::SCN control_scn_;
    } ls_key_;
  };
};

inline bool is_ss_meta_ctrl_ls_id_consistent(
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &ctrl_key)
{
  return (!ctrl_key.is_ls_type() || ls_id.id() == ctrl_key.ls_key_.first_id_)
      && (!ctrl_key.is_tablet_type()
          || !blocksstable::ObStorageObjectTypeInstance::get_instance(
              ctrl_key.type_).is_path_include_inner_tablet()
          || !ctrl_key.tablet_key_.tablet_id_.is_ls_inner_tablet()
          || ls_id == ctrl_key.tablet_key_.ls_id_)
      && (!ctrl_key.is_sstable_type()
          || !blocksstable::ObStorageObjectTypeInstance::get_instance(
              ctrl_key.type_).is_path_include_inner_tablet()
          || !ctrl_key.sstable_key_.tablet_id_.is_ls_inner_tablet()
          || ls_id == ctrl_key.sstable_key_.ls_id_);
}

int validate_ss_meta_ctrl_row_key(
    const share::ObLSID &ls_id,
    const blocksstable::ObStorageObjectType meta_type,
    const ObSSMetaCtrlKey &ctrl_key);

struct ObSSMetaCtrlStatus final
{
  OB_UNIS_VERSION(1);
public:
  enum class STATUS : int8_t
  {
    INVALID = 0,
    START = 1,
    COMPLETE = 2,
    ABANDON = 3,
    MAX,
  };

  ObSSMetaCtrlStatus();
  ~ObSSMetaCtrlStatus() = default;
  bool is_valid() const;
  void reset();
  int set_status_start();
  int set_status_complete();
  int set_status_abandon();
  bool is_start_status() const { return STATUS::START == status_; }
  bool is_complete_status() const { return STATUS::COMPLETE == status_; }
  bool is_abandon_status() const { return STATUS::ABANDON == status_; }
  static const char *get_str(const STATUS &status);
  static STATUS get_status(const char *status_str);
  bool operator==(const ObSSMetaCtrlStatus &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  STATUS status_;
};

struct ObSSMetaCtrlInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObSSMetaCtrlInfo();
  ~ObSSMetaCtrlInfo() = default;
  void reset();
  bool is_valid() const;
  bool operator==(const ObSSMetaCtrlInfo &other) const;
  // Set trace_id_ from the current thread's trace id. If the current trace id
  // is invalid, only report an ERROR log and leave trace_id_ untouched -- an
  // invalid trace id is diagnostic-only and must not fail the caller.
  void set_cur_trace_id();
  TO_STRING_KV(K_(status), K_(op_id), K_(lease_epoch), K_(trace_id));

  ObSSMetaCtrlStatus status_;
  int64_t op_id_;
  int64_t lease_epoch_;
  common::ObCurTraceId::TraceId trace_id_;
};

struct ObSSMetaCtrlObjectInfo final
{
  OB_UNIS_VERSION(1);
public:
  enum class TYPE : int8_t
  {
    INVALID = 0,
    NONE = 1,
    MAJOR = 2,
    NONE_MAJOR = 3,
    INC_MAJOR = 4, // 仅用于 inc-major compaction 场景。
    MAX,
  };
  static const char *get_str(const TYPE type);
  ObSSMetaCtrlObjectInfo();
  ~ObSSMetaCtrlObjectInfo() = default;
  void reset();
  bool is_valid() const;
  bool operator==(const ObSSMetaCtrlObjectInfo &other) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool is_major() const { return TYPE::MAJOR == type_; }
  bool is_inc_major() const { return TYPE::INC_MAJOR == type_; }
  bool is_none_major() const { return TYPE::NONE_MAJOR == type_; }
  bool is_none() const { return TYPE::NONE == type_; }
  void set_none() { type_ = TYPE::NONE; }

  // 通过单个接口绑定 type_ 和对应形态的字段，避免调用方混用不同形态的字段。
  // 需要完全清空对象时，先调用 reset() 再重新初始化。
  int init_by_major(const share::SCN &snapshot_version, const int64_t seq_step,
      const int64_t parallel_cnt, const int64_t cg_cnt);
  int init_by_inc_major(const share::SCN &snapshot_version, const int64_t seq_step,
      const int64_t parallel_cnt, const int64_t cg_cnt);
  int init_by_none_major(
      const share::SCN &snapshot_version,
      const int64_t parallel_cnt);

  TYPE type_;
  share::SCN snapshot_version_;
  int64_t seq_step_;
  int64_t parallel_cnt_;

  // MAJOR 和 INC_MAJOR 形态使用。
  int64_t cg_cnt_;         // column group count
};

struct ObSSMetaCtrlValue final
{
  OB_UNIS_VERSION(1);
public:
  ObSSMetaCtrlValue();
  ~ObSSMetaCtrlValue() = default;
  void reset();
  bool is_valid() const;
  bool operator==(const ObSSMetaCtrlValue &other) const;
  TO_STRING_KV(K_(ctrl_info), K_(object_info));

  ObSSMetaCtrlInfo ctrl_info_;
  ObSSMetaCtrlObjectInfo object_info_;
};

struct ObSSMetaCtrlLockRowValue final
{
  OB_UNIS_VERSION(1);
public:
  ObSSMetaCtrlLockRowValue();
  ~ObSSMetaCtrlLockRowValue() = default;
  void reset();
  bool is_valid() const;
  bool operator==(const ObSSMetaCtrlLockRowValue &other) const;
  TO_STRING_KV(K_(is_mark_deleted), K_(op_id), K_(meta_scn), K_(deleted_scn),
      K_(last_major_snapshot), K_(transfer_seq), K_(kept_meta_scn));

  bool is_mark_deleted_;
  int64_t op_id_;
  share::SCN meta_scn_;
  share::SCN deleted_scn_;
  int64_t last_major_snapshot_;
  int64_t transfer_seq_;
  share::SCN kept_meta_scn_;
};

struct ObSSMetaCtrlKeyInfo final
{
public:
  ObSSMetaCtrlKeyInfo();
  ~ObSSMetaCtrlKeyInfo() = default;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(key));
  void reset();
  bool is_valid() const;
  int assign(const ObSSMetaCtrlKeyInfo &key_info);

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSMetaCtrlKey key_;
};


} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_CTRL_DEFINE
