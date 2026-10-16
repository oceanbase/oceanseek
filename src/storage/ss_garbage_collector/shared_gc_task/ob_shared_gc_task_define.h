/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_GC_TASK_DEFINE_H_
#define OCEANBASE_STORAGE_OB_SHARED_GC_TASK_DEFINE_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/ob_table_range.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"

namespace oceanbase
{
namespace storage
{

// ===========================================================================
// Type column of __all_shared_gc_task / __all_shared_gc_task_history.
// ===========================================================================
enum class ObSharedGCTaskType : uint8_t
{
  INVALID_TYPE = 0,
  MARK_SWEEP_GC = 1,
  FAIL_GC = 2,
  FAIL_GC_MACRO_ID = 3,  // macro ids detected/deleted by a FAIL_GC task
  LS_META_GC = 4,
  DELETE_TENANT = 5,
  DELETE_SERVER = 6,
  LS_CKPT_GC = 7, //FARM COMPAT WHITELIST
  MARK_SWEEP_GC_IN_TABLE_STAT = 8, //FARM COMPAT WHITELIST
  MARK_SWEEP_GC_OBJECT_STORAGE_STAT = 9, //FARM COMPAT WHITELIST
  MARK_SWEEP_GC_DIAG = 10,
  MAX_TYPE
};

OB_INLINE bool is_valid_shared_gc_task_type(const ObSharedGCTaskType t)
{
  return t > ObSharedGCTaskType::INVALID_TYPE && t < ObSharedGCTaskType::MAX_TYPE;
}
OB_INLINE bool is_mark_sweep_gc_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::MARK_SWEEP_GC == t;
}
OB_INLINE bool is_fail_gc_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::FAIL_GC == t;
}
OB_INLINE bool is_fail_gc_macro_id_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::FAIL_GC_MACRO_ID == t;
}
OB_INLINE bool is_ls_meta_gc_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::LS_META_GC == t;
}
OB_INLINE bool is_ls_ckpt_gc_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::LS_CKPT_GC == t;
}
OB_INLINE bool is_delete_tenant_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::DELETE_TENANT == t;
}
OB_INLINE bool is_delete_server_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::DELETE_SERVER == t;
}
OB_INLINE bool is_mark_sweep_gc_in_table_stat_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::MARK_SWEEP_GC_IN_TABLE_STAT == t;
}
OB_INLINE bool is_mark_sweep_gc_object_storage_stat_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::MARK_SWEEP_GC_OBJECT_STORAGE_STAT == t;
}
OB_INLINE bool is_mark_sweep_gc_stat_type(const ObSharedGCTaskType t)
{
  return is_mark_sweep_gc_in_table_stat_type(t)
      || is_mark_sweep_gc_object_storage_stat_type(t);
}
OB_INLINE bool is_shared_data_gc_task_type(const ObSharedGCTaskType t)
{
  return is_mark_sweep_gc_type(t)
      || is_fail_gc_type(t)
      || is_fail_gc_macro_id_type(t);
}

OB_INLINE bool is_mark_sweep_gc_diag_type(const ObSharedGCTaskType t)
{
  return ObSharedGCTaskType::MARK_SWEEP_GC_DIAG == t;
}

const char *ob_shared_gc_task_type_to_string(const ObSharedGCTaskType type);
ObSharedGCTaskType ob_string_to_shared_gc_task_type(const common::ObString &str);

// ===========================================================================
// Primary key of __all_shared_gc_task / __all_shared_gc_task_history.
// ===========================================================================
struct ObSharedGCTaskKey
{
public:
  ObSharedGCTaskKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      type_(ObSharedGCTaskType::INVALID_TYPE),
      round_id_(0),
      task_id_(0)
  {}
  ObSharedGCTaskKey(const uint64_t tenant_id,
                    const ObSharedGCTaskType type,
                    const int64_t round_id,
                    const int64_t task_id)
    : tenant_id_(tenant_id), type_(type), round_id_(round_id), task_id_(task_id)
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(type), K_(round_id), K_(task_id));
public:
  uint64_t tenant_id_;
  ObSharedGCTaskType type_;
  int64_t round_id_;
  int64_t task_id_;
};

// ===========================================================================
// One row of __all_shared_gc_task / __all_shared_gc_task_history.
// ===========================================================================
struct ObSharedGCTask
{
public:
  static constexpr int64_t MAX_VALUE_LENGTH      = 256 * 1024;
  static constexpr int64_t MAX_EXTRA_INFO_LENGTH = 16 * 1024;
  static constexpr int64_t MAX_TRACE_ID_LENGTH   = 1024;
  static constexpr int64_t INVALID_SERVER_ID     = -1;

  ObSharedGCTask() { reset(); }
  virtual ~ObSharedGCTask() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(key), "value_length", value_.length(),
               "extra_info_length", extra_info_.length(),
               K_(server_id), K_(trace_id));
public:
  ObSharedGCTaskKey key_;
  common::ObString value_;
  common::ObString extra_info_;
  int64_t server_id_;
  common::ObString trace_id_;
};

typedef common::ObIArray<ObSharedGCTask> ObSharedGCTaskIArray;
typedef common::ObArray<ObSharedGCTask> ObSharedGCTaskArray;

// ===========================================================================
// Candidate range within a given (tenant_id, type) bucket of
// __all_shared_gc_task. The iterator scans exactly one round (round_id)
// and the task_id slice gives disjoint task ownership.
// ===========================================================================
struct ObSSGCTaskIdRange
{
  ObSSGCTaskIdRange()
    : start_task_id(-1), end_task_id(-1) {}
  ObSSGCTaskIdRange(const int64_t start_task_id,
                    const int64_t end_task_id)
    : start_task_id(start_task_id), end_task_id(end_task_id) {}

  bool is_valid() const { return start_task_id >= 0 && end_task_id >= start_task_id; }
  TO_STRING_KV(K(start_task_id), K(end_task_id));
  int64_t start_task_id;
  int64_t end_task_id;
};

// ===========================================================================
// MarkSweep GC value: list of MacroBlockIds to delete.
// Stored in __all_shared_gc_task.value as UNIS-serialized bytes (varbinary).
// ===========================================================================
struct ObSharedGCMacroIdList
{
  OB_UNIS_VERSION(1);
public:
  ObSharedGCMacroIdList();
  virtual ~ObSharedGCMacroIdList() = default;
  void reset();
  bool is_valid() const;
  int add(const blocksstable::MacroBlockId &macro_id);
  int get_serialize_size_after_add(const blocksstable::MacroBlockId &macro_id,
                                   const int64_t cur_serialize_size,
                                   int64_t &next_serialize_size) const;
  OB_INLINE int64_t count() const { return macro_id_array_.count(); }
  OB_INLINE bool empty() const { return macro_id_array_.empty(); }
  OB_INLINE const blocksstable::MacroBlockId &at(const int64_t idx) const
  {
    return macro_id_array_.at(idx);
  }

  int to_value(common::ObIAllocator &allocator, common::ObString &value) const;
  int from_value(const common::ObString &value);

  TO_STRING_KV("count", macro_id_array_.count(), K_(macro_id_array));
public:
  common::ObSEArray<blocksstable::MacroBlockId, 8> macro_id_array_;
};

// ===========================================================================
// Fail GC value: a single unified struct carrying all fields across all meta
// types. meta_type_ (ObStorageObjectType) is the discriminator. Fields
// not applicable to a given meta type carry their default-constructed values.
// UNIS-serialized into ObSharedGCTask::value_ (varbinary).
// ===========================================================================
struct ObSharedGCFailInfo
{
  OB_UNIS_VERSION(1);
public:
  ObSharedGCFailInfo();
  virtual ~ObSharedGCFailInfo() = default;
  void reset();
  bool is_valid() const;
  int to_value(common::ObIAllocator &allocator, common::ObString &value) const;
  int from_value(const common::ObString &value);

  TO_STRING_KV(K_(meta_type), K_(op_id), K_(ls_id), K_(tablet_id),
               K_(transfer_scn), K_(ctrl_object_info));
public:
  blocksstable::ObStorageObjectType meta_type_;          // discriminator
  int64_t                 op_id_;
  share::ObLSID           ls_id_;
  common::ObTabletID      tablet_id_;
  share::SCN              transfer_scn_;
  ObSSMetaCtrlObjectInfo  ctrl_object_info_;   // SSTABLE only
};

// ===========================================================================
// LS Meta GC value: identifies one stale ls_meta version to reclaim, keyed by
// (ls_id, meta_scn). UNIS-serialized into ObSharedGCTask::value_ (varbinary).
// ===========================================================================
struct ObSharedGCLSMetaInfo
{
  OB_UNIS_VERSION(1);
public:
  ObSharedGCLSMetaInfo();
  virtual ~ObSharedGCLSMetaInfo() = default;
  void reset();
  bool is_valid() const;
  int to_value(common::ObIAllocator &allocator, common::ObString &value) const;
  int from_value(const common::ObString &value);

  TO_STRING_KV(K_(ls_id), K_(meta_scn));
public:
  share::ObLSID  ls_id_;
  share::SCN     meta_scn_;
};

// 两类任务的持久化字段完全一致，复用同一 UNIS V1 格式。
using ObSharedGCLSCheckpointInfo = ObSharedGCLSMetaInfo;

// ===========================================================================
// 删除租户 GC 值：标识共享存储数据仍待回收的已删除 user/meta 租户。
// ===========================================================================
struct ObDeleteTenantTaskValue
{
  OB_UNIS_VERSION(1);
public:
  ObDeleteTenantTaskValue();
  virtual ~ObDeleteTenantTaskValue() = default;
  void reset();
  bool is_valid() const;
  int to_value(common::ObIAllocator &allocator, common::ObString &value) const;
  int from_value(const common::ObString &value);

  TO_STRING_KV(K_(tenant_id), K_(delete_ts));
public:
  uint64_t tenant_id_;
  int64_t delete_ts_;
};

// ===========================================================================
// 删除 Server GC 值：标识共享存储目录和持久化 Server 元数据仍待回收的
// 已删除 Server。
// ===========================================================================
struct ObDeleteServerTaskValue
{
  OB_UNIS_VERSION(1);
public:
  ObDeleteServerTaskValue();
  virtual ~ObDeleteServerTaskValue() = default;
  void reset();
  bool is_valid() const;
  int to_value(common::ObIAllocator &allocator, common::ObString &value) const;
  int from_value(const common::ObString &value);

  TO_STRING_KV(K_(server_id), K_(server_addr), K_(delete_ts));
public:
  uint64_t server_id_;
  common::ObAddr server_addr_;
  int64_t delete_ts_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SHARED_GC_TASK_DEFINE_H_
