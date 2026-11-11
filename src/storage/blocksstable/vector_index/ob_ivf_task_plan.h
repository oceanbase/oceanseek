/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_TASK_PLAN_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_TASK_PLAN_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

enum class ObIVFProcessType : uint8_t
{
  INVALID = 0,
  FULL_REBUILD,   // Merge rebuild from persisted IVF SSTables
  REFRESH,        // DDL incremental refresh
  CONCATENATION,  // Merge segment concatenation
  INSERTION,      // Immutable leaves/codes; route and quantize only delta vectors.
  INITIAL_BUILD,  // Initial index build from source-table scan
  MAX
};

inline bool is_valid_ivf_process_type(const ObIVFProcessType process_type)
{
  return process_type > ObIVFProcessType::INVALID
      && process_type < ObIVFProcessType::MAX;
}

inline bool is_ivf_full_build_process_type(
    const ObIVFProcessType process_type)
{
  return ObIVFProcessType::INITIAL_BUILD == process_type
      || ObIVFProcessType::FULL_REBUILD == process_type;
}

class ObIVFTaskPlan final
{
public:
  ObIVFTaskPlan();
  ~ObIVFTaskPlan() = default;
  int init(
      const ObIVFProcessType process_type,
      const int64_t plan_id,
      const int64_t task_count);
  bool is_valid() const;
  bool is_valid_task_idx(const int64_t task_idx) const
  {
    return task_idx >= 0 && task_idx < task_count_;
  }
  ObIVFProcessType get_process_type() const { return process_type_; }
  int64_t get_plan_id() const { return plan_id_; }
  int64_t get_task_count() const { return task_count_; }
  TO_STRING_KV("process_type", static_cast<int64_t>(process_type_),
      K_(plan_id), K_(task_count));
private:
  bool is_inited_;
  ObIVFProcessType process_type_;
  int64_t plan_id_;
  int64_t task_count_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFTaskPlan);
};

class ObIVFTaskResult final
{
public:
  ObIVFTaskResult();
  ~ObIVFTaskResult() = default;
  int init(const ObIVFTaskPlan &task_plan, const int64_t task_idx);
  int seal();
  bool is_valid() const;
  bool is_sealed() const { return is_sealed_; }
  ObIVFProcessType get_process_type() const { return process_type_; }
  int64_t get_plan_id() const { return plan_id_; }
  int64_t get_task_idx() const { return task_idx_; }
  TO_STRING_KV("process_type", static_cast<int64_t>(process_type_),
      K_(plan_id), K_(task_idx), K_(is_sealed));
private:
  bool is_inited_;
  ObIVFProcessType process_type_;
  int64_t plan_id_;
  int64_t task_idx_;
  bool is_sealed_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_TASK_PLAN_H_
