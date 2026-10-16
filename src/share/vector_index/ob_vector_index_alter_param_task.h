/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_INDEX_ALTER_PARAM_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_INDEX_ALTER_PARAM_TASK_H_

#include "share/vector_index/ob_vector_index_async_task.h"

namespace oceanbase
{
namespace share
{

struct ObVecAlterParamParentInfo
{
  ObVecAlterParamParentInfo()
      : table_id_(OB_INVALID_ID), task_id_(OB_INVALID_ID), trace_id_(), task_info_()
  {}
  uint64_t table_id_;
  int64_t task_id_;
  common::ObCurTraceId::TraceId trace_id_;
  ObVecIndexAlterParamTaskInfo task_info_;
  TO_STRING_KV(K_(table_id), K_(task_id), K_(trace_id), K_(task_info));
};

class ObVecAlterIndexParamTaskExecutor final : public ObVecAsyncTaskExecutor
{
public:
  ObVecAlterIndexParamTaskExecutor() : ObVecAsyncTaskExecutor() {}
  virtual ~ObVecAlterIndexParamTaskExecutor() {}
  int load_triggered_task(const ObVecIndexTaskStatus &task_row) override;
  int load_task(uint64_t &task_trace_base_num) override;
  int load_task(
      uint64_t &task_trace_base_num,
      const common::ObIArray<ObVecAlterParamParentInfo> *parents);
  static int do_check_task_result(
      ObVecIndexAsyncTaskCtx *task_ctx,
      ObVecTaskResultCheckAction &action);

private:
  bool check_operation_allow() override;
};

class ObVecAlterIndexParamTask final : public ObVecIndexIAsyncTask
{
public:
  ObVecAlterIndexParamTask()
      : ObVecIndexIAsyncTask(ObMemAttr(MTL_ID(), "VecAlterParam"))
  {}
  virtual ~ObVecAlterIndexParamTask() {}
  int do_work() override;

private:
  enum AlterParamTabletStatus
  {
    TRIGGER_FREEZE = 0,
    WAIT_FREEZE_FINISH,
    TRIGGER_MERGE,
    WAIT_MERGE_FINISH,
    FINISHED
  };

  struct AlterParamChildTaskInfo
  {
    AlterParamChildTaskInfo()
        : task_id_(OB_INVALID_ID), task_type_(OB_VECTOR_ASYNC_TASK_TYPE_INVALID),
          status_(OB_VECTOR_ASYNC_TASK_INVALID),
          ret_code_(VEC_ASYNC_TASK_DEFAULT_ERR_CODE), gmt_create_(0)
    {}
    bool is_valid() const { return OB_INVALID_ID != task_id_; }
    bool is_finished() const { return OB_VECTOR_ASYNC_TASK_FINISH == status_; }
    bool is_success() const { return is_finished() && OB_SUCCESS == ret_code_; }
    int64_t task_id_;
    int64_t task_type_;
    int64_t status_;
    int ret_code_;
    int64_t gmt_create_;
    TO_STRING_KV(K_(task_id), K_(task_type), K_(status), K_(ret_code), K_(gmt_create));
  };

  struct AlterParamTabletProgress
  {
    AlterParamTabletProgress()
        : inc_table_id_(OB_INVALID_ID), inc_tablet_id_(),
          vbitmap_table_id_(OB_INVALID_ID), vbitmap_tablet_id_(),
          has_active_segment_(false), active_param_match_(false),
          has_frozen_segment_(false), has_old_persistent_segment_(false),
          has_running_conflict_task_(false),
          freeze_task_(), merge_task_(), status_(TRIGGER_FREEZE)
    {}
    uint64_t inc_table_id_;
    ObTabletID inc_tablet_id_;
    uint64_t vbitmap_table_id_;
    ObTabletID vbitmap_tablet_id_;
    bool has_active_segment_;
    bool active_param_match_;
    bool has_frozen_segment_;
    bool has_old_persistent_segment_;
    bool has_running_conflict_task_;
    AlterParamChildTaskInfo freeze_task_;
    AlterParamChildTaskInfo merge_task_;
    AlterParamTabletStatus status_;
    TO_STRING_KV(K_(inc_table_id), K_(inc_tablet_id), K_(vbitmap_table_id),
                 K_(vbitmap_tablet_id), K_(has_active_segment),
                 K_(active_param_match), K_(has_frozen_segment),
                 K_(has_old_persistent_segment), K_(has_running_conflict_task),
                 K_(freeze_task), K_(merge_task), K_(status));
  };

  int check_parent_(bool &valid);
  int load_tablet_progresses_(
      const ObVectorIndexParam &target_param,
      common::ObIArray<AlterParamTabletProgress> &progresses,
      int64_t &schema_version);
  int load_child_task_infos_(common::ObIArray<AlterParamTabletProgress> &progresses);
  static int restore_tablet_status_(AlterParamTabletProgress &progress);
  static int process_freeze_result_(AlterParamTabletProgress &progress);
  static int process_merge_result_(AlterParamTabletProgress &progress);
  int create_child_(const AlterParamTabletProgress &progress, const bool freeze);
  int check_cancel_(bool &is_cancel);
  int cancel_children_(const ObVecIndexAlterParamTaskInfo *target_info = nullptr);
  int finish_parent_as_canceled_();
  int handle_cancel_(bool &all_finished);
  int update_parent_(
      const int64_t schema_version,
      const int64_t progress,
      const bool converged,
      bool &parent_valid);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_VECTOR_INDEX_ALTER_PARAM_TASK_H_
