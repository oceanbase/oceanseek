/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_CTX_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_CTX_H_

#include "lib/lock/ob_spin_lock.h"
#include "storage/ss_data_check/ob_ss_data_check_define.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sorter.h"

namespace oceanbase
{
namespace storage
{

// 管理单轮数据检查的预期数据流水线及其所有临时文件。持有者必须串行调用
// init() 和 cleanup()，只能在 init() 成功后发布 Context，并且必须在清理
// 或析构前停止并等待所有任务退出。
class ObSSDataCheckCtx final
{
public:
  ObSSDataCheckCtx();
  ~ObSSDataCheckCtx();

  int init(
      const uint64_t tenant_id,
      const share::SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode,
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp);
  int inc_active_task_count();
  void dec_active_task_count(const int result);
  int finish_expected_collect();
  int finish_list_check();
  // 记录已保留已知引用、但因依赖对象缺失而无法继续展开的 Expected。
  int record_incomplete_expected();
  // 仅保留本轮首个仍被 shared meta 引用的缺失项，用于结果事件诊断。
  int record_referenced_missing(const ObSSDataCheckExpectedItem &item);
  int get_first_referenced_missing_item(
      ObSSDataCheckExpectedItem &item) const;
  int publish_status();
  int get_status(ObSSDataCheckStatus &status) const;
  bool has_published_status() const;
  int cleanup();

  // 调用方必须持有 active task 引用；Handle 负责把临时文件生命周期
  // 绑定到本轮 Context 的统一临时文件管理器。
  int acquire_tmp_file(
      const int64_t dir_id,
      const int64_t fd,
      ObMarkSweepTmpFileHandle &file_handle);

  bool is_valid() const;
  uint64_t get_tenant_id() const;
  share::SCN get_snapshot_meta_scn() const;
  ObSSDataCheckMode get_check_mode() const;
  int64_t get_memory_limit() const;
  int64_t get_file_buf_size() const;
  int64_t get_io_expire_timestamp() const;
  int64_t get_active_task_count() const;
  int get_first_error() const;
  bool is_expected_collect_complete() const;
  bool is_list_check_complete() const;

  // 调用方使用返回的流水线时，必须持有通过 inc_active_task_count() 获取的
  // 任务引用。从流水线复制出的所有 Handle 都必须在 dec_active_task_count()
  // 前释放，避免清理期间 Handle 的生命周期超过临时文件管理器。
  ObSSDataCheckExpectedSortPipeline &get_expected_sort_pipeline()
  { return expected_pipeline_; }
  const ObSSDataCheckExpectedSortPipeline &get_expected_sort_pipeline() const
  { return expected_pipeline_; }

private:
  void reset_();

private:
  mutable common::ObSpinLock lock_;
  bool is_inited_;
  uint64_t tenant_id_;
  share::SCN snapshot_meta_scn_;
  ObSSDataCheckMode check_mode_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  int64_t active_task_count_;
  int first_error_;
  bool expected_collect_complete_;
  bool has_incomplete_expected_;
  bool list_check_complete_;
  bool has_referenced_missing_;
  ObSSDataCheckExpectedItem first_referenced_missing_item_;
  bool has_published_status_;
  ObSSDataCheckStatus published_status_;

  // 此处刻意按该顺序声明。C++ 按声明的逆序析构成员，确保流水线及其持有的
  // 所有 Handle 都先于临时文件管理器析构。
  ObMarkSweepTmpFileMgr tmp_file_mgr_;
  ObSSDataCheckExpectedSortPipeline expected_pipeline_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckCtx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_CTX_H_
