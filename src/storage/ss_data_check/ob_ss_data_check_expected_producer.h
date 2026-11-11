/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_PRODUCER_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_PRODUCER_H_

#include "lib/lock/ob_mutex.h"
#include "share/ob_check_stop_provider.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sort_task.h"

namespace oceanbase
{
namespace storage
{

// SINGLE 枚举租户全部逻辑 tablet 在固定快照内的最新版本，FULL 枚举固定
// 快照内的全部版本；两种模式复用相同的宏块展开逻辑。
class ObSSDataCheckSharedMetaExpectedSource final
{
public:
  ObSSDataCheckSharedMetaExpectedSource();
  ~ObSSDataCheckSharedMetaExpectedSource();

  int open(
      const uint64_t tenant_id,
      const share::SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode);
  void reset();
  int get_next_tablet_batch(
      common::ObIAllocator &allocator,
      const int64_t max_tablet_count,
      const int64_t max_batch_size,
      const share::ObCheckStopProvider &stop_checker,
      common::ObIArray<ObSSRawMetaRow> &rows);

private:
  void reset_();
  int collect_ls_ids_();
  int open_next_ls_(const int64_t page_size);

private:
  mutable lib::ObMutex lock_;
  bool is_inited_;
  bool is_end_;
  uint64_t tenant_id_;
  share::SCN snapshot_meta_scn_;
  ObSSDataCheckMode check_mode_;
  ObSSMetaService *meta_service_;
  common::ObSEArray<share::ObLSID, 16> ls_ids_;
  int64_t ls_idx_;
  ObSSMetaIterGuard<ObSSMetaRawRowIter> raw_meta_iter_guard_;
  ObSSMetaRawRowIter *raw_meta_iter_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckSharedMetaExpectedSource);
};

// generate_next_task() 每次生成一个同级 Worker，Worker 总数读取
// DAG_PRIO_SS_DATA_CHECK 的配置上限，默认为 3。
// 所有 Worker 从共享 Source 领取 Tablet 批次。
class ObSSDataCheckExpectedCollectTask final
    : public ObSSDataCheckExpectedSortBaseTask,
      public share::ObCheckStopProvider
{
public:
  ObSSDataCheckExpectedCollectTask();
  virtual ~ObSSDataCheckExpectedCollectTask() = default;

  int init(
      ObSSDataCheckCtx *ctx,
      ObSSDataCheckSharedMetaExpectedSource *shared_meta_source);
  virtual int generate_next_task(share::ObITask *&next_task) override;
  virtual int process() override;
  virtual int check_stop() const override;

  VIRTUAL_TO_STRING_KV("task", "ObSSDataCheckExpectedCollectTask", KP(this),
      K_(worker_idx), K_(next_task_generated));

private:
  // SQL Page 与 Worker 批次使用相同的行数上限，减少频繁分页查询。
  // 原始 Meta 批次使用独立字节上限，不复用临时文件 IO Buffer。
  static const int64_t TABLET_META_PAGE_SIZE = 1024;
  static const int64_t TABLET_META_BATCH_SIZE = 16L << 20; // 16MB
  static const int64_t COLLECT_ARRAY_CAPACITY = 64;
  int init_(
      ObSSDataCheckCtx *ctx,
      ObSSDataCheckSharedMetaExpectedSource *shared_meta_source,
      const int64_t worker_idx);
  int get_worker_limit_(int64_t &worker_limit) const;
  int check_collect_available_(const ObSSDataCheckCtx &ctx) const;
  int collect_expected_(ObSSDataCheckCtx &ctx);
  int collect_expected_from_shared_meta_(
      ObSSDataCheckCtx &ctx,
      ObSSDataCheckExpectedSorter &sorter);
  int collect_tablet_row_(
      const ObSSRawMetaRow &row,
      const share::SCN &snapshot_meta_scn,
      ObSSDataCheckExpectedSorter &sorter,
      ObSSDataCheckCtx &ctx);
  static bool is_checkable_object_(
      const blocksstable::MacroBlockId &macro_id);

private:
  bool is_inited_;
  bool next_task_generated_;
  int64_t worker_idx_;
  ObSSDataCheckSharedMetaExpectedSource *shared_meta_source_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedCollectTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_EXPECTED_PRODUCER_H_
