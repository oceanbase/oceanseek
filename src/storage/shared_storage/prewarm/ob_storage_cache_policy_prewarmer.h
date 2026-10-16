/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_STORAGE_CACHE_POLICY_PREWARMER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_STORAGE_CACHE_POLICY_PREWARMER_H_

#include "storage/tx_storage/ob_ls_service.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/index_block/ob_index_block_bare_iterator.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"

namespace oceanbase
{
namespace storage
{

class ObStorageCacheTabletTask;

// Helper class to calculate policy status for macro blocks
// This class caches the threshold values and column info to avoid repeated calculation in loops
class ObPolicyStatusCalculator
{
public:
  ObPolicyStatusCalculator();
  ~ObPolicyStatusCalculator() = default;
  int init(const ObStorageCachePolicy &storage_cache_policy);
  int get_macro_block_status(const blocksstable::ObDataMacroBlockMeta &macro_meta,
                             PolicyStatus &macro_block_policy_status) const;
  bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K_(is_inited), K_(hot_threshold_ns), K_(cold_threshold_ns), K_(col_idx), K_(col_type),
               K_(storage_cache_policy));
private:
  int init_column_info_();
  int cal_macro_block_status_by_skip_index_(const blocksstable::ObDataMacroBlockMeta &macro_meta,
                                            PolicyStatus &policy_status) const;
  static int read_skip_index_min_max_(const char *agg_row_buf,
                                      const int64_t agg_row_len,
                                      const int64_t col_idx,
                                      blocksstable::ObStorageDatum &min_datum,
                                      blocksstable::ObStorageDatum &max_datum);
private:
  bool is_inited_;
  int64_t hot_threshold_ns_;
  int64_t cold_threshold_ns_;
  // For custom block time policy (skip index based)
  int64_t col_idx_;
  common::ObObjMeta col_type_;
  ObStorageCachePolicy storage_cache_policy_;
};

class ObStorageCachePolicyHelper
{
public:
  ObStorageCachePolicyHelper();
  ~ObStorageCachePolicyHelper();
  static int is_storage_cache_task_suspended(const uint64_t tenant_id, bool &is_suspend);
};

/*
 * @brief prewarm a tablet by query range the wrapper function 
 *        of ObStorageCachePolicyPrewarmer prewarm_tablet_by_query_range
 */
int prewarm_tablet_by_query_range(const int64_t ls_id,
                                  const int64_t tablet_id,
                                  const ObNewRange &query_range);


// ObStorageCachePolicyPrewarmer
//   Executes tablet prewarming: collects macro blocks from a tablet
//   (optionally with filters) and prewarms them.
//   Currently supports:
//     1. prewarming an entire tablet
//     2. prewarming a tablet filtered by time policy
//     3. prewarming a tablet filtered by query range
//
// Usage:
//   ObStorageCachePolicyPrewarmer prewarmer;
//   prewarmer.set_tablet_id(tablet_id);
//   prewarmer.set_ls_id(ls_id);
//   OK(prewarmer.prewarm_tablet());
//   ObSSPrewarmStat stat = prewarmer.get_hot_retention_prewarm_stat();
//
// TODO:
//   1. Move filter-related logic to task layer.
//      The prewarmer should only be responsible for prewarming.
class ObStorageCachePolicyPrewarmer: public ObSSBasePrewarmer
{
public:
  ObStorageCachePolicyPrewarmer(const int64_t parallelism = MAX_PARALLELISM);
  virtual ~ObStorageCachePolicyPrewarmer();
  void reset();
  void reset_io_resources();
  void set_tablet_id(const int64_t tablet_id);
  void set_ls_id(const int64_t ls_id);
  int prewarm_tablet(ObStorageCacheTabletTask *task);
  // Backward-compatible wrappers for historical test callers.
  int batch_read_wait() { return batch_read_wait_(); }
  /*
   * @brief Prewarms a tablet filtered by query range. All macro blocks within the query range 
   *        are prewarmed as a AUTO status. If prewarm fails for any block, the entire operation 
   *        fails.
   */
  int prewarm_tablet_by_query_range(const ObNewRange &query_range);
  
  int filter_tablet_macro_blocks(ObIArray<MacroBlockId> &hot_data_block_ids, 
                                 ObIArray<MacroBlockId> &meta_block_ids,
                                 ObIArray<MacroBlockId> &cold_data_block_ids,
                                 const ObStorageCacheTabletTask *task);

protected:
  // 预热指定 LS 中的单个 tablet 实例，普通 task 和 LS inner tablet group task 共用该入口。
  int prewarm_one_tablet_(
      const share::ObLSID &ls_id,
      ObStorageCacheTabletTask *task);
  // 遍历当前全部 LS，预热同一 tablet_id 对应的所有 LS inner tablet 实例。
  int prewarm_ls_inner_tablet_group_(ObStorageCacheTabletTask *task);
  int prewarm_partition_task_(ObStorageCacheTabletTask *task);
  int prewarm_block_task_(ObStorageCacheTabletTask *task);
  int prewarm_blocks_to_cache_(const ObIArray<MacroBlockId> &block_ids,
                               ObStorageCacheTabletTask *task,
                               const bool is_hot_block);
  /*
   * @brief Used for tablet prewarm by query range. Unlike prewarm_blocks_to_cache(block_ids, 
   *        task), this path is fail-fast: it returns an error if any block read or cache write     
   *        fails.
   */
  int prewarm_blocks_to_cache_(const ObIArray<MacroBlockId> &block_ids);
  int batch_read_from_object_storage_with_retry_(const ObIArray<blocksstable::MacroBlockId> &block_ids,
                                                const int64_t start_idx,
                                                const int64_t batch_size,
                                                const PolicyStatus &policy_status);
  int async_read_from_object_storage_(const blocksstable::MacroBlockId &macro_id);
  int batch_prewarm_macro_blocks_(const bool is_hot_block);
  int prewarm_data_macro_(const MacroBlockId &macro_id,
                          const char *macro_block_buf,
                          const int64_t macro_block_size);
  int batch_read_wait_();
  int batch_write_wait_();
  int filter_sstable_meta_macro_blocks_(const ObSSTable *sstable, 
                                        ObIArray<MacroBlockId> &meta_block_ids);
  int filter_sstable_data_macro_blocks_(const ObSSTable *sstable,
                                        const ObStorageCacheTabletTask *task,
                                        const ObTabletHandle &tablet_handle,
                                        ObIArray<MacroBlockId> &hot_block_ids,
                                        ObIArray<MacroBlockId> &cold_block_ids);
  int filter_tablet_by_query_range_(const ObNewRange &query_range,
                                    ObIArray<MacroBlockId> &block_ids);
  int filter_sstable_by_query_range_(ObSSTable *sstable,
                                     const ObNewRange &query_range,
                                     const ObTabletHandle &tablet_handle,
                                     ObIArray<MacroBlockId> &block_ids);
  // @TODO: this funciton need to be own by ObStorageCacheTabletTask
  int check_task_status_(ObStorageCacheTabletTask *task) const;
protected:
  static const int64_t MAX_PARALLELISM = 64;
  static const int64_t MIN_PARALLELISM = 1;

  common::ObArenaAllocator allocator_;
  const int64_t parallel_num_;
  int64_t read_handle_idx_;
  int64_t write_handle_idx_;

  blocksstable::ObStorageObjectHandle read_handles_[MAX_PARALLELISM];
  blocksstable::ObStorageObjectHandle write_handles_[MAX_PARALLELISM];
  int64_t write_sizes_[MAX_PARALLELISM]; // record write size, for free disk space on write fail
  ObTabletID tablet_id_;
  ObLSID ls_id_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_STORAGE_CACHE_POLICY_PREWARMER_H_ */
