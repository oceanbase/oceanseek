// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef STORAGE_COMPACTION_OB_TABLET_SS_MAJOR_MERGE_CTX_H_
#define STORAGE_COMPACTION_OB_TABLET_SS_MAJOR_MERGE_CTX_H_
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/column_store/ob_co_merge_ctx.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"

namespace oceanbase
{
namespace compaction
{
struct ObSSTabletMajorMergeCtx : public ObTabletMajorMergeCtx
{
public:
  ObSSTabletMajorMergeCtx(ObTabletMergeDagParam &param,
                          common::ObArenaAllocator &allocator)
    : ObTabletMajorMergeCtx(param, allocator),
      task_cnt_(0),
      sswriter_epoch_(0),
      pre_warm_writer_(param.tablet_id_.id(), param.merge_version_, param.reorganization_scn_),
      major_pre_warm_param_(pre_warm_writer_)
  {}
  virtual ~ObSSTabletMajorMergeCtx() { destroy(); }
  void destroy();
  virtual ObStorageObjectWriteStrategy get_io_write_strategy() const override;
  virtual int generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq) override;
  virtual int get_index_tree_macro_start_seq(int64_t &macro_start_seq) const override;
  virtual int init_tablet_merge_info() override;
  virtual const share::ObPreWarmerParam &get_pre_warm_param() const override { return major_pre_warm_param_; }
  virtual int update_tablet_after_merge() override;
private:
  int task_cnt_;
  int64_t sswriter_epoch_;
  storage::ObHotTabletInfoWriter pre_warm_writer_;
  ObMajorPreWarmerParam major_pre_warm_param_;
  storage::ObSSTableAtomicProtocol sstable_protocol_;
};
struct ObSSCOTabletMergeCtx : public ObCOTabletMergeCtx
{
  ObSSCOTabletMergeCtx(
    ObIDagNet &dag_net,
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
    : ObCOTabletMergeCtx(dag_net, param, allocator),
      task_cnt_(0),
      sswriter_epoch_(0),
      pre_warm_writer_(param.tablet_id_.id(), param.merge_version_, param.reorganization_scn_),
      major_pre_warm_param_(pre_warm_writer_),
      update_tablet_failed_times_(0)
  {}
  virtual ~ObSSCOTabletMergeCtx() { destroy(); }
  void destroy();
  virtual ObStorageObjectWriteStrategy get_io_write_strategy() const override;
  virtual int init_tablet_merge_info() override;
  virtual int get_index_tree_macro_start_seq(int64_t &macro_start_seq) const override;
  virtual int update_tablet_after_merge() override;
  virtual int generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq) override;
  virtual const share::ObPreWarmerParam &get_pre_warm_param() const override { return major_pre_warm_param_; }
private:
  int task_cnt_;
  int64_t sswriter_epoch_;
  storage::ObHotTabletInfoWriter pre_warm_writer_;
  ObMajorPreWarmerParam major_pre_warm_param_;
  int update_tablet_failed_times_;
  storage::ObSSTableAtomicProtocol sstable_protocol_;
};
struct ObSSAsyncIndexMergeCtx : public ObAsyncIndexMergeCtx
{
  ObSSAsyncIndexMergeCtx(
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
    : ObAsyncIndexMergeCtx(param, allocator),
      task_cnt_(0),
      sswriter_epoch_(0),
      pre_warm_writer_(param.tablet_id_.id(), param.merge_version_, param.reorganization_scn_),
      major_pre_warm_param_(pre_warm_writer_)
  {}
  virtual ~ObSSAsyncIndexMergeCtx() { destroy(); }
  void destroy();
  virtual ObStorageObjectWriteStrategy get_io_write_strategy() const override;
  virtual int generate_macro_seq_info(const int64_t task_idx, int64_t &macro_start_seq) override;
  virtual int get_index_tree_macro_start_seq(int64_t &macro_start_seq) const override;
  virtual int init_tablet_merge_info() override;
  virtual int update_tablet_after_merge() override;
  virtual const share::ObPreWarmerParam &get_pre_warm_param() const override { return major_pre_warm_param_; }
private:
  int64_t get_data_macro_seq_slot_count() const;

private:
  int task_cnt_;
  int64_t sswriter_epoch_;
  storage::ObHotTabletInfoWriter pre_warm_writer_;
  ObMajorPreWarmerParam major_pre_warm_param_;
  storage::ObSSTableAtomicProtocol sstable_protocol_;
};
} // namespace compaction
} // namespace oceanbase

#endif
