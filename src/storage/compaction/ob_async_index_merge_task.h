/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_TASK_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_ASYNC_INDEX_MERGE_TASK_H_

#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_dag.h"
#include "storage/blocksstable/async_index/ob_partition_inv_sstable_merger.h"

namespace oceanbase
{
namespace compaction
{

class ObAsyncIndexMergePrepareTask : public share::ObITask
{
public:
  ObAsyncIndexMergePrepareTask();
  virtual ~ObAsyncIndexMergePrepareTask() = default;
  int init();
protected:
  virtual int process() override;
private:
  bool is_inited_;
  ObTabletMergeDag *merge_dag_;
};

class ObAsyncIndexMergeFinishTask : public share::ObITask
{
public:
  ObAsyncIndexMergeFinishTask();
  virtual ~ObAsyncIndexMergeFinishTask() = default;
  int init();
protected:
  virtual int process() override;
private:
  bool is_inited_;
  ObTabletMergeDag *merge_dag_;
};

class ObAsyncIndexMergeTask : public share::ObITask
{
public:
  ObAsyncIndexMergeTask();
  virtual ~ObAsyncIndexMergeTask();
  int init(const int64_t idx, ObAsyncIndexMergeCtx &ctx);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int64_t get_sub_task_id() const override { return idx_; }
private:
  compaction::ObLocalArena allocator_;
  int64_t idx_;
  ObAsyncIndexMergeCtx *ctx_;
  blocksstable::ObPartitionInvSSTableMerger *merger_;
  bool is_inited_;
};

class ObAsyncIndexMergeDag : public ObTabletMergeDag
{
public:
  ObAsyncIndexMergeDag(const share::ObDagType::ObDagTypeEnum type);
  virtual ~ObAsyncIndexMergeDag() override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int generate_merge_task(ObBasicTabletMergeCtx &ctx, share::ObITask *prepare_task) override;

private:
  int generate_inv_merge_task(ObAsyncIndexMergeCtx &ctx, share::ObITask *prepare_task);
  ObIVFMergeDagCtx ivf_merge_dag_ctx_;
};

class ObAsyncIndexMajorMergeDag : public ObAsyncIndexMergeDag
{
public:
  ObAsyncIndexMajorMergeDag() : ObAsyncIndexMergeDag(share::ObDagType::DAG_TYPE_ASYNC_INDEX_MAJOR_MERGE) {}
};

class ObAsyncIndexIncMergeDag : public ObAsyncIndexMergeDag
{
public:
  ObAsyncIndexIncMergeDag() : ObAsyncIndexMergeDag(share::ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE) {}
};

} // namespace compaction
} // namespace oceanbase

#endif
