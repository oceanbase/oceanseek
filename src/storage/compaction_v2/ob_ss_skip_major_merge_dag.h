// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_SS_SKIP_MAJOR_MERGE_DAG_H_
#define OB_STORAGE_COMPACTION_SS_SKIP_MAJOR_MERGE_DAG_H_
#include "share/compaction/ob_batch_exec_dag.h"
namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObLS;
class ObTableHandleV2;
class ObCOSSTableV2;
}
namespace blocksstable
{
class ObSSTable;
}
namespace compaction
{

struct ObUpdateSkipMajorParam : public ObBatchExecParam<ObTabletID>
{
  ObUpdateSkipMajorParam()
    : ObBatchExecParam(UPDATE_SKIP_MAJOR_TABLET)
  {}
  ObUpdateSkipMajorParam(
    const share::ObLSID &ls_id,
    const int64_t merge_version)
    : ObBatchExecParam(UPDATE_SKIP_MAJOR_TABLET, ls_id, merge_version, DEFAULT_BATCH_SIZE)
  {}
  virtual ~ObUpdateSkipMajorParam() {}
  static const int64_t DEFAULT_BATCH_SIZE = 32;
};

struct ObUpdateTabletCnt
{
  ObUpdateTabletCnt()
    : ss_cnt_(0),
      local_cnt_(0)
  {}
  TO_STRING_KV(K_(ss_cnt), K_(local_cnt));
  int64_t ss_cnt_;
  int64_t local_cnt_;
};

class ObSSSkipMajorMergeTask : public ObBatchExecTask<ObSSSkipMajorMergeTask, ObUpdateSkipMajorParam>
{
public:
  ObSSSkipMajorMergeTask();
  virtual int inner_process() override;
private:
  int update_skip_major_tablet_meta(
    common::ObArenaAllocator &allocator,
    storage::ObLS &ls,
    const int64_t compaction_scn,
    const ObTabletID &tablet_id,
    int64_t &batch_finished_data_size);
  int prep_new_major_sstable(
    common::ObArenaAllocator &allocator,
    blocksstable::ObSSTable &old_major,
    const int64_t compaction_scn,
    ObTableHandleV2 &new_major_handle);
  int update_tablet(
    common::ObArenaAllocator &allocator,
    ObLS &ls,
    const ObTablet &old_tablet,
    blocksstable::ObSSTable &last_major,
    const int64_t compaction_scn,
    blocksstable::ObSSTable *last_shared_major);
  int do_update_tablet(
    common::ObArenaAllocator &allocator,
    ObLS &ls,
    const ObTablet &old_tablet,
    const blocksstable::ObSSTable &new_major,
    const blocksstable::ObSSTable *new_shared_major);
  int generate_new_co_sstable(
    const int64_t compaction_scn,
    storage::ObCOSSTableV2 &last_major);
  int update_sstable(
    const int64_t compaction_scn,
    blocksstable::ObSSTable &new_major);
  int prepare_storage_schema_(
    const ObTablet &tablet,
    const int64_t compaction_scn,
    common::ObArenaAllocator &allocator,
    ObStorageSchema *&storage_schema);
private:
  ObUpdateTabletCnt update_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObSSSkipMajorMergeTask);
};

class ObSSSkipMajorMergeDag : public ObBatchExecDag<ObSSSkipMajorMergeTask, ObUpdateSkipMajorParam>
{
public:
  ObSSSkipMajorMergeDag()
    : ObBatchExecDag(share::ObDagType::DAG_TYPE_UPDATE_SKIP_MAJOR)
  {}
  virtual ~ObSSSkipMajorMergeDag() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSSkipMajorMergeDag);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_UPDATE_SKIP_MAJOR_TABLET_DAG_H_
