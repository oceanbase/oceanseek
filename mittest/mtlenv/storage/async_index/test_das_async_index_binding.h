/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_UNITTEST_DAS_ASYNC_INDEX_BINDING_H_
#define OCEANBASE_UNITTEST_DAS_ASYNC_INDEX_BINDING_H_

#include "sql/das/search/ob_das_async_index_query_ctx.h"

namespace oceanbase
{
namespace unittest
{

class MockDASAsyncIndexBinding
{
public:
  MockDASAsyncIndexBinding()
    : partition_(), version_range_()
  {}

  int init(sql::ObDASSearchCtx &search_ctx,
           const share::ObLSID &ls_id,
           const uint64_t index_table_id,
           const common::ObTabletID &index_tablet_id,
           const int64_t base_version,
           const int64_t snapshot_version)
  {
    int ret = OB_SUCCESS;
    typedef storage::ObAsyncIndexSegmentGroup StorageSegmentGroup;
    StorageSegmentGroup *group = nullptr;
    bool group_added = false;
    version_range_.base_version_ = base_version;
    version_range_.snapshot_version_ = snapshot_version;
    if (OB_UNLIKELY(!ls_id.is_valid()
                    || common::OB_INVALID_ID == index_table_id
                    || !index_tablet_id.is_valid()
                    || !version_range_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      search_ctx.set_ls_id(ls_id);
      partition_.ls_id_ = ls_id;
      if (OB_FAIL(partition_.index_table_ids_.push_back(index_table_id))) {
      } else if (OB_FAIL(partition_.index_tablet_ids_.push_back(index_tablet_id))) {
      } else if (OB_FAIL(partition_.storage_group_ctx_.async_index_read_snapshot_
                             .convert_for_tx(snapshot_version))) {
      } else if (OB_ISNULL(group = OB_NEWx(
                     StorageSegmentGroup,
                     &partition_.storage_group_ctx_.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(group->init(1))) {
      } else if (OB_FAIL(group->add_version_range(0, version_range_))) {
      } else if (OB_FAIL(partition_.storage_group_ctx_.groups_.push_back(group))) {
      } else {
        group_added = true;
        partition_.storage_group_ctx_.is_strict_aligned_ = true;
        search_ctx.set_current_async_partition(&partition_);
      }
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(group) && !group_added) {
      group->reset();
      OB_DELETEx(StorageSegmentGroup,
                 &partition_.storage_group_ctx_.allocator_,
                 group);
    }
    return ret;
  }

  const common::ObNewVersionRange &get_version_range() const
  {
    return version_range_;
  }

private:
  sql::ObAsyncIndexPartitionSegCtx partition_;
  common::ObNewVersionRange version_range_;
};

} // namespace unittest
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_DAS_ASYNC_INDEX_BINDING_H_
