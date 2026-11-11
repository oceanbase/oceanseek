/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_DISK_BBQ_CACHE_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_DISK_BBQ_CACHE_TASK_H_

#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
}
namespace share
{

class ObDiskBBQCacheTaskUtil
{
public:
  static int check_sstable_exists(storage::ObLS &ls,
                                  const ObIvfCacheMgrKey &cache_key,
                                  bool &exists);
};

// One standard vector async task for a Tablet's DiskBBQ SSTable caches. A
// CLEAN task checks every cached TableKey of the Tablet; an HGraph task builds
// every TableKey that was pending when the task took its cache-map snapshot.
class ObDiskBBQCacheAsyncTask final : public ObVecIndexIAsyncTask
{
public:
  ObDiskBBQCacheAsyncTask()
    : ObVecIndexIAsyncTask(ObMemAttr(MTL_ID(), "DskBBQCacheTsk"))
  {}
  virtual ~ObDiskBBQCacheAsyncTask() = default;

  int do_work() override;

private:
  int clean_invalid_caches_();
  int build_pending_hgraphs_();
  int process_one_hgraph_(const ObIvfCacheMgrKey &cache_key);
  int build_one_hgraph_(ObIvfCacheMgr &cache_mgr,
                        ObIvfSSTableCentCache &cache,
                        bool &built);

  DISALLOW_COPY_AND_ASSIGN(ObDiskBBQCacheAsyncTask);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_DISK_BBQ_CACHE_OB_DISK_BBQ_CACHE_TASK_H_
