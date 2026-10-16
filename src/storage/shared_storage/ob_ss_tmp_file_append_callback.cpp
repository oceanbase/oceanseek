/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_tmp_file_append_callback.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;

int ObSSTmpFileEvictCallback::process_on_appending(TmpFileMeta &meta)
{
  int ret = OB_SUCCESS;
  UNUSED(meta);
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(macro_cache_mgr->push_to_flush_map(macro_id_, 0/*ls_epoch_id*/))) {
    LOG_WARN("fail to push to flush map", KR(ret), K_(macro_id));
  }
  return ret;
}

int ObSSTmpFileEvictCallback::process_on_non_appending(TmpFileMeta &meta)
{
  int ret = OB_SUCCESS;
  // 1. flushed sealed seg: delete seg_meta, and delete local file
  if (meta.is_flushed_seg_sealed_) {
    // Note: must set is_in_local_ = false, otherwise concurrent read may fail to read local file.
    // e.g., concurrent read get meta with is_in_local = true, then here delete local file,
    // finally concurrent read fail to read local file.
    meta.is_in_local_ = false;
    if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(macro_id_, 0/*ls_epoch_id*/,
                true/*is_print_log*/, true/*is_del_seg_meta*/, false/*is_logical_delete*/))) {
      LOG_WARN("fail to delete local file", KR(ret), K_(macro_id));
    }
  } else { // 2. flushed unsealed seg: update seg_meta's is_in_local to false, and delete local file
    meta.is_in_local_ = false; // update seg meta
    if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(macro_id_, 0/*ls_epoch_id*/,
                true/*is_print_log*/, false/*is_del_seg_meta*/, false/*is_logical_delete*/))) {
      LOG_WARN("fail to delete local file", KR(ret), K_(macro_id));
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
