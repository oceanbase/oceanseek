/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_lock.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::lib;

ObMutex &ObSSMacroCacheLock::get_lock_by_macro_id(const MacroBlockId &macro_id)
{
  const uint64_t idx = (macro_id.hash() % LOCK_NUM);
  return locks_[idx].lock_;
}

ObSSMacroCacheLockGuard::ObSSMacroCacheLockGuard(
    const MacroBlockId &macro_id,
    ObSSMacroCacheMgr &macro_cache_mgr)
  : guard_(macro_cache_mgr.get_lock_by_macro_id(macro_id))
{
}

} // namespace storage
} // namespace oceanbase
