/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_LOCK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_LOCK_H_

#include "lib/lock/ob_mutex.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
}
namespace storage
{
class ObSSMacroCacheMgr;

class ObSSMacroCacheLock
{
public:
  ObSSMacroCacheLock() : locks_()
  {
  }
  virtual ~ObSSMacroCacheLock() {}
  lib::ObMutex &get_lock_by_macro_id(const blocksstable::MacroBlockId &macro_id);

private:
  struct MacroCacheLockWrapper {
    lib::ObMutex lock_;
    MacroCacheLockWrapper() : lock_(common::ObLatchIds::OB_SS_MACRO_CACHE_LOCK) {}
  };
  static const uint64_t LOCK_NUM = 4096;
  MacroCacheLockWrapper locks_[LOCK_NUM];
};

class ObSSMacroCacheLockGuard
{
public:
  ObSSMacroCacheLockGuard(const blocksstable::MacroBlockId &macro_id,
                          ObSSMacroCacheMgr &macro_cache_mgr);
  virtual ~ObSSMacroCacheLockGuard() {}

private:
  lib::ObMutexGuard guard_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_LOCK_H_ */
