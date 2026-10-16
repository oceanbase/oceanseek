/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OB_SS_COMPACT_OBJECT_CLEANER_H_
#define OB_SS_COMPACT_OBJECT_CLEANER_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"

namespace oceanbase
{
namespace blocksstable
{

class ObSSTableSharedObjectCacheCleaner final : public ObISSTableObjectCleaner
{
public:
  static int build(const ObDataStoreDesc &data_store_desc, ObIAllocator &allocator, ObISSTableObjectCleaner *&cleaner);
private:
  static const int64_t CLEANER_MACRO_ID_LOCAL_ARRAY_SIZE = 32;
public:
  ObSSTableSharedObjectCacheCleaner();
  ~ObSSTableSharedObjectCacheCleaner();
  void reset() override;
  int add_new_macro_block_id(const MacroBlockId &macro_id) override;
  int mark_succeed() override;
  VIRTUAL_TO_STRING_KV(K_(new_macro_block_ids), K_(task_succeed));
private:
  void clean_();
  ObSpinLock lock_;
  bool task_succeed_;
  ObSEArray<MacroBlockId, CLEANER_MACRO_ID_LOCAL_ARRAY_SIZE> new_macro_block_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableSharedObjectCacheCleaner);
};

} // namespace blocksstable
} // namespace oceanbase

#endif