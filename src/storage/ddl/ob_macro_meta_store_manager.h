/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_DDL_OB_MACRO_META_STORE_MANAGER_H_
#define _OCEANBASE_STORAGE_DDL_OB_MACRO_META_STORE_MANAGER_H_

#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{

namespace blocksstable
{
class ObMacroMetaTempStore;
}

namespace storage
{

class ObMacroMetaStoreManager
{
public:
  struct StoreItem
  {
  public:
    StoreItem()
      : macro_meta_store_(nullptr), cg_idx_(-1), parallel_idx_(-1),
        is_lob_store_(false), lob_col_idx_(OB_INVALID_INDEX_INT64),
        lob_id_interval_start_(0), lob_id_interval_end_(0)
    {}
    TO_STRING_KV(KP(macro_meta_store_), K(tablet_id_), K(cg_idx_),
                 K(parallel_idx_), K(is_lob_store_), K(lob_col_idx_),
                 K(lob_id_interval_start_),
                 K(lob_id_interval_end_));
  public:
    blocksstable::ObMacroMetaTempStore *macro_meta_store_;
    ObTabletID tablet_id_; // for distinguish data tablet and lob meta tablet
    int64_t cg_idx_;
    int64_t parallel_idx_;
    bool is_lob_store_;
    int64_t lob_col_idx_;
    uint64_t lob_id_interval_start_;
    uint64_t lob_id_interval_end_;
  };
public:
  ObMacroMetaStoreManager();
  ~ObMacroMetaStoreManager();
  int add_macro_meta_store(
      const ObTabletID &tablet_id,
      const int64_t cg_idx,
      const int64_t parallel_idx,
      blocksstable::ObMacroMetaTempStore *&macro_meta_store);
  // LOB 辅助表按列级 writer 和 LobId 区间排序，parent_slice_idx 仅用于任务归属和诊断。
  int add_lob_macro_meta_store(
      const ObTabletID &tablet_id,
      const int64_t cg_idx,
      const int64_t parent_slice_idx,
      const int64_t lob_col_idx,
      const uint64_t lob_id_interval_start,
      const uint64_t lob_id_interval_end,
      blocksstable::ObMacroMetaTempStore *&macro_meta_store);
  int get_sorted_macro_meta_stores(const ObTabletID &tablet_id, const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores);
  int get_sorted_lob_macro_meta_stores(
      const ObTabletID &tablet_id,
      const int64_t cg_idx,
      ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores);

  int add_macro_meta_store(const int64_t cg_idx, const int64_t parallel_idx, const int64_t dir_id, blocksstable::ObMacroMetaTempStore *&macro_meta_store);
  int get_sorted_macro_meta_stores(const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores);
  int get_macro_meta_store(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, blocksstable::ObMacroMetaTempStore *&macro_meta_store);

  TO_STRING_KV(K(dir_id_), K(store_items_.count()));
private:
  int add_store_item(
      const ObTabletID &tablet_id,
      const int64_t cg_idx,
      const int64_t parallel_idx,
      const bool is_lob_store,
      const int64_t lob_col_idx,
      const uint64_t lob_id_interval_start,
      const uint64_t lob_id_interval_end,
      const int64_t dir_id,
      blocksstable::ObMacroMetaTempStore *&macro_meta_store);
  int get_sorted_macro_meta_stores(
      const ObTabletID &tablet_id,
      const int64_t cg_idx,
      const bool is_lob_store,
      ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores);
private:
  ObArenaAllocator allocator_;
  lib::ObMutex mutex_;
  int64_t dir_id_;
  ObArray<StoreItem> store_items_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif//_OCEANBASE_STORAGE_DDL_OB_MACRO_META_STORE_MANAGER_H_
