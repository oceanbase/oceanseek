/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

ObMacroMetaStoreManager::ObMacroMetaStoreManager()
  : allocator_(ObMemAttr(MTL_ID(), "MbMetaStoreMgr")), mutex_(ObLatchIds::COLUMN_STORE_DDL_RESCAN_LOCK), dir_id_(-1)
{

}

ObMacroMetaStoreManager::~ObMacroMetaStoreManager()
{
  for (int64_t i = 0; i < store_items_.count(); ++i) {
    ObMacroMetaTempStore *&cur_store = store_items_.at(i).macro_meta_store_;
    if (nullptr != cur_store) {
      cur_store->~ObMacroMetaTempStore();
      cur_store = nullptr;
    }
  }
  store_items_.reset();
}

int ObMacroMetaStoreManager::add_macro_meta_store(
    const ObTabletID &tablet_id,
    const int64_t cg_idx,
    const int64_t parallel_idx,
    blocksstable::ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  if (dir_id_ < 0) {
    ObMutexGuard guard(mutex_);
    if (dir_id_ < 0) {
      if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id_))) {
        LOG_WARN("alloc dir id failed", K(MTL_ID()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_store_item(
        tablet_id, cg_idx, parallel_idx, false/*is_lob_store*/,
        OB_INVALID_INDEX_INT64, 0/*lob_id_interval_start*/,
        0/*lob_id_interval_end*/, dir_id_, macro_meta_store))) {
      LOG_WARN("add macro meta store failed", K(ret), K(dir_id_));
    }
  }
  return ret;
}

int ObMacroMetaStoreManager::add_lob_macro_meta_store(
    const ObTabletID &tablet_id,
    const int64_t cg_idx,
    const int64_t parent_slice_idx,
    const int64_t lob_col_idx,
    const uint64_t lob_id_interval_start,
    const uint64_t lob_id_interval_end,
    blocksstable::ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  if (dir_id_ < 0) {
    ObMutexGuard guard(mutex_);
    if (dir_id_ < 0
        && OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.alloc_dir(MTL_ID(), dir_id_))) {
      LOG_WARN("alloc dir id failed", K(ret), K(MTL_ID()));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(add_store_item(
      tablet_id, cg_idx, parent_slice_idx, true/*is_lob_store*/,
      lob_col_idx, lob_id_interval_start, lob_id_interval_end,
      dir_id_, macro_meta_store))) {
    LOG_WARN("add lob macro meta store failed", K(ret), K(tablet_id),
             K(cg_idx), K(parent_slice_idx), K(lob_col_idx),
             K(lob_id_interval_start), K(lob_id_interval_end));
  }
  return ret;
}

int ObMacroMetaStoreManager::add_macro_meta_store(const int64_t cg_idx, const int64_t parallel_idx, const int64_t dir_id, ObMacroMetaTempStore *&macro_meta_store)
{
  static ObTabletID dummy_tablet_id;
  return add_store_item(
      dummy_tablet_id, cg_idx, parallel_idx, false/*is_lob_store*/,
      OB_INVALID_INDEX_INT64, 0/*lob_id_interval_start*/,
      0/*lob_id_interval_end*/, dir_id, macro_meta_store);
}

int ObMacroMetaStoreManager::add_store_item(
    const ObTabletID &tablet_id,
    const int64_t cg_idx,
    const int64_t parallel_idx,
    const bool is_lob_store,
    const int64_t lob_col_idx,
    const uint64_t lob_id_interval_start,
    const uint64_t lob_id_interval_end,
    const int64_t dir_id,
    ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  macro_meta_store = nullptr;
  if (OB_UNLIKELY(cg_idx < 0
      || parallel_idx < 0
      || dir_id < 0
      || (is_lob_store
          && (lob_col_idx < 0
              || lob_id_interval_start > lob_id_interval_end)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_idx), K(parallel_idx),
             K(is_lob_store), K(lob_col_idx), K(lob_id_interval_start),
             K(lob_id_interval_end), K(dir_id));
  } else {
    ObMutexGuard guard(mutex_);
    StoreItem item;
    item.tablet_id_ = tablet_id;
    item.cg_idx_ = cg_idx;
    item.parallel_idx_ = parallel_idx;
    item.is_lob_store_ = is_lob_store;
    item.lob_col_idx_ = lob_col_idx;
    item.lob_id_interval_start_ = lob_id_interval_start;
    item.lob_id_interval_end_ = lob_id_interval_end;
    if (OB_ISNULL(item.macro_meta_store_ = OB_NEWx(ObMacroMetaTempStore, &allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(item.macro_meta_store_->init(dir_id))) {
      LOG_WARN("init macro meta store failed", K(ret), K(dir_id));
    } else if (OB_FAIL(store_items_.push_back(item))) {
      LOG_WARN("push back store item failed", K(ret));
    } else {
      macro_meta_store = item.macro_meta_store_;
    }
    if (OB_FAIL(ret) && nullptr != item.macro_meta_store_) {
      item.macro_meta_store_->~ObMacroMetaTempStore();
      allocator_.free(item.macro_meta_store_);
      item.macro_meta_store_ = nullptr;
    }
  }
  return ret;
}

int ObMacroMetaStoreManager::get_sorted_macro_meta_stores(const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores) 
{
  static ObTabletID dummy_tablet_id;
  return get_sorted_macro_meta_stores(
      dummy_tablet_id, cg_idx, false/*is_lob_store*/, macro_meta_stores);
}

int ObMacroMetaStoreManager::get_sorted_macro_meta_stores(const ObTabletID &tablet_id, const int64_t cg_idx, ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores)
{
  return get_sorted_macro_meta_stores(
      tablet_id, cg_idx, false/*is_lob_store*/, macro_meta_stores);
}

int ObMacroMetaStoreManager::get_sorted_lob_macro_meta_stores(
    const ObTabletID &tablet_id,
    const int64_t cg_idx,
    ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores)
{
  return get_sorted_macro_meta_stores(
      tablet_id, cg_idx, true/*is_lob_store*/, macro_meta_stores);
}

int ObMacroMetaStoreManager::get_sorted_macro_meta_stores(
    const ObTabletID &tablet_id,
    const int64_t cg_idx,
    const bool is_lob_store,
    ObIArray<ObMacroMetaStoreManager::StoreItem> &macro_meta_stores)
{
  int ret = OB_SUCCESS;
  macro_meta_stores.reset();
  ObArray<StoreItem> temp_items;
  if (OB_UNLIKELY(cg_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < store_items_.count(); ++i) {
      if (cg_idx == store_items_.at(i).cg_idx_
          && tablet_id == store_items_.at(i).tablet_id_
          && is_lob_store == store_items_.at(i).is_lob_store_) {
        if (OB_FAIL(temp_items.push_back(store_items_.at(i)))) {
          LOG_WARN("push back store item failed", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && temp_items.count() > 0) {
    struct StoreItemCmp {
      explicit StoreItemCmp(const bool is_lob_store)
        : is_lob_store_(is_lob_store)
      {}
      bool operator () (const StoreItem &a, const StoreItem &b)
      {
        bool less = false;
        if (!is_lob_store_) {
          less = a.parallel_idx_ < b.parallel_idx_;
        } else if (a.lob_col_idx_ != b.lob_col_idx_) {
          less = a.lob_col_idx_ < b.lob_col_idx_;
        } else if (a.lob_id_interval_start_ != b.lob_id_interval_start_) {
          less = a.lob_id_interval_start_ < b.lob_id_interval_start_;
        } else {
          less = a.parallel_idx_ < b.parallel_idx_;
        }
        return less;
      }
      bool is_lob_store_;
    } store_item_cmp(is_lob_store);
    lib::ob_sort(temp_items.begin(), temp_items.end(), store_item_cmp);
    if (OB_FAIL(macro_meta_stores.reserve(temp_items.count()))) {
      LOG_WARN("reserve macro meta stores failed", K(ret), K(temp_items.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_items.count(); ++i) {
      ObMacroMetaTempStore *cur_store = temp_items.at(i).macro_meta_store_;
      if (OB_ISNULL(cur_store)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current macro meta store is null", K(ret), K(i), K(temp_items.at(i)));
      } else if (is_lob_store
                 && i > 0
                 && temp_items.at(i - 1).lob_col_idx_ == temp_items.at(i).lob_col_idx_
                 && temp_items.at(i).lob_id_interval_start_
                    <= temp_items.at(i - 1).lob_id_interval_end_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lob macro meta store intervals overlap",
                 K(ret), "previous", temp_items.at(i - 1),
                 "current", temp_items.at(i));
      } else if (OB_FAIL(macro_meta_stores.push_back(temp_items.at(i)))) {
        LOG_WARN("push back macro meta store failed", K(ret), K(i), KPC(cur_store));
      }
    }
  }
  return ret;
}

int ObMacroMetaStoreManager::get_macro_meta_store(const ObTabletID &tablet_id, const int64_t cg_idx, const int64_t parallel_idx, blocksstable::ObMacroMetaTempStore *&macro_meta_store)
{
  int ret = OB_SUCCESS;
  macro_meta_store = nullptr;
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; i < store_items_.count(); ++i) {
    if (!store_items_.at(i).is_lob_store_ &&
        tablet_id == store_items_.at(i).tablet_id_ &&
        cg_idx == store_items_.at(i).cg_idx_ &&
        parallel_idx == store_items_.at(i).parallel_idx_) {
      macro_meta_store = store_items_.at(i).macro_meta_store_;
      break;
    }
  }
  if (nullptr == macro_meta_store) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}
