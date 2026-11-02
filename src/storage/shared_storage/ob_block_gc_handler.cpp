/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_block_gc_handler.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/ls/ob_ls.h"
#include "share/ob_debug_sync.h" // for DEBUG_SYNC

namespace oceanbase
{
namespace storage
{

int GCBaseOP::operator()(
    const ObIArray<blocksstable::MacroBlockId> &block_ids,
    ObIArray<blocksstable::MacroBlockId> *result_block_ids)
{
  int ret = OB_SUCCESS;
  if (result_block_id_set_.created()) {
  } else if (OB_FAIL(result_block_id_set_.create(MAX(2, block_ids.count()), "BlkIdSetBkt", "BlkIdSetNode",
          OB_SERVER_TENANT_ID))) {
    LOG_WARN("failed to create macro id set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); i++) {
    bool need_insert = false;
    const blocksstable::MacroBlockId &block_id = block_ids.at(i);
    if (OB_ISNULL(block_id_set_)) {
      need_insert = true;
    } else if (OB_FAIL(block_id_set_->exist_refactored(block_id))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        need_insert = true;
      } else if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
        add_or_print_debug_block(block_id);
      } else {
        LOG_WARN("failed to exist_refactored", K(ret), K(block_id));
      }
    }
    if (OB_SUCC(ret) && need_insert) {
      if (OB_FAIL(result_block_id_set_.set_refactored(block_id, 0))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to exist_refactored", K(ret), K(block_id));
        }
      } else if (NULL != result_block_ids
                 && OB_FAIL(result_block_ids->push_back(block_id))) {
        LOG_WARN("failed to push_back", K(ret), K(block_id));
      }
    }
  }
  return ret;
}

int GCTabletMetaVersionOP::operator()(
    const ObIArray<blocksstable::MacroBlockId> &block_ids,
    ObIArray<blocksstable::MacroBlockId> &result_block_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(GCBaseOP::operator()(block_ids, &result_block_ids))) {
    LOG_WARN("failed to get gc blocks", K(ret), K(block_ids));
  } else if (!blocks_for_error_check_.created()) {
    // no need error check
  } else {
    for (int64_t i = result_block_ids.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const blocksstable::MacroBlockId &block_id = result_block_ids.at(i);
      if (OB_FAIL(blocks_for_error_check_.exist_refactored(block_id))) {
        if (OB_HASH_EXIST == ret) {
#ifndef OB_BUILD_PACKAGE
          ob_abort();
#endif
          LOG_ERROR("block_id is in unexpected major meta", K(ret), K(block_id));
          // ignore error code
          if (OB_FAIL(result_block_ids.remove(i))) {
            LOG_WARN("failed to remove block", K(ret), K(i), K(block_ids));
          }
        } else if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to exist_refactored", K(ret), K(block_id));
        }
      }
    }
  }
  return ret;
}

int PrivateMacroBlockCheckOP::filter_block(
    const blocksstable::MacroBlockId &block_id,
    bool &need_filter) const
{
  int ret = OB_SUCCESS;
  need_filter = true;
  if (!block_id.is_private_data_or_meta()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("block id is not private", K(ret), K(block_id), KPC(this));
  } else if (block_id.tenant_seq() == max_block_id_
             || block_id.tenant_seq() == min_block_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block is equal to min/max_block_id", K(ret), K(block_id), KPC(this));
  } else if (block_id.tenant_seq() < min_block_id_ || block_id.tenant_seq() > max_block_id_) {
    LOG_INFO("block_id is less than min_block_id or more than max_block_id, skip it", K(ret), K(block_id), KPC(this));
  } else {
    need_filter = false;
  }
  return ret;
}

int MacroBlockCheckOP::operator()(
    const ObIArray<blocksstable::MacroBlockId> &block_ids,
    ObIArray<blocksstable::MacroBlockId> &result_block_ids) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); i++) {
    const blocksstable::MacroBlockId &block_id = block_ids.at(i);
    bool need_filter = true;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(filter_block(block_id, need_filter))) {
      LOG_WARN("failed to filter_block", KR(ret), KR(tmp_ret), K(block_id), KPC(this));
    } else if (need_filter) {
    } else if (OB_FAIL(block_id_set_.exist_refactored(block_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        // swallow error code
        if (OB_FAIL(result_block_ids.push_back(block_id))) {
          LOG_WARN("failed to push_back", K(ret), K(block_id), KPC(this));
        }
      } else {
        LOG_WARN("failed to exist_refactored", K(ret), K(block_id), KPC(this));
      }
    }
  }
  return ret;
}

void GCTabletMetaVersionOP::print_debug_block() 
{
  if (debug_block_ids_.count() > 0) {
    FLOG_INFO("blocks is referenced by lastest tablet meta version", K(debug_block_ids_.count()), K(debug_block_ids_));
  }
}

void GCTabletMetaVersionOP::add_or_print_debug_block(
    const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  if (debug_block_ids_.count() > 50) {
    print_debug_block();
    debug_block_ids_.reset();
  } else if (OB_FAIL(debug_block_ids_.push_back(block_id))) {
    LOG_WARN("failed to push_block", K(ret), K(debug_block_ids_.count()), K(block_id));
  }
}

int ObBlockGCHandler::gc_tablet(
    const ObIArray<int64_t> &tablet_versions,
    const GCTabletType gc_type,
    const uint64_t min_block_id,
    const uint64_t max_block_id)
{
  int ret = OB_SUCCESS;
  GCTabletOP private_op;
  GCTabletOP shared_op;
  // tablet not exist need skip
  if (!is_valid() || tablet_versions.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("handler is not valid", K(ret), KPC(this));
  } else if (GCTabletType::TransferOut == gc_type) {
    // if TRANSFER_OUT, then skip gc shared macro cache. because shared macro cache will be
    // prewarmed through stream-rpc from src-observer to dest-observer during migration.
  } else { // !GCTabletType::TransferOut
    if (OB_FAIL(gc_blocks_(tablet_versions, INT64_MAX /* all tablet_versions will be deleted */,
                           true/*is_shared*/, shared_op))) {
      LOG_WARN("failed to gc_blocks", K(ret), KPC(this));
    }
  }
  if (FAILEDx(gc_blocks_(tablet_versions, INT64_MAX /* all tablet_versions will be deleted */,
                         false/*is_shared*/, private_op))) {
    LOG_WARN("failed to gc_blocks", K(ret), KPC(this));
  } 

  DEBUG_SYNC(BEFORE_GC_TABLET);

  if (FAILEDx(macro_block_check(private_op.get_result_block_id_set(), min_block_id, max_block_id))) {
    LOG_WARN("failed to macro_block_check", K(ret), KPC(this), K(max_block_id));
  } else if (OB_FAIL(try_delete_tablet_data_dir())) {
    if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("new tablet_version is writen as transfer in", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete_tablet_data_dir", K(ret), KPC(this));
    }
  } else {
    LOG_INFO("success to delete_tablet_data_dir", K(ret), KPC(this));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(delete_tablet_meta_versions_(tablet_versions, INT64_MAX /* all tablet_versions will be deleted */))) {
    LOG_WARN("failed to delete_tablet_meta_version", K(tablet_versions.count()), KPC(this));
  } else if (OB_FAIL(try_delete_tablet_meta_dir())) {
    if (OB_FILE_OR_DIRECTORY_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("new tablet_version is writen as transfer in", K(ret), KPC(this));
    } else {
      LOG_WARN("failed to delete_tablet_meta_dir", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObBlockGCHandler::delete_macro_blocks(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  LOG_TRACE("prepare delete macro blocks", KPC(this), K(block_ids));
  return storage::delete_macro_blocks(block_ids);
}

int ObBlockGCHandler::safe_delete_blocks_for_macro_check_(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  return delete_macro_blocks(block_ids);
}

int ObBlockGCHandler::macro_block_check(
    const BlockIDSet &block_set,
    const uint64_t min_block_id,
    const uint64_t max_block_id)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::MacroBlockId> block_ids;
  ObArray<blocksstable::MacroBlockId> gc_block_ids;

  PrivateMacroBlockCheckOP op(block_set, min_block_id, max_block_id);
  if (tablet_id_.is_ls_inner_tablet()) {
  } else if (OB_FAIL(get_block_ids_from_dir(block_ids))) {
    LOG_WARN("failed to get_block_ids", K(ret), KPC(this));
  } else if(OB_FAIL(op(block_ids, gc_block_ids))) {
    LOG_WARN("failed to get_gc_blocks", K(ret), KPC(this));
  } else if (gc_block_ids.count() > 0) {
    if (OB_FAIL(safe_delete_blocks_for_macro_check_(gc_block_ids))) {
      LOG_WARN("failed to delete_macro_blocks", K(ret), K(gc_block_ids.count()), KPC(this));
    } else {
      LOG_WARN("success delete error block", K(ret), K(gc_block_ids.count()), KPC(this));
    }
  }
  return ret;
}

int ObBlockGCHandler::delete_tablet_meta_versions_(
    const ObIArray<int64_t> &tablet_versions,
    int64_t gc_tablet_meta_ts)
{
  int ret = OB_SUCCESS;
  // list tablet meta in private dir by detect tablet_meta_version, so delete in reverse
  for (int64_t i = tablet_versions.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
    const int64_t tablet_version = tablet_versions.at(i);
    if (check_stop()) {
      ret = OB_SERVICE_STOPPED;
      LOG_WARN("the service is already stop, skip check", K(ret), K(tablet_id_));
    } else if (gc_tablet_meta_ts >= tablet_version
        && OB_FAIL(delete_tablet_meta_version(tablet_version))) {
      LOG_WARN("failed to delete_tablet_meta_version", K(ret), K(tablet_version), KPC(this));
    }
  }
  return ret;
}

int ObBlockGCHandler::gc_blocks_for_tablet_meta_versions_(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t min_retain_tablet_meta_version,
    const bool is_shared)
{
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> block_ids;
  BlockCollectOP collect_check_error_block_op;
  BlockIDSet exist_block_set;
  if (OB_UNLIKELY(!is_valid() || (tablet_versions.count() < 2)
      || (INT64_MAX == min_retain_tablet_meta_version)
      || (min_retain_tablet_meta_version <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is not valid", K(ret), KPC(this), K(tablet_versions.count()), K(min_retain_tablet_meta_version));
  } else if (OB_FAIL(get_blocks_for_tablet(min_retain_tablet_meta_version, is_shared, block_ids))) {
    LOG_WARN("failed to get_blocks_for_tablet", K(ret), K(is_shared), KPC(this));
  } else if (OB_FAIL(exist_block_set.create(MAX(2, block_ids.count()), "BlkIdSetBkt", "BlkIdSetNode",
             OB_SERVER_TENANT_ID))) {
    LOG_WARN("failed to create macro id set", K(ret), KPC(this));
  } else if (OB_FAIL(get_blocks_for_error_block_check(tablet_versions,
             min_retain_tablet_meta_version, collect_check_error_block_op))) {
    LOG_WARN("failed to get_blocks_for_error_block_check", K(ret), KPC(this));
  } else {
    // build hash_set
    for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); i++) {
      if (OB_FAIL(exist_block_set.set_refactored(block_ids.at(i)))) {
        LOG_WARN("fail to put macro id into set", K(ret), K(i), KPC(this));
      }
    }
    GCTabletMetaVersionOP op(exist_block_set, collect_check_error_block_op.get_result_block_id_set());
    // delete tablet meta versions
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(gc_blocks_(tablet_versions, min_retain_tablet_meta_version - 1, is_shared, op))) {
      LOG_WARN("failed to gc_blocks", KR(ret), KPC(this), K(tablet_versions.count()),
               K(min_retain_tablet_meta_version), K(is_shared));
    }
  }
  exist_block_set.destroy();
  return ret;
}

int ObBlockGCHandler::gc_tablet_meta_versions(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t min_retain_tablet_meta_version,
    const bool is_transfer_out_deleted)
{
  int ret = OB_SUCCESS;
  // must gc shared macro cache before gc private macro. otherwise, private macro_info will be
  // deleted during gc private macro, and then lead to getting shared blocks failed.
  if (is_transfer_out_deleted) {
    // if TRANSFER_OUT_DELETED, then skip gc shared macro cache. because shared macro cache will be
    // prewarmed through stream-rpc from src-observer to dest-observer during migration.
  } else { // !is_transfer_out_deleted
    if (OB_FAIL(gc_blocks_for_tablet_meta_versions_(tablet_versions, min_retain_tablet_meta_version, true/*is_shared*/))) {
      LOG_WARN("failed to gc_blocks_for_tablet_meta_versions_", KR(ret), KPC(this), K(tablet_versions.count()),
               K(min_retain_tablet_meta_version));
    }
  }
  if (FAILEDx(gc_blocks_for_tablet_meta_versions_(tablet_versions, min_retain_tablet_meta_version, false/*is_shared*/))) {
    LOG_WARN("failed to gc_blocks_for_tablet_meta_versions_", KR(ret), KPC(this), K(tablet_versions.count()),
             K(min_retain_tablet_meta_version));
  } else if (OB_FAIL(delete_tablet_meta_versions_(tablet_versions, min_retain_tablet_meta_version - 1))) {
    LOG_WARN("failed to delete_tablet_meta_version", KR(ret), KPC(this), K(tablet_versions.count()),
             K(min_retain_tablet_meta_version));
  }
  LOG_INFO("finish tablet meta gc", KR(ret), KPC(this), K(tablet_versions.count()),
           K(min_retain_tablet_meta_version), K(is_transfer_out_deleted));
  return ret;
}

int ObBlockGCHandler::gc_blocks_(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t gc_tablet_meta_ts,
    const bool is_shared,
    GCBaseOP &get_gc_blocks_op)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_versions.count(); i++) {
    ObArray<blocksstable::MacroBlockId> block_ids;
    ObArray<blocksstable::MacroBlockId> gc_block_ids;
    const int64_t tablet_version = tablet_versions.at(i);
    if (gc_tablet_meta_ts >= tablet_version) {
      if (check_stop()) {
        ret = OB_SERVICE_STOPPED;
        LOG_WARN("the service is already stop, skip check", K(ret), K(tablet_id_));
      } else if (OB_FAIL(get_blocks_for_tablet(tablet_version, is_shared, block_ids))) {
        LOG_WARN("failed to get_blocks_for_tablet", KR(ret), K(tablet_version), K(is_shared), KPC(this));
      } else if (OB_FAIL(get_gc_blocks_op(block_ids, gc_block_ids))) {
        LOG_WARN("failed to get block for tablet meta version", KR(ret), K(tablet_version), KPC(this));
      } else if (is_shared) {
        // todo  split  skip this step
        // Note: only delete local cache for shared macros
        if (OB_FAIL(delete_macro_cache_(gc_block_ids))) {
          LOG_WARN("failed to delete macro cache", KR(ret), K(gc_block_ids), KPC(this));
        } else {
          LOG_TRACE("macro cache gc success", K(tablet_version), K(gc_block_ids.count()), K(gc_block_ids), KPC(this));
        }
      } else { // !is_shared
        // todo  split  skip this step
        if (OB_FAIL(delete_macro_blocks(gc_block_ids))) {
          LOG_WARN("failed to delete_macro_blocks", KR(ret), K(gc_block_ids), KPC(this));
        } else {
          gc_macro_block_cnt_ += gc_block_ids.count();
          LOG_TRACE("block gc success", KR(ret), K(tablet_version), K(gc_block_ids.count()), K(gc_block_ids), KPC(this));
        }
      }
    }
  }
  return ret;
}

int ObBlockGCHandler::delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  ObTenantFileManager *tfm = MTL(ObTenantFileManager *);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  if (block_ids.empty()) {
  } else if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tenant config for private gc", KR(ret), K(tenant_id));
  } else if (tenant_config->_enable_private_gc_hot_macro_cache_convert_to_auto) {
    if (OB_ISNULL(macro_cache_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache mgr is null", KR(ret), K(tenant_id));
    } else if (OB_FAIL(macro_cache_mgr->modify_macro_cache_type_and_touch(
        block_ids,
        ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
        ObSSMacroCacheType::MACRO_BLOCK))) {
      LOG_WARN("fail to convert private gc hot macro cache to auto",
          KR(ret), K(tenant_id), "macro_block_count", block_ids.count());
    } else {
      FLOG_INFO("convert private gc hot macro cache to auto",
          K(tenant_id), "macro_block_count", block_ids.count());
    }
  } else if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", KR(ret));
  } else if (OB_FAIL(tfm->delete_local_files(block_ids))) {
    LOG_WARN("failed to delete_local_files", KR(ret), K(block_ids));
  }
  return ret;
}

int ObBlockGCHandler::build_macro_block(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t min_retain_tablet_meta_version,
    BlockCollectOP &get_blocks_op)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_versions.count(); i++) {
    ObArray<blocksstable::MacroBlockId> block_ids;
    const int64_t tablet_version = tablet_versions.at(i);
    if (check_stop()) {
      ret = OB_SERVICE_STOPPED;
      LOG_WARN("the service is already stop, skip check", K(ret), K(tablet_id_));
    } else if (min_retain_tablet_meta_version < tablet_version) {
      if (OB_FAIL(get_blocks_for_tablet(tablet_version, false/*is_shared*/, block_ids))) {
        LOG_WARN("failed to get_blocks_for_tablet", K(ret), K(tablet_version), KPC(this));
      } else if (OB_FAIL(get_blocks_op(block_ids))) {
        LOG_WARN("failed to get_blocks_op", K(ret), K(tablet_version), KPC(this));
      }
    }
  }
  
  return ret;
}

bool ObBlockGCHandler::check_stop()
{
  return ATOMIC_LOAD(&is_set_stop_) == true;
}

int delete_macro_blocks(
      ObIArray<blocksstable::MacroBlockId> &block_ids) 
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tfm = MTL(ObTenantFileManager*);
  if (OB_ISNULL(tfm)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", K(ret));
  } else if (OB_FAIL(tfm->delete_files(block_ids))) {
    LOG_WARN("failed to delete_macro_blocks", K(ret), K(block_ids));
  }
  return ret;
}
  
} /* namespace storage */
} /* namespace oceanbase */
