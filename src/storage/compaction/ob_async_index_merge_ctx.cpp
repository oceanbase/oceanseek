/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/table/ob_table_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"

namespace oceanbase
{
namespace compaction
{
using namespace share::schema;
using namespace blocksstable;

void ObAsyncIndexMergeCtx::reset()
{
  deletion_bitmap_handle_.reset();
  delete_index_table_id_ = OB_INVALID_ID;
  delete_index_tablet_id_.reset();
  merged_table_handle_.reset();
  merge_info_.destroy();
}

int ObAsyncIndexMergeCtx::build_ctx(bool &finish_flag)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBasicTabletMergeCtx::build_ctx(finish_flag))) {
    LOG_WARN("failed to build basic merge ctx", KR(ret), KPC(this));
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else if (OB_FAIL(prepare_inv_row_desc())) {
    LOG_WARN("failed to prepare inv row desc", KR(ret), KPC(this));
    CTX_SET_DIAGNOSE_LOCATION(*this);
  }
  return ret;
}

int ObAsyncIndexMergeCtx::prepare_compaction_filter()
{
  return prepare_deletion_bitmap();
}

int ObAsyncIndexMergeCtx::check_merge_ctx_valid()
{
  // Verify all input tables are inv SSTables. If tables_handle_ is empty,
  // return OB_NO_NEED_MERGE so DAG finishes without creating merge tasks.
  int ret = OB_SUCCESS;
  if (!tablet_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", K(ret), K_(tablet_handle));
  } else if (static_param_.tables_handle_.empty()) {
    ret = OB_NO_NEED_MERGE;
    LOG_INFO("no async index sstables to merge", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < static_param_.tables_handle_.get_count(); ++i) {
      const storage::ObITable *table = static_param_.tables_handle_.get_table(i);
      if (OB_ISNULL(table) || !table->is_inv_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected input table for async index merge", K(ret), K(i), KPC(table));
      }
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_sstable_merge_history())) {
    LOG_WARN("failed to init merge history", KR(ret));
  } else if (OB_FAIL(merge_info_.init(static_history_))) {
    LOG_WARN("failed to init merge info", KR(ret));
  }
  return ret;
}

int ObAsyncIndexMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  return ObBasicTabletMergeCtx::get_merge_tables(get_merge_table_result);
}

int ObAsyncIndexMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  if (is_major_merge_type(get_merge_type())) {
    // Major/MEDIUM restores both schema and data version from medium info.
    ObArenaAllocator allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObMediumCompactionInfo *medium_info = nullptr;
    if (OB_FAIL(ObTabletMediumInfoReader::get_medium_info_with_merge_version(
        get_merge_version(), *get_tablet(), allocator, medium_info))) {
      LOG_WARN("failed to get async index medium info", KR(ret), KPC(this));
    } else if (OB_FAIL(prepare_from_medium_compaction_info(medium_info))) {
      LOG_WARN("failed to restore async index compaction parameters", KR(ret), KPC(medium_info), KPC(this));
    }
    ObTabletObjLoadHelper::free(allocator, medium_info);
  } else if (OB_FAIL(ObBasicTabletMergeCtx::prepare_schema())) {
    LOG_WARN("failed to prepare async index storage schema", KR(ret), KPC(this));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), static_param_.data_version_))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else {
    // INC_MAJOR uses the Tablet schema and current data version. Medium info
    // only supplies the optional scheduling reason for merge history.
    int tmp_ret = OB_SUCCESS;
    ObArenaAllocator allocator("GetMediumInfo", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObMediumCompactionInfo *medium_info = nullptr;
    if (OB_TMP_FAIL(ObTabletMediumInfoReader::get_medium_info_with_merge_version(
        get_merge_version(), *get_tablet(), allocator, medium_info))) {
      LOG_WARN("failed to get async index medium info for merge reason",
          KR(tmp_ret), "merge_version", get_merge_version(), KPC(this));
    } else {
      static_param_.merge_reason_ =
          static_cast<ObAdaptiveMergePolicy::AdaptiveMergeReason>(medium_info->medium_merge_reason_);
    }
    if (OB_NOT_NULL(medium_info)) {
      ObTabletObjLoadHelper::free(allocator, medium_info);
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::prepare_delete_index_tablet_id()
{
  // Resolve the delete-index tablet for deletion filtering:
  //   1. Use data_tablet_id to look up the data (main) table schema
  //   2. Find the delete-index auxiliary index in data table's simple_index_infos
  //   3. Map data_tablet_id to partition offset via data table schema
  //   4. Map partition offset to delete-index tablet via delete-index schema
  int ret = OB_SUCCESS;
  const ObTableSchema *data_table_schema = nullptr;
  ObSEArray<ObAuxTableMetaInfo, 8> index_infos;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t subpart_idx = OB_INVALID_INDEX;
  const uint64_t tenant_id = MTL_ID();
  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService *)->get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const ObTablet *tablet = get_tablet();
  const ObTabletID data_tablet_id = OB_ISNULL(tablet) ? ObTabletID() : tablet->get_data_tablet_id();
  delete_index_table_id_ = OB_INVALID_ID;
  delete_index_tablet_id_.reset();
  if (!data_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data tablet id", K(ret), K(data_tablet_id));
  } else if (OB_ISNULL(static_param_.schema_) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema or schema service", K(ret), KP_(static_param_.schema), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_ids.push_back(data_tablet_id))) {
    LOG_WARN("failed to push back data tablet id", K(ret));
  } else if (OB_FAIL(schema_service->get_tablet_to_table_history(
      tenant_id, tablet_ids, static_param_.schema_->schema_version_, table_ids))) {
    LOG_WARN("failed to get table id from data tablet", K(ret), K(data_tablet_id));
  } else if (OB_UNLIKELY(table_ids.empty()) || table_ids.at(0) == OB_INVALID_ID) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("data table is deleted", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(0), data_table_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), K(table_ids.at(0)));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("data table schema is null", K(ret), K(table_ids.at(0)));
  } else if (OB_FAIL(data_table_schema->get_simple_index_infos(index_infos))) {
    LOG_WARN("failed to get simple index infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == delete_index_table_id_ && i < index_infos.count(); ++i) {
      if (is_async_delete_index(index_infos.at(i).index_type_)) {
        delete_index_table_id_ = index_infos.at(i).table_id_;
      }
    }
    if (OB_INVALID_ID == delete_index_table_id_) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no async delete index auxiliary table found", K(ret));
    } else if (data_table_schema->is_partitioned_table() && OB_FAIL(data_table_schema->get_part_idx_by_tablet(data_tablet_id, part_idx, subpart_idx))) {
      LOG_WARN("failed to get data tablet partition index", K(ret), K(data_tablet_id));
    } else if (OB_FAIL(table::ObTableUtils::get_tablet_id_by_part_idx(
        schema_guard, delete_index_table_id_, part_idx, subpart_idx, delete_index_tablet_id_))) {
      LOG_WARN("failed to map partition index to delete log tablet", K(ret), K(part_idx), K(subpart_idx),
          K_(delete_index_table_id));
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::check_deletion_bitmap_ready(
    storage::ObLS &ls,
    const common::ObTabletID &delete_index_tablet_id,
    const int64_t async_snapshot)
{
  int ret = OB_SUCCESS;
  int64_t query_published_version = 0;
  storage::ObTabletHandle tablet_handle;

  if (OB_UNLIKELY(!delete_index_tablet_id.is_valid()
      || async_snapshot <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid deletion bitmap snapshot arguments", KR(ret),
        K(delete_index_tablet_id), K(async_snapshot));
  } else if (OB_FAIL(share::SSCompactHelper::ensure_async_index_tablet_ready(
      ls, delete_index_tablet_id))) {
    LOG_WARN("failed to ensure deletion tablet readiness", KR(ret), K(delete_index_tablet_id));
  } else if (OB_FAIL(ls.get_tablet(delete_index_tablet_id, tablet_handle))) {
    LOG_WARN("failed to get deletion tablet", KR(ret), K(delete_index_tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deletion tablet is null", KR(ret), K(delete_index_tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.get_obj()->is_async_delete_index())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet is not async deletion index", KR(ret), K(delete_index_tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_latest_async_index_published_version(
      query_published_version))) {
    LOG_WARN("failed to get committed deletion published version", KR(ret),
        K(delete_index_tablet_id), K(async_snapshot));
  } else if (query_published_version < async_snapshot) {
    ret = OB_NEED_RETRY;
    LOG_INFO("deletion tablet has not reached the fulltext merge snapshot",
        KR(ret), K(delete_index_tablet_id), K(async_snapshot),
        K(query_published_version));
  } else {
    LOG_INFO("deletion bitmap input is ready", K(delete_index_tablet_id),
        K(async_snapshot), K(query_published_version));
  }
  return ret;
}

int ObAsyncIndexMergeCtx::prepare_deletion_bitmap()
{
  int ret = OB_SUCCESS;
  deletion_bitmap_handle_.reset();
  if (! is_major_merge_type(get_merge_type())) {
    LOG_INFO("skip prepare deletion bitmap for non-major merge", K(get_merge_type()));
  } else if (OB_FAIL(prepare_delete_index_tablet_id())) {
    LOG_WARN("failed to get delete index tablet id", K(ret));
  } else if (OB_UNLIKELY(!delete_index_tablet_id_.is_valid() || OB_INVALID_ID == delete_index_table_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid delete index tablet info", K(ret), K_(delete_index_table_id), K_(delete_index_tablet_id));
  } else {
    const int64_t async_snapshot = get_snapshot();
    // The input Major already applied deletions through this fixed snapshot.
    // Exclude those rows even before deletion GC, so retries make the same
    // macro/logical block reuse decisions after they are physically removed.
    const int64_t base_snapshot = get_read_base_version();
    if (OB_UNLIKELY(async_snapshot <= 0 || base_snapshot < 0 || base_snapshot > async_snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid logical snapshot for deletion bitmap", K(ret),
          K(async_snapshot), K(base_snapshot));
    } else if (OB_ISNULL(get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null when preparing deletion bitmap", K(ret), K(get_ls_id()));
    } else if (OB_FAIL(check_deletion_bitmap_ready(
        *get_ls(), delete_index_tablet_id_, async_snapshot))) {
      if (OB_NEED_RETRY == ret) {
        LOG_INFO("deletion bitmap input is not ready, retry fulltext merge later",
            K(ret), K_(delete_index_tablet_id), K(async_snapshot));
      } else {
        LOG_WARN("failed to check deletion bitmap input", K(ret),
            K_(delete_index_tablet_id), K(async_snapshot));
      }
    } else if (OB_FAIL(ObDeletionBitmapProvider::get_or_build_bitmap(
        get_ls_id(), delete_index_tablet_id_, delete_index_table_id_,
        async_snapshot,
        mem_ctx_.get_allocator(), deletion_bitmap_handle_, base_snapshot))) {
      LOG_WARN("failed to get or build deletion bitmap", K(ret), K_(delete_index_tablet_id),
          K_(delete_index_table_id), K(async_snapshot), K(base_snapshot));
    } else {
      LOG_INFO("success to prepare deletion bitmap", K(ret), K_(delete_index_tablet_id),
          K_(delete_index_table_id), K(async_snapshot), K(base_snapshot));
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::prepare_index_tree()
{
  return ObBasicTabletMergeCtx::build_index_tree(
      merge_info_, &get_tablet()->get_rowkey_read_info());
}

int ObAsyncIndexMergeCtx::create_sstable(const ObSSTable *&new_sstable)
{
  new_sstable = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(merged_table_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("merged table handle is unexpected valid before create", KR(ret), K(merged_table_handle_));
  } else {
    LOG_INFO("create new merged sstable", "dag_param", get_dag_param(),
             "snapshot_version", get_snapshot(), "scn_range", static_param_.scn_range_);
    mem_ctx_.mem_click();
    bool skip_to_create_empty_cg = false;
    if (OB_FAIL(merge_info_.create_sstable(*this, merged_table_handle_, skip_to_create_empty_cg))) {
      LOG_WARN("fail to create sstable", KR(ret));
    } else if (OB_FAIL(merged_table_handle_.get_sstable(new_sstable))) {
      LOG_WARN("fail to get sstable", KR(ret), K_(merged_table_handle));
    } else {
      LOG_INFO("success to create sstable", K(ret), KPC(new_sstable), K(new_sstable->get_row_count()));
      time_guard_click(ObStorageCompactionTimeGuard::CREATE_SSTABLE);
      mem_ctx_.mem_click();
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::collect_running_info()
{
  int ret = OB_SUCCESS;
  time_guard_click(ObStorageCompactionTimeGuard::DAG_FINISH);
  const ObSSTable *new_table = nullptr;
  if (OB_FAIL(merged_table_handle_.get_sstable(new_table))) {
    LOG_WARN("failed to get sstable", KR(ret), "merged_table_handle", merged_table_handle_);
  } else if (nullptr != merge_dag_) {
    add_sstable_merge_info(merge_info_.get_merge_history(), merge_dag_->get_dag_id(),
                           merge_dag_->hash(), info_collector_.time_guard_, new_table,
                           &static_param_.snapshot_info_);
  }
  return ret;
}

// Publish still goes through ObBasicTabletMergeCtx: create sstable + update tablet
// table store. This override only fills the diagnostic gap that row-store merge
// already has in ObTabletMergeFinishTask ("sstable merge finish" + merge_info +
// sstable). Async index finish task logs KPC(ctx), but ObAsyncIndexMergeCtx::
// to_string does not print merged_table_handle_ or merge_block_info, so reuse /
// publish results would be invisible.
// After the base call succeeds the tablet is already published. get_sstable()
// here is logging-only, so it must use tmp_ret: a fail-ed get_sstable must not
// overwrite ret, or FinishTask would retry an already-updated tablet.
int ObAsyncIndexMergeCtx::update_tablet_after_merge()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBasicTabletMergeCtx::update_tablet_after_merge())) {
    LOG_WARN("failed to publish async index merged sstable",
        K(ret), "dag_param", get_dag_param(), K_(merged_table_handle));
  } else {
    const ObSSTable *merged_sstable = nullptr;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = merged_table_handle_.get_sstable(merged_sstable))) {
      LOG_WARN("failed to get published async index sstable",
          K(tmp_ret), "dag_param", get_dag_param(), K_(merged_table_handle));
    } else {
      LOG_INFO("finish to publish async index merged sstable",
          "dag_param", get_dag_param(), KPC(merged_sstable),
          "merge_block_info", merge_info_.get_merge_history().block_info_);
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::prepare_inv_row_desc()
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *row_desc_data_schema = nullptr;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<uint64_t, 2> table_ids;
  const uint64_t tenant_id = MTL_ID();
  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService *)->get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const ObTablet *tablet = get_tablet();
  const ObTabletID index_tablet_id = OB_ISNULL(tablet) ? ObTabletID() : tablet->get_tablet_id();
  const ObTabletID data_tablet_id = OB_ISNULL(tablet) ? ObTabletID() : tablet->get_data_tablet_id();
  if (!index_tablet_id.is_valid() || !data_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv index or data tablet id", K(ret), K(index_tablet_id), K(data_tablet_id));
  } else if (OB_ISNULL(static_param_.schema_) || OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null schema or schema service", K(ret), KP_(static_param_.schema), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_ids.push_back(index_tablet_id))) {
    LOG_WARN("failed to push back inv index tablet id", K(ret));
  } else if (OB_FAIL(tablet_ids.push_back(data_tablet_id))) {
    LOG_WARN("failed to push back data tablet id", K(ret));
  } else if (OB_FAIL(schema_service->get_tablet_to_table_history(
      tenant_id, tablet_ids, static_param_.schema_->schema_version_, table_ids))) {
    LOG_WARN("failed to get table id from inv index and data tablet",
        K(ret), K(index_tablet_id), K(data_tablet_id));
  } else if (OB_UNLIKELY(table_ids.count() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table id count from tablet history", K(ret), K(table_ids));
  } else if (OB_UNLIKELY(table_ids.at(0) == OB_INVALID_ID)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("inv index table is deleted", K(ret), K(index_tablet_id));
  } else if (OB_UNLIKELY(table_ids.at(1) == OB_INVALID_ID)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("data table is deleted", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(0), index_table_schema))) {
    LOG_WARN("failed to get inv index table schema", K(ret), K(table_ids.at(0)));
  } else if (OB_ISNULL(index_table_schema)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("inv index table schema is null", K(ret), K(table_ids.at(0)));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(1), data_table_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), K(table_ids.at(1)));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_IS_DELETED;
    LOG_WARN("data table schema is null", K(ret), K(table_ids.at(1)));
  } else {
    row_desc_data_schema = data_table_schema;
    if (index_table_schema->is_async_search_data_index()) {
      const uint64_t search_def_table_id = index_table_schema->get_data_table_id();
      if (OB_UNLIKELY(OB_INVALID_ID == search_def_table_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("search data index has invalid definition table id",
            K(ret), KPC(index_table_schema));
      } else if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id, search_def_table_id, row_desc_data_schema))) {
        LOG_WARN("failed to get search definition table schema",
            K(ret), K(search_def_table_id));
      } else if (OB_ISNULL(row_desc_data_schema)) {
        ret = OB_TABLE_IS_DELETED;
        LOG_WARN("search definition table schema is null",
            K(ret), K(search_def_table_id));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(row_desc_data_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row desc data schema is null", K(ret), KPC(index_table_schema));
  } else if (OB_FAIL(inv_idx_row_desc_.init(*index_table_schema, *row_desc_data_schema))) {
    LOG_WARN("failed to init inv row desc", K(ret));
  } else {
    LOG_INFO("success to init inv row desc", K(ret), KPC(index_table_schema), KPC(data_table_schema),
        KPC(row_desc_data_schema), K(inv_idx_row_desc_));
  }
  return ret;
}

int ObAsyncIndexMergeCtx::init_inc_major_uncommit_tx_info()
{
  int ret = OB_SUCCESS;
  int concurrent_cnt = get_concurrent_cnt();
  void *uncommit_tx_info_buf = nullptr;
  if (is_major_merge_type(get_merge_type())) {
    LOG_INFO("skip init inc major uncommit tx info for medium major merge", K(get_merge_type()));
  } else if (! is_inc_major_merge(get_merge_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge type for inc major uncommit tx info", K(ret), K(get_merge_type()));
  } else if (OB_ISNULL(uncommit_tx_info_buf = static_cast<void *>(mem_ctx_.get_allocator().alloc(concurrent_cnt * sizeof(ObMemUncommitTxInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for uncommit txinfo", K(ret), K(concurrent_cnt));
  } else {
    uncommit_tx_info_ = new (uncommit_tx_info_buf) ObMemUncommitTxInfo[concurrent_cnt];
    const ObTablesHandleArray &tables_handle = get_tables_handle();
    ObUncommitTxDesc tx_desc(-1, 0);
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle.get_count(); ++i) {
      const ObITable *table = tables_handle.get_table(i);
      const ObSSTable *sstable = nullptr;
      ObSSTableMetaHandle meta_handle;
      if (OB_ISNULL(table) || !table->is_inc_inv_sstable()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid input table for async index inc major merge", KR(ret), KPC(table));
      } else if (OB_FALSE_IT(sstable = static_cast<const ObSSTable *>(table))) {
      } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("failed to get sstable meta handle", KR(ret), KPC(sstable));
      } else if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_uncommit_tx_info().get_info_count() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get empty tx info for async index inc major input", KR(ret), KPC(sstable));
      } else {
        const ObUncommitTxDesc &cur_tx_desc = meta_handle.get_sstable_meta().get_uncommit_tx_info().tx_infos_[0];
        if (OB_UNLIKELY(! cur_tx_desc.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tx desc for async index inc major merge", KR(ret), K(i), K(cur_tx_desc), KPC(sstable));
        } else if (!tx_desc.is_valid() || tx_desc.commit_version_ < cur_tx_desc.commit_version_) {
          tx_desc = cur_tx_desc;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(! tx_desc.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tx desc for async index inc major merge", KR(ret), K(tx_desc), K(tables_handle));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < concurrent_cnt; ++i) {
        uncommit_tx_info_[i].reuse();
        if (OB_FAIL(uncommit_tx_info_[i].push_back(tx_desc))) {
          LOG_WARN("failed to push back uncommit tx desc", KR(ret), K(tx_desc));
        } else {
          LOG_INFO("success to init inc major uncommit tx info", KR(ret), K(i), KPC(&uncommit_tx_info_[i]));
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexMergeCtx::cal_merge_param()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(static_param_.tables_handle_.empty()
      || nullptr == static_param_.tables_handle_.get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables handle is invalid for async index merge", KR(ret), K(static_param_.tables_handle_));
  } else {
    static_param_.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    // ObStaticMergeParam::init_sstable_logic_seq() in ob_basic_tablet_merge_ctx.cpp
    // sets is_full_merge_ = true when INC_MAJOR wraps to generation 0.
    // Otherwise, INC_MAJOR uses max(input generation) + 1, which is at least 1.
    if (is_inc_major_merge(get_merge_type())) {
      static_param_.is_full_merge_ = 0 == static_param_.sstable_logic_seq_;
    } else if (is_major_merge(get_merge_type())) {
      static_param_.is_full_merge_ = false;
    } else {
      static_param_.is_full_merge_ = false;
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
