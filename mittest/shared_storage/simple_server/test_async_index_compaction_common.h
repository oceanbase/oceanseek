/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_MITTEST_ASYNC_INDEX_COMPACTION_COMMON_H_
#define OCEANBASE_MITTEST_ASYNC_INDEX_COMPACTION_COMMON_H_

// Shared fixture and helpers for two independent, single-translation-unit test
// executables. Keep scenario-specific helpers in the corresponding .cpp file.
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGE_COMPACTION
#endif

#include <cstdlib>
#include <gtest/gtest.h>

#define protected public
#define private public

#include "share/ob_async_index_refresh_util.h"
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_async_index_params.h"
#include "storage/ob_storage_schema.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction/ob_medium_compaction_info_mds_helper.h"
#include "storage/compaction/ob_medium_loop.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "storage/compaction/ob_async_index_merge_policy.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/tx/wrs/ob_ls_wrs_handler.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_merge_iter.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_dir_manager.h"
#endif
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/ob_i_store.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_sort.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/async_index/ob_async_index_segment_group.h"
#include "rootserver/freeze/ob_major_freeze_helper.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/async_index/ob_deletion_row_iter.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/compaction/ob_async_index_merge_task.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/blocksstable/async_index/ob_partition_inv_sstable_merger.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/compaction/ob_partition_parallel_merge_ctx.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/ob_gc_upper_trans_helper.h"
#include "storage/compaction/ob_schedule_tablet_func.h"
#include "unittest/storage/test_tablet_helper.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{
bool ObUpdateTableStoreParam::is_valid() const { return true; }
} // namespace storage

namespace
{
using namespace oceanbase;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::compaction;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

int get_table_schema_for_tablet(
    const ObTabletID &tablet_id,
    const int64_t schema_version,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService *)->get_schema_service();
  const uint64_t tenant_id = MTL_ID();
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<uint64_t, 1> table_ids;
  table_schema = nullptr;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("failed to push tablet id", KR(ret), K(tablet_id));
  } else if (OB_FAIL(schema_service->get_tablet_to_table_history(
      tenant_id, tablet_ids, schema_version, table_ids))) {
    LOG_WARN("failed to get table id from tablet", KR(ret), K(tablet_id), K(schema_version));
  } else if (OB_UNLIKELY(table_ids.empty()) || table_ids.at(0) == OB_INVALID_ID) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("tablet table id is invalid", KR(ret), K(tablet_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_ids.at(0), table_schema))) {
    LOG_WARN("failed to get table schema", KR(ret), K(tablet_id), "table_id", table_ids.at(0));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K(tablet_id), "table_id", table_ids.at(0));
  }
  return ret;
}

int get_inv_idx_and_data_table_schema(
    const ObTabletID &inv_tablet_id,
    const ObTabletID &data_tablet_id,
    const int64_t schema_version,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&index_schema,
    const ObTableSchema *&data_table_schema)
{
  int ret = OB_SUCCESS;
  index_schema = nullptr;
  data_table_schema = nullptr;
  if (OB_FAIL(get_table_schema_for_tablet(
      inv_tablet_id, schema_version, schema_guard, index_schema))) {
    LOG_WARN("failed to get inv index table schema", KR(ret), K(inv_tablet_id));
  } else if (OB_FAIL(get_table_schema_for_tablet(
      data_tablet_id, schema_version, schema_guard, data_table_schema))) {
    LOG_WARN("failed to get data table schema", KR(ret), K(data_tablet_id));
  }
  return ret;
}

int make_single_inv_idx_row(
    ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt,
    const ObString &term_str,
    const uint64_t doc_id,
    const uint64_t token_freq,
    const uint64_t doc_length,
    const ObString &pos_list_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mv_col_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(row_desc.doc_id_col_idx_ < 0 || row_desc.doc_id_col_idx_ >= mv_col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid doc_id column idx", KR(ret), K(row_desc), K(mv_col_cnt));
  } else if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() != mv_col_cnt)) {
    row.reset();
    if (OB_FAIL(row.init(mv_col_cnt))) {
      LOG_WARN("failed to init datum row", KR(ret), K(mv_col_cnt));
    }
  }
  if (OB_SUCC(ret)) {
    row.count_ = static_cast<uint16_t>(mv_col_cnt);
    // Clear ext flags and repoint inline buffers so set_uint is safe after set_string
    // on a reused row (same pattern as test_inv_idx_macro_block_writer.cpp).
    for (int16_t i = 0; i < row.count_; ++i) {
      row.storage_datums_[i].reuse();
      row.storage_datums_[i].set_int(0);
    }
    row.storage_datums_[0].set_string(term_str);
    row.storage_datums_[row_desc.doc_id_col_idx_].set_uint(doc_id);
    if (row_desc.token_freq_col_idx_ >= 0) {
      row.storage_datums_[row_desc.token_freq_col_idx_].set_uint(token_freq);
    }
    if (row_desc.doc_length_col_idx_ >= 0) {
      row.storage_datums_[row_desc.doc_length_col_idx_].set_uint(doc_length);
    }
    if (row_desc.pos_list_col_idx_ >= 0) {
      row.storage_datums_[row_desc.pos_list_col_idx_].set_string(pos_list_str);
    }
  }
  return ret;
}

void fill_inv_meta_from_row_desc(
    const ObInvIdxRowDesc &row_desc,
    const uint64_t min_doc_id,
    const uint64_t max_doc_id,
    const uint64_t total_doc_length,
    ObInvIdxStatsMeta &inv_meta)
{
  inv_meta.reset();
  // inv_meta.index_type_ = (row_desc.doc_id_col_idx_ >= 0) ? INV_IDX_TYPE_FTS : INV_IDX_TYPE_INVALID;
  // inv_meta.has_position_list_ = (row_desc.pos_list_col_idx_ >= 0);
  // inv_meta.has_nested_key_ = false;
  // inv_meta.doc_id_obj_meta_.set_uint64();
  // inv_meta.min_doc_id_ = min_doc_id;
  // inv_meta.max_doc_id_ = max_doc_id;
  // inv_meta.total_doc_length_ = total_doc_length;
  inv_meta.doc_id_obj_meta_.set_uint64();
  inv_meta.min_doc_id_ = common::OB_INVALID_ID;
  inv_meta.max_doc_id_ = 0;
  inv_meta.index_type_ = INV_IDX_TYPE_FTS;
  inv_meta.has_relevance_params_ = true;
  inv_meta.has_position_list_ = true;
  inv_meta.has_nested_key_ = false;
}

struct InvIdxRowSnapshot
{
  ObString term_;
  uint64_t doc_id_;
  uint64_t token_freq_;
  uint64_t doc_length_;
  ObString pos_list_;
  TO_STRING_KV(K_(term), K_(doc_id), K_(token_freq), K_(doc_length), K_(pos_list));
};

// Read-info for ObInvSSTableMergeIter — mirrors test_inv_sstable_merge_iter.cpp.
class InvMergeIterReadInfo : public ObReadInfoStruct
{
public:
  InvMergeIterReadInfo() : ObReadInfoStruct(false /*rowkey_mode*/) {}

  int init(
      ObIAllocator &allocator,
      const ObInvIdxRowDesc &row_desc,
      const bool has_pos_list)
  {
    int ret = OB_SUCCESS;
    const int64_t out_cnt = has_pos_list ? 7 : 6;
    ObSEArray<ObColDesc, 8> cols_desc;
    ObColDesc d;
    d.col_type_.set_varchar();
    d.col_type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if (OB_FAIL(cols_desc.push_back(d))) {
    } else {
      d.col_type_.set_uint64();
      if (OB_FAIL(cols_desc.push_back(d))) {
      } else if (OB_FAIL(cols_desc.push_back(d))) {
      } else if (OB_FAIL(cols_desc.push_back(d))) {
      } else if (OB_FAIL(cols_desc.push_back(d))) {
      } else if (OB_FAIL(cols_desc.push_back(d))) {
      } else if (has_pos_list) {
        d.col_type_.set_varchar();
        d.col_type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        if (OB_FAIL(cols_desc.push_back(d))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      init_basic_info(out_cnt, 2 /*schema_rowkey_cnt*/, false /*is_oracle_mode*/,
          false /*is_cg_sstable*/, false /*is_cs_replica_compat*/,
          false /*is_delete_insert_table*/, false /*is_global_index_table*/,
          1 /*micro_block_format_version*/, false /*is_mv_major_refresh_tablet*/,
          false /*has_ttl_definition*/);
      if (OB_FAIL(prepare_arrays(allocator, cols_desc, out_cnt))) {
        LOG_WARN("failed to prepare inv merge iter read info arrays", KR(ret), K(out_cnt));
      } else {
        cols_index_.array_[0] = -1;
        memtable_cols_index_.array_[0] = -1;
        for (int64_t i = 1; i < out_cnt; ++i) {
          const int32_t store_idx = static_cast<int32_t>(i - 1);
          cols_index_.array_[i] = store_idx;
          memtable_cols_index_.array_[i] = store_idx;
        }
        ObSEArray<ObColDesc, 4> rowkey_descs;
        ObColDesc term_desc;
        term_desc.col_type_.set_varchar();
        term_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        ObColDesc doc_desc;
        doc_desc.col_type_.set_uint64();
        if (OB_FAIL(rowkey_descs.push_back(term_desc))) {
        } else if (OB_FAIL(rowkey_descs.push_back(doc_desc))) {
        } else if (OB_FAIL(datum_utils_.init(rowkey_descs, 2, false, allocator, false))) {
          LOG_WARN("failed to init inv merge iter datum utils", KR(ret));
        } else {
          is_inited_ = true;
        }
      }
    }
    return ret;
  }
};

inline int cmp_inv_term_doc(const ObString &t1, const uint64_t d1,
                            const ObString &t2, const uint64_t d2)
{
  const int64_t n = t1.length() < t2.length() ? t1.length() : t2.length();
  int c = (n > 0) ? MEMCMP(t1.ptr(), t2.ptr(), n) : 0;
  if (c < 0) {
    return -1;
  } else if (c > 0) {
    return 1;
  } else if (t1.length() != t2.length()) {
    return t1.length() < t2.length() ? -1 : 1;
  } else if (d1 < d2) {
    return -1;
  } else if (d1 > d2) {
    return 1;
  }
  return 0;
}

inline int cmp_inv_row_snapshots(const InvIdxRowSnapshot &lhs, const InvIdxRowSnapshot &rhs)
{
  return cmp_inv_term_doc(lhs.term_, lhs.doc_id_, rhs.term_, rhs.doc_id_);
}

inline ObString inv_row_snapshot_string(const ObStorageDatum &datum)
{
  return ObString(datum.len_, datum.ptr_);
}

int deep_copy_inv_row_string(ObIAllocator &alloc, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  if (src.empty()) {
    dst.reset();
  } else {
    char *buf = static_cast<char *>(alloc.alloc(src.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, src.ptr(), src.length());
      dst.assign_ptr(buf, static_cast<ObString::obstr_size_t>(src.length()));
    }
  }
  return ret;
}

int append_inv_row_snapshot(
    ObIAllocator &alloc,
    const ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const bool has_pos_list,
    ObIArray<InvIdxRowSnapshot> &out)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = has_pos_list ? 7 : 6;
  if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() != col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    InvIdxRowSnapshot snap;
    const ObString term_src = inv_row_snapshot_string(row.storage_datums_[0]);
    if (OB_FAIL(deep_copy_inv_row_string(alloc, term_src, snap.term_))) {
      LOG_WARN("failed to deep copy inv row term", KR(ret));
    } else {
      snap.doc_id_ = row.storage_datums_[row_desc.doc_id_col_idx_].get_uint();
      snap.token_freq_ = row.storage_datums_[row_desc.token_freq_col_idx_].get_uint();
      snap.doc_length_ = row.storage_datums_[row_desc.doc_length_col_idx_].get_uint();
      if (has_pos_list) {
        const ObString pos_src = inv_row_snapshot_string(row.storage_datums_[row_desc.pos_list_col_idx_]);
        if (OB_FAIL(deep_copy_inv_row_string(alloc, pos_src, snap.pos_list_))) {
          LOG_WARN("failed to deep copy inv row pos_list", KR(ret));
        }
      } else {
        snap.pos_list_.reset();
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(out.push_back(snap))) {
      LOG_WARN("failed to push inv row snapshot", KR(ret));
    }
  }
  return ret;
}

bool less_inv_row_snapshots(const InvIdxRowSnapshot &lhs, const InvIdxRowSnapshot &rhs)
{
  return cmp_inv_row_snapshots(lhs, rhs) < 0;
}

void sort_inv_row_snapshots(ObIArray<InvIdxRowSnapshot> &rows)
{
  if (rows.count() > 1) {
    lib::ob_sort(rows.get_data(), rows.get_data() + rows.count(), less_inv_row_snapshots);
  }
}

bool is_inv_index_input_table(const ObITable *table)
{
  return OB_NOT_NULL(table)
      && (ObITable::TableType::INV_INDEX_SSTABLE == table->get_key().table_type_
          || ObITable::TableType::INC_INV_INDEX_SSTABLE == table->get_key().table_type_);
}

int init_inv_merge_iter_read_info_for_tablet(
    ObTablet &tablet,
    ObIAllocator &alloc,
    InvMergeIterReadInfo &read_info,
    ObInvIdxRowDesc &row_desc,
    bool &has_pos_list)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  has_pos_list = false;
  if (OB_FAIL(tablet.load_storage_schema(alloc, storage_schema))) {
    LOG_WARN("failed to load storage schema", KR(ret));
  } else if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(get_inv_idx_and_data_table_schema(
      tablet.get_tablet_id(),
      tablet.get_data_tablet_id(),
      storage_schema->get_schema_version(),
      schema_guard,
      index_schema,
      data_table_schema))) {
    LOG_WARN("failed to get inv index schema", KR(ret));
  } else if (OB_ISNULL(index_schema) || OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(row_desc.init(*index_schema, *data_table_schema))) {
    LOG_WARN("failed to init inv idx row desc", KR(ret));
  } else {
    has_pos_list = row_desc.pos_list_col_idx_ >= 0;
    if (OB_FAIL(read_info.init(alloc, row_desc, has_pos_list))) {
      LOG_WARN("failed to init inv merge iter read info", KR(ret), K(has_pos_list));
    }
  }
  return ret;
}

int collect_inv_rows_from_sstable(
    ObInvSSTable &sstable,
    const ObITableReadInfo &read_info,
    const ObInvIdxRowDesc &row_desc,
    const bool has_pos_list,
    ObIAllocator &alloc,
    ObIArray<InvIdxRowSnapshot> &out)
{
  int ret = OB_SUCCESS;
  ObDatumRange term_range;
  term_range.set_whole_range();
  ObInvSSTableMergeIter iter;
  if (OB_FAIL(iter.init(sstable, read_info, row_desc, term_range, alloc))) {
    LOG_WARN("failed to init inv sstable merge iter", KR(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(iter.next())) {
      break;
    }
    const ObDatumRow *row = iter.get_curr_row();
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inv merge iter returned null row", KR(ret));
    } else if (OB_FAIL(append_inv_row_snapshot(alloc, *row, row_desc, has_pos_list, out))) {
      LOG_WARN("failed to append inv row snapshot", KR(ret));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int collect_inv_rows_from_async_index_merge_inputs(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t merge_snapshot,
    ObIAllocator &alloc,
    ObIArray<InvIdxRowSnapshot> &out,
    const bool include_base_major = true)
{
  int ret = OB_SUCCESS;
  out.reset();
  ObGetMergeTablesResult result;
  InvMergeIterReadInfo read_info;
  ObInvIdxRowDesc row_desc;
  bool has_pos_list = false;
  if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(
          ls, tablet, merge_snapshot, result, false /*need_check_tablet*/))) {
    LOG_WARN("failed to get async index merge input tables", KR(ret), K(merge_snapshot));
  } else if (OB_FAIL(init_inv_merge_iter_read_info_for_tablet(
      tablet, alloc, read_info, row_desc, has_pos_list))) {
    LOG_WARN("failed to init inv merge iter read info for tablet", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      ObITable *table = result.handle_.get_table(i);
      if (!is_inv_index_input_table(table)) {
        continue;
      } else if (!include_base_major
          && ObITable::TableType::INV_INDEX_SSTABLE == table->get_key().table_type_) {
        continue;
      }
      ObInvSSTable *inv_sstable = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table));
      if (OB_FAIL(collect_inv_rows_from_sstable(
          *inv_sstable, read_info, row_desc, has_pos_list, alloc, out))) {
        LOG_WARN("failed to collect inv rows from merge input sstable", KR(ret), K(i),
                 "snapshot", table->get_snapshot_version());
      }
    }
    if (OB_SUCC(ret)) {
      sort_inv_row_snapshots(out);
    }
  }
  return ret;
}

// Verify the precise precondition for the macro-reuse overlap fallback. The
// loser-tree pops the source with the smallest current macro low key; reuse is
// rejected when another source's low key is no greater than that macro's high
// key. Keeping the first iterator alive preserves the borrowed macro range.
int has_async_index_input_macro_overlap(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t merge_snapshot,
    ObIAllocator &alloc,
    bool &has_overlap)
{
  int ret = OB_SUCCESS;
  has_overlap = false;
  ObGetMergeTablesResult result;
  InvMergeIterReadInfo read_info;
  ObInvIdxRowDesc row_desc;
  bool has_pos_list = false;
  ObDatumRange whole_range;
  ObInvSSTableMergeIter first_iter;
  ObDatumRowkey first_high;
  bool first_iter_opened = false;
  whole_range.set_whole_range();
  if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(
          ls, tablet, merge_snapshot, result, false /*need_check_tablet*/))) {
    LOG_WARN("failed to get async index merge input tables", KR(ret), K(merge_snapshot));
  } else if (OB_FAIL(init_inv_merge_iter_read_info_for_tablet(
      tablet, alloc, read_info, row_desc, has_pos_list))) {
    LOG_WARN("failed to init inv merge read info", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_overlap && i < result.handle_.get_count(); ++i) {
      ObITable *table = result.handle_.get_table(i);
      if (!is_inv_index_input_table(table)) {
        continue;
      }
      ObInvSSTable *inv_sstable = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table));
      if (!first_iter_opened) {
        if (OB_FAIL(first_iter.init(*inv_sstable, read_info, row_desc, whole_range, alloc))) {
          LOG_WARN("failed to init first inv merge iter", KR(ret), K(i));
        } else if (OB_FAIL(first_iter.next_macro_block())) {
          LOG_WARN("failed to open first inv macro block", KR(ret), K(i));
        } else {
          ObDatumRowkey first_low;
          if (OB_FAIL(first_iter.get_curr_range(first_low, first_high))) {
            LOG_WARN("failed to get first inv macro range", KR(ret), K(i));
          } else {
            first_iter_opened = true;
          }
        }
      } else {
        ObInvSSTableMergeIter candidate_iter;
        ObDatumRowkey candidate_low;
        ObDatumRowkey candidate_high;
        int cmp_ret = 0;
        if (OB_FAIL(candidate_iter.init(*inv_sstable, read_info, row_desc, whole_range, alloc))) {
          LOG_WARN("failed to init candidate inv merge iter", KR(ret), K(i));
        } else if (OB_FAIL(candidate_iter.next_macro_block())) {
          LOG_WARN("failed to open candidate inv macro block", KR(ret), K(i));
        } else if (OB_FAIL(candidate_iter.get_curr_range(candidate_low, candidate_high))) {
          LOG_WARN("failed to get candidate inv macro range", KR(ret), K(i));
        } else if (OB_FAIL(candidate_low.compare(
            first_high, read_info.get_datum_utils(), cmp_ret))) {
          LOG_WARN("failed to compare inv macro ranges", KR(ret), K(i));
        } else {
          has_overlap = (cmp_ret <= 0);
        }
      }
    }
  }
  return ret;
}

void filter_inv_rows_by_doc_ids(
    ObIArray<InvIdxRowSnapshot> &rows,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt)
{
  if (OB_ISNULL(doc_ids) || doc_id_cnt <= 0) {
    return;
  }
  int64_t write_idx = 0;
  for (int64_t i = 0; i < rows.count(); ++i) {
    bool deleted = false;
    for (int64_t j = 0; !deleted && j < doc_id_cnt; ++j) {
      if (rows.at(i).doc_id_ == doc_ids[j]) {
        deleted = true;
      }
    }
    if (!deleted) {
      if (write_idx != i) {
        rows.at(write_idx) = rows.at(i);
      }
      ++write_idx;
    }
  }
  while (rows.count() > write_idx) {
    rows.pop_back();
  }
}

int64_t count_inv_rows_by_doc_ids(
    const ObIArray<InvIdxRowSnapshot> &rows,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt)
{
  int64_t count = 0;
  if (OB_NOT_NULL(doc_ids) && doc_id_cnt > 0) {
    for (int64_t i = 0; i < rows.count(); ++i) {
      for (int64_t j = 0; j < doc_id_cnt; ++j) {
        if (rows.at(i).doc_id_ == doc_ids[j]) {
          ++count;
          break;
        }
      }
    }
  }
  return count;
}

void verify_inv_row_snapshots_equal(
    const ObIArray<InvIdxRowSnapshot> &expected,
    const ObIArray<InvIdxRowSnapshot> &actual)
{
  ASSERT_EQ(expected.count(), actual.count()) << "inv row count mismatch";
  for (int64_t i = 0; i < expected.count(); ++i) {
    const InvIdxRowSnapshot &e = expected.at(i);
    const InvIdxRowSnapshot &a = actual.at(i);
    EXPECT_EQ(0, cmp_inv_term_doc(e.term_, 0, a.term_, 0))
        << "term mismatch at row " << i << ", expected_doc_id=" << e.doc_id_
        << ", actual_doc_id=" << a.doc_id_;
    EXPECT_EQ(e.doc_id_, a.doc_id_) << "doc_id mismatch at row " << i;
    EXPECT_EQ(e.token_freq_, a.token_freq_) << "token_freq mismatch at row " << i;
    EXPECT_EQ(e.doc_length_, a.doc_length_) << "doc_length mismatch at row " << i;
    if (!e.pos_list_.empty() || !a.pos_list_.empty()) {
      EXPECT_EQ(e.pos_list_.length(), a.pos_list_.length())
          << "pos_list length mismatch at row " << i;
      if (e.pos_list_.length() == a.pos_list_.length() && e.pos_list_.length() > 0) {
        EXPECT_EQ(0, MEMCMP(e.pos_list_.ptr(), a.pos_list_.ptr(), e.pos_list_.length()))
            << "pos_list content mismatch at row " << i;
      }
    }
  }
}

const char *inv_table_type_str(const ObITable *table)
{
  if (OB_ISNULL(table)) {
    return "null";
  }
  switch (table->get_key().table_type_) {
    case ObITable::TableType::INV_INDEX_SSTABLE:
      return "INV_INDEX_SSTABLE";
    case ObITable::TableType::INC_INV_INDEX_SSTABLE:
      return "INC_INV_INDEX_SSTABLE";
    default:
      return "other";
  }
}

void print_inv_sstable_meta(
    ObITable *table,
    const int64_t input_idx,
    const ObTabletID &tablet_id,
    const int64_t merge_snapshot)
{
  if (OB_ISNULL(table)) {
    std::cout << "[inv sstable meta] input_idx=" << input_idx
              << " tablet_id=" << tablet_id.id_
              << " merge_snapshot=" << merge_snapshot
              << " table=null" << std::endl;
    return;
  }
  // ObSSTable *sstable = static_cast<ObSSTable *>(table);
  // ObInvSSTable *inv_sstable =
  //     (ObITable::TableType::INV_INDEX_SSTABLE == table->get_key().table_type_
  //      || ObITable::TableType::INC_INV_INDEX_SSTABLE == table->get_key().table_type_)
  //         ? static_cast<ObInvSSTable *>(sstable)
  //         : nullptr;
  // std::cout << "[inv sstable meta] input_idx=" << input_idx
  //           << " tablet_id=" << tablet_id.id_
  //           << " merge_snapshot=" << merge_snapshot
  //           << " table_type=" << inv_table_type_str(table)
  //           << " snapshot=" << table->get_snapshot_version()
  //           << " base_version=" << table->get_version_range().base_version_
  //           << " row_count=" << sstable->get_row_count()
  //           << " macro_block_cnt=" << sstable->get_data_macro_block_count();
  // if (OB_NOT_NULL(inv_sstable)) {
  //   const ObInvIdxStatsMeta &meta = inv_sstable->get_inv_meta().get_stats();
  //   std::cout << " min_doc_id=" << meta.min_doc_id_
  //             << " max_doc_id=" << meta.max_doc_id_
  //             << " total_doc_length=" << meta.total_doc_length_;
  // }
  // std::cout << std::endl;
}

void print_inv_row_snapshots(
    const char *label,
    const ObTabletID &tablet_id,
    const int64_t merge_snapshot,
    const ObIArray<InvIdxRowSnapshot> &rows)
{
  // std::cout << "[inv sstable rows] " << label
  //           << " tablet_id=" << tablet_id.id_
  //           << " merge_snapshot=" << merge_snapshot
  //           << " row_cnt=" << rows.count() << std::endl;
  // for (int64_t i = 0; i < rows.count(); ++i) {
  //   const InvIdxRowSnapshot &r = rows.at(i);
  //   std::cout << "  [" << i << "] term=";
  //   if (r.term_.length() > 0 && OB_NOT_NULL(r.term_.ptr())) {
  //     std::cout.write(r.term_.ptr(), r.term_.length());
  //   }
  //   std::cout << " doc_id=" << r.doc_id_
  //             << " token_freq=" << r.token_freq_
  //             << " doc_length=" << r.doc_length_
  //             << " pos_list_len=" << r.pos_list_.length() << std::endl;
  // }
}

int dump_async_index_merge_input_sstables(
    ObLS &ls,
    ObTablet &tablet,
    const int64_t merge_snapshot,
    ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObGetMergeTablesResult result;
  InvMergeIterReadInfo read_info;
  ObInvIdxRowDesc row_desc;
  bool has_pos_list = false;
  const ObTabletID tablet_id = tablet.get_tablet_id();
  if (OB_FAIL(ObPartitionMergePolicy::get_result_by_snapshot(
          ls, tablet, merge_snapshot, result, false /*need_check_tablet*/))) {
    LOG_WARN("failed to get async index merge input tables for print", KR(ret), K(merge_snapshot));
  } else if (OB_FAIL(init_inv_merge_iter_read_info_for_tablet(
      tablet, alloc, read_info, row_desc, has_pos_list))) {
    LOG_WARN("failed to init inv merge iter read info for print", KR(ret));
  } else {
    int64_t inv_input_idx = 0;
    std::cout << "======== async index merge INPUT sstables ========"
              << " tablet_id=" << tablet_id.id_
              << " merge_snapshot=" << merge_snapshot
              << " input_table_cnt=" << result.handle_.get_count() << std::endl;
    for (int64_t i = 0; OB_SUCC(ret) && i < result.handle_.get_count(); ++i) {
      ObITable *table = result.handle_.get_table(i);
      if (!is_inv_index_input_table(table)) {
        continue;
      }
      print_inv_sstable_meta(table, inv_input_idx, tablet_id, merge_snapshot);
      ObSEArray<InvIdxRowSnapshot, 64> table_rows;
      ObInvSSTable *inv_sstable = static_cast<ObInvSSTable *>(static_cast<ObSSTable *>(table));
      if (OB_FAIL(collect_inv_rows_from_sstable(
          *inv_sstable, read_info, row_desc, has_pos_list, alloc, table_rows))) {
        LOG_WARN("failed to collect inv rows for print", KR(ret), K(i));
      } else {
        sort_inv_row_snapshots(table_rows);
        print_inv_row_snapshots("input sstable rows", tablet_id, merge_snapshot, table_rows);
      }
      ++inv_input_idx;
    }
    std::cout << "======== async index merge INPUT sstables end ========" << std::endl;
  }
  return ret;
}

int ensure_inv_tablet_data_dir_on_ss(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
            MTL_ID(), MTL_EPOCH_ID(), tablet_id.id(), 0 /*private_macro_third_id*/))) {
      LOG_WARN("failed to create inv tablet data dir on shared storage", KR(ret), K(tablet_id));
    }
  }
#endif
  return ret;
}

int deep_copy_index_tree_root_block(
    ObArenaAllocator &allocator,
    ObIndexTreeRootBlockDesc &root_desc)
{
  int ret = OB_SUCCESS;
  if (root_desc.is_mem_type() && nullptr != root_desc.buf_ && root_desc.addr_.size() > 0) {
    const int64_t buf_size = root_desc.addr_.size();
    char *copied_buf = static_cast<char *>(allocator.alloc(buf_size));
    if (OB_ISNULL(copied_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc root block buf", KR(ret), K(buf_size));
    } else {
      MEMCPY(copied_buf, root_desc.buf_, buf_size);
      root_desc.buf_ = copied_buf;
    }
  }
  return ret;
}

int build_non_empty_inv_merge_res(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t row_cnt,
    const uint64_t start_doc_id,
    const bool micro_index_clustered,
    ObInvSSTableMeta &inv_meta,
    ObArenaAllocator &allocator,
    ObSSTableMergeRes &res,
    const uint64_t doc_id_step = 1,
    const bool single_term = false,
    const int64_t rows_per_macro = 0)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  ObTabletHandle tablet_handle;
  ObInvIdxRowDesc inv_idx_row_desc;
  ObWholeDataStoreDesc whole_desc;
  ObSSTableIndexBuilder index_builder(false /*not need writer buffer*/);
  ObInvIdxMacroBlockWriter writer;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  ObMacroSeqParam seq_param;
  ObDatumRow row;
  char pos_list_buf[8];
  uint64_t min_doc_id = 0;
  uint64_t max_doc_id = 0;
  uint64_t total_token_freq = 0;
  res.reset();
  if (OB_UNLIKELY(row_cnt <= 0 || doc_id_step == 0 || !tablet_id.is_valid() || OB_ISNULL(ls_handle.get_ls()))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get inv index tablet handle", KR(ret), K(tablet_id));
  } else if (OB_FAIL(get_inv_idx_and_data_table_schema(
      tablet_id,
      tablet_handle.get_obj()->get_data_tablet_id(),
      storage_schema.get_schema_version(),
      schema_guard,
      index_schema,
      data_table_schema))) {
    LOG_WARN("failed to get inv index and data table schema", KR(ret), K(tablet_id));
  } else if (OB_FAIL(inv_idx_row_desc.init(*index_schema, *data_table_schema))) {
    LOG_WARN("failed to init inv idx row desc", KR(ret), K(tablet_id));
  } else {
    SCN end_scn;
    const SCN reorganization_scn(SCN::min_scn());
    const bool is_inc_major = is_inc_major_merge(merge_type);
    if (OB_FAIL(end_scn.convert_for_tx(snapshot_version))) {
      LOG_WARN("failed to convert end scn", KR(ret), K(snapshot_version));
    } else if (OB_FAIL(whole_desc.init(
        false /*is_ddl*/,
        storage_schema,
        ls_handle.get_ls()->get_ls_id(),
        tablet_id,
        merge_type,
        snapshot_version,
        DATA_CURRENT_VERSION,
        micro_index_clustered,
        0 /*private_transfer_epoch*/,
        0 /*concurrent_cnt*/,
        reorganization_scn,
        end_scn,
        nullptr /*cg_schema*/,
        0 /*table_cg_idx*/,
        EXEC_MODE_LOCAL,
        true /*need_submit_io*/,
        is_inc_major))) {
      LOG_WARN("failed to init whole data store desc", KR(ret), K(tablet_id), K(merge_type));
    } else {
      ObDataStoreDesc &desc = whole_desc.get_desc();
      desc.data_store_type_ = ObMacroBlockCommonHeader::SSTableData;
      // ObInvIndexBlockMicroIterator cannot read FLAG_COMPRESSED data micro blocks yet;
      // keep unittest inv writer on none compressor, same as test_inv_idx_macro_block_writer.cpp.
      desc.static_desc_->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
      if (OB_FAIL(ensure_inv_tablet_data_dir_on_ss(tablet_id))) {
        LOG_WARN("failed to ensure inv tablet data dir", KR(ret), K(tablet_id));
      } else if (OB_FAIL(index_builder.init(desc))) {
        LOG_WARN("failed to init sstable index builder", KR(ret), K(tablet_id));
      } else if (OB_ISNULL(object_cleaner = index_builder.get_object_cleaner())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inv sstable index builder object cleaner is null", KR(ret), K(tablet_id));
      } else {
        desc.sstable_index_builder_ = &index_builder;
        seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
        seq_param.start_ = 0;
        seq_param.interval_ = 1;
        seq_param.step_ = 1;
        if (OB_FAIL(writer.open(
            inv_idx_row_desc,
            desc,
            0 /*parallel_idx*/,
            seq_param,
            pre_warm_param,
            *object_cleaner,
            nullptr /*callback*/))) {
          LOG_WARN("failed to open inv macro block writer", KR(ret), K(tablet_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    MEMSET(pos_list_buf, 'p', sizeof(pos_list_buf));
    const ObString pos_list(static_cast<int32_t>(sizeof(pos_list_buf)), pos_list_buf);
    const int64_t mv_col_cnt = whole_desc.get_desc().get_row_column_count();
    if (OB_FAIL(row.init(mv_col_cnt))) {
      LOG_WARN("failed to init feeder row", KR(ret), K(mv_col_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      const uint64_t doc_id = start_doc_id + static_cast<uint64_t>(i) * doc_id_step;
      char term_buf[32];
      snprintf(term_buf, sizeof(term_buf), "term%010lu", single_term ? uint64_t(0) : doc_id);
      const ObString term_str(static_cast<int32_t>(strlen(term_buf)), term_buf);
      const uint64_t token_freq = 1;
      if (OB_FAIL(make_single_inv_idx_row(
              row, inv_idx_row_desc, mv_col_cnt, term_str, doc_id, token_freq, 100, pos_list))) {
        LOG_WARN("failed to make inv idx row", KR(ret), K(i), K(doc_id));
      } else if (OB_FAIL(writer.append_row(row))) {
        LOG_WARN("failed to append inv idx row", KR(ret), K(i), K(doc_id));
      } else {
        if (0 == min_doc_id) {
          min_doc_id = doc_id;
        }
        max_doc_id = doc_id;
        total_token_freq += token_freq;
      }
      // Small, valid input macros make parallel retry tests independent of
      // compression ratios and large data sets. Existing callers never force flush.
      if (OB_SUCC(ret) && rows_per_macro > 0 && (i + 1) % rows_per_macro == 0
          && i + 1 < row_cnt) {
        if (OB_FAIL(writer.flush_logical_block())) {
          LOG_WARN("failed to flush test logical block", KR(ret));
        } else if (OB_FAIL(writer.flush_micro_block_pair())) {
          LOG_WARN("failed to flush test micro block", KR(ret));
        } else if (OB_FAIL(writer.flush_macro_block(false /*is_close_flush*/))) {
          LOG_WARN("failed to flush test input macro", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer.close())) {
        LOG_WARN("failed to close inv macro block writer", KR(ret), K(tablet_id));
      } else if (OB_FAIL(index_builder.close(res))) {
        LOG_WARN("failed to close sstable index builder", KR(ret), K(tablet_id));
      } else if (OB_UNLIKELY(!res.root_desc_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inv merge res root is invalid", KR(ret), K(tablet_id), K(row_cnt));
      } else if (OB_FAIL(deep_copy_index_tree_root_block(allocator, res.root_desc_))) {
        LOG_WARN("failed to deep copy inv merge res root block buf", KR(ret), K(tablet_id));
      } else if (OB_FAIL(deep_copy_index_tree_root_block(allocator, res.data_root_desc_))) {
        LOG_WARN("failed to deep copy inv merge res data root block buf", KR(ret), K(tablet_id));
      } else {
        ObInvIdxStatsMeta stats;
        fill_inv_meta_from_row_desc(inv_idx_row_desc, min_doc_id, max_doc_id, total_token_freq, stats);
        if (OB_FAIL(inv_meta.init(stats))) {
          LOG_WARN("failed to initialize inverted sstable metadata", K(ret));
        } else if (OB_FAIL(res.inv_meta_.assign(inv_meta))) {
          LOG_WARN("failed to copy inverted sstable metadata", K(ret));
        }
      }
    }
  }
  return ret;
}

int prepare_data_merge_context(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const ObMergeType merge_type,
    const int64_t snapshot_version,
    const int64_t base_version,
    ObTabletMergeCtx &merge_context)
{
  int ret = OB_SUCCESS;
  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = base_version;
  compaction::ObStaticMergeParam &static_param = merge_context.static_param_;
  static_param.ls_handle_ = ls_handle;
  static_param.schema_ = &storage_schema;
  static_param.data_version_ = DATA_CURRENT_VERSION;
  static_param.is_full_merge_ = false;
  static_param.is_inv_index_ = false;
  static_param.for_unittest_ = true;
  static_param.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
  static_param.dag_param_.merge_type_ = merge_type;
  static_param.dag_param_.merge_version_ = is_major_merge_type(merge_type) ? snapshot_version : 0;
  static_param.dag_param_.ls_id_ = ls_handle.get_ls()->get_ls_id();
  static_param.dag_param_.tablet_id_ = tablet_id;
  static_param.version_range_ = trans_version_range;
  static_param.scn_range_.start_scn_.set_min();
  static_param.scn_range_.end_scn_.convert_for_tx(snapshot_version);
  static_param.merge_scn_ = static_param.scn_range_.end_scn_;
  static_param.rec_scn_.set_min();
  if (static_param.tables_handle_.empty()) {
    static_param.merge_version_range_ = trans_version_range;
  } else if (OB_FAIL(static_param.init_merge_version_range(trans_version_range))) {
    LOG_WARN("failed to init merge version range", KR(ret), K(trans_version_range));
  }
  if (OB_SUCC(ret) && OB_FAIL(static_param.init_major_sstable_count())) {
    LOG_WARN("failed to init major sstable count", KR(ret));
  }
  return ret;
}

int build_non_empty_delete_index_merge_res(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObStorageSchema &storage_schema,
    const int64_t snapshot_version,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt,
    ObArenaAllocator &allocator,
    ObSSTableMergeRes &res,
    const int64_t row_snapshot_version = 0,
    const int64_t *row_snapshots = nullptr,
    const ObICompactionFilter *filter = nullptr)
{
  int ret = OB_SUCCESS;
  ObSSTableIndexBuilder index_builder(false /*not need writer buffer*/);
  ObMacroBlockWriter macro_writer;
  ObWholeDataStoreDesc whole_desc;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  ObMacroSeqParam seq_param;
  ObDatumRow row;
  int64_t written_row_count = 0;
  res.reset();
  if (OB_UNLIKELY(doc_id_cnt <= 0 || !tablet_id.is_valid() || OB_ISNULL(ls_handle.get_ls()) || OB_ISNULL(doc_ids))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    SCN end_scn;
    const SCN reorganization_scn(SCN::min_scn());
    if (OB_FAIL(end_scn.convert_for_tx(snapshot_version))) {
      LOG_WARN("failed to convert end scn", KR(ret), K(snapshot_version));
    } else if (OB_FAIL(whole_desc.init(
        false /*is_ddl*/,
        storage_schema,
        ls_handle.get_ls()->get_ls_id(),
        tablet_id,
        MAJOR_MERGE,
        snapshot_version,
        DATA_CURRENT_VERSION,
        false /*micro_index_clustered*/,
        0 /*private_transfer_epoch*/,
        0 /*concurrent_cnt*/,
        reorganization_scn,
        end_scn,
        nullptr /*cg_schema*/,
        0 /*table_cg_idx*/,
        EXEC_MODE_LOCAL,
        true /*need_submit_io*/,
        false /*is_inc_major*/))) {
      LOG_WARN("failed to init delete index whole data store desc", KR(ret), K(tablet_id));
    } else {
      ObDataStoreDesc &desc = whole_desc.get_desc();
      desc.data_store_type_ = ObMacroBlockCommonHeader::SSTableData;
      desc.static_desc_->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
      if (OB_FAIL(ensure_inv_tablet_data_dir_on_ss(tablet_id))) {
        LOG_WARN("failed to ensure delete index tablet data dir", KR(ret), K(tablet_id));
      } else if (OB_FAIL(index_builder.init(desc))) {
        LOG_WARN("failed to init delete index sstable index builder", KR(ret), K(tablet_id));
      } else if (OB_ISNULL(object_cleaner = index_builder.get_object_cleaner())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete index sstable index builder object cleaner is null", KR(ret), K(tablet_id));
      } else {
        desc.sstable_index_builder_ = &index_builder;
        seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
        seq_param.start_ = 0;
        seq_param.interval_ = 1;
        seq_param.step_ = 1;
        if (OB_FAIL(macro_writer.open(
            desc, 0 /*parallel_idx*/, seq_param, pre_warm_param, *object_cleaner))) {
          LOG_WARN("failed to open delete index macro block writer", KR(ret), K(tablet_id));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t store_col_cnt = whole_desc.get_desc().get_row_column_count();
    if (OB_FAIL(row.init(store_col_cnt))) {
      LOG_WARN("failed to init delete index feeder row", KR(ret), K(store_col_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < doc_id_cnt; ++i) {
      for (int16_t j = 0; j < row.count_; ++j) {
        row.storage_datums_[j].reuse();
        row.storage_datums_[j].set_int(0);
      }
      row.storage_datums_[0].set_uint(doc_ids[i]);
      const int64_t row_snapshot = nullptr != row_snapshots ? row_snapshots[i]
          : (row_snapshot_version > 0 ? row_snapshot_version : snapshot_version);
      row.storage_datums_[1].set_int(-row_snapshot);
      row.storage_datums_[2].set_int(0);
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.mvcc_row_flag_.reset();
      row.mvcc_row_flag_.set_last_multi_version_row(true);
      ObICompactionFilter::ObFilterRet filter_ret = ObICompactionFilter::FILTER_RET_KEEP;
      if (nullptr != filter && OB_FAIL(filter->filter(row, filter_ret))) {
        LOG_WARN("failed to filter delete index row", KR(ret), K(i));
      } else if (ObICompactionFilter::FILTER_RET_REMOVE == filter_ret) {
      } else if (OB_FAIL(macro_writer.append_row(row))) {
        LOG_WARN("failed to append delete index row", KR(ret), K(i), "doc_id", doc_ids[i]);
      } else {
        ++written_row_count;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(macro_writer.close())) {
        LOG_WARN("failed to close delete index macro block writer", KR(ret), K(tablet_id));
      } else if (OB_FAIL(index_builder.close(res))) {
        LOG_WARN("failed to close delete index sstable index builder", KR(ret), K(tablet_id));
      } else if (OB_UNLIKELY(!res.root_desc_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete index merge res root is invalid", KR(ret), K(tablet_id), K(doc_id_cnt));
      } else if (OB_FAIL(deep_copy_index_tree_root_block(allocator, res.root_desc_))) {
        LOG_WARN("failed to deep copy delete index merge res root block buf", KR(ret), K(tablet_id));
      } else if (OB_FAIL(deep_copy_index_tree_root_block(allocator, res.data_root_desc_))) {
        LOG_WARN("failed to deep copy delete index merge res data root block buf", KR(ret), K(tablet_id));
      } else {
        res.row_count_ = written_row_count;
      }
    }
  }
  return ret;
}

int add_non_empty_delete_index_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt,
    ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam update_param;
  ObSSTable *sstable = nullptr;
  ObTableHandleV2 table_handle;
  ObTabletMergeDagParam dag_param;
  ObTabletMergeCtx merge_context(dag_param, allocator);
  ObTabletCreateSSTableParam create_param;
  ObSSTableMergeRes res;
  ObStorageSchema *storage_schema = nullptr;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get delete index tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load delete index storage schema", KR(ret), K(tablet_id));
  } else if (OB_FAIL(build_non_empty_delete_index_merge_res(
      ls_handle, tablet_id, *storage_schema, snapshot_version, doc_ids, doc_id_cnt, allocator, res))) {
    LOG_WARN("failed to build delete index merge res", KR(ret), K(tablet_id), K(doc_id_cnt));
  } else if (OB_FAIL(prepare_data_merge_context(
      ls_handle, tablet_id, *storage_schema, MAJOR_MERGE, snapshot_version, 0 /*base_version*/, merge_context))) {
    LOG_WARN("failed to prepare delete index merge context", KR(ret), K(tablet_id));
  } else if (OB_FAIL(create_param.init_for_merge(merge_context, res, nullptr /*cg_schema*/, 0 /*cg_idx*/))) {
    LOG_WARN("failed to init delete index sstable param", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(create_param, allocator, table_handle))) {
    LOG_WARN("failed to create delete index sstable", KR(ret), K(tablet_id));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get delete index sstable from handle", KR(ret), K(tablet_id));
  } else {
    update_param.snapshot_version_ = snapshot_version;
    update_param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
    update_param.storage_schema_ = storage_schema;
    update_param.rebuild_seq_ = ls->get_rebuild_seq();
    update_param.ddl_info_.update_with_major_flag_ = false;
    update_param.sstable_ = sstable;
    update_param.allow_duplicate_sstable_ = false;
    if (OB_FAIL(update_param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
      LOG_WARN("failed to init delete index ha info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(update_param.init_with_compaction_info(ObCompactionTableStoreParam(
                      ObMergeType::MAJOR_MERGE, sstable->get_end_scn(), false,
                      tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init delete index compaction info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ls->update_tablet_table_store(tablet_id, update_param, new_tablet_handle))) {
      LOG_WARN("failed to update delete index tablet table store", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int add_non_empty_delete_index_major_via_merge_ctx(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const uint64_t *doc_ids,
    const int64_t doc_id_cnt,
    ObArenaAllocator &allocator,
    int64_t &actual_multi_version_start,
    const int64_t row_snapshot_version,
    const int64_t *row_snapshots,
    const ObMdsFilterInfo *mds_filter_info)
{
  int ret = OB_SUCCESS;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam update_param;
  ObSSTable *sstable = nullptr;
  ObTableHandleV2 table_handle;
  ObTabletMergeDagParam dag_param;
  ObTabletMajorMergeCtx merge_context(dag_param, allocator);
  ObTabletCreateSSTableParam create_param;
  ObSSTableMergeRes res;
  ObStorageSchema *storage_schema = nullptr;
  ObLS *ls = ls_handle.get_ls();
  actual_multi_version_start = 0;
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, merge_context.tablet_handle_))) {
    LOG_WARN("failed to get delete index tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(merge_context.tablet_handle_.get_obj()->load_storage_schema(
      allocator, storage_schema))) {
    LOG_WARN("failed to load delete index storage schema", KR(ret), K(tablet_id));
  } else if (OB_FAIL(prepare_data_merge_context(
      ls_handle, tablet_id, *storage_schema, MAJOR_MERGE, snapshot_version, 0 /*base_version*/, merge_context))) {
    LOG_WARN("failed to prepare delete index merge context", KR(ret), K(tablet_id));
  } else {
    merge_context.static_param_.ls_rebuild_seq_ = ls->get_rebuild_seq();
    merge_context.static_param_.version_range_.multi_version_start_ = multi_version_start;
    merge_context.static_param_.merge_version_range_.multi_version_start_ = multi_version_start;
    // Direct merge contexts must receive the filter boundary fixed by the Medium plan.
    if (nullptr != mds_filter_info && OB_FAIL(merge_context.filter_ctx_.mds_filter_info_.assign(
        allocator, *mds_filter_info))) {
      LOG_WARN("failed to assign planned delete index filter info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(merge_context.prepare_compaction_filter())) {
      LOG_WARN("failed to prepare delete index compaction filter", KR(ret), K(tablet_id));
    } else if (OB_FAIL(build_non_empty_delete_index_merge_res(
        ls_handle, tablet_id, *storage_schema, snapshot_version,
        doc_ids, doc_id_cnt, allocator, res, row_snapshot_version, row_snapshots,
        merge_context.get_compaction_filter()))) {
      LOG_WARN("failed to build delete index merge res", KR(ret), K(tablet_id), K(doc_id_cnt));
    } else if (OB_FAIL(create_param.init_for_merge(merge_context, res, nullptr /*cg_schema*/, 0 /*cg_idx*/))) {
      LOG_WARN("failed to init delete index sstable param", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
        create_param, allocator, table_handle))) {
      LOG_WARN("failed to create delete index sstable", KR(ret), K(tablet_id));
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get delete index sstable from handle", KR(ret), K(tablet_id));
    } else if (OB_FAIL(merge_context.build_update_table_store_param(sstable, update_param))) {
      LOG_WARN("failed to build delete index update param", KR(ret), K(tablet_id));
    } else if (FALSE_IT(actual_multi_version_start = update_param.multi_version_start_)) {
    } else if (OB_FAIL(ls->update_tablet_table_store(tablet_id, update_param, new_tablet_handle))) {
      LOG_WARN("failed to update delete index tablet table store", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int calc_major_merge_multi_version_start(
    ObLS &ls,
    const ObTablet &tablet,
    const int64_t merge_snapshot,
    int64_t &multi_version_start)
{
  int ret = OB_SUCCESS;
  ObVersionRange version_range;
  ObStorageSnapshotInfo snapshot_info;
  version_range.base_version_ = 0;
  version_range.multi_version_start_ = tablet.get_multi_version_start();
  version_range.snapshot_version_ = merge_snapshot;
  if (OB_FAIL(ObPartitionMergePolicy::get_multi_version_start(
      MAJOR_MERGE, ls, tablet, version_range, snapshot_info))) {
    LOG_WARN("failed to calc major merge multi version start",
        KR(ret), "tablet_id", tablet.get_tablet_id(), K(merge_snapshot));
  } else {
    multi_version_start = version_range.multi_version_start_;
  }
  return ret;
}

} // namespace

} // namespace oceanbase

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::compaction;
using namespace oceanbase::blocksstable;

static const int64_t DELETE_PERCENT_DATA_ROW_COUNT = 100;
static const int64_t DELETE_PERCENT_DELETE_ROW_COUNT = 34;

namespace oceanbase
{
namespace unittest
{

struct AsyncIndexTabletCtx
{
  int64_t data_table_id_;
  int64_t fts_index_table_id_;
  int64_t delete_index_table_id_;
  ObTabletID data_tablet_id_;
  ObTabletID fts_index_tablet_id_;
  ObTabletID delete_index_tablet_id_;
  ObLSID ls_id_;

  TO_STRING_KV(K_(data_table_id), K_(fts_index_table_id), K_(delete_index_table_id),
               K_(data_tablet_id), K_(fts_index_tablet_id), K_(delete_index_tablet_id), K_(ls_id));
};

struct MultiAsyncIndexTabletCtx
{
  int64_t data_table_id_;
  int64_t delete_index_table_id_;
  ObTabletID data_tablet_id_;
  ObTabletID delete_index_tablet_id_;
  ObLSID ls_id_;
  ObSEArray<int64_t, 8> fts_index_table_ids_;
  ObSEArray<ObTabletID, 8> fts_index_tablet_ids_;

  TO_STRING_KV(K_(data_table_id), K_(delete_index_table_id), K_(data_tablet_id),
               K_(delete_index_tablet_id), K_(ls_id), K_(fts_index_table_ids), K_(fts_index_tablet_ids));
};

enum PartitionTableType : int64_t
{
  PART_HASH_4 = 0,
  PART_RANGE_3 = 1,
  PART_LIST_3 = 2,
  PART_HASH_MULTI_IDX_3 = 3,
};

class ObAsyncIndexCompactionTest : public ObStorageCachePolicyPrewarmerTest
{
public:
  ObAsyncIndexCompactionTest() : allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "AsyncIdxMit")) {}

  static void TearDownTestCase()
  {
    // The runner tears down the shared cluster after RUN_ALL_TESTS(), so all
    // parameterized suites and gtest result listeners can finish first.
  }

  static bool waited_;
  virtual void SetUp() override
  {
    // Allow an isolated local run to fit the host's available disk and memory.
    if (!is_started_) {
      const char *extra_options = std::getenv("OB_MITTEST_EXTRA_OPTSTR");
      if (OB_NOT_NULL(extra_options) && '\0' != extra_options[0]) {
        get_curr_simple_server().set_extra_optstr(extra_options);
      }
    }
    ObStorageCachePolicyPrewarmerTest::SetUp();
    share::ObTenantSwitchGuard tguard;
    OK(tguard.switch_to(run_ctx_.tenant_id_));
    enable_async_index_flags();
    // CREATE + post-DDL async index refresh can exceed the default ~9s DDL RPC floor
    // under shared-storage tablet create races; enlarge session/RPC timeouts for mittest.
    OK(sys_exe_sql("alter system set rpc_timeout = '60s';"));
    OK(exe_sql("set global ob_query_timeout = 120000000"));
    OK(exe_sql("set global ob_trx_timeout = 120000000"));
    OK(exe_sql("set ob_query_timeout = 120000000"));
    OK(exe_sql("set ob_trx_timeout = 120000000"));
    // 暂停后台 medium/major 调度：后台 MediumLoop 定时器（30s）会遍历全租户 tablet
    // 并连带调度 async index，与用例显式 schedule_for_async_index 竞争，造成 -4677
    // 漂移失败。显式 async-index DAG 不依赖此开关；需要测试租户级调度的
    // 用例自行控制 resume_major_merge()。
    MTL(ObTenantTabletScheduler *)->stop_major_merge();
    OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
    allocator_.reset();
    allocator_.set_attr(ObMemAttr(run_ctx_.tenant_id_, "AsyncIdxMit"));
    if (!waited_) {
      sleep(10);
      waited_ = true;
    }
  }

  void exe_prepare_sql();
  void enable_async_index_flags();
  void wait_async_index_flags(const bool enabled);
  void prepare_delete_index_publish_for_merge(
      const MultiAsyncIndexTabletCtx &ctx, ObLSHandle &ls_handle);
  void drop_table_with_recyclebin_mode(const char *table_name, const bool enable_recyclebin);
  void verify_async_delete_drop(const bool recyclebin);
  void verify_recycled_delete_merge_progress(
      const MultiAsyncIndexTabletCtx &ctx, const int64_t merge_snapshot, const int64_t schema_version);

  void create_async_fts_table(const char *table_name);
  void create_async_fts_table_with_index_cnt(const char *table_name, const int64_t fts_index_cnt);
  void create_async_fts_partition_table(const char *table_name, const PartitionTableType part_type);
  int64_t get_expected_part_num(const PartitionTableType part_type) const;
  int64_t get_expected_fts_index_cnt(const PartitionTableType part_type) const;
  int get_data_table_id(sqlclient::ObISQLConnection &conn, const char *table_name, int64_t &data_table_id);
  int get_aux_table_id(
      sqlclient::ObISQLConnection &conn,
      const int64_t data_table_id,
      const int64_t index_type,
      int64_t &aux_table_id);
  int count_aux_tables_by_index_type(
      sqlclient::ObISQLConnection &conn,
      const int64_t data_table_id,
      const int64_t index_type,
      int64_t &table_cnt);
  int get_fts_index_aux_table_ids(
      common::ObMySQLProxy &sql_proxy,
      const int64_t data_table_id,
      ObIArray<int64_t> &fts_index_table_ids);
  int get_tablet_and_ls_id_by_table_id(
      common::ObMySQLProxy &sql_proxy,
      const int64_t table_id,
      ObTabletID &tablet_id,
      ObLSID &ls_id);
  int get_tablet_and_ls_id_by_table_id_and_part_name(
      common::ObMySQLProxy &sql_proxy,
      const int64_t table_id,
      const char *part_name,
      ObTabletID &tablet_id,
      ObLSID &ls_id);
  int get_tablet_and_ls_id_by_table_name_and_part_name(
      common::ObMySQLProxy &sql_proxy,
      const char *table_name,
      const char *part_name,
      ObTabletID &tablet_id,
      ObLSID &ls_id);
  int get_aux_part_info(
      common::ObMySQLProxy &sql_proxy,
      const int64_t data_table_id,
      const int64_t index_type,
      int64_t &part_level,
      int64_t &part_num);
  void resolve_async_index_tablet_ctx(const char *table_name, AsyncIndexTabletCtx &ctx);
  void resolve_multi_async_index_tablet_ctx(const char *table_name, MultiAsyncIndexTabletCtx &ctx);
  void resolve_multi_async_index_tablet_ctx_for_refresh(
      const char *table_name, MultiAsyncIndexTabletCtx &ctx);
  void resolve_search_index_tablet_ctx(
      const char *table_name,
      ObLSID &ls_id,
      ObTabletID &tablet_id);
  void resolve_partition_multi_async_index_tablet_ctx(
      const char *table_name,
      const char *part_name,
      MultiAsyncIndexTabletCtx &ctx);
  int remove_async_index_refresh_produced_sstables(
      ObLSHandle &ls_handle, const ObTabletID &tablet_id);
  void remove_async_index_refresh_produced_sstables(MultiAsyncIndexTabletCtx &ctx);
  void remove_async_index_refresh_produced_sstables(AsyncIndexTabletCtx &ctx);
  void disable_and_drain_async_index_refresh();
  void prepare_read_boundary_refresh(
      const char *table_name, MultiAsyncIndexTabletCtx &ctx,
      ObIArray<uint64_t> &deleted_doc_ids);
  void observe_read_boundary_publication(
      const MultiAsyncIndexTabletCtx &ctx, SCN &commit_scn,
      ObAsyncIndexSegmentGroupCtx &groups);
  void verify_read_boundary_bitmap(
      const ObDeletionBitmapHandle &bitmap, const ObIArray<uint64_t> &expected);
  void check_multi_async_fts_table_ddl(const char *table_name, const int64_t fts_index_cnt);
  void check_partition_async_fts_table_ddl(const char *table_name, const PartitionTableType part_type);
  void run_async_index_compaction_schedule(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_compaction_execute(
      MultiAsyncIndexTabletCtx &ctx, const bool gc_after_schedule = false);
  void run_async_index_compaction_execute_with_nonempty_delete_index(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_inc_major_merge_execute(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_inc_major_merge_execute_empty(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_inc_major_merge_execute_nonempty(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_inc_major_compaction_schedule_and_execute(MultiAsyncIndexTabletCtx &ctx);
  int get_reused_macro_count_in_compaction_history(
      const ObTabletID &tablet_id,
      const int64_t compaction_scn,
      int64_t &reused_macro_count);
  void run_async_index_alternating_medium_inc_major_merge(MultiAsyncIndexTabletCtx &ctx);
  void set_undo_retention_zero();
  void set_undo_retention_default();
  void set_mvcc_gc_using_min_txn_snapshot(const bool enable);
  void trigger_async_index_sstable_gc(
      MultiAsyncIndexTabletCtx &ctx, const int64_t min_snapshot_gc_ts = 0);
  void run_scalar_tablets_keep_async_published_snapshot();
  bool has_major_sstable_at_snapshot(
      const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t snapshot_version);
  void schedule_and_execute_medium_major_merge(
      ObLSHandle &ls_handle,
      MultiAsyncIndexTabletCtx &ctx,
      int64_t &merge_snapshot);
  void add_small_inc_major_batch(
      ObLSHandle &ls_handle,
      MultiAsyncIndexTabletCtx &ctx,
      const int64_t inc_major_cnt,
      uint64_t &next_doc_id);
  void execute_inc_major_merge_round(
      ObLSHandle &ls_handle,
      MultiAsyncIndexTabletCtx &ctx,
      const int64_t expected_last_major_snapshot);
  void run_multi_major_async_index_compaction_schedule(MultiAsyncIndexTabletCtx &ctx);
  void run_choose_merge_range_at_incremental_capacity(const bool single_group);
  void run_choose_merge_range_capacity_without_first_group(const bool interleaved);
  void assert_major_sstable_cnt_at_least(
      const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t expect_min);
  void verify_empty_fts_inv_major_on_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool has_position_list);
  void verify_empty_search_inv_major_on_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool has_nested_key);
  void verify_empty_fts_inc_copies_inv_meta(
      const char *table_name,
      const bool has_position_list);
  void verify_empty_search_inc_copies_inv_meta(
      const char *table_name,
      const bool has_nested_key);
  void assert_latest_async_index_version_range(
      const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t expect_min);
  int get_tablet_inc_major_cnt(const ObTablet &tablet, int64_t &inc_major_cnt);
  int count_inc_major_segments_with_snapshot_lt(
      const ObTablet &tablet, const int64_t snapshot_upper_bound, int64_t &cnt);
  int count_inc_major_segments_with_base_lt(
      const ObTablet &tablet, const int64_t base_upper_bound, int64_t &cnt);
  int count_inc_major_segments_with_snapshot_gt(
      const ObTablet &tablet, const int64_t snapshot_lower_bound, int64_t &cnt);
  int has_inv_inc_major_at_version_range(
      const ObTablet &tablet,
      const int64_t expected_base_snapshot,
      const int64_t expected_snapshot,
      bool &found);
  int find_inv_inc_major_meta(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool expect_empty,
      ObInvSSTableMeta &inv_meta,
      SCN &end_scn,
      bool &found);
  int find_inv_inc_major_meta_since_min_start(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool expect_empty,
      const SCN &min_start_scn,
      ObInvSSTableMeta &inv_meta,
      SCN &end_scn,
      bool &found);
  int find_inv_inc_major_meta_impl(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool expect_empty,
      const SCN *min_start_scn,
      ObInvSSTableMeta &inv_meta,
      SCN &end_scn,
      bool &found);
  int wait_inv_inc_major_meta(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool expect_empty,
      ObInvSSTableMeta &inv_meta,
      SCN &end_scn);
  int wait_inv_inc_major_meta_since_min_start(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const bool expect_empty,
      const SCN &min_start_scn,
      ObInvSSTableMeta &inv_meta,
      SCN &end_scn);
  int get_tablet_handle(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  void check_storage_schema_async_index_flags(
      const ObTablet &tablet,
      const bool expect_is_async_index,
      const bool expect_is_async_delete_index,
      const ObIndexType expect_index_type);
  int get_ls_handle(const ObLSID &ls_id, ObLSHandle &ls_handle);
  void ensure_ls_reserved_snapshot_ready(const ObLSID &ls_id);
  int mock_inc_major_sstable(
      const ObTabletID &tablet_id,
      const int64_t base_version,
      const int64_t snapshot_version,
      const int64_t occupy_size,
      ObSSTable *&sstable,
      const bool committed = true,
      const bool is_delete_index = false,
      const ObScnRange *physical_range = nullptr,
      const int64_t trans_id = 100);
  int mock_major_sstable(
      const ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObInvSSTable *&sstable,
      const int64_t row_count = 100,
      const ObIndexType index_type = INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL,
      const int64_t occupy_size = 1000);
  int publish_async_index_version(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t published_version);
  int add_major_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const int64_t row_count = 100,
      const ObIndexType index_type = INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL,
      const int64_t occupy_size = 1000);
  int add_empty_data_major_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t snapshot_version);
  int add_delete_inc_major_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t base_version,
      const int64_t snapshot_version,
      const int64_t commit_version,
      const bool empty = false);
  int update_tablet_multi_version_start(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t multi_version_start);
  void verify_medium_info_medium_major(const ObMediumCompactionInfo &medium_info) const;
  void verify_medium_info_inc_major(
      const ObMediumCompactionInfo &medium_info,
      const ObNewVersionRange &merge_version_range) const;
  // as_inc_major_merge_result: install the table as an IncMajor *merge output*
  // (merge type INC_MAJOR_MERGE) instead of a plain refresh result. Only the
  // merge-output form drives try_recycle_inc_major_tables_(), which drops the
  // older IncMajors whose logical window the new table fully covers.
  int add_inc_major_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t base_version,
      const int64_t snapshot_version,
      const int64_t occupy_size,
      const bool committed = true,
      const bool as_inc_major_merge_result = false,
      const int64_t trans_id = 100);
  int fill_inc_major_segments(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t major_snapshot,
      const int64_t inc_major_cnt,
      const int64_t inc_major_step,
      const int64_t seg_size);
  const ObMediumCompactionInfo *find_medium_info_by_snapshot(
      const ObMediumCompactionInfoList *medium_list,
      const int64_t medium_snapshot) const;
  bool has_medium_snapshot(
      const ObMediumCompactionInfoList *medium_list,
      const int64_t medium_snapshot) const;
  int complete_mds_register_chain(const ObTabletMediumCompactionInfoMdsArg &arg, const int64_t tx_id);
  int register_mds_in_transaction(const ObTabletMediumCompactionInfoMdsArg &arg, const int64_t tx_id);
  void verify_submitted_medium_info_matches_expected(
      const ObMediumCompactionInfo &expected,
      const ObMediumCompactionInfo &actual,
      const ObNewVersionRange &merge_version_range) const;
  void verify_inc_major_cnt(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t expected_cnt);
  void verify_collect_merge_infos(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      const int64_t schedule_snapshot_version,
      const int64_t expected_tier_cnt,
      const ObAsyncIndexSSTableMergeInfo *expected_tiers = nullptr,
      const int64_t expected_tier_spec_cnt = 0);
  void build_and_submit_medium_info(
      ObLSHandle &ls_handle,
      ObTabletHandle &index_handle,
      const ObTabletID &index_tablet_id,
      const ObNewVersionRange &merge_version_range,
      const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason,
      const int64_t tx_id);
  int mock_inc_major_sstable_with_meta(
      const int64_t base_version,
      const int64_t snapshot_version,
      const int64_t data_checksum,
      const int64_t row_count,
      ObSSTable *&sstable,
      const bool committed = true);
  int add_sstable_to_handle(ObTablesHandleArray &handle, ObSSTable *sstable);
  int build_inc_major_table_array(
      const int64_t seg_cnt,
      const int64_t major_snapshot,
      const int64_t step,
      ObSEArray<ObITable *, 8> &tables);
  void run_choose_merge_range_tenant_major(MultiAsyncIndexTabletCtx &ctx);
  void run_choose_merge_range_no_need_merge(MultiAsyncIndexTabletCtx &ctx);
  void run_choose_merge_range_multi_index_varying_cnts(const int64_t fts_index_cnt);
  void run_choose_merge_range_multi_index_min_snapshot(const int64_t fts_index_cnt);
  void run_build_async_index_medium_info_with_merge_tables(MultiAsyncIndexTabletCtx &ctx);
  void fill_inv_meta_for_test(ObInvSSTableMeta &meta);
  int build_empty_inv_merge_res(
      ObStorageSchema &storage_schema,
      const int64_t row_count,
      const int64_t occupy_size,
      const int64_t snapshot_version,
      ObInvSSTableMeta &inv_meta,
      ObSSTableMergeRes &res);
  int prepare_inv_merge_context(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const ObMergeType merge_type,
      const int64_t snapshot_version,
      const int64_t base_version,
      ObTabletMergeCtx &merge_context);
  int build_inv_sstable_param_for_merge(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const ObMergeType merge_type,
      const int64_t snapshot_version,
      const int64_t base_version,
      const int64_t row_count,
      const int64_t occupy_size,
      ObInvSSTableMeta &inv_meta,
      ObTabletMergeCtx &merge_context,
      ObTabletCreateSSTableParam &param);
  int create_inv_sstable_from_param(
      ObTabletCreateSSTableParam &param,
      ObTableHandleV2 &table_handle,
      ObInvSSTable *&sstable);
  int add_empty_inv_inc_major_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const int64_t base_version,
      const int64_t snapshot_version);
  int fill_empty_inv_inc_major_segments(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const int64_t major_snapshot,
      const int64_t inc_major_cnt,
      const int64_t inc_major_step);
  int build_non_empty_inv_sstable_param_for_merge(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const ObMergeType merge_type,
      const int64_t snapshot_version,
      const int64_t base_version,
      const int64_t row_cnt,
      const uint64_t start_doc_id,
      const bool micro_index_clustered,
      ObTabletMergeCtx &merge_context,
      ObTabletCreateSSTableParam &param,
      const uint64_t doc_id_step = 1,
      const bool single_term = false,
      const int64_t rows_per_macro = 0);
  int add_non_empty_inv_sstable_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const ObMergeType merge_type,
      const int64_t base_version,
      const int64_t snapshot_version,
      const int64_t row_cnt,
      const uint64_t start_doc_id,
      const bool micro_index_clustered,
      const uint64_t doc_id_step = 1,
      const bool single_term = false,
      const int64_t rows_per_macro = 0);
  int add_non_empty_major_to_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const int64_t snapshot_version,
      const int64_t row_cnt,
      const uint64_t start_doc_id);
  int fill_non_empty_inv_inc_major_segments(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const int64_t major_snapshot,
      const int64_t inc_major_cnt,
      const int64_t inc_major_step,
      const uint64_t start_doc_id);
  int execute_async_index_merge_via_scheduler(
      ObLSHandle &ls_handle,
      ObTabletHandle &fts_handle,
      const ObMergeType merge_type,
      const int64_t merge_snapshot,
      const int64_t merge_base_snapshot = 0,
      const int64_t timeout_sec = 120,
      const ObExecMode exec_mode = EXEC_MODE_LOCAL);
  void prepare_shared_async_index_compactions(
      const char *table_name,
      MultiAsyncIndexTabletCtx &ctx,
      int64_t &major_snapshot,
      int64_t &inc_snapshot);
  void run_shared_async_index_readiness_case(
      const char *table_name, const bool fail_inc_major, const bool follower);
  int resolve_inc_major_merge_snapshot(
      ObLS &ls,
      const ObTablet &tablet,
      int64_t &merge_snapshot,
      int64_t &merge_base_snapshot,
      int64_t &input_table_cnt);
  void verify_merged_inv_inc_major_on_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t expected_base_snapshot,
      const int64_t expected_snapshot,
      const int64_t expected_row_cnt,
      const uint64_t expected_min_doc_id,
      const uint64_t expected_max_doc_id);
  void verify_merged_empty_inv_major_on_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t expected_snapshot,
      const int64_t expected_row_cnt = EXPECTED_MERGED_INV_ROW_CNT,
      const uint64_t expected_total_doc_length = EXPECTED_MERGED_INV_TOTAL_DOC_LENGTH);
  void verify_merged_inv_rows_match_snapshot(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t expected_snapshot,
      const ObIArray<InvIdxRowSnapshot> &expected_rows,
      const ObITable::TableType expected_table_type = ObITable::TableType::INV_INDEX_SSTABLE,
      const bool expect_clustered_index = false);
  void verify_delete_index_doc_ids_on_tablet(
      const MultiAsyncIndexTabletCtx &ctx,
      const int64_t snapshot_version,
      const uint64_t *doc_ids,
      const int64_t doc_id_cnt);
  void create_async_fts_table_for_parallel_merge(const char *table_name);
  int fill_parallel_inv_inc_major_segments(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObStorageSchema &storage_schema,
      const int64_t major_snapshot,
      const int64_t inc_major_cnt,
      const int64_t inc_major_step,
      const uint64_t start_doc_id,
      const bool micro_index_clustered);
  void reinit_async_index_parallel_merge_ctx(
      compaction::ObAsyncIndexMergeCtx *merge_ctx,
      const int64_t tablet_size);
  void verify_async_index_parallel_merge_ctx(
      compaction::ObAsyncIndexMergeCtx *merge_ctx,
      const int64_t expected_min_concurrent_cnt);
  void run_async_index_inc_major_parallel_merge_ctx_test(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_inc_major_parallel_merge_execute(MultiAsyncIndexTabletCtx &ctx);

  void fill_inc_major_info_if_needed_(
      ObMediumCompactionInfo &medium_info,
      const ObNewVersionRange &merge_version_range,
      ObLS &ls,
      const ObTablet &tablet);
  void submit_cloned_inc_major_medium_info_batch_(
      ObLSHandle &ls_handle,
      const ObTabletID &index_tablet_id,
      const ObMediumCompactionInfo &template_info,
      const int64_t *medium_snapshots,
      const int64_t *merge_base_versions,
      const int64_t *submit_order,
      const int64_t item_cnt,
      const int64_t tx_id);
  void assert_medium_info_list_sorted_by_key_(
      const ObMediumCompactionInfoList *medium_list,
      const int64_t *expected_merge_base_versions,
      const int64_t *expected_medium_snapshots,
      const int64_t expected_cnt) const;
  void run_async_index_multi_inc_major_medium_info_key_order(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_inc_major_merge_base_version(MultiAsyncIndexTabletCtx &ctx);
  void run_async_index_suffix_first_prefix_orphan_blocked(MultiAsyncIndexTabletCtx &ctx);

  int wait_ls_weak_read_ts(ObLS &ls, const int64_t target_ts_ns);
  int64_t calc_schedule_snapshot_version(const int64_t major_snapshot) const;
  int calc_common_published_snapshot(
      ObLS &ls,
      const ObIArray<ObTabletID> &index_tablet_ids,
      const ObTabletID &delete_index_tablet_id,
      int64_t &common_published_snapshot) const;
  int64_t get_max_tablet_create_commit_version(const MultiAsyncIndexTabletCtx &ctx);
  int64_t get_mock_major_snapshot(const MultiAsyncIndexTabletCtx &ctx);
  int64_t get_mock_row_count_major_snapshot(const int64_t mock_major_snapshot) const;
  void prepare_async_index_merge_candidate(
      MultiAsyncIndexTabletCtx &ctx,
      ObLSHandle &ls_handle,
      ObTabletHandle &data_tablet_handle,
      ObTabletHandle &fts_index_handle,
      ObNewVersionRange &merge_version_range);

  ObArenaAllocator allocator_;
  // Preserve segment ordering without putting later merge rounds hours ahead of GTS.
  static constexpr int64_t DEFAULT_INC_MAJOR_CNT = 12;
  static constexpr int64_t DEFAULT_INC_MAJOR_STEP = 1000L * 1000L; // 1 ms
  static constexpr int64_t DEFAULT_SEG_SIZE = 10000;
  static constexpr int64_t LARGE_MAJOR_OCCUPY_SIZE = 8000000L;
  // At least 2 rows per segment for non-trivial merge payload.
  static constexpr int64_t NON_EMPTY_INV_ROWS_PER_SEGMENT = 2;
  // ObPartitionMergePolicy::INC_MAJOR_COMPACT_TRIGGER is 3; need strictly more segments to merge.
  static constexpr int64_t INC_MAJOR_MERGE_INPUT_CNT = 5;
  // First inc major needs multiple macro blocks for range split (see get_async_index_parallel_ranges).
  static constexpr int64_t PARALLEL_FIRST_INC_MAJOR_ROW_CNT = 32000;
  // Keep multiple input macro blocks per parallel range to exercise whole-macro reuse.
  static int64_t parallel_merge_test_tablet_size()
  {
    return 4 * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  }
  // Small inc major batch size for alternating merge choreography.
  static constexpr int64_t SMALL_INC_MAJOR_BATCH_CNT = INC_MAJOR_MERGE_INPUT_CNT;
  // Enough inc majors to reach merge_segment_threshold_ (default 10) after prior rounds.
  static constexpr int64_t MEDIUM_TRIGGER_INC_MAJOR_BATCH_CNT = DEFAULT_INC_MAJOR_CNT;
  static constexpr int64_t EXPECTED_INC_MAJOR_MERGED_ROW_CNT =
      NON_EMPTY_INV_ROWS_PER_SEGMENT * INC_MAJOR_MERGE_INPUT_CNT;
  static constexpr int64_t EXPECTED_PARALLEL_INC_MAJOR_MERGED_ROW_CNT =
      PARALLEL_FIRST_INC_MAJOR_ROW_CNT
      + static_cast<int64_t>(INC_MAJOR_MERGE_INPUT_CNT - 1) * NON_EMPTY_INV_ROWS_PER_SEGMENT;
  // Parallel execute merges a large first segment; allow extra time for scheduler DAG.
  static constexpr int64_t PARALLEL_MERGE_EXECUTE_TIMEOUT_SEC = 600;
  static constexpr int64_t EXPECTED_MERGED_INV_ROW_CNT =
      NON_EMPTY_INV_ROWS_PER_SEGMENT * (1 + DEFAULT_INC_MAJOR_CNT);
  static constexpr uint64_t EXPECTED_MERGED_INV_START_DOC_ID = 1;
  static constexpr uint64_t EXPECTED_MERGED_INV_MIN_DOC_ID = EXPECTED_MERGED_INV_START_DOC_ID;
  static constexpr uint64_t EXPECTED_MERGED_INV_MAX_DOC_ID =
      EXPECTED_MERGED_INV_START_DOC_ID + EXPECTED_MERGED_INV_ROW_CNT - 1;
  static constexpr uint64_t EXPECTED_MERGED_INV_TOTAL_DOC_LENGTH = EXPECTED_MERGED_INV_ROW_CNT;
  static constexpr uint64_t DELETE_INDEX_MERGE_DOC_IDS[] = {2, 4, 6, 8};
  static constexpr int64_t DELETE_INDEX_MERGE_DOC_ID_CNT = 4;
  static constexpr int64_t EXPECTED_MERGED_INV_ROW_CNT_AFTER_DELETE =
      EXPECTED_MERGED_INV_ROW_CNT - DELETE_INDEX_MERGE_DOC_ID_CNT;
  static constexpr uint64_t EXPECTED_MERGED_INV_TOTAL_DOC_LENGTH_AFTER_DELETE =
      EXPECTED_MERGED_INV_TOTAL_DOC_LENGTH - DELETE_INDEX_MERGE_DOC_ID_CNT;
};

bool ObAsyncIndexCompactionTest::waited_ = false;

void ObAsyncIndexCompactionTest::enable_async_index_flags()
{
  OK(exe_sql("alter system set _enable_async_index = true;"));
  OK(exe_sql("alter system set _enable_async_index_refresh = true;"));
  ASSERT_NO_FATAL_FAILURE(wait_async_index_flags(true));
}

void ObAsyncIndexCompactionTest::wait_async_index_flags(const bool enabled)
{
  const int64_t deadline = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  bool applied = false;
  while (!applied && ObTimeUtility::current_time() < deadline) {
    applied = share::ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(MTL_ID()) == enabled
        && (!enabled || share::ObAsyncIndexRefreshUtil::is_async_index_enabled(MTL_ID()));
    if (!applied) {
      ob_usleep(100 * 1000);
    }
  }
  ASSERT_TRUE(applied) << "async index configuration did not take effect";
}

void ObAsyncIndexCompactionTest::prepare_delete_index_publish_for_merge(
    const MultiAsyncIndexTabletCtx &ctx, ObLSHandle &ls_handle)
{
  // Model the commit publication separately from installing the mock tables.
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    ObTabletHandle index_handle;
    int64_t materialized_snapshot = 0;
    ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
        ctx.ls_id_, ctx.fts_index_tablet_ids_.at(i), index_handle));
    ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->get_async_index_max_published_version(
        materialized_snapshot));
    ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
        ls_handle, ctx.fts_index_tablet_ids_.at(i), materialized_snapshot));
  }
  int64_t snapshot = 0;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      *ls_handle.get_ls(), ctx.fts_index_tablet_ids_, snapshot));
  ASSERT_GT(snapshot, 1);
  ASSERT_LT(snapshot, INT64_MAX);
  ObTabletHandle delete_handle;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  int64_t published = 0;
  ASSERT_EQ(OB_SUCCESS, delete_handle.get_obj()->get_async_index_max_published_version(published));
  if (published < snapshot) {
    ASSERT_EQ(OB_SUCCESS, add_empty_data_major_to_tablet(
        ls_handle, ctx.delete_index_tablet_id_, snapshot));
  }
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(
      ctx.ls_id_, ctx.delete_index_tablet_id_, delete_handle));
  ASSERT_EQ(OB_SUCCESS,
      delete_handle.get_obj()->get_async_index_max_published_version(published));
  ASSERT_GE(published, snapshot);
  ASSERT_EQ(OB_SUCCESS, publish_async_index_version(
      ls_handle, ctx.delete_index_tablet_id_, snapshot));
}

void ObAsyncIndexCompactionTest::exe_prepare_sql()
{
  enable_async_index_flags();
  // Keep aligned with SetUp: pooled sql_proxy connections may need session vars refreshed.
  OK(sys_exe_sql("alter system set rpc_timeout = '60s';"));
  OK(exe_sql("set global ob_query_timeout = 120000000"));
  OK(exe_sql("set global ob_trx_timeout = 120000000"));
  OK(exe_sql("set ob_query_timeout = 120000000"));
  OK(exe_sql("set ob_trx_timeout = 120000000"));
  ObStorageCachePolicyPrewarmerTest::exe_prepare_sql();
  // The base fixture shortens ob_compaction_schedule_interval to 3s.  Persist
  // the paused state in __all_zone as well, otherwise FreezeInfoMgr resumes the
  // local scheduler and MediumLoop races this test's explicit scheduling.
  OK(sys_exe_sql("alter system suspend merge tenant = tt1;"));
  MTL(ObTenantTabletScheduler *)->stop_major_merge();
  // MergeLoopTask (ss minor / inc-major upper-trans fill-in) is NOT gated by
  // the major-merge pause: an in-flight scan can still commit refresh-produced
  // inc majors onto this test's tablets mid-run. Stretching the interval only
  // reduces that interference; it does not drain existing scans or SSTable GC.
  // These scenarios explicitly schedule compaction instead of relying on this
  // timer. NOTE: the config range is [3s,5m]; larger values are rejected with
  // OB_INVALID_CONFIG.
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '5m' tenant tt1;"));
}

int ObAsyncIndexCompactionTest::remove_async_index_refresh_produced_sstables(
    ObLSHandle &ls_handle, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTabletTableStore *table_store = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  int64_t max_sync_medium_scn = 0;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid() || OB_ISNULL(tablet_handle.get_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_id));
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    // Hold the meta-cache reference throughout cleanup, including disk-backed
    // table stores; a block-scoped handle must not leave a borrowed raw pointer.
    if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
      LOG_WARN("failed to fetch table store", KR(ret), K(tablet_id));
    } else if (OB_ISNULL(table_store_wrapper.get_member())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table store is null", KR(ret), K(tablet_id));
    } else {
      table_store = const_cast<ObTabletTableStore *>(table_store_wrapper.get_member());
      table_store->inc_major_tables_.reset();
      table_store->inc_major_ddl_sstables_.reset();
      table_store->inc_major_ddl_mem_sstables_.reset();
      table_store->ddl_mem_sstables_.reset();
      if (OB_FAIL(tablet->load_storage_schema(allocator_, storage_schema))) {
        LOG_WARN("failed to load storage schema", KR(ret), K(tablet_id));
      } else if (OB_FAIL(tablet->get_max_sync_medium_scn(max_sync_medium_scn))) {
        LOG_WARN("failed to get max sync medium scn", KR(ret), K(tablet_id));
      } else if (OB_FAIL(tablet->table_store_cache_.init(
          *table_store, *storage_schema, max_sync_medium_scn))) {
        LOG_WARN("failed to reinit table store cache", KR(ret), K(tablet_id));
      } else {
        const int64_t bootstrap_snapshot = tablet->get_last_major_snapshot_version();
        if (bootstrap_snapshot > 0) {
          tablet->tablet_meta_.snapshot_version_ = bootstrap_snapshot;
          tablet->tablet_meta_.report_status_.merge_snapshot_version_ = bootstrap_snapshot;
        }
      }
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::disable_and_drain_async_index_refresh()
{
  // Async-index DDL requires refresh to be enabled.  Once the table has been
  // created, stop accepting refresh work and drain any DDL-triggered task
  // before this test installs its own deterministic inc-major inputs;
  // otherwise an in-flight refresh can install sstables after the cleanup
  // below and race the mock sstables (e.g. leaving two majors on the tablet).
  OK(exe_sql("alter system set _enable_async_index_refresh = false;"));
  ASSERT_NO_FATAL_FAILURE(wait_async_index_flags(false));
  ObAsyncIndexService *service = MTL(ObAsyncIndexService *);
  ASSERT_NE(nullptr, service);
  ObAsyncIndexRefreshWorker &worker = service->get_refresher().get_refresh_worker();
  const int64_t deadline_us = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  while (worker.get_in_flight_count() > 0 && ObTimeUtility::current_time() < deadline_us) {
    ob_usleep(100 * 1000);
  }
  ASSERT_EQ(0, worker.get_in_flight_count());
}

void ObAsyncIndexCompactionTest::remove_async_index_refresh_produced_sstables(
    MultiAsyncIndexTabletCtx &ctx)
{
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, remove_async_index_refresh_produced_sstables(
        ls_handle, ctx.fts_index_tablet_ids_.at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, remove_async_index_refresh_produced_sstables(
      ls_handle, ctx.delete_index_tablet_id_));
}

void ObAsyncIndexCompactionTest::remove_async_index_refresh_produced_sstables(
    AsyncIndexTabletCtx &ctx)
{
  ASSERT_NO_FATAL_FAILURE(disable_and_drain_async_index_refresh());
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ctx.ls_id_, ls_handle));
  ASSERT_EQ(OB_SUCCESS, remove_async_index_refresh_produced_sstables(
      ls_handle, ctx.fts_index_tablet_id_));
  ASSERT_EQ(OB_SUCCESS, remove_async_index_refresh_produced_sstables(
      ls_handle, ctx.delete_index_tablet_id_));
}

void ObAsyncIndexCompactionTest::create_async_fts_table(const char *table_name)
{
  create_async_fts_table_with_index_cnt(table_name, 1);
}

void ObAsyncIndexCompactionTest::create_async_fts_table_with_index_cnt(
    const char *table_name, const int64_t fts_index_cnt)
{
  ASSERT_NO_FATAL_FAILURE(enable_async_index_flags());
  static const char *COL_DEFS[] = {
      "c2 varchar(128)",
      "c3 varchar(256)",
      "c4 text",
      "c5 varchar(128)",
      "c6 varchar(256)",
  };
  static const char *IDX_NAMES[] = {"idx_a", "idx_b", "idx_c", "idx_d", "idx_e"};
  static const char *IDX_COLS[] = {"c2", "c3", "c4", "c5", "c6"};
  ASSERT_GE(fts_index_cnt, 1);
  ASSERT_LE(fts_index_cnt, 5);

  ObSqlString sql;
  OK(sql.assign_fmt("create table %s (c1 int", table_name));
  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    OK(sql.append_fmt(", %s", COL_DEFS[i]));
  }
  for (int64_t i = 0; i < fts_index_cnt; ++i) {
    OK(sql.append_fmt(", async fulltext index %s (%s)", IDX_NAMES[i], IDX_COLS[i]));
  }
  OK(sql.append(") organization heap"));
  OK(exe_sql(sql.ptr()));
}

int ObAsyncIndexCompactionTest::get_data_table_id(
    sqlclient::ObISQLConnection &conn, const char *table_name, int64_t &data_table_id)
{
  int ret = OB_SUCCESS;
  data_table_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table "
                 "where table_name = '%s' and data_table_id = 0 limit 1",
                 table_name);
  if (OB_FAIL(SimpleServerHelper::select_int64(&conn, sql.ptr(), data_table_id))) {
    LOG_WARN("failed to get data table id", KR(ret), K(sql));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_aux_table_id(
    sqlclient::ObISQLConnection &conn,
    const int64_t data_table_id,
    const int64_t index_type,
    int64_t &aux_table_id)
{
  int ret = OB_SUCCESS;
  aux_table_id = 0;
  ObSqlString sql;
  sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table "
                   "where data_table_id = %ld and index_type = %ld limit 1",
                   data_table_id, index_type);
  if (OB_FAIL(SimpleServerHelper::select_int64(&conn, sql.ptr(), aux_table_id))) {
    LOG_WARN("failed to get aux table id", KR(ret), K(sql));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_fts_index_aux_table_ids(
    common::ObMySQLProxy &sql_proxy,
    const int64_t data_table_id,
    ObIArray<int64_t> &fts_index_table_ids)
{
  int ret = OB_SUCCESS;
  fts_index_table_ids.reuse();
  ObSqlString sql;
  sql.assign_fmt("select table_id as val from oceanbase.__all_virtual_table "
                 "where data_table_id = %ld and index_type = %ld order by table_id",
                 data_table_id, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("failed to read sql", KR(ret), K(sql));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (OB_ISNULL(result)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCCESS == result->next()) {
          int64_t table_id = 0;
          if (OB_FAIL(result->get_int("val", table_id))) {
            LOG_WARN("failed to get table id", KR(ret));
          } else if (OB_FAIL(fts_index_table_ids.push_back(table_id))) {
            LOG_WARN("failed to push back fts index table id", KR(ret), K(table_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_tablet_and_ls_id_by_table_id(
    common::ObMySQLProxy &sql_proxy,
    const int64_t table_id,
    ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_ENTRY_NOT_EXIST;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;
  ObSqlString sql;
  for (int64_t i = 0; i < 60; ++i) {
    tmp_tablet_id = 0;
    tmp_ls_id = 0;
    sql.reuse();
    sql.assign_fmt("select tablet_id as val from oceanbase.__all_virtual_table "
                   "where table_id = %ld limit 1", table_id);
    if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_tablet_id))) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      sql.reuse();
      sql.assign_fmt("select ls_id as val from oceanbase.__all_tablet_to_ls "
                     "where tablet_id = %ld limit 1", tmp_tablet_id);
      if (OB_FAIL(SimpleServerHelper::select_int64(sql_proxy, sql.ptr(), tmp_ls_id))) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        tablet_id = ObTabletID(tmp_tablet_id);
        ls_id = ObLSID(tmp_ls_id);
        ret = OB_SUCCESS;
        break;
      }
    }
    sleep(1);
  }
  return ret;
}

// =============================================================================
// 倒排索引整宏块复用 — simple_server 端到端覆盖
//
// 测试目录（本分支增量）：
//   1. get_reused_macro_count_in_compaction_history()
//      - 按 tablet_id + compaction_scn 精确查询
//        __all_virtual_tablet_compaction_history.multiplexed_macro_block_count；
//      - 轮询等待 history 发布后再读取。
//   2. run_async_index_inc_major_parallel_merge_execute()
//      - tablet_size = macro_block_size，构造多宏块增量段后触发并行 INC_MAJOR_MERGE；
//      - 断言 multiplexed_macro_block_count > 0；
//      - verify_merged_inv_rows_match_snapshot / verify_merged_inv_inc_major_on_tablet
//        校验合并后 posting 与 inv_meta。
//   3. TEST async_index_inc_major_parallel_merge_ctx
//      - 并行 merge ctx 初始化与最小并发数校验（不含 history 断言）。
//   4. TEST async_index_inc_major_parallel_merge_execute
//      - 上述 execute 路径的 scheduler 端到端入口；输入增量段启用 clustered index，
//        合并后重新加载 SSTable 并断言 other blocks 非空。
//
// 关联覆盖：test_inv_sstable_merge_reuse.cpp（merger iter/writer 层）、
// test_inv_idx_macro_block_writer.cpp（writer append_macro_block）、
// async_index_macro_reuse.test（obtest SQL 场景）。
// =============================================================================

void ObAsyncIndexCompactionTest::resolve_multi_async_index_tablet_ctx(
    const char *table_name, MultiAsyncIndexTabletCtx &ctx)
{
  resolve_multi_async_index_tablet_ctx_for_refresh(table_name, ctx);
  remove_async_index_refresh_produced_sstables(ctx);
  LOG_INFO("resolve multi async index tablet ctx", K(table_name), K(ctx));
}

void ObAsyncIndexCompactionTest::resolve_multi_async_index_tablet_ctx_for_refresh(
    const char *table_name, MultiAsyncIndexTabletCtx &ctx)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *conn = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(conn));
  ASSERT_NE(nullptr, conn);
  DEFER(if (OB_NOT_NULL(conn)) {
    EXPECT_EQ(OB_SUCCESS, sql_proxy.close(conn, OB_SUCCESS));
  });

  ctx.fts_index_table_ids_.reuse();
  ctx.fts_index_tablet_ids_.reuse();
  ASSERT_EQ(OB_SUCCESS, get_data_table_id(*conn, table_name, ctx.data_table_id_));
  ASSERT_GT(ctx.data_table_id_, 0);
  ASSERT_EQ(OB_SUCCESS, get_fts_index_aux_table_ids(sql_proxy, ctx.data_table_id_, ctx.fts_index_table_ids_));
  ASSERT_GT(ctx.fts_index_table_ids_.count(), 0);
  ASSERT_EQ(OB_SUCCESS, get_aux_table_id(*conn, ctx.data_table_id_, INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, ctx.delete_index_table_id_));
  ASSERT_GT(ctx.delete_index_table_id_, 0);

  ObLSID delete_ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(sql_proxy, ctx.data_table_id_, ctx.data_tablet_id_, ctx.ls_id_));
  for (int64_t i = 0; i < ctx.fts_index_table_ids_.count(); ++i) {
    ObTabletID fts_tablet_id;
    ObLSID fts_ls_id;
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
        sql_proxy, ctx.fts_index_table_ids_.at(i), fts_tablet_id, fts_ls_id));
    ASSERT_EQ(ctx.ls_id_, fts_ls_id);
    ASSERT_EQ(OB_SUCCESS, ctx.fts_index_tablet_ids_.push_back(fts_tablet_id));
  }
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id_by_table_id(
      sql_proxy, ctx.delete_index_table_id_, ctx.delete_index_tablet_id_, delete_ls_id));
  ASSERT_EQ(ctx.ls_id_, delete_ls_id);
  LOG_INFO("resolve multi async index tablet ctx for refresh", K(table_name), K(ctx));
}

int64_t ObAsyncIndexCompactionTest::calc_schedule_snapshot_version(const int64_t major_snapshot) const
{
  return major_snapshot + DEFAULT_INC_MAJOR_CNT * DEFAULT_INC_MAJOR_STEP;
}

int ObAsyncIndexCompactionTest::calc_common_published_snapshot(
    ObLS &ls,
    const ObIArray<ObTabletID> &index_tablet_ids,
    const ObTabletID &delete_index_tablet_id,
    int64_t &common_published_snapshot) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 8> merge_tablet_ids;
  if (OB_FAIL(merge_tablet_ids.assign(index_tablet_ids))) {
    LOG_WARN("failed to copy async index tablet ids", KR(ret), K(index_tablet_ids));
  } else if (delete_index_tablet_id.is_valid()
             && OB_FAIL(merge_tablet_ids.push_back(delete_index_tablet_id))) {
    LOG_WARN("failed to add async delete index tablet", KR(ret), K(delete_index_tablet_id));
  } else if (OB_FAIL(ObAsyncIndexMergePolicyHelper::calc_schedule_snapshot_version(
      ls, merge_tablet_ids, common_published_snapshot))) {
    LOG_WARN("failed to calc common published snapshot", KR(ret), K(merge_tablet_ids));
  }
  return ret;
}

int64_t ObAsyncIndexCompactionTest::get_max_tablet_create_commit_version(
    const MultiAsyncIndexTabletCtx &ctx)
{
  int64_t max_create_commit = 0;
  auto collect_create_commit = [&](const ObTabletID &tablet_id) {
    ObTabletHandle handle;
    ObTabletCreateDeleteMdsUserData user_data;
    mds::MdsWriter writer;
    mds::TwoPhaseCommitState trans_state;
    share::SCN trans_version;
    if (OB_SUCCESS != get_tablet_handle(ctx.ls_id_, tablet_id, handle)) {
      // ignore lookup failure; caller asserts tablet exists when needed
    } else if (OB_SUCCESS != handle.get_obj()->get_latest_tablet_status(
        user_data, writer, trans_state, trans_version)) {
      // ignore MDS read failure
    } else if (transaction::ObTransVersion::INVALID_TRANS_VERSION != user_data.create_commit_version_
        && user_data.create_commit_version_ > max_create_commit) {
      max_create_commit = user_data.create_commit_version_;
    }
  };

  collect_create_commit(ctx.data_tablet_id_);
  collect_create_commit(ctx.delete_index_tablet_id_);
  for (int64_t i = 0; i < ctx.fts_index_tablet_ids_.count(); ++i) {
    collect_create_commit(ctx.fts_index_tablet_ids_.at(i));
  }
  return max_create_commit;
}

int64_t ObAsyncIndexCompactionTest::get_mock_major_snapshot(
    const MultiAsyncIndexTabletCtx &ctx)
{
  const int64_t max_create_commit = get_max_tablet_create_commit_version(ctx);
  if (max_create_commit <= 0 || max_create_commit >= INT64_MAX - DEFAULT_INC_MAJOR_STEP - 1) {
    ADD_FAILURE() << "invalid tablet create commit version: " << max_create_commit;
    return 0;
  }
  // Even the earlier row-count Major must follow tablet creation. Actual reads
  // and merge execution wait for the real GTS/WRS to reach their target version.
  const int64_t mock_major = max_create_commit + DEFAULT_INC_MAJOR_STEP + 1;
  LOG_INFO("mock major snapshot aligned with tablet create commit",
           K(max_create_commit), K(mock_major), K(ctx));
  return mock_major;
}

int64_t ObAsyncIndexCompactionTest::get_mock_row_count_major_snapshot(const int64_t mock_major_snapshot) const
{
  return mock_major_snapshot - DEFAULT_INC_MAJOR_STEP;
}

void ObAsyncIndexCompactionTest::assert_major_sstable_cnt_at_least(
    const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t expect_min)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  ASSERT_GE(table_store_wrapper.get_member()->get_major_sstables().count(), expect_min);
}

int ObAsyncIndexCompactionTest::get_tablet_inc_major_cnt(
    const ObTablet &tablet, int64_t &inc_major_cnt)
{
  int ret = OB_SUCCESS;
  inc_major_cnt = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret));
  } else if (OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    inc_major_cnt = table_store_wrapper.get_member()->get_inc_major_sstables().count();
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_tablet_handle(
    const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService *);
  ObLSHandle ls_handle;
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::get_ls_handle(const ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService *);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  }
  return ret;
}

void ObAsyncIndexCompactionTest::ensure_ls_reserved_snapshot_ready(const ObLSID &ls_id)
{
  ObTenantFreezeInfoMgr *freeze_info_mgr = MTL(ObTenantFreezeInfoMgr *);
  ASSERT_NE(nullptr, freeze_info_mgr);
  ASSERT_EQ(OB_SUCCESS, freeze_info_mgr->try_update_reserved_snapshot());

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, get_ls_handle(ls_id, ls_handle));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // Reserved snapshots are monotonic and persisted. Wait for the normal producer
  // instead of moving the LS into the future to bypass scheduling checks.
  const int64_t deadline_us = ObTimeUtility::current_time() + 30L * 1000 * 1000;
  while (0 == ls->get_min_reserved_snapshot()
      && ObTimeUtility::current_time() < deadline_us) {
    ::ob_usleep(100 * 1000);
    ASSERT_EQ(OB_SUCCESS, freeze_info_mgr->try_update_reserved_snapshot());
  }
  ASSERT_GT(ls->get_min_reserved_snapshot(), 0);
  LOG_INFO("ls reserved snapshot ready", K(ls_id), "min_reserved_snapshot", ls->get_min_reserved_snapshot());
}

int ObAsyncIndexCompactionTest::mock_inc_major_sstable(
    const ObTabletID &tablet_id,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t occupy_size,
    ObSSTable *&sstable,
    const bool committed,
    const bool is_delete_index,
    const ObScnRange *physical_range,
    const int64_t trans_id)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  sstable = nullptr;
  ObTabletCreateSSTableParam param;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.table_type_ = is_delete_index
      ? ObITable::TableType::INC_MAJOR_SSTABLE : ObITable::TableType::INC_INV_INDEX_SSTABLE;
  // IncMajor TableKeys now carry the physical CLOG range and are compared as a
  // whole when the table store de-duplicates same-key IncMajors. Deriving the
  // physical range from the logical one keeps distinct logical windows from
  // collapsing onto an identical key (e.g. a wide [base, base + N*STEP] segment
  // and the last narrow [base + (N-1)*STEP, base + N*STEP] segment share a
  // snapshot but are different tables). base_version 0 means "from origin" and
  // has no usable physical scn, so fall back to the minimal base scn there.
  if (base_version > 0) {
    param.table_key_.scn_range_.start_scn_.convert_for_tx(base_version);
  } else {
    param.table_key_.scn_range_.start_scn_.set_base();
  }
  param.table_key_.scn_range_.end_scn_.convert_for_tx(snapshot_version);
  if (nullptr != physical_range) {
    param.table_key_.scn_range_ = *physical_range;
  }
  param.min_merged_trans_version_ = base_version;
  param.max_merged_trans_version_ = snapshot_version;
  param.upper_trans_version_ = committed ? snapshot_version : INT64_MAX;
  param.schema_version_ = ObTimeUtility::current_time();
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.index_type_ = is_delete_index
      ? INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL : INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
  param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  param.rowkey_column_cnt_ = 2;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = occupy_size;
  param.original_size_ = occupy_size;
  param.ddl_scn_.set_base();
  param.filled_tx_scn_ = param.table_key_.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_ = SCN::plus(param.table_key_.get_start_scn(), 1);
  if (param.table_key_.get_start_scn() == param.table_key_.get_end_scn()) {
    param.rec_scn_ = param.table_key_.get_start_scn();
  }
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 100;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  param.full_column_cnt_ = 0;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;
  param.column_cnt_ = param.rowkey_column_cnt_;
  compaction::ObUncommitTxDesc tx_desc(trans_id, ObTxSEQ::from_seq(200).cast_to_int());
  param.uncommit_tx_info_.push_back(tx_desc);
  param.uncommit_tx_info_.set_info_status_record_seq();
  ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_);
  ObInvIdxStatsMeta inv_stats;
  inv_stats.index_type_ = INV_IDX_TYPE_FTS;
  inv_stats.has_relevance_params_ = true;
  inv_stats.has_position_list_ = false;
  inv_stats.has_nested_key_ = false;
  inv_stats.doc_id_obj_meta_.set_uint64();
  inv_stats.min_doc_id_ = common::OB_INVALID_ID;
  inv_stats.max_doc_id_ = 0;
  if (OB_FAIL(param.inv_meta_.init(inv_stats))) {
    LOG_WARN("failed to initialize inverted sstable metadata", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(is_delete_index ? sizeof(ObSSTable) : sizeof(ObInvSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sstable", K(ret));
  } else if (OB_ISNULL(sstable = is_delete_index
      ? new (buf) ObSSTable() : new (buf) ObInvSSTable())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(sstable->init(param, &allocator_))) {
    LOG_WARN("failed to init inc major sstable", KR(ret));
  } else {
    sstable->meta_->basic_meta_.occupy_size_ = occupy_size;
    sstable->meta_cache_.occupy_size_ = occupy_size;
  }
  return ret;
}

void ObAsyncIndexCompactionTest::verify_medium_info_medium_major(
    const ObMediumCompactionInfo &medium_info) const
{
  ASSERT_TRUE(medium_info.is_medium_compaction());
  ASSERT_FALSE(medium_info.is_inc_major_compaction());
}

int ObAsyncIndexCompactionTest::mock_major_sstable(
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    ObInvSSTable *&sstable,
    const int64_t row_count,
    const ObIndexType index_type,
    const int64_t occupy_size)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  sstable = nullptr;
  ObTabletCreateSSTableParam param;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.table_type_ = ObITable::TableType::INV_INDEX_SSTABLE;
  param.table_key_.version_range_.base_version_ = 0;
  param.table_key_.version_range_.snapshot_version_ = snapshot_version;
  param.table_key_.scn_range_.start_scn_.set_min();
  param.table_key_.scn_range_.end_scn_.convert_for_tx(snapshot_version);
  param.max_merged_trans_version_ = snapshot_version;
  param.schema_version_ = ObTimeUtility::current_time();
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.index_type_ = index_type;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
  param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  param.rowkey_column_cnt_ = 2;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = occupy_size;
  param.original_size_ = occupy_size;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_ = param.table_key_.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_.set_min();
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = row_count;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  param.full_column_cnt_ = 0;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;
  param.column_cnt_ = param.rowkey_column_cnt_;
  ObInvIdxStatsMeta inv_stats;
  inv_stats.index_type_ = INV_IDX_TYPE_FTS;
  inv_stats.has_relevance_params_ = true;
  inv_stats.has_position_list_ = false;
  inv_stats.has_nested_key_ = false;
  inv_stats.doc_id_obj_meta_.set_uint64();
  inv_stats.min_doc_id_ = common::OB_INVALID_ID;
  inv_stats.max_doc_id_ = 0;
  if (OB_FAIL(param.inv_meta_.init(inv_stats))) {
    LOG_WARN("failed to initialize inverted sstable metadata", K(ret));
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(
      param.column_cnt_, param.column_checksums_))) {
    LOG_WARN("failed to fill column checksum", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObInvSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate sstable", K(ret));
  } else if (OB_ISNULL(sstable = new (buf) ObInvSSTable())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(sstable->init(param, &allocator_))) {
    LOG_WARN("failed to init major sstable", KR(ret));
  }
  return ret;
}

int ObAsyncIndexCompactionTest::publish_async_index_version(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t published_version)
{
  int ret = OB_SUCCESS;
  ObAsyncIndexDepProvider provider;
  ObTabletAsyncIndexRefreshInfo info;
  if (OB_FAIL(provider.get_async_index_refresh_info(
          MTL_ID(), ls_handle.get_ls(), tablet_id, info))) {
    LOG_WARN("failed to read test refresh info", KR(ret), K(tablet_id));
  } else {
    // Installing mock SSTables alone does not publish the refresh.
    info.set_published_version(published_version);
    if (OB_FAIL(provider.set_async_index_refresh_info(
            MTL_ID(), ls_handle.get_ls(), tablet_id, info))) {
      LOG_WARN("failed to publish test refresh", KR(ret), K(tablet_id), K(published_version));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version,
    const int64_t row_count,
    const ObIndexType index_type,
    const int64_t occupy_size)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam param;
  ObInvSSTable *sstable = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(mock_major_sstable(tablet_id, snapshot_version, sstable, row_count, index_type, occupy_size))) {
    LOG_WARN("failed to mock major sstable", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema))) {
    LOG_WARN("failed to load storage schema", KR(ret), K(tablet_id));
  } else {
    // A committed mock SSTable must also advance the tablet read snapshot.
    param.snapshot_version_ = MAX(tablet_handle.get_obj()->get_snapshot_version(), snapshot_version);
    param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
    param.storage_schema_ = storage_schema;
    param.rebuild_seq_ = ls->get_rebuild_seq();
    param.ddl_info_.update_with_major_flag_ = false;
    param.sstable_ = sstable;
    param.allow_duplicate_sstable_ = false;
    if (OB_FAIL(param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
      LOG_WARN("failed to init ha info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                      ObMergeType::MAJOR_MERGE, sstable->get_end_scn(), false,
                      tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init compaction info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ls->update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_empty_data_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObTableHandleV2 table_handle;
  ObSSTable *sstable = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObTabletCreateSSTableParam create_param;
  ObUpdateTableStoreParam update_param;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get data tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(
      allocator_, storage_schema))) {
    LOG_WARN("failed to load data tablet storage schema", KR(ret), K(tablet_id));
  } else if (OB_FAIL(create_param.init_for_empty_major_sstable(
      tablet_id,
      *storage_schema,
      snapshot_version,
      -1 /*column_group_idx*/,
      false /*has_all_column_group*/,
      false /*is_shared*/))) {
    LOG_WARN("failed to init empty data major", KR(ret), K(tablet_id), K(snapshot_version));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
      create_param, allocator_, table_handle))) {
    LOG_WARN("failed to create empty data major", KR(ret), K(tablet_id), K(snapshot_version));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get empty data major", KR(ret), K(tablet_id), K(snapshot_version));
  } else {
    update_param.snapshot_version_ = snapshot_version;
    update_param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
    update_param.storage_schema_ = storage_schema;
    update_param.rebuild_seq_ = ls->get_rebuild_seq();
    update_param.ddl_info_.update_with_major_flag_ = false;
    update_param.sstable_ = sstable;
    update_param.allow_duplicate_sstable_ = false;
    if (OB_FAIL(update_param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
      LOG_WARN("failed to init data major ha info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(update_param.init_with_compaction_info(ObCompactionTableStoreParam(
                      ObMergeType::MAJOR_MERGE,
                      sstable->get_end_scn(),
                      false /*need_report*/,
                      tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init data major compaction info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(ls->update_tablet_table_store(
        tablet_id, update_param, new_tablet_handle))) {
      LOG_WARN("failed to add empty data major", KR(ret), K(tablet_id), K(snapshot_version));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::add_inc_major_to_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t base_version,
    const int64_t snapshot_version,
    const int64_t occupy_size,
    const bool committed,
    const bool as_inc_major_merge_result,
    const int64_t trans_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam param;
  ObSSTable *sstable = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTablesHandleArray input_tables;
  ObLS *ls = ls_handle.get_ls();
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(mock_inc_major_sstable(tablet_id, base_version, snapshot_version,
      occupy_size, sstable, committed, false /*is_delete_index*/, nullptr, trans_id))) {
    LOG_WARN("failed to mock inc major sstable", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator_, storage_schema))) {
    LOG_WARN("failed to load storage schema", KR(ret), K(tablet_id));
  } else if (as_inc_major_merge_result
      && OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", KR(ret), K(tablet_id));
  } else if (as_inc_major_merge_result && OB_ISNULL(table_store_wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is null", KR(ret), K(tablet_id));
  } else {
    // A COMPACTION install must list the tables it merges; the table store checks
    // that every input still exists before accepting the result. Here the merge
    // consumes all IncMajors currently on the tablet.
    if (as_inc_major_merge_result) {
      const ObSSTableArray &inc_majors = table_store_wrapper.get_member()->get_inc_major_sstables();
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_majors.count(); ++i) {
        ObTableHandleV2 handle;
        if (OB_ISNULL(inc_majors.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inc major input table is null", KR(ret), K(i), K(tablet_id));
        } else if (OB_FAIL(handle.set_sstable_with_tablet(inc_majors.at(i)))) {
          LOG_WARN("failed to set inc major input handle", KR(ret), K(i), K(tablet_id));
        } else if (OB_FAIL(input_tables.add_table(handle))) {
          LOG_WARN("failed to add inc major input table", KR(ret), K(i), K(tablet_id));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    // A committed mock SSTable must also advance the tablet read snapshot.
    param.snapshot_version_ = committed
        ? MAX(tablet_handle.get_obj()->get_snapshot_version(), snapshot_version)
        : tablet_handle.get_obj()->get_snapshot_version();
    param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
    param.storage_schema_ = storage_schema;
    param.rebuild_seq_ = ls->get_rebuild_seq();
    param.ddl_info_.update_with_major_flag_ = false;
    param.sstable_ = sstable;
    param.allow_duplicate_sstable_ = false;
    param.inc_major_sstable_action_ = committed
        ? ObIncMajorSSTableAction::LINK
        : ObIncMajorSSTableAction::ADD;

    if (OB_FAIL(param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_, true)))) {
      LOG_WARN("failed to init ha info", KR(ret), K(tablet_id));
    } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                      as_inc_major_merge_result ? ObMergeType::INC_MAJOR_MERGE
                                                : ObMergeType::MAJOR_MERGE,
                      sstable->get_end_scn(), false,
                      tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init compaction info", KR(ret), K(tablet_id));
    } else if (as_inc_major_merge_result
        && FALSE_IT(param.input_tables_handle_ = &input_tables)) {
    } else if (OB_FAIL(ls->update_tablet_table_store(tablet_id, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexCompactionTest::fill_inc_major_segments(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const int64_t major_snapshot,
    const int64_t inc_major_cnt,
    const int64_t inc_major_step,
    const int64_t seg_size)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < inc_major_cnt; ++i) {
    const int64_t base = major_snapshot + i * inc_major_step;
    const int64_t snap = base + inc_major_step;
    if (OB_FAIL(add_inc_major_to_tablet(ls_handle, tablet_id, base, snap, seg_size))) {
      LOG_WARN("failed to add inc major segment", KR(ret), K(tablet_id), K(i));
    }
  }
  return ret;
}

const ObMediumCompactionInfo *ObAsyncIndexCompactionTest::find_medium_info_by_snapshot(
    const ObMediumCompactionInfoList *medium_list,
    const int64_t medium_snapshot) const
{
  const ObMediumCompactionInfo *found = nullptr;
  if (OB_NOT_NULL(medium_list)) {
    DLIST_FOREACH_NORET(info, medium_list->get_list()) {
      if (OB_NOT_NULL(info) && info->medium_snapshot_ == medium_snapshot) {
        found = info;
        break;
      }
    }
  }
  return found;
}

int ObAsyncIndexCompactionTest::register_mds_in_transaction(
    const ObTabletMediumCompactionInfoMdsArg &arg, const int64_t tx_id)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  UNUSED(tx_id);
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, MTL_ID()))) {
    STORAGE_LOG(WARN, "failed to start mds register transaction", K(ret), K(arg));
  } else if (OB_FAIL(ObTabletMediumCompactionInfoMdsHelper::register_mds(arg, trans))) {
    STORAGE_LOG(WARN, "failed to register medium compaction mds", K(ret), K(arg));
  }
  if (trans.is_started()) {
    const int trans_ret = trans.end(OB_SUCCESS == ret);
    if (OB_SUCCESS != trans_ret && OB_SUCCESS == ret) {
      ret = trans_ret;
    }
  }
  return ret;
}

void ObAsyncIndexCompactionTest::verify_submitted_medium_info_matches_expected(
    const ObMediumCompactionInfo &expected,
    const ObMediumCompactionInfo &actual,
    const ObNewVersionRange &merge_version_range) const
{
  ASSERT_TRUE(expected.is_valid());
  ASSERT_TRUE(actual.is_valid());
  ASSERT_EQ(merge_version_range.snapshot_version_, actual.medium_snapshot_);
  ASSERT_EQ(expected.medium_snapshot_, actual.medium_snapshot_);
  const ObMediumCompactionInfo::ObCompactionType expected_compaction_type =
      (0 != merge_version_range.base_version_)
      ? ObMediumCompactionInfo::INC_MAJOR_COMPACTION
      : ObMediumCompactionInfo::MEDIUM_COMPACTION;
  ASSERT_EQ(expected_compaction_type, actual.compaction_type_);
  ASSERT_EQ(expected.compaction_type_, actual.compaction_type_);
  ASSERT_EQ(expected.last_medium_snapshot_, actual.last_medium_snapshot_);
  ASSERT_EQ(expected.contain_inc_major_info_, actual.contain_inc_major_info_);
  if (expected.contain_inc_major_info_) {
    ASSERT_TRUE(actual.contain_inc_major_info_);
    ASSERT_EQ(expected.inc_major_info_.get_size(), actual.inc_major_info_.get_size());
    for (int64_t i = 0; i < expected.inc_major_info_.get_size(); ++i) {
      const ObIncMajorSSTableInfo::IncMajorInfo &exp_item = expected.inc_major_info_.info_list_[i];
      const ObIncMajorSSTableInfo::IncMajorInfo &act_item = actual.inc_major_info_.info_list_[i];
      ASSERT_EQ(exp_item.start_scn_, act_item.start_scn_);
      ASSERT_EQ(exp_item.end_scn_, act_item.end_scn_);
      ASSERT_EQ(exp_item.data_checksum_, act_item.data_checksum_);
    }
  } else {
    ASSERT_FALSE(actual.contain_inc_major_info_);
  }
}

void ObAsyncIndexCompactionTest::verify_inc_major_cnt(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t expected_cnt)
{
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, get_tablet_handle(ls_id, tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  ASSERT_NE(nullptr, table_store_wrapper.get_member());
  ASSERT_EQ(expected_cnt, table_store_wrapper.get_member()->get_inc_major_sstables().count());
}

static ObAsyncIndexSSTableMergeInfo make_expected_merge_tier(
    const int64_t start_snapshot_version,
    const int64_t end_snapshot_version)
{
  ObAsyncIndexSSTableMergeInfo info;
  info.start_snapshot_version_ = start_snapshot_version;
  info.end_snapshot_version_ = end_snapshot_version;
  info.seg_size_ = 0;
  info.level_ = 0.0f;
  return info;
}

void ObAsyncIndexCompactionTest::build_and_submit_medium_info(
    ObLSHandle &ls_handle,
    ObTabletHandle &index_handle,
    const ObTabletID &index_tablet_id,
    const ObNewVersionRange &merge_version_range,
    const ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason,
    const int64_t tx_id)
{
  SCN weak_read_scn;
  const ObMediumCompactionInfoList *medium_list_ptr = nullptr;
  ObMediumCompactionInfo expected_info(allocator_);
  ObTabletMediumCompactionInfoMdsArg mds_arg;

  ensure_ls_reserved_snapshot_ready(ls_handle.get_ls()->get_ls_id());
  ASSERT_EQ(OB_SUCCESS, weak_read_scn.convert_for_tx(6000 + tx_id));
  ASSERT_EQ(OB_SUCCESS, index_handle.get_obj()->read_medium_info_list(allocator_, medium_list_ptr));
  ASSERT_NE(nullptr, medium_list_ptr);
  ObMediumCompactionScheduleFunc index_schedule_func(
      *ls_handle.get_ls(),
      weak_read_scn,
      *medium_list_ptr,
      nullptr,
      merge_reason,
      0,
      nullptr);
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.init_tablet_handle(index_handle));
  ASSERT_EQ(OB_SUCCESS, index_schedule_func.build_async_index_medium_info(merge_version_range, expected_info));
  int64_t tablet_inc_major_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, get_tablet_inc_major_cnt(*index_handle.get_obj(), tablet_inc_major_cnt));
  mds_arg.ls_id_ = ls_handle.get_ls()->get_ls_id();
  ASSERT_EQ(OB_SUCCESS, mds_arg.add_item(index_tablet_id, expected_info));
  ASSERT_EQ(OB_SUCCESS, register_mds_in_transaction(mds_arg, tx_id));

  ObTabletHandle reload_handle;
  const ObMediumCompactionInfoList *registered_medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(index_tablet_id, reload_handle));
  ASSERT_EQ(OB_SUCCESS, reload_handle.get_obj()->read_medium_info_list(allocator_, registered_medium_list));
  ASSERT_NE(nullptr, registered_medium_list);
  const ObMediumCompactionInfo *actual_info = find_medium_info_by_snapshot(
      registered_medium_list, merge_version_range.snapshot_version_);
  ASSERT_NE(nullptr, actual_info);
  verify_submitted_medium_info_matches_expected(expected_info, *actual_info, merge_version_range);
}

/*
*/

} // namespace unittest
} // namespace oceanbase

int run_async_index_compaction_tests(int argc, char **argv)
{
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  if (OB_ISNULL(s3_bucket) || '\0' == s3_bucket[0]) {
    s3_bucket = oceanbase::unittest::S3_BUCKET;
  }
  const char *s3_endpoint_override = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const bool use_test_endpoint = OB_NOT_NULL(s3_endpoint_override)
      && '\0' != s3_endpoint_override[0];
  const char *s3_endpoint = use_test_endpoint
      ? s3_endpoint_override : oceanbase::unittest::S3_ENDPOINT;
  // Endpoint override is only for local MinIO and must not accept real credentials.
  const char *s3_ak = use_test_endpoint
      ? "minioadmin" : oceanbase::unittest::S3_AK;
  const char *s3_sk = use_test_endpoint
      ? "minioadmin" : oceanbase::unittest::S3_SK;
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  if (OB_ISNULL(s3_region) || '\0' == s3_region[0]) {
    s3_region = oceanbase::unittest::S3_REGION;
  }
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  const int format_ret = databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket, cur_time_ns, s3_endpoint, s3_ak, s3_sk, s3_region);
  if (OB_SUCCESS != format_ret) {
    return format_ret;
  }
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  const int test_ret = RUN_ALL_TESTS();
  // Finalize every suite and gtest result listener before the base fixture exits.
  // This also cleans up the cluster directory for filtered runs of either binary.
  fflush(nullptr);
  oceanbase::unittest::ObSimpleClusterTestBase::TearDownTestCase();
  std::_Exit(test_ret);
}

#endif // OCEANBASE_MITTEST_ASYNC_INDEX_COMPACTION_COMMON_H_
