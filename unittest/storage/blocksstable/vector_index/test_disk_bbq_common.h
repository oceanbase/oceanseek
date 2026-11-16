/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <deque>
#include <functional>
#include <future>
#include <list>
#include <limits>
#include <mutex>
#include <new>
#include <numeric>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <queue>
#include <string>
#include <vector>

#define private public
#define protected public

#include "share/ob_cluster_version.h"
#include "common/ob_target_specific.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/cpu/ob_cpu_topology.h"
#include "lib/rc/context.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_type/ob_vector_cosine_distance.h"
#include "share/vector_type/ob_vector_ip_distance.h"
#include "share/vector_type/ob_vector_l2_distance.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_fast_scanner.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_searcher.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "storage/ob_micro_block_index_iterator.h"
#include "storage/ob_i_store.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;
using namespace compaction;

namespace unittest
{

class ObIvfSearcherGuard
{
public:
  ObIvfSearcherGuard() : searcher_(nullptr) {}
  ~ObIvfSearcherGuard()
  {
    if (nullptr != searcher_) {
      ObIvfSSTableSearcher::destroy(searcher_);
      searcher_ = nullptr;
    }
  }
  ObIvfSSTableSearcher *&get() { return searcher_; }
  ObIvfSSTableSearcher *operator->() { return searcher_; }

private:
  ObIvfSSTableSearcher *searcher_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfSearcherGuard);
};

class ObIvfCentroidIteratorGuard
{
public:
  explicit ObIvfCentroidIteratorGuard(ObIAllocator &allocator)
    : allocator_(allocator), iterator_(nullptr)
  {}
  ~ObIvfCentroidIteratorGuard()
  {
    if (nullptr != iterator_) {
      iterator_->~ObIvfCentroidIterator();
      allocator_.free(iterator_);
      iterator_ = nullptr;
    }
  }
  ObIvfCentroidIterator *&get() { return iterator_; }
  ObIvfCentroidIterator *operator->() { return iterator_; }

private:
  ObIAllocator &allocator_;
  ObIvfCentroidIterator *iterator_;
  DISALLOW_COPY_AND_ASSIGN(ObIvfCentroidIteratorGuard);
};

static int init_ivf_access_context(
    const ObIvfSSTable &sstable,
    ObTableScanParam &scan_param,
    ObStoreCtx &store_ctx,
    ObTableAccessContext &access_ctx)
{
  ObVersionRange trans_version_range;
  store_ctx.tablet_id_ = sstable.get_key().get_tablet_id();
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = sstable.get_snapshot_version();
  return access_ctx.init(scan_param.scan_flag_, store_ctx,
      *scan_param.allocator_, trans_version_range);
}

static void prepare_ivf_scan_param(
    const ObTableScanParam &source,
    ObIAllocator &allocator,
    ObTableScanParam &scan_param)
{
  scan_param.allocator_ = &allocator;
  scan_param.table_param_ = source.table_param_;
  scan_param.tablet_id_ = source.tablet_id_;
  scan_param.ls_id_ = source.ls_id_;
  scan_param.scan_flag_ = source.scan_flag_;
  scan_param.pd_storage_flag_ = source.pd_storage_flag_;
}

static int init_ivf_output_row_desc_for_test(
    const ObIvfSSTable &sstable,
    const ObITableReadInfo &read_info,
    ObIvfSSTableRowDesc &output_row_desc)
{
  int ret = OB_SUCCESS;
  ObIvfSSTableRowDesc storage_row_desc;
  if (OB_FAIL(sstable.get_row_desc(storage_row_desc))) {
  } else {
    output_row_desc = storage_row_desc;
    const ObColumnIndexArray &column_indexes = read_info.get_columns_index();
    for (int64_t i = 0; i < column_indexes.count(); ++i) {
      const int32_t storage_index = column_indexes.at(i);
      if (storage_index == storage_row_desc.row_type_col_idx_) {
        output_row_desc.row_type_col_idx_ = static_cast<int16_t>(i);
      } else if (storage_index == storage_row_desc.centroid_id_col_idx_) {
        output_row_desc.centroid_id_col_idx_ = static_cast<int16_t>(i);
      } else if (storage_index == storage_row_desc.vector_id_col_idx_) {
        output_row_desc.vector_id_col_idx_ = static_cast<int16_t>(i);
      } else if (storage_index == storage_row_desc.payload_col_idx_) {
        output_row_desc.payload_col_idx_ = static_cast<int16_t>(i);
      }
    }
  }
  return ret;
}

static int init_disk_bbq_fast_scanner(
    ObDiskBBQFastScanner &scanner,
    const ObTableScanParam &scan_param,
    ObIvfSSTable &sstable,
    ObTableAccessContext &access_ctx,
    ObTableAccessParam &access_param,
    const ObTableReadInfo &ivf_read_info,
    ObIvfSSTableRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_param.table_param_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(access_param.init(scan_param, nullptr, &ivf_read_info))) {
  } else if (OB_FAIL(init_ivf_output_row_desc_for_test(
      sstable, ivf_read_info, row_desc))) {
  } else {
    access_param.iter_param_.read_info_ = &ivf_read_info;
    access_param.iter_param_.rowkey_read_info_ = nullptr;
    access_param.iter_param_.is_same_schema_column_ = false;
    ret = scanner.init(access_param.iter_param_, row_desc, sstable, access_ctx);
  }
  return ret;
}

static int create_centroid_iterator_with_reader(
    ObIvfSSTable &sstable,
    const ObIvfKnnSearchParam &query_param,
    const float *rotated_query,
    ObIAllocator &object_allocator,
    ObIAllocator &query_allocator,
    const ObTableReadInfo &read_info,
    ObIvfCentroidIterator *&iterator)
{
  int ret = OB_SUCCESS;
  iterator = nullptr;
  ObTableScanParam scan_param;
  scan_param.allocator_ = &object_allocator;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_ctx;
  ObTableIterParam iter_param;
  ObIvfSSTableRowDesc row_desc;
  iter_param.table_id_ = sstable.get_key().get_tablet_id().id();
  iter_param.tablet_id_ = sstable.get_key().get_tablet_id();
  iter_param.read_info_ = &read_info;
  iter_param.rowkey_read_info_ = nullptr;
  if (OB_FAIL(init_ivf_access_context(
      sstable, scan_param, store_ctx, access_ctx))) {
  } else if (OB_FAIL(init_ivf_output_row_desc_for_test(
      sstable, read_info, row_desc))) {
  } else {
    ret = sstable.create_centroid_iterator(
        query_param, rotated_query, object_allocator, query_allocator,
        iter_param, row_desc, access_ctx, iterator);
  }
  return ret;
}

static int create_centroid_iterator_with_reader(
    ObIvfSSTable &sstable,
    const ObIvfKnnSearchParam &query_param,
    const float *rotated_query,
    ObIAllocator &query_allocator,
    const ObTableReadInfo &read_info,
    ObIvfCentroidIterator *&iterator)
{
  return create_centroid_iterator_with_reader(sstable, query_param,
      rotated_query, query_allocator, query_allocator, read_info, iterator);
}

static int run_ivf_knn_search_with_deletion_bitmap(
    ObIvfSSTable &sstable,
    ObTableScanParam &scan_param,
    const ObIvfKnnSearchParam &query_param,
    const ObDeletionBitmapHandle *deletion_bitmap,
    ObIvfKnnSearchResult &result)
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_ctx;
  ObTableHandleV2 table_handle;
  ObIvfSSTableSearcher *raw_searcher = nullptr;
  if (OB_FAIL(init_ivf_access_context(
      sstable, scan_param, store_ctx, access_ctx))) {
  } else if (OB_FAIL(table_handle.set_sstable_with_tablet(&sstable))) {
  } else if (OB_FAIL(sstable.create_searcher(
      table_handle, scan_param, access_ctx, raw_searcher))) {
  } else if (OB_ISNULL(raw_searcher)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObIvfSearcherGuard searcher;
    searcher.get() = raw_searcher;
    ObIvfKnnSearchParam search_param = query_param;
    search_param.deletion_bitmap_handle_ = deletion_bitmap;
    ret = searcher->knn_search(search_param, result);
  }
  return ret;
}

static int run_ivf_knn_search(
    ObIvfSSTable &sstable,
    ObTableScanParam &scan_param,
    const ObIvfKnnSearchParam &query_param,
    ObIvfKnnSearchResult &result)
{
  return run_ivf_knn_search_with_deletion_bitmap(sstable, scan_param, query_param, nullptr, result);
}

static const int64_t IVF_SNAPSHOT_VERSION = 100;
static const uint64_t IVF_TABLE_ID = 200001;
static const uint64_t IVF_TABLET_ID = 200001;
static const int64_t IVF_LS_ID = 1;

static void prepare_ivf_schema(ObTableSchema &table_schema)
{
  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_ivf_sstable"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(IVF_TABLE_ID);
  table_schema.set_rowkey_column_num(3);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + 4);
  table_schema.set_block_size(16 * 1024);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(CS_ENCODING_ROW_STORE);
  table_schema.set_pctfree(10);

  struct ColMeta
  {
    const char *name_;
    int64_t rowkey_pos_;
    ObObjType obj_type_;
    int64_t data_length_;
    ObCollationType cs_type_;
  };
  const ColMeta cols[] = {
      {"type", 1, ObIntType, 0, CS_TYPE_BINARY},
      {"cid", 2, ObIntType, 0, CS_TYPE_BINARY},
      {"vid", 3, ObIntType, 0, CS_TYPE_BINARY},
      {"data", 0, ObVarcharType, 1048576, CS_TYPE_BINARY},
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(cols); ++i) {
    ObColumnSchemaV2 column;
    column.set_table_id(IVF_TABLE_ID);
    column.set_column_id(common::OB_APP_MIN_COLUMN_ID + i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(cols[i].name_));
    column.set_data_type(cols[i].obj_type_);
    column.set_collation_type(cols[i].cs_type_);
    column.set_data_length(cols[i].data_length_);
    column.set_rowkey_position(cols[i].rowkey_pos_);
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
}

class TestIVFSSTableWriter : public ::testing::Test
{
protected:
  TestIVFSSTableWriter()
    : table_param_(allocator_),
      sstable_builder_(false /*use_double_write_buffer*/),
      corrupt_extended_msb_(false), posting_layout_size_(0)
  {}

  virtual void SetUp() override
  {
    allocator_.reset();
    scan_param_.allocator_ = &allocator_;
    scan_store_ctx_.tablet_id_ = ObTabletID(IVF_TABLET_ID);
    ObVersionRange trans_version_range;
    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    ASSERT_EQ(OB_SUCCESS, scan_context_.init(scan_param_.scan_flag_,
        scan_store_ctx_, allocator_, trans_version_range));
    table_param_.reset();
    table_schema_.reset();
    whole_desc_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    sstable_.reset();
    corrupt_extended_msb_ = false;
    posting_layout_size_ = 0;
  }

  virtual void TearDown() override
  {
    sstable_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    whole_desc_.reset();
    table_schema_.reset();
    scan_context_.reset();
    scan_store_ctx_.reset();
    table_param_.reset();
    allocator_.reset();
  }

  int prepare_write_env(
      const int64_t micro_block_size = 0,
      const ObRowStoreType row_store_type = CS_ENCODING_ROW_STORE)
  {
    int ret = OB_SUCCESS;
    prepare_ivf_schema(table_schema_);
    table_schema_.set_row_store_type(row_store_type);
    ObSEArray<uint64_t, 4> output_column_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_.get_column_count(); ++i) {
      const ObColumnSchemaV2 *column = table_schema_.get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(output_column_ids.push_back(column->get_column_id()))) {
      }
    }
    if (OB_SUCC(ret)) {
      table_param_.reset();
      if (OB_FAIL(table_param_.convert(
          table_schema_, output_column_ids, scan_param_.pd_storage_flag_))) {
        LOG_WARN("failed to prepare ivf table param", K(ret));
      } else {
        scan_param_.table_param_ = &table_param_;
        scan_param_.tablet_id_ = ObTabletID(IVF_TABLET_ID);
        scan_param_.ls_id_ = share::ObLSID(IVF_LS_ID);
      }
    }
    if (micro_block_size > 0) {
      table_schema_.set_block_size(micro_block_size);
    }
    share::SCN end_scn;
    const share::SCN reorganization_scn(share::SCN::min_scn());
    end_scn.convert_for_tx(IVF_SNAPSHOT_VERSION);
    if (OB_FAIL(whole_desc_.init(
        false /*is_ddl*/,
        table_schema_,
        ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID),
        MAJOR_MERGE,
        IVF_SNAPSHOT_VERSION,
        DATA_CURRENT_VERSION,
        table_schema_.get_micro_index_clustered(),
        0 /*private_transfer_epoch*/,
        0 /*concurrent_cnt*/,
        reorganization_scn,
        end_scn))) {
      LOG_WARN("failed to init whole data store desc", K(ret));
    } else {
      whole_desc_.get_desc().static_desc_->schema_version_ = 10;
      if (micro_block_size > 0) {
        whole_desc_.get_desc().static_desc_->micro_block_size_limit_ = micro_block_size;
      }
      whole_desc_.get_desc().sstable_index_builder_ = &sstable_builder_;
      if (OB_FAIL(sstable_builder_.init(whole_desc_.get_desc()))) {
        LOG_WARN("failed to init sstable index builder", K(ret));
      }
    }
    return ret;
  }

  int open_writer(ObMacroBlockWriter &writer, const int64_t start_seq)
  {
    return open_writer(writer, start_seq, whole_desc_.get_desc());
  }

  int open_writer(
      ObMacroBlockWriter &writer,
      const int64_t start_seq,
      const ObDataStoreDesc &data_store_desc)
  {
    ObMacroSeqParam seq_param;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = start_seq;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    return writer.open(data_store_desc, 0 /*parallel_idx*/, seq_param, pre_warm_param, object_cleaner_);
  }

  int make_ivf_row(
      ObDatumRow &row,
      const int64_t type,
      const int64_t cid,
      const int64_t vid,
      const ObString &data)
  {
    int ret = OB_SUCCESS;
    const int64_t schema_col_cnt = table_schema_.get_column_count();
    const int64_t mv_col_cnt = schema_col_cnt + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (!row.is_valid() || row.get_column_count() < mv_col_cnt) {
      row.reset();
      if (OB_FAIL(row.init(allocator_, mv_col_cnt))) {
        LOG_WARN("failed to init datum row", K(ret), K(mv_col_cnt));
      }
    }
    if (OB_SUCC(ret)) {
      row.storage_datums_[0].set_int(type);
      row.storage_datums_[1].set_int(cid);
      row.storage_datums_[2].set_int(vid);
      row.storage_datums_[3].set_int(-IVF_SNAPSHOT_VERSION);
      row.storage_datums_[4].set_int(0);
      row.storage_datums_[5].set_string(data);
      row.count_ = static_cast<int16_t>(mv_col_cnt);
      row.row_flag_.reset();
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.mvcc_row_flag_.reset();
      row.mvcc_row_flag_.set_last_multi_version_row(true);
    }
    return ret;
  }

  int make_ivf_row(
      ObDatumRow &row,
      const int64_t type,
      const int64_t cid,
      const int64_t vid,
      const char *data)
  {
    return make_ivf_row(row, type, cid, vid, ObString(data));
  }

  int write_type_rows(
      const int64_t type,
      const int64_t start_seq,
      const int64_t row_cnt,
      const int64_t cid_base)
  {
    int ret = OB_SUCCESS;
    ObMacroBlockWriter writer;
    if (OB_FAIL(open_writer(writer, start_seq))) {
      LOG_WARN("failed to open writer", K(ret), K(type), K(start_seq));
    } else {
      ObDatumRow row;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        char data[64];
        snprintf(data, sizeof(data), "t%ld_c%ld_v%ld", type, cid_base, i);
        if (OB_FAIL(make_ivf_row(row, type, cid_base, i, data))) {
          LOG_WARN("failed to make ivf row", K(ret), K(type), K(i));
        } else if (OB_FAIL(writer.append_row(row))) {
          LOG_WARN("failed to append ivf row", K(ret), K(type), K(i));
        }
      }
      if (FAILEDx(writer.close())) {
        LOG_WARN("failed to close writer", K(ret), K(type));
      }
    }
    return ret;
  }

  int write_low_precision_vector_rows(
      const int64_t start_seq,
      const int64_t row_cnt,
      const int64_t cid,
      const int32_t code_len)
  {
    int ret = OB_SUCCESS;
    ObMacroBlockWriter writer;
    if (OB_UNLIKELY(code_len <= 0 || code_len > 64)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(open_writer(writer, start_seq))) {
      LOG_WARN("failed to open base code writer", K(ret), K(start_seq));
    } else {
      ObDatumRow row;
      char code[64];
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        uint64_t state = (static_cast<uint64_t>(i) + 1) * 0x9E3779B97F4A7C15ULL
            ^ (static_cast<uint64_t>(cid) + 1);
        for (int32_t j = 0; j < code_len; ++j) {
          state ^= state >> 12;
          state ^= state << 25;
          state ^= state >> 27;
          code[j] = static_cast<char>(state & 0xFF);
        }
        if (code_len >= 3 * static_cast<int32_t>(sizeof(float))) {
          const float metadata[] = {1.0F, 1.0F, 1.0F};
          MEMCPY(code + code_len - sizeof(metadata), metadata, sizeof(metadata));
        }
        if (OB_FAIL(make_ivf_row(
            row, IVF_ROW_TYPE_LOW_PRECISION_VECTOR, cid, i, ObString(code_len, code)))) {
          LOG_WARN("failed to make base code row", K(ret), K(i));
        } else if (OB_FAIL(writer.append_row(row))) {
          LOG_WARN("failed to append base code row", K(ret), K(i));
        }
      }
      if (FAILEDx(writer.close())) {
        LOG_WARN("failed to close base code writer", K(ret));
      }
    }
    return ret;
  }

  // Physical keys are explicit: (parent ID, self CID), including (0,c) for FLAT.
  int write_centroid_row(ObMacroBlockWriter &writer, const int64_t key_cid,
      const int64_t key_vid, const float *vec, const int32_t dim, const uint32_t child_count)
  {
    int ret = OB_SUCCESS;
    std::vector<char> bytes(ObIvfCentroidPayload::get_size(dim));
    ObDatumRow row;
    if (OB_FAIL(ObIvfCentroidPayload::encode(bytes.data(), bytes.size(), vec, dim, child_count))) {
    } else if (OB_FAIL(make_ivf_row(row, IVF_ROW_TYPE_CENTROID, key_cid, key_vid,
        ObString(static_cast<int32_t>(bytes.size()), bytes.data())))) {
    } else { ret = writer.append_row(row); }
    return ret;
  }

  int write_float_vec_row(
      ObMacroBlockWriter &writer,
      const int64_t type,
      const int64_t cid,
      const int64_t vid,
      const float *vec,
      const int32_t dim)
  {
    if (type == IVF_ROW_TYPE_CENTROID) { return write_centroid_row(writer, cid, vid, vec, dim, 0); }
    int ret = OB_SUCCESS;
    ObDatumRow row;
    if (OB_FAIL(make_ivf_row(row, type, cid, vid,
        ObString(static_cast<int32_t>(dim * sizeof(float)), reinterpret_cast<const char *>(vec))))) {
      LOG_WARN("failed to make float vec row", K(ret), K(type), K(cid), K(vid));
    } else if (OB_FAIL(writer.append_row(row))) {
      LOG_WARN("failed to append float vec row", K(ret), K(type), K(cid), K(vid));
    }
    return ret;
  }

  int write_zero_centroids(
      const int64_t start_seq,
      const int64_t *cids,
      const int64_t cid_cnt,
      const int32_t dim)
  {
    int ret = OB_SUCCESS;
    ObMacroBlockWriter writer;
    float *zeros = nullptr;
    const int64_t bytes = static_cast<int64_t>(dim) * static_cast<int64_t>(sizeof(float));
    if (OB_UNLIKELY(dim <= 0 || cid_cnt <= 0 || nullptr == cids)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(zeros = static_cast<float *>(allocator_.alloc(bytes)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(open_writer(writer, start_seq))) {
      LOG_WARN("failed to open zero centroid writer", K(ret), K(start_seq));
    } else {
      MEMSET(zeros, 0, bytes);
      for (int64_t i = 0; OB_SUCC(ret) && i < cid_cnt; ++i) {
        if (OB_FAIL(write_float_vec_row(
            writer, IVF_ROW_TYPE_CENTROID, 0, cids[i], zeros, dim))) {
          LOG_WARN("failed to write zero centroid", K(ret), K(cids[i]));
        }
      }
      if (FAILEDx(writer.close())) {
        LOG_WARN("failed to close zero centroid writer", K(ret));
      }
    }
    return ret;
  }

  // Build centroid, one-bit RaBitQ posting and optional rotated raw-vector rows.
  int write_disk_bbq_index(
      const float *vectors,
      const int64_t *cids,
      const int64_t *vids,
      const int64_t vector_cnt,
      const float *centroids,
      const int32_t nlist,
      const int32_t dim,
      const char *flip,
      const int32_t nbits = 1,
      const float *extra_xipnorm_override = nullptr,
      const share::ObVectorIndexDistAlgorithm dist_type = share::VIDA_L2,
      const bool write_raw_vectors = true,
      const float *raw_vectors_override = nullptr,
      const int64_t *low_precision_row_order = nullptr,
      const int64_t low_precision_micro_block_size = 0,
      const bool allow_shared_raw_vectors = false,
      const int32_t parent_count = 0)
  {
    int ret = OB_SUCCESS;
    UNUSED(extra_xipnorm_override);
    std::vector<float> rotated_centroids;
    std::vector<float> rotated;
    std::vector<std::vector<char> > low_precision_payloads;
    std::vector<std::vector<char> > high_precision_payloads;
    std::vector<int64_t> raw_row_order;
    std::vector<int64_t> high_precision_row_order;
    quantization::ObQuantizationEncoder one_bit_encoder;
    quantization::ObQuantizationEncoder extended_encoder;
    quantization::ObQuantizationParams one_bit_params;
    quantization::ObQuantizationParams extended_params;
    quantization::ObFhtKacRotator rotator;
    UNUSED(flip);
    if (OB_ISNULL(vectors) || OB_ISNULL(cids) || OB_ISNULL(vids)
        || OB_ISNULL(centroids) || vector_cnt < 0
        || nlist <= 0 || dim <= 0 || nbits < 1 || nbits > 9
        || parent_count < 0 || parent_count > nlist || (parent_count > 0 && nbits != 1)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(quantization::ObFhtKacRotator::generate(
        allocator_, dim, rotator))) {
      LOG_WARN("failed to generate RaBitQ rotator", K(ret), K(dim));
    } else {
      rotated_centroids.resize(static_cast<int64_t>(nlist) * dim);
      rotated.resize(dim);
      low_precision_payloads.resize(vector_cnt);
      one_bit_params.dim_ = dim;
      one_bit_params.bits_ = 1;
      one_bit_params.rotator_ = rotator;
      extended_params = one_bit_params;
      extended_params.bits_ = static_cast<int8_t>(nbits);
      if (1 == nbits && write_raw_vectors) {
        raw_row_order.resize(vector_cnt);
        std::iota(raw_row_order.begin(), raw_row_order.end(), 0);
        std::sort(raw_row_order.begin(), raw_row_order.end(),
            [vids](const int64_t lhs, const int64_t rhs) {
              return vids[lhs] < vids[rhs];
            });
        for (int64_t i = 1; OB_SUCC(ret) && i < vector_cnt; ++i) {
          if (vids[raw_row_order[i - 1]] == vids[raw_row_order[i]]) {
            const float *raw = nullptr == raw_vectors_override ? vectors : raw_vectors_override;
            if (!allow_shared_raw_vectors || 0 != MEMCMP(
                raw + raw_row_order[i - 1] * dim,
                raw + raw_row_order[i] * dim, dim * sizeof(float))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("duplicate or inconsistent DiskBBQ raw vector vid", K(ret),
                  "vid", vids[raw_row_order[i]]);
            }
          }
        }
        if (OB_SUCC(ret) && allow_shared_raw_vectors) {
          raw_row_order.erase(std::unique(raw_row_order.begin(), raw_row_order.end(),
              [vids](const int64_t lhs, const int64_t rhs) {
                return vids[lhs] == vids[rhs];
              }), raw_row_order.end());
        }
      } else if (nbits >= 2) {
        high_precision_payloads.resize(vector_cnt);
        high_precision_row_order.resize(vector_cnt);
        std::iota(high_precision_row_order.begin(), high_precision_row_order.end(), 0);
        std::sort(high_precision_row_order.begin(), high_precision_row_order.end(),
            [cids, vids](const int64_t lhs, const int64_t rhs) {
              return cids[lhs] != cids[rhs]
                  ? cids[lhs] < cids[rhs] : vids[lhs] < vids[rhs];
            });
        for (int64_t i = 1; OB_SUCC(ret) && i < vector_cnt; ++i) {
          const int64_t previous = high_precision_row_order[i - 1];
          const int64_t current = high_precision_row_order[i];
          if (cids[previous] == cids[current] && vids[previous] == vids[current]) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicate DiskBBQ high precision vector key", K(ret),
                "cid", cids[current], "vid", vids[current]);
          }
        }
      }
    }
    for (int64_t cid = 0; OB_SUCC(ret) && cid < nlist; ++cid) {
      float *rotated_centroid = rotated_centroids.data() + cid * dim;
      MEMCPY(rotated_centroid, centroids + cid * dim, dim * sizeof(float));
      if (OB_FAIL(one_bit_params.rotator_.rotate(dim, rotated_centroid))) {
        LOG_WARN("failed to rotate DiskBBQ centroid", K(ret), K(cid));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(one_bit_encoder.init(allocator_,
          quantization::ObQuantizationType::RABITQ,
          dist_type, one_bit_params))) {
        LOG_WARN("failed to init official RaBitQ encoder", K(ret));
      } else if (nbits >= 2 && OB_FAIL(extended_encoder.init(allocator_,
          quantization::ObQuantizationType::EXTEND_RABITQ,
          dist_type, extended_params))) {
        LOG_WARN("failed to init Extended RaBitQ encoder", K(ret), K(nbits));
      }
    }

    const int64_t sign_code_size = (static_cast<int64_t>(dim) + 7) / 8;
    for (int64_t row = 0; OB_SUCC(ret) && row < vector_cnt; ++row) {
      if (cids[row] < 0 || cids[row] >= nlist || vids[row] < 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        MEMCPY(rotated.data(), vectors + row * dim, dim * sizeof(float));
        low_precision_payloads[row].resize(one_bit_encoder.code_size());
        // Match capture: cosine postings and RAW_VECTOR use normalized vectors.
        if (share::VIDA_COS == dist_type
            && OB_FAIL(share::ObVectorNormalize::L2_normalize_vector(
                dim, rotated.data(), rotated.data()))) {
          LOG_WARN("failed to normalize DiskBBQ test vector", K(ret), K(row));
        } else if (OB_FAIL(one_bit_params.rotator_.rotate(dim, rotated.data()))) {
          LOG_WARN("failed to rotate DiskBBQ vector", K(ret), K(row));
        } else if (OB_FAIL(one_bit_encoder.encode(rotated.data(),
            rotated_centroids.data() + cids[row] * dim,
            {reinterpret_cast<uint8_t *>(low_precision_payloads[row].data()),
                static_cast<int64_t>(low_precision_payloads[row].size())}))) {
          LOG_WARN("failed to encode official one-bit RaBitQ row", K(ret),
              K(row), "cid", cids[row], "vid", vids[row]);
        } else if (nbits >= 2) {
          high_precision_payloads[row].resize(extended_encoder.code_size());
          if (OB_FAIL(extended_encoder.encode(rotated.data(),
              rotated_centroids.data() + cids[row] * dim,
              {reinterpret_cast<uint8_t *>(high_precision_payloads[row].data()),
                  static_cast<int64_t>(high_precision_payloads[row].size())}))) {
            LOG_WARN("failed to encode Extended RaBitQ row", K(ret), K(row),
                "cid", cids[row], "vid", vids[row], K(nbits));
          } else {
            if (corrupt_extended_msb_ && 0 == row && sign_code_size > 0) {
              high_precision_payloads[row][0] ^= static_cast<char>(0x80);
            }
            if (0 != MEMCMP(low_precision_payloads[row].data(),
                high_precision_payloads[row].data(), sign_code_size)) {
              ret = OB_INVALID_DATA;
              LOG_WARN("inconsistent DiskBBQ low/high MSB code", K(ret),
                  K(row), "cid", cids[row], "vid", vids[row], K(nbits));
            } else {
              MEMCPY(low_precision_payloads[row].data(),
                  high_precision_payloads[row].data(), sign_code_size);
            }
          }
        }
      }
    }

    ObMacroBlockWriter centroid_writer;
    if (OB_SUCC(ret) && OB_FAIL(open_writer(centroid_writer, 0))) {
      LOG_WARN("failed to open DiskBBQ centroid writer", K(ret));
    }
    std::vector<uint32_t> child_counts(nlist, 0);
    for (int64_t row = 0; OB_SUCC(ret) && row < vector_cnt; ++row) {
      ret = ObIvfCentroidPayload::add_child_count(child_counts[cids[row]], 1);
    }
    // Optional deterministic hierarchy for end-to-end centroid/posting tests.
    for (int64_t parent = 0; OB_SUCC(ret) && parent < parent_count; ++parent) {
      const int64_t first = parent * nlist / parent_count;
      const int64_t end = (parent + 1) * nlist / parent_count;
      std::fill(rotated.begin(), rotated.end(), 0.0F);
      for (int64_t cid = first; cid < end; ++cid) {
        for (int64_t d = 0; d < dim; ++d) {
          rotated[d] += rotated_centroids[cid * dim + d] / (end - first);
        }
      }
      ret = write_centroid_row(centroid_writer, 0, parent + 1, rotated.data(), dim, end - first);
    }
    for (int64_t cid = 0; OB_SUCC(ret) && cid < nlist; ++cid) {
      const int64_t parent = parent_count == 0 ? 0 : ((cid + 1) * parent_count - 1) / nlist + 1;
      if (OB_FAIL(write_centroid_row(centroid_writer, parent, parent_count + cid + 1,
          rotated_centroids.data() + cid * dim, dim, child_counts[cid]))) {
        LOG_WARN("failed to write rotated centroid", K(ret), K(cid));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(centroid_writer.close())) {
      LOG_WARN("failed to close DiskBBQ centroid writer", K(ret));
    }

    ObWholeDataStoreDesc low_precision_desc;
    const ObDataStoreDesc *low_precision_data_desc = &whole_desc_.get_desc();
    if (OB_SUCC(ret) && low_precision_micro_block_size > 0) {
      if (OB_UNLIKELY(low_precision_micro_block_size < ObDataStoreDesc::MIN_MICRO_BLOCK_SIZE
          || low_precision_micro_block_size > ObIMicroBlockWriter::DEFAULT_MICRO_MAX_SIZE)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DiskBBQ low precision micro block size",
            K(ret), K(low_precision_micro_block_size));
      } else if (OB_FAIL(low_precision_desc.assign(whole_desc_))) {
        LOG_WARN("failed to copy DiskBBQ low precision data store descriptor", K(ret));
      } else {
        low_precision_desc.get_static_desc().micro_block_size_limit_ =
            low_precision_micro_block_size;
        low_precision_desc.get_desc().micro_block_size_ =
            low_precision_micro_block_size;
        low_precision_data_desc = &low_precision_desc.get_desc();
      }
    }

    ObMacroBlockWriter generic_writer;
    ObIvfMacroBlockWriter cid_writer(posting_layout_size_ > 0 ? posting_layout_size_ : 128L * 1024);
    ObMacroBlockWriter &base_writer = posting_layout_size_ > 0
        ? static_cast<ObMacroBlockWriter &>(cid_writer) : generic_writer;
    if (OB_SUCC(ret) && posting_layout_size_ > 0) {
      ObIvfSSTableRowDesc rows;
      ObMacroSeqParam seq; seq.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC; seq.start_ = 100;
      ObPreWarmerParam warm(PRE_WARM_TYPE_NONE);
      if (OB_FAIL(rows.init(IVF_SSTABLE_FORMAT_VERSION))) {
      } else { ret = cid_writer.open(rows, *low_precision_data_desc, 0, seq, warm, object_cleaner_); }
    } else if (OB_SUCC(ret)) { ret = open_writer(base_writer, 100, *low_precision_data_desc); }
    if (OB_FAIL(ret)) { LOG_WARN("failed to open DiskBBQ low precision vector writer", K(ret)); }
    for (int64_t row = 0; OB_SUCC(ret) && row < vector_cnt; ++row) {
      const int64_t source_row = nullptr == low_precision_row_order
          ? row : low_precision_row_order[row];
      const int64_t previous_source_row = row > 0
          ? (nullptr == low_precision_row_order ? row - 1 : low_precision_row_order[row - 1])
          : source_row;
      if (source_row < 0 || source_row >= vector_cnt
          || previous_source_row < 0 || previous_source_row >= vector_cnt
          || cids[source_row] < 0 || cids[source_row] >= nlist
          || (row > 0 && cids[source_row] < cids[previous_source_row])) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ObDatumRow datum_row;
        if (OB_FAIL(make_ivf_row(datum_row,
            IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
            parent_count + cids[source_row] + 1, vids[source_row],
            ObString(static_cast<int32_t>(low_precision_payloads[source_row].size()),
                low_precision_payloads[source_row].data())))
            || OB_FAIL(base_writer.append_row(datum_row))) {
          LOG_WARN("failed to write one-bit RaBitQ low precision vector",
              K(ret), K(row), K(source_row), "cid", cids[source_row],
              "vid", vids[source_row]);
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(base_writer.close())) {
      LOG_WARN("failed to close DiskBBQ low precision vector writer", K(ret));
    }

    if (OB_SUCC(ret) && 1 == nbits && write_raw_vectors) {
      ObMacroBlockWriter raw_writer;
      if (OB_FAIL(open_writer(raw_writer, 300))) {
        LOG_WARN("failed to open DiskBBQ raw-vector writer", K(ret));
      }
      for (int64_t order_idx = 0;
          OB_SUCC(ret) && order_idx < static_cast<int64_t>(raw_row_order.size()); ++order_idx) {
        const int64_t row = raw_row_order[order_idx];
        const float *raw_vectors = nullptr == raw_vectors_override
            ? vectors : raw_vectors_override;
        MEMCPY(rotated.data(), raw_vectors + row * dim, dim * sizeof(float));
        // Match capture: cosine postings and RAW_VECTOR use normalized vectors.
        if (share::VIDA_COS == dist_type
            && OB_FAIL(share::ObVectorNormalize::L2_normalize_vector(
                dim, rotated.data(), rotated.data()))) {
          LOG_WARN("failed to normalize DiskBBQ test vector", K(ret), K(row));
        } else if (OB_FAIL(one_bit_params.rotator_.rotate(dim, rotated.data()))) {
          LOG_WARN("failed to rotate DiskBBQ raw vector", K(ret), K(row));
        } else {
          ObDatumRow datum_row;
          if (OB_FAIL(make_ivf_row(datum_row, IVF_ROW_TYPE_RAW_VECTOR,
            IVF_RAW_VECTOR_CID, vids[row],
            ObString(dim * sizeof(float),
                reinterpret_cast<const char *>(rotated.data()))))
            || OB_FAIL(raw_writer.append_row(datum_row))) {
            LOG_WARN("failed to write DiskBBQ raw vector",
                K(ret), K(row), K(vids[row]));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(raw_writer.close())) {
        LOG_WARN("failed to close DiskBBQ raw-vector writer", K(ret));
      }
    }
    if (OB_SUCC(ret) && nbits >= 2) {
      ObMacroBlockWriter high_precision_writer;
      if (OB_FAIL(open_writer(high_precision_writer, 300))) {
        LOG_WARN("failed to open DiskBBQ high-precision-vector writer", K(ret));
      }
      for (int64_t order_idx = 0;
          OB_SUCC(ret) && order_idx < vector_cnt; ++order_idx) {
        const int64_t row = high_precision_row_order[order_idx];
        ObDatumRow datum_row;
        if (OB_FAIL(make_ivf_row(datum_row,
            IVF_ROW_TYPE_HIGH_PRECISION_VECTOR, cids[row], vids[row],
            ObString(static_cast<int32_t>(high_precision_payloads[row].size()),
                high_precision_payloads[row].data())))
            || OB_FAIL(high_precision_writer.append_row(datum_row))) {
          LOG_WARN("failed to write DiskBBQ high precision vector", K(ret),
              K(row), K(cids[row]), K(vids[row]), K(nbits));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(high_precision_writer.close())) {
        LOG_WARN("failed to close DiskBBQ high-precision-vector writer", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(close_builder_and_init_ivf_sstable(
        dim, flip, nbits, dist_type))) {
      LOG_WARN("failed to finish DiskBBQ SSTable", K(ret));
    } else if (OB_SUCC(ret) && parent_count > 0) {
      sstable_.ivf_meta_.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
      sstable_.ivf_meta_.parent_centroid_count_ = parent_count;
      sstable_.ivf_meta_.centroid_count_ = nlist;
    }
    return ret;
  }

  int close_builder_and_init_ivf_sstable(
      const int32_t dim = 8,
      const char *fht_flip = nullptr,
      const int32_t nbits = 1,
      const share::ObVectorIndexDistAlgorithm dist_type = share::VIDA_L2)
  {
    int ret = OB_SUCCESS;
    ObSSTableMergeRes res;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    UNUSED(fht_flip);
    if (OB_UNLIKELY(dim <= 0 || nbits < 1 || nbits > 9)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(sstable_builder_.close(res))) {
      LOG_WARN("failed to close sstable index builder", K(ret));
    } else if (OB_FAIL(fill_create_param(res, param))) {
      LOG_WARN("failed to fill create param", K(ret));
    } else {
      ivf_meta.index_type_ = share::VIAT_DISK_BBQ;
      ivf_meta.dist_type_ = dist_type;
      ivf_meta.dim_ = dim;
      ivf_meta.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
      ivf_meta.nbits_ = nbits;
      ivf_meta.fht_algo_version_ =
          quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
      param.ivf_meta_ = ivf_meta;
    }
    if (OB_SUCC(ret) && OB_FAIL(sstable_.init(param, &allocator_))) {
      LOG_WARN("failed to init ivf sstable", K(ret), K(param));
    }
    return ret;
  }

  int fill_create_param(const ObSSTableMergeRes &res, ObTabletCreateSSTableParam &param)
  {
    int ret = OB_SUCCESS;
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::TableType::IVF_INDEX_SSTABLE;
    table_key.tablet_id_ = IVF_TABLET_ID;
    table_key.version_range_.base_version_ = 0;
    table_key.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    param.table_key_ = table_key;
    param.schema_version_ = 10;
    param.create_snapshot_version_ = 0;
    param.progressive_merge_round_ = 0;
    param.progressive_merge_step_ = 0;
    param.table_mode_ = table_schema_.get_table_mode_struct();
    param.index_type_ = table_schema_.get_index_type();
    param.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
        + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.column_cnt_ = table_schema_.get_column_count()
        + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (OB_FAIL(ObSSTableMergeRes::fill_addr_and_data(
        res.root_desc_, param.root_block_addr_, param.root_block_data_))) {
      LOG_WARN("failed to fill root addr", K(ret));
    } else if (OB_FAIL(ObSSTableMergeRes::fill_addr_and_data(
        res.data_root_desc_, param.data_block_macro_meta_addr_, param.data_block_macro_meta_))) {
      LOG_WARN("failed to fill data root addr", K(ret));
    } else if (OB_FAIL(param.data_block_ids_.assign(res.data_block_ids_))) {
      LOG_WARN("failed to assign data block ids", K(ret));
    } else if (OB_FAIL(param.other_block_ids_.assign(res.other_block_ids_))) {
      LOG_WARN("failed to assign other block ids", K(ret));
    } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(
        param.column_cnt_, param.column_checksums_))) {
      LOG_WARN("failed to fill column checksum", K(ret));
    } else {
      param.is_meta_root_ = res.data_root_desc_.is_meta_root_;
      param.max_merged_trans_version_ = res.max_merged_trans_version_;
      param.min_merged_trans_version_ = res.min_merged_trans_version_;
      param.row_count_ = res.row_count_;
      param.root_row_store_type_ = res.root_row_store_type_;
      param.latest_row_store_type_ = table_schema_.get_row_store_type();
      param.data_index_tree_height_ = res.root_desc_.height_;
      param.index_blocks_cnt_ = res.index_blocks_cnt_;
      param.data_blocks_cnt_ = res.data_blocks_cnt_;
      param.micro_block_cnt_ = res.micro_block_cnt_;
      param.use_old_macro_block_count_ = res.use_old_macro_block_count_;
      param.data_checksum_ = res.data_checksum_;
      param.occupy_size_ = res.occupy_size_;
      param.original_size_ = res.original_size_;
      param.nested_offset_ = res.nested_offset_;
      param.nested_size_ = res.nested_size_;
      param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
      param.ddl_scn_.set_min();
      param.contain_uncommitted_row_ = false;
      param.encrypt_id_ = res.encrypt_id_;
      param.master_key_id_ = res.master_key_id_;
      param.table_backup_flag_.reset();
      param.table_shared_flag_.reset();
      param.filled_tx_scn_ = SCN::min_scn();
      param.tx_data_recycle_scn_.set_min();
      param.sstable_logic_seq_ = 0;
      param.column_group_cnt_ = 1;
      param.full_column_cnt_ = 0;
      param.recycle_version_ = 0;
      param.co_base_snapshot_version_ = 0;
      param.rec_scn_ = table_key_start_scn_();
      MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    }
    return ret;
  }

  share::SCN table_key_start_scn_() const
  {
    return share::SCN::min_scn();
  }

  int prepare_read_info(ObTableReadInfo &read_info)
  {
    int ret = OB_SUCCESS;
    schema_cols_.reset();
    schema_cols_.set_allocator(&allocator_);
    if (OB_FAIL(schema_cols_.init(table_schema_.get_column_count()))) {
      LOG_WARN("failed to init schema cols", K(ret));
    } else if (OB_FAIL(table_schema_.get_column_ids(schema_cols_))) {
      LOG_WARN("failed to get column ids", K(ret));
    } else if (OB_FAIL(read_info.init(
        allocator_,
        table_schema_.get_column_count(),
        table_schema_.get_rowkey_column_num(),
        false /*is_oracle_mode*/,
        schema_cols_,
        nullptr /*storage_cols_index*/))) {
      LOG_WARN("failed to init ivf sstable schema read info", K(ret));
    }
    return ret;
  }

  int make_type_prefix_range(
      const int64_t type,
      ObStorageDatum &start_datum,
      ObStorageDatum &end_datum,
      ObDatumRowkey &start_key,
      ObDatumRowkey &end_key,
      ObDatumRange &range)
  {
    int ret = OB_SUCCESS;
    start_datum.set_int(type);
    end_datum.set_int(type);
    if (OB_FAIL(start_key.assign(&start_datum, 1))) {
      LOG_WARN("failed to assign type prefix start key", K(ret), K(type));
    } else if (OB_FAIL(end_key.assign(&end_datum, 1))) {
      LOG_WARN("failed to assign type prefix end key", K(ret), K(type));
    } else {
      range.set_start_key(start_key);
      range.set_end_key(end_key);
      range.set_left_closed();
      range.set_right_closed();
    }
    return ret;
  }

  int locate_ivf_row_micro_block(
      const ObTableReadInfo &read_info,
      const int64_t type,
      const int64_t cid,
      const int64_t vid,
      MacroBlockId &macro_id,
      uint64_t &block_offset)
  {
    int ret = OB_SUCCESS;
    macro_id.reset();
    block_offset = 0;
    ObStorageDatum datums[3];
    datums[0].set_int(type);
    datums[1].set_int(cid);
    datums[2].set_int(vid);
    ObDatumRowkey start_key;
    ObDatumRowkey end_key;
    ObDatumRange range;
    ObMicroBlockIndexIterator *iter = nullptr;
    if (OB_FAIL(start_key.assign(datums, ARRAYSIZEOF(datums)))) {
    } else if (OB_FAIL(end_key.assign(datums, ARRAYSIZEOF(datums)))) {
    } else {
      range.set_start_key(start_key);
      range.set_end_key(end_key);
      range.set_left_closed();
      range.set_right_closed();
      if (OB_FAIL(sstable_.scan_micro_block(
          range, read_info, allocator_, iter, false))) {
      } else if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ObMicroIndexInfo index_info;
        if (OB_FAIL(iter->get_next(index_info))) {
        } else {
          macro_id = index_info.get_macro_id();
          block_offset = index_info.get_block_offset();
          ObMicroIndexInfo unexpected;
          const int next_ret = iter->get_next(unexpected);
          if (OB_ITER_END != next_ret) {
            ret = OB_SUCCESS == next_ret ? OB_ERR_UNEXPECTED : next_ret;
          }
        }
      }
    }
    if (nullptr != iter) {
      iter->~ObMicroBlockIndexIterator();
      allocator_.free(iter);
      iter = nullptr;
    }
    return ret;
  }

  int init_scan_context(
      const ObTableReadInfo &read_info,
      ObTableIterParam &iter_param,
      ObTableAccessContext &context,
      ObStoreCtx &store_ctx)
  {
    int ret = OB_SUCCESS;
    iter_param.table_id_ = IVF_TABLE_ID;
    iter_param.tablet_id_ = IVF_TABLET_ID;
    iter_param.read_info_ = &read_info;
    context.query_flag_.set_not_use_row_cache();
    context.query_flag_.set_not_use_block_cache();
    context.query_flag_.scan_order_ = ObQueryFlag::Forward;
    context.store_ctx_ = &store_ctx;
    context.ls_id_ = IVF_LS_ID;
    context.tablet_id_ = IVF_TABLET_ID;
    context.allocator_ = &allocator_;
    context.stmt_allocator_ = &allocator_;
    context.trans_version_range_.base_version_ = 0;
    context.trans_version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    if (OB_FAIL(context.micro_block_handle_mgr_.init(
        false, context.tablet_id_, context.table_store_stat_, context.table_scan_stat_, context.query_flag_))) {
      LOG_WARN("failed to init micro block handle mgr", K(ret));
    } else {
      context.is_inited_ = true;
    }
    return ret;
  }

  int scan_range_and_count(
      const ObTableReadInfo &read_info,
      const ObDatumRange &range,
      const int64_t expected_type,
      int64_t &scanned)
  {
    int ret = OB_SUCCESS;
    scanned = 0;
    ObTableIterParam iter_param;
    ObTableAccessContext context;
    ObStoreCtx store_ctx;
    ObStoreRowIterator *row_iter = nullptr;
    if (OB_FAIL(init_scan_context(read_info, iter_param, context, store_ctx))) {
      LOG_WARN("failed to init scan context", K(ret));
    } else if (OB_FAIL(sstable_.scan(iter_param, context, range, row_iter))) {
      LOG_WARN("failed to scan ivf sstable", K(ret), K(range));
    } else if (OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null row iter", K(ret));
    } else if (OB_UNLIKELY(context.query_flag_.is_use_block_cache())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ivf scan should disable data block cache", K(ret));
    } else {
      const ObDatumRow *cur_row = nullptr;
      int tmp_ret = OB_SUCCESS;
      while (OB_SUCC(ret) && OB_SUCC(tmp_ret)) {
        tmp_ret = row_iter->get_next_row(cur_row);
        if (OB_ITER_END == tmp_ret) {
          break;
        } else if (OB_FAIL(tmp_ret)) {
          LOG_WARN("failed to get next ivf row", K(ret));
        } else if (OB_ISNULL(cur_row) || cur_row->count_ < 3) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ivf scan row", K(ret), KPC(cur_row));
        } else if (expected_type != cur_row->storage_datums_[0].get_int()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ivf scan type", K(ret), K(expected_type), KPC(cur_row));
        } else {
          ++scanned;
        }
      }
      row_iter->~ObStoreRowIterator();
    }
    return ret;
  }

  int read_ivf_payload(
      const ObTableReadInfo &read_info,
      const int64_t type,
      const int64_t cid,
      const int64_t vid,
      std::vector<char> &payload)
  {
    int ret = OB_SUCCESS;
    ObStorageDatum start_datum;
    ObStorageDatum end_datum;
    ObDatumRowkey start_key;
    ObDatumRowkey end_key;
    ObDatumRange range;
    ObTableIterParam iter_param;
    ObTableAccessContext context;
    ObStoreCtx store_ctx;
    ObStoreRowIterator *row_iter = nullptr;
    bool found = false;
    payload.clear();
    if (OB_FAIL(make_type_prefix_range(
        type, start_datum, end_datum, start_key, end_key, range))) {
    } else if (OB_FAIL(init_scan_context(read_info, iter_param, context, store_ctx))) {
    } else if (OB_FAIL(sstable_.scan(iter_param, context, range, row_iter))) {
    } else if (OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const ObDatumRow *cur_row = nullptr;
      int tmp_ret = OB_SUCCESS;
      while (OB_SUCC(ret) && !found && OB_SUCC(tmp_ret)) {
        tmp_ret = row_iter->get_next_row(cur_row);
        if (OB_ITER_END == tmp_ret) {
        } else if (OB_FAIL(tmp_ret)) {
        } else if (OB_ISNULL(cur_row) || cur_row->get_column_count() < 4) {
          ret = OB_INVALID_DATA;
        } else if (cur_row->storage_datums_[IVF_ROWKEY_COL_CID].get_int() == cid
            && cur_row->storage_datums_[IVF_ROWKEY_COL_VID].get_int() == vid) {
          const ObString data =
              cur_row->storage_datums_[cur_row->get_column_count() - 1].get_string();
          if (data.length() < 0 || (data.length() > 0 && OB_ISNULL(data.ptr()))) {
            ret = OB_INVALID_DATA;
          } else {
            payload.assign(data.ptr(), data.ptr() + data.length());
            found = true;
          }
        }
      }
      row_iter->~ObStoreRowIterator();
      if (OB_SUCC(ret) && !found) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }

  void verify_disk_bbq_rejects_non_finite_centroid(const float invalid_value)
  {
    const int32_t dim = 64;
    const int64_t cid = 1;
    float centroid[dim] = {0};
    float query[dim] = {0};
    char flip[4 * dim / CHAR_BIT] = {0};
    centroid[8] = invalid_value;
    ASSERT_EQ(OB_SUCCESS, prepare_write_env());
    ObMacroBlockWriter writer;
    ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
    ASSERT_EQ(OB_SUCCESS, write_float_vec_row(
        writer, IVF_ROW_TYPE_CENTROID, 0, cid, centroid, dim));
    ASSERT_EQ(OB_SUCCESS, writer.close());
    ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim, flip, 4));
    ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
    ObTableReadInfo read_info;
    ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
    ObIvfKnnSearchParam query_param;
    fill_search_params(query_param, query, dim, 1);
    ObIvfKnnSearchResult results;
    ASSERT_EQ(OB_INVALID_DATA, run_ivf_knn_search(sstable_, scan_param_, query_param, results));
  }

  void fill_search_params(
      ObIvfKnnSearchParam &query_param,
      const float *query,
      const int64_t dim,
      const int64_t k)
  {
    query_param.reset();
    query_param.query_ = query;
    query_param.dim_ = dim;
    query_param.nprobe_ = 1;
    query_param.k_ = k;
  }

  void run_disk_bbq_sstable_10k_512d_recall_at_10(
      const share::ObVectorIndexDistAlgorithm dist_type);

  ObArenaAllocator allocator_;
  share::schema::ObTableParam table_param_;
  ObTableScanParam scan_param_;
  ObStoreCtx scan_store_ctx_;
  ObTableAccessContext scan_context_;
  ObTableSchema table_schema_;
  ObWholeDataStoreDesc whole_desc_;
  ObSSTableIndexBuilder sstable_builder_;
  ObSSTablePrivateObjectCleaner object_cleaner_;
  ObIvfSSTable sstable_;
  ObFixedArray<ObColDesc, ObIAllocator> schema_cols_;
  bool corrupt_extended_msb_;
  int64_t posting_layout_size_; // Zero preserves the historical generic writer in existing fixtures.
};


} // namespace unittest
} // namespace oceanbase
