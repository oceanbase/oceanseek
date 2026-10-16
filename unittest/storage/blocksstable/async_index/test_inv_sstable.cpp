/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
// unittest/storage/blocksstable/test_inv_sstable.cpp
//
// 测试用例目录:
//  [类型分类]
//   - table_type_contract:              验证 INV_INDEX_SSTABLE 的类型标记 (is_inv/is_major/!is_co/!is_row_store_major)
//   - type_tags_do_not_collide:         验证 TableType、SerializeTableType、MetaType 枚举值不冲突
//   - inc_inv_sstable_type_classification: 验证 INC_INV_INDEX_SSTABLE 的类型标记 (is_inv/is_inc_inv/is_inc_major_type/!is_major)
//
//  [扩展元数据]
//   - extended_meta_reset_sets_empty_sentinel: reset() 设空哨兵 (min=UINT64_MAX, max=0, is_empty_range)
//
//  [初始化]
//   - init_accepts_only_inv_table_type: init 仅接受 INV 类型，拒绝 MAJOR 类型
//   - init_with_inv_meta_in_param:      init 从 param 中提取 inv_meta
//   - init_without_inv_meta_in_param_returns_error: param 无合法 inv_meta 时 init 报 OB_INVALID_ARGUMENT
//
//  [深拷贝]
//   - inv_sstable_deep_copy_keeps_type_and_meta: deep_copy 保留类型、meta_type 和 inv_meta 内容
//
//  [序列化 / 反序列化]
//   - inv_sstable_serialize_deserialize_round_trip: serialize + deserialize 往返保留类型和 inv_meta
//   - full_serialize_round_trip:        serialize_full_table + deserialize 往返
//   - storage_meta_bypass_processor_restores_inv_sstable: bypass_process_storage_meta 恢复 INV_SSTABLE
//   - sstable_array_round_trip_with_normal_and_inv_tables: 混合类型的 SSTableArray 序列化/反序列化
//
//  [接口拦截]
//   - inv_sstable_rejects_row_store_interfaces:   get/multi_get/multi_scan/set_upper_trans_version 返回 NOT_SUPPORTED
//   - ob_sstable_public_helpers_keep_unsupported_inv_interfaces_rejected: 保持拦截 micro/secondary_meta/index/bf/major-inv-backfill/cs_range
//   - inc_inv_sstable_supports_backfill_commit_version: INC_INV 支持 transfer 回填 upper_trans_version + filled_tx_scn
//   - scan_macro_block_allows_only_forward_macro_level_inv_scan: 公共 macro scan 只放开正向、不记录标准微信息的 INV 组合
//   - scan_macro_block_preserves_error_precedence: 未初始化和非法 range 不被能力报错掩盖
//   - normal_empty_sstable_still_accepts_record_micro_info: 普通 SSTable 的 record=true 能力不被全局拦截
//   - inv_query_interfaces_are_explicitly_unsupported_before_reader_implementation: scan/estimate_row_count 返回 NOT_SUPPORTED；scan_posting 对非法 param 返回 INVALID_ARGUMENT（功能性测试见 mittest test_inv_sstable_posting_iter）
//
//  [生命周期与属性]
//   - reset_clears_inv_meta:            reset() 清除 inv_meta 指针并使 sstable 失效
//   - version_getters_return_table_key_values: Major INV 的逻辑区间仍取自 TableKey VersionRange
//   - inc_inv_v2_reads_logical_range_from_basic_meta: Inc INV V2 的 TableKey 是物理 SCN，逻辑区间取自 BasicMeta
//   - get_frozen_schema_version_delegates_to_base: 委托给 ObSSTable 基类
//   - data_macro_block_count_on_empty_inv_sstable: 空 sstable 返回 0
//   - inc_dec_macro_ref_on_empty_inv_sstable: 空 sstable 上 inc/dec 宏块引用计数成功
//   - contain_uncommitted_row_always_false: 恒返回 false
//   - no_data_to_read_on_empty_inv_sstable: 空 sstable 返回 true
//   - posting_scanner_accepts_empty_inv_sstable_with_valid_meta: 空倒排 sstable 携带有效类型时不阻断 EOF
//

#define USING_LOG_PREFIX STORAGE

#include <cstring>
#include <gtest/gtest.h>

#define private public
#define protected public

#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/access/ob_rows_info.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_range_iter.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"
#include "storage/schema_utils.h"
#include "storage/tablet/ob_table_store_util.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;

namespace unittest
{

class TestInvSSTable : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // init_for_empty_major_sstable() reaches GET_MIN_DATA_VERSION(MTL_ID()), which
    // needs a tenant context and a resolvable data version. Provide a minimal mock:
    // a tenant binding satisfies MTL_ID(), and update_data_version() switches
    // GET_MIN_DATA_VERSION onto its unittest path (independent of a running tenant).
    static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
    ObTenantEnv::set_tenant(&tenant_ctx);
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  }

  void make_inv_sstable_ready(ObSSTable &sstable)
  {
    sstable.key_.reset();
    sstable.key_.table_type_ = ObITable::TableType::INV_INDEX_SSTABLE;
    sstable.valid_for_reading_ = true;
  }

  int build_empty_sstable_param(
      const ObITable::TableType table_type,
      ObStorageSchema &storage_schema,
      ObTabletCreateSSTableParam &param)
  {
    int ret = OB_SUCCESS;
    share::schema::ObTableSchema table_schema;
    const ObTabletID tablet_id(1001);
    const int64_t snapshot_version = 100;
    TestSchemaUtils::prepare_data_schema(table_schema);
    if (OB_FAIL(storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("failed to init storage schema", K(ret));
    } else if (OB_FAIL(param.init_for_empty_major_sstable(
        tablet_id, storage_schema, snapshot_version, -1, false, false))) {
      LOG_WARN("failed to init empty major sstable param", K(ret));
    } else {
      param.table_key_.table_type_ = table_type;
      param.table_key_.tablet_id_ = tablet_id;
      if (ObITable::TableType::INC_INV_INDEX_SSTABLE == table_type) {
        param.table_key_.scn_range_.start_scn_.set_base();
      } else {
        param.table_key_.version_range_.base_version_ = 0;
        param.table_key_.version_range_.snapshot_version_ = snapshot_version;
      }
    }
    return ret;
  }

  int build_async_storage_schema(
      const share::schema::ObIndexType index_type,
      const int64_t column_cnt,
      const int64_t rowkey_column_cnt,
      ObStorageSchema &storage_schema)
  {
    int ret = OB_SUCCESS;
    storage_schema.reset();
    storage_schema.allocator_ = &allocator_;
    storage_schema.rowkey_array_.set_allocator(&allocator_);
    storage_schema.rowkey_array_.set_capacity(static_cast<uint32_t>(rowkey_column_cnt));
    storage_schema.index_type_ = index_type;
    storage_schema.column_cnt_ = column_cnt;
    storage_schema.is_inited_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_cnt; ++i) {
      ObStorageRowkeyColumnSchema rowkey_column;
      rowkey_column.column_idx_ = static_cast<uint32_t>(common::OB_APP_MIN_COLUMN_ID + i);
      rowkey_column.meta_type_.set_uint64();
      if (OB_FAIL(storage_schema.rowkey_array_.push_back(rowkey_column))) {
        LOG_WARN("failed to push rowkey column", K(ret), K(i), K(rowkey_column_cnt));
      }
    }
    return ret;
  }

  void fill_inv_meta(
      const uint64_t min_doc_id,
      const uint64_t max_doc_id,
      ObInvIdxStatsMeta &meta)
  {
    meta.reset();
    meta.min_doc_id_ = min_doc_id;
    meta.max_doc_id_ = max_doc_id;
    meta.doc_id_obj_meta_.set_uint64();
    meta.index_type_ = INV_IDX_TYPE_FTS;
    meta.has_relevance_params_ = true;
  }

  int build_inv_sstable(ObInvSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObInvIdxStatsMeta inv_meta;
    fill_inv_meta(7, 100, inv_meta);
    if (OB_FAIL(build_empty_sstable_param(ObITable::TableType::INV_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build sstable param", K(ret));
    } else {
      param.inv_meta_ = inv_meta;
      if (OB_FAIL(sstable.init(param, &allocator_))) {
        LOG_WARN("failed to init inv sstable", K(ret));
      }
    }
    return ret;
  }

  int build_normal_sstable(ObSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::MAJOR_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build normal sstable param", K(ret));
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init normal sstable", K(ret));
    }
    return ret;
  }

  void destroy_macro_iter(ObIMacroBlockIterator *&iter)
  {
    if (nullptr != iter) {
      iter->~ObIMacroBlockIterator();
      allocator_.free(iter);
      iter = nullptr;
    }
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestInvSSTable, range_iter_accepts_only_term_or_sentinel_padded_keys)
{
  static const int64_t TERM_COLUMN_CNT = 3;
  static const int64_t SCHEMA_ROWKEY_CNT = 4;
  ObStorageDatum term_start[TERM_COLUMN_CNT];
  ObStorageDatum term_end[TERM_COLUMN_CNT];
  ObStorageDatum full_start[SCHEMA_ROWKEY_CNT];
  ObStorageDatum full_end[SCHEMA_ROWKEY_CNT];
  ObStorageDatum short_start[TERM_COLUMN_CNT - 1];
  ObStorageDatum long_end[SCHEMA_ROWKEY_CNT + 1];
  ObDatumRange range;
  for (int64_t i = 0; i < TERM_COLUMN_CNT; ++i) {
    term_start[i].set_uint(i + 1);
    term_end[i].set_uint(i + 11);
    full_start[i].set_uint(i + 1);
    full_end[i].set_uint(i + 11);
    if (i < TERM_COLUMN_CNT - 1) {
      short_start[i].set_uint(i + 1);
    }
    long_end[i].set_uint(i + 11);
  }
  long_end[TERM_COLUMN_CNT].set_max();
  long_end[SCHEMA_ROWKEY_CNT].set_max();

  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      term_start, TERM_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      term_end, TERM_COLUMN_CNT));
  EXPECT_EQ(OB_SUCCESS, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  full_start[TERM_COLUMN_CNT].set_min();
  full_end[TERM_COLUMN_CNT].set_max();
  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      full_start, SCHEMA_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      full_end, SCHEMA_ROWKEY_CNT));
  EXPECT_EQ(OB_SUCCESS, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  range.get_start_key().set_min_rowkey();
  EXPECT_EQ(OB_SUCCESS, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      full_start, SCHEMA_ROWKEY_CNT));
  range.get_end_key().set_max_rowkey();
  EXPECT_EQ(OB_SUCCESS, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));
  range.set_whole_range();
  EXPECT_EQ(OB_SUCCESS, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  full_start[TERM_COLUMN_CNT].set_uint(100);
  full_end[TERM_COLUMN_CNT].set_uint(300);
  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      full_start, SCHEMA_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      full_end, SCHEMA_ROWKEY_CNT));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  full_start[TERM_COLUMN_CNT].set_max();
  full_end[TERM_COLUMN_CNT].set_max();
  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      full_start, SCHEMA_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      full_end, SCHEMA_ROWKEY_CNT));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  full_start[TERM_COLUMN_CNT].set_min();
  full_end[TERM_COLUMN_CNT].set_min();
  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      full_start, SCHEMA_ROWKEY_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      full_end, SCHEMA_ROWKEY_CNT));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      short_start, TERM_COLUMN_CNT - 1));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      term_end, TERM_COLUMN_CNT));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));

  ASSERT_EQ(OB_SUCCESS, range.get_start_key().assign(
      term_start, TERM_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, range.get_end_key().assign(
      long_end, SCHEMA_ROWKEY_CNT + 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObInvSSTableRangeIter::validate_scan_range(
      range, TERM_COLUMN_CNT, SCHEMA_ROWKEY_CNT));
}

TEST_F(TestInvSSTable, table_type_contract)
{
  ObITable::TableKey table_key;
  table_key.reset();
  table_key.table_type_ = ObITable::TableType::INV_INDEX_SSTABLE;

  ASSERT_TRUE(table_key.is_inv_sstable());
  ASSERT_TRUE(table_key.is_major_sstable());
  ASSERT_FALSE(table_key.is_co_sstable());
  ASSERT_FALSE(table_key.is_row_store_major_sstable());

  ObSSTable sstable;
  make_inv_sstable_ready(sstable);
  ASSERT_TRUE(sstable.is_inv_sstable());
  ASSERT_TRUE(sstable.is_major_sstable());
  ASSERT_FALSE(sstable.is_co_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::INV_SSTABLE, sstable.get_meta_type());
}

TEST_F(TestInvSSTable, type_tags_do_not_collide)
{
  ASSERT_EQ(41, static_cast<int64_t>(ObITable::TableType::INV_INDEX_SSTABLE));
  ASSERT_EQ(42, static_cast<int64_t>(ObITable::TableType::INC_INV_INDEX_SSTABLE));
  ASSERT_EQ(0, static_cast<int64_t>(storage::ObSerializeTableType::NORMAL));
  ASSERT_EQ(1, static_cast<int64_t>(storage::ObSerializeTableType::CO));
  ASSERT_EQ(2, static_cast<int64_t>(storage::ObSerializeTableType::INV));
  ASSERT_EQ(3, static_cast<int64_t>(ObStorageMetaValue::MetaType::INV_SSTABLE));
}

TEST_F(TestInvSSTable, inc_inv_sstable_type_classification)
{
  ObITable::TableKey table_key;
  table_key.reset();
  table_key.table_type_ = ObITable::TableType::INC_INV_INDEX_SSTABLE;

  ASSERT_TRUE(table_key.is_inv_sstable());
  ASSERT_TRUE(table_key.is_inc_inv_sstable());
  ASSERT_TRUE(table_key.is_inc_major_type_sstable());
  ASSERT_FALSE(table_key.is_major_sstable());
  ASSERT_FALSE(table_key.is_co_sstable());
  ASSERT_TRUE(table_key.is_inc_major_sstable());
}

TEST_F(TestInvSSTable, extended_meta_reset_sets_empty_sentinel)
{
  ObInvIdxStatsMeta meta;

  ASSERT_EQ(UINT64_MAX, meta.min_doc_id_);
  ASSERT_EQ(0, meta.max_doc_id_);
  ASSERT_TRUE(meta.is_empty_range());
  ASSERT_EQ(ObNullType, meta.doc_id_obj_meta_.get_type());

  meta.min_doc_id_ = 7;
  meta.max_doc_id_ = 100;
  meta.doc_id_obj_meta_.set_uint64();
  meta.index_type_ = INV_IDX_TYPE_FTS;
  meta.has_relevance_params_ = true;
  ASSERT_EQ(7, meta.min_doc_id_);
  ASSERT_EQ(100, meta.max_doc_id_);
  ASSERT_FALSE(meta.is_empty_range());
  ASSERT_EQ(ObUInt64Type, meta.doc_id_obj_meta_.get_type());
  ASSERT_TRUE(meta.is_valid());

  meta.reset();
  ASSERT_EQ(UINT64_MAX, meta.min_doc_id_);
  ASSERT_EQ(0, meta.max_doc_id_);
  ASSERT_TRUE(meta.is_empty_range());
  ASSERT_EQ(ObNullType, meta.doc_id_obj_meta_.get_type());
}

TEST_F(TestInvSSTable, init_empty_inv_meta_derives_all_static_formats)
{
  ObStorageSchema storage_schema;
  ObInvIdxStatsMeta inv_meta;

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, 4, 2, storage_schema));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_EQ(INV_IDX_TYPE_FTS, inv_meta.index_type_);
  ASSERT_TRUE(inv_meta.has_relevance_params_);
  ASSERT_FALSE(inv_meta.has_position_list_);
  ASSERT_FALSE(inv_meta.has_nested_key_);
  ASSERT_EQ(ObUInt64Type, inv_meta.doc_id_obj_meta_.get_type());
  ASSERT_EQ(UINT64_MAX, inv_meta.min_doc_id_);
  ASSERT_EQ(0, inv_meta.max_doc_id_);
  ASSERT_EQ(0, inv_meta.total_doc_length_);

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, 5, 2, storage_schema));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_TRUE(inv_meta.has_position_list_);

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, 4, 4, storage_schema));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_EQ(INV_IDX_TYPE_SEARCH, inv_meta.index_type_);
  ASSERT_FALSE(inv_meta.has_relevance_params_);
  ASSERT_FALSE(inv_meta.has_position_list_);
  ASSERT_FALSE(inv_meta.has_nested_key_);

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, 5, 4, storage_schema));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));
  ASSERT_TRUE(inv_meta.is_valid());
  ASSERT_TRUE(inv_meta.has_nested_key_);
}

TEST_F(TestInvSSTable, init_empty_inv_meta_rejects_invalid_layout)
{
  ObStorageSchema storage_schema;
  ObInvIdxStatsMeta inv_meta;

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL, 4, 2, storage_schema));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, 6, 2, storage_schema));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));

  ASSERT_EQ(OB_SUCCESS, build_async_storage_schema(
      share::schema::INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL, 6, 4, storage_schema));
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObTabletCreateSSTableParam::init_empty_inv_meta(
      storage_schema, inv_meta));
}

TEST_F(TestInvSSTable, init_accepts_only_inv_table_type)
{
  ObStorageSchema inv_storage_schema;
  ObStorageSchema normal_storage_schema;
  ObTabletCreateSSTableParam inv_param;
  ObTabletCreateSSTableParam normal_param;
  ObInvSSTable inv_sstable;
  ObInvSSTable normal_typed_sstable;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INV_INDEX_SSTABLE, inv_storage_schema, inv_param));
  {
    ObInvIdxStatsMeta inv_meta;
    fill_inv_meta(1, 10, inv_meta);
    inv_param.inv_meta_ = inv_meta;
  }
  ASSERT_EQ(OB_SUCCESS, inv_sstable.init(inv_param, &allocator_));
  ASSERT_TRUE(inv_sstable.is_inv_sstable());
  ASSERT_TRUE(inv_sstable.is_loaded());
  ASSERT_TRUE(inv_sstable.get_inv_meta().is_valid());

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::MAJOR_SSTABLE, normal_storage_schema, normal_param));
  ASSERT_EQ(OB_INVALID_ARGUMENT, normal_typed_sstable.init(normal_param, &allocator_));
  ASSERT_FALSE(normal_typed_sstable.is_inv_sstable());
}

TEST_F(TestInvSSTable, inv_sstable_deep_copy_keeps_type_and_meta)
{
  ObInvSSTable sstable;
  ObIStorageMetaObj *copied_obj = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  const int64_t deep_copy_size = sstable.get_deep_copy_size();
  char *buf = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable.deep_copy(buf, deep_copy_size, copied_obj));
  ASSERT_NE(nullptr, copied_obj);

  ObInvSSTable *copied_sstable = static_cast<ObInvSSTable *>(copied_obj);
  ASSERT_TRUE(copied_sstable->is_inv_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::INV_SSTABLE, copied_sstable->get_meta_type());
  ASSERT_TRUE(copied_sstable->get_inv_meta().is_valid());
  ASSERT_NE(&sstable.get_inv_meta(), &copied_sstable->get_inv_meta());
  ASSERT_EQ(100, copied_sstable->get_inv_meta().max_doc_id_);
  ASSERT_EQ(7, copied_sstable->get_inv_meta().min_doc_id_);
}

TEST_F(TestInvSSTable, inv_sstable_serialize_deserialize_round_trip)
{
  ObInvSSTable sstable;
  ObInvSSTable decoded_sstable;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  const int64_t buf_len = sstable.get_serialize_size(data_version);
  ASSERT_GT(buf_len, 0);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(decoded_sstable.is_inv_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::INV_SSTABLE, decoded_sstable.get_meta_type());
  ASSERT_TRUE(decoded_sstable.get_inv_meta().is_valid());
  ASSERT_EQ(100, decoded_sstable.get_inv_meta().max_doc_id_);
  ASSERT_EQ(7, decoded_sstable.get_inv_meta().min_doc_id_);
}

TEST_F(TestInvSSTable, inv_sstable_rejects_invalid_inv_meta_on_serialize_and_deserialize)
{
  ObInvSSTable sstable;
  ObInvSSTable decoded_sstable;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  sstable.inv_meta_.reset();
  const int64_t serialize_buf_len = sstable.get_serialize_size(data_version);
  char *serialize_buf = static_cast<char *>(allocator_.alloc(serialize_buf_len));
  ASSERT_NE(nullptr, serialize_buf);
  ASSERT_EQ(OB_ERR_UNEXPECTED, sstable.serialize(data_version, serialize_buf, serialize_buf_len, pos));

  sstable.reset();
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  const int64_t buf_len = sstable.ObSSTable::get_serialize_size(data_version)
      + sstable.inv_meta_.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, sstable.ObSSTable::serialize(data_version, buf, buf_len, pos));
  sstable.inv_meta_.index_type_ = INV_IDX_TYPE_INVALID;
  ASSERT_EQ(OB_SUCCESS, sstable.inv_meta_.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_FALSE(decoded_sstable.get_inv_meta().is_valid());
}

TEST_F(TestInvSSTable, storage_meta_bypass_processor_restores_inv_sstable)
{
  ObInvSSTable sstable;
  ObStorageMetaValueHandle handle;
  ObArenaAllocator handle_arena;
  ObSafeArenaAllocator safe_arena(handle_arena);
  const ObStorageMetaValue *value = nullptr;
  ObSSTable *decoded_sstable = nullptr;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  const int64_t buf_len = sstable.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(OB_SUCCESS, handle.new_value(handle_arena));
  ASSERT_EQ(OB_SUCCESS, ObStorageMetaValue::bypass_process_storage_meta<ObInvSSTable>(
      ObStorageMetaValue::MetaType::INV_SSTABLE, safe_arena, handle, buf, buf_len));
  ASSERT_NE(nullptr, handle.get_cache_value());
  value = handle.get_cache_value()->value_;
  ASSERT_NE(nullptr, value);
  ASSERT_EQ(OB_SUCCESS, value->get_sstable(decoded_sstable));
  ASSERT_NE(nullptr, decoded_sstable);
  ASSERT_TRUE(decoded_sstable->is_inv_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::INV_SSTABLE, decoded_sstable->get_meta_type());
}

TEST_F(TestInvSSTable, sstable_array_round_trip_with_normal_and_inv_tables)
{
  ObSSTable normal_sstable;
  ObInvSSTable inv_sstable;
  ObStorageSchema normal_storage_schema;
  ObTabletCreateSSTableParam normal_param;
  ObArray<ObITable *> tables;
  ObSSTableArray sstable_array;
  ObSSTableArray decoded_array;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::MAJOR_SSTABLE, normal_storage_schema, normal_param));
  ASSERT_EQ(OB_SUCCESS, normal_sstable.init(normal_param, &allocator_));
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(inv_sstable));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&normal_sstable));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&inv_sstable));
  ASSERT_EQ(OB_SUCCESS, sstable_array.init(allocator_, tables));
  ASSERT_EQ(storage::ObSerializeTableType::INV, sstable_array.serialize_table_type_);

  const int64_t buf_len = sstable_array.get_serialize_size(data_version);
  ASSERT_GT(buf_len, 0);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable_array.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_array.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_EQ(2, decoded_array.count());
  ASSERT_FALSE(decoded_array.at(0)->is_inv_sstable());
  ASSERT_TRUE(decoded_array.at(1)->is_inv_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::INV_SSTABLE, decoded_array.at(1)->get_meta_type());
}

TEST_F(TestInvSSTable, inv_sstable_rejects_row_store_interfaces)
{
  ObInvSSTable sstable;
  ObTableIterParam param;
  ObTableAccessContext context;
  ObDatumRowkey rowkey;
  ObArray<ObDatumRowkey> rowkeys;
  ObArray<ObDatumRange> ranges;
  ObStoreRowIterator *row_iter = reinterpret_cast<ObStoreRowIterator *>(0x1);

  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.get(param, context, rowkey, row_iter));
  ASSERT_EQ(nullptr, row_iter);

  row_iter = reinterpret_cast<ObStoreRowIterator *>(0x1);
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.multi_get(param, context, rowkeys, row_iter));
  ASSERT_EQ(nullptr, row_iter);

  row_iter = reinterpret_cast<ObStoreRowIterator *>(0x1);
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.multi_scan(param, context, ranges, row_iter));
  ASSERT_EQ(nullptr, row_iter);

  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.set_upper_trans_version(allocator_, 100));
}

TEST_F(TestInvSSTable, ob_sstable_public_helpers_keep_unsupported_inv_interfaces_rejected)
{
  ObSSTable sstable;
  make_inv_sstable_ready(sstable);

  ObReadInfoStruct read_info;
  ObDatumRange range;
  ObDatumRowkey rowkey;
  storage::ObMicroBlockIndexIterator *micro_iter = nullptr;
  ObSSTableSecMetaIterator *sec_meta_iter = nullptr;
  ObSSTableIndexScanner *index_scanner = nullptr;
  ObSSTableIndexScanParam scan_param;
  bool contain = false;
  ObDatumRange cs_range;
  ObDatumRowkey last_rowkey;

  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.scan_micro_block(
      range, read_info, allocator_, micro_iter));
  ASSERT_EQ(nullptr, micro_iter);

  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.scan_secondary_meta(
      allocator_, range, read_info, ObMacroBlockMetaType::DATA_BLOCK_META, sec_meta_iter));
  ASSERT_EQ(nullptr, sec_meta_iter);

  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.scan_index(
      range, scan_param, allocator_, index_scanner));
  ASSERT_EQ(nullptr, index_scanner);

  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.bf_may_contain_rowkey(rowkey, contain));
  // major INV_INDEX 仍不支持 backfill_commit_version；INC_INV 见下一条用例
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.backfill_commit_version(
      allocator_, 100, share::SCN::min_scn()));
  // get_last_rowkey 对 inv sstable 故意不拦截：它是基于索引树根的接口
  // （非行存接口），inv sstable 上可正常工作。
  // 下面的 get_last_rowkey_works_for_inv_sstable 已验证。
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.get_cs_range(
      range, read_info, allocator_, cs_range));
}

TEST_F(TestInvSSTable, inc_inv_sstable_supports_backfill_commit_version)
{
  ObInvSSTable sstable;
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObInvIdxStatsMeta inv_meta;
  share::SCN filled_tx_scn;
  const int64_t commit_version = 150;

  fill_inv_meta(7, 100, inv_meta);
  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INC_INV_INDEX_SSTABLE, storage_schema, param));
  param.inv_meta_ = inv_meta;
  // INC_INV 属于 is_inc_major_sstable，ObSSTableMeta::init 要求 inc major 的 param
  // 恰好携带一条未提交事务信息（真实链路由 inc major DDL 的 tx_id/seq_no 填入），
  // 否则 init 报 OB_ERR_UNEXPECTED；info_status 也需从默认的 INFO_OVERFLOW 改为
  // RECORD_SEQ，否则带一条 tx info 的 param 判定为非法。
  // 与 mittest test_async_index_compaction 的 mock_inc_major_sstable 写法保持一致。
  compaction::ObUncommitTxDesc tx_desc(100/*tx_id*/, 200/*sql_seq*/);
  ASSERT_EQ(OB_SUCCESS, param.uncommit_tx_info_.push_back(tx_desc));
  param.uncommit_tx_info_.set_info_status_record_seq();
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));
  ASSERT_TRUE(sstable.is_inc_inv_sstable());
  ASSERT_TRUE(sstable.is_inc_major_type_sstable());
  ASSERT_TRUE(sstable.is_loaded());

  ASSERT_EQ(OB_SUCCESS, filled_tx_scn.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, sstable.backfill_commit_version(
      allocator_, commit_version, filled_tx_scn));
  ASSERT_EQ(commit_version, sstable.get_upper_trans_version());
  ASSERT_EQ(filled_tx_scn, sstable.get_filled_tx_scn());
}

TEST_F(TestInvSSTable, scan_macro_block_allows_only_forward_macro_level_inv_scan)
{
  ObInvSSTable sstable;
  ObReadInfoStruct read_info;
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_TRUE(sstable.is_empty());

  // The two supported combinations cover both the plain macro iterator and the
  // macro + DATA_BLOCK_META iterator used by macro_info.
  for (const bool need_scan_sec_meta : {false, true}) {
    ObIMacroBlockIterator *macro_iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, sstable.scan_macro_block(
        whole_range, read_info, allocator_, macro_iter,
        false /* is_reverse_scan */, false /* need_record_micro_info */,
        need_scan_sec_meta));
    ASSERT_NE(nullptr, macro_iter);
    ObMacroBlockDesc macro_desc;
    ObDataMacroBlockMeta macro_meta;
    if (need_scan_sec_meta) {
      macro_desc.macro_meta_ = &macro_meta;
    }
    EXPECT_EQ(OB_ITER_END, macro_iter->get_next_macro_block(macro_desc));
    destroy_macro_iter(macro_iter);
  }

  // Recording standard micro info remains unsupported, with and without the
  // secondary-meta half of the Dual iterator.
  for (const bool need_scan_sec_meta : {false, true}) {
    ObIMacroBlockIterator *macro_iter = nullptr;
    EXPECT_EQ(OB_NOT_SUPPORTED, sstable.scan_macro_block(
        whole_range, read_info, allocator_, macro_iter,
        false /* is_reverse_scan */, true /* need_record_micro_info */,
        need_scan_sec_meta));
    EXPECT_EQ(nullptr, macro_iter);
  }

  // Reverse INV macro scanning is outside this change's capability boundary.
  // Cover all record/sec-meta combinations so neither factory branch can bypass it.
  for (const bool need_record_micro_info : {false, true}) {
    for (const bool need_scan_sec_meta : {false, true}) {
      ObIMacroBlockIterator *macro_iter = nullptr;
      EXPECT_EQ(OB_NOT_SUPPORTED, sstable.scan_macro_block(
          whole_range, read_info, allocator_, macro_iter,
          true /* is_reverse_scan */, need_record_micro_info,
          need_scan_sec_meta));
      EXPECT_EQ(nullptr, macro_iter);
    }
  }
}

TEST_F(TestInvSSTable, scan_macro_block_preserves_error_precedence)
{
  ObReadInfoStruct read_info;
  ObDatumRange whole_range;
  whole_range.set_whole_range();

  ObSSTable uninitialized_sstable;
  ObIMacroBlockIterator *macro_iter = nullptr;
  EXPECT_EQ(OB_NOT_INIT, uninitialized_sstable.scan_macro_block(
      whole_range, read_info, allocator_, macro_iter,
      false /* is_reverse_scan */, true /* need_record_micro_info */));
  EXPECT_EQ(nullptr, macro_iter);

  ObInvSSTable inv_sstable;
  ObDatumRange invalid_range;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(inv_sstable));
  EXPECT_EQ(OB_INVALID_ARGUMENT, inv_sstable.scan_macro_block(
      invalid_range, read_info, allocator_, macro_iter,
      false /* is_reverse_scan */, false /* need_record_micro_info */));
  EXPECT_EQ(nullptr, macro_iter);
}

TEST_F(TestInvSSTable, scan_macro_block_repeated_open_drain_destroy_and_allocator_reuse)
{
  ObInvSSTable sstable;
  ObReadInfoStruct read_info;
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));

  ObArenaAllocator iter_allocator;
  for (int64_t i = 0; i < 100; ++i) {
    ObIMacroBlockIterator *macro_iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, sstable.scan_macro_block(
        whole_range, read_info, iter_allocator, macro_iter,
        false /* is_reverse_scan */, false /* need_record_micro_info */,
        i % 2 == 0 /* need_scan_sec_meta */));
    ASSERT_NE(nullptr, macro_iter);
    ObMacroBlockDesc macro_desc;
    ObDataMacroBlockMeta macro_meta;
    if (i % 2 == 0) {
      macro_desc.macro_meta_ = &macro_meta;
    }
    EXPECT_EQ(OB_ITER_END, macro_iter->get_next_macro_block(macro_desc));
    macro_iter->~ObIMacroBlockIterator();
    iter_allocator.free(macro_iter);
    macro_iter = nullptr;
    iter_allocator.reuse();

    EXPECT_EQ(OB_NOT_SUPPORTED, sstable.scan_macro_block(
        whole_range, read_info, iter_allocator, macro_iter,
        false /* is_reverse_scan */, true /* need_record_micro_info */,
        i % 2 == 0 /* need_scan_sec_meta */));
    EXPECT_EQ(nullptr, macro_iter);
    iter_allocator.reuse();
  }
}

TEST_F(TestInvSSTable, normal_empty_sstable_still_accepts_record_micro_info)
{
  ObSSTable sstable;
  ObReadInfoStruct read_info;
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, build_normal_sstable(sstable));
  ASSERT_TRUE(sstable.is_empty());

  for (const bool need_scan_sec_meta : {false, true}) {
    ObIMacroBlockIterator *macro_iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, sstable.scan_macro_block(
        whole_range, read_info, allocator_, macro_iter,
        false /* is_reverse_scan */, true /* need_record_micro_info */,
        need_scan_sec_meta));
    ASSERT_NE(nullptr, macro_iter);
    ObMacroBlockDesc macro_desc;
    ObDataMacroBlockMeta macro_meta;
    if (need_scan_sec_meta) {
      macro_desc.macro_meta_ = &macro_meta;
    }
    EXPECT_EQ(OB_ITER_END, macro_iter->get_next_macro_block(macro_desc));
    destroy_macro_iter(macro_iter);
  }
}

// get_last_rowkey 对 inv sstable 已放通：它读取根索引块的最后一个 endkey
// （inv 使用标准 ObIndexBlock 格式），空 inv sstable 则通过 is_empty() 短路
// 返回 MAX_ROWKEY。此前 ObSSTable::get_last_rowkey 中的 OB_NOT_SUPPORTED
// 拦截已移除。
TEST_F(TestInvSSTable, get_last_rowkey_works_for_inv_sstable)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_TRUE(sstable.is_valid());
  ASSERT_TRUE(sstable.is_empty()) << "build_inv_sstable produces an empty sstable";

  ObDatumRowkey last_rowkey;
  ASSERT_EQ(OB_SUCCESS, sstable.get_last_rowkey(allocator_, last_rowkey));
  // 空 sstable => endkey 为 MAX_ROWKEY。
  EXPECT_TRUE(last_rowkey.is_max_rowkey()) << "empty inv sstable endkey must be MAX_ROWKEY";
}

// TODO: @yilan.zyn
TEST_F(TestInvSSTable, inv_query_interfaces_are_explicitly_unsupported_before_reader_implementation)
{
  ObInvSSTable sstable;
  ObReadInfoStruct read_info;
  ObDatumRowkey rowkey;
  ObInvSSTablePostingIter *posting_iter = reinterpret_cast<ObInvSSTablePostingIter *>(0x1);
  ObTableIterParam param;
  ObTableAccessContext context;
  ObDatumRange range;
  ObStoreRowIterator *row_iter = reinterpret_cast<ObStoreRowIterator *>(0x1);
  int64_t row_count = -1;

  // scan_posting now takes ObInvPostingScanParam + a rowkey read_info; an invalid (default)
  // param is rejected with OB_INVALID_ARGUMENT before the read_info is consumed. Functional
  // coverage lives in the mittest test_inv_sstable_posting_iter against a prepared inverted
  // SSTable.
  UNUSED(rowkey);
  ObInvPostingScanParam posting_param;
  ASSERT_EQ(OB_INVALID_ARGUMENT, sstable.scan_posting(posting_param, read_info, posting_iter));
  ASSERT_EQ(nullptr, posting_iter);
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.scan(param, context, range, row_iter));
  ASSERT_EQ(nullptr, row_iter);
  // inv row-count estimation now lives in ObIndexBlockScanEstimator; a default (invalid)
  // read_info / range is rejected with OB_INVALID_ARGUMENT. Functional coverage lives in
  // test_inv_sstable_posting_iter.
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObIndexBlockScanEstimator::estimate_inv_sstable_row_count(
          sstable, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(0, row_count);
}

TEST_F(TestInvSSTable, reset_clears_inv_meta)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_TRUE(sstable.get_inv_meta().is_valid());
  ASSERT_TRUE(sstable.is_inv_sstable());

  sstable.reset();
  ASSERT_FALSE(sstable.get_inv_meta().is_valid());
  ASSERT_FALSE(sstable.is_valid());
}

TEST_F(TestInvSSTable, version_getters_return_table_key_values)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));

  // Major INV 仍把逻辑区间放在 TableKey VersionRange。
  const int64_t snapshot_version = sstable.key_.version_range_.snapshot_version_;
  const int64_t base_version = sstable.key_.version_range_.base_version_;
  ASSERT_EQ(snapshot_version, sstable.get_snapshot_version());
  ASSERT_EQ(snapshot_version, sstable.get_upper_trans_version());
  ASSERT_EQ(snapshot_version, sstable.get_max_merged_trans_version());
  ASSERT_EQ(base_version, sstable.get_min_merged_trans_version());
  ASSERT_EQ(base_version, sstable.get_async_index_base_version());
  ASSERT_EQ(snapshot_version, sstable.get_async_index_snapshot_version());
  ObNewVersionRange version_range;
  ASSERT_EQ(OB_SUCCESS, sstable.get_async_index_version_range(version_range));
  ASSERT_EQ(base_version, version_range.base_version_);
  ASSERT_EQ(snapshot_version, version_range.snapshot_version_);
}

TEST_F(TestInvSSTable, inc_inv_reads_logical_range_from_basic_meta)
{
  ObInvSSTable sstable;
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObInvIdxStatsMeta inv_meta;
  const int64_t logical_base = 100;
  const int64_t logical_snapshot = 150;
  fill_inv_meta(7, 100, inv_meta);
  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INC_INV_INDEX_SSTABLE, storage_schema, param));
  param.inv_meta_ = inv_meta;
  compaction::ObUncommitTxDesc tx_desc(100/*tx_id*/, 200/*sql_seq*/);
  ASSERT_EQ(OB_SUCCESS, param.uncommit_tx_info_.push_back(tx_desc));
  param.uncommit_tx_info_.set_info_status_record_seq();
  ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.start_scn_.convert_for_tx(900));
  ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.end_scn_.convert_for_tx(980));
  param.min_merged_trans_version_ = logical_base;
  param.max_merged_trans_version_ = logical_snapshot;
  param.create_snapshot_version_ = logical_snapshot;
  param.filled_tx_scn_ = param.table_key_.scn_range_.end_scn_;
  param.rec_scn_ = param.table_key_.scn_range_.start_scn_;
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));
  ASSERT_EQ(900, sstable.get_start_scn().get_val_for_tx());
  ASSERT_EQ(980, sstable.get_end_scn().get_val_for_tx());
  ASSERT_NE(logical_snapshot, sstable.get_end_scn().get_val_for_tx());
  ASSERT_EQ(logical_base, sstable.get_min_merged_trans_version());
  ASSERT_EQ(logical_snapshot, sstable.get_max_merged_trans_version());
  ASSERT_EQ(logical_snapshot, sstable.get_snapshot_version()); // ObInvSSTable override
  ASSERT_NE(logical_snapshot, sstable.ObSSTable::get_snapshot_version()); // TableKey is ScnRange
  ASSERT_EQ(logical_base, sstable.get_async_index_base_version());
  ASSERT_EQ(logical_snapshot, sstable.get_async_index_snapshot_version());
  ObNewVersionRange version_range;
  ASSERT_EQ(OB_SUCCESS, sstable.get_async_index_version_range(version_range));
  ASSERT_EQ(logical_base, version_range.base_version_);
  ASSERT_EQ(logical_snapshot, version_range.snapshot_version_);
}

TEST_F(TestInvSSTable, get_frozen_schema_version_delegates_to_base)
{
  ObInvSSTable sstable;
  int64_t schema_version = -1;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS, sstable.get_frozen_schema_version(schema_version));
  ASSERT_GE(schema_version, 0);
}

TEST_F(TestInvSSTable, data_macro_block_count_on_empty_inv_sstable)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_EQ(0, sstable.get_data_macro_block_count());
}

TEST_F(TestInvSSTable, full_serialize_round_trip)
{
  ObInvSSTable sstable;
  ObInvSSTable decoded_sstable;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  const int64_t buf_len = sstable.get_full_serialize_size(data_version);
  ASSERT_GT(buf_len, 0);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  ASSERT_EQ(OB_SUCCESS, sstable.serialize_full_table(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(decoded_sstable.is_inv_sstable());
  ASSERT_TRUE(decoded_sstable.get_inv_meta().is_valid());
  ASSERT_EQ(100, decoded_sstable.get_inv_meta().max_doc_id_);
  ASSERT_EQ(7, decoded_sstable.get_inv_meta().min_doc_id_);
}

TEST_F(TestInvSSTable, inc_dec_macro_ref_on_empty_inv_sstable)
{
  ObInvSSTable sstable;
  bool inc_success = false;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS, sstable.inc_macro_ref(inc_success));
  ASSERT_TRUE(inc_success);
  sstable.dec_macro_ref();
}

TEST_F(TestInvSSTable, contain_uncommitted_row_always_false)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_FALSE(sstable.contain_uncommitted_row());
}

TEST_F(TestInvSSTable, no_data_to_read_on_empty_inv_sstable)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_TRUE(sstable.no_data_to_read());
}

TEST_F(TestInvSSTable, posting_scanner_accepts_empty_inv_sstable_with_valid_meta)
{
  ObInvSSTable sstable;
  ObSSTableMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(sstable));
  ASSERT_TRUE(sstable.is_empty());
  ASSERT_TRUE(sstable.get_inv_meta().is_valid());
  ASSERT_EQ(INV_IDX_TYPE_FTS, sstable.get_inv_meta().index_type_);
  ASSERT_EQ(OB_SUCCESS, sstable.get_meta(meta_handle));

  const int64_t rowkey_column_cnt =
      meta_handle.get_sstable_meta().get_schema_rowkey_column_count();
  ASSERT_GT(rowkey_column_cnt, 1);
  ObSEArray<ObColDesc, 4> rowkey_col_descs;
  ObSEArray<ObStorageDatum, 4> term_datums;
  for (int64_t i = 0; i < rowkey_column_cnt; ++i) {
    ObColDesc col_desc;
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    col_desc.col_type_.set_uint64();
    col_desc.col_order_ = ObOrderType::ASC;
    ASSERT_EQ(OB_SUCCESS, rowkey_col_descs.push_back(col_desc));
    if (i < rowkey_column_cnt - 1) {
      ObStorageDatum datum;
      datum.set_uint(i + 1);
      ASSERT_EQ(OB_SUCCESS, term_datums.push_back(datum));
    }
  }

  ObRowkeyReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, read_info.init(
      allocator_, rowkey_column_cnt, rowkey_column_cnt,
      false /*is_oracle_mode*/, rowkey_col_descs));
  ObDatumRowkey query_key(term_datums.get_data(), term_datums.count());
  ObQueryFlag query_flag;
  ObSSTablePostingScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.open(
      read_info, query_key, sstable.get_key().get_tablet_id(), query_flag,
      sstable, allocator_, false /*need_posting_value*/));
  EXPECT_TRUE(scanner.scan_iter_end_);
  bool block_changed = false;
  EXPECT_EQ(OB_ITER_END, scanner.next_block(block_changed));
}

TEST_F(TestInvSSTable, init_with_inv_meta_in_param)
{
  ObInvSSTable sstable;
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObInvIdxStatsMeta inv_meta;
  fill_inv_meta(42, 9999, inv_meta);

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INV_INDEX_SSTABLE, storage_schema, param));
  param.inv_meta_ = inv_meta;
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));
  ASSERT_TRUE(sstable.is_inv_sstable());
  ASSERT_TRUE(sstable.get_inv_meta().is_valid());
  ASSERT_EQ(9999, sstable.get_inv_meta().max_doc_id_);
  ASSERT_EQ(42, sstable.get_inv_meta().min_doc_id_);
  ASSERT_EQ(ObUInt64Type, sstable.get_inv_meta().doc_id_obj_meta_.get_type());
}

TEST_F(TestInvSSTable, init_without_inv_meta_in_param_returns_error)
{
  ObInvSSTable sstable;
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INV_INDEX_SSTABLE, storage_schema, param));
  ASSERT_FALSE(param.inv_meta_.is_valid());
  // An inv sstable must carry a valid inv_meta; a missing one is an upstream
  // inconsistency and init must reject it instead of silently filling a blank.
  ASSERT_EQ(OB_INVALID_ARGUMENT, sstable.init(param, &allocator_));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_sstable.log*");
  OB_LOGGER.set_file_name("test_inv_sstable.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
