/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
// unittest/storage/blocksstable/vector_index/test_ivf_sstable.cpp
//
// 测试用例目录:
//  [类型分类]
//   - table_type_contract:              验证 IVF_INDEX_SSTABLE 的类型标记
//   - inc_ivf_sstable_type_classification: 验证 INC_IVF_INDEX_SSTABLE 的类型标记
//
//  [扩展元数据]
//   - ivf_meta_reset_and_valid:         reset 清零；完整 DiskBBQ 元数据才合法
//   - ivf_meta_validation_boundaries:   验证算法、距离和量化元数据边界
//
//  [初始化]
//   - init_accepts_only_ivf_table_type: init / create_sstable 接受 IVF，拒绝 MAJOR
//   - init_accepts_inc_ivf_table_type:  init / create_sstable 接受 INC_IVF
//   - init_rejects_invalid_args_and_clears_state: 非法参数失败后清理旧状态
//   - set_ivf_meta_rejects_invalid:     set_ivf_meta 拒绝非法 meta
//
//  [深拷贝 / 序列化]
//   - ivf_sstable_deep_copy_keeps_type_and_meta
//   - deep_copy_rejects_invalid_buffer
//   - ivf_sstable_serialize_deserialize_round_trip
//   - ivf_commit_log_round_trip_replay_and_shared_table_store
//   - deserialize_rejects_invalid_ivf_meta
//   - deserialize_rejects_non_ivf_and_truncated_meta
//   - full_serialize_round_trip
//   - storage_meta_bypass_processor_restores_ivf_sstable
//   - storage_meta_real_processor_restores_ivf_sstable
//   - prewarm_classifies_ivf_and_uses_real_processor
//   - table_store_classifies_normal_co_inv_and_ivf_without_cross_talk
//   - sstable_array_round_trip_with_normal_and_ivf_tables
//   - sstable_array_rejects_mixed_inv_and_ivf
//
//  [接口]
//   - set_upper_trans_version_reuses_parent
//   - inc_ivf_sstable_supports_set_upper_trans_version
//   - inc_ivf_sstable_supports_backfill_commit_version
//   - row_read_rejects_data_block_cache
//
//  [生命周期与属性]
//   - reset_clears_ivf_meta
//   - version_getters_return_table_key_values
//   - empty_ivf_sstable_basic_properties
//   - create_empty_ivf_sstable_from_schema
//
//  [写盘 / 读回]
//   - write_types_with_shared_writer_flush_micro
//   - append_batch_flushes_row_type_boundaries
//   - append_batch_failure_keeps_successful_prefix
//   - flush_micro_block_not_init_and_empty: 未 open 返回 NOT_INIT；空微块 skip 后仍可继续写
//   - scan_macros_and_rows_after_write
//

#define USING_LOG_PREFIX STORAGE

#include "mtlenv/mock_tenant_module_env.h"
#include "lib/hash_func/splitmix64.h"
#include "lib/alloc/alloc_func.h"
#include "lib/utility/ob_tracepoint.h"

#include <gtest/gtest.h>
#include <atomic>
#include <algorithm>
#include <cmath>
#include <thread>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>

#define private public
#define protected public

#include "lib/alloc/memory_dump.h"
#include "share/ob_cluster_version.h"
#include "share/ob_rpc_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/ob_ivf_replay_memory_mgr.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_sstable_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_sstable_writer.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_capture_pipeline.h"
#include "storage/blocksstable/vector_index/ob_ivf_hca_algorithm_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_hca_adapter.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/blocksstable/vector_index/ob_ivf_merge_planner.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/compaction/ob_async_index_merge_task.h"
#include "storage/blocksstable/vector_index/ob_ivf_macro_block_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "storage/ddl/ob_ddl_inc_clog.h"
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ss_ddl_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/ob_i_store.h"
#include "storage/schema_utils.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tx/ob_trans_define.h"
#include "unittest/storage/blocksstable/cs_encoding/ob_row_vector_converter.h"
#include "unittest/storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"

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

static void fill_ivf_meta(const int32_t dim, ObIvfSSTableMeta &meta)
{
  meta.reset();
  meta.dim_ = dim;
  meta.index_type_ = VIAT_DISK_BBQ;
  meta.dist_type_ = VIDA_L2;
  meta.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
  meta.nbits_ = 1;
  meta.fht_algo_version_ =
      quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
}

// Keep the old UNIS payload shape to verify append-only metadata evolution.
struct LegacyIVFSSTableMeta
{
  OB_UNIS_VERSION(1);
public:
  ObIvfSSTableMeta meta_;
};

OB_SERIALIZE_MEMBER(LegacyIVFSSTableMeta,
    meta_.flags_, meta_.index_type_, meta_.dist_type_, meta_.dim_,
    meta_.format_version_, meta_.nbits_, meta_.fht_algo_version_, meta_.centroid_count_);

// The immediately preceding wire shape includes counts/layout but no
// canonical-source capability. Its reader must ignore the optional new tail.
struct PreCanonicalIVFSSTableMeta
{
  OB_UNIS_VERSION(1);
public:
  ObIvfSSTableMeta meta_;
};

OB_SERIALIZE_MEMBER(PreCanonicalIVFSSTableMeta,
    meta_.flags_, meta_.index_type_, meta_.dist_type_, meta_.dim_,
    meta_.format_version_, meta_.nbits_, meta_.fht_algo_version_, meta_.centroid_count_,
    meta_.vector_count_, meta_.centroid_layout_, meta_.parent_centroid_count_);

class TestIVFSSTable : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // Reuse MockTenantModuleEnv's sys tenant. Do not replace it with a local
    // ObTenantBase, which drops the IO manager and timer run_wrapper.
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
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
      param.table_key_.version_range_.base_version_ = 0;
      param.table_key_.version_range_.snapshot_version_ = snapshot_version;
      if (ObITable::is_ivf_sstable(table_type)) {
        param.ivf_meta_.set_empty();
      }
    }
    return ret;
  }

  int fill_inc_major_uncommit_tx(ObTabletCreateSSTableParam &param)
  {
    int ret = OB_SUCCESS;
    compaction::ObUncommitTxDesc tx_desc(100 /*tx_id*/, 200 /*sql_seq*/);
    if (OB_FAIL(param.uncommit_tx_info_.push_back(tx_desc))) {
      LOG_WARN("failed to push uncommit tx desc", K(ret));
    } else {
      param.uncommit_tx_info_.set_info_status_record_seq();
    }
    return ret;
  }

  int build_inc_ivf_sstable(ObIvfSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    fill_ivf_meta(128, ivf_meta);
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::INC_IVF_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build inc ivf sstable param", K(ret));
    } else if (OB_FAIL(fill_inc_major_uncommit_tx(param))) {
      LOG_WARN("failed to fill inc major uncommit tx", K(ret));
    } else {
      param.ivf_meta_ = ivf_meta;
      if (OB_FAIL(sstable.init(param, &allocator_))) {
        LOG_WARN("failed to init inc ivf sstable", K(ret));
      }
    }
    return ret;
  }

  int build_ivf_sstable(ObIvfSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    fill_ivf_meta(128, ivf_meta);
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::IVF_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build sstable param", K(ret));
    } else {
      param.ivf_meta_ = ivf_meta;
      if (OB_FAIL(sstable.init(param, &allocator_))) {
        LOG_WARN("failed to init ivf sstable", K(ret));
      }
    }
    return ret;
  }

  int build_disk_bbq_ivf_sstable(ObIvfSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    const int32_t dim = 10;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    fill_ivf_meta(dim, ivf_meta);
    if (OB_FAIL(build_empty_sstable_param(
                   ObITable::TableType::IVF_INDEX_SSTABLE,
                   storage_schema, param))) {
      LOG_WARN("failed to build Disk BBQ sstable param", K(ret));
    } else if (FALSE_IT(param.ivf_meta_ = ivf_meta)) {
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init Disk BBQ ivf sstable", K(ret));
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

  int build_co_sstable(ObTableHandleV2 &table_handle, ObSSTable *&sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    sstable = nullptr;
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::COLUMN_ORIENTED_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build co sstable param", K(ret));
    } else {
      param.co_base_type_ = ObCOSSTableBaseType::ALL_CG_TYPE;
      param.column_group_cnt_ = 2;
      param.is_co_table_without_cgs_ = true;
      if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
          param, allocator_, table_handle))) {
        LOG_WARN("failed to create co sstable", K(ret));
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get co sstable", K(ret));
      }
    }
    return ret;
  }

  int build_single_major_table_store(
      const ObSSTable &sstable,
      ObTabletTableStore &table_store)
  {
    int ret = table_store.major_tables_.init(allocator_, &sstable);
    if (OB_SUCC(ret)) {
      table_store.is_inited_ = true;
    }
    return ret;
  }

  int build_async_ivf_storage_schema(ObStorageSchema &storage_schema)
  {
    int ret = OB_SUCCESS;
    share::schema::ObTableSchema table_schema;
    TestSchemaUtils::prepare_data_schema(table_schema);
    table_schema.set_table_type(USER_INDEX);
    table_schema.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
    table_schema.set_data_table_id(table_schema.get_table_id());
    if (OB_FAIL(storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("failed to init async ivf storage schema", K(ret));
    }
    return ret;
  }

  int build_inv_sstable(ObInvSSTable &sstable)
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObInvIdxStatsMeta inv_meta;
    inv_meta.reset();
    inv_meta.min_doc_id_ = 7;
    inv_meta.max_doc_id_ = 100;
    inv_meta.doc_id_obj_meta_.set_uint64();
    inv_meta.index_type_ = INV_IDX_TYPE_FTS;
    inv_meta.has_relevance_params_ = true;
    if (OB_FAIL(build_empty_sstable_param(
        ObITable::TableType::INV_INDEX_SSTABLE, storage_schema, param))) {
      LOG_WARN("failed to build inv sstable param", K(ret));
    } else if (OB_FAIL(param.inv_meta_.init(inv_meta))) {
      LOG_WARN("failed to init inv sstable meta", K(ret));
    } else if (OB_FAIL(sstable.init(param, &allocator_))) {
      LOG_WARN("failed to init inv sstable", K(ret));
    }
    return ret;
  }

  MacroBlockId build_tiered_meta_block_id(const int64_t seq_id)
  {
    MacroBlockId block_id;
    block_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    block_id.set_storage_object_type(
        static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
    block_id.set_location(ObMacroBlockIdLocation::TABLE);
    block_id.set_second_id(1001);
    block_id.set_reorganization_scn(100);
    block_id.set_op_id(200);
    block_id.set_seq_id(seq_id);
    return block_id;
  }

#ifdef OB_BUILD_SHARED_STORAGE
  struct SSTableBlockIdSetBundle
  {
    SSTableBlockIdSetBundle() = default;
    ~SSTableBlockIdSetBundle()
    {
      normal_sst_ids_.destroy();
      co_sst_ids_.destroy();
      inv_sst_ids_.destroy();
      ivf_sst_ids_.destroy();
    }

    SSTableBlockIdSetBundle(const SSTableBlockIdSetBundle &) = delete;
    SSTableBlockIdSetBundle &operator=(const SSTableBlockIdSetBundle &) = delete;

    hash::ObHashSet<MacroBlockId> normal_sst_ids_;
    hash::ObHashSet<MacroBlockId> co_sst_ids_;
    hash::ObHashSet<MacroBlockId> inv_sst_ids_;
    hash::ObHashSet<MacroBlockId> ivf_sst_ids_;
  };

  enum ExpectedSSTableSet
  {
    NORMAL,
    CO,
    INV,
    IVF,
  };

  void assert_only_in_expected_set(
      const MacroBlockId &block_id,
      const ExpectedSSTableSet expected_set,
      const hash::ObHashSet<MacroBlockId> &normal_sst_ids,
      const hash::ObHashSet<MacroBlockId> &co_sst_ids,
      const hash::ObHashSet<MacroBlockId> &inv_sst_ids,
      const hash::ObHashSet<MacroBlockId> &ivf_sst_ids)
  {
    EXPECT_EQ(NORMAL == expected_set ? OB_HASH_EXIST : OB_HASH_NOT_EXIST,
        normal_sst_ids.exist_refactored(block_id));
    EXPECT_EQ(CO == expected_set ? OB_HASH_EXIST : OB_HASH_NOT_EXIST,
        co_sst_ids.exist_refactored(block_id));
    EXPECT_EQ(INV == expected_set ? OB_HASH_EXIST : OB_HASH_NOT_EXIST,
        inv_sst_ids.exist_refactored(block_id));
    EXPECT_EQ(IVF == expected_set ? OB_HASH_EXIST : OB_HASH_NOT_EXIST,
        ivf_sst_ids.exist_refactored(block_id));
    EXPECT_EQ(NORMAL == expected_set ? 1 : 0, normal_sst_ids.size());
    EXPECT_EQ(CO == expected_set ? 1 : 0, co_sst_ids.size());
    EXPECT_EQ(INV == expected_set ? 1 : 0, inv_sst_ids.size());
    EXPECT_EQ(IVF == expected_set ? 1 : 0, ivf_sst_ids.size());
  }
#endif

  ObArenaAllocator allocator_;
};

TEST_F(TestIVFSSTable, table_type_contract)
{
  ObITable::TableKey table_key;
  table_key.reset();
  table_key.table_type_ = ObITable::TableType::IVF_INDEX_SSTABLE;

  ASSERT_TRUE(table_key.is_ivf_sstable());
  ASSERT_TRUE(table_key.is_major_sstable());
  ASSERT_TRUE(ObITable::is_sstable(table_key.table_type_));
  ASSERT_FALSE(table_key.is_inv_sstable());
  ASSERT_FALSE(table_key.is_co_sstable());
  ASSERT_FALSE(table_key.is_row_store_major_sstable());
  ASSERT_FALSE(table_key.is_inc_major_sstable());
  ASSERT_FALSE(table_key.is_inc_ivf_sstable());
  ASSERT_FALSE(table_key.is_inc_major_type_sstable());

  ObIvfSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_TRUE(sstable.is_ivf_sstable());
  ASSERT_TRUE(sstable.is_major_sstable());
  ASSERT_FALSE(sstable.is_inc_ivf_sstable());
  ASSERT_FALSE(sstable.is_inv_sstable());
  ASSERT_FALSE(sstable.is_co_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable.get_meta_type());
  ASSERT_TRUE(is_serialize_table_type_valid(static_cast<int8_t>(ObSerializeTableType::IVF)));
}

TEST_F(TestIVFSSTable, inc_ivf_sstable_type_classification)
{
  ObITable::TableKey table_key;
  table_key.reset();
  table_key.table_type_ = ObITable::TableType::INC_IVF_INDEX_SSTABLE;

  ASSERT_TRUE(table_key.is_ivf_sstable());
  ASSERT_TRUE(table_key.is_inc_ivf_sstable());
  ASSERT_TRUE(table_key.is_inc_major_type_sstable());
  ASSERT_TRUE(table_key.is_inc_major_sstable());
  ASSERT_TRUE(ObITable::is_sstable(table_key.table_type_));
  ASSERT_FALSE(table_key.is_major_sstable());
  ASSERT_FALSE(table_key.is_inv_sstable());
  ASSERT_FALSE(table_key.is_inc_inv_sstable());
  ASSERT_FALSE(table_key.is_co_sstable());
  ASSERT_FALSE(table_key.is_row_store_major_sstable());
}

TEST_F(TestIVFSSTable, ivf_meta_reset_and_valid)
{
  ObIvfSSTableMeta meta;
  ASSERT_EQ(0, meta.dim_);
  ASSERT_EQ(0, meta.format_version_);
  ASSERT_EQ(0, meta.flags_);
  ASSERT_EQ(0, meta.vector_count_);
  ASSERT_FALSE(meta.is_valid());
  ASSERT_FALSE(meta.is_empty());

  meta.set_empty();
  ASSERT_EQ(1, meta.flags_);
  ASSERT_TRUE(meta.is_valid());
  ASSERT_TRUE(meta.is_empty());

  meta.reset();
  fill_ivf_meta(128, meta);
  ASSERT_TRUE(meta.is_valid());
  ASSERT_FALSE(meta.is_empty());
  ASSERT_EQ(128, meta.dim_);
  ASSERT_EQ(IVF_SSTABLE_FORMAT_VERSION, meta.format_version_);
  ASSERT_EQ(VIAT_DISK_BBQ, meta.index_type_);
  ASSERT_EQ(VIDA_L2, meta.dist_type_);

  meta.vector_count_ = 100;
  meta.reset();
  ASSERT_EQ(0, meta.dim_);
  ASSERT_EQ(0, meta.vector_count_);
  ASSERT_FALSE(meta.is_valid());
  ASSERT_FALSE(meta.is_empty());
}

TEST_F(TestIVFSSTable, ivf_meta_vector_count_serialization_compatibility)
{
  ObIvfSSTableMeta source;
  fill_ivf_meta(128, source);
  source.centroid_count_ = 8;
  source.vector_count_ = 12345;
  ObIvfSSTableMeta decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.assign(allocator_, source));
  EXPECT_EQ(source.vector_count_, decoded.vector_count_);

  char buf[256];
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, sizeof(buf), size));
  EXPECT_EQ(source.get_serialize_size(), size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, size, pos));
  EXPECT_EQ(size, pos);
  EXPECT_EQ(source.vector_count_, decoded.vector_count_);
  ASSERT_TRUE(decoded.is_valid());

  // Old readers skip the new trailing field using the UNIS payload length.
  LegacyIVFSSTableMeta legacy;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.deserialize(buf, size, pos));
  EXPECT_EQ(size, pos);
  EXPECT_EQ(source.centroid_count_, legacy.meta_.centroid_count_);
  EXPECT_EQ(source.dim_, legacy.meta_.dim_);
  ASSERT_TRUE(legacy.meta_.is_valid());

  // Production readers construct a fresh destination; reset when reusing one.
  size = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(buf, sizeof(buf), size));
  pos = 0;
  decoded.reset();
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, size, pos));
  EXPECT_EQ(size, pos);
  EXPECT_EQ(0, decoded.vector_count_);
  EXPECT_EQ(source.centroid_count_, decoded.centroid_count_);
  ASSERT_TRUE(decoded.is_valid());

  decoded.vector_count_ = -1;
  EXPECT_FALSE(decoded.is_valid());
  decoded.set_empty();
  EXPECT_EQ(0, decoded.vector_count_);
  EXPECT_TRUE(decoded.is_valid());
  decoded.vector_count_ = 1;
  EXPECT_FALSE(decoded.is_valid());
}



TEST_F(TestIVFSSTable, ivf_meta_validation_boundaries)
{
  ObIvfSSTableMeta meta;

  fill_ivf_meta(1, meta);
  ASSERT_TRUE(meta.is_valid());
  meta.format_version_ = IVF_SSTABLE_FORMAT_VERSION + 1;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  ASSERT_TRUE(meta.is_valid());

  meta.index_type_ = VIAT_MAX;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  ASSERT_TRUE(meta.is_valid());
  meta.dist_type_ = VIDA_MAX;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  ASSERT_TRUE(meta.is_valid());
  meta.dist_type_ = -1;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  ASSERT_TRUE(meta.is_valid());
  meta.dim_ = 0;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  ASSERT_TRUE(meta.is_valid());
  meta.dim_ = -1;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  meta.nbits_ = 0;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  meta.nbits_ = -1;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  meta.nbits_ = 2;
  ASSERT_FALSE(meta.is_valid());

  fill_ivf_meta(1, meta);
  meta.fht_algo_version_ = 2;
  ASSERT_FALSE(meta.is_valid());
}

TEST_F(TestIVFSSTable, init_accepts_only_ivf_table_type)
{
  ObStorageSchema ivf_storage_schema;
  ObStorageSchema normal_storage_schema;
  ObTabletCreateSSTableParam ivf_param;
  ObTabletCreateSSTableParam normal_param;
  ObIvfSSTable ivf_sstable;
  ObIvfSSTable normal_typed_sstable;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::IVF_INDEX_SSTABLE, ivf_storage_schema, ivf_param));
  ASSERT_EQ(OB_SUCCESS, ivf_sstable.init(ivf_param, &allocator_));
  ASSERT_TRUE(ivf_sstable.is_ivf_sstable());
  ASSERT_TRUE(ivf_sstable.is_loaded());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, ivf_sstable.get_meta_type());

  ObIvfSSTable factory_sstable;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      ivf_param, allocator_, factory_sstable));
  ASSERT_TRUE(factory_sstable.is_ivf_sstable());
  ASSERT_TRUE(factory_sstable.is_loaded());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, factory_sstable.get_meta_type());

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::MAJOR_SSTABLE, normal_storage_schema, normal_param));
  ASSERT_EQ(OB_INVALID_ARGUMENT, normal_typed_sstable.init(normal_param, &allocator_));
  ASSERT_FALSE(normal_typed_sstable.is_ivf_sstable());
}

TEST_F(TestIVFSSTable, init_accepts_inc_ivf_table_type)
{
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  ObIvfSSTable sstable;

  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::INC_IVF_INDEX_SSTABLE, storage_schema, param));
  ASSERT_EQ(OB_SUCCESS, fill_inc_major_uncommit_tx(param));
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));
  ASSERT_TRUE(sstable.is_ivf_sstable());
  ASSERT_TRUE(sstable.is_inc_ivf_sstable());
  ASSERT_TRUE(sstable.is_inc_major_type_sstable());
  ASSERT_TRUE(sstable.is_inc_major_sstable());
  ASSERT_FALSE(sstable.is_major_sstable());
  ASSERT_TRUE(sstable.is_loaded());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable.get_meta_type());

  ObIvfSSTable factory_sstable;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      param, allocator_, factory_sstable));
  ASSERT_TRUE(factory_sstable.is_ivf_sstable());
  ASSERT_TRUE(factory_sstable.is_inc_ivf_sstable());
  ASSERT_TRUE(factory_sstable.is_loaded());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, factory_sstable.get_meta_type());
}

TEST_F(TestIVFSSTable, init_rejects_invalid_args_and_clears_state)
{
  ObStorageSchema null_allocator_storage_schema;
  ObTabletCreateSSTableParam null_allocator_param;
  ObIvfSSTable null_allocator_sstable;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(null_allocator_sstable));
  ASSERT_TRUE(null_allocator_sstable.is_loaded());
  ASSERT_TRUE(null_allocator_sstable.is_ivf_sstable());
  ASSERT_TRUE(null_allocator_sstable.get_ivf_meta().is_valid());
  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::IVF_INDEX_SSTABLE,
      null_allocator_storage_schema,
      null_allocator_param));
  fill_ivf_meta(1, null_allocator_param.ivf_meta_);
  ASSERT_TRUE(null_allocator_param.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, null_allocator_sstable.init(null_allocator_param, nullptr));
  ASSERT_FALSE(null_allocator_sstable.is_loaded());
  ASSERT_FALSE(null_allocator_sstable.is_ivf_sstable());
  ASSERT_EQ(ObITable::TableType::MAX_TABLE_TYPE,
      null_allocator_sstable.get_key().table_type_);
  ASSERT_FALSE(null_allocator_sstable.get_ivf_meta().is_valid());

  ObStorageSchema invalid_meta_storage_schema;
  ObTabletCreateSSTableParam invalid_meta_param;
  ObIvfSSTable invalid_meta_sstable;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(invalid_meta_sstable));
  ASSERT_TRUE(invalid_meta_sstable.is_loaded());
  ASSERT_TRUE(invalid_meta_sstable.is_ivf_sstable());
  ASSERT_TRUE(invalid_meta_sstable.get_ivf_meta().is_valid());
  ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
      ObITable::TableType::IVF_INDEX_SSTABLE,
      invalid_meta_storage_schema,
      invalid_meta_param));
  invalid_meta_param.ivf_meta_.reset();
  ASSERT_FALSE(invalid_meta_param.ivf_meta_.is_valid());
  ASSERT_TRUE(invalid_meta_param.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_meta_sstable.init(invalid_meta_param, &allocator_));
  ASSERT_FALSE(invalid_meta_sstable.is_loaded());
  ASSERT_FALSE(invalid_meta_sstable.is_ivf_sstable());
  ASSERT_EQ(ObITable::TableType::MAX_TABLE_TYPE,
      invalid_meta_sstable.get_key().table_type_);
  ASSERT_FALSE(invalid_meta_sstable.get_ivf_meta().is_valid());
}

TEST_F(TestIVFSSTable, set_ivf_meta_rejects_invalid)
{
  ObIvfSSTable sstable;
  ObIvfSSTableMeta invalid_meta;
  ObIvfSSTableMeta valid_meta;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sstable.set_ivf_meta(allocator_, invalid_meta));
  fill_ivf_meta(64, valid_meta);
  ASSERT_EQ(OB_SUCCESS, sstable.set_ivf_meta(allocator_, valid_meta));
  ASSERT_EQ(64, sstable.get_ivf_meta().dim_);
}

TEST_F(TestIVFSSTable, capture_ignores_empty_sstables_preserving_source_indexes)
{
  ObIvfSSTable empty_base;
  ObIvfSSTable empty_inc;
  ObIvfSSTable data_inc;
  empty_base.key_.table_type_ = ObITable::IVF_INDEX_SSTABLE;
  empty_inc.key_.table_type_ = data_inc.key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
  empty_base.key_.tablet_id_ = empty_inc.key_.tablet_id_ = data_inc.key_.tablet_id_ = ObTabletID(1001);
  data_inc.meta_cache_.data_macro_block_count_ = 1;
  // A zero row statistic must not hide a physically nonempty source.
  data_inc.meta_cache_.row_count_ = 0;
  ObIVFMergeInputAdapter input(allocator_);
  ObTableHandleV2 handle;
  for (ObIvfSSTable *table : {&empty_base, &data_inc, &empty_inc, &data_inc, &empty_inc}) {
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(table));
    ASSERT_EQ(OB_SUCCESS, input.source_tables_.add_table(handle));
  }
  ObIVFMergeCaptureCtx capture;
  capture.input_ = &input;
  ASSERT_EQ(OB_SUCCESS, capture.plan_(false));
  ASSERT_EQ(1, capture.slices_.count());
  ASSERT_EQ(2, capture.slices_.at(0)->ranges_.count());
  EXPECT_EQ(1, capture.slices_.at(0)->ranges_.at(0).table_idx_);
  EXPECT_EQ(3, capture.slices_.at(0)->ranges_.at(1).table_idx_);
  EXPECT_EQ(5, input.source_tables_.get_count());
  EXPECT_EQ(&empty_base, input.source_tables_.get_table(0));
  EXPECT_EQ(&data_inc, input.source_tables_.get_table(1));
  EXPECT_EQ(&empty_inc, input.source_tables_.get_table(2));
}

TEST_F(TestIVFSSTable, prior_arrays_reserve_once_for_all_eligible_sources)
{
  constexpr int64_t segments = 32, leaves = 31, dimension = 128, live_rows = 19;
  for (int64_t postings_per_vector : {1, 2}) {
    ObIvfSSTable empty, data, unsupported;
    for (auto *table : {&empty, &data, &unsupported}) {
      table->key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
      table->key_.tablet_id_ = ObTabletID(1001);
      table->ivf_meta_.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
      table->ivf_meta_.centroid_count_ = leaves;
    }
    data.meta_cache_.data_macro_block_count_ = 1;
    unsupported.meta_cache_.data_macro_block_count_ = 1;
    unsupported.ivf_meta_.format_version_ = IVF_SSTABLE_FORMAT_VERSION + 1;
    unsupported.ivf_meta_.centroid_count_ = INT64_MAX; // must be skipped, not reserved
    ObArenaAllocator arena("IVFPriorReserve");
    ObIVFMergeInputAdapter input(arena);
    input.vector_dim_ = dimension;
    input.prior_.vector_count_ = segments * live_rows + 7;
    for (int64_t i = 0; i < segments + 2; ++i) {
      ObTableHandleV2 handle;
      ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(
          i == 0 ? &empty : i == segments + 1 ? &unsupported : &data));
      ASSERT_EQ(OB_SUCCESS, input.source_tables_.add_table(handle));
    }
    ASSERT_EQ(OB_SUCCESS, input.prior_.segments_.prepare_allocate(segments + 2));
    for (int64_t i = 1; i <= segments; ++i) {
      input.prior_.segments_.at(i).live_vector_count_ = live_rows;
    }
    input.prior_.segments_.at(segments + 1).live_vector_count_ = 7;
    const int64_t before_reserve = arena.used();
    int64_t centers = 0, posting_limit = 0;
    ASSERT_EQ(OB_SUCCESS, input.reserve_prior_arrays_(centers, posting_limit));
    EXPECT_EQ(segments * leaves, centers);
    EXPECT_EQ(segments * live_rows * 2, posting_limit);
    const int64_t reserved = arena.used();
    const int64_t posting_reserved = input.prior_posting_allocator_.used();
    const int64_t payload_bytes = centers * dimension * sizeof(float)
        + centers * sizeof(int64_t) * 2 + posting_limit * sizeof(ObIVFPriorPosting);
    // Allow block rounding and Sanity redzones, but not historical buffers.
    EXPECT_LE(reserved - before_reserve + posting_reserved, payload_bytes + 8 * OB_MALLOC_NORMAL_BLOCK_SIZE);
    for (int64_t segment = 0; segment < segments; ++segment) {
      for (int64_t cid = 0; cid < leaves; ++cid) {
        ASSERT_EQ(OB_SUCCESS, input.prior_.old_cids_.push_back(cid + 1));
        ASSERT_EQ(OB_SUCCESS, input.prior_.posting_mass_.push_back(0));
        for (int64_t d = 0; d < dimension; ++d) {
          ASSERT_EQ(OB_SUCCESS, input.prior_.centroids_.push_back(float(segment + cid + d)));
        }
      }
      for (int64_t row = 0; row < live_rows; ++row) {
        for (int64_t copy = 0; copy < postings_per_vector; ++copy) {
          ASSERT_EQ(OB_SUCCESS, input.prior_.postings_.push_back(
              ObIVFPriorPosting(segment * live_rows + row, segment * leaves + copy)));
        }
      }
      EXPECT_EQ(reserved, arena.used());
      EXPECT_EQ(posting_reserved, input.prior_posting_allocator_.used());
    }
    EXPECT_EQ(centers * dimension, input.prior_.centroids_.count());
    EXPECT_EQ(segments * live_rows * postings_per_vector, input.prior_.postings_.count());
    input.release_prior_postings();
    input.release_prior_postings();
    EXPECT_EQ(0, input.prior_posting_allocator_.total());
    EXPECT_EQ(centers * dimension, input.prior_.centroids_.count());
    EXPECT_EQ(centers, input.prior_.old_cids_.count());
    EXPECT_EQ(centers, input.prior_.posting_mass_.count());
    EXPECT_EQ(reserved, arena.used());
  }
}

TEST_F(TestIVFSSTable, prior_reservation_rejects_overflow_before_allocating)
{
  for (int variant = 0; variant < 8; ++variant) {
    SCOPED_TRACE(variant);
    ObIvfSSTable table;
    table.key_.table_type_ = ObITable::IVF_INDEX_SSTABLE;
    table.key_.tablet_id_ = ObTabletID(1001);
    table.meta_cache_.data_macro_block_count_ = 1;
    table.ivf_meta_.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
    table.ivf_meta_.centroid_count_ = 1;
    ObArenaAllocator arena("IVFPriorBound");
    ObIVFMergeInputAdapter input(arena);
    ObTableHandleV2 handle;
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&table));
    ASSERT_EQ(OB_SUCCESS, input.source_tables_.add_table(handle));
    ASSERT_EQ(OB_SUCCESS, input.prior_.segments_.prepare_allocate(1));
    input.vector_dim_ = 2;
    input.prior_.vector_count_ = input.prior_.segments_.at(0).live_vector_count_ = 1;
    int expected = OB_SIZE_OVERFLOW;
    switch (variant) {
      case 0: input.vector_dim_ = 0; expected = OB_INVALID_ARGUMENT; break;
      case 1: table.ivf_meta_.centroid_count_ = 0; expected = OB_INVALID_DATA; break;
      case 2: table.ivf_meta_.centroid_count_ = int64_t(INT32_MAX) + 1; expected = OB_INVALID_DATA; break;
      case 3: table.ivf_meta_.centroid_count_ = 2; input.vector_dim_ = INT64_MAX; break;
      case 4: input.vector_dim_ = INT64_MAX / sizeof(float); break; // byte alignment overflows
      case 5:
        input.prior_.vector_count_ = input.prior_.segments_.at(0).live_vector_count_ = INT64_MAX / 2 + 1;
        break;
      case 6:
        input.prior_.vector_count_ = input.prior_.segments_.at(0).live_vector_count_
            = (INT64_MAX / sizeof(ObIVFPriorPosting)) / 2;
        break; // posting bytes fit, block rounding does not
      case 7:
        input.prior_.segments_.at(0).live_vector_count_ = 2;
        expected = OB_INVALID_DATA;
        break;
    }
    const int64_t before = arena.used();
    int64_t centers = 0, postings = 0;
    EXPECT_EQ(expected, input.reserve_prior_arrays_(centers, postings));
    EXPECT_EQ(before, arena.used());
    EXPECT_EQ(0, input.prior_.centroids_.get_data_size());
    EXPECT_EQ(0, input.prior_.postings_.get_data_size());
  }
}

TEST_F(TestIVFSSTable, capture_all_empty_inputs_has_no_scan_ranges)
{
  ObIvfSSTable empty;
  empty.key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
  empty.key_.tablet_id_ = ObTabletID(1001);
  ObIVFMergeInputAdapter input(allocator_);
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&empty));
  ASSERT_EQ(OB_SUCCESS, input.source_tables_.add_table(handle));
  ASSERT_EQ(OB_SUCCESS, input.source_tables_.add_table(handle));
  ObIVFMergeCaptureCtx capture;
  capture.input_ = &input;
  ASSERT_EQ(OB_SUCCESS, capture.plan_(true));
  ASSERT_EQ(1, capture.slices_.count());
  EXPECT_TRUE(capture.slices_.at(0)->ranges_.empty());
  EXPECT_EQ(2, input.source_tables_.get_count());
}

TEST_F(TestIVFSSTable, empty_capture_requires_scan_completion_and_finishes_once)
{
  ObIvfSSTable empty;
  ASSERT_EQ(OB_SUCCESS, build_inc_ivf_sstable(empty));
  ASSERT_TRUE(empty.is_empty());
  const ObITable::TableKey original_key = empty.get_key();
  for (const bool parallel : {false, true}) {
    SCOPED_TRACE(parallel);
    ObIVFMergeInputAdapter input(allocator_);
    ObTableHandleV2 handle;
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&empty));
    ASSERT_EQ(OB_SUCCESS, input.source_tables_.add_table(handle));
    ObIVFMergeCaptureCtx capture;
    capture.input_ = &input;
    ASSERT_EQ(OB_SUCCESS, capture.plan_(parallel));
    // This focused state test bypasses CaptureCtx::init; reproduce its replay
    // ownership setup before driving the empty scan/seal barrier.
    input.vector_dim_ = empty.get_ivf_meta().dim_;
    ASSERT_EQ(OB_SUCCESS, input.init_replay_inputs_(capture.slices_.count()));
    for (int64_t i = 0; i < capture.slices_.count(); ++i) {
      capture.slices_.at(i)->replay_ = input.replay_inputs_.at(i);
      capture.slices_.at(i)->slice_idx_ = i;
    }
    capture.is_inited_ = true;
    ObIVFMergeCapturePipeline task;
    ASSERT_EQ(OB_SUCCESS, task.init(&capture, 0));
    EXPECT_EQ(OB_STATE_NOT_MATCH, capture.finish());
    EXPECT_FALSE(input.is_inited_);
    ASSERT_EQ(OB_SUCCESS, capture.scan(0));
    EXPECT_EQ(OB_INIT_TWICE, capture.scan(0));
    ASSERT_EQ(OB_SUCCESS, capture.finish());
    EXPECT_TRUE(input.is_inited_);
    EXPECT_TRUE(input.is_empty());
    EXPECT_TRUE(capture.slices_.empty());
    EXPECT_EQ(1, input.source_tables_.get_count());
    EXPECT_EQ(original_key, input.source_tables_.get_table(0)->get_key());
    EXPECT_EQ(OB_STATE_NOT_MATCH, capture.finish());
    EXPECT_EQ(OB_NOT_INIT, capture.scan(0));
  }
}

TEST_F(TestIVFSSTable, empty_capture_source_keeps_latest_transaction_metadata)
{
  ObIvfSSTable data;
  ObIvfSSTable empty;
  for (ObIvfSSTable *table : {&data, &empty}) {
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam create_param;
    ASSERT_EQ(OB_SUCCESS, build_empty_sstable_param(
        ObITable::INC_IVF_INDEX_SSTABLE, storage_schema, create_param));
    const int64_t commit_version = table == &empty ? 250 : 150;
    ObUncommitTxDesc tx_desc(commit_version, commit_version, ObUncommitTxDesc::COMMIT_VERSION);
    ASSERT_EQ(OB_SUCCESS, create_param.uncommit_tx_info_.push_back(tx_desc));
    create_param.uncommit_tx_info_.set_info_status_record_seq();
    ASSERT_EQ(OB_SUCCESS, table->init(create_param, &allocator_));
  }
  SCN filled_tx_scn;
  ASSERT_EQ(OB_SUCCESS, filled_tx_scn.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, data.backfill_commit_version(allocator_, 150, filled_tx_scn));
  ASSERT_EQ(OB_SUCCESS, empty.backfill_commit_version(allocator_, 250, filled_tx_scn));
  data.meta_cache_.data_macro_block_count_ = 1;
  const ObITable::TableKey empty_key = empty.get_key();
  ObTabletMergeDagParam param(INC_MAJOR_MERGE, ObLSID(1001), empty.get_key().tablet_id_, -1);
  ObAsyncIndexMergeCtx merge_ctx(param, allocator_);
  merge_ctx.static_param_.concurrent_cnt_ = 1;
  merge_ctx.parallel_merge_ctx_.concurrent_cnt_ = 1;
  ObTableHandleV2 handle;
  for (ObIvfSSTable *table : {&data, &empty}) {
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(table));
    ASSERT_EQ(OB_SUCCESS, merge_ctx.static_param_.tables_handle_.add_table(handle));
  }
  // Transaction aggregation happens before capture and includes physical empties.
  ASSERT_EQ(OB_SUCCESS, merge_ctx.init_inc_major_uncommit_tx_info());
  ObIVFMergeInputAdapter input(allocator_);
  ASSERT_EQ(OB_SUCCESS, input.source_tables_.assign(merge_ctx.get_tables_handle()));
  ObIVFMergeCaptureCtx capture;
  capture.input_ = &input;
  ASSERT_EQ(OB_SUCCESS, capture.plan_(false));
  ASSERT_EQ(1, capture.slices_.count());
  ASSERT_EQ(1, capture.slices_.at(0)->ranges_.count());
  EXPECT_EQ(0, capture.slices_.at(0)->ranges_.at(0).table_idx_);
  EXPECT_EQ(2, merge_ctx.get_tables_handle().get_count());
  EXPECT_EQ(2, input.source_tables_.get_count());
  EXPECT_EQ(empty_key, input.source_tables_.get_table(1)->get_key());
  ObUncommitTxDesc tx_desc;
  ASSERT_EQ(OB_SUCCESS, merge_ctx.uncommit_tx_info_[0].get_uncommit_tx_desc(tx_desc, 0));
  EXPECT_TRUE(tx_desc.is_commit_version());
  EXPECT_EQ(250, tx_desc.tx_id_);
  EXPECT_EQ(250, tx_desc.commit_version_);
  EXPECT_EQ(250, empty.get_upper_trans_version());
  EXPECT_EQ(filled_tx_scn, empty.get_filled_tx_scn());
}

TEST_F(TestIVFSSTable, ivf_sstable_deep_copy_keeps_type_and_meta)
{
  ObIvfSSTable sstable;
  ObIStorageMetaObj *copied_obj = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t deep_copy_size = sstable.get_deep_copy_size();
  char *buf = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable.deep_copy(buf, deep_copy_size, copied_obj));
  ASSERT_NE(nullptr, copied_obj);

  ObIvfSSTable *copied_sstable = static_cast<ObIvfSSTable *>(copied_obj);
  ASSERT_TRUE(copied_sstable->is_ivf_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, copied_sstable->get_meta_type());
  ASSERT_TRUE(copied_sstable->get_ivf_meta().is_valid());
  ASSERT_NE(&sstable.get_ivf_meta(), &copied_sstable->get_ivf_meta());
  ASSERT_EQ(128, copied_sstable->get_ivf_meta().dim_);
  ASSERT_TRUE(sstable.get_fht_rotator().is_inited());
  ASSERT_TRUE(copied_sstable->get_fht_rotator().is_inited());
  ASSERT_NE(sstable.get_fht_rotator().signs(),
      copied_sstable->get_fht_rotator().signs());
  ASSERT_EQ(sstable.get_fht_rotator().signs_size(),
      copied_sstable->get_fht_rotator().signs_size());
  for (int64_t i = 0; i < sstable.get_fht_rotator().signs_size(); ++i) {
    ASSERT_EQ(sstable.get_fht_rotator().signs()[i],
        copied_sstable->get_fht_rotator().signs()[i]);
  }
}

TEST_F(TestIVFSSTable, disk_bbq_format_v1_deep_copy_and_serialize_round_trip)
{
  ObIvfSSTable sstable;
  ObIvfSSTable decoded_sstable;
  ObIStorageMetaObj *copied_obj = nullptr;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  ASSERT_EQ(OB_SUCCESS, build_disk_bbq_ivf_sstable(sstable));
  const ObIvfSSTableMeta &source_meta = sstable.get_ivf_meta();
  ASSERT_TRUE(source_meta.is_valid());
  ASSERT_EQ(IVF_SSTABLE_FORMAT_VERSION, source_meta.format_version_);

  const int64_t deep_copy_size = sstable.get_deep_copy_size();
  char *deep_copy_buf = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, deep_copy_buf);
  ASSERT_EQ(OB_SUCCESS,
            sstable.deep_copy(deep_copy_buf, deep_copy_size, copied_obj));
  ASSERT_NE(nullptr, copied_obj);
  const ObIvfSSTableMeta &copied_meta =
      static_cast<ObIvfSSTable *>(copied_obj)->get_ivf_meta();
  ASSERT_TRUE(copied_meta.is_valid());
  EXPECT_EQ(source_meta.format_version_, copied_meta.format_version_);
  EXPECT_EQ(source_meta.nbits_, copied_meta.nbits_);
  EXPECT_EQ(source_meta.fht_algo_version_, copied_meta.fht_algo_version_);

  const int64_t serialize_size = sstable.get_serialize_size(data_version);
  char *serialize_buf = static_cast<char *>(allocator_.alloc(serialize_size));
  ASSERT_NE(nullptr, serialize_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            sstable.serialize(data_version, serialize_buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            decoded_sstable.deserialize(
                allocator_, serialize_buf, serialize_size, pos));
  ASSERT_EQ(serialize_size, pos);
  const ObIvfSSTableMeta &decoded_meta = decoded_sstable.get_ivf_meta();
  ASSERT_TRUE(decoded_meta.is_valid());
  EXPECT_EQ(source_meta.format_version_, decoded_meta.format_version_);
  EXPECT_EQ(source_meta.nbits_, decoded_meta.nbits_);
  EXPECT_EQ(source_meta.fht_algo_version_, decoded_meta.fht_algo_version_);
}

TEST_F(TestIVFSSTable, deep_copy_rejects_invalid_buffer)
{
  ObIvfSSTable sstable;
  ObIStorageMetaObj *copied_obj = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t deep_copy_size = sstable.get_deep_copy_size();
  ASSERT_GT(deep_copy_size, 0);
  char *buf = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, buf);

  copied_obj = &sstable;
  ASSERT_EQ(OB_INVALID_ARGUMENT, sstable.deep_copy(nullptr, deep_copy_size, copied_obj));
  ASSERT_EQ(nullptr, copied_obj);

  copied_obj = &sstable;
  ASSERT_EQ(OB_INVALID_ARGUMENT, sstable.deep_copy(buf, deep_copy_size - 1, copied_obj));
  ASSERT_EQ(nullptr, copied_obj);

  ASSERT_EQ(OB_SUCCESS, sstable.deep_copy(buf, deep_copy_size, copied_obj));
  ASSERT_NE(nullptr, copied_obj);
  ObIvfSSTable *copied_sstable = static_cast<ObIvfSSTable *>(copied_obj);
  ASSERT_EQ(sstable.get_ivf_meta().index_type_, copied_sstable->get_ivf_meta().index_type_);
  ASSERT_EQ(sstable.get_ivf_meta().dist_type_, copied_sstable->get_ivf_meta().dist_type_);
  ASSERT_EQ(sstable.get_ivf_meta().dim_, copied_sstable->get_ivf_meta().dim_);
  ASSERT_EQ(sstable.get_ivf_meta().format_version_,
      copied_sstable->get_ivf_meta().format_version_);
}

TEST_F(TestIVFSSTable, ivf_sstable_serialize_deserialize_round_trip)
{
  ObIvfSSTable sstable;
  ObIvfSSTable decoded_sstable;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t buf_len = sstable.get_serialize_size(data_version);
  ASSERT_GT(buf_len, 0);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(decoded_sstable.is_ivf_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, decoded_sstable.get_meta_type());
  ASSERT_TRUE(decoded_sstable.get_ivf_meta().is_valid());
  ASSERT_EQ(128, decoded_sstable.get_ivf_meta().dim_);
  ASSERT_EQ(IVF_SSTABLE_FORMAT_VERSION,
      decoded_sstable.get_ivf_meta().format_version_);
  ASSERT_TRUE(decoded_sstable.get_fht_rotator().is_inited());
  ASSERT_EQ(sstable.get_fht_rotator().signs_size(),
      decoded_sstable.get_fht_rotator().signs_size());
  for (int64_t i = 0; i < sstable.get_fht_rotator().signs_size(); ++i) {
    ASSERT_EQ(sstable.get_fht_rotator().signs()[i],
        decoded_sstable.get_fht_rotator().signs()[i]);
  }
}

TEST_F(TestIVFSSTable, remote_inc_commit_rpc_preserves_ivf_type_and_meta)
{
  ObIvfSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inc_ivf_sstable(sstable));
  transaction::ObTxDesc tx_desc;
  tx_desc.tenant_id_ = MTL_ID();
  tx_desc.tx_id_ = transaction::ObTransID(2001);
  tx_desc.addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 8888);
  tx_desc.sess_id_ = 1;
  tx_desc.cluster_id_ = 1;
  tx_desc.cluster_version_ = DATA_CURRENT_VERSION;
  tx_desc.state_ = transaction::ObTxDesc::State::IDLE;

  obrpc::ObRpcRemoteWriteDDLIncCommitLogArg source;
  ASSERT_EQ(ObITable::TableType::MAX_TABLE_TYPE, source.table_type_);
  ASSERT_EQ(OB_SUCCESS, source.init(MTL_ID(), ObLSID(1), sstable.get_key().tablet_id_,
      ObTabletID(), ObTabletID(), &tx_desc, ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
      tx_desc.tx_id_, transaction::ObTxSEQ(3001, 0), 100, DATA_CURRENT_VERSION, SCN::base_scn()));
  ASSERT_EQ(OB_SUCCESS, source.set_ss_inc_major(&sstable, nullptr, nullptr));
  ASSERT_EQ(ObITable::TableType::INC_IVF_INDEX_SSTABLE, source.table_type_);
  ASSERT_FALSE(source.is_co_sstable_);
  ASSERT_FALSE(source.is_inv_sstable_);

  const int64_t size = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  ASSERT_EQ(size, pos);
  obrpc::ObRpcRemoteWriteDDLIncCommitLogArg decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, size, pos));
  ASSERT_EQ(size, pos);
  ASSERT_EQ(ObITable::TableType::INC_IVF_INDEX_SSTABLE, decoded.table_type_);
  ASSERT_FALSE(decoded.is_co_sstable_);
  ASSERT_FALSE(decoded.is_inv_sstable_);

  // Reject the base decoder instead of silently dropping the IVF metadata.
  const ObString &payload = decoded.data_inc_major_buffer_;
  ObSSTable base;
  pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, base.deserialize(allocator_, payload.ptr(), payload.length(), pos));
  ASSERT_EQ(0, pos);
  ASSERT_FALSE(base.is_valid());

  ObIvfSSTable restored;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, restored.deserialize(allocator_, payload.ptr(), payload.length(), pos));
  ASSERT_EQ(payload.length(), pos);
  ASSERT_TRUE(restored.is_valid());
  ASSERT_TRUE(restored.is_inc_ivf_sstable());
  EXPECT_EQ(sstable.get_ivf_meta().dim_, restored.get_ivf_meta().dim_);
  EXPECT_EQ(sstable.get_ivf_meta().index_type_, restored.get_ivf_meta().index_type_);
  EXPECT_EQ(sstable.get_ivf_meta().dist_type_, restored.get_ivf_meta().dist_type_);
  EXPECT_EQ(sstable.get_ivf_meta().format_version_, restored.get_ivf_meta().format_version_);
  EXPECT_EQ(sstable.get_ivf_meta().nbits_, restored.get_ivf_meta().nbits_);
  EXPECT_EQ(sstable.get_ivf_meta().fht_algo_version_, restored.get_ivf_meta().fht_algo_version_);
}

TEST_F(TestIVFSSTable, remote_inc_commit_rpc_reads_legacy_type_flags)
{
  ObSSTable normal;
  ObInvSSTable inv;
  ObTableHandleV2 co_handle;
  ObSSTable *co = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_normal_sstable(normal));
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(inv));
  ASSERT_EQ(OB_SUCCESS, build_co_sstable(co_handle, co));
  const ObSSTable *tables[] = {&normal, &inv, co};
  transaction::ObTxDesc tx_desc;
  tx_desc.tenant_id_ = MTL_ID();
  tx_desc.tx_id_ = transaction::ObTransID(2002);
  tx_desc.addr_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 8888);
  tx_desc.sess_id_ = 1;
  tx_desc.cluster_id_ = 1;
  tx_desc.cluster_version_ = DATA_CURRENT_VERSION;
  tx_desc.state_ = transaction::ObTxDesc::State::IDLE;
  for (const ObSSTable *table : tables) {
    obrpc::ObRpcRemoteWriteDDLIncCommitLogArg source;
    ASSERT_EQ(OB_SUCCESS, source.init(MTL_ID(), ObLSID(1), table->get_key().tablet_id_,
        ObTabletID(), ObTabletID(), &tx_desc, ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
        tx_desc.tx_id_, transaction::ObTxSEQ(3002, 0), 100, DATA_CURRENT_VERSION, SCN::base_scn()));
    ASSERT_EQ(OB_SUCCESS, source.set_ss_inc_major(table, nullptr, nullptr));
    ASSERT_EQ(table->get_key().table_type_, source.table_type_);
    const int64_t size = source.get_serialize_size_();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, source.serialize_(buf, size, pos));
    ASSERT_EQ(size, pos);
    // Remove only the appended table type from the unframed body to model an old sender.
    const int64_t legacy_size = size - serialization::encoded_length(source.table_type_);
    obrpc::ObRpcRemoteWriteDDLIncCommitLogArg decoded;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize_(buf, legacy_size, pos));
    ASSERT_EQ(legacy_size, pos);
    EXPECT_EQ(ObITable::TableType::MAX_TABLE_TYPE, decoded.table_type_);
    EXPECT_EQ(table->is_co_sstable(), decoded.is_co_sstable_);
    EXPECT_EQ(table->is_inv_sstable(), decoded.is_inv_sstable_);
    EXPECT_EQ(source.data_inc_major_buffer_, decoded.data_inc_major_buffer_);
  }
}

class TestIVFCommitLogReplay : public TestIVFSSTable
{
protected:
  static void SetUpTestCase()
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
    ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(
        OB_SERVER_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle_));
  }

  static void TearDownTestCase()
  {
    ls_handle_.reset();
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->remove_ls(ObLSID(TEST_LS_ID)));
  }

  void TearDown() override
  {
    EventItem reset_event;
    EXPECT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_CHECK_EXIST", reset_event));
    EXPECT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_LINK", reset_event));
  }

  static const int64_t TEST_LS_ID = 1002;
  static const int64_t TEST_TABLE_ID = 500002;
  static ObLSHandle ls_handle_;
};

const int64_t TestIVFCommitLogReplay::TEST_LS_ID;
const int64_t TestIVFCommitLogReplay::TEST_TABLE_ID;
ObLSHandle TestIVFCommitLogReplay::ls_handle_;

TEST_F(TestIVFCommitLogReplay, ivf_commit_log_round_trip_replay_and_shared_table_store)
{
  ObTabletHandle tablet_handle;
  ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam create_param;
  ObIvfSSTable source_sstable;
  const transaction::ObTransID trans_id(100);
  const transaction::ObTxSEQ seq_no(200, 0);
  SCN start_scn;
  SCN replay_scn;
  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_tx(300));
  const ObTabletID ivf_tablet_id(200002);
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, TEST_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(
      ls_handle_, ivf_tablet_id, table_schema, allocator_,
      ObTabletStatus::NORMAL, SCN::min_scn(), tablet_handle));
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_EQ(OB_SUCCESS, create_param.init_for_empty_major_sstable(
      ivf_tablet_id, storage_schema, 200, -1, false, false));
  create_param.table_key_.table_type_ = ObITable::TableType::INC_IVF_INDEX_SSTABLE;
  create_param.table_key_.scn_range_.start_scn_ = start_scn;
  ASSERT_EQ(OB_SUCCESS, create_param.table_key_.scn_range_.end_scn_.convert_for_tx(200));
  create_param.rec_scn_ = SCN::plus(start_scn, 1);
  create_param.ivf_meta_.dim_ = 128;
  create_param.ivf_meta_.index_type_ = VIAT_DISK_BBQ;
  create_param.ivf_meta_.dist_type_ = VIDA_L2;
  create_param.ivf_meta_.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
  create_param.ivf_meta_.nbits_ = 1;
  create_param.ivf_meta_.fht_algo_version_ =
      quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
  ASSERT_EQ(OB_SUCCESS, create_param.uncommit_tx_info_.push_back(
      compaction::ObUncommitTxDesc(trans_id.get_id(), seq_no.cast_to_int())));
  create_param.uncommit_tx_info_.set_info_status_record_seq();
  ASSERT_EQ(OB_SUCCESS, source_sstable.init(create_param, &allocator_));
  // The fixture needs metadata only; make it nonempty to exercise the publish path.
  source_sstable.meta_->basic_meta_.data_macro_block_count_ = 1;
  source_sstable.meta_cache_.data_macro_block_count_ = 1;
  source_sstable.meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  source_sstable.meta_cache_.upper_trans_version_ = INT64_MAX;
  ASSERT_TRUE(source_sstable.is_valid());
  ASSERT_FALSE(source_sstable.is_empty());

  ObDDLIncLogBasic log_basic;
  ObDDLIncCommitLog source_log;
  ObDDLIncCommitLog decoded_log;
  ASSERT_EQ(OB_SUCCESS, log_basic.init(
      ivf_tablet_id, ObTabletID(), ObTabletID(), ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR,
      trans_id, seq_no, 200, DATA_CURRENT_VERSION));
  ASSERT_EQ(OB_SUCCESS, source_log.init(log_basic, false, start_scn));
  ASSERT_EQ(OB_SUCCESS, source_log.set_ss_inc_major(
      &source_sstable, nullptr, nullptr, false /*need_check_ddl_checkpoint*/));
  ASSERT_EQ(ObITable::TableType::INC_IVF_INDEX_SSTABLE, source_log.get_table_type());
  const int64_t log_size = source_log.get_serialize_size();
  char *log_buf = static_cast<char *>(allocator_.alloc(log_size));
  ASSERT_NE(nullptr, log_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source_log.serialize(log_buf, log_size, pos));
  ASSERT_EQ(log_size, pos);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_log.deserialize(log_buf, log_size, pos));
  ASSERT_EQ(log_size, pos);
  ASSERT_EQ(ObITable::TableType::INC_IVF_INDEX_SSTABLE, decoded_log.get_table_type());
  ASSERT_FALSE(decoded_log.is_co_sstable());
  ASSERT_EQ(start_scn, decoded_log.get_start_scn());
  const ObDDLIncLogBasic &decoded_basic = decoded_log.get_log_basic();
  ASSERT_EQ(ivf_tablet_id, decoded_basic.get_tablet_id());
  ASSERT_EQ(trans_id, decoded_basic.get_trans_id());
  ASSERT_EQ(seq_no, decoded_basic.get_seq_no());

  struct ReplayModeGuard {
    ReplayModeGuard() : mode_(GCTX.startup_mode_), role_(MTL_GET_TENANT_ROLE_CACHE())
    {
      GCTX.startup_mode_ = share::ObServerMode::SHARED_STORAGE_MODE;
      MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::STANDBY_TENANT);
    }
    ~ReplayModeGuard()
    {
      MTL_SET_TENANT_ROLE_CACHE(role_);
      GCTX.startup_mode_ = mode_;
    }
    share::ObServerMode mode_;
    share::ObTenantRole::Role role_;
  } mode_guard;
  ObDDLIncMajorCommitReplayExecutor replay_executor;
  ASSERT_EQ(OB_SUCCESS, replay_executor.init(
      ls_handle_.get_ls(), decoded_basic.get_tablet_id(), replay_scn,
      decoded_basic.get_trans_id(), decoded_basic.get_seq_no(), decoded_basic.get_snapshot_version(),
      decoded_basic.get_data_format_version(), decoded_log.is_rollback(), decoded_log.is_co_sstable(),
      decoded_log.get_table_type(), decoded_log.get_data_inc_major_buffer(), decoded_log.get_start_scn(),
      decoded_log.need_check_ddl_checkpoint()));
  // The unit fixture has no shared metadata service. Stop at its boundary only
  // after the real standby replay has deserialized and classified the IVF object.
  EventItem replay_event;
  replay_event.error_code_ = OB_BREAK_BY_TEST;
  replay_event.occur_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_CHECK_EXIST", replay_event));
  ASSERT_EQ(OB_BREAK_BY_TEST, replay_executor.do_replay_(tablet_handle));
  EventItem reset_event;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_REPLAY_INC_MAJOR_CHECK_EXIST", reset_event));
  // Losing the table type reproduces the original incomplete deserialize, before publication.
  replay_executor.table_type_ = ObITable::TableType::MAX_TABLE_TYPE;
  bool need_publish = true;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      replay_executor.deserialize_and_update_ss_inc_major(tablet_handle, need_publish));

  // Feed the very same commit payload through the production shared table-store
  // ADD and retry paths, independently of the absent remote metadata service.
  ObIvfSSTable restored_sstable;
  ObSSTable *restored = &restored_sstable;
  ASSERT_EQ(OB_SUCCESS, ObIncDDLMergeTaskUtils::deserialize_inc_major_from_string(
      allocator_, decoded_log.get_data_inc_major_buffer(), restored));
  ObUpdateTableStoreParam update_param;
  ASSERT_EQ(OB_SUCCESS, ObSSDDLUtil::build_update_table_store_param_(
      *ls_handle_.get_ls(), *restored, storage_schema, DATA_CURRENT_VERSION, update_param));
  ASSERT_EQ(ObIncMajorSSTableAction::ADD, update_param.inc_major_sstable_action_);
  ObTabletTableStore empty_store;
  ObTabletTableStore shared_store;
  ObTabletTableStore retry_store;
  ASSERT_EQ(OB_SUCCESS, shared_store.inner_build_inc_major_tables_for_ss(allocator_, update_param, empty_store, 0));
  ASSERT_EQ(1, shared_store.get_inc_major_sstables().count());
  ASSERT_EQ(OB_SSTABLE_EXIST, retry_store.inner_build_inc_major_tables_for_ss(allocator_, update_param, shared_store, 0));
  ASSERT_EQ(0, retry_store.get_inc_major_sstables().count());
  ASSERT_EQ(1, shared_store.get_inc_major_sstables().count());
  const ObSSTable *stored = shared_store.get_inc_major_sstables().at(0);
  ASSERT_NE(nullptr, stored);
  ASSERT_TRUE(stored->is_inc_ivf_sstable());
  ASSERT_EQ(source_sstable.get_key(), stored->get_key());
  ASSERT_EQ(ObStorageMetaValue::IVF_SSTABLE, stored->get_meta_type());
  const ObIvfSSTable &stored_ivf = *static_cast<const ObIvfSSTable *>(stored);
  EXPECT_EQ(source_sstable.get_ivf_meta().dim_, stored_ivf.get_ivf_meta().dim_);
  EXPECT_EQ(source_sstable.get_ivf_meta().index_type_, stored_ivf.get_ivf_meta().index_type_);
  EXPECT_EQ(source_sstable.get_ivf_meta().dist_type_, stored_ivf.get_ivf_meta().dist_type_);
  EXPECT_EQ(source_sstable.get_ivf_meta().format_version_, stored_ivf.get_ivf_meta().format_version_);
  EXPECT_EQ(source_sstable.get_ivf_meta().nbits_, stored_ivf.get_ivf_meta().nbits_);
  EXPECT_EQ(source_sstable.get_ivf_meta().fht_algo_version_, stored_ivf.get_ivf_meta().fht_algo_version_);
  EXPECT_TRUE(stored_ivf.get_fht_rotator().is_inited());
}

TEST_F(TestIVFSSTable, deserialize_rejects_invalid_ivf_meta)
{
  ObIvfSSTable sstable;
  ObIvfSSTable decoded_sstable;
  ObIvfSSTableMeta invalid_meta;
  const uint64_t data_version = DATA_CURRENT_VERSION;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  invalid_meta.dim_ = 1;
  const int64_t base_len = sstable.ObSSTable::get_serialize_size(data_version);
  const int64_t buf_len = base_len + invalid_meta.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, sstable.ObSSTable::serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(base_len, pos);
  ASSERT_EQ(OB_SUCCESS, invalid_meta.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
}

TEST_F(TestIVFSSTable, deserialize_rejects_non_ivf_and_truncated_meta)
{
  const uint64_t data_version = DATA_CURRENT_VERSION;

  ObSSTable normal_sstable;
  ObIvfSSTable non_ivf_decoded_sstable;
  ASSERT_EQ(OB_SUCCESS, build_normal_sstable(normal_sstable));
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(non_ivf_decoded_sstable));
  ASSERT_TRUE(non_ivf_decoded_sstable.is_loaded());
  ASSERT_TRUE(non_ivf_decoded_sstable.get_ivf_meta().is_valid());

  const int64_t normal_buf_len = normal_sstable.get_serialize_size(data_version);
  ASSERT_GT(normal_buf_len, 0);
  char *normal_buf = static_cast<char *>(allocator_.alloc(normal_buf_len));
  ASSERT_NE(nullptr, normal_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      normal_sstable.serialize(data_version, normal_buf, normal_buf_len, pos));
  ASSERT_EQ(normal_buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      non_ivf_decoded_sstable.deserialize(allocator_, normal_buf, normal_buf_len, pos));
  ASSERT_FALSE(non_ivf_decoded_sstable.is_loaded());
  ASSERT_FALSE(non_ivf_decoded_sstable.is_ivf_sstable());
  ASSERT_EQ(ObITable::TableType::MAX_TABLE_TYPE,
      non_ivf_decoded_sstable.get_key().table_type_);
  ASSERT_FALSE(non_ivf_decoded_sstable.get_ivf_meta().is_valid());

  ObIvfSSTable ivf_sstable;
  ObIvfSSTable truncated_decoded_sstable;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(ivf_sstable));
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(truncated_decoded_sstable));
  ASSERT_TRUE(truncated_decoded_sstable.is_loaded());
  ASSERT_TRUE(truncated_decoded_sstable.get_ivf_meta().is_valid());

  const int64_t ivf_buf_len = ivf_sstable.get_serialize_size(data_version);
  const int64_t truncated_data_len =
      ivf_sstable.ObSSTable::get_serialize_size(data_version)
      + ivf_sstable.get_ivf_meta().get_serialize_size() - 1;
  ASSERT_EQ(ivf_buf_len - 1, truncated_data_len);
  char *ivf_buf = static_cast<char *>(allocator_.alloc(ivf_buf_len));
  ASSERT_NE(nullptr, ivf_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ivf_sstable.serialize(data_version, ivf_buf, ivf_buf_len, pos));
  ASSERT_EQ(ivf_buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_DESERIALIZE_ERROR,
      truncated_decoded_sstable.deserialize(allocator_, ivf_buf, truncated_data_len, pos));
  ASSERT_FALSE(truncated_decoded_sstable.is_loaded());
  ASSERT_FALSE(truncated_decoded_sstable.is_ivf_sstable());
  ASSERT_EQ(ObITable::TableType::MAX_TABLE_TYPE,
      truncated_decoded_sstable.get_key().table_type_);
  ASSERT_FALSE(truncated_decoded_sstable.get_ivf_meta().is_valid());
}

TEST_F(TestIVFSSTable, full_serialize_round_trip)
{
  ObIvfSSTable sstable;
  ObIvfSSTable decoded_sstable;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t buf_len = sstable.get_full_serialize_size(data_version);
  ASSERT_GT(buf_len, 0);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  ASSERT_EQ(OB_SUCCESS, sstable.serialize_full_table(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(decoded_sstable.is_ivf_sstable());
  ASSERT_TRUE(decoded_sstable.get_ivf_meta().is_valid());
  ASSERT_EQ(128, decoded_sstable.get_ivf_meta().dim_);
}

TEST_F(TestIVFSSTable, storage_meta_bypass_processor_restores_ivf_sstable)
{
  ObIvfSSTable sstable;
  ObArenaAllocator handle_arena;
  ObSafeArenaAllocator safe_arena(handle_arena);
  ObStorageMetaValueHandle handle;
  const ObStorageMetaValue *value = nullptr;
  ObSSTable *decoded_sstable = nullptr;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t buf_len = sstable.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(OB_SUCCESS, handle.new_value(handle_arena));
  ASSERT_EQ(OB_SUCCESS, ObStorageMetaValue::bypass_process_storage_meta<ObIvfSSTable>(
      ObStorageMetaValue::MetaType::IVF_SSTABLE, safe_arena, handle, buf, buf_len));
  ASSERT_NE(nullptr, handle.get_cache_value());
  value = handle.get_cache_value()->value_;
  ASSERT_NE(nullptr, value);
  ASSERT_EQ(OB_SUCCESS, value->get_sstable(decoded_sstable));
  ASSERT_NE(nullptr, decoded_sstable);
  ASSERT_TRUE(decoded_sstable->is_ivf_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, decoded_sstable->get_meta_type());
  ASSERT_EQ(128, static_cast<ObIvfSSTable *>(decoded_sstable)->get_ivf_meta().dim_);
}

TEST_F(TestIVFSSTable, storage_meta_real_processor_restores_ivf_sstable)
{
  ObIvfSSTable sstable;
  ObArenaAllocator handle_allocator;
  ObStorageMetaValueHandle processor_handle;
  ObStorageMetaValueHandle lookup_handle;
  ObSSTable *decoded_sstable = nullptr;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t buf_len = sstable.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  const MacroBlockId block_id = build_tiered_meta_block_id(5101);
  ASSERT_TRUE(block_id.is_valid());
  ASSERT_TRUE(block_id.is_shared_tablet_sub_meta_in_table());
  ObMetaDiskAddr meta_addr;
  ASSERT_EQ(OB_SUCCESS, meta_addr.set_block_addr(
      block_id, 0, buf_len, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  const ObStorageMetaKey key(MTL_ID(), meta_addr);
  ASSERT_TRUE(key.is_valid());

  ASSERT_EQ(&ObStorageMetaValue::process_ivf_sstable,
      ObStorageMetaValue::processor[ObStorageMetaValue::IVF_SSTABLE]);
  ASSERT_EQ(OB_SUCCESS, processor_handle.new_value(handle_allocator));
  ASSERT_EQ(OB_SUCCESS, ObStorageMetaValue::process_ivf_sstable(
      processor_handle, key, buf, buf_len, nullptr));

  ASSERT_EQ(OB_SUCCESS, lookup_handle.new_value(handle_allocator));
  ASSERT_EQ(OB_SUCCESS, OB_STORE_CACHE.get_storage_meta_cache().get(
      key,
      lookup_handle.get_cache_value()->value_,
      lookup_handle.get_cache_value()->cache_handle_));
  const ObStorageMetaValue *value = lookup_handle.get_cache_value()->value_;
  ASSERT_NE(nullptr, value);
  ASSERT_EQ(ObStorageMetaValue::IVF_SSTABLE, value->type_);
  ASSERT_EQ(OB_SUCCESS, value->get_sstable(decoded_sstable));
  ASSERT_NE(nullptr, decoded_sstable);
  ASSERT_TRUE(decoded_sstable->is_ivf_sstable());
  const ObIvfSSTableMeta &expected_meta = sstable.get_ivf_meta();
  const ObIvfSSTableMeta &actual_meta =
      static_cast<ObIvfSSTable *>(decoded_sstable)->get_ivf_meta();
  ASSERT_TRUE(actual_meta.is_valid());
  ASSERT_EQ(expected_meta.flags_, actual_meta.flags_);
  ASSERT_EQ(expected_meta.dim_, actual_meta.dim_);
  ASSERT_EQ(expected_meta.index_type_, actual_meta.index_type_);
  ASSERT_EQ(expected_meta.dist_type_, actual_meta.dist_type_);

  lookup_handle.reset();
  processor_handle.reset();
  ASSERT_EQ(OB_SUCCESS, OB_STORE_CACHE.get_storage_meta_cache().erase(key));
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestIVFSSTable, prewarm_classifies_ivf_and_uses_real_processor)
{
  ObIvfSSTable sstable;
  ObArenaAllocator handle_allocator;
  ObStorageMetaValueHandle handle;
  hash::ObHashSet<MacroBlockId> normal_sst_ids;
  hash::ObHashSet<MacroBlockId> co_sst_ids;
  hash::ObHashSet<MacroBlockId> inv_sst_ids;
  hash::ObHashSet<MacroBlockId> ivf_sst_ids;
  const ObMemAttr mem_attr(MTL_ID(), "TstIvfPrewarm");
  const int64_t bucket_count = 16;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;
  int64_t prewarm_sst_cnt = 0;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  const int64_t buf_len = sstable.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  const MacroBlockId block_id = build_tiered_meta_block_id(5102);
  ASSERT_TRUE(block_id.is_valid());
  ASSERT_TRUE(block_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_EQ(OB_SUCCESS, normal_sst_ids.create(bucket_count, mem_attr));
  ASSERT_EQ(OB_SUCCESS, co_sst_ids.create(bucket_count, mem_attr));
  ASSERT_EQ(OB_SUCCESS, inv_sst_ids.create(bucket_count, mem_attr));
  ASSERT_EQ(OB_SUCCESS, ivf_sst_ids.create(bucket_count, mem_attr));
  ASSERT_EQ(OB_SUCCESS, ivf_sst_ids.set_refactored(block_id));
  ASSERT_EQ(OB_SUCCESS, handle.new_value(handle_allocator));

  const ObString meta_raw_buf(buf_len, buf);
  ObStorageMetaCache &cache = OB_STORE_CACHE.get_storage_meta_cache();
  ASSERT_EQ(OB_SUCCESS, cache.process_one_sstable_meta(
      block_id,
      meta_raw_buf,
      normal_sst_ids,
      co_sst_ids,
      inv_sst_ids,
      ivf_sst_ids,
      prewarm_sst_cnt,
      handle));
  ASSERT_EQ(1, prewarm_sst_cnt);
  ASSERT_NE(nullptr, handle.get_cache_value()->value_);
  ASSERT_EQ(ObStorageMetaValue::IVF_SSTABLE,
      handle.get_cache_value()->value_->type_);

  ObSSTable *decoded_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      handle.get_cache_value()->value_->get_sstable(decoded_sstable));
  ASSERT_NE(nullptr, decoded_sstable);
  ASSERT_TRUE(decoded_sstable->is_ivf_sstable());
  const ObIvfSSTableMeta &expected_meta = sstable.get_ivf_meta();
  const ObIvfSSTableMeta &actual_meta =
      static_cast<ObIvfSSTable *>(decoded_sstable)->get_ivf_meta();
  ASSERT_TRUE(actual_meta.is_valid());
  ASSERT_EQ(expected_meta.flags_, actual_meta.flags_);
  ASSERT_EQ(expected_meta.dim_, actual_meta.dim_);
  ASSERT_EQ(expected_meta.index_type_, actual_meta.index_type_);
  ASSERT_EQ(expected_meta.dist_type_, actual_meta.dist_type_);

  ObMetaDiskAddr meta_addr;
  ASSERT_EQ(OB_SUCCESS, meta_addr.set_block_addr(
      block_id, 0, buf_len, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  const ObStorageMetaKey key(MTL_ID(), meta_addr);
  handle.reset();
  ASSERT_EQ(OB_SUCCESS, cache.erase(key));
  normal_sst_ids.destroy();
  co_sst_ids.destroy();
  inv_sst_ids.destroy();
  ivf_sst_ids.destroy();
}

TEST_F(TestIVFSSTable, table_store_classifies_normal_co_inv_and_ivf_without_cross_talk)
{
  ObSSTable normal_sstable;
  ObInvSSTable inv_sstable;
  ObIvfSSTable ivf_sstable;
  ObTableHandleV2 co_handle;
  ObSSTable *co_sstable = nullptr;
  ObTabletTableStore normal_table_store;
  ObTabletTableStore co_table_store;
  ObTabletTableStore inv_table_store;
  ObTabletTableStore ivf_table_store;

  ASSERT_EQ(OB_SUCCESS, build_normal_sstable(normal_sstable));
  ASSERT_EQ(OB_SUCCESS, build_co_sstable(co_handle, co_sstable));
  ASSERT_NE(nullptr, co_sstable);
  ASSERT_TRUE(co_sstable->is_co_sstable());
  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(inv_sstable));
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(ivf_sstable));

  const MacroBlockId normal_block_id = build_tiered_meta_block_id(5201);
  const MacroBlockId co_block_id = build_tiered_meta_block_id(5202);
  const MacroBlockId inv_block_id = build_tiered_meta_block_id(5203);
  const MacroBlockId ivf_block_id = build_tiered_meta_block_id(5204);
  ASSERT_TRUE(normal_block_id.is_valid());
  ASSERT_TRUE(co_block_id.is_valid());
  ASSERT_TRUE(inv_block_id.is_valid());
  ASSERT_TRUE(ivf_block_id.is_valid());
  ObMetaDiskAddr normal_addr;
  ObMetaDiskAddr co_addr;
  ObMetaDiskAddr inv_addr;
  ObMetaDiskAddr ivf_addr;
  ASSERT_EQ(OB_SUCCESS, normal_addr.set_block_addr(
      normal_block_id, 0, 4096, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_TRUE(normal_addr.is_disked());
  ASSERT_TRUE(normal_block_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_EQ(OB_SUCCESS, normal_sstable.set_addr(normal_addr));
  ASSERT_EQ(OB_SUCCESS, co_addr.set_block_addr(
      co_block_id, 0, 4096, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_TRUE(co_addr.is_disked());
  ASSERT_TRUE(co_block_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_EQ(OB_SUCCESS, co_sstable->set_addr(co_addr));
  ASSERT_EQ(OB_SUCCESS, inv_addr.set_block_addr(
      inv_block_id, 0, 4096, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_TRUE(inv_addr.is_disked());
  ASSERT_TRUE(inv_block_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_EQ(OB_SUCCESS, inv_sstable.set_addr(inv_addr));
  ASSERT_EQ(OB_SUCCESS, ivf_addr.set_block_addr(
      ivf_block_id, 0, 4096, ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_TRUE(ivf_addr.is_disked());
  ASSERT_TRUE(ivf_block_id.is_shared_tablet_sub_meta_in_table());
  ASSERT_EQ(OB_SUCCESS, ivf_sstable.set_addr(ivf_addr));

  ASSERT_EQ(OB_SUCCESS, build_single_major_table_store(normal_sstable, normal_table_store));
  ASSERT_EQ(OB_SUCCESS, build_single_major_table_store(*co_sstable, co_table_store));
  ASSERT_EQ(OB_SUCCESS, build_single_major_table_store(inv_sstable, inv_table_store));
  ASSERT_EQ(OB_SUCCESS, build_single_major_table_store(ivf_sstable, ivf_table_store));
  ASSERT_TRUE(normal_table_store.is_valid());
  ASSERT_TRUE(co_table_store.is_valid());
  ASSERT_TRUE(inv_table_store.is_valid());
  ASSERT_TRUE(ivf_table_store.is_valid());

  MacroBlockId copied_block_id;
  ASSERT_EQ(OB_SUCCESS,
      normal_table_store.major_tables_.at(0)->get_addr().get_macro_block_id(copied_block_id));
  ASSERT_EQ(normal_block_id, copied_block_id);
  copied_block_id.reset();
  ASSERT_EQ(OB_SUCCESS,
      co_table_store.major_tables_.at(0)->get_addr().get_macro_block_id(copied_block_id));
  ASSERT_EQ(co_block_id, copied_block_id);
  copied_block_id.reset();
  ASSERT_EQ(OB_SUCCESS,
      inv_table_store.major_tables_.at(0)->get_addr().get_macro_block_id(copied_block_id));
  ASSERT_EQ(inv_block_id, copied_block_id);
  copied_block_id.reset();
  ASSERT_EQ(OB_SUCCESS,
      ivf_table_store.major_tables_.at(0)->get_addr().get_macro_block_id(copied_block_id));
  ASSERT_EQ(ivf_block_id, copied_block_id);

  {
    SSTableBlockIdSetBundle sst_ids;
    ASSERT_EQ(OB_SUCCESS, ObStorageMetaCache::classify_sst_block_ids_by_table_store(
        normal_table_store,
        sst_ids.normal_sst_ids_,
        sst_ids.co_sst_ids_,
        sst_ids.inv_sst_ids_,
        sst_ids.ivf_sst_ids_));
    assert_only_in_expected_set(normal_block_id, NORMAL,
        sst_ids.normal_sst_ids_, sst_ids.co_sst_ids_, sst_ids.inv_sst_ids_, sst_ids.ivf_sst_ids_);
  }
  {
    SSTableBlockIdSetBundle sst_ids;
    ASSERT_EQ(OB_SUCCESS, ObStorageMetaCache::classify_sst_block_ids_by_table_store(
        co_table_store,
        sst_ids.normal_sst_ids_,
        sst_ids.co_sst_ids_,
        sst_ids.inv_sst_ids_,
        sst_ids.ivf_sst_ids_));
    assert_only_in_expected_set(co_block_id, CO,
        sst_ids.normal_sst_ids_, sst_ids.co_sst_ids_, sst_ids.inv_sst_ids_, sst_ids.ivf_sst_ids_);
  }
  {
    SSTableBlockIdSetBundle sst_ids;
    ASSERT_EQ(OB_SUCCESS, ObStorageMetaCache::classify_sst_block_ids_by_table_store(
        inv_table_store,
        sst_ids.normal_sst_ids_,
        sst_ids.co_sst_ids_,
        sst_ids.inv_sst_ids_,
        sst_ids.ivf_sst_ids_));
    assert_only_in_expected_set(inv_block_id, INV,
        sst_ids.normal_sst_ids_, sst_ids.co_sst_ids_, sst_ids.inv_sst_ids_, sst_ids.ivf_sst_ids_);
  }
  {
    SSTableBlockIdSetBundle sst_ids;
    ASSERT_EQ(OB_SUCCESS, ObStorageMetaCache::classify_sst_block_ids_by_table_store(
        ivf_table_store,
        sst_ids.normal_sst_ids_,
        sst_ids.co_sst_ids_,
        sst_ids.inv_sst_ids_,
        sst_ids.ivf_sst_ids_));
    assert_only_in_expected_set(ivf_block_id, IVF,
        sst_ids.normal_sst_ids_, sst_ids.co_sst_ids_, sst_ids.inv_sst_ids_, sst_ids.ivf_sst_ids_);
  }
}
#endif

TEST_F(TestIVFSSTable, sstable_array_round_trip_with_normal_and_ivf_tables)
{
  ObSSTable normal_sstable;
  ObIvfSSTable ivf_sstable;
  ObArray<ObITable *> tables;
  ObSSTableArray sstable_array;
  ObSSTableArray decoded_array;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t pos = 0;

  ASSERT_EQ(OB_SUCCESS, build_normal_sstable(normal_sstable));
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(ivf_sstable));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&normal_sstable));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&ivf_sstable));
  ASSERT_EQ(OB_SUCCESS, sstable_array.init(allocator_, tables));
  ASSERT_EQ(storage::ObSerializeTableType::IVF, sstable_array.serialize_table_type_);

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
  ASSERT_FALSE(decoded_array.at(0)->is_ivf_sstable());
  ASSERT_TRUE(decoded_array.at(1)->is_ivf_sstable());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, decoded_array.at(1)->get_meta_type());
  ASSERT_EQ(128, static_cast<ObIvfSSTable *>(decoded_array.at(1))->get_ivf_meta().dim_);
}

TEST_F(TestIVFSSTable, sstable_array_rejects_mixed_inv_and_ivf)
{
  ObInvSSTable inv_sstable;
  ObIvfSSTable ivf_sstable;
  ObArray<ObITable *> tables;
  ObSSTableArray sstable_array;

  ASSERT_EQ(OB_SUCCESS, build_inv_sstable(inv_sstable));
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(ivf_sstable));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&inv_sstable));
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&ivf_sstable));
  ASSERT_EQ(OB_ERR_UNEXPECTED, sstable_array.init(allocator_, tables));
}

TEST_F(TestIVFSSTable, set_upper_trans_version_reuses_parent)
{
  ObIvfSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_EQ(OB_SUCCESS, sstable.set_upper_trans_version(allocator_, 100));
  ASSERT_EQ(100, sstable.get_upper_trans_version());
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.backfill_commit_version(
      allocator_, 100, share::SCN::min_scn()));
}

TEST_F(TestIVFSSTable, inc_ivf_sstable_supports_set_upper_trans_version)
{
  ObIvfSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_inc_ivf_sstable(sstable));
  ASSERT_TRUE(sstable.is_inc_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, sstable.set_upper_trans_version(allocator_, 150));
  ASSERT_EQ(150, sstable.get_upper_trans_version());
}

TEST_F(TestIVFSSTable, inc_ivf_sstable_supports_backfill_commit_version)
{
  ObIvfSSTable sstable;
  share::SCN filled_tx_scn;
  const int64_t commit_version = 150;

  ASSERT_EQ(OB_SUCCESS, build_inc_ivf_sstable(sstable));
  ASSERT_TRUE(sstable.is_inc_ivf_sstable());
  ASSERT_TRUE(sstable.is_inc_major_type_sstable());
  ASSERT_TRUE(sstable.is_loaded());

  ASSERT_EQ(OB_SUCCESS, filled_tx_scn.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, sstable.backfill_commit_version(
      allocator_, commit_version, filled_tx_scn));
  ASSERT_EQ(commit_version, sstable.get_upper_trans_version());
  ASSERT_EQ(filled_tx_scn, sstable.get_filled_tx_scn());
}

TEST_F(TestIVFSSTable, row_read_rejects_data_block_cache)
{
  ObIvfSSTable sstable;
  ObTableIterParam param;
  ObTableAccessContext context;
  ObDatumRange range;
  ObDatumRowkey rowkey;
  ObArray<ObDatumRowkey> rowkeys;
  ObArray<ObDatumRange> ranges;
  ObStoreRowIterator *const dirty_row_iter = reinterpret_cast<ObStoreRowIterator *>(&sstable);
  ObStoreRowIterator *row_iter = dirty_row_iter;

  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));

  context.query_flag_.set_use_block_cache();
  ASSERT_TRUE(context.query_flag_.is_use_block_cache());
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.scan(param, context, range, row_iter));
  ASSERT_EQ(nullptr, row_iter);
  ASSERT_TRUE(context.query_flag_.is_use_block_cache());

  row_iter = dirty_row_iter;
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.get(param, context, rowkey, row_iter));
  ASSERT_EQ(nullptr, row_iter);
  ASSERT_TRUE(context.query_flag_.is_use_block_cache());

  row_iter = dirty_row_iter;
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.multi_get(param, context, rowkeys, row_iter));
  ASSERT_EQ(nullptr, row_iter);
  ASSERT_TRUE(context.query_flag_.is_use_block_cache());

  row_iter = dirty_row_iter;
  ASSERT_EQ(OB_NOT_SUPPORTED, sstable.multi_scan(param, context, ranges, row_iter));
  ASSERT_EQ(nullptr, row_iter);
  ASSERT_TRUE(context.query_flag_.is_use_block_cache());
}

TEST_F(TestIVFSSTable, reset_clears_ivf_meta)
{
  ObIvfSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_TRUE(sstable.get_ivf_meta().is_valid());
  ASSERT_TRUE(sstable.is_ivf_sstable());

  sstable.reset();
  ASSERT_FALSE(sstable.get_ivf_meta().is_valid());
  ASSERT_FALSE(sstable.is_valid());
}

TEST_F(TestIVFSSTable, version_getters_return_table_key_values)
{
  ObIvfSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));

  const int64_t snapshot_version = sstable.key_.version_range_.snapshot_version_;
  const int64_t base_version = sstable.key_.version_range_.base_version_;
  ASSERT_EQ(snapshot_version, sstable.get_snapshot_version());
  ASSERT_EQ(snapshot_version, sstable.get_max_merged_trans_version());
  ASSERT_EQ(base_version, sstable.get_min_merged_trans_version());
}

TEST_F(TestIVFSSTable, empty_ivf_sstable_basic_properties)
{
  ObIvfSSTable sstable;
  bool inc_success = false;
  ASSERT_EQ(OB_SUCCESS, build_ivf_sstable(sstable));
  ASSERT_EQ(0, sstable.get_data_macro_block_count());
  ASSERT_TRUE(sstable.no_data_to_read());
  ASSERT_FALSE(sstable.contain_uncommitted_row());
  ASSERT_EQ(OB_SUCCESS, sstable.inc_macro_ref(inc_success));
  ASSERT_TRUE(inc_success);
  sstable.dec_macro_ref();
}

TEST_F(TestIVFSSTable, create_empty_ivf_sstable_from_schema)
{
  ObStorageSchema storage_schema;
  ObTabletCreateSSTableParam param;
  const ObTabletID tablet_id(1001);
  const int64_t snapshot_version = 100;

  ASSERT_EQ(OB_SUCCESS, build_async_ivf_storage_schema(storage_schema));
  ASSERT_TRUE(storage_schema.is_async_ivf_index());
  ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(
      tablet_id, storage_schema, snapshot_version, -1, false, false));
  ASSERT_TRUE(param.table_key().is_ivf_sstable());
  ASSERT_FALSE(param.table_key().is_inv_sstable());
  ASSERT_FALSE(param.table_key().is_co_sstable());

  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_empty_sstable(
      allocator_, storage_schema, tablet_id, snapshot_version, table_handle));
  ObITable *table = table_handle.get_table();
  ASSERT_NE(nullptr, table);
  ASSERT_TRUE(table->is_ivf_sstable());
  ObIvfSSTable *sstable = static_cast<ObIvfSSTable *>(table);
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable->get_meta_type());
  ASSERT_EQ(snapshot_version, sstable->get_snapshot_version());
  ASSERT_EQ(0, sstable->get_data_macro_block_count());
  ASSERT_TRUE(sstable->no_data_to_read());
  ASSERT_TRUE(sstable->get_ivf_meta().is_valid());
  ASSERT_TRUE(sstable->get_ivf_meta().is_empty());

  const uint64_t data_version = DATA_CURRENT_VERSION;
  const int64_t buf_len = sstable->get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, sstable->serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObIvfSSTable decoded_sstable;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_sstable.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(decoded_sstable.is_ivf_sstable());
  ASSERT_TRUE(decoded_sstable.no_data_to_read());
  ASSERT_TRUE(decoded_sstable.get_ivf_meta().is_valid());
  ASSERT_TRUE(decoded_sstable.get_ivf_meta().is_empty());

  const int64_t full_buf_len = sstable->get_full_serialize_size(data_version);
  char *full_buf = static_cast<char *>(allocator_.alloc(full_buf_len));
  ASSERT_NE(nullptr, full_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, sstable->serialize_full_table(data_version, full_buf, full_buf_len, pos));
  ASSERT_EQ(full_buf_len, pos);
  ObIvfSSTable full_decoded_sstable;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, full_decoded_sstable.deserialize(allocator_, full_buf, full_buf_len, pos));
  ASSERT_TRUE(full_decoded_sstable.get_ivf_meta().is_empty());
}

static const int64_t IVF_SNAPSHOT_VERSION = 100;
static const uint64_t IVF_TABLE_ID = 200001;
static const uint64_t IVF_TABLET_ID = 200001;
static const int64_t IVF_LS_ID = 9107;

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
    : sstable_builder_(false /*use_double_write_buffer*/)
  {}

  virtual void SetUp() override
  {
    ObClusterVersion::get_instance().update_cluster_version(DATA_CURRENT_VERSION);
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    allocator_.reset();
    table_schema_.reset();
    whole_desc_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    sstable_.reset();
  }

  virtual void TearDown() override
  {
    sstable_.reset();
    sstable_builder_.reset();
    object_cleaner_.reset();
    whole_desc_.reset();
    table_schema_.reset();
    allocator_.reset();
  }

  int prepare_write_env(
      const int64_t concurrent_cnt = 0,
      const ObSSTableIndexBuilder::ObSpaceOptimizationMode optimization_mode =
          ObSSTableIndexBuilder::ENABLE,
      const bool micro_index_clustered = false,
      const ObRowStoreType row_store_type = CS_ENCODING_ROW_STORE,
      const char *compressor = "none")
  {
    int ret = OB_SUCCESS;
    prepare_ivf_schema(table_schema_);
    table_schema_.set_row_store_type(row_store_type);
    table_schema_.set_micro_index_clustered(micro_index_clustered);
    share::SCN end_scn;
    const share::SCN reorganization_scn(share::SCN::min_scn());
    end_scn.convert_for_tx(IVF_SNAPSHOT_VERSION);
    if (OB_FAIL(table_schema_.set_compress_func_name(compressor))) {
      LOG_WARN("failed to set IVF test compressor", K(ret), K(compressor));
    } else if (OB_FAIL(whole_desc_.init(
        false /*is_ddl*/,
        table_schema_,
        ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID),
        MAJOR_MERGE,
        IVF_SNAPSHOT_VERSION,
        DATA_CURRENT_VERSION,
        table_schema_.get_micro_index_clustered(),
        0 /*private_transfer_epoch*/,
        concurrent_cnt,
        reorganization_scn,
        end_scn))) {
      LOG_WARN("failed to init whole data store desc", K(ret));
    } else {
      whole_desc_.get_desc().static_desc_->schema_version_ = 10;
      whole_desc_.get_desc().sstable_index_builder_ = &sstable_builder_;
      if (OB_FAIL(sstable_builder_.init(whole_desc_.get_desc(), optimization_mode))) {
        LOG_WARN("failed to init sstable index builder", K(ret));
      }
    }
    return ret;
  }

  int open_writer(ObIvfMacroBlockWriter &writer, const int64_t start_seq)
  {
    ObMacroSeqParam seq_param;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = start_seq;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    return writer.open(whole_desc_.get_desc(), 0 /*parallel_idx*/, seq_param,
                       pre_warm_param, object_cleaner_);
  }

  int open_format_aware_writer(
      ObIvfMacroBlockWriter &writer,
      const int64_t start_seq)
  {
    int ret = OB_SUCCESS;
    ObIvfSSTableRowDesc row_desc;
    ObMacroSeqParam seq_param;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = start_seq;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    if (OB_FAIL(row_desc.init(IVF_SSTABLE_FORMAT_VERSION))) {
      LOG_WARN("failed to initialize IVF SSTable row descriptor", K(ret));
    } else if (OB_FAIL(writer.open(row_desc, whole_desc_.get_desc(),
                                   0 /*parallel_idx*/, seq_param,
                                   pre_warm_param, object_cleaner_))) {
      LOG_WARN("failed to open format-aware IVF writer", K(ret), K(start_seq));
    }
    return ret;
  }

  int make_ivf_batch(
      const int64_t *row_types,
      const int64_t row_count,
      ObDatumRow *rows,
      ObRowVectorConverter &converter,
      ObBatchDatumRows &batch)
  {
    int ret = OB_SUCCESS;
    ObArray<VectorFormat> vector_formats;
    const ObIArray<ObColDesc> &column_descs =
        whole_desc_.get_desc().get_full_stored_col_descs();
    if (OB_UNLIKELY(nullptr == row_types || row_count <= 0 || nullptr == rows)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF batch test arguments", K(ret), KP(row_types),
               K(row_count), KP(rows));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
      if (OB_FAIL(vector_formats.push_back(VectorFormat::VEC_UNIFORM))) {
        LOG_WARN("failed to append IVF test vector format", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(
        converter.init(column_descs, vector_formats, row_count))) {
      LOG_WARN("failed to initialize IVF row vector converter", K(ret), K(row_count));
    }
    bool is_full = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      const int64_t cid = 0;
      const int64_t vid = IVF_ROW_TYPE_CENTROID == row_types[i] ? i + 1 : 100 + i;
      if (OB_FAIL(make_ivf_row(rows[i], row_types[i], cid, vid, "batch_payload"))) {
        LOG_WARN("failed to make IVF batch row", K(ret), K(i), K(row_types[i]));
      } else if (OB_FAIL(converter.append_row(rows[i], is_full))) {
        LOG_WARN("failed to append IVF row to vector converter", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(converter.get_batch_datums(batch))) {
      LOG_WARN("failed to materialize IVF batch datums", K(ret));
    } else if (OB_SUCC(ret)) {
      batch.row_flag_ = rows[0].row_flag_;
      batch.mvcc_row_flag_ = rows[0].mvcc_row_flag_;
      batch.trans_id_ = rows[0].trans_id_;
    }
    return ret;
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

  int append_type_rows(
      ObIvfMacroBlockWriter &writer,
      const int64_t type,
      const int64_t row_cnt,
      const int64_t cid_base)
  {
    int ret = OB_SUCCESS;
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
    return ret;
  }

  int write_type_rows(
      const int64_t type,
      const int64_t start_seq,
      const int64_t row_cnt,
      const int64_t cid_base)
  {
    int ret = OB_SUCCESS;
    ObIvfMacroBlockWriter writer;
    if (OB_FAIL(open_writer(writer, start_seq))) {
      LOG_WARN("failed to open writer", K(ret), K(type), K(start_seq));
    } else if (OB_FAIL(append_type_rows(writer, type, row_cnt, cid_base))) {
      LOG_WARN("failed to append type rows", K(ret), K(type));
    } else if (OB_FAIL(writer.close())) {
      LOG_WARN("failed to close writer", K(ret), K(type));
    }
    return ret;
  }

  int close_builder_and_init_ivf_sstable(const int32_t dim = 8)
  {
    int ret = OB_SUCCESS;
    ObSSTableMergeRes res;
    ObTabletCreateSSTableParam param;
    if (OB_FAIL(sstable_builder_.close(res))) {
      LOG_WARN("failed to close sstable index builder", K(ret));
    } else if (OB_FAIL(fill_create_param(res, param))) {
      LOG_WARN("failed to fill create param", K(ret));
    } else {
      fill_ivf_meta(dim, param.ivf_meta_);
      if (OB_FAIL(sstable_.init(param, &allocator_))) {
        LOG_WARN("failed to init ivf sstable", K(ret), K(param));
      }
    }
    if (OB_SUCCESS != ret && sstable_.is_valid()) {
      sstable_.reset();
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
      param.compressor_type_ = whole_desc_.get_desc().get_compressor_type();
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
      param.rec_scn_ = share::SCN::min_scn();
      MEMCPY(param.encrypt_key_, res.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    }
    return ret;
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

  ObArenaAllocator allocator_;
  ObTableSchema table_schema_;
  ObWholeDataStoreDesc whole_desc_;
  ObSSTableIndexBuilder sstable_builder_;
  ObSSTablePrivateObjectCleaner object_cleaner_;
  ObIvfSSTable sstable_;
  ObFixedArray<ObColDesc, ObIAllocator> schema_cols_;
};

// Exercise the production empty-delta entry with real persisted source macros.
// Input metadata is supplied through the same resolved-input seam as the merge
// capture tests; schema-service discovery is outside this writer fixture.
class TestIVFSingleIncReuse : public TestIVFSSTableWriter
{
protected:
  void build_source(const bool clustered = false)
  {
    ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE, clustered));
    ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_CENTROID, 0, 4, 0));
    ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 100, 6, 1));
    ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_RAW_VECTOR, 200, 5, 0));
    ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(8));
    ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
    ASSERT_FALSE(sstable_.is_small_sstable());
    ObIvfSSTableMeta meta = sstable_.get_ivf_meta();
    meta.centroid_count_ = 4;
    ASSERT_EQ(OB_SUCCESS, sstable_.set_ivf_meta(allocator_, meta));
  }

  void convert_source_to_inc()
  {
    // Recreate the same real macros with valid INC transaction metadata. This
    // lets the production collector inspect both populated and empty inputs.
    ObSSTableMergeRes res;
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, sstable_builder_.close(res));
    ASSERT_EQ(OB_SUCCESS, fill_create_param(res, param));
    param.ivf_meta_ = sstable_.get_ivf_meta();
    param.table_key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
    ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.start_scn_.convert_for_tx(900));
    ASSERT_EQ(OB_SUCCESS, param.table_key_.scn_range_.end_scn_.convert_for_tx(940));
    param.min_merged_trans_version_ = 50;
    param.max_merged_trans_version_ = IVF_SNAPSHOT_VERSION;
    ObUncommitTxDesc tx_desc(9001, IVF_SNAPSHOT_VERSION, ObUncommitTxDesc::COMMIT_VERSION);
    ASSERT_EQ(OB_SUCCESS, param.uncommit_tx_info_.push_back(tx_desc));
    param.uncommit_tx_info_.set_info_status_record_seq();
    sstable_.reset();
    ASSERT_EQ(OB_SUCCESS, sstable_.init(param, &allocator_));
    ASSERT_TRUE(sstable_.is_inc_ivf_sstable());
  }

  void build_empty_delta(ObIvfSSTable &empty)
  {
    ObStorageSchema schema;
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(
        ObTabletID(IVF_TABLET_ID), schema, IVF_SNAPSHOT_VERSION + 10, -1, false, false));
    param.table_key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
    param.table_key_.version_range_.base_version_ = IVF_SNAPSHOT_VERSION;
    param.table_key_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION + 10;
    param.ivf_meta_.set_empty();
    ObUncommitTxDesc tx_desc(1001, IVF_SNAPSHOT_VERSION + 10, ObUncommitTxDesc::COMMIT_VERSION);
    ASSERT_EQ(OB_SUCCESS, param.uncommit_tx_info_.push_back(tx_desc));
    param.uncommit_tx_info_.set_info_status_record_seq();
    ASSERT_EQ(OB_SUCCESS, empty.init(param, &allocator_));
    ASSERT_TRUE(empty.is_empty());
  }

  void prepare_context(ObAsyncIndexMergeCtx &ctx, ObIvfSSTable &empty,
      ObIVFMergeInputAdapter &input, const bool empty_first = false)
  {
    table_schema_.set_table_type(USER_INDEX);
    table_schema_.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
    table_schema_.set_data_table_id(table_schema_.get_table_id());
    ObStorageSchema *schema = OB_NEWx(ObStorageSchema, &allocator_);
    ASSERT_NE(nullptr, schema);
    ASSERT_EQ(OB_SUCCESS, schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    ctx.static_param_.schema_ = schema;
    ctx.static_param_.rec_scn_.set_min();
    ctx.static_param_.is_ivf_index_ = true;
    ctx.parallel_merge_ctx_.concurrent_cnt_ = 1;
    if (ctx.get_merge_type() == INC_MAJOR_MERGE) {
      ASSERT_EQ(OB_SUCCESS, ctx.static_param_.scn_range_.start_scn_.convert_for_tx(900));
      ASSERT_EQ(OB_SUCCESS, ctx.static_param_.scn_range_.end_scn_.convert_for_tx(980));
    }
    ctx.static_param_.concurrent_cnt_ = 1;
    ctx.static_param_.sstable_logic_seq_ = 0;
    ctx.static_param_.version_range_.base_version_ =
        ctx.get_merge_type() == INC_MAJOR_MERGE ? 50 : 0;
    ctx.static_param_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION + 10;
    ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator_, table_schema_.get_column_count(),
        table_schema_.get_rowkey_column_num(), false,
        whole_desc_.get_desc().get_full_stored_col_descs()));
    ObTableHandleV2 source_handle, empty_handle;
    ASSERT_EQ(OB_SUCCESS, source_handle.set_sstable_with_tablet(&sstable_));
    ASSERT_EQ(OB_SUCCESS, empty_handle.set_sstable_with_tablet(&empty));
    ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(empty_first ? empty_handle : source_handle));
    ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(empty_first ? source_handle : empty_handle));
    ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
    ASSERT_NE(nullptr, bitmap);
    ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));
    input.index_table_id_ = IVF_TABLE_ID;
    input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
    input.vector_dim_ = 8;
    input.max_sample_count_ = 16;
    input.dist_type_ = VIDA_L2;
    ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
    ASSERT_EQ(OB_SUCCESS, input.prepare_source_(ctx));

    auto &desc = ctx.get_merge_info().get_sstable_build_desc();
    auto *builder = ctx.get_merge_info().get_index_builder();
    SCN end_scn;
    ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(IVF_SNAPSHOT_VERSION + 10));
    ASSERT_EQ(OB_SUCCESS, desc.init(false, table_schema_, ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID), ctx.get_merge_type(), IVF_SNAPSHOT_VERSION + 10,
        DATA_CURRENT_VERSION, table_schema_.get_micro_index_clustered(), 0, 1,
        SCN::min_scn(), end_scn));
    desc.get_desc().static_desc_->schema_version_ = 10;
    desc.get_desc().sstable_index_builder_ = builder;
    ASSERT_EQ(OB_SUCCESS, builder->init(desc.get_desc(), ObSSTableIndexBuilder::DISABLE));
    builder->set_is_async_index(true);
  }

  void prepare_single_inc_context(ObAsyncIndexMergeCtx &ctx,
      const ObTableHandleV2 &source, const int64_t snapshot, const int16_t logic_seq)
  {
    ObIvfSSTable empty;
    ASSERT_NO_FATAL_FAILURE(build_empty_delta(empty));
    ObIVFMergeInputAdapter input(allocator_);
    ASSERT_NO_FATAL_FAILURE(prepare_context(ctx, empty, input));
    ctx.static_param_.tables_handle_.reset();
    ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(source));
    ctx.static_param_.scn_range_ = source.get_table()->get_key().scn_range_;
    ctx.static_param_.version_range_.snapshot_version_ = snapshot;
    ctx.static_param_.sstable_logic_seq_ = logic_seq;
    ctx.deletion_bitmap_handle_.reset();
    ASSERT_EQ(OB_SUCCESS, ctx.init_inc_major_uncommit_tx_info());
  }

  void set_output_layout(ObAsyncIndexMergeCtx &ctx, const bool clustered)
  {
    auto &desc = ctx.get_merge_info().get_sstable_build_desc().get_desc();
    auto *builder = ctx.get_merge_info().get_index_builder();
    desc.static_desc_->micro_index_clustered_ = clustered;
    builder->reset();
    ASSERT_EQ(OB_SUCCESS, builder->init(desc, ObSSTableIndexBuilder::DISABLE));
    builder->set_is_async_index(true);
  }

  void create_reused_output(ObAsyncIndexMergeCtx &ctx, ObTableHandleV2 &output)
  {
    // The fixture has no tablet/LS to publish into; use the real output factory
    // on the builder result. The mysqltest covers the complete Finish path.
    ObSSTableMergeRes res;
    auto *builder = ctx.get_merge_info().get_index_builder();
    ASSERT_EQ(OB_SUCCESS, builder->close(res));
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, param.init_for_merge(ctx, res, nullptr, 0));
    ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
        param, allocator_, output));
  }



  void verify_reused_output(ObAsyncIndexMergeCtx &ctx)
  {
    ObSSTableMergeRes source, output;
    ASSERT_EQ(OB_SUCCESS, sstable_builder_.close(source));
    ASSERT_EQ(OB_SUCCESS, ctx.get_merge_info().get_index_builder()->close(output));
    EXPECT_EQ(source.row_count_, output.row_count_);
    EXPECT_EQ(source.data_checksum_, output.data_checksum_);
    EXPECT_EQ(source.data_blocks_cnt_, output.data_blocks_cnt_);
    EXPECT_EQ(source.data_blocks_cnt_, output.use_old_macro_block_count_);
    ASSERT_EQ(source.data_block_ids_.count(), output.data_block_ids_.count());
    for (int64_t i = 0; i < source.data_block_ids_.count(); ++i) {
      EXPECT_EQ(source.data_block_ids_.at(i), output.data_block_ids_.at(i));
    }
    const ObIvfSSTableMeta &meta = sstable_.get_ivf_meta();
    EXPECT_EQ(meta.flags_, output.ivf_meta_.flags_);
    EXPECT_EQ(meta.index_type_, output.ivf_meta_.index_type_);
    EXPECT_EQ(meta.dist_type_, output.ivf_meta_.dist_type_);
    EXPECT_EQ(meta.dim_, output.ivf_meta_.dim_);
    EXPECT_EQ(meta.format_version_, output.ivf_meta_.format_version_);
    EXPECT_EQ(meta.nbits_, output.ivf_meta_.nbits_);
    EXPECT_EQ(meta.fht_algo_version_, output.ivf_meta_.fht_algo_version_);
    EXPECT_EQ(meta.centroid_count_, output.ivf_meta_.centroid_count_);

    // Exercise the actual publication-parameter factory. The output must carry
    // the new watermark even though every data macro still belongs to the base.
    ObTabletCreateSSTableParam published_param;
    ASSERT_EQ(OB_SUCCESS, published_param.init_for_merge(ctx, output, nullptr, 0));
    const bool incremental = ctx.get_merge_type() == INC_MAJOR_MERGE;
    EXPECT_EQ(incremental ? ObITable::INC_IVF_INDEX_SSTABLE : ObITable::IVF_INDEX_SSTABLE,
        published_param.table_key_.table_type_);
    EXPECT_EQ(ObTabletID(IVF_TABLET_ID), published_param.table_key_.tablet_id_);
    if (incremental) {
      EXPECT_EQ(ctx.static_param_.scn_range_.start_scn_, published_param.table_key_.scn_range_.start_scn_);
      EXPECT_EQ(ctx.static_param_.scn_range_.end_scn_, published_param.table_key_.scn_range_.end_scn_);
      EXPECT_EQ(50, published_param.min_merged_trans_version_);
      EXPECT_EQ(IVF_SNAPSHOT_VERSION + 10, published_param.max_merged_trans_version_);
      ASSERT_EQ(1, published_param.uncommit_tx_info_.get_info_count());
      ObUncommitTxDesc newest;
      ASSERT_EQ(OB_SUCCESS, published_param.uncommit_tx_info_.get_uncommit_tx_desc(newest, 0));
      EXPECT_TRUE(newest.is_commit_version());
      EXPECT_EQ(1001, newest.tx_id_); // comes from the empty increment, not the base
      EXPECT_EQ(IVF_SNAPSHOT_VERSION + 10, newest.commit_version_);
    } else {
      EXPECT_EQ(0, published_param.table_key_.version_range_.base_version_);
      EXPECT_EQ(IVF_SNAPSHOT_VERSION + 10, published_param.table_key_.version_range_.snapshot_version_);
    }
    ObIvfSSTable published;
    ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
        published_param, allocator_, published));
    ObNewVersionRange published_range;
    ASSERT_EQ(OB_SUCCESS, published.get_async_index_version_range(published_range));
    EXPECT_EQ(incremental ? 50 : 0, published_range.base_version_);
    EXPECT_EQ(IVF_SNAPSHOT_VERSION + 10, published_range.snapshot_version_);
    EXPECT_EQ(sstable_.get_row_count(), published.get_row_count());
    EXPECT_EQ(meta.centroid_count_, published.get_ivf_meta().centroid_count_);
  }
};

TEST_F(TestIVFSingleIncReuse, single_inc_reuses_data_macros_with_source_layout)
{
  ASSERT_NO_FATAL_FAILURE(build_source(false));
  ASSERT_NO_FATAL_FAILURE(convert_source_to_inc());
  ObIvfSSTable empty;
  ASSERT_NO_FATAL_FAILURE(build_empty_delta(empty));
  ObTabletMergeDagParam param(INC_MAJOR_MERGE, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  param.merge_version_ = IVF_SNAPSHOT_VERSION + 10;
  ObAsyncIndexMergeCtx ctx(param, allocator_);
  ObIVFMergeInputAdapter input(allocator_);
  ASSERT_NO_FATAL_FAILURE(prepare_context(ctx, empty, input));
  ObTableHandleV2 source_handle;
  ASSERT_EQ(OB_SUCCESS, source_handle.set_sstable_with_tablet(&sstable_));
  ctx.static_param_.tables_handle_.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(source_handle));
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.scn_range_.end_scn_.convert_for_tx(940));
  ctx.static_param_.sstable_logic_seq_ = 1;
  ctx.deletion_bitmap_handle_.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.init_inc_major_uncommit_tx_info());
  // Refresh writes ordinary macros, while a merge may request clustered layout.
  ASSERT_NO_FATAL_FAILURE(set_output_layout(ctx, true));
  struct ResetEvent {
    ~ResetEvent() { EventTable::set_event("ERRSIM_IVF_FORCE_FULL_REBUILD", EventItem()); }
  } reset_event;
  EventItem event;
  event.error_code_ = OB_EAGAIN;
  event.occur_ = 1;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("ERRSIM_IVF_FORCE_FULL_REBUILD", event));
  ObAsyncIndexMergeDag dag(ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE);
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
  ObFakeTask *prepare = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
  ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start(dag, ctx, *prepare));
  EXPECT_TRUE(dag.ivf_merge_dag_ctx_.reuse_input_);
  EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.input_adapter_);
  EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.planner_);
  EXPECT_EQ(1, prepare->get_child_nodes().count());
  EXPECT_FALSE(ctx.merged_table_handle_.is_valid());
  EXPECT_FALSE(ctx.get_merge_info().get_index_builder()->micro_index_clustered());
  EXPECT_FALSE(ctx.get_merge_info().get_sstable_build_desc().get_desc().micro_index_clustered());
  ObTableHandleV2 output_handle;
  ASSERT_NO_FATAL_FAILURE(create_reused_output(ctx, output_handle));
  const ObSSTable *output = nullptr;
  ASSERT_EQ(OB_SUCCESS, output_handle.get_sstable(output));
  ASSERT_NE(nullptr, output);
  ASSERT_NE(&sstable_, output);
  EXPECT_EQ(sstable_.get_key(), output->get_key());
  EXPECT_EQ(50, output->get_min_merged_trans_version());
  EXPECT_EQ(IVF_SNAPSHOT_VERSION + 10, output->get_max_merged_trans_version());
  EXPECT_EQ(IVF_SNAPSHOT_VERSION + 10, output->get_upper_trans_version());
  EXPECT_EQ(IVF_SNAPSHOT_VERSION, sstable_.get_max_merged_trans_version());
  ObSSTableMetaHandle source_meta, output_meta;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_meta(source_meta));
  ASSERT_EQ(OB_SUCCESS, output->get_meta(output_meta));
  const auto &src = source_meta.get_sstable_meta();
  const auto &dst = output_meta.get_sstable_meta();
  EXPECT_NE(&src, &dst);
  EXPECT_EQ(src.get_data_checksum(), dst.get_data_checksum());
  EXPECT_EQ(src.get_row_count(), dst.get_row_count());
  EXPECT_EQ(1, dst.get_basic_meta().sstable_logic_seq_);
  EXPECT_EQ(0, src.get_basic_meta().sstable_logic_seq_);
  ObMacroIdIterator src_iter, dst_iter;
  ASSERT_EQ(OB_SUCCESS, src.get_macro_info().get_data_block_iter(src_iter));
  ASSERT_EQ(OB_SUCCESS, dst.get_macro_info().get_data_block_iter(dst_iter));
  MacroBlockId src_id, dst_id;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = src_iter.get_next_macro_id(src_id))) {
    ASSERT_EQ(OB_SUCCESS, dst_iter.get_next_macro_id(dst_id));
    EXPECT_EQ(src_id, dst_id);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(OB_ITER_END, dst_iter.get_next_macro_id(dst_id));
  const auto &ivf = *static_cast<const ObIvfSSTable *>(output);
  char src_buf[512], dst_buf[512];
  int64_t src_pos = 0, dst_pos = 0;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_ivf_meta().serialize(src_buf, sizeof(src_buf), src_pos));
  ASSERT_EQ(OB_SUCCESS, ivf.get_ivf_meta().serialize(dst_buf, sizeof(dst_buf), dst_pos));
  ASSERT_EQ(src_pos, dst_pos);
  EXPECT_EQ(0, MEMCMP(src_buf, dst_buf, src_pos));
  ASSERT_EQ(sstable_.get_fht_rotator().signs_size(), ivf.get_fht_rotator().signs_size());
  for (int64_t i = 0; i < ivf.get_fht_rotator().signs_size(); ++i) {
    EXPECT_EQ(sstable_.get_fht_rotator().signs()[i], ivf.get_fht_rotator().signs()[i]);
  }
}

TEST_F(TestIVFSingleIncReuse, consecutive_single_inc_reuses_previous_output_without_mutation)
{
  ASSERT_NO_FATAL_FAILURE(build_source(true));
  ASSERT_NO_FATAL_FAILURE(convert_source_to_inc());
  ObTabletMergeDagParam first_param(INC_MAJOR_MERGE, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  first_param.merge_version_ = IVF_SNAPSHOT_VERSION + 10;
  ObTabletMergeDagParam second_param(INC_MAJOR_MERGE, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  second_param.merge_version_ = IVF_SNAPSHOT_VERSION + 20;
  // Round two consumes the actual SSTable created by the first round's builder.
  ObAsyncIndexMergeCtx first_ctx(first_param, allocator_);
  ObAsyncIndexMergeCtx second_ctx(second_param, allocator_);
  ObAsyncIndexMergeCtx *contexts[] = {&first_ctx, &second_ctx};
  ObTableHandleV2 source;
  ASSERT_EQ(OB_SUCCESS, source.set_sstable_with_tablet(&sstable_));
  for (int64_t round = 1; round <= 2; ++round) {
    SCOPED_TRACE(round);
    auto &ctx = *contexts[round - 1];
    const int64_t target = IVF_SNAPSHOT_VERSION + 10 * round;
    ASSERT_NO_FATAL_FAILURE(prepare_single_inc_context(ctx, source, target, round));
    // Exercise the opposite conversion too: clustered source, ordinary target.
    ASSERT_NO_FATAL_FAILURE(set_output_layout(ctx, false));
    const ObSSTable *input = nullptr;
    ASSERT_EQ(OB_SUCCESS, source.get_sstable(input));
    const int64_t input_snapshot = input->get_max_merged_trans_version();
    ObAsyncIndexMergeDag dag(ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE);
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
    ObFakeTask *prepare = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
    ASSERT_EQ(OB_SUCCESS, dag.ivf_merge_dag_ctx_.start(dag, ctx, *prepare));
    EXPECT_STREQ("SINGLE_INC_REUSE", dag.ivf_merge_dag_ctx_.strategy_name());
    EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.input_adapter_);
    EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.planner_);
    EXPECT_EQ(1, prepare->get_child_nodes().count());
    EXPECT_FALSE(ctx.merged_table_handle_.is_valid());
    EXPECT_TRUE(ctx.get_merge_info().get_index_builder()->micro_index_clustered());
    EXPECT_TRUE(ctx.get_merge_info().get_sstable_build_desc().get_desc().micro_index_clustered());
    ObTableHandleV2 output_handle;
    ASSERT_NO_FATAL_FAILURE(create_reused_output(ctx, output_handle));
    const ObSSTable *output = nullptr;
    ASSERT_EQ(OB_SUCCESS, output_handle.get_sstable(output));
    ASSERT_NE(nullptr, output);
    EXPECT_NE(input, output);
    EXPECT_EQ(sstable_.get_key(), output->get_key());
    EXPECT_EQ(50, output->get_min_merged_trans_version());
    EXPECT_EQ(target, output->get_max_merged_trans_version());
    EXPECT_EQ(target, output->get_upper_trans_version());
    EXPECT_EQ(input_snapshot, input->get_max_merged_trans_version());
    EXPECT_EQ(sstable_.get_row_count(), output->get_row_count());
    EXPECT_EQ(sstable_.get_data_checksum(), output->get_data_checksum());
    ObSSTableMetaHandle source_meta, output_meta;
    ASSERT_EQ(OB_SUCCESS, input->get_meta(source_meta));
    ASSERT_EQ(OB_SUCCESS, output->get_meta(output_meta));
    EXPECT_EQ(round - 1, source_meta.get_sstable_meta().get_basic_meta().sstable_logic_seq_);
    EXPECT_EQ(round, output_meta.get_sstable_meta().get_basic_meta().sstable_logic_seq_);
    ObMacroIdIterator src_iter, dst_iter;
    ASSERT_EQ(OB_SUCCESS, source_meta.get_sstable_meta().get_macro_info().get_data_block_iter(src_iter));
    ASSERT_EQ(OB_SUCCESS, output_meta.get_sstable_meta().get_macro_info().get_data_block_iter(dst_iter));
    MacroBlockId src_id, dst_id;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = src_iter.get_next_macro_id(src_id))) {
      ASSERT_EQ(OB_SUCCESS, dst_iter.get_next_macro_id(dst_id));
      EXPECT_EQ(src_id, dst_id);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    ASSERT_EQ(OB_ITER_END, dst_iter.get_next_macro_id(dst_id));
    const auto &ivf = *static_cast<const ObIvfSSTable *>(output);
    char expected[512], actual[512];
    int64_t expected_len = 0, actual_len = 0;
    ASSERT_EQ(OB_SUCCESS, sstable_.get_ivf_meta().serialize(expected, sizeof(expected), expected_len));
    ASSERT_EQ(OB_SUCCESS, ivf.get_ivf_meta().serialize(actual, sizeof(actual), actual_len));
    ASSERT_EQ(expected_len, actual_len);
    EXPECT_EQ(0, MEMCMP(expected, actual, expected_len));
    ASSERT_EQ(sstable_.get_fht_rotator().signs_size(), ivf.get_fht_rotator().signs_size());
    for (int64_t i = 0; i < ivf.get_fht_rotator().signs_size(); ++i) {
      EXPECT_EQ(sstable_.get_fht_rotator().signs()[i], ivf.get_fht_rotator().signs()[i]);
    }
    source = output_handle;
  }
  EXPECT_EQ(IVF_SNAPSHOT_VERSION, sstable_.get_max_merged_trans_version());
}

TEST_F(TestIVFSingleIncReuse, single_inc_reuse_failure_returns_error_without_reclustering)
{
  ASSERT_NO_FATAL_FAILURE(build_source(true));
  ASSERT_NO_FATAL_FAILURE(convert_source_to_inc());
  const auto &source_table = sstable_;
  ObTabletMergeDagParam param(INC_MAJOR_MERGE, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  param.merge_version_ = IVF_SNAPSHOT_VERSION + 10;
  ObAsyncIndexMergeCtx ctx(param, allocator_);
  ObTableHandleV2 source;
  ASSERT_EQ(OB_SUCCESS, source.set_sstable_with_tablet(&sstable_));
  ASSERT_NO_FATAL_FAILURE(prepare_single_inc_context(ctx, source, param.merge_version_, 1));
  const int64_t size = source_table.get_serialize_size(DATA_CURRENT_VERSION);
  ASSERT_GT(size, 0);
  char *before = static_cast<char *>(allocator_.alloc(size));
  char *after = static_cast<char *>(allocator_.alloc(size));
  ASSERT_NE(nullptr, before);
  ASSERT_NE(nullptr, after);
  int64_t before_len = 0, after_len = 0;
  ASSERT_EQ(OB_SUCCESS, source_table.serialize(DATA_CURRENT_VERSION, before, size, before_len));
  ObAsyncIndexMergeDag dag(ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE);
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
  ObFakeTask *prepare = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(prepare));
  // Fail at the real macro-reuse entry after matching the single-input key.
  ctx.get_merge_info().get_index_builder()->reset();
  ASSERT_EQ(OB_NOT_INIT, dag.ivf_merge_dag_ctx_.start(dag, ctx, *prepare));
  EXPECT_FALSE(ctx.merged_table_handle_.is_valid());
  EXPECT_FALSE(dag.ivf_merge_dag_ctx_.reuse_input_);
  EXPECT_FALSE(dag.ivf_merge_dag_ctx_.is_inited_);
  EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.input_adapter_);
  EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.capture_ctx_);
  EXPECT_EQ(nullptr, dag.ivf_merge_dag_ctx_.planner_);
  EXPECT_EQ(0, prepare->get_child_nodes().count());
  ASSERT_EQ(OB_SUCCESS, source_table.serialize(DATA_CURRENT_VERSION, after, size, after_len));
  ASSERT_EQ(before_len, after_len);
  EXPECT_EQ(0, MEMCMP(before, after, before_len));
}

class FullRebuildWriterVectorStore final : public storage::ObIVFVectorStore
{
public:
  explicit FullRebuildWriterVectorStore(const storage::ObIVFVectorVID first_vid = 11)
    : first_vid_(first_vid) {}
  virtual bool is_valid() const override { return true; }
  virtual int64_t get_vector_count() const override { return 2; }
  virtual int64_t get_dimension() const override { return 2; }
  virtual int get_all_vids(
      ObIArray<storage::ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS;
    vids.reset();
    if (OB_FAIL(vids.push_back(first_vid_))) {
    } else {
      ret = vids.push_back(22);
    }
    return ret;
  }
  virtual int batch_get_by_vid(
      const ObIArray<storage::ObIVFVectorVID> &vids,
      const int64_t vid_begin,
      const int64_t vid_end,
      ObIArray<float> &vectors) const override
  {
    int ret = OB_SUCCESS;
    static const float TEST_VECTORS[][2] = {
      {1.0F, 2.0F}, {3.0F, 4.0F}
    };
    vectors.reset();
    if (vid_begin < 0 || vid_end <= vid_begin || vid_end > vids.count()) {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = vid_begin; OB_SUCC(ret) && i < vid_end; ++i) {
      const int64_t vector_idx = first_vid_ == vids.at(i)
          ? 0 : (22 == vids.at(i) ? 1 : -1);
      if (vector_idx < 0) {
        ret = OB_INVALID_ARGUMENT;
      }
      for (int64_t d = 0; OB_SUCC(ret) && d < 2; ++d) {
        ret = vectors.push_back(TEST_VECTORS[vector_idx][d]);
      }
    }
    return ret;
  }
private:
  storage::ObIVFVectorVID first_vid_;
};

// Records physical rows at the DDL slice boundary, including the payload
// lifetime: append must consume a reader batch before the next batch reuses it.
class RecordingIVFDDLSliceWriter final : public ObITabletSliceWriter
{
public:
  struct Row
  {
    int64_t type_;
    int64_t cid_;
    uint64_t vid_;
    std::string payload_;
  };
  RecordingIVFDDLSliceWriter()
      : close_calls_(0), fail_append_(false), fail_close_(false) {}
  virtual int init(const ObWriteMacroParam &) override { return OB_SUCCESS; }
  virtual void reset() override { rows_.clear(); }
  virtual int append_row(const ObDatumRow &row) override
  {
    int ret = OB_SUCCESS;
    ObIvfSSTableRowDesc desc;
    if (OB_FAIL(desc.init(IVF_SSTABLE_FORMAT_VERSION))) {
    } else if (fail_append_) {
      ret = OB_IO_ERROR;
    } else if (close_calls_ != 0) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      const ObString payload = row.storage_datums_[desc.payload_col_idx_].get_string();
      Row value = {row.storage_datums_[desc.row_type_col_idx_].get_int(),
          row.storage_datums_[desc.centroid_id_col_idx_].get_int(),
          row.storage_datums_[desc.vector_id_col_idx_].get_uint(),
          std::string(payload.ptr(), payload.length())};
      if (!rows_.empty() && std::make_tuple(rows_.back().type_, rows_.back().cid_, rows_.back().vid_)
          >= std::make_tuple(value.type_, value.cid_, value.vid_)) {
        ret = OB_ROWKEY_ORDER_ERROR;
      } else {
        rows_.push_back(value);
      }
    }
    return ret;
  }
  virtual int append_batch(const ObBatchDatumRows &) override { return OB_NOT_SUPPORTED; }
  virtual int64_t get_row_count() const override { return rows_.size(); }
  virtual int close() override
  {
    ++close_calls_;
    return fail_close_ ? OB_IO_ERROR : OB_SUCCESS;
  }
  TO_STRING_KV(K_(close_calls), K_(fail_append), K_(fail_close));
  std::vector<Row> rows_;
  int64_t close_calls_;
  bool fail_append_;
  bool fail_close_;
};

class RecordingIVFDDLRawWriters final : public ObIVFDDLRawWriterProvider
{
public:
  RecordingIVFDDLRawWriters() : create_count_(0), fail_create_(false) {}
  virtual int create_raw_writer(const int64_t task_idx, ObITabletSliceWriter *&writer) override
  {
    writer = nullptr;
    ++create_count_;
    if (fail_create_) {
      return OB_ALLOCATE_MEMORY_FAILED;
    } else if (task_idx < 0 || task_idx >= ObIVFMergeProcessor::MAX_RAW_TASK_COUNT) {
      return OB_INVALID_ARGUMENT;
    }
    writer = &writers_[task_idx];
    return OB_SUCCESS;
  }
  int create_posting_writer(int64_t task_idx, ObITabletSliceWriter *&writer) override
  {
    writer = nullptr;
    if (task_idx == fail_posting_create_) { return OB_ALLOCATE_MEMORY_FAILED; }
    if (task_idx < 0 || task_idx >= ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT) {
      return OB_INVALID_ARGUMENT;
    }
    writer = &posting_writers_[task_idx];
    return OB_SUCCESS;
  }
  int64_t fail_posting_create_ = -1;
  RecordingIVFDDLSliceWriter posting_writers_[ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT];
  RecordingIVFDDLSliceWriter writers_[ObIVFMergeProcessor::MAX_RAW_TASK_COUNT];
  int64_t create_count_;
  bool fail_create_;
};

// Destroy real sink objects at the output-call boundary. Keeping the provider
// alive across tablets must not keep completed physical writers alive with it.
class OwnedIVFDDLWriters final : public ObIVFDDLRawWriterProvider
{
public:
  enum Kind { CENTER, POSTING, RAW, KIND_COUNT };
  enum Failure { NONE, CREATE, APPEND, CLOSE };
  class Slice final : public ObITabletSliceWriter
  {
  public:
    Slice(OwnedIVFDDLWriters &owner, Kind kind) : owner_(owner), kind_(kind)
    {
      ++owner_.live_;
      ++owner_.created_[kind_];
      sink_.fail_append_ = owner_.fail_kind_ == kind_ && owner_.failure_ == APPEND;
      sink_.fail_close_ = owner_.fail_kind_ == kind_ && owner_.failure_ == CLOSE;
    }
    ~Slice() override
    {
      EXPECT_EQ(1, sink_.close_calls_);
      --owner_.live_;
      ++owner_.destroyed_[kind_];
    }
    int init(const ObWriteMacroParam &) override { return OB_SUCCESS; }
    void reset() override { sink_.reset(); }
    int append_row(const ObDatumRow &row) override { return sink_.append_row(row); }
    int append_batch(const ObBatchDatumRows &rows) override { return sink_.append_batch(rows); }
    int64_t get_row_count() const override
    {
      return sink_.get_row_count() + (kind_ == CENTER && sink_.close_calls_ > 0
          && owner_.wrong_center_count_ ? 1 : 0);
    }
    int close() override { return sink_.close(); }
    TO_STRING_KV(K_(kind));
    OwnedIVFDDLWriters &owner_;
    Kind kind_;
    RecordingIVFDDLSliceWriter sink_;
  };
  int create_center_writer(ObITabletSliceWriter *&writer) override { return create(CENTER, writer); }
  int create_posting_writer(int64_t, ObITabletSliceWriter *&writer) override { return create(POSTING, writer); }
  int create_raw_writer(int64_t, ObITabletSliceWriter *&writer) override { return create(RAW, writer); }
  void destroy_writer(ObITabletSliceWriter *writer) override { delete static_cast<Slice *>(writer); }
  int create(Kind kind, ObITabletSliceWriter *&writer)
  {
    writer = nullptr;
    if (fail_kind_ == kind && failure_ == CREATE) { return OB_ALLOCATE_MEMORY_FAILED; }
    writer = new Slice(*this, kind);
    return OB_SUCCESS;
  }
  int64_t live_ = 0;
  int64_t created_[KIND_COUNT] = {};
  int64_t destroyed_[KIND_COUNT] = {};
  Kind fail_kind_ = CENTER;
  Failure failure_ = NONE;
  bool wrong_center_count_ = false;
};

TEST_F(TestIVFSSTableWriter, ddl_global_sorter_stays_unallocated_until_global_output)
{
  for (bool borrowed_center : {false, true}) {
    OwnedIVFDDLWriters provider;
    RecordingIVFDDLSliceWriter center;
    ObIVFDDLSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, borrowed_center
        ? writer.init(center, provider, IVF_SNAPSHOT_VERSION, 768, VIDA_L2)
        : writer.init(provider, IVF_SNAPSHOT_VERSION, 768, VIDA_L2));
    // No sorter implementation means no 4096-row input workspace during training.
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    ObArray<ObIVFBuildRow> empty;
    ASSERT_EQ(OB_SUCCESS, writer.append_loaded_rows_(empty));
    EXPECT_EQ(OB_INVALID_ARGUMENT, writer.finish_quantization(0, 0, 0));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(1, 1));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    // Lazy initialization must not silently revive the global output mode.
    EXPECT_EQ(OB_STATE_NOT_MATCH, writer.append_sort_row_(
        IVF_ROW_TYPE_CENTROID, 0, 1, ObString::make_string("unused")));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    EXPECT_EQ(0, writer.get_row_count());
    EXPECT_EQ(0, provider.live_);
  }
}

TEST_F(TestIVFSSTableWriter, ddl_lazy_global_sorter_serializes_first_appends)
{
  FullRebuildWriterVectorStore store;
  ObIVFVectorReader reader;
  ObArray<ObIVFVectorVID> vids;
  ObArray<float> centers, vectors;
  ObArray<int64_t> primary;
  ObArray<uint32_t> counts;
  RecordingIVFDDLSliceWriter center;
  RecordingIVFDDLRawWriters provider;
  ObIVFDDLSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  ASSERT_EQ(OB_SUCCESS, store.batch_get_by_vid(vids, 0, 2, vectors));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(3.0F));
  ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
  ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
  ASSERT_EQ(OB_SUCCESS, counts.push_back(2));
  ASSERT_EQ(OB_SUCCESS, writer.init(center, provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, writer.set_leaf_posting_counts(counts));
  EXPECT_EQ(nullptr, writer.sorter_.impl_);
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  std::atomic<bool> start(false);
  int results[2] = {OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED};
  std::thread workers[2];
  for (int i = 0; i < 2; ++i) {
    workers[i] = std::thread([&, i]() {
      ObTenantEnv::set_tenant(tenant);
      while (!start.load()) { std::this_thread::yield(); }
      results[i] = i == 0 ? writer.write_loaded_centers(centers, 2, 0, 1)
          : writer.quantize_loaded_vectors(centers, 2, vids, vectors, primary, nullptr);
      ObTenantEnv::set_tenant(nullptr);
    });
  }
  start = true;
  for (auto &worker : workers) { worker.join(); }
  ASSERT_EQ(OB_SUCCESS, results[0]);
  ASSERT_EQ(OB_SUCCESS, results[1]);
  EXPECT_NE(nullptr, writer.sorter_.impl_);
  ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 1, 2));
  EXPECT_EQ(3, center.get_row_count());
  EXPECT_EQ(nullptr, writer.sorter_.impl_);
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 2, 0));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
  EXPECT_TRUE(writer.is_finished());
}

TEST_F(TestIVFSSTableWriter, ddl_lazy_global_sorter_init_failure_blocks_publication)
{
  RecordingIVFDDLSliceWriter center;
  RecordingIVFDDLRawWriters provider;
  ObIVFDDLSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, writer.init(center, provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  const uint64_t tenant_id = writer.sorter_tenant_id_;
  writer.sorter_tenant_id_ = OB_INVALID_TENANT_ID;
  ObIVFBuildRow row;
  row.row_type_ = IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
  row.cid_ = 1;
  row.vid_ = 7;
  row.payload_ = ObString::make_string("unused");
  ObArray<ObIVFBuildRow> rows;
  ASSERT_EQ(OB_SUCCESS, rows.push_back(row));
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_loaded_rows_(rows));
  EXPECT_EQ(nullptr, writer.sorter_.impl_);
  EXPECT_EQ(0, writer.get_row_count());
  // The loaded-output failure remains sticky, even after the cause is removed.
  writer.sorter_tenant_id_ = tenant_id;
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_loaded_rows_(rows));
  EXPECT_EQ(nullptr, writer.sorter_.impl_);
  EXPECT_NE(OB_SUCCESS, writer.finish_quantization(1, 1, 1));
  EXPECT_EQ(0, center.close_calls_);
  EXPECT_FALSE(writer.is_finished());
}

TEST_F(TestIVFSSTableWriter, ddl_output_tasks_destroy_writers_before_phase_completion)
{
  // One success and create/append/close failures in each kind of output task.
  for (int variant = 0; variant < 10; ++variant) {
    SCOPED_TRACE(variant);
    OwnedIVFDDLWriters provider;
    if (variant > 0) {
      provider.fail_kind_ = static_cast<OwnedIVFDDLWriters::Kind>((variant - 1) / 3);
      provider.failure_ = static_cast<OwnedIVFDDLWriters::Failure>(1 + (variant - 1) % 3);
    }
    FullRebuildWriterVectorStore store;
    ObIVFVectorReader reader;
    ObArray<ObIVFVectorVID> vids;
    ObArray<float> centers, vectors;
    ObArray<int64_t> primary;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ASSERT_EQ(OB_SUCCESS, store.batch_get_by_vid(vids, 0, 2, vectors));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(3.0F));
    ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
    ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
    ObIVFDDLSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, writer.init(provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, true));
    ASSERT_EQ(0, provider.live_);
    ASSERT_EQ(0, provider.created_[OwnedIVFDDLWriters::CENTER]);
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(1, 1));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    ASSERT_EQ(OB_SUCCESS, writer.quantize_loaded_vectors(centers, 2, vids, vectors, primary, nullptr));
    int ret = writer.write_loaded_centers(centers, 2, 0, 1);
    EXPECT_EQ(0, provider.live_);
    if (OB_SUCC(ret)) {
      ret = writer.write_posting_range(0);
      EXPECT_EQ(0, provider.live_);
    }
    if (OB_SUCC(ret)) {
      // The center sink is already gone; validate its saved physical row count.
      ASSERT_EQ(1, provider.destroyed_[OwnedIVFDDLWriters::CENTER]);
      ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 1, 2));
      ret = writer.write_raw_vectors(reader, vids, 0, 2, 0);
      EXPECT_EQ(0, provider.live_);
    }
    if (variant == 0) {
      ASSERT_EQ(OB_SUCCESS, ret);
      EXPECT_EQ(OB_INIT_TWICE, writer.write_raw_vectors(reader, vids, 0, 2, 0));
      ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
      EXPECT_TRUE(writer.is_finished());
      EXPECT_EQ(5, writer.get_row_count());
    } else {
      EXPECT_EQ(provider.failure_ == OwnedIVFDDLWriters::CREATE ? OB_ALLOCATE_MEMORY_FAILED : OB_IO_ERROR, ret);
      EXPECT_FALSE(writer.is_finished());
      EXPECT_NE(OB_SUCCESS, writer.finish_raw_write(2));
    }
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    for (int kind = 0; kind < OwnedIVFDDLWriters::KIND_COUNT; ++kind) {
      EXPECT_EQ(provider.created_[kind], provider.destroyed_[kind]);
    }
  }
}

TEST_F(TestIVFSSTableWriter, ddl_output_validates_center_count_after_writer_destruction)
{
  OwnedIVFDDLWriters provider;
  provider.wrong_center_count_ = true;
  ObIVFDDLSSTableWriter writer(allocator_);
  ObArray<float> centers, vectors;
  ObArray<ObIVFVectorVID> vids;
  ObArray<int64_t> primary;
  ASSERT_EQ(OB_SUCCESS, centers.push_back(1.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
  ASSERT_EQ(OB_SUCCESS, vectors.assign(centers));
  ASSERT_EQ(OB_SUCCESS, vids.push_back(11));
  ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
  ASSERT_EQ(OB_SUCCESS, writer.init(provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, true));
  ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(1, 1));
  ASSERT_EQ(OB_SUCCESS, writer.quantize_loaded_vectors(centers, 2, vids, vectors, primary, nullptr));
  ASSERT_EQ(OB_SUCCESS, writer.write_loaded_centers(centers, 2, 0, 1));
  ASSERT_EQ(0, provider.live_);
  ASSERT_EQ(1, provider.destroyed_[OwnedIVFDDLWriters::CENTER]);
  ASSERT_EQ(OB_SUCCESS, writer.write_posting_range(0));
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.finish_quantization(1, 1, 1));
  EXPECT_FALSE(writer.is_finished());
  EXPECT_EQ(0, provider.live_);
}

TEST_F(TestIVFSSTableWriter, ddl_serial_output_owns_only_its_created_writers)
{
  for (bool borrowed_center : {false, true}) {
    FullRebuildWriterVectorStore store;
    ObIVFVectorReader reader;
    ObArray<ObIVFVectorVID> vids;
    ObArray<float> centers;
    OwnedIVFDDLWriters provider;
    RecordingIVFDDLSliceWriter center;
    ObIVFDDLSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(3.0F));
    ASSERT_EQ(OB_SUCCESS, borrowed_center
        ? writer.init(center, provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2)
        : writer.init(provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    ASSERT_EQ(OB_SUCCESS, writer.quantize_cluster(reader, centers, 1, 0, vids, nullptr));
    EXPECT_NE(nullptr, writer.sorter_.impl_);
    ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 1, 2));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    EXPECT_EQ(0, provider.live_);
    EXPECT_EQ(borrowed_center ? 0 : 1, provider.destroyed_[OwnedIVFDDLWriters::CENTER]);
    if (borrowed_center) {
      EXPECT_EQ(3, center.get_row_count());
      EXPECT_EQ(1, center.close_calls_);
    }
    ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 2, 0));
    EXPECT_EQ(0, provider.live_);
    EXPECT_EQ(1, provider.destroyed_[OwnedIVFDDLWriters::RAW]);
    ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
    EXPECT_TRUE(writer.is_finished());
  }
}

static int64_t raw_buffer_hold()
{
  // MockTenant does not expose live per-label totals. Measure the isolated
  // RAW call's VECTOR_CTX delta; this is also the counter used by the reproducer.
  return lib::get_tenant_memory_hold(MTL_ID(), ObCtxIds::VECTOR_CTX_ID);
}

// Bound RAW scratch memory across enough 768D batches to expose arena growth.
// The sink consumes payloads synchronously and retains no vector copies.
class BoundedRawVectorStore final : public ObIVFVectorStore
{
public:
  static const int64_t DIM = 768;
  explicit BoundedRawVectorStore(int64_t rows, int64_t fail_batch = -1)
    : rows_(rows), fail_batch_(fail_batch), batches_(0), baseline_(0), peak_extra_(0) {}
  bool is_valid() const override { return true; }
  int64_t get_vector_count() const override { return rows_; }
  int64_t get_dimension() const override { return DIM; }
  static float value(uint64_t vid, int64_t d)
  { return static_cast<float>(vid % 997) * 0.001F + static_cast<float>(d) * 0.01F; }
  int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override
  {
    int ret = vids.reserve(rows_);
    for (int64_t i = 0; OB_SUCC(ret) && i < rows_; ++i) { ret = vids.push_back(i); }
    return ret;
  }
  int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids,
      int64_t begin, int64_t end, ObIArray<float> &vectors) const override
  {
    int ret = OB_SUCCESS;
    if (batches_ == 0) { baseline_ = raw_buffer_hold(); }
    ++batches_;
    if (batches_ == fail_batch_) { return OB_IO_ERROR; }
    // Exercise the real DDL input's reuse/prepare_allocate contract.
    vectors.reuse();
    if (begin < 0 || end <= begin || end > vids.count()) { ret = OB_INVALID_ARGUMENT; }
    else if (OB_FAIL(vectors.prepare_allocate((end - begin) * DIM))) {
    } else {
      peak_extra_ = MAX(peak_extra_, raw_buffer_hold() - baseline_);
      if (peak_extra_ > 32L * 1024 * 1024) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      float *v = &vectors.at((i - begin) * DIM);
      for (int64_t d = 0; d < DIM; ++d) { v[d] = value(vids.at(i), d); }
    }
    return ret;
  }
  int64_t rows_, fail_batch_;
  mutable int64_t batches_, baseline_, peak_extra_;
};

class CountingRawSlice final : public ObITabletSliceWriter
{
public:
  int init(const ObWriteMacroParam &) override { return OB_SUCCESS; }
  void reset() override { rows_ = 0; close_calls_ = 0; }
  int append_row(const ObDatumRow &row) override
  {
    ObIvfSSTableRowDesc desc;
    int ret = desc.init(IVF_SSTABLE_FORMAT_VERSION);
    if (OB_SUCC(ret)) {
      const uint64_t vid = row.storage_datums_[desc.vector_id_col_idx_].get_uint();
      const ObString payload = row.storage_datums_[desc.payload_col_idx_].get_string();
      if (close_calls_ != 0 || vid != rows_ || payload.length() != BoundedRawVectorStore::DIM * sizeof(float)
          || row.storage_datums_[desc.row_type_col_idx_].get_int() != IVF_ROW_TYPE_RAW_VECTOR
          || row.storage_datums_[desc.centroid_id_col_idx_].get_int() != IVF_RAW_VECTOR_CID) {
        ret = OB_INVALID_DATA;
      } else {
        float first = 0, last = 0;
        MEMCPY(&first, payload.ptr(), sizeof(float));
        MEMCPY(&last, payload.ptr() + payload.length() - sizeof(float), sizeof(float));
        if (first != BoundedRawVectorStore::value(vid, 0)
            || last != BoundedRawVectorStore::value(vid, BoundedRawVectorStore::DIM - 1)) {
          ret = OB_INVALID_DATA;
        } else { ++rows_; }
      }
    }
    return ret;
  }
  int append_batch(const ObBatchDatumRows &) override { return OB_NOT_SUPPORTED; }
  int64_t get_row_count() const override { return rows_; }
  int close() override { ++close_calls_; return OB_SUCCESS; }
  TO_STRING_KV(K_(rows), K_(close_calls));
  int64_t rows_ = 0;
  int64_t close_calls_ = 0;
};

class CountingRawProvider final : public ObIVFDDLRawWriterProvider
{
public:
  int create_raw_writer(int64_t task_idx, ObITabletSliceWriter *&writer) override
  {
    writer = task_idx == 0 ? &sink_ : nullptr;
    return writer == nullptr ? OB_INVALID_ARGUMENT : OB_SUCCESS;
  }
  CountingRawSlice sink_;
};

TEST_F(TestIVFSSTableWriter, ddl_raw_batches_have_bounded_memory_and_preserve_payload)
{
  const int64_t rows = 12 * 4096 + 37;
  for (int64_t fail_batch : {-1, 5}) {
    BoundedRawVectorStore store(rows, fail_batch);
    ObIVFVectorReader reader;
    ObArray<ObIVFVectorVID> vids;
    RecordingIVFDDLSliceWriter posting;
    CountingRawProvider raw;
    ObIVFDDLSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ASSERT_EQ(OB_SUCCESS, writer.init(posting, raw, IVF_SNAPSHOT_VERSION,
        BoundedRawVectorStore::DIM, VIDA_L2, true /* input_vectors_are_rotated */));
    // Isolate RAW from quantization, which has its own physical-output tests.
    writer.quantization_finished_ = true;
    writer.centroid_row_count_ = 1;
    writer.one_bit_row_count_ = rows;
    writer.output_row_count_ = rows + 1;
    writer.ivf_meta_.centroid_count_ = 1;
    const int ret = writer.write_raw_vectors(reader, vids, 0, rows, 0);
    EXPECT_EQ(fail_batch < 0 ? OB_SUCCESS : OB_IO_ERROR, ret);
    EXPECT_GE(store.peak_extra_, 12L * 1024 * 1024);
    EXPECT_LE(store.peak_extra_, 32L * 1024 * 1024);
    EXPECT_EQ(fail_batch < 0 ? 13 : fail_batch, store.batches_);
    EXPECT_EQ(fail_batch < 0 ? rows : (fail_batch - 1) * 4096, raw.sink_.get_row_count());
    EXPECT_EQ(1, raw.sink_.close_calls_);
    if (fail_batch < 0) {
      EXPECT_EQ(OB_SUCCESS, writer.finish_raw_write(rows));
      EXPECT_TRUE(writer.is_finished());
      EXPECT_EQ(2 * rows + 1, writer.get_row_count());
    } else {
      EXPECT_FALSE(writer.raw_ranges_[0].finished_);
      EXPECT_EQ(OB_ERR_UNEXPECTED, writer.finish_raw_write(rows));
      EXPECT_FALSE(writer.is_finished());
    }
    // Task-local storage is reclaimed even when the reader fails mid-range.
    EXPECT_LE(raw_buffer_hold() - store.baseline_, 2L * 1024 * 1024);
  }
}

static int prepare_ddl_quantization(ObIVFDDLSSTableWriter &writer,
    RecordingIVFDDLSliceWriter &posting_writer, RecordingIVFDDLRawWriters &raw_writers,
    ObIVFVectorReader &reader, const ObIArray<ObIVFVectorVID> &vids)
{
  int ret = OB_SUCCESS;
  ObSEArray<float, 2> centers;
  if (OB_FAIL(centers.push_back(2.0F))) {
  } else if (OB_FAIL(centers.push_back(3.0F))) {
  } else if (OB_FAIL(writer.init(posting_writer, raw_writers,
      IVF_SNAPSHOT_VERSION, 2 /* vector_dim */, VIDA_L2))) {
  } else if (OB_FAIL(writer.quantize_cluster(reader, centers, 1, 0, vids, nullptr))) {
  } else {
    ret = writer.finish_quantization(2, 1, 2);
  }
  return ret;
}

TEST_F(TestIVFSSTableWriter, ddl_writer_streams_raw_slices_in_reverse_completion_order)
{
  FullRebuildWriterVectorStore store;
  ObIVFVectorReader reader;
  ObSEArray<ObIVFVectorVID, 2> vids;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  RecordingIVFDDLSliceWriter posting;
  RecordingIVFDDLRawWriters raw;
  ObIVFDDLSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_ddl_quantization(writer, posting, raw, reader, vids));
  ASSERT_EQ(3, posting.get_row_count());
  EXPECT_EQ(1, posting.close_calls_);
  EXPECT_EQ(0, raw.create_count_);
  EXPECT_EQ(0, writer.sorter_.get_row_count());
  EXPECT_FALSE(writer.is_finished());
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 1, 2, 1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.finish_raw_write(2));
  EXPECT_FALSE(writer.is_finished());
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 1, 0));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
  EXPECT_TRUE(writer.is_finished());
  EXPECT_EQ(5, writer.get_row_count());
  EXPECT_EQ(2, raw.create_count_);
  EXPECT_EQ(0, writer.sorter_.get_row_count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(1, raw.writers_[i].get_row_count());
    EXPECT_EQ(1, raw.writers_[i].close_calls_);
    const auto &row = raw.writers_[i].rows_[0];
    EXPECT_EQ(IVF_ROW_TYPE_RAW_VECTOR, row.type_);
    EXPECT_EQ(IVF_RAW_VECTOR_CID, row.cid_);
    EXPECT_EQ(vids.at(i), row.vid_);
    ASSERT_EQ(2 * sizeof(float), row.payload_.size());
    float values[2];
    MEMCPY(values, row.payload_.data(), sizeof(values));
    EXPECT_FLOAT_EQ(1.0F + 2 * i, values[0]);
    EXPECT_FLOAT_EQ(2.0F + 2 * i, values[1]);
  }
  EXPECT_EQ(1, writer.get_ivf_meta().centroid_count_);
}

TEST_F(TestIVFSSTableWriter, ddl_writer_rejects_empty_duplicate_and_unordered_ranges)
{
  FullRebuildWriterVectorStore store;
  ObIVFVectorReader reader;
  ObSEArray<ObIVFVectorVID, 2> vids;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  RecordingIVFDDLSliceWriter posting;
  RecordingIVFDDLRawWriters raw;
  ObIVFDDLSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_ddl_quantization(writer, posting, raw, reader, vids));
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.write_raw_vectors(reader, vids, 0, 0, 0));
  EXPECT_EQ(0, raw.create_count_);
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.finish_raw_write(0));
  // Intentionally give the higher VID range a lower slice number. Each
  // individual stream is sorted, but the publication barrier must reject it.
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 1, 2, 0));
  EXPECT_EQ(OB_INIT_TWICE, writer.write_raw_vectors(reader, vids, 1, 2, 0));
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 1, 1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.finish_raw_write(2));
  EXPECT_FALSE(writer.is_finished());
  EXPECT_EQ(2, raw.create_count_);
}

TEST_F(TestIVFSSTableWriter, ddl_writer_failed_raw_slice_cannot_publish)
{
  for (int failure = 0; failure < 3; ++failure) {
    FullRebuildWriterVectorStore store;
    ObIVFVectorReader reader;
    ObSEArray<ObIVFVectorVID, 2> vids;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    RecordingIVFDDLSliceWriter posting;
    RecordingIVFDDLRawWriters raw;
    ObIVFDDLSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, prepare_ddl_quantization(writer, posting, raw, reader, vids));
    raw.fail_create_ = 0 == failure;
    raw.writers_[0].fail_append_ = 1 == failure;
    raw.writers_[0].fail_close_ = 2 == failure;
    EXPECT_EQ(0 == failure ? OB_ALLOCATE_MEMORY_FAILED : OB_IO_ERROR,
        writer.write_raw_vectors(reader, vids, 0, 2, 0));
    EXPECT_EQ(0 == failure ? 0 : 1, raw.writers_[0].close_calls_);
    EXPECT_EQ(OB_ERR_UNEXPECTED, writer.finish_raw_write(2));
    EXPECT_FALSE(writer.is_finished());
    EXPECT_EQ(3, writer.get_row_count());
  }
}

TEST_F(TestIVFSSTableWriter, ddl_writer_failed_posting_close_does_not_start_raw)
{
  FullRebuildWriterVectorStore store;
  ObIVFVectorReader reader;
  ObSEArray<ObIVFVectorVID, 2> vids;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  RecordingIVFDDLSliceWriter posting;
  RecordingIVFDDLRawWriters raw;
  ObIVFDDLSSTableWriter writer(allocator_);
  posting.fail_close_ = true;
  EXPECT_EQ(OB_IO_ERROR, prepare_ddl_quantization(writer, posting, raw, reader, vids));
  EXPECT_EQ(1, posting.close_calls_);
  EXPECT_FALSE(writer.is_finished());
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.write_raw_vectors(reader, vids, 0, 2, 0));
  EXPECT_EQ(0, raw.create_count_);
}

TEST_F(TestIVFSSTableWriter, ddl_loaded_quantization_matches_cluster_output_with_secondary)
{
  for (bool rotated : {false, true}) {
    FullRebuildWriterVectorStore store;
    ObIVFVectorReader reader;
    ObArray<ObIVFVectorVID> vids, batch_vids;
    ObArray<float> centers, values;
    ObArray<int64_t> primary, secondary;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    for (float value : {-3.0F, 1.0F, 10.0F, -2.0F}) { ASSERT_EQ(OB_SUCCESS, centers.push_back(value)); }
    RecordingIVFDDLSliceWriter old_posting, new_posting;
    RecordingIVFDDLRawWriters old_raw, new_raw;
    ObIVFDDLSSTableWriter reference(allocator_), writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, reference.init(old_posting, old_raw, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, rotated));
    ASSERT_EQ(OB_SUCCESS, writer.init(new_posting, new_raw, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, rotated));
    // Same coordinate system for the two independently initialized sinks.
    writer.rotator_ = reference.rotator_;
    for (int64_t cid = 0; cid < 2; ++cid) {
      ObArray<ObIVFVectorVID> own, extra;
      ASSERT_EQ(OB_SUCCESS, own.push_back(vids.at(cid)));
      ASSERT_EQ(OB_SUCCESS, extra.push_back(vids.at(1 - cid)));
      ASSERT_EQ(OB_SUCCESS, reference.quantize_cluster(reader, centers, 2, cid, own, &extra));
    }
    ASSERT_EQ(OB_SUCCESS, reference.finish_quantization(2, 2, 4));
    // Source order deliberately differs from business VID and cluster order.
    ASSERT_EQ(OB_SUCCESS, batch_vids.push_back(vids.at(1)));
    ASSERT_EQ(OB_SUCCESS, batch_vids.push_back(vids.at(0)));
    ASSERT_EQ(OB_SUCCESS, reader.batch_get_by_vid(batch_vids, 0, 2, values));
    ASSERT_EQ(OB_SUCCESS, primary.push_back(1)); ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
    ASSERT_EQ(OB_SUCCESS, secondary.push_back(0)); ASSERT_EQ(OB_SUCCESS, secondary.push_back(1));
    secondary.at(1) = 0;
    EXPECT_EQ(OB_INVALID_DATA, writer.quantize_loaded_vectors(centers, 2, batch_vids, values, primary, &secondary));
    EXPECT_EQ(0, writer.get_row_count());
    secondary.at(1) = 1;
    ASSERT_EQ(OB_SUCCESS, writer.quantize_loaded_vectors(centers, 2, batch_vids, values, primary, &secondary));
    ObArray<uint32_t> counts;
    ASSERT_EQ(OB_SUCCESS, counts.push_back(2));
    ASSERT_EQ(OB_SUCCESS, counts.push_back(2));
    ASSERT_EQ(OB_SUCCESS, writer.set_leaf_posting_counts(counts));
    ASSERT_EQ(OB_SUCCESS, writer.write_loaded_centers(centers, 2, 1, 2));
    ASSERT_EQ(OB_SUCCESS, writer.write_loaded_centers(centers, 2, 0, 1));
    ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 2, 4));
    ASSERT_EQ(old_posting.rows_.size(), new_posting.rows_.size());
    for (size_t i = 0; i < old_posting.rows_.size(); ++i) {
      const auto &old = old_posting.rows_[i], &current = new_posting.rows_[i];
      EXPECT_EQ(old.type_, current.type_); EXPECT_EQ(old.cid_, current.cid_); EXPECT_EQ(old.vid_, current.vid_);
      EXPECT_EQ(old.payload_, current.payload_);
    }
    ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 2, 0));
    ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
    EXPECT_TRUE(writer.is_finished());
  }
}

TEST_F(TestIVFSSTableWriter, ddl_loaded_single_center_quantization_matches_cluster_output)
{
  for (const bool rotated : {false, true}) {
    FullRebuildWriterVectorStore store;
    ObIVFVectorReader reader;
    ObArray<ObIVFVectorVID> vids;
    ObArray<float> centers, values;
    ObArray<int64_t> primary;
    ASSERT_EQ(OB_SUCCESS, reader.init(store));
    ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
    ASSERT_EQ(2, vids.count());
    ASSERT_EQ(OB_SUCCESS, centers.push_back(3.0F));
    ASSERT_EQ(OB_SUCCESS, centers.push_back(-1.0F));
    RecordingIVFDDLSliceWriter old_posting, new_posting;
    RecordingIVFDDLRawWriters old_raw, new_raw;
    ObIVFDDLSSTableWriter reference(allocator_), writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, reference.init(old_posting, old_raw, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, rotated));
    ASSERT_EQ(OB_SUCCESS, writer.init(new_posting, new_raw, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, rotated));
    writer.rotator_ = reference.rotator_;
    ASSERT_EQ(OB_SUCCESS, reference.quantize_cluster(reader, centers, 1, 0, vids, nullptr));
    ASSERT_EQ(OB_SUCCESS, reference.finish_quantization(2, 1, 2));
    ASSERT_EQ(OB_SUCCESS, reader.batch_get_by_vid(vids, 0, 2, values));
    ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
    ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
    ASSERT_EQ(OB_SUCCESS, writer.quantize_loaded_vectors(centers, 2, vids, values, primary, nullptr));
    ObArray<uint32_t> counts;
    ASSERT_EQ(OB_SUCCESS, counts.push_back(2));
    ASSERT_EQ(OB_SUCCESS, writer.set_leaf_posting_counts(counts));
    ASSERT_EQ(OB_SUCCESS, writer.write_loaded_centers(centers, 2, 0, 1));
    ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 1, 2));
    ASSERT_EQ(old_posting.rows_.size(), new_posting.rows_.size());
    for (size_t i = 0; i < old_posting.rows_.size(); ++i) {
      const auto &old = old_posting.rows_[i], &current = new_posting.rows_[i];
      EXPECT_EQ(old.type_, current.type_);
      EXPECT_EQ(old.cid_, current.cid_);
      EXPECT_EQ(old.vid_, current.vid_);
      EXPECT_EQ(old.payload_, current.payload_);
    }
    ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 2, 0));
    ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
    EXPECT_TRUE(writer.is_finished());
  }
}

TEST_F(TestIVFSSTableWriter, partitioned_posting_output_matches_serial_and_fails_closed)
{
  for (int variant = 0; variant < 12; ++variant) {
    SCOPED_TRACE(variant);
    const int mode = variant % 6;
    const bool grouped = variant >= 6;
    const int64_t k = 5, n = 8193, workers = 3;
    ObArray<float> centers;
    for (int64_t i = 0; i < k * 2; ++i) { ASSERT_EQ(OB_SUCCESS, centers.push_back(float(i))); }
    RecordingIVFDDLSliceWriter expected, actual;
    RecordingIVFDDLRawWriters old_provider, provider;
    ObIVFDDLSSTableWriter serial(allocator_), parallel(allocator_);
    ASSERT_EQ(OB_SUCCESS, serial.init(expected, old_provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, true));
    ASSERT_EQ(OB_SUCCESS, parallel.init(actual, provider, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, true));
    parallel.rotator_ = serial.rotator_;
    ObIvfCentroidLayout layout(allocator_);
    if (grouped) {
      ASSERT_EQ(OB_SUCCESS, layout.build(centers, 2, ObHcaTrainParam(), ObHcaSoarParam(),
          nullptr, 1 /* threshold */, 2 /* parent target */));
      ASSERT_TRUE(layout.is_hierarchical());
    } else { ASSERT_EQ(OB_SUCCESS, layout.init_flat(k, 2)); }
    ASSERT_EQ(OB_SUCCESS, serial.set_centroid_layout(layout));
    ObArray<uint32_t> counts;
    ASSERT_EQ(OB_SUCCESS, counts.prepare_allocate(k));
    MEMSET(counts.get_data(), 0, k * sizeof(uint32_t));
    for (int64_t i = 0; i < n; ++i) {
      ++counts.at(mode == 1 ? 0 : i % k);
      if (mode != 1) { ++counts.at((i + 1) % k); }
    }
    ASSERT_EQ(OB_SUCCESS, serial.set_leaf_posting_counts(counts));
    ASSERT_EQ(OB_SUCCESS, parallel.prepare_loaded_output(k, workers));
    ASSERT_EQ(workers, parallel.posting_write_task_count());
    auto encode = [&](ObIVFDDLSSTableWriter &writer, int64_t begin) {
      ObArray<ObIVFVectorVID> vids;
      ObArray<float> values;
      ObArray<int64_t> primary, secondary;
      int ret = OB_SUCCESS;
      for (int64_t i = begin; OB_SUCC(ret) && i < MIN(n, begin + 4096); ++i) {
        const uint64_t vid = i == 0 ? 0 : (1ULL << 48) + i;
        if (OB_FAIL(vids.push_back(vid)) || OB_FAIL(values.push_back(float(i % 17)))
            || OB_FAIL(values.push_back(float(i % 31)))
            || OB_FAIL(primary.push_back(mode == 1 ? 0 : i % k))
            || OB_FAIL(secondary.push_back(mode == 1 ? -1 : (i + 1) % k))) { break; }
      }
      return OB_FAIL(ret) ? ret : writer.quantize_loaded_vectors(centers, 2, vids, values, primary, &secondary);
    };
    for (int64_t begin = 0; begin < n; begin += 4096) { ASSERT_EQ(OB_SUCCESS, encode(serial, begin)); }
    ASSERT_EQ(OB_SUCCESS, serial.write_loaded_centers(centers, 2, 0, k));
    const int64_t postings = mode == 1 ? n : n * 2;
    ASSERT_EQ(OB_SUCCESS, serial.finish_quantization(n, k, postings));
    ObTenantBase *tenant = ObTenantEnv::get_tenant();
    int results[3] = {OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED};
    std::vector<std::thread> threads;
    for (int64_t i = 0; i < workers; ++i) {
      threads.emplace_back([&, i] {
        ObTenantEnv::set_tenant(tenant);
        THIS_WORKER.set_timeout_ts(INT64_MAX);
        results[i] = encode(parallel, i * 4096);
        ObTenantEnv::set_tenant(nullptr);
      });
    }
    for (auto &thread : threads) { thread.join(); }
    for (int ret : results) { ASSERT_EQ(OB_SUCCESS, ret); }
    // Fused SOAR has already accepted logical-CID postings before the parent
    // stage installs the persisted layout and the final leaf counts.
    ASSERT_EQ(OB_SUCCESS, parallel.set_leaf_posting_counts(counts));
    ASSERT_EQ(OB_SUCCESS, parallel.set_centroid_layout(layout));
    EXPECT_EQ(OB_STATE_NOT_MATCH, parallel.finish_quantization(n, k, postings));
    if (mode == 2) { provider.posting_writers_[0].fail_append_ = true; }
    if (mode == 3) { provider.posting_writers_[0].fail_close_ = true; }
    if (mode == 4) { provider.fail_posting_create_ = 0; }
    if (mode == 5) { actual.fail_append_ = true; }
    threads.clear();
    std::atomic<int64_t> next(0), failures(0);
    for (int64_t i = 0; i < workers; ++i) {
      threads.emplace_back([&] {
        ObTenantEnv::set_tenant(tenant);
        THIS_WORKER.set_timeout_ts(INT64_MAX);
        for (int64_t task = next.fetch_add(1); task <= workers; task = next.fetch_add(1)) {
          const int ret = task == 0 ? parallel.write_loaded_centers(centers, 2, 0, k)
              : parallel.write_posting_range(task - 1);
          if (ret != OB_SUCCESS) { ++failures; }
        }
        ObTenantEnv::set_tenant(nullptr);
      });
    }
    for (auto &thread : threads) { thread.join(); }
    if (mode >= 2) {
      EXPECT_GT(failures.load(), 0);
      EXPECT_NE(OB_SUCCESS, parallel.finish_quantization(n, k, postings));
      const int64_t accepted = provider.posting_writers_[0].rows_.size();
      EXPECT_NE(OB_SUCCESS, parallel.write_posting_range(0));
      EXPECT_EQ(accepted, provider.posting_writers_[0].rows_.size());
      EXPECT_FALSE(parallel.is_finished());
    } else {
      ASSERT_EQ(0, failures.load());
      EXPECT_EQ(OB_INIT_TWICE, parallel.write_posting_range(0));
      ASSERT_EQ(OB_SUCCESS, parallel.finish_quantization(n, k, postings));
      EXPECT_EQ(n, parallel.get_ivf_meta().vector_count_);
      std::vector<RecordingIVFDDLSliceWriter::Row> rows = actual.rows_;
      EXPECT_EQ(1, actual.close_calls_);
      EXPECT_EQ(layout.center_row_count(), actual.get_row_count());
      EXPECT_EQ(grouped, parallel.get_ivf_meta().is_hierarchical());
      for (const auto &center : actual.rows_) {
        int64_t self = 0, group = 0;
        uint32_t child_count = 0;
        const float *vector = nullptr;
        ASSERT_EQ(OB_SUCCESS, parallel.get_ivf_meta().decode_centroid_key(center.cid_, center.vid_, self, group));
        ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(
            ObString(center.payload_.size(), center.payload_.data()), 2, vector, child_count));
        if (grouped && group == 0) {
          EXPECT_EQ(layout.group_count(self - 1), child_count);
        } else { EXPECT_EQ(counts.at(self - layout.parent_count() - 1), child_count); }
      }
      for (int64_t i = 0; i < workers; ++i) {
        EXPECT_EQ(1, provider.posting_writers_[i].close_calls_);
        rows.insert(rows.end(), provider.posting_writers_[i].rows_.begin(), provider.posting_writers_[i].rows_.end());
      }
      ASSERT_EQ(expected.rows_.size(), rows.size());
      for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(expected.rows_[i].type_, rows[i].type_);
        EXPECT_EQ(expected.rows_[i].cid_, rows[i].cid_);
        EXPECT_EQ(expected.rows_[i].vid_, rows[i].vid_);
        EXPECT_EQ(expected.rows_[i].payload_, rows[i].payload_);
      }
    }
  }
}

// Uses the real macro writer and shared index builder. Environment overrides
// make the same case usable as a serial/parallel benchmark in separate processes.
TEST_F(TestIVFSSTableWriter, partitioned_macro_output_round_trip)
{
  const auto setting = [](const char *name, int64_t fallback) {
    const char *value = std::getenv(name);
    return value == nullptr ? fallback : std::strtoll(value, nullptr, 10);
  };
  // Keep benchmark measurements independent of per-microblock INFO logging.
  if (std::getenv("IVF_BENCH_ROWS") != nullptr) { OB_LOGGER.set_log_level("WARN"); }
  const int64_t n = setting("IVF_BENCH_ROWS", 16385);
  const int64_t dim = setting("IVF_BENCH_DIM", 8);
  const int64_t workers = setting("IVF_BENCH_WORKERS", 4);
  const bool staged = setting("IVF_BENCH_STAGED", 1) != 0;
  const int64_t k = 257;
  ASSERT_GT(n, 0);
  ASSERT_GT(dim, 0);
  ASSERT_GT(workers, 0);
  ASSERT_LE(workers, ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(workers + 1));
  class Slice final : public ObITabletSliceWriter {
  public:
    int init(const ObWriteMacroParam &) override { return OB_NOT_SUPPORTED; }
    void reset() override { writer_.reset(); }
    int append_row(const ObDatumRow &row) override { return writer_.append_row(row); }
    int append_batch(const ObBatchDatumRows &rows) override { return writer_.append_batch(rows); }
    int64_t get_row_count() const override { return writer_.get_appended_row_count(); }
    int close() override { return writer_.close(); }
    TO_STRING_KV("rows", get_row_count());
    ObIvfMacroBlockWriter writer_;
  };
  class Provider final : public ObIVFDDLRawWriterProvider {
  public:
    int create_raw_writer(int64_t, ObITabletSliceWriter *&writer) override {
      writer = nullptr; return OB_NOT_SUPPORTED;
    }
    int create_posting_writer(int64_t idx, ObITabletSliceWriter *&writer) override {
      writer = slices_.at(idx + 1).get(); return OB_SUCCESS;
    }
    std::vector<std::unique_ptr<Slice>> slices_;
  } provider;
  for (int64_t i = 0; i < (staged ? workers + 1 : 1); ++i) {
    provider.slices_.emplace_back(new Slice());
    ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(provider.slices_.back()->writer_, i * 1000000));
  }
  ObIVFDDLSSTableWriter output(allocator_);
  ASSERT_EQ(OB_SUCCESS, output.init(*provider.slices_[0], provider,
      IVF_SNAPSHOT_VERSION, dim, VIDA_L2, true));
  if (staged) { ASSERT_EQ(OB_SUCCESS, output.prepare_loaded_output(k, workers)); }
  ObArray<float> centers;
  ASSERT_EQ(OB_SUCCESS, centers.prepare_allocate(k * dim));
  for (int64_t i = 0; i < k * dim; ++i) { centers.at(i) = float(i % 193) / 19.0F; }
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  std::atomic<int64_t> next(0), failures(0);
  const auto run = [&](int64_t tasks, const std::function<int(int64_t)> &function) {
    next = 0;
    std::vector<std::thread> threads;
    for (int64_t i = 0; i < workers; ++i) {
      threads.emplace_back([&] {
        ObTenantEnv::set_tenant(tenant);
        THIS_WORKER.set_timeout_ts(INT64_MAX);
        for (int64_t task = next.fetch_add(1); task < tasks; task = next.fetch_add(1)) {
          if (function(task) != OB_SUCCESS) { ++failures; }
        }
        ObTenantEnv::set_tenant(nullptr);
      });
    }
    for (auto &thread : threads) { thread.join(); }
  };
  const int64_t started = ObTimeUtility::current_time();
  run((n + 4095) / 4096, [&](int64_t task) {
    ObArray<ObIVFVectorVID> vids;
    ObArray<float> values;
    ObArray<int64_t> primary, secondary;
    int ret = OB_SUCCESS;
    const int64_t begin = task * 4096, end = MIN(n, begin + 4096);
    if (OB_FAIL(vids.prepare_allocate(end - begin))
        || OB_FAIL(values.prepare_allocate((end - begin) * dim))
        || OB_FAIL(primary.prepare_allocate(end - begin))
        || OB_FAIL(secondary.prepare_allocate(end - begin))) { return ret; }
    for (int64_t i = begin; i < end; ++i) {
      vids.at(i - begin) = i + 1;
      primary.at(i - begin) = i % k;
      secondary.at(i - begin) = (i + 1) % k;
      for (int64_t d = 0; d < dim; ++d) {
        values.at((i - begin) * dim + d) = float((i * 37 + d * 11) % 191) / 19.0F;
      }
    }
    return output.quantize_loaded_vectors(centers, dim, vids, values, primary, &secondary);
  });
  ASSERT_EQ(0, failures.load());
  const int64_t encoded = ObTimeUtility::current_time();
  ObArray<uint32_t> counts;
  ASSERT_EQ(OB_SUCCESS, counts.prepare_allocate(k));
  MEMSET(counts.get_data(), 0, k * sizeof(uint32_t));
  for (int64_t i = 0; i < n; ++i) {
    ++counts.at(i % k);
    ++counts.at((i + 1) % k);
  }
  ASSERT_EQ(OB_SUCCESS, output.set_leaf_posting_counts(counts));
  if (staged) {
    run(workers + 1, [&](int64_t task) {
      return task == 0 ? output.write_loaded_centers(centers, dim, 0, k)
          : output.write_posting_range(task - 1);
    });
    ASSERT_EQ(0, failures.load());
  } else { ASSERT_EQ(OB_SUCCESS, output.write_loaded_centers(centers, dim, 0, k)); }
  ASSERT_EQ(OB_SUCCESS, output.finish_quantization(n, k, 2 * n));
  const int64_t written = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(dim));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(k + 2 * n, sstable_.get_row_count());
  ASSERT_TRUE(sstable_.is_valid());
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObDatumRange range;
  range.set_whole_range();
  ObIMacroBlockIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable_.scan_macro_block(range, read_info, allocator_, iter,
      false, false, true));
  int64_t count = 0, macro_count = 0;
  ObMacroBlockDesc desc;
  ObDataMacroBlockMeta meta;
  desc.macro_meta_ = &meta;
  for (;;) {
    const int ret = iter->get_next_macro_block(desc);
    if (ret == OB_ITER_END) { break; }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(desc.macro_block_id_.is_valid());
    count += desc.row_count_;
    ++macro_count;
  }
  iter->~ObIMacroBlockIterator();
  allocator_.free(iter);
  EXPECT_EQ(k + 2 * n, count);
  fprintf(stdout, "OUTPUT_BENCH staged=%d workers=%ld n=%ld dim=%ld encode_us=%ld "
      "output_us=%ld total_us=%ld macro_count=%ld\n", staged, workers, n, dim,
      encoded - started, written - encoded, written - started, macro_count);
}

TEST_F(TestIVFSSTableWriter, storage_sort_orders_rows_and_releases_files)
{
  ObArray<int64_t> before, after;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), before));
  const int64_t rows = 131073;
  ObIVFSorter sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(MTL_ID()));
  char payload[128];
  for (int64_t i = rows; i > 0; --i) {
    MEMSET(payload, i % 127, sizeof(payload));
    ObIVFBuildRow row;
    row.row_type_ = IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
    row.cid_ = i % 17;
    row.vid_ = i;
    row.payload_.assign_ptr(payload, sizeof(payload));
    ASSERT_EQ(OB_SUCCESS, sorter.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, sorter.finish());
  int64_t count = 0;
  for (int64_t cid = 0; cid < 17; ++cid) {
    for (int64_t i = cid == 0 ? 17 : cid; i <= rows; i += 17) {
      const ObIVFSortItem *row = nullptr;
      ASSERT_EQ(OB_SUCCESS, sorter.get_next_item(row));
      ASSERT_NE(nullptr, row);
      EXPECT_EQ(cid, row->cid_);
      EXPECT_EQ(i, row->vid_);
      EXPECT_EQ(char(i % 127), row->payload_.ptr()[127]);
      ++count;
    }
  }
  EXPECT_EQ(rows, count);
  sorter.reset();
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), after));
  EXPECT_EQ(before.count(), after.count());
}

TEST_F(TestIVFSSTableWriter, storage_sorter_orders_key_addon_across_batches)
{
  static const int64_t BATCH_SIZE = 4096;
  static const int64_t PAYLOAD_SIZE = 128;
  for (const int64_t row_count : {0L, 1L, BATCH_SIZE - 1, BATCH_SIZE, BATCH_SIZE + 1, 2 * BATCH_SIZE + 1}) {
    SCOPED_TRACE(row_count);
    ObIVFSorter sorter;
    ASSERT_EQ(OB_SUCCESS, sorter.init(MTL_ID()));
    char payload[PAYLOAD_SIZE];
    for (int64_t i = 0; i < row_count; ++i) {
      const uint64_t vid = row_count - i;
      MEMSET(payload, static_cast<char>(vid % 127), sizeof(payload));
      ObIVFBuildRow row;
      row.row_type_ = blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
      row.cid_ = 0;
      row.vid_ = vid;
      row.payload_.assign_ptr(payload, sizeof(payload));
      ASSERT_EQ(OB_SUCCESS, sorter.append_row(row));
      if ((i + 1) % BATCH_SIZE == 0) {
        // Recycle scratch-sized allocations before later batches/sort consume
        // the rows. Stored row addresses must not depend on the scratch array.
        ObArray<void *> scratch;
        scratch.set_attr(ObMemAttr(MTL_ID(), "IVFSortInput"));
        ASSERT_EQ(OB_SUCCESS, scratch.prepare_allocate(2 * BATCH_SIZE));
        MEMSET(scratch.get_data(), 0, scratch.count() * sizeof(void *));
      }
    }
    MEMSET(payload, 'x', sizeof(payload));

    ASSERT_EQ(row_count, sorter.get_row_count());
    ASSERT_EQ(OB_SUCCESS, sorter.finish());
    EXPECT_EQ(OB_INIT_TWICE, sorter.finish());
    for (const int64_t pointer_count : {2L, 2 * BATCH_SIZE}) {
      ObArray<void *> scratch;
      scratch.set_attr(ObMemAttr(MTL_ID(), "IVFSortInput"));
      ASSERT_EQ(OB_SUCCESS, scratch.prepare_allocate(pointer_count));
      MEMSET(scratch.get_data(), 0, scratch.count() * sizeof(void *));
    }
    for (uint64_t expected_vid = 1; expected_vid <= static_cast<uint64_t>(row_count);
         ++expected_vid) {
      const ObIVFSortItem *item = nullptr;
      ASSERT_EQ(OB_SUCCESS, sorter.get_next_item(item));
      ASSERT_NE(nullptr, item);
      EXPECT_EQ(blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR, item->row_type_);
      EXPECT_EQ(0, item->cid_);
      EXPECT_EQ(expected_vid, item->vid_);
      ASSERT_EQ(PAYLOAD_SIZE, item->payload_.length());
      EXPECT_EQ(static_cast<char>(expected_vid % 127), item->payload_.ptr()[0]);
      EXPECT_EQ(static_cast<char>(expected_vid % 127), item->payload_.ptr()[PAYLOAD_SIZE - 1]);
    }
    const ObIVFSortItem *end_item = nullptr;
    EXPECT_EQ(OB_ITER_END, sorter.get_next_item(end_item));
    sorter.reset();
    sorter.reset();
  }
}

// Production MemoryDump label scanning is disabled under ASAN.
#ifndef OB_USE_ASAN
static int64_t get_tenant_label_used(const char *label_name)
{
  auto &dump = ObMemoryDump::get_instance();
  EXPECT_TRUE(dump.is_inited());
  if (!dump.is_inited()) { return -1; }
  ObMemoryDumpTask *task = dump.alloc_task();
  EXPECT_NE(nullptr, task);
  if (nullptr == task) { return -1; }
  // Refresh the same label snapshot used by GV$OB_MEMORY synchronously.
  task->type_ = STAT_LABEL;
  task->memory_check_ctx_ = nullptr;
  task->min_print_size_ = INT64_MAX;
  dump.handle(task);
  lib::ObLabel label(label_name);
  ObLabelItem item;
  lib::get_tenant_label_memory(MTL_ID(), label, item);
  return item.used_;
}

TEST_F(TestIVFSSTableWriter, storage_sorter_finish_releases_each_shards_input_independently)
{
  const int64_t input_before = get_tenant_label_used("IVFSortInput");
  const int64_t payload_before = get_tenant_label_used("IVFSortPayload");
  ObIVFSorter first, pending;
  ObIVFBuildRow row;
  row.row_type_ = IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
  row.cid_ = 0;
  row.vid_ = 2;
  row.payload_ = ObString::make_string("pending");
  ASSERT_EQ(OB_SUCCESS, pending.init(MTL_ID()));
  ASSERT_EQ(OB_SUCCESS, pending.append_row(row));
  const int64_t pending_input = get_tenant_label_used("IVFSortInput");
  const int64_t pending_payload = get_tenant_label_used("IVFSortPayload");
  ASSERT_GT(pending_input, input_before);
  ASSERT_GT(pending_payload, payload_before);

  ASSERT_EQ(OB_SUCCESS, first.init(MTL_ID()));
  row.payload_ = ObString::make_string("first");
  ASSERT_EQ(OB_SUCCESS, first.append_row(row));
  ASSERT_EQ(OB_SUCCESS, first.finish());
  EXPECT_EQ(pending_input, get_tenant_label_used("IVFSortInput"));
  EXPECT_EQ(pending_payload, get_tenant_label_used("IVFSortPayload"));
  EXPECT_EQ(OB_INVALID_ARGUMENT, first.append_row(row));

  // A different shard must still accept its final batch while the first
  // shard's sorted result remains alive and has not been read yet.
  row.vid_ = 1;
  row.payload_ = ObString::make_string("later");
  ASSERT_EQ(OB_SUCCESS, pending.append_row(row));
  ASSERT_EQ(OB_SUCCESS, pending.finish());
  EXPECT_EQ(input_before, get_tenant_label_used("IVFSortInput"));
  EXPECT_EQ(payload_before, get_tenant_label_used("IVFSortPayload"));
  const ObIVFSortItem *item = nullptr;
  ASSERT_EQ(OB_SUCCESS, first.get_next_item(item));
  ASSERT_NE(nullptr, item);
  EXPECT_EQ(2, item->vid_);
  EXPECT_EQ(ObString::make_string("first"), item->payload_);
  EXPECT_EQ(OB_ITER_END, first.get_next_item(item));
  for (int64_t vid = 1; vid <= 2; ++vid) {
    ASSERT_EQ(OB_SUCCESS, pending.get_next_item(item));
    ASSERT_NE(nullptr, item);
    EXPECT_EQ(vid, item->vid_);
    EXPECT_EQ(vid == 1 ? ObString::make_string("later") : ObString::make_string("pending"), item->payload_);
  }
  EXPECT_EQ(OB_ITER_END, pending.get_next_item(item));
  first.reset();
  ASSERT_EQ(OB_SUCCESS, first.init(MTL_ID()));
  ASSERT_EQ(OB_SUCCESS, first.append_row(row));
  ASSERT_EQ(OB_SUCCESS, first.finish());
  ASSERT_EQ(OB_SUCCESS, first.get_next_item(item));
  ASSERT_NE(nullptr, item);
  EXPECT_EQ(1, item->vid_);
  EXPECT_EQ(ObString::make_string("later"), item->payload_);
}
#endif

namespace {
class ParallelSampleStore final : public ObIVFVectorStore, public ObIVFFirstLevelSampleSet
{
public:
  static constexpr int64_t ROWS = 32769;
  static constexpr int64_t DIM = 768;
  mutable std::atomic<int64_t> reads_{0};
  mutable std::atomic<int64_t> buffer_changes_{0};
  bool fail_read_ = false;
  int64_t cancel_after_reads_ = 1;
  ObIVFMergeProcessor *cancel_on_read_ = nullptr;
  bool is_valid() const override { return true; }
  bool source_enabled_ = true;
  bool supports_source_order() const override { return source_enabled_; }
  int64_t get_vector_count() const override { return ROWS; }
  int64_t get_sample_count() const override { return 1; }
  int64_t get_dimension() const override { return DIM; }
  int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS; vids.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < ROWS; ++i) { ret = vids.push_back(i + 1); }
    return ret;
  }
  int get_source_row_idx(const int64_t idx, int64_t &source) const override
  { source = ROWS - 1 - idx; return idx >= 0 && idx < ROWS ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
  static float value(const int64_t idx, const int64_t d) { return float(idx % 127) + float(d) / 1024; }
  int batch_get_by_source(const ObIArray<int64_t> &idxs, ObIArray<float> &values) const override
  {
    ++reads_;
    if (fail_read_) { return OB_IO_ERROR; }
    // The real DDL adapter also calls reuse(). Multiple full batches must
    // reuse the allocation, rather than retaining one arena buffer per read.
    const float *before = values.empty() ? nullptr : &values.at(0);
    const int64_t old_count = values.count();
    values.reuse();
    int ret = values.prepare_allocate(idxs.count() * DIM);
    if (OB_SUCC(ret) && before != nullptr && old_count == values.count()
        && before != &values.at(0)) { ++buffer_changes_; }
    for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
      if (i > 0 && idxs.at(i - 1) <= idxs.at(i)) { ret = OB_INVALID_DATA; }
      for (int64_t d = 0; OB_SUCC(ret) && d < DIM; ++d) { values.at(i * DIM + d) = value(idxs.at(i), d); }
    }
    if (cancel_on_read_ != nullptr && reads_ >= cancel_after_reads_) { cancel_on_read_->cancel(OB_CANCELED); }
    return ret;
  }
  int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &, int64_t, int64_t, ObIArray<float> &) const override
  { return OB_NOT_SUPPORTED; }
  int batch_get_samples(int64_t, int64_t, ObIArray<ObIVFVectorVID> &, ObIArray<float> &) const override
  { return OB_NOT_SUPPORTED; }
};

class ParallelSampleAlgorithm final : public ObIVFDDLHcaAdapter
{
public:
  bool hierarchy_case_ = false;
  bool prepared_enabled_ = true;
  bool supports_prepared_cluster() const override { return prepared_enabled_; }
  int hca_train_local_centers(int64_t depth, int64_t node, ObIVFVectorReader &reader,
      const ObIArray<ObIVFVectorVID> &vids, const ObIVFFirstLevelSampleSet *samples,
      int64_t cluster_size, int64_t &k, ObIArray<float> &centers) override
  {
    UNUSEDx(node, reader, vids, samples, cluster_size);
    if (!hierarchy_case_ || depth != 0) { return OB_NOT_SUPPORTED; }
    k = 2;
    int ret = OB_SUCCESS;
    centers.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < k * ParallelSampleStore::DIM; ++i) {
      ret = centers.push_back(0.0F);
    }
    return ret;
  }
  int hca_assign_vectors(ObIVFVectorReader &reader, const ObIArray<ObIVFVectorVID> &vids,
      const ObIArray<float> &centers, int64_t k, int64_t begin, int64_t end,
      ObIArray<int64_t> &cids) override
  {
    UNUSEDx(reader, centers);
    if (!hierarchy_case_ || k != 2) { return OB_NOT_SUPPORTED; }
    int ret = OB_SUCCESS;
    cids.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      ret = cids.push_back((vids.at(i) - 1) % 2);
    }
    return ret;
  }
  int hca_collect_training_rows(const int64_t count, const int64_t, ObIArray<int64_t> &ordinals) override
  {
    int ret = OB_SUCCESS; ordinals.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) { ret = ordinals.push_back(i); }
    return ret;
  }
  int hca_train_loaded_samples(const int64_t count, const int64_t, const int64_t dim,
      const ObIArray<int64_t> &ordinals, const ObIArray<float> &values,
      int64_t &k, ObIArray<float> &centers) override
  {
    int ret = OB_SUCCESS;
    const int64_t parity = count == (ParallelSampleStore::ROWS + 1) / 2 ? 0 : 1;
    const bool valid_count = hierarchy_case_
        ? count == (ParallelSampleStore::ROWS + 1 - parity) / 2 : count == ParallelSampleStore::ROWS;
    if (!valid_count || dim != ParallelSampleStore::DIM
        || ordinals.count() != count || values.count() != count * dim) { ret = OB_INVALID_DATA; }
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (ordinals.at(i) != i) { ret = OB_INVALID_DATA; }
      for (int64_t d = 0; OB_SUCC(ret) && d < dim; ++d) {
        const int64_t idx = hierarchy_case_ ? 2 * i + parity : i;
        if (values.at(i * dim + d) != ParallelSampleStore::value(idx, d)) { ret = OB_INVALID_DATA; }
      }
    }
    k = 1; centers.reuse();
    for (int64_t d = 0; OB_SUCC(ret) && d < dim; ++d) { ret = centers.push_back(0.0F); }
    return ret;
  }
};
} // namespace

TEST_F(TestIVFSSTableWriter, sample_prepare_step_is_shared_by_ddl_merge_and_monitored_separately)
{
  class PerfObserver final : public ObIVFMergePerfObserver
  {
  public:
    void on_step_perf(const ObIVFMergeStepPerf &perf) override { snapshots_.push_back(perf); }
    std::vector<ObIVFMergeStepPerf> snapshots_;
  };
  const auto hierarchy = ObIVFMergeStage::HIERARCHY;
  const auto sample_step = ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE;
  ObArray<int64_t> initial_fds, final_fds;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), initial_fds));
  // DDL and FULL_REBUILD share source-ordered sample preparation. Keep
  // checking the fallback when source-order or prepared samples are disabled.
  for (int scenario = 0; scenario < 4; ++scenario) {
    SCOPED_TRACE(scenario);
    ParallelSampleStore store;
    store.source_enabled_ = scenario != 2;
    ParallelSampleAlgorithm algorithm;
    algorithm.hierarchy_case_ = true;
    algorithm.prepared_enabled_ = scenario != 3;
    ObIVFHcaAlgorithmParam param;
    ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
    ObIVFMergeInput input;
    ASSERT_EQ(OB_SUCCESS, input.init(store, store, 256, false, true, false, VIDA_L2));
    ObIVFDDLSSTableWriter unused_output(allocator_);
    ObIVFMergeProcessor processor;
    const auto type = scenario == 1 ? ObIVFProcessType::FULL_REBUILD : ObIVFProcessType::INITIAL_BUILD;
    ASSERT_EQ(OB_SUCCESS, processor.init(allocator_, input, algorithm, unused_output, type));
    if (store.source_enabled_) {
      ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 2));
    }
    const bool prepared_samples = scenario == 0 || scenario == 1;
    int64_t task_count = 0;
    EXPECT_EQ(OB_INVALID_ARGUMENT, processor.prepare_step(hierarchy, sample_step, 0, 0, task_count));
    if (!prepared_samples) {
      EXPECT_EQ(OB_STATE_NOT_MATCH, processor.prepare_step(hierarchy, sample_step, 1, 0, task_count));
    } else {
      // The enabled path cannot bypass its sample preparation barrier.
      EXPECT_EQ(OB_STATE_NOT_MATCH, processor.prepare_step(
          hierarchy, ObIVFMergeStep::HIERARCHY_CLUSTER, 1, 0, task_count));
    }
    PerfObserver observer;
    ObIVFFullBuildPlanner planner;
    ASSERT_EQ(OB_SUCCESS, planner.init(allocator_, processor, type));
    planner.set_perf_observer(&observer);
    const ObIVFTaskPlan *plan = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.start(plan));
    for (const auto root_step : {ObIVFMergeStep::HIERARCHY_CLUSTER,
        ObIVFMergeStep::HIERARCHY_ASSIGN, ObIVFMergeStep::HIERARCHY_UPDATE}) {
      ASSERT_EQ(root_step, planner.get_step());
      ASSERT_EQ(0, planner.get_depth());
      for (int64_t t = 0; t < plan->get_task_count(); ++t) {
        ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
      }
      const ObIVFTaskPlan *next = nullptr;
      ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
      ASSERT_NE(nullptr, next);
      plan = next;
    }
    ASSERT_EQ(1, planner.get_depth());
    EXPECT_EQ(0, store.reads_);
    if (prepared_samples) {
      ASSERT_EQ(sample_step, planner.get_step());
      ASSERT_EQ(2, plan->get_task_count());
      const int64_t sample_plan_id = plan->get_plan_id();
      for (int phase = 0; phase < 2; ++phase) {
        for (int64_t t = 0; t < plan->get_task_count(); ++t) {
          ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, t));
          if (t + 1 < plan->get_task_count()) {
            EXPECT_EQ(OB_STATE_NOT_MATCH, planner.check_task_plan_finished(*plan));
          }
        }
        const ObIVFTaskPlan *next = nullptr;
        ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
        ASSERT_NE(nullptr, next);
        plan = next;
        EXPECT_EQ(phase == 0 ? ObIVFMergeStep::HIERARCHY_CLUSTER : ObIVFMergeStep::HIERARCHY_ASSIGN,
            planner.get_step());
        EXPECT_EQ(1, planner.get_depth());
      }
      std::vector<ObIVFMergeStepPerf> finished;
      for (const auto &perf : observer.snapshots_) {
        if (perf.depth_ == 1 && perf.end_us_ > 0) { finished.push_back(perf); }
      }
      ASSERT_EQ(2, finished.size());
      EXPECT_EQ(sample_step, finished[0].step_);
      EXPECT_EQ(ObIVFMergeStep::HIERARCHY_CLUSTER, finished[1].step_);
      EXPECT_EQ(sample_plan_id, finished[0].plan_id_);
      EXPECT_EQ(sample_plan_id + 1, finished[1].plan_id_);
      for (const auto &perf : finished) {
        EXPECT_EQ(2, perf.task_count_);
        EXPECT_EQ(OB_SUCCESS, perf.ret_code_);
        EXPECT_GE(perf.end_us_, perf.finish_begin_us_);
        EXPECT_GE(perf.finish_begin_us_, perf.prepared_us_);
        EXPECT_GE(perf.prepared_us_, perf.start_us_);
      }
      EXPECT_GE(finished[1].start_us_, finished[0].end_us_);
      ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), final_fds));
      EXPECT_EQ(initial_fds.count(), final_fds.count());
    } else {
      EXPECT_EQ(ObIVFMergeStep::HIERARCHY_CLUSTER, planner.get_step());
      for (const auto &perf : observer.snapshots_) { EXPECT_NE(sample_step, perf.step_); }
    }
    planner.cancel(OB_CANCELED);
  }
}

TEST_F(TestIVFSSTableWriter, parallel_cluster_samples_retry_restore_order_and_reuse_buffers)
{
  ObArray<int64_t> initial_fds, final_fds;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), initial_fds));
  ParallelSampleStore store;
  ParallelSampleAlgorithm algorithm;
  ObIVFHcaAlgorithmParam param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, 256, false, true, false, VIDA_L2));
  ObIVFDDLSSTableWriter unused_output(allocator_);
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator_, input, algorithm, unused_output, ObIVFProcessType::INITIAL_BUILD));
  ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 2));
  const auto stage = ObIVFMergeStage::HIERARCHY;
  const auto step = ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE;
  const auto train_step = ObIVFMergeStep::HIERARCHY_CLUSTER;
  int64_t tasks = 0;
  // A non-root plan over one synthetic node isolates shard gathering from HCA.
  ASSERT_EQ(OB_SUCCESS, processor.prepare_step(stage, step, 1, 0, tasks));
  ASSERT_EQ(2, tasks);
  EXPECT_EQ(0, store.reads_); // prepare performs no payload I/O
  store.fail_read_ = true;
  EXPECT_EQ(OB_IO_ERROR, processor.execute_hierarchy_task(stage, step, 1, 0, 0));
  store.fail_read_ = false;
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  int results[2] = {OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED};
  std::vector<std::thread> workers;
  for (int64_t t = 0; t < tasks; ++t) {
    workers.emplace_back([&, t]() {
      ObTenantEnv::set_tenant(tenant);
      THIS_WORKER.set_timeout_ts(INT64_MAX);
      results[t] = processor.execute_hierarchy_task(stage, step, 1, 0, t);
      ObTenantEnv::set_tenant(nullptr);
    });
  }
  for (auto &worker : workers) { worker.join(); }
  for (int result : results) { ASSERT_EQ(OB_SUCCESS, result); }
  EXPECT_GT(store.reads_, 8);
  EXPECT_EQ(0, store.buffer_changes_);
  ObIVFMergeStepResult result;
  ASSERT_EQ(OB_SUCCESS, processor.finish_step(stage, step, 1, 0, result));
  EXPECT_EQ(step, result.step_);
  ASSERT_TRUE(result.is_valid());
  ASSERT_EQ(OB_SUCCESS, processor.prepare_step(stage, train_step, 1, 1, tasks));
  ASSERT_EQ(1, tasks);
  const int64_t reads = store.reads_;
  ASSERT_EQ(OB_SUCCESS, processor.execute_hierarchy_task(stage, train_step, 1, 1, 0));
  EXPECT_EQ(reads, store.reads_); // training consumes compact shards only
  result = ObIVFMergeStepResult();
  ASSERT_EQ(OB_SUCCESS, processor.finish_step(stage, train_step, 1, 1, result));
  EXPECT_EQ(train_step, result.step_);
  ASSERT_TRUE(result.is_valid());
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), final_fds));
  EXPECT_EQ(initial_fds.count(), final_fds.count());
}

TEST_F(TestIVFSSTableWriter, parallel_cluster_sample_gather_observes_cancel_between_batches)
{
  ObArray<int64_t> initial_fds, final_fds;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), initial_fds));
  ParallelSampleStore store;
  ParallelSampleAlgorithm algorithm;
  ObIVFHcaAlgorithmParam param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, 256, false, true, false, VIDA_L2));
  ObIVFDDLSSTableWriter unused_output(allocator_);
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator_, input, algorithm, unused_output, ObIVFProcessType::INITIAL_BUILD));
  ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 2));
  int64_t tasks = 0;
  ASSERT_EQ(OB_SUCCESS, processor.prepare_step(ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE, 1, 0, tasks));
  store.cancel_on_read_ = &processor;
  store.cancel_after_reads_ = 3; // spill an external-sort run before cancellation
  EXPECT_EQ(OB_CANCELED, processor.execute_hierarchy_task(ObIVFMergeStage::HIERARCHY,
      ObIVFMergeStep::HIERARCHY_SAMPLE_PREPARE, 1, 0, 0));
  EXPECT_EQ(3, store.reads_);
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), final_fds));
  EXPECT_EQ(initial_fds.count(), final_fds.count());
}

TEST_F(TestIVFSSTableWriter, sample_sort_merges_bounded_spill_runs)
{
  ObIVFClusterSampleCompare compare;
  ObExternalSort<ObIVFSortItem, ObIVFClusterSampleCompare> sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(8L * 1024 * 1024,
      OB_STORAGE_OBJECT_MGR.get_macro_block_size(), 0, MTL_ID(), &compare));
  static const int64_t COUNT = 8193;
  for (int64_t i = COUNT - 1; i >= 0; --i) {
    float payload[1024];
    for (int64_t d = 0; d < 1024; ++d) { payload[d] = float(i + d); }
    ObIVFSortItem row;
    row.row_type_ = 1; row.cid_ = i % 7; row.vid_ = i;
    row.payload_.assign_ptr(reinterpret_cast<char *>(payload), sizeof(payload));
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(row));
  }
  ASSERT_EQ(OB_SUCCESS, sorter.do_sort(true));
  int64_t seen = 0;
  for (int64_t cid = 0; cid < 7; ++cid) {
    for (int64_t i = cid; i < COUNT; i += 7) {
      const ObIVFSortItem *item = nullptr;
      ASSERT_EQ(OB_SUCCESS, sorter.get_next_item(item));
      ASSERT_NE(nullptr, item); EXPECT_EQ(cid, item->cid_); EXPECT_EQ(i, item->vid_);
      float payload[1024]; ASSERT_EQ(sizeof(payload), item->payload_.length());
      MEMCPY(payload, item->payload_.ptr(), sizeof(payload));
      EXPECT_FLOAT_EQ(float(i), payload[0]); EXPECT_FLOAT_EQ(float(i + 1023), payload[1023]);
      ++seen;
    }
  }
  EXPECT_EQ(COUNT, seen);
  const ObIVFSortItem *item = nullptr;
  EXPECT_EQ(OB_ITER_END, sorter.get_next_item(item));
  sorter.clean_up();
}

TEST_F(TestIVFSSTableWriter, storage_sorter_rejects_duplicate_physical_key)
{
  ObIVFSorter sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(MTL_ID()));
  ObIVFBuildRow row;
  const char first_payload = 'a';
  const char second_payload = 'b';
  row.row_type_ = blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
  row.cid_ = 7;
  row.vid_ = 7001;
  row.payload_.assign_ptr(&first_payload, sizeof(first_payload));
  ASSERT_EQ(OB_SUCCESS, sorter.append_row(row));
  row.payload_.assign_ptr(&second_payload, sizeof(second_payload));
  ASSERT_EQ(OB_SUCCESS, sorter.append_row(row));
  ASSERT_EQ(OB_SUCCESS, sorter.finish());

  const ObIVFSortItem *item = nullptr;
  ASSERT_EQ(OB_SUCCESS, sorter.get_next_item(item));
  ASSERT_NE(nullptr, item);
  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, sorter.get_next_item(item));
}

TEST_F(TestIVFSSTableWriter, empty_storage_sorter_returns_iter_end)
{
  ObIVFSorter sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(MTL_ID()));
  ASSERT_EQ(OB_SUCCESS, sorter.finish());
  ASSERT_EQ(0, sorter.get_row_count());

  const ObIVFSortItem *item = nullptr;
  EXPECT_EQ(OB_ITER_END, sorter.get_next_item(item));
  EXPECT_EQ(nullptr, item);
}

TEST_F(TestIVFSSTableWriter, async_index_deletion_preparation_follows_merge_type)
{
  for (const bool is_ivf : {false, true}) {
    for (const ObMergeType type : {MAJOR_MERGE, MEDIUM_MERGE, INC_MAJOR_MERGE}) {
      SCOPED_TRACE(is_ivf);
      SCOPED_TRACE(type);
      ObTabletMergeDagParam param(type, ObLSID(IVF_LS_ID),
          ObTabletID(IVF_TABLET_ID), -1);
      param.merge_version_ = IVF_SNAPSHOT_VERSION;
      ObAsyncIndexMergeCtx ctx(param, allocator_);
      ctx.static_param_.is_ivf_index_ = is_ivf;
      ctx.static_param_.is_inv_index_ = !is_ivf;
      const int expected_ret = type == INC_MAJOR_MERGE ? OB_SUCCESS : OB_INVALID_ARGUMENT;
      EXPECT_EQ(expected_ret, ctx.prepare_compaction_filter());
      EXPECT_FALSE(ctx.deletion_bitmap_handle_.is_valid());
    }
  }
}

// Source reads use the production StoreCtx, which resolves the selected LS and its
// transaction table. The plain SSTable writer fixture has neither.
class TestIVFMergeInput : public TestIVFSSTableWriter
{
protected:
  static void SetUpTestCase()
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
    int ret = MTL(ObLSService *)->get_ls(ObLSID(IVF_LS_ID), ls_handle_, ObLSGetMod::STORAGE_MOD);
    if (OB_LS_NOT_EXIST == ret) {
      ret = TestDmlCommon::create_ls(OB_SERVER_TENANT_ID, ObLSID(IVF_LS_ID), ls_handle_);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  static void TearDownTestCase()
  {
    ls_handle_.reset();
    // The base and parameterized suites share this LS. Keep the service-owned
    // instance until tenant teardown instead of racing asynchronous LS removal.
  }
  static ObLSHandle ls_handle_;
};
ObLSHandle TestIVFMergeInput::ls_handle_;

TEST_F(TestIVFMergeInput, merge_source_applies_major_bitmap_and_allows_inc_without_bitmap)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(writer, 0));
  float vectors[2][2] = {{1.0F, 2.0F}, {3.0F, 4.0F}};
  for (int64_t i = 0; i < 2; ++i) {
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_RAW_VECTOR,
        IVF_RAW_VECTOR_CID, (i + 1) * 11,
        ObString(sizeof(vectors[i]), reinterpret_cast<char *>(vectors[i]))));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(2));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ObITable::TableKey &source_key = const_cast<ObITable::TableKey &>(sstable_.get_key());
  source_key.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
  source_key.version_range_.base_version_ = IVF_SNAPSHOT_VERSION / 2;

  for (const ObMergeType merge_type : {MAJOR_MERGE, INC_MAJOR_MERGE}) {
    SCOPED_TRACE(merge_type);
    ObTabletMergeDagParam param(merge_type, ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID), -1);
    param.merge_version_ = IVF_SNAPSHOT_VERSION;
    ObAsyncIndexMergeCtx ctx(param, allocator_);
    ctx.static_param_.is_ivf_index_ = true;
    ctx.static_param_.version_range_.base_version_ =
        merge_type == INC_MAJOR_MERGE ? source_key.version_range_.base_version_ : 0;
    ctx.static_param_.version_range_.snapshot_version_ = source_key.version_range_.snapshot_version_;
    ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator_, table_schema_.get_column_count(),
        table_schema_.get_rowkey_column_num(), false /* oracle */,
        whole_desc_.get_desc().get_full_stored_col_descs()));
    ObTableHandleV2 source_handle;
    // The fixture owns this SSTable until all source readers and ctx are destroyed.
    ASSERT_EQ(OB_SUCCESS, source_handle.set_sstable_with_tablet(&sstable_));
    ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(source_handle));

    const int64_t max_deleted_count = merge_type == INC_MAJOR_MERGE ? -1 : 2;
    for (int64_t deleted_count = -1; deleted_count <= max_deleted_count; ++deleted_count) {
      SCOPED_TRACE(deleted_count);
      ctx.deletion_bitmap_handle_.reset();
      if (deleted_count >= 0) {
        ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
        ASSERT_NE(nullptr, bitmap);
        ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));
        for (int64_t i = 0; i < deleted_count; ++i) {
          ASSERT_EQ(OB_SUCCESS, bitmap->value_add((i + 1) * 11));
        }
      }
      ObIVFMergeInputAdapter input(allocator_);
      input.index_table_id_ = IVF_TABLE_ID;
      input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
      input.vector_dim_ = 2;
      input.max_sample_count_ = 2;
      input.dist_type_ = VIDA_L2;
      ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
      if (deleted_count < 0 && merge_type == MAJOR_MERGE) {
        EXPECT_EQ(OB_STATE_NOT_MATCH, input.init_source_(ctx));
      } else {
        const int64_t filtered_count = deleted_count < 0 ? 0 : deleted_count;
        ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
        EXPECT_EQ(2 - filtered_count, input.get_vector_count());
        EXPECT_EQ(2 - filtered_count, input.get_sample_count());
        EXPECT_EQ(2 == filtered_count, input.is_empty());
        if (!input.is_empty()) {
          ObSEArray<ObIVFVectorVID, 2> vids;
          ObSEArray<float, 4> samples;
          ASSERT_EQ(OB_SUCCESS, input.get_all_vids(vids));
          EXPECT_EQ((filtered_count + 1) * 11, vids.at(0));
          ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(
              0, input.get_sample_count(), vids, samples));
          EXPECT_EQ(vectors[filtered_count][0], samples.at(0));
          EXPECT_EQ(vectors[filtered_count][1], samples.at(1));
          if (filtered_count == 0) {
            // RAW multi-get preserves caller order and repeated VIDs while
            // sharing cached blocks across ordinary and sample reads.
            vids.reuse();
            ASSERT_EQ(OB_SUCCESS, vids.push_back(22));
            ASSERT_EQ(OB_SUCCESS, vids.push_back(11));
            ASSERT_EQ(OB_SUCCESS, vids.push_back(22));
            for (int round = 0; round < 2; ++round) {
              ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 0, vids.count(), samples));
              ASSERT_EQ(6, samples.count());
              EXPECT_EQ(3.0F, samples.at(0));
              EXPECT_EQ(4.0F, samples.at(1));
              EXPECT_EQ(1.0F, samples.at(2));
              EXPECT_EQ(2.0F, samples.at(3));
              EXPECT_EQ(3.0F, samples.at(4));
              EXPECT_EQ(4.0F, samples.at(5));
            }
            vids.reuse();
            ASSERT_EQ(OB_SUCCESS, vids.push_back(33));
            EXPECT_EQ(OB_ENTRY_NOT_EXIST, input.batch_get_by_vid(vids, 0, vids.count(), samples));
            EXPECT_TRUE(samples.empty());
          }
        }
      }
    }
  }
}

TEST_F(TestIVFMergeInput, raw_capture_matches_pipeline_bottom_k_samples)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(writer, 0));
  static const int64_t VECTOR_COUNT = 4;
  static const int64_t VECTOR_DIM = 2;
  static const ObIVFVectorVID VIDS[VECTOR_COUNT] = {11, 22, 33, 44};
  float vectors[VECTOR_COUNT][VECTOR_DIM] = {
      {1.0F, 2.0F}, {3.0F, 4.0F}, {5.0F, 6.0F}, {7.0F, 8.0F}};
  for (int64_t i = 0; i < VECTOR_COUNT; ++i) {
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_RAW_VECTOR,
        IVF_RAW_VECTOR_CID, VIDS[i],
        ObString(sizeof(vectors[i]), reinterpret_cast<char *>(vectors[i]))));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(VECTOR_DIM));
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ObTabletMergeDagParam param(MAJOR_MERGE, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  param.merge_version_ = IVF_SNAPSHOT_VERSION;
  ObAsyncIndexMergeCtx ctx(param, allocator_);
  ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator_, table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(), false, whole_desc_.get_desc().get_full_stored_col_descs()));
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
  ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
  ASSERT_NE(nullptr, bitmap);
  ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));

  ObIVFMergeInputAdapter input(allocator_);
  input.index_table_id_ = IVF_TABLE_ID;
  input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
  input.vector_dim_ = VECTOR_DIM;
  input.max_sample_count_ = 2;
  input.dist_type_ = VIDA_L2;
  ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
  ASSERT_TRUE(input.is_valid());

  // The refresh pipeline still has its own capture input. Verify merge's
  // locator-only sampling remains identical to that independent pipeline.
  ObIVFReplayInput captured(allocator_);
  ASSERT_EQ(OB_SUCCESS, captured.init(OB_SYS_TENANT_ID, VECTOR_DIM, 1L << 20, 2,
      ObIVFReplayInput::build_sample_seed(input.index_table_id_, input.index_tablet_id_.id()), 0));
  for (int64_t i = 0; i < VECTOR_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, captured.add_row(VIDS[i], ObString(sizeof(vectors[i]),
        reinterpret_cast<char *>(vectors[i]))));
  }
  ASSERT_EQ(OB_SUCCESS, captured.seal());
  // REFRESH samples vectors without retaining the INITIAL_BUILD VID array.
  ASSERT_TRUE(captured.is_sampling_enabled());
  EXPECT_EQ(0, captured.captured_vids_.count());
  EXPECT_EQ(0, captured.captured_vids_.get_capacity());
  uint64_t captured_vid = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, captured.get_captured_vid(0, captured_vid));
  ASSERT_EQ(captured.get_sample_count(), input.get_sample_count());
  for (int64_t i = 0; i < captured.get_sample_count(); ++i) {
    const ObIVFSampleCandidate &candidate = captured.get_sample_candidates().at(i);
    const float *sample_vector = nullptr;
    ASSERT_EQ(OB_SUCCESS, captured.get_row_at(candidate.row_id_, captured_vid, sample_vector));
    ASSERT_NE(nullptr, sample_vector);
    EXPECT_EQ(candidate.vid_, captured_vid);
    EXPECT_EQ(0, MEMCMP(vectors[candidate.row_id_], sample_vector, sizeof(vectors[0])));
  }
  struct Candidate
  {
    uint64_t priority_;
    ObIVFVectorVID vid_;
  };
  Candidate candidates[VECTOR_COUNT];
  const uint64_t seed0 = common::splitmix64(
      0x6a09e667f3bcc909ULL ^ input.index_table_id_);
  const uint64_t sample_seed = common::splitmix64(
      seed0 ^ input.index_tablet_id_.id());
  for (int64_t i = 0; i < VECTOR_COUNT; ++i) {
    candidates[i].priority_ = common::splitmix64(sample_seed ^ VIDS[i]);
    candidates[i].vid_ = VIDS[i];
  }
  lib::ob_sort(candidates, candidates + VECTOR_COUNT,
      [](const Candidate &left, const Candidate &right) {
        return left.priority_ != right.priority_
            ? left.priority_ < right.priority_ : left.vid_ < right.vid_;
      });

  ObSEArray<ObIVFVectorVID, 2> sample_vids;
  ObSEArray<float, 4> sample_vectors;
  ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(
      0, 2, sample_vids, sample_vectors));
  ASSERT_EQ(2, sample_vids.count());
  ASSERT_EQ(4, sample_vectors.count());
  ObIVFVectorVID actual_vids[2] = {sample_vids.at(0), sample_vids.at(1)};
  ObIVFVectorVID expected_vids[2] = {
      candidates[0].vid_, candidates[1].vid_};
  lib::ob_sort(actual_vids, actual_vids + 2);
  lib::ob_sort(expected_vids, expected_vids + 2);
  EXPECT_EQ(expected_vids[0], actual_vids[0]);
  EXPECT_EQ(expected_vids[1], actual_vids[1]);
  ObIVFVectorVID pipeline_vids[2] = {
      captured.get_sample_candidates().at(0).vid_, captured.get_sample_candidates().at(1).vid_};
  lib::ob_sort(pipeline_vids, pipeline_vids + 2);
  EXPECT_EQ(pipeline_vids[0], actual_vids[0]);
  EXPECT_EQ(pipeline_vids[1], actual_vids[1]);
  for (int64_t i = 0; i < sample_vids.count(); ++i) {
    const int64_t source_idx = sample_vids.at(i) / 11 - 1;
    ASSERT_GE(source_idx, 0);
    ASSERT_LT(source_idx, VECTOR_COUNT);
    EXPECT_EQ(vectors[source_idx][0], sample_vectors.at(i * VECTOR_DIM));
    EXPECT_EQ(vectors[source_idx][1],
        sample_vectors.at(i * VECTOR_DIM + 1));
  }
}

static void clear_hierarchical_test_cache(const ObLSID &ls_id,
    const ObITable::TableKey &table_key, const int64_t parent_count)
{
  auto *service = MTL(ObPluginVectorIndexService *);
  if (service == nullptr) { return; }
  for (int64_t group = 0; group <= parent_count; ++group) {
    ObIvfCacheMgrGuard guard;
    const ObIvfCacheMgrKey key(table_key, group);
    const int ret = service->acquire_ivf_cache_mgr_guard(ls_id, key, guard);
    if (ret == OB_HASH_NOT_EXIST || ret == OB_ENTRY_NOT_EXIST) { continue; }
    ASSERT_EQ(OB_SUCCESS, ret);
    auto *manager = guard.get_ivf_cache_mgr();
    ObIvfSSTableCentCache *cache = nullptr;
    ASSERT_EQ(OB_SUCCESS, manager->get_cache_node<ObIvfSSTableCentCache>(IVF_SSTABLE_CENTROID_CACHE, cache));
    ASSERT_NE(nullptr, cache);
    bool erased = false;
    uint64_t hold = 0;
    ASSERT_EQ(OB_SUCCESS, service->try_erase_ivf_sstable_cache(ls_id, key,
        manager, cache, cache->get_last_access_ts(), false, erased, hold));
    EXPECT_TRUE(erased);
  }
}

class TestIVFMergeInputLayout : public TestIVFMergeInput,
    public ::testing::WithParamInterface<bool>
{
public:
  using TestIVFMergeInput::SetUpTestCase;
  using TestIVFMergeInput::TearDownTestCase;
};
INSTANTIATE_TEST_CASE_P(FlatAndGrouped, TestIVFMergeInputLayout, ::testing::Bool());

TEST_P(TestIVFMergeInputLayout, major_and_inc_prior_match_source_visibility)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(writer, 0));
  const int64_t n = 2048;
  const int64_t k = 32;
  const bool grouped = GetParam();
  const int64_t base = grouped ? 3 : 1;
  float vector[2] = {1.0F, 2.0F};
  const ObString payload(sizeof(vector), reinterpret_cast<char *>(vector));
  if (grouped) {
    for (int64_t p = 1; p <= 2; ++p) {
      ObDatumRow row;
      char bytes[sizeof(vector) + sizeof(ObIvfCentroidPayloadHeader)];
      ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::encode(bytes, sizeof(bytes), vector, 2, k));
      ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_CENTROID, 0, p, ObString(sizeof(bytes), bytes)));
      ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    }
  }
  for (int64_t group = grouped ? 1 : 0; group <= (grouped ? 2 : 0); ++group) {
    for (int64_t cid = 0; cid < k; ++cid) {
      ObDatumRow row;
      char bytes[sizeof(vector) + sizeof(ObIvfCentroidPayloadHeader)];
      ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::encode(bytes, sizeof(bytes), vector, 2, cid == 1 ? 80 : 64));
      ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_CENTROID,
          group, cid + base, ObString(sizeof(bytes), bytes)));
      ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    }
  }
  for (int64_t cid = 0; cid < k; ++cid) {
    for (int64_t vid = 1; vid <= n; ++vid) {
      // Each vector has a primary posting and the first 16 also have SOAR postings.
      if ((vid - 1) / 64 == cid || (vid <= 16 && cid == 1)) {
        ObDatumRow row;
        ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_LOW_PRECISION_VECTOR, cid + base, vid, payload));
        ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
      }
    }
  }
  for (int64_t vid = 1; vid <= n; ++vid) {
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_RAW_VECTOR, IVF_RAW_VECTOR_CID, vid, payload));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(2));
  sstable_.ivf_meta_.centroid_count_ = k;
  sstable_.ivf_meta_.centroid_layout_ = grouped ? IVF_CENTROID_HIERARCHICAL : IVF_CENTROID_FLAT;
  sstable_.ivf_meta_.parent_centroid_count_ = grouped ? 2 : 0;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  auto &key = const_cast<ObITable::TableKey &>(sstable_.get_key());
  key.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
  key.version_range_.base_version_ = IVF_SNAPSHOT_VERSION / 2;
  for (const ObMergeType merge_type : {MAJOR_MERGE, INC_MAJOR_MERGE}) {
    SCOPED_TRACE(merge_type);
    ObTabletMergeDagParam param(merge_type, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
    param.merge_version_ = IVF_SNAPSHOT_VERSION;
    ObAsyncIndexMergeCtx ctx(param, allocator_);
    ctx.static_param_.is_ivf_index_ = true;
    ctx.static_param_.version_range_.base_version_ =
        merge_type == INC_MAJOR_MERGE ? key.version_range_.base_version_ : 0;
    ctx.static_param_.version_range_.snapshot_version_ = key.version_range_.snapshot_version_;
    ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator_, table_schema_.get_column_count(),
        table_schema_.get_rowkey_column_num(), false, whole_desc_.get_desc().get_full_stored_col_descs()));
    ObTableHandleV2 handle;
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
    ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
    const int64_t deleted_count = merge_type == MAJOR_MERGE ? 8 : 0;
    if (merge_type == MAJOR_MERGE) {
      ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &allocator_, &allocator_);
      ASSERT_NE(nullptr, bitmap);
      ASSERT_EQ(OB_SUCCESS, ctx.deletion_bitmap_handle_.init_from_bitmap(allocator_, bitmap));
      for (uint64_t vid = 1; vid <= 8; ++vid) { ASSERT_EQ(OB_SUCCESS, bitmap->value_add(vid)); }
    } else {
      ASSERT_EQ(OB_SUCCESS, ctx.prepare_compaction_filter());
      ASSERT_FALSE(ctx.deletion_bitmap_handle_.is_valid());
    }
    ObIVFMergeInputAdapter input(allocator_);
    input.index_table_id_ = IVF_TABLE_ID;
    input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
    input.vector_dim_ = 2;
    input.max_sample_count_ = 128;
    input.dist_type_ = VIDA_L2;
    ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
    ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
    ASSERT_EQ(OB_SUCCESS, input.capture_prior_(ctx));
    const ObIVFMergePrior &prior = input.get_prior();
    EXPECT_EQ(n - deleted_count, prior.vector_count_);
    EXPECT_EQ(n - deleted_count, prior.covered_vector_count_);
    ASSERT_EQ(k, prior.posting_mass_.count());
    ASSERT_EQ(k, prior.old_cids_.count());
    for (int64_t j = 0; j < k; ++j) { EXPECT_EQ(base + j, prior.old_cids_.at(j)); }
    int32_t candidates[16];
    int64_t candidate_count = 0;
    ASSERT_EQ(OB_SUCCESS, input.search_centroids(0, vector, 2, candidates, 16, candidate_count));
    ASSERT_EQ(16, candidate_count);
    for (int64_t j = 0; j < candidate_count; ++j) { EXPECT_EQ(j, candidates[j]); }
    EXPECT_EQ(64 - deleted_count, prior.posting_mass_.at(0));
    EXPECT_EQ(80 - deleted_count, prior.posting_mass_.at(1));
    int64_t posting_count = 0;
    for (int64_t cid = 0; cid < k; ++cid) { posting_count += prior.posting_mass_.at(cid); }
    EXPECT_EQ(n + 16 - 2 * deleted_count, posting_count);
    EXPECT_TRUE(prior.postings_complete_);
    ASSERT_EQ(posting_count, prior.postings_.count());
    for (int64_t i = 0; i < prior.postings_.count(); ++i) {
      const ObIVFPriorPosting &posting = prior.postings_.at(i);
      EXPECT_GT(posting.vid_, static_cast<uint64_t>(deleted_count));
      EXPECT_LE(posting.vid_, static_cast<uint64_t>(n));
      EXPECT_GE(posting.centroid_idx_, 0);
      EXPECT_LT(posting.centroid_idx_, k);
    }
    EXPECT_EQ(n - deleted_count, prior.segments_.at(0).live_vector_count_);
    ObIVFProcessType selected;
    ASSERT_EQ(OB_SUCCESS, ObIVFMergeStrategy::select(prior, 384, selected));
    EXPECT_EQ(ObIVFProcessType::INSERTION, selected);
    EXPECT_EQ(IVF_SNAPSHOT_VERSION / 2, key.version_range_.base_version_);
  }
  clear_hierarchical_test_cache(ObLSID(IVF_LS_ID), sstable_.get_key(),
      sstable_.get_ivf_meta().parent_centroid_count_);
}

TEST_F(TestIVFMergeInput, prior_capture_rejects_postings_above_reserved_bound)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(writer, 0));
  const int64_t n = 2048, k = 3;
  float vector[2] = {1.0F, 2.0F};
  const ObString payload(sizeof(vector), reinterpret_cast<char *>(vector));
  for (int64_t cid = 1; cid <= k; ++cid) {
    ObDatumRow row;
    char bytes[sizeof(vector) + sizeof(ObIvfCentroidPayloadHeader)];
    ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::encode(bytes, sizeof(bytes), vector, 2, n));
    ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_CENTROID, 0, cid, ObString(sizeof(bytes), bytes)));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  // Malformed input: each live vector belongs to three centers. The capture
  // must reject it before push_back can allocate another Arena buffer.
  for (int64_t cid = 1; cid <= k; ++cid) {
    for (int64_t vid = 1; vid <= n; ++vid) {
      ObDatumRow row;
      ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_LOW_PRECISION_VECTOR, cid, vid, payload));
      ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
    }
  }
  for (int64_t vid = 1; vid <= n; ++vid) {
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, make_ivf_row(row, IVF_ROW_TYPE_RAW_VECTOR, IVF_RAW_VECTOR_CID, vid, payload));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable(2));
  sstable_.ivf_meta_.centroid_count_ = k;
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  auto &key = const_cast<ObITable::TableKey &>(sstable_.get_key());
  key.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
  key.version_range_.base_version_ = IVF_SNAPSHOT_VERSION / 2;
  ObTabletMergeDagParam param(INC_MAJOR_MERGE, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
  param.merge_version_ = IVF_SNAPSHOT_VERSION;
  ObAsyncIndexMergeCtx ctx(param, allocator_);
  ctx.static_param_.is_ivf_index_ = true;
  ctx.static_param_.version_range_.base_version_ = key.version_range_.base_version_;
  ctx.static_param_.version_range_.snapshot_version_ = key.version_range_.snapshot_version_;
  ASSERT_EQ(OB_SUCCESS, ctx.read_info_.init(allocator_, table_schema_.get_column_count(),
      table_schema_.get_rowkey_column_num(), false, whole_desc_.get_desc().get_full_stored_col_descs()));
  ASSERT_EQ(OB_SUCCESS, ctx.prepare_compaction_filter());
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, handle.set_sstable_with_tablet(&sstable_));
  ASSERT_EQ(OB_SUCCESS, ctx.static_param_.tables_handle_.add_table(handle));
  ObIVFMergeInputAdapter input(allocator_);
  input.index_table_id_ = IVF_TABLE_ID;
  input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
  input.vector_dim_ = 2;
  input.max_sample_count_ = 128;
  input.dist_type_ = VIDA_L2;
  ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
  ASSERT_EQ(OB_SUCCESS, input.init_source_(ctx));
  EXPECT_EQ(OB_INVALID_DATA, input.capture_prior_(ctx));
  EXPECT_EQ(2 * n, input.prior_.postings_.count());
  EXPECT_FALSE(input.prior_.postings_complete_);
}

TEST_F(TestIVFSSTableWriter, merge_writer_windows_stay_below_final_index)
{
  // RAW plans may have 64 writers even when compaction has only one data slot.
  // All producers must remain inside the range registered with the SS protocol.
  const int64_t data_starts[] = {0, 123456, INT64_MAX - 1000};
  for (const int64_t data_start : data_starts) {
    ObMacroSeqWindow data_window;
    data_window.start_ = data_start;
    data_window.end_ = data_start + 1000;
    int64_t previous_end = data_start;
    for (int64_t writer_idx = 0;
         writer_idx < ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT; ++writer_idx) {
      ObMacroSeqWindow window;
      ASSERT_EQ(OB_SUCCESS, ObIVFMergeSSTableWriter::build_macro_seq_window_(
          data_window, writer_idx, window));
      EXPECT_EQ(previous_end, window.start_);
      ASSERT_LT(window.start_, window.end_);
      EXPECT_LE(window.end_, data_window.end_);
      // Exhaustion must fail before entering the next producer's window.
      ObMacroSeqParam param;
      param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      param.start_ = window.end_ - 1;
      param.end_ = window.end_;
      ObMacroIncSeqGenerator generator;
      ASSERT_EQ(OB_SUCCESS, generator.init(param));
      int64_t seq = -1;
      ASSERT_EQ(OB_SUCCESS, generator.get_next(seq));
      EXPECT_EQ(window.end_ - 1, seq);
      EXPECT_EQ(OB_SIZE_OVERFLOW, generator.get_next(seq));
      previous_end = window.end_;
    }
    EXPECT_EQ(data_window.end_, previous_end);
  }
  ObMacroSeqWindow data_window;
  ObMacroSeqWindow window;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObIVFMergeSSTableWriter::build_macro_seq_window_(data_window, 0, window));
  data_window.start_ = 0;
  data_window.end_ = 1000;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObIVFMergeSSTableWriter::build_macro_seq_window_(data_window, -1, window));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObIVFMergeSSTableWriter::build_macro_seq_window_(
          data_window, ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT, window));
  data_window.end_ = ObIVFMergeSSTableWriter::OUTPUT_WRITER_COUNT - 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObIVFMergeSSTableWriter::build_macro_seq_window_(data_window, 0, window));
  EXPECT_EQ(-1, window.start_);
  EXPECT_EQ(-1, window.end_);
}

TEST_F(TestIVFSSTableWriter, merge_global_sorter_initializes_only_for_global_rows)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE));
  ObMacroSeqWindow data_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ObArray<float> centers;
  ASSERT_EQ(OB_SUCCESS, centers.push_back(1.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    ObIVFMergeSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, writer.init(whole_desc_.get_desc(), sstable_builder_, data_window,
        IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_sort_row_(
        IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0, 1, ObString()));
    EXPECT_EQ(nullptr, writer.sorter_.impl_);
    if (mode == 0) {
      ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(1, 1));
    } else if (mode == 1) {
      int64_t task_count = 0;
      ASSERT_EQ(OB_SUCCESS, writer.prepare_quantization(centers, 1, task_count));
      EXPECT_EQ(1, task_count);
    } else {
      ObTenantBase *tenant = ObTenantEnv::get_tenant();
      std::atomic<bool> start(false);
      int results[2] = {OB_ERR_UNEXPECTED, OB_ERR_UNEXPECTED};
      std::thread workers[2];
      for (int i = 0; i < 2; ++i) {
        workers[i] = std::thread([&, i]() {
          ObTenantEnv::set_tenant(tenant);
          THIS_WORKER.set_timeout_ts(INT64_MAX);
          while (!start.load()) { std::this_thread::yield(); }
          results[i] = writer.append_sort_row_(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0, i + 1,
              ObString::make_string(i == 0 ? "first" : "second"));
          ObTenantEnv::set_tenant(nullptr);
        });
      }
      start.store(true);
      for (auto &worker : workers) { worker.join(); }
      ASSERT_EQ(OB_SUCCESS, results[0]);
      ASSERT_EQ(OB_SUCCESS, results[1]);
      EXPECT_EQ(2, writer.get_row_count());
      ASSERT_NE(nullptr, writer.sorter_.impl_);
      ASSERT_EQ(OB_SUCCESS, writer.sorter_.finish());
      const ObIVFSortItem *item = nullptr;
      for (int i = 0; i < 2; ++i) {
        ASSERT_EQ(OB_SUCCESS, writer.sorter_.get_next_item(item));
        ASSERT_NE(nullptr, item);
        EXPECT_EQ(i + 1, item->vid_);
        EXPECT_EQ(ObString::make_string(i == 0 ? "first" : "second"), item->payload_);
      }
      EXPECT_EQ(OB_ITER_END, writer.sorter_.get_next_item(item));
    }
    if (mode < 2) {
      EXPECT_EQ(nullptr, writer.sorter_.impl_);
      EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_sort_row_(
          IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0, 1, ObString::make_string("unused")));
      EXPECT_EQ(nullptr, writer.sorter_.impl_);
    }
  }
}

#ifndef OB_USE_ASAN
TEST_F(TestIVFSSTableWriter, merge_loaded_writers_release_buffers_before_quant_barrier)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE));
  FullRebuildWriterVectorStore store;
  ObIVFVectorReader reader;
  ObArray<ObIVFVectorVID> vids;
  ObArray<float> centers, vectors;
  ObArray<int64_t> primary;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  ASSERT_EQ(OB_SUCCESS, reader.batch_get_by_vid(vids, 0, 2, vectors));
  for (int64_t i = 0; i < 6; ++i) { ASSERT_EQ(OB_SUCCESS, centers.push_back(float(i))); }
  ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
  ASSERT_EQ(OB_SUCCESS, primary.push_back(2));
  ObMacroSeqWindow data_window, index_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(1, index_window));
  ObIVFMergeSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, writer.init(whole_desc_.get_desc(), sstable_builder_, data_window,
      IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(3, 3));
  ASSERT_EQ(OB_SUCCESS, writer.quantize_loaded_vectors(centers, 2, vids, vectors, primary, nullptr));
  const int64_t macro_before = get_tenant_label_used("MacroBlock");
  ASSERT_EQ(OB_SUCCESS, writer.write_loaded_centers(centers, 2, 0, 3));
  EXPECT_EQ(macro_before, get_tenant_label_used("MacroBlock"));
  for (int64_t shard : {2L, 1L, 0L}) {
    SCOPED_TRACE(shard);
    // Include an empty shard and keep the LoadedOutput owner alive: a closed
    // sibling must not retain macro buffers until the final QUANT barrier.
    ASSERT_EQ(OB_SUCCESS, writer.write_posting_range(shard));
    EXPECT_NE(nullptr, writer.loaded_output_);
    EXPECT_FALSE(writer.quantization_finished_);
    EXPECT_EQ(macro_before, get_tenant_label_used("MacroBlock"));
  }
  // Row-count checks must still work after the underlying writers are reset.
  ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 3, 2));
  EXPECT_EQ(5, writer.get_row_count());
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 2, 0));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
  EXPECT_EQ(7, writer.get_row_count());
  ObSSTableMergeRes merge_res;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  int64_t index_seq = index_window.start_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close_with_macro_seq(merge_res, index_seq,
      OB_DEFAULT_MACRO_BLOCK_SIZE, 0, pre_warm_param, index_window.end_));
  EXPECT_TRUE(merge_res.ivf_meta_.is_valid());
}
#endif

TEST_F(TestIVFSSTableWriter, merge_loaded_postings_match_ddl_with_empty_shard_and_encoded_reuse)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE));
  FullRebuildWriterVectorStore store(0);
  ObIVFVectorReader reader;
  ObArray<ObIVFVectorVID> vids, inserted_vids, reused_vids;
  ObArray<float> centers, vectors, inserted_vectors;
  ObArray<int64_t> primary, secondary, inserted_primary, inserted_secondary;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  ASSERT_EQ(OB_SUCCESS, reader.batch_get_by_vid(vids, 0, 2, vectors));
  for (int64_t i = 0; i < 10; ++i) { ASSERT_EQ(OB_SUCCESS, centers.push_back(float(i))); }
  ASSERT_EQ(OB_SUCCESS, primary.push_back(4)); ASSERT_EQ(OB_SUCCESS, primary.push_back(0));
  ASSERT_EQ(OB_SUCCESS, secondary.push_back(-1)); ASSERT_EQ(OB_SUCCESS, secondary.push_back(1));
  RecordingIVFDDLSliceWriter expected;
  RecordingIVFDDLRawWriters unused_raw;
  ObIVFDDLSSTableWriter reference(allocator_);
  ASSERT_EQ(OB_SUCCESS, reference.init(expected, unused_raw, IVF_SNAPSHOT_VERSION, 2, VIDA_L2, true));
  ASSERT_EQ(OB_SUCCESS, reference.quantize_loaded_vectors(centers, 2, vids, vectors, primary, &secondary));
  ObArray<uint32_t> child_counts;
  for (uint32_t count : {1U, 1U, 0U, 0U, 1U}) {
    ASSERT_EQ(OB_SUCCESS, child_counts.push_back(count));
  }
  ASSERT_EQ(OB_SUCCESS, reference.set_leaf_posting_counts(child_counts));
  ASSERT_EQ(OB_SUCCESS, reference.write_loaded_centers(centers, 2, 0, 5));
  ASSERT_EQ(OB_SUCCESS, reference.finish_quantization(2, 5, 3));
  ASSERT_EQ(8, expected.rows_.size());

  ObMacroSeqWindow data_window, index_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(1, index_window));
  ObIVFMergeSSTableWriter writer(allocator_);
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.prepare_loaded_output(5, 3));
  ASSERT_EQ(OB_SUCCESS, writer.init(whole_desc_.get_desc(), sstable_builder_, data_window,
      IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  ASSERT_TRUE(writer.supports_loaded_quantization());
  ASSERT_TRUE(writer.supports_loaded_posting_reuse());
  ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(5, 3));
  EXPECT_EQ(3, writer.posting_write_task_count());
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.prepare_loaded_output(5, 3));
  int64_t range_tasks = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.prepare_quantization(centers, 5, range_tasks));
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.finish_quantization(2, 5, 3));
  // Reuse the encoded source bytes without loading or rotating its vector.
  class Source final : public ObIVFEncodedPostingSource {
  public:
    ObString payload_;
    int scan_postings(int64_t cid, ObIVFEncodedPostingConsumer &consumer) const override
    { return cid == 7 ? consumer.append(0, payload_) : OB_INVALID_ARGUMENT; }
  } source;
  const auto &old_posting = expected.rows_.back();
  ASSERT_EQ(5, old_posting.cid_); ASSERT_EQ(0, old_posting.vid_);
  source.payload_.assign_ptr(old_posting.payload_.data(), old_posting.payload_.size());
  ASSERT_EQ(OB_SUCCESS, reused_vids.push_back(0));
  ASSERT_EQ(OB_SUCCESS, writer.copy_loaded_postings(source, 7, 4, reused_vids));
  ASSERT_EQ(OB_SUCCESS, inserted_vids.push_back(22));
  ASSERT_EQ(OB_SUCCESS, reader.batch_get_by_vid(inserted_vids, 0, 1, inserted_vectors));
  ASSERT_EQ(OB_SUCCESS, inserted_primary.push_back(0));
  ASSERT_EQ(OB_SUCCESS, inserted_secondary.push_back(1));
  ASSERT_EQ(OB_SUCCESS, writer.quantize_loaded_vectors(centers, 2, inserted_vids, inserted_vectors,
      inserted_primary, &inserted_secondary));
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.write_posting_range(3));
  // The middle CID shard has no postings. Roots close in reverse key order.
  for (int64_t i = 2; i >= 0; --i) { ASSERT_EQ(OB_SUCCESS, writer.write_posting_range(i)); }
  EXPECT_EQ(OB_INIT_TWICE, writer.write_posting_range(1));
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.finish_quantization(2, 5, 3));
  ASSERT_EQ(OB_SUCCESS, writer.write_loaded_centers(centers, 2, 0, 5));
  EXPECT_EQ(OB_INIT_TWICE, writer.write_loaded_centers(centers, 2, 0, 5));
  ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 5, 3));
  EXPECT_EQ(nullptr, writer.loaded_output_);
  EXPECT_EQ(0, writer.posting_write_task_count());
  EXPECT_EQ(OB_STATE_NOT_MATCH, writer.write_posting_range(0));
  ASSERT_EQ(1, writer.reused_posting_count_);
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 1, 2, 1));
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 1, 0));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
  ASSERT_TRUE(writer.is_finished());
  ASSERT_EQ(10, writer.get_row_count());

  ObSSTableMergeRes merge_res;
  ObTabletCreateSSTableParam create_param;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  int64_t index_seq = index_window.start_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close_with_macro_seq(merge_res, index_seq,
      OB_DEFAULT_MACRO_BLOCK_SIZE, 0, pre_warm_param, index_window.end_));
  ASSERT_EQ(OB_SUCCESS, fill_create_param(merge_res, create_param));
  create_param.ivf_meta_ = writer.get_ivf_meta();
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      create_param, allocator_, table_handle));
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObTableIterParam iter_param;
  iter_param.table_id_ = IVF_TABLE_ID; iter_param.tablet_id_ = IVF_TABLET_ID;
  iter_param.read_info_ = &read_info;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  context.query_flag_.set_not_use_row_cache(); context.query_flag_.set_not_use_block_cache();
  context.query_flag_.scan_order_ = ObQueryFlag::Forward;
  context.store_ctx_ = &store_ctx; context.ls_id_ = IVF_LS_ID; context.tablet_id_ = IVF_TABLET_ID;
  context.allocator_ = &allocator_; context.stmt_allocator_ = &allocator_;
  context.trans_version_range_.base_version_ = 0;
  context.trans_version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, context.micro_block_handle_mgr_.init(false, context.tablet_id_,
      context.table_store_stat_, context.table_scan_stat_, context.query_flag_));
  context.is_inited_ = true;
  ObDatumRange range;
  range.set_whole_range();
  ObStoreRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable->scan(iter_param, context, range, iter));
  for (int64_t i = 0; i < 10; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(row));
    const ObString payload = row->storage_datums_[3].get_string();
    if (i < 8) {
      const auto &want = expected.rows_[i];
      EXPECT_EQ(want.type_, row->storage_datums_[0].get_int());
      EXPECT_EQ(want.cid_, row->storage_datums_[1].get_int());
      EXPECT_EQ(want.vid_, row->storage_datums_[2].get_uint());
      EXPECT_EQ(want.payload_, std::string(payload.ptr(), payload.length()));
    } else {
      EXPECT_EQ(IVF_ROW_TYPE_RAW_VECTOR, row->storage_datums_[0].get_int());
      EXPECT_EQ(vids.at(i - 8), row->storage_datums_[2].get_uint());
      ASSERT_EQ(2 * sizeof(float), payload.length());
      EXPECT_EQ(0, MEMCMP(&vectors.at((i - 8) * 2), payload.ptr(), payload.length()));
    }
  }
  const ObDatumRow *row = nullptr;
  EXPECT_EQ(OB_ITER_END, iter->get_next_row(row));
  iter->~ObStoreRowIterator();
}

TEST_F(TestIVFSSTableWriter, merge_loaded_runtime_without_soar_prepares_and_finishes_physical_output)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE));
  class SourceStore final : public ObIVFVectorStore, public ObIVFFirstLevelSampleSet {
  public:
    bool is_valid() const override { return true; }
    bool supports_source_order() const override { return true; }
    bool is_fully_memory_resident() const override { return true; }
    int64_t get_available_replay_memory() const override { return 64L * 1024 * 1024; }
    int64_t get_vector_count() const override { return 2; }
    int64_t get_sample_count() const override { return 2; }
    int64_t get_dimension() const override { return 2; }
    int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override { return data_.get_all_vids(vids); }
    int get_source_row_idx(int64_t idx, int64_t &source) const override
    { source = idx; return idx >= 0 && idx < 2 ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
    int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids, int64_t begin, int64_t end,
        ObIArray<float> &values) const override { return data_.batch_get_by_vid(vids, begin, end, values); }
    int batch_get_by_source(const ObIArray<int64_t> &idxs, ObIArray<float> &values) const override
    {
      ObArray<ObIVFVectorVID> vids;
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
        if (idxs.at(i) < 0 || idxs.at(i) >= 2) { ret = OB_INVALID_ARGUMENT; }
        else { ret = vids.push_back(idxs.at(i) == 0 ? 11 : 22); }
      }
      return OB_FAIL(ret) ? ret : data_.batch_get_by_vid(vids, 0, vids.count(), values);
    }
    int batch_get_samples(int64_t begin, int64_t end, ObIArray<ObIVFVectorVID> &vids,
        ObIArray<float> &values) const override
    {
      vids.reuse();
      int ret = begin < 0 || end > 2 || end <= begin ? OB_INVALID_ARGUMENT : OB_SUCCESS;
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) { ret = vids.push_back(i == 0 ? 11 : 22); }
      return OB_FAIL(ret) ? ret : data_.batch_get_by_vid(vids, 0, vids.count(), values);
    }
    FullRebuildWriterVectorStore data_;
  } store;
  ObIVFMergeInput input;
  ASSERT_EQ(OB_SUCCESS, input.init(store, store, 2, false, true, false /* no SOAR */, VIDA_L2));
  ObIVFDDLHcaAdapter algorithm;
  ObIVFHcaAlgorithmParam algorithm_param;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(algorithm_param));
  ObMacroSeqWindow data_window, index_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(1, index_window));
  ObIVFMergeSSTableWriter output(allocator_);
  ASSERT_EQ(OB_SUCCESS, output.init(whole_desc_.get_desc(), sstable_builder_, data_window,
      IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  ObIVFMergeProcessor processor;
  ASSERT_EQ(OB_SUCCESS, processor.init(allocator_, input, algorithm, output, ObIVFProcessType::FULL_REBUILD));
  ASSERT_EQ(OB_SUCCESS, processor.configure_ddl_runtime(algorithm, 2));
  ObIVFFullRebuildPlanner planner;
  ASSERT_EQ(OB_SUCCESS, planner.init(allocator_, processor));
  const ObIVFTaskPlan *plan = nullptr;
  ASSERT_EQ(OB_SUCCESS, planner.start(plan));
  bool saw_soar = false, saw_quant = false;
  for (int64_t steps = 0; plan != nullptr && steps < 64; ++steps) {
    const auto step = planner.get_step();
    if (step == ObIVFMergeStep::SOAR_ASSIGN) { saw_soar = true; }
    if (step == ObIVFMergeStep::QUANT) {
      saw_quant = true;
      EXPECT_GT(output.posting_write_task_count(), 0);
    }
    // Reverse execution catches QUANT tasks accidentally mixing encoders and
    // sorter draining in the same barrier when secondary assignment is off.
    for (int64_t task = plan->get_task_count(); task-- > 0;) {
      ASSERT_EQ(OB_SUCCESS, planner.process_task(*plan, task));
    }
    const ObIVFTaskPlan *next = nullptr;
    ASSERT_EQ(OB_SUCCESS, planner.finish_task_plan(*plan, next));
    plan = next;
  }
  ASSERT_EQ(nullptr, plan);
  ASSERT_TRUE(saw_soar); ASSERT_TRUE(saw_quant); ASSERT_TRUE(output.is_finished());
  ASSERT_EQ(2, output.one_bit_row_count_);
  ASSERT_EQ(2, output.raw_vector_row_count_);
  ASSERT_GT(output.get_ivf_meta().centroid_count_, 0);
  ASSERT_EQ(4 + output.get_ivf_meta().centroid_count_, output.get_row_count());
  ObSSTableMergeRes result;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  int64_t index_seq = index_window.start_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close_with_macro_seq(result, index_seq,
      OB_DEFAULT_MACRO_BLOCK_SIZE, 0, pre_warm_param, index_window.end_));
  EXPECT_EQ(output.get_row_count(), result.row_count_);
  EXPECT_EQ(output.get_ivf_meta().centroid_count_, result.ivf_meta_.centroid_count_);
  EXPECT_EQ(IVF_SNAPSHOT_VERSION, result.max_merged_trans_version_);
}

TEST_F(TestIVFSSTableWriter, merge_loaded_reuse_partial_source_failure_blocks_publication)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE));
  ObMacroSeqWindow data_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ObIVFMergeSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, writer.init(whole_desc_.get_desc(), sstable_builder_, data_window,
      IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, writer.prepare_loaded_output(1, 1));
  class FailingSource final : public ObIVFEncodedPostingSource {
  public:
    std::string code_;
    int scan_postings(int64_t cid, ObIVFEncodedPostingConsumer &consumer) const override
    {
      UNUSED(cid);
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < 4096; ++i) {
        ret = consumer.append(i, ObString(code_.size(), code_.data()));
      }
      return ret == OB_SUCCESS ? OB_IO_ERROR : ret;
    }
  } source;
  source.code_.assign(writer.get_one_bit_code_size(), 'x');
  ObArray<ObIVFVectorVID> expected;
  for (int64_t i = 0; i < 4097; ++i) { ASSERT_EQ(OB_SUCCESS, expected.push_back(i)); }
  // One whole batch has entered the sorter before the source read fails.
  EXPECT_EQ(OB_IO_ERROR, writer.copy_loaded_postings(source, 0, 0, expected));
  EXPECT_EQ(OB_IO_ERROR, writer.write_posting_range(0));
  EXPECT_EQ(OB_IO_ERROR, writer.finish_quantization(4097, 1, 4097));
  EXPECT_NE(nullptr, writer.loaded_output_); // failure teardown waits for sibling tasks
  EXPECT_FALSE(writer.is_finished());
  EXPECT_EQ(0, writer.reused_posting_count_);
}

TEST_F(TestIVFSSTableWriter, merge_writer_preserves_rotated_raw_vectors)
{
  // Match production SS, where index builder init disables small-SSTable
  // packing. A single compaction slot still has multiple IVF writer roots.
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(
      1 /* concurrent_cnt */, ObSSTableIndexBuilder::DISABLE));
  FullRebuildWriterVectorStore vector_store;
  storage::ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(vector_store));
  ObSEArray<storage::ObIVFVectorVID, 2> all_vids;
  ASSERT_EQ(OB_SUCCESS, vector_store.get_all_vids(all_vids));
  ObSEArray<float, 2> centers;
  ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(3.0F));

  ObMacroSeqWindow data_window;
  ObMacroSeqWindow index_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(1, index_window));
  ObIVFMergeSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, writer.init(
      whole_desc_.get_desc(), sstable_builder_, data_window, IVF_SNAPSHOT_VERSION,
      2 /* vector_dim */, VIDA_L2));
  ASSERT_EQ(OB_SUCCESS, writer.quantize_cluster(
      reader, centers, 1 /* center_count */, 0 /* cid */,
      all_vids, nullptr /* secondary_member_vids */));
  ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(
      2 /* vector_count */, 1 /* center_count */, 2 /* posting_count */));
  EXPECT_EQ(2, writer.get_ivf_meta().vector_count_);
  // Close RAW roots in reverse range order. The SSTable index builder must
  // merge the independent writers by rowkey, not by task completion order.
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(
      reader, all_vids, 1 /* vid_begin */, 2 /* vid_end */, 1 /* task_idx */));
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(
      reader, all_vids, 0 /* vid_begin */, 1 /* vid_end */, 0 /* task_idx */));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2 /* vector_count */));
  ASSERT_TRUE(writer.is_finished());
  ASSERT_EQ(5, writer.get_row_count());
  ASSERT_EQ(0, writer.sorter_.get_row_count());
  ASSERT_TRUE(writer.get_ivf_meta().is_valid());
  ASSERT_EQ(1, writer.get_ivf_meta().nbits_);

  ObSSTableMergeRes merge_res;
  ObTabletCreateSSTableParam create_param;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  int64_t index_seq = index_window.start_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close_with_macro_seq(
      merge_res, index_seq, OB_DEFAULT_MACRO_BLOCK_SIZE, 0 /* nested_offset */,
      pre_warm_param, index_window.end_));
  ASSERT_TRUE(merge_res.ivf_meta_.is_valid());
  EXPECT_EQ(writer.get_ivf_meta().dist_type_, merge_res.ivf_meta_.dist_type_);
  EXPECT_EQ(writer.get_ivf_meta().dim_, merge_res.ivf_meta_.dim_);
  EXPECT_EQ(2, merge_res.ivf_meta_.vector_count_);
  ASSERT_EQ(OB_SUCCESS, fill_create_param(merge_res, create_param));
  create_param.ivf_meta_ = writer.get_ivf_meta();
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS,
      ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
          create_param, allocator_, table_handle));
  ObSSTable *base_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(base_sstable));
  ASSERT_NE(nullptr, base_sstable);
  ObIvfSSTable *ivf_sstable = static_cast<ObIvfSSTable *>(base_sstable);
  ASSERT_EQ(5, ivf_sstable->get_row_count());
  EXPECT_EQ(2, ivf_sstable->get_ivf_meta().vector_count_);

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObTableIterParam iter_param;
  iter_param.table_id_ = IVF_TABLE_ID;
  iter_param.tablet_id_ = IVF_TABLET_ID;
  iter_param.read_info_ = &read_info;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
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
  ASSERT_EQ(OB_SUCCESS, context.micro_block_handle_mgr_.init(
      false, context.tablet_id_, context.table_store_stat_,
      context.table_scan_stat_, context.query_flag_));
  context.is_inited_ = true;
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObStoreRowIterator *row_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      ivf_sstable->scan(iter_param, context, whole_range, row_iter));
  ASSERT_NE(nullptr, row_iter);

  const int64_t expected_types[] = {
      blocksstable::IVF_ROW_TYPE_CENTROID,
      blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
      blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
      blocksstable::IVF_ROW_TYPE_RAW_VECTOR,
      blocksstable::IVF_ROW_TYPE_RAW_VECTOR,
  };
  const int64_t expected_cids[] = {0, 1, 1, IVF_RAW_VECTOR_CID,
                                   IVF_RAW_VECTOR_CID};
  const uint64_t expected_vids[] = {1, 11, 22, 11, 22};
  float expected_raw_vectors[2][2] = {};
  expected_raw_vectors[0][0] = 1.0F;
  expected_raw_vectors[0][1] = 2.0F;
  expected_raw_vectors[1][0] = 3.0F;
  expected_raw_vectors[1][1] = 4.0F;
  int64_t type_counts[6] = {0};
  int64_t row_idx = 0;
  const ObDatumRow *row = nullptr;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ret = row_iter->get_next_row(row);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(nullptr, row);
    const int64_t row_type = row->storage_datums_[0].get_int();
    ASSERT_TRUE(blocksstable::IVF_ROW_TYPE_CENTROID == row_type
        || blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type
        || blocksstable::IVF_ROW_TYPE_RAW_VECTOR == row_type);
    ASSERT_LT(row_idx, ARRAYSIZEOF(expected_types));
    EXPECT_EQ(expected_types[row_idx], row_type);
    EXPECT_EQ(expected_cids[row_idx], row->storage_datums_[1].get_int());
    EXPECT_EQ(expected_vids[row_idx], row->storage_datums_[2].get_uint());
    const ObString payload = row->storage_datums_[3].get_string();
    if (blocksstable::IVF_ROW_TYPE_CENTROID == row_type) {
      const float expected_center[] = {2.0F, 3.0F};
      ASSERT_EQ(ObIvfCentroidPayload::get_size(2), payload.length());
      const float *decoded = nullptr;
      uint32_t children = 0;
      ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(payload, 2, decoded, children));
      EXPECT_EQ(0, MEMCMP(expected_center, decoded, sizeof(expected_center)));
      EXPECT_FLOAT_EQ(2.0F, centers.at(0));
      EXPECT_FLOAT_EQ(3.0F, centers.at(1));
    } else if (blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type) {
      EXPECT_EQ(writer.get_one_bit_code_size(), payload.length());
    } else {
      const int64_t raw_idx = row_idx - 3;
      ASSERT_GE(raw_idx, 0);
      ASSERT_LT(raw_idx, 2);
      ASSERT_EQ(2 * sizeof(float), payload.length());
      EXPECT_EQ(0, MEMCMP(expected_raw_vectors[raw_idx], payload.ptr(),
                          payload.length()));
    }
    ++type_counts[row_type];
    ++row_idx;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  row_iter->~ObStoreRowIterator();
  EXPECT_EQ(ARRAYSIZEOF(expected_types), row_idx);
  EXPECT_EQ(1, type_counts[blocksstable::IVF_ROW_TYPE_CENTROID]);
  EXPECT_EQ(2,
      type_counts[blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR]);
  EXPECT_EQ(0,
      type_counts[blocksstable::IVF_ROW_TYPE_HIGH_PRECISION_VECTOR]);
  EXPECT_EQ(2, type_counts[blocksstable::IVF_ROW_TYPE_RAW_VECTOR]);
}

TEST_F(TestIVFSSTableWriter, frozen_empty_leaf_survives_quantization_and_readback)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(1, ObSSTableIndexBuilder::DISABLE));
  FullRebuildWriterVectorStore store(0);
  ObIVFVectorReader reader;
  ObArray<ObIVFVectorVID> vids, empty;
  ObArray<float> centers;
  ASSERT_EQ(OB_SUCCESS, reader.init(store));
  ASSERT_EQ(OB_SUCCESS, store.get_all_vids(vids));
  for (float v : {2.0F, 3.0F, 100.0F, 101.0F}) { ASSERT_EQ(OB_SUCCESS, centers.push_back(v)); }
  class EmptySource final : public ObIVFEncodedPostingSource {
  public:
    int scan_postings(int64_t cid, ObIVFEncodedPostingConsumer &consumer) const override
    { UNUSED(consumer); return cid == 1 ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
  } source;
  ObMacroSeqWindow data_window, index_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(1, index_window));
  ObIVFMergeSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, writer.init(whole_desc_.get_desc(), sstable_builder_, data_window,
      IVF_SNAPSHOT_VERSION, 2, VIDA_L2));
  int64_t tasks = 0;
  ASSERT_EQ(OB_SUCCESS, writer.prepare_quantization(centers, 2, tasks));
  for (int64_t cid = 1; cid >= 0; --cid) {
    ObIVFMergeQuantizer *range = nullptr;
    ASSERT_EQ(OB_SUCCESS, writer.open_quantization_range(cid, cid + 1, cid, allocator_, range));
    const int code = cid == 0 ? range->quantize_cluster(reader, centers, 2, cid, vids, nullptr)
        : range->quantize_cluster_with_reuse(reader, centers, 2, cid, empty, nullptr, source, cid, 0);
    const int close_code = writer.close_quantization_range(range, code == OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, code); ASSERT_EQ(OB_SUCCESS, close_code);
  }
  ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 2, 2));
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, vids, 0, 2, 0));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2));
  ObSSTableMergeRes merge_res;
  ObTabletCreateSSTableParam create_param;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  int64_t index_seq = index_window.start_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close_with_macro_seq(merge_res, index_seq,
      OB_DEFAULT_MACRO_BLOCK_SIZE, 0, pre_warm_param, index_window.end_));
  ASSERT_EQ(OB_SUCCESS, fill_create_param(merge_res, create_param));
  create_param.ivf_meta_ = writer.get_ivf_meta();
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      create_param, allocator_, table_handle));
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(sstable));
  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObTableIterParam iter_param;
  iter_param.table_id_ = IVF_TABLE_ID; iter_param.tablet_id_ = IVF_TABLET_ID;
  iter_param.read_info_ = &read_info;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  context.query_flag_.set_not_use_row_cache(); context.query_flag_.set_not_use_block_cache();
  context.query_flag_.scan_order_ = ObQueryFlag::Forward;
  context.store_ctx_ = &store_ctx; context.ls_id_ = IVF_LS_ID; context.tablet_id_ = IVF_TABLET_ID;
  context.allocator_ = &allocator_; context.stmt_allocator_ = &allocator_;
  context.trans_version_range_.base_version_ = 0;
  context.trans_version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
  ASSERT_EQ(OB_SUCCESS, context.micro_block_handle_mgr_.init(false, context.tablet_id_,
      context.table_store_stat_, context.table_scan_stat_, context.query_flag_));
  context.is_inited_ = true;
  ObDatumRange range;
  range.set_whole_range();
  ObStoreRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable->scan(iter_param, context, range, iter));
  for (int64_t i = 0; i < 6; ++i) {
    const ObDatumRow *row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter->get_next_row(row));
    const ObString bytes = row->storage_datums_[3].get_string();
    if (i < 2) {
      EXPECT_EQ(IVF_ROW_TYPE_CENTROID, row->storage_datums_[0].get_int());
      EXPECT_EQ(i + 1, row->storage_datums_[2].get_uint());
      const float *center = nullptr;
      uint32_t count = 0;
      ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(bytes, 2, center, count));
      EXPECT_EQ(i == 0 ? 2 : 0, count);
      EXPECT_EQ(0, MEMCMP(center, &centers.at(i * 2), 2 * sizeof(float)));
    } else if (i < 4) {
      EXPECT_EQ(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, row->storage_datums_[0].get_int());
      EXPECT_EQ(1, row->storage_datums_[1].get_int());
      EXPECT_EQ(vids.at(i - 2), row->storage_datums_[2].get_uint());
    } else {
      EXPECT_EQ(IVF_ROW_TYPE_RAW_VECTOR, row->storage_datums_[0].get_int());
      EXPECT_EQ(vids.at(i - 4), row->storage_datums_[2].get_uint());
    }
  }
  const ObDatumRow *row = nullptr;
  EXPECT_EQ(OB_ITER_END, iter->get_next_row(row));
  iter->~ObStoreRowIterator();
}

TEST_F(TestIVFSSTableWriter, parallel_quant_ranges_merge_interleaved_reused_vids)
{
  // Match production SS, where index builder init disables small-SSTable
  // packing. A single compaction slot still has multiple IVF writer roots.
  ASSERT_EQ(OB_SUCCESS, prepare_write_env(
      1 /* concurrent_cnt */, ObSSTableIndexBuilder::DISABLE));
  FullRebuildWriterVectorStore vector_store(0 /* first_vid */);
  storage::ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(vector_store));
  ObSEArray<storage::ObIVFVectorVID, 2> all_vids;
  ASSERT_EQ(OB_SUCCESS, vector_store.get_all_vids(all_vids));
  ObSEArray<float, 4> centers;
  ASSERT_EQ(OB_SUCCESS, centers.push_back(2.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(3.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(4.0F));
  ASSERT_EQ(OB_SUCCESS, centers.push_back(5.0F));

  ObMacroSeqWindow data_window;
  ObMacroSeqWindow index_window;
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, data_window));
  ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(1, index_window));
  ObIVFMergeSSTableWriter writer(allocator_);
  ASSERT_EQ(OB_SUCCESS, writer.init(
      whole_desc_.get_desc(), sstable_builder_, data_window, IVF_SNAPSHOT_VERSION,
      2 /* vector_dim */, VIDA_L2));
  int64_t task_count = 0;
  ASSERT_EQ(OB_SUCCESS, writer.prepare_quantization(centers, 2, task_count));
  ASSERT_EQ(2, task_count);
  class Source final : public storage::ObIVFEncodedPostingSource {
  public:
    ObString codes_[2];
    int scan_postings(int64_t cid, storage::ObIVFEncodedPostingConsumer &consumer) const override
    { return consumer.append(cid == 0 ? 22 : 0, codes_[cid]); }
  } source;
  quantization::ObQuantizationParams params = {};
  params.dim_ = 2; params.bits_ = 1;
  quantization::ObQuantizationEncoder encoder;
  ASSERT_EQ(OB_SUCCESS, encoder.init(allocator_, quantization::ObQuantizationType::RABITQ, VIDA_L2, params));
  for (int64_t cid = 0; cid < 2; ++cid) {
    char *code = static_cast<char *>(allocator_.alloc(encoder.code_size()));
    ASSERT_NE(nullptr, code);
    const float vector[2] = {cid == 0 ? 3.0F : 1.0F, cid == 0 ? 4.0F : 2.0F};
    ASSERT_EQ(OB_SUCCESS, encoder.encode(vector, &centers.at(cid * 2),
        {reinterpret_cast<uint8_t *>(code), encoder.code_size()}));
    source.codes_[cid].assign_ptr(code, static_cast<int32_t>(encoder.code_size()));
  }
  // Complete the higher CID range first, then merge through the shared builder.
  for (int64_t cid = 1; cid >= 0; --cid) {
    storage::ObIVFMergeQuantizer *range = nullptr;
    ASSERT_EQ(OB_SUCCESS, writer.open_quantization_range(cid, cid + 1, cid, allocator_, range));
    ASSERT_NE(nullptr, range);
    ObSEArray<storage::ObIVFVectorVID, 2> members;
    ASSERT_EQ(OB_SUCCESS, members.push_back(cid == 0 ? 22 : 0)); // old prefix, including VID 0
    ASSERT_EQ(OB_SUCCESS, members.push_back(cid == 0 ? 0 : 22)); // inserted, including VID 0
    const int code = range->quantize_cluster_with_reuse(reader, centers, 2, cid, members,
        nullptr, source, cid, 1);
    const int close_code = writer.close_quantization_range(range, code == OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, code); ASSERT_EQ(OB_SUCCESS, close_code);
    ASSERT_EQ(nullptr, range);
  }
  ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(2, 2, 4));
  // Secondary assignments double postings, but N must remain two vectors.
  EXPECT_EQ(2, writer.get_ivf_meta().vector_count_);
  ASSERT_EQ(2, writer.reused_posting_count_);
  // Close RAW roots in reverse range order. The SSTable index builder must
  // merge the independent writers by rowkey, not by task completion order.
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(
      reader, all_vids, 1 /* vid_begin */, 2 /* vid_end */, 1 /* task_idx */));
  ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(
      reader, all_vids, 0 /* vid_begin */, 1 /* vid_end */, 0 /* task_idx */));
  ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(2 /* vector_count */));
  ASSERT_TRUE(writer.is_finished());
  ASSERT_EQ(8, writer.get_row_count());
  ASSERT_EQ(0, writer.sorter_.get_row_count());
  ASSERT_TRUE(writer.get_ivf_meta().is_valid());
  ASSERT_EQ(1, writer.get_ivf_meta().nbits_);

  ObSSTableMergeRes merge_res;
  ObTabletCreateSSTableParam create_param;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  int64_t index_seq = index_window.start_;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close_with_macro_seq(
      merge_res, index_seq, OB_DEFAULT_MACRO_BLOCK_SIZE, 0 /* nested_offset */,
      pre_warm_param, index_window.end_));
  ASSERT_TRUE(merge_res.ivf_meta_.is_valid());
  EXPECT_EQ(writer.get_ivf_meta().dist_type_, merge_res.ivf_meta_.dist_type_);
  EXPECT_EQ(writer.get_ivf_meta().dim_, merge_res.ivf_meta_.dim_);
  EXPECT_EQ(2, merge_res.ivf_meta_.vector_count_);
  ASSERT_EQ(OB_SUCCESS, fill_create_param(merge_res, create_param));
  create_param.ivf_meta_ = writer.get_ivf_meta();
  ObTableHandleV2 table_handle;
  ASSERT_EQ(OB_SUCCESS,
      ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
          create_param, allocator_, table_handle));
  ObSSTable *base_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, table_handle.get_sstable(base_sstable));
  ASSERT_NE(nullptr, base_sstable);
  ObIvfSSTable *ivf_sstable = static_cast<ObIvfSSTable *>(base_sstable);
  ASSERT_EQ(8, ivf_sstable->get_row_count());
  EXPECT_EQ(2, ivf_sstable->get_ivf_meta().vector_count_);

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ObTableIterParam iter_param;
  iter_param.table_id_ = IVF_TABLE_ID;
  iter_param.tablet_id_ = IVF_TABLET_ID;
  iter_param.read_info_ = &read_info;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
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
  ASSERT_EQ(OB_SUCCESS, context.micro_block_handle_mgr_.init(
      false, context.tablet_id_, context.table_store_stat_,
      context.table_scan_stat_, context.query_flag_));
  context.is_inited_ = true;
  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObStoreRowIterator *row_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      ivf_sstable->scan(iter_param, context, whole_range, row_iter));
  ASSERT_NE(nullptr, row_iter);

  const int64_t expected_types[] = {
      IVF_ROW_TYPE_CENTROID, IVF_ROW_TYPE_CENTROID,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR, IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR, IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
      IVF_ROW_TYPE_RAW_VECTOR, IVF_ROW_TYPE_RAW_VECTOR};
  const int64_t expected_cids[] = {0, 0, 1, 1, 2, 2, IVF_RAW_VECTOR_CID, IVF_RAW_VECTOR_CID};
  const uint64_t expected_vids[] = {1, 2, 0, 22, 0, 22, 0, 22};
  float expected_raw_vectors[2][2] = {};
  expected_raw_vectors[0][0] = 1.0F;
  expected_raw_vectors[0][1] = 2.0F;
  expected_raw_vectors[1][0] = 3.0F;
  expected_raw_vectors[1][1] = 4.0F;
  int64_t type_counts[6] = {0};
  int64_t row_idx = 0;
  const ObDatumRow *row = nullptr;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ret = row_iter->get_next_row(row);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(nullptr, row);
    const int64_t row_type = row->storage_datums_[0].get_int();
    ASSERT_TRUE(blocksstable::IVF_ROW_TYPE_CENTROID == row_type
        || blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type
        || blocksstable::IVF_ROW_TYPE_RAW_VECTOR == row_type);
    ASSERT_LT(row_idx, ARRAYSIZEOF(expected_types));
    EXPECT_EQ(expected_types[row_idx], row_type);
    EXPECT_EQ(expected_cids[row_idx], row->storage_datums_[1].get_int());
    EXPECT_EQ(expected_vids[row_idx], row->storage_datums_[2].get_uint());
    const ObString payload = row->storage_datums_[3].get_string();
    if (blocksstable::IVF_ROW_TYPE_CENTROID == row_type) {
      ASSERT_EQ(ObIvfCentroidPayload::get_size(2), payload.length());
      const float *decoded = nullptr;
      uint32_t children = 0;
      ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(payload, 2, decoded, children));
      EXPECT_EQ(2, children);
      EXPECT_EQ(0, MEMCMP(&centers.at((expected_vids[row_idx] - 1) * 2), decoded, 2 * sizeof(float)));
      EXPECT_FLOAT_EQ(2.0F, centers.at(0));
      EXPECT_FLOAT_EQ(3.0F, centers.at(1));
    } else if (blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type) {
      EXPECT_EQ(writer.get_one_bit_code_size(), payload.length());
      if (row_idx == 3 || row_idx == 4) {
        const ObString &code = source.codes_[expected_cids[row_idx] - 1];
        EXPECT_EQ(0, MEMCMP(code.ptr(), payload.ptr(), payload.length()));
      }
    } else {
      const int64_t raw_idx = row_idx - 6;
      ASSERT_GE(raw_idx, 0);
      ASSERT_LT(raw_idx, 2);
      ASSERT_EQ(2 * sizeof(float), payload.length());
      EXPECT_EQ(0, MEMCMP(expected_raw_vectors[raw_idx], payload.ptr(),
                          payload.length()));
    }
    ++type_counts[row_type];
    ++row_idx;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  row_iter->~ObStoreRowIterator();
  EXPECT_EQ(ARRAYSIZEOF(expected_types), row_idx);
  EXPECT_EQ(2, type_counts[blocksstable::IVF_ROW_TYPE_CENTROID]);
  EXPECT_EQ(4,
      type_counts[blocksstable::IVF_ROW_TYPE_LOW_PRECISION_VECTOR]);
  EXPECT_EQ(0,
      type_counts[blocksstable::IVF_ROW_TYPE_HIGH_PRECISION_VECTOR]);
  EXPECT_EQ(2, type_counts[blocksstable::IVF_ROW_TYPE_RAW_VECTOR]);
}

TEST_F(TestIVFSSTableWriter, write_types_with_shared_writer_flush_micro)
{
  const int64_t centroid_cnt = 4;
  const int64_t posting_cnt = 5;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());

  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, append_type_rows(writer, IVF_ROW_TYPE_CENTROID, centroid_cnt, 0));
  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block());
  ASSERT_EQ(OB_SUCCESS,
      append_type_rows(writer, IVF_ROW_TYPE_LOW_PRECISION_VECTOR, posting_cnt, 1));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ASSERT_TRUE(sstable_.is_ivf_sstable());
  ASSERT_EQ(centroid_cnt + posting_cnt, sstable_.get_row_count());
  ASSERT_GE(sstable_.get_data_macro_block_count(), 1);
}

TEST_F(TestIVFSSTableWriter, append_batch_flushes_row_type_boundaries)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());

  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(writer, 0));

  const int64_t first_types[] = {
      IVF_ROW_TYPE_CENTROID,
      IVF_ROW_TYPE_CENTROID,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
  };
  ObDatumRow first_rows[ARRAYSIZEOF(first_types)];
  ObRowVectorConverter first_converter;
  ObBatchDatumRows first_batch;
  ASSERT_EQ(OB_SUCCESS, make_ivf_batch(
      first_types, ARRAYSIZEOF(first_types), first_rows,
      first_converter, first_batch));
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(first_batch));
  EXPECT_EQ(1, writer.get_merge_block_info().new_micro_count_in_new_macro_);

  const int64_t second_types[] = {
      IVF_ROW_TYPE_RAW_VECTOR,
      IVF_ROW_TYPE_RAW_VECTOR,
  };
  ObDatumRow second_rows[ARRAYSIZEOF(second_types)];
  ObRowVectorConverter second_converter;
  ObBatchDatumRows second_batch;
  ASSERT_EQ(OB_SUCCESS, make_ivf_batch(
      second_types, ARRAYSIZEOF(second_types), second_rows,
      second_converter, second_batch));
  ASSERT_EQ(OB_SUCCESS, writer.append_batch(second_batch));
  EXPECT_EQ(2, writer.get_merge_block_info().new_micro_count_in_new_macro_);
  EXPECT_EQ(ARRAYSIZEOF(first_types) + ARRAYSIZEOF(second_types),
            writer.get_appended_row_count());

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(3, writer.get_merge_block_info().new_micro_count_in_new_macro_);
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(ARRAYSIZEOF(first_types) + ARRAYSIZEOF(second_types),
            sstable_.get_row_count());
}

TEST_F(TestIVFSSTableWriter, append_batch_failure_keeps_successful_prefix)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());

  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_format_aware_writer(writer, 0));

  const int64_t row_types[] = {
      IVF_ROW_TYPE_CENTROID,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR,
      IVF_ROW_TYPE_INVALID,
      IVF_ROW_TYPE_RAW_VECTOR,
  };
  ObDatumRow rows[ARRAYSIZEOF(row_types)];
  ObRowVectorConverter converter;
  ObBatchDatumRows batch;
  ASSERT_EQ(OB_SUCCESS, make_ivf_batch(
      row_types, ARRAYSIZEOF(row_types), rows, converter, batch));
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.append_batch(batch));
  EXPECT_EQ(2, writer.get_appended_row_count());
  EXPECT_EQ(1, writer.get_merge_block_info().new_micro_count_in_new_macro_);

  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(2, writer.get_merge_block_info().new_micro_count_in_new_macro_);
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());
  ASSERT_EQ(2, sstable_.get_row_count());
}

TEST_F(TestIVFSSTableWriter, flush_micro_block_not_init_and_empty)
{
  ObIvfMacroBlockWriter not_opened;
  ASSERT_EQ(OB_NOT_INIT, not_opened.flush_micro_block());

  const int64_t row_cnt = 4;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());

  ObIvfMacroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, open_writer(writer, 0));
  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block());
  ASSERT_EQ(OB_SUCCESS, append_type_rows(writer, IVF_ROW_TYPE_CENTROID, row_cnt, 0));
  const int64_t micro_cnt_before_flush =
      writer.get_merge_block_info().new_micro_count_in_new_macro_;
  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block());
  ASSERT_EQ(micro_cnt_before_flush + 1,
      writer.get_merge_block_info().new_micro_count_in_new_macro_);
  ASSERT_EQ(OB_SUCCESS, writer.flush_micro_block());
  ASSERT_EQ(micro_cnt_before_flush + 1,
      writer.get_merge_block_info().new_micro_count_in_new_macro_);
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ASSERT_TRUE(sstable_.is_ivf_sstable());
  ASSERT_EQ(row_cnt, sstable_.get_row_count());
}

TEST_F(TestIVFSSTableWriter, empty_merge_metadata_survives_builder_close)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ObIvfSSTableMeta empty_meta;
  empty_meta.set_empty();
  EXPECT_EQ(OB_ERR_UNEXPECTED, sstable_builder_.set_ivf_meta(empty_meta));
  sstable_builder_.set_is_async_index(true);
  ObIvfSSTableMeta invalid_meta;
  EXPECT_EQ(OB_INVALID_ARGUMENT, sstable_builder_.set_ivf_meta(invalid_meta));
  invalid_meta.set_empty();
  invalid_meta.dim_ = 2;
  EXPECT_EQ(OB_INVALID_ARGUMENT, sstable_builder_.set_ivf_meta(invalid_meta));
  ASSERT_FALSE(sstable_builder_.get_ivf_meta().is_valid());
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.set_ivf_meta(empty_meta));
  EXPECT_EQ(OB_INIT_TWICE, sstable_builder_.set_ivf_meta(empty_meta));

  table_schema_.set_table_type(USER_INDEX);
  table_schema_.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  table_schema_.set_data_table_id(table_schema_.get_table_id());
  for (const ObMergeType merge_type : {MAJOR_MERGE, MEDIUM_MERGE, INC_MAJOR_MERGE}) {
    SCOPED_TRACE(merge_type);
    ObTabletMergeDagParam merge_param(merge_type, ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID), -1);
    merge_param.merge_version_ = IVF_SNAPSHOT_VERSION;
    ObAsyncIndexMergeCtx merge_ctx(merge_param, allocator_);
    ObStorageSchema *merge_schema = OB_NEWx(ObStorageSchema, &allocator_);
    ASSERT_NE(nullptr, merge_schema);
    ASSERT_EQ(OB_SUCCESS, merge_schema->init(
        allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    merge_ctx.static_param_.schema_ = merge_schema;
    merge_ctx.static_param_.is_ivf_index_ = true;
    merge_ctx.static_param_.version_range_.base_version_ =
        INC_MAJOR_MERGE == merge_type ? IVF_SNAPSHOT_VERSION / 2 : 0;
    merge_ctx.static_param_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    merge_ctx.static_param_.rec_scn_.set_min();
    if (INC_MAJOR_MERGE == merge_type) {
      ASSERT_EQ(OB_SUCCESS,
          merge_ctx.static_param_.scn_range_.start_scn_.convert_for_tx(900));
      ASSERT_EQ(OB_SUCCESS,
          merge_ctx.static_param_.scn_range_.end_scn_.convert_for_tx(980));
      merge_ctx.static_param_.concurrent_cnt_ = 1;
      merge_ctx.parallel_merge_ctx_.concurrent_cnt_ = 1;
      merge_ctx.uncommit_tx_info_ = OB_NEWx(ObMemUncommitTxInfo, &merge_ctx.mem_ctx_.get_allocator());
      ASSERT_NE(nullptr, merge_ctx.uncommit_tx_info_);
      ObUncommitTxDesc tx_desc(1001, IVF_SNAPSHOT_VERSION, ObUncommitTxDesc::COMMIT_VERSION);
      ASSERT_EQ(OB_SUCCESS, merge_ctx.uncommit_tx_info_[0].push_back(tx_desc));
      merge_ctx.uncommit_tx_info_[0].set_info_status_record_seq();
    }

    // Repeated close must preserve the metadata cached by the first close.
    ObSSTableMergeRes res;
    ASSERT_EQ(OB_SUCCESS, sstable_builder_.close(res));
    ASSERT_TRUE(res.ivf_meta_.is_valid());
    ASSERT_TRUE(res.ivf_meta_.is_empty());
    EXPECT_EQ(0, res.row_count_);
    EXPECT_EQ(0, res.data_blocks_cnt_);
    ObTabletCreateSSTableParam merged_param;
    ASSERT_EQ(OB_SUCCESS, merged_param.init_for_merge(merge_ctx, res, nullptr, 0));
    if (INC_MAJOR_MERGE == merge_type) {
      EXPECT_EQ(merge_ctx.static_param_.scn_range_.start_scn_,
          merged_param.table_key_.scn_range_.start_scn_);
      EXPECT_EQ(merge_ctx.static_param_.scn_range_.end_scn_,
          merged_param.table_key_.scn_range_.end_scn_);
      EXPECT_EQ(merge_ctx.static_param_.version_range_.base_version_,
          merged_param.min_merged_trans_version_);
      EXPECT_EQ(IVF_SNAPSHOT_VERSION, merged_param.max_merged_trans_version_);
      ASSERT_EQ(1, merged_param.uncommit_tx_info_.get_info_count());
      ObUncommitTxDesc tx_desc;
      ASSERT_EQ(OB_SUCCESS, merged_param.uncommit_tx_info_.get_uncommit_tx_desc(tx_desc, 0));
      EXPECT_TRUE(tx_desc.is_commit_version());
      EXPECT_EQ(1001, tx_desc.tx_id_);
      EXPECT_EQ(IVF_SNAPSHOT_VERSION, tx_desc.commit_version_);
    } else {
      EXPECT_EQ(merge_ctx.static_param_.version_range_.base_version_,
          merged_param.table_key_.version_range_.base_version_);
      EXPECT_EQ(IVF_SNAPSHOT_VERSION,
          merged_param.table_key_.version_range_.snapshot_version_);
    }
    ObIvfSSTable output;
    ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
        merged_param, allocator_, output));
    EXPECT_EQ(INC_MAJOR_MERGE == merge_type, output.is_inc_ivf_sstable());
    EXPECT_EQ(0, output.get_row_count());
    EXPECT_TRUE(output.get_ivf_meta().is_valid());
    EXPECT_TRUE(output.get_ivf_meta().is_empty());
    EXPECT_EQ(merged_param.table_key_, output.get_key());
    ObNewVersionRange output_version_range;
    ASSERT_EQ(OB_SUCCESS, output.get_async_index_version_range(output_version_range));
    EXPECT_EQ(merge_ctx.static_param_.version_range_.base_version_,
        output_version_range.base_version_);
    EXPECT_EQ(IVF_SNAPSHOT_VERSION, output_version_range.snapshot_version_);
  }
}

TEST_F(TestIVFSSTableWriter, zero_row_merge_preserves_supplied_ivf_metadata)
{
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  table_schema_.set_table_type(USER_INDEX);
  table_schema_.set_index_type(INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL);
  table_schema_.set_data_table_id(table_schema_.get_table_id());
  ObStorageSchema storage_schema;
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(
      allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));

  ObSSTableMergeRes res;
  ASSERT_EQ(OB_SUCCESS, sstable_builder_.close(res));
  ASSERT_EQ(0, res.row_count_);
  ASSERT_FALSE(res.ivf_meta_.is_valid());

  for (const ObMergeType merge_type : {MEDIUM_MERGE, INC_MAJOR_MERGE}) {
    SCOPED_TRACE(merge_type);
    ObTabletMergeDagParam merge_param(merge_type, ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID), -1);
    merge_param.merge_version_ = IVF_SNAPSHOT_VERSION;
    ObAsyncIndexMergeCtx merge_ctx(merge_param, allocator_);
    // Merge ctx owns and destroys its schema. Keep the later DDL schema independent.
    ObStorageSchema *merge_schema = OB_NEWx(ObStorageSchema, &allocator_);
    ASSERT_NE(nullptr, merge_schema);
    ASSERT_EQ(OB_SUCCESS, merge_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
    merge_ctx.static_param_.schema_ = merge_schema;
    merge_ctx.static_param_.is_ivf_index_ = true;
    merge_ctx.static_param_.version_range_.base_version_ =
        merge_type == INC_MAJOR_MERGE ? IVF_SNAPSHOT_VERSION / 2 : 0;
    merge_ctx.static_param_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
    merge_ctx.static_param_.rec_scn_.set_min();
    if (merge_type == INC_MAJOR_MERGE) {
      ASSERT_EQ(OB_SUCCESS,
          merge_ctx.static_param_.scn_range_.start_scn_.convert_for_tx(900));
      ASSERT_EQ(OB_SUCCESS,
          merge_ctx.static_param_.scn_range_.end_scn_.convert_for_tx(980));
      // Empty INC output still retains the selected transaction descriptor.
      merge_ctx.static_param_.concurrent_cnt_ = 1;
      merge_ctx.parallel_merge_ctx_.concurrent_cnt_ = 1;
      merge_ctx.uncommit_tx_info_ = OB_NEWx(ObMemUncommitTxInfo, &merge_ctx.mem_ctx_.get_allocator());
      ASSERT_NE(nullptr, merge_ctx.uncommit_tx_info_);
      ObUncommitTxDesc tx_desc(1001, 0);
      tx_desc.commit_version_ = IVF_SNAPSHOT_VERSION;
      ASSERT_EQ(OB_SUCCESS, merge_ctx.uncommit_tx_info_[0].push_back(tx_desc));
      merge_ctx.uncommit_tx_info_[0].set_info_status_record_seq();
    }
    for (int64_t meta_case = 0; meta_case < 3; ++meta_case) {
      SCOPED_TRACE(meta_case);
      res.ivf_meta_.reset();
      if (meta_case == 1) {
        res.ivf_meta_.set_empty();
      } else if (meta_case == 2) {
        fill_ivf_meta(2, res.ivf_meta_);
      }
      ObTabletCreateSSTableParam merged_param;
      ASSERT_EQ(OB_SUCCESS, merged_param.init_for_merge(merge_ctx, res, nullptr, 0));
      if (meta_case == 0) {
        EXPECT_FALSE(merged_param.ivf_meta_.is_valid());
        ObIvfSSTable invalid_output;
        EXPECT_EQ(OB_INVALID_ARGUMENT, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
            merged_param, allocator_, invalid_output));
      } else {
        EXPECT_EQ(res.ivf_meta_.is_empty(), merged_param.ivf_meta_.is_empty());
        EXPECT_EQ(res.ivf_meta_.dim_, merged_param.ivf_meta_.dim_);
        EXPECT_EQ(res.ivf_meta_.format_version_, merged_param.ivf_meta_.format_version_);
        if (INC_MAJOR_MERGE == merge_type) {
          EXPECT_EQ(merge_ctx.static_param_.scn_range_.start_scn_,
              merged_param.table_key_.scn_range_.start_scn_);
          EXPECT_EQ(merge_ctx.static_param_.scn_range_.end_scn_,
              merged_param.table_key_.scn_range_.end_scn_);
          EXPECT_EQ(merge_ctx.static_param_.version_range_.base_version_,
              merged_param.min_merged_trans_version_);
          EXPECT_EQ(IVF_SNAPSHOT_VERSION, merged_param.max_merged_trans_version_);
        } else {
          EXPECT_EQ(merge_ctx.static_param_.version_range_.base_version_,
              merged_param.table_key_.version_range_.base_version_);
          EXPECT_EQ(IVF_SNAPSHOT_VERSION,
              merged_param.table_key_.version_range_.snapshot_version_);
        }
        ObIvfSSTable empty_output;
        ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
            merged_param, allocator_, empty_output));
        EXPECT_EQ(merge_type == INC_MAJOR_MERGE, empty_output.is_inc_ivf_sstable());
        EXPECT_EQ(0, empty_output.get_row_count());
        EXPECT_EQ(res.ivf_meta_.is_empty(), empty_output.get_ivf_meta().is_empty());
        EXPECT_EQ(res.ivf_meta_.dim_, empty_output.get_ivf_meta().dim_);
        ObNewVersionRange output_version_range;
        ASSERT_EQ(OB_SUCCESS,
            empty_output.get_async_index_version_range(output_version_range));
        EXPECT_EQ(merge_ctx.static_param_.version_range_.base_version_,
            output_version_range.base_version_);
        EXPECT_EQ(IVF_SNAPSHOT_VERSION, output_version_range.snapshot_version_);
      }
    }
  }
  res.ivf_meta_.reset();

  ObITable::TableKey table_key;
  table_key.table_type_ = ObITable::TableType::INC_IVF_INDEX_SSTABLE;
  table_key.tablet_id_ = ObTabletID(IVF_TABLET_ID);
  table_key.version_range_.base_version_ = 0;
  table_key.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;

  ObTabletCreateSSTableParam param;
  const transaction::ObTransID trans_id(1001);
  const transaction::ObTxSEQ seq_no(2001, 0);
  ASSERT_EQ(OB_SUCCESS, param.init_for_ss_ddl(
      res, table_key, storage_schema, 10 /*create_schema_version_on_tablet*/,
      DIRECT_LOAD_INCREMENTAL_MAJOR, trans_id, seq_no));
  ASSERT_TRUE(param.table_key().is_inc_ivf_sstable());
  ASSERT_EQ(IVF_SNAPSHOT_VERSION, param.table_key().get_snapshot_version());
  ASSERT_EQ(0, param.row_count_);
  ASSERT_TRUE(param.ivf_meta_.is_valid());
  ASSERT_TRUE(param.ivf_meta_.is_empty());

  ObIvfSSTable inc_ivf_sstable;
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable<ObIvfSSTable>(
      param, allocator_, inc_ivf_sstable));
  ASSERT_TRUE(inc_ivf_sstable.is_inc_ivf_sstable());
  ASSERT_EQ(IVF_SNAPSHOT_VERSION, inc_ivf_sstable.get_snapshot_version());
  ASSERT_EQ(0, inc_ivf_sstable.get_data_macro_block_count());
  ASSERT_TRUE(inc_ivf_sstable.get_ivf_meta().is_empty());
}

TEST_F(TestIVFSSTableWriter, scan_macros_and_rows_after_write)
{
  const int64_t centroid_cnt = 4;
  const int64_t posting_cnt = 6;
  const int64_t raw_cnt = 5;
  const int64_t total_cnt = centroid_cnt + posting_cnt + raw_cnt;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_CENTROID, 0, centroid_cnt, 0));
  ASSERT_EQ(OB_SUCCESS,
      write_type_rows(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 100, posting_cnt, 1));
  ASSERT_EQ(OB_SUCCESS, write_type_rows(IVF_ROW_TYPE_RAW_VECTOR, 200, raw_cnt, 0));
  ASSERT_EQ(OB_SUCCESS, close_builder_and_init_ivf_sstable());
  ASSERT_EQ(OB_SUCCESS, object_cleaner_.mark_succeed());

  ASSERT_TRUE(sstable_.is_ivf_sstable());
  ASSERT_TRUE(sstable_.is_major_sstable());
  ASSERT_TRUE(sstable_.is_valid());
  ASSERT_EQ(ObStorageMetaValue::MetaType::IVF_SSTABLE, sstable_.get_meta_type());
  ASSERT_EQ(total_cnt, sstable_.get_row_count());
  ASSERT_GE(sstable_.get_data_macro_block_count(), 3);
  ASSERT_FALSE(sstable_.no_data_to_read());
  ASSERT_TRUE(sstable_.get_ivf_meta().is_valid());
  ASSERT_EQ(8, sstable_.get_ivf_meta().dim_);

  ObTableReadInfo read_info;
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));

  ObDatumRange whole_range;
  whole_range.set_whole_range();
  ObIMacroBlockIterator *macro_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable_.scan_macro_block(
      whole_range, read_info, allocator_, macro_iter,
      false /*is_reverse_scan*/, false /*need_record_micro_info*/, true /*need_scan_sec_meta*/));
  ASSERT_NE(nullptr, macro_iter);

  int64_t macro_cnt = 0;
  int64_t macro_row_cnt = 0;
  ObMacroBlockDesc macro_desc;
  ObDataMacroBlockMeta macro_meta;
  macro_desc.macro_meta_ = &macro_meta;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ret = macro_iter->get_next_macro_block(macro_desc);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(macro_desc.macro_block_id_.is_valid());
    ASSERT_GT(macro_desc.row_count_, 0);
    ASSERT_TRUE(macro_desc.range_.is_valid());
    const ObDatumRowkey &endkey = macro_desc.range_.get_end_key();
    ASSERT_GE(endkey.get_datum_cnt(), 1);
    const int64_t type = endkey.get_datum(0).get_int();
    ASSERT_TRUE(IVF_ROW_TYPE_CENTROID == type
        || IVF_ROW_TYPE_RAW_VECTOR == type
        || IVF_ROW_TYPE_LOW_PRECISION_VECTOR == type);
    ++macro_cnt;
    macro_row_cnt += macro_desc.row_count_;
  }
  if (nullptr != macro_iter) {
    macro_iter->~ObIMacroBlockIterator();
    allocator_.free(macro_iter);
  }
  ASSERT_GE(macro_cnt, 3);
  ASSERT_EQ(total_cnt, macro_row_cnt);
  ASSERT_EQ(sstable_.get_row_count(), macro_row_cnt);

  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
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
  ASSERT_EQ(OB_SUCCESS, context.micro_block_handle_mgr_.init(
      false, context.tablet_id_, context.table_store_stat_, context.table_scan_stat_, context.query_flag_));
  context.is_inited_ = true;

  ObStoreRowIterator *row_iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, sstable_.scan(iter_param, context, whole_range, row_iter));
  ASSERT_NE(nullptr, row_iter);
  ASSERT_FALSE(context.query_flag_.is_use_block_cache());

  int64_t scanned = 0;
  int64_t last_type = -1;
  const ObDatumRow *cur_row = nullptr;
  ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ret = row_iter->get_next_row(cur_row);
    if (OB_ITER_END == ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_NE(nullptr, cur_row);
    ASSERT_GE(cur_row->count_, 3);
    const int64_t type = cur_row->storage_datums_[0].get_int();
    ASSERT_TRUE(IVF_ROW_TYPE_CENTROID == type
        || IVF_ROW_TYPE_RAW_VECTOR == type
        || IVF_ROW_TYPE_LOW_PRECISION_VECTOR == type);
    ASSERT_GE(type, last_type);
    last_type = type;
    ++scanned;
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(total_cnt, scanned);
  row_iter->~ObStoreRowIterator();
}

// The fixture uses the real tenant temporary-file manager from MockTenantModuleEnv.
// Each concurrent reader owns its cursor and buffers; the store is no longer written.
// Real storage A/B coverage. Input construction is deliberately outside the
// measured merge interval; both paths consume the same immutable SSTables.
class TestIVFInsertionStorage : public TestIVFMergeInput
{
protected:
  static constexpr int64_t DIM = 128;
  static constexpr int64_t REPLAY_BYTES = 64L * 1024 * 1024;
  struct Record
  {
    int64_t type_, cid_;
    uint64_t vid_;
    std::string payload_;
    auto key() const { return std::make_tuple(type_, cid_, vid_); }
    bool operator==(const Record &other) const
    { return key() == other.key() && payload_ == other.payload_; }
  };
  struct SourceMacro
  {
    MacroBlockId id_;
    int64_t type_;
  };
  struct Result
  {
    bool eligible_ = false;
    int64_t elapsed_us_ = 0, readback_us_ = 0, close_us_ = 0, verify_us_ = 0, compare_us_ = 0;
    int64_t replay_reserved_ = 0, replay_hold_ = 0, spill_bytes_ = 0;
    int64_t run_arena_bytes_ = 0, rss_before_readback_ = 0, rss_after_validation_ = 0;
    int64_t capture_rows_ = 0, old_capture_rows_ = 0, old_vids_ = 0;
    int64_t reused_ids_[IVF_ROW_TYPE_MAX] = {};
    struct { int64_t removed_base_raw_rows_ = 0; } stats_;
    ObIvfSSTableMeta meta_;
    std::vector<Record> rows_;
  };
  class Data : public ObIVFVectorStore
  {
  public:
    int64_t n_ = 10000, k_ = 32, delta_ = 1000;
    bool spread_ = false, empty_leaf_ = false;
    bool is_valid() const override { return n_ > 0 && k_ > 1; }
    int64_t get_vector_count() const override { return n_; }
    int64_t get_dimension() const override { return DIM; }
    uint64_t base_vid(int64_t i) const { return 2 * (i + 1); }
    uint64_t delta_vid(int64_t i) const { return 2 * n_ + 2 * i + 1; }
    float center(int64_t cid, int64_t d) const
    {
      return d == 0 ? (empty_leaf_ && cid == k_ - 1 ? 1000000.125F : cid * 8.0F + 0.125F)
          : static_cast<float>(d + 1) / 128.0F;
    }
    int64_t primary(int64_t i, bool delta) const
    { return delta ? (empty_leaf_ ? k_ - 1 : spread_ ? i % k_ : k_ / 2) : i % (k_ - empty_leaf_); }
    void vector(int64_t i, bool delta, float *out) const
    {
      const uint64_t vid = delta ? delta_vid(i) : base_vid(i);
      const int64_t cid = primary(i, delta);
      for (int64_t d = 0; d < DIM; ++d) {
        uint64_t bits = vid * UINT64_C(0x9e3779b97f4a7c15) + d + 20261025;
        bits = (bits ^ (bits >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        bits ^= bits >> 27;
        // Keep coordinate 0 away from exact midpoint ties, including after
        // FP32 rounding at the largest regular center used by the benchmark.
        const int64_t noise = d == 0 ? ((bits & 1) ? 1 : -1) * (128 + static_cast<int64_t>(bits % 373))
            : static_cast<int64_t>(bits % 1001) - 500;
        out[d] = center(cid, d) + static_cast<float>(noise) / 16384.0F;
      }
    }
    // Independent exact L2/SOAR reference. Centers differ only in coordinate 0,
    // so shared residual terms avoid O(N*K*DIM) work when constructing inputs.
    void reference(const float *x, int64_t &primary_cid, int64_t &secondary_cid) const
    {
      double common = 0;
      for (int64_t d = 1; d < DIM; ++d) {
        const double r = static_cast<double>(x[d]) - center(0, d);
        common += r * r;
      }
      double best = HUGE_VAL;
      primary_cid = secondary_cid = -1;
      for (int64_t c = 0; c < k_; ++c) {
        const double r = static_cast<double>(x[0]) - center(c, 0);
        const double distance = common + r * r;
        if (distance < best) { best = distance; primary_cid = c; }
      }
      if (best > 1e-16) {
        const double r1 = static_cast<double>(x[0]) - center(primary_cid, 0);
        double loss_min = HUGE_VAL;
        for (int64_t c = 0; c < k_; ++c) {
          if (c == primary_cid) { continue; }
          const double r2 = static_cast<double>(x[0]) - center(c, 0);
          const double dot = common + r1 * r2;
          const double loss = common + r2 * r2 + dot * dot / best;
          if (loss < loss_min) { loss_min = loss; secondary_cid = c; }
        }
      }
    }
    int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override
    {
      vids.reuse();
      int ret = vids.reserve(n_);
      for (int64_t i = 0; OB_SUCC(ret) && i < n_; ++i) { ret = vids.push_back(base_vid(i)); }
      return ret;
    }
    int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids, int64_t begin,
        int64_t end, ObIArray<float> &values) const override
    {
      int ret = values.prepare_allocate((end - begin) * DIM);
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        const uint64_t vid = vids.at(i);
        if ((vid & 1) != 0 || vid == 0 || vid / 2 > static_cast<uint64_t>(n_)) {
          ret = OB_INVALID_ARGUMENT;
        } else { vector(vid / 2 - 1, false, &values.at((i - begin) * DIM)); }
      }
      return ret;
    }
  } data_;
  class DeltaStore : public ObIVFVectorStore
  {
  public:
    DeltaStore(const Data &data, bool duplicate) : data_(data), duplicate_(duplicate) {}
    bool is_valid() const override { return data_.delta_ > 0; }
    int64_t get_vector_count() const override { return data_.delta_; }
    int64_t get_dimension() const override { return DIM; }
    int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override
    {
      int ret = OB_SUCCESS;
      vids.reuse();
      for (int64_t i = 0; OB_SUCC(ret) && i < data_.delta_; ++i) {
        ret = vids.push_back(duplicate_ && i == 0 ? data_.base_vid(0) : data_.delta_vid(i));
      }
      return ret;
    }
    int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids, int64_t begin,
        int64_t end, ObIArray<float> &values) const override
    {
      int ret = values.prepare_allocate((end - begin) * DIM);
      for (int64_t j = begin; OB_SUCC(ret) && j < end; ++j) {
        const uint64_t vid = vids.at(j);
        int64_t i = -1;
        if (duplicate_ && vid == data_.base_vid(0)) { i = 0; }
        else if ((vid & 1) != 0 && vid >= data_.delta_vid(0)) { i = (vid - data_.delta_vid(0)) / 2; }
        if (i < 0 || i >= data_.delta_) { ret = OB_INVALID_ARGUMENT; }
        else { data_.vector(i, true, &values.at((j - begin) * DIM)); }
      }
      return ret;
    }
    const Data &data_;
    bool duplicate_;
  };

  void SetUp() override
  {
    TestIVFMergeInput::SetUp();
    omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
    ASSERT_TRUE(config.is_valid());
    saved_replay_bytes_ = config->_vector_index_ivf_build_cache_size;
    ASSERT_TRUE(config->_vector_index_ivf_build_cache_size.set_value("64M"));
  }
  void TearDown() override
  {
    delta_.reset(); base_.reset();
    base_rows_.clear(); source_macros_.clear();
    omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
    if (config.is_valid()) {
      char value[64];
      snprintf(value, sizeof(value), "%ld", saved_replay_bytes_);
      EXPECT_TRUE(config->_vector_index_ivf_build_cache_size.set_value(value));
    }
    TestIVFMergeInput::TearDown();
  }
  int reset_output(ObRowStoreType type = CS_ENCODING_ROW_STORE,
      int64_t snapshot = IVF_SNAPSHOT_VERSION, ObMergeType merge_type = MAJOR_MERGE)
  {
    int ret = OB_SUCCESS;
    sstable_builder_.reset(); object_cleaner_.reset();
    whole_desc_.reset(); table_schema_.reset();
    prepare_ivf_schema(table_schema_);
    table_schema_.set_row_store_type(type);
    table_schema_.set_micro_index_clustered(false);
    SCN end_scn;
    if (OB_FAIL(end_scn.convert_for_tx(snapshot))) {
    } else if (OB_FAIL(table_schema_.set_compress_func_name("none"))) {
    } else if (OB_FAIL(whole_desc_.init(false, table_schema_, ObLSID(IVF_LS_ID),
        ObTabletID(IVF_TABLET_ID), merge_type, snapshot, DATA_CURRENT_VERSION,
        false, 0, 1, SCN::min_scn(), end_scn))) {
    } else {
      whole_desc_.get_desc().static_desc_->schema_version_ = 10;
      whole_desc_.get_desc().sstable_index_builder_ = &sstable_builder_;
      ret = sstable_builder_.init(whole_desc_.get_desc(), ObSSTableIndexBuilder::DISABLE);
      if (OB_SUCC(ret)) { sstable_builder_.set_is_async_index(true); }
    }
    return ret;
  }
  static int64_t resident_bytes()
  {
    long pages = 0, resident = 0;
    FILE *file = fopen("/proc/self/statm", "r");
    if (file == nullptr) { return -1; }
    const int fields = fscanf(file, "%ld %ld", &pages, &resident);
    fclose(file);
    return fields == 2 ? resident * sysconf(_SC_PAGESIZE) : -1;
  }
  int seal_table(ObIvfSSTable &table, ObArenaAllocator &allocator, bool delta,
      const ObIvfSSTableMeta &meta, ObSSTableMergeRes &res,
      int64_t snapshot = IVF_SNAPSHOT_VERSION)
  {
    int ret = OB_SUCCESS;
    ObMacroSeqWindow index_window;
    ObTabletCreateSSTableParam param;
    const ObPreWarmerParam warm(PRE_WARM_TYPE_NONE);
    if (OB_FAIL(ObVersionLocalMacroSeq::build_window(1, index_window))) {
    } else if (OB_FAIL(sstable_builder_.close_with_macro_seq(res, index_window.start_,
        OB_DEFAULT_MACRO_BLOCK_SIZE, 0, warm, index_window.end_))) {
    } else if (OB_FAIL(fill_create_param(res, param))) {
    } else {
      param.ivf_meta_ = meta;
      param.table_key_.version_range_.snapshot_version_ = snapshot;
      if (delta || inc_source_) {
        param.table_key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
        param.table_key_.scn_range_.start_scn_.convert_for_tx(
            delta && snapshot == IVF_SNAPSHOT_VERSION ? IVF_SNAPSHOT_VERSION : IVF_SNAPSHOT_VERSION - 10);
        param.table_key_.scn_range_.end_scn_.convert_for_tx(
            delta || snapshot > IVF_SNAPSHOT_VERSION ? IVF_SNAPSHOT_VERSION + 10 : IVF_SNAPSHOT_VERSION);
        ObUncommitTxDesc tx(20261025, IVF_SNAPSHOT_VERSION, ObUncommitTxDesc::COMMIT_VERSION);
        ret = param.uncommit_tx_info_.push_back(tx);
        param.uncommit_tx_info_.set_info_status_record_seq();
      }
      if (OB_SUCC(ret)) { ret = table.init(param, &allocator); }
    }
    return ret;
  }
  int read_rows(ObIvfSSTable &table, std::vector<Record> &rows)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator arena("IVFABRead");
    ObTableReadInfo info;
    ObTableIterParam param;
    ObTableAccessContext context;
    ObStoreCtx store;
    if (OB_FAIL(store.init_for_read(ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID),
        INT64_MAX, -1, SCN::max_scn()))) { return ret; }
    ObArray<ObColDesc> columns;
    // Query columns exclude the two physical MVCC columns. ObTableReadInfo
    // derives their storage offsets itself, as prepare_read_info() does.
    if (OB_FAIL(table_schema_.get_column_ids(columns))
        || OB_FAIL(info.init(arena, table_schema_.get_column_count(), table_schema_.get_rowkey_column_num(),
            false, columns, nullptr))) { return ret; }
    param.table_id_ = IVF_TABLE_ID; param.tablet_id_ = IVF_TABLET_ID; param.read_info_ = &info;
    context.query_flag_.set_not_use_row_cache(); context.query_flag_.set_not_use_block_cache();
    context.query_flag_.scan_order_ = ObQueryFlag::Forward;
    context.store_ctx_ = &store; context.ls_id_ = IVF_LS_ID; context.tablet_id_ = IVF_TABLET_ID;
    context.allocator_ = &arena; context.stmt_allocator_ = &arena;
    context.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
    if (OB_FAIL(context.micro_block_handle_mgr_.init(false, context.tablet_id_,
        context.table_store_stat_, context.table_scan_stat_, context.query_flag_))) { return ret; }
    context.is_inited_ = true;
    ObDatumRange range;
    range.set_whole_range();
    ObStoreRowIterator *iter = nullptr;
    rows.clear();
    if (OB_FAIL(table.scan(param, context, range, iter))) { return ret; }
    const ObDatumRow *row = nullptr;
    while (OB_SUCC(ret) && OB_SUCCESS == (ret = iter->get_next_row(row))) {
      if (row == nullptr || row->get_column_count() < 4) { ret = OB_INVALID_DATA; }
      else {
        const ObString payload = row->storage_datums_[3].get_string();
        Record value{row->storage_datums_[0].get_int(), row->storage_datums_[1].get_int(),
            row->storage_datums_[2].get_uint(), std::string(payload.ptr(), payload.length())};
        if (!rows.empty() && rows.back().key() >= value.key()) { ret = OB_ROWKEY_ORDER_ERROR; }
        else { rows.push_back(std::move(value)); }
      }
    }
    if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
    iter->~ObStoreRowIterator();
    return ret;
  }
  void build_inputs(bool duplicate_delta = false, ObRowStoreType row_store_type = CS_ENCODING_ROW_STORE,
      bool grouped = false, bool distinct_delta = false)
  {
    ASSERT_EQ(OB_SUCCESS, reset_output(row_store_type));
    base_.reset(new ObIvfSSTable()); delta_.reset(new ObIvfSSTable());
    ObArray<float> centers;
    ObArray<ObIVFVectorVID> all;
    std::vector<std::vector<ObIVFVectorVID>> primary(data_.k_), secondary(data_.k_);
    ASSERT_EQ(OB_SUCCESS, centers.prepare_allocate(data_.k_ * DIM));
    for (int64_t c = 0; c < data_.k_; ++c) {
      for (int64_t d = 0; d < DIM; ++d) { centers.at(c * DIM + d) = data_.center(c, d); }
    }
    float x[DIM];
    int64_t postings = 0;
    for (int64_t i = 0; i < data_.n_; ++i) {
      int64_t p = -1, s = -1;
      data_.vector(i, false, x); data_.reference(x, p, s);
      ASSERT_EQ(data_.primary(i, false), p);
      primary[p].push_back(data_.base_vid(i)); ++postings;
      if (s >= 0) { secondary[s].push_back(data_.base_vid(i)); ++postings; }
    }
    ObIVFVectorReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.init(data_));
    ASSERT_EQ(OB_SUCCESS, data_.get_all_vids(all));
    ObMacroSeqWindow window;
    ASSERT_EQ(OB_SUCCESS, ObVersionLocalMacroSeq::build_window(0, window));
    ObIVFMergeSSTableWriter writer(allocator_);
    ASSERT_EQ(OB_SUCCESS, writer.init(whole_desc_.get_desc(), sstable_builder_, window,
        IVF_SNAPSHOT_VERSION, DIM, VIDA_L2));
    ObIvfCentroidLayout layout(allocator_);
    if (grouped) {
      ObArray<float> parents;
      const int64_t half = data_.k_ / 2;
      for (int64_t p = 0; p < 2; ++p) {
        const int64_t first = p == 0 ? 0 : half, end = p == 0 ? half : data_.k_;
        for (int64_t d = 0; d < DIM; ++d) {
          double sum = 0;
          for (int64_t c = first; c < end; ++c) { sum += data_.center(c, d); }
          ASSERT_EQ(OB_SUCCESS, parents.push_back(static_cast<float>(sum / (end - first))));
        }
      }
      ASSERT_EQ(OB_SUCCESS, layout.begin_grouped(parents, data_.k_, DIM));
      for (int64_t c = 0; c < data_.k_; ++c) {
        const int64_t parent = c < half ? 0 : 1;
        ASSERT_EQ(OB_SUCCESS, layout.set_parent(c, parent, false));
        if (c % 4 == 0) { ASSERT_EQ(OB_SUCCESS, layout.set_parent(c, 1 - parent, true)); }
      }
      ASSERT_EQ(OB_SUCCESS, layout.finish_grouped());
      ASSERT_EQ(OB_SUCCESS, writer.set_centroid_layout(layout));
    }
    int64_t tasks = 0;
    ASSERT_EQ(OB_SUCCESS, writer.prepare_quantization(centers, data_.k_, tasks));
    class EmptyLeaf final : public ObIVFEncodedPostingSource {
      int scan_postings(int64_t, ObIVFEncodedPostingConsumer &) const override { return OB_SUCCESS; }
    } empty;
    for (int64_t t = 0; t < tasks; ++t) {
      const int64_t width = data_.k_ / tasks, extra = data_.k_ % tasks;
      const int64_t begin = t * width + MIN(t, extra), end = begin + width + (t < extra);
      ObIVFMergeQuantizer *range = nullptr;
      ASSERT_EQ(OB_SUCCESS, writer.open_quantization_range(begin, end, t, allocator_, range));
      for (int64_t c = begin; c < end; ++c) {
        ObArray<ObIVFVectorVID> p, s;
        for (auto vid : primary[c]) { ASSERT_EQ(OB_SUCCESS, p.push_back(vid)); }
        for (auto vid : secondary[c]) { ASSERT_EQ(OB_SUCCESS, s.push_back(vid)); }
        const int rc = p.empty() ? range->quantize_cluster_with_reuse(reader, centers, data_.k_, c,
            p, &s, empty, c, 0) : range->quantize_cluster(reader, centers, data_.k_, c, p, &s);
        ASSERT_EQ(OB_SUCCESS, rc);
      }
      ASSERT_EQ(OB_SUCCESS, writer.close_quantization_range(range, true));
    }
    ASSERT_EQ(OB_SUCCESS, writer.finish_quantization(data_.n_, data_.k_, postings));
    ASSERT_EQ(OB_SUCCESS, writer.write_raw_vectors(reader, all, 0, all.count(), 0));
    ASSERT_EQ(OB_SUCCESS, writer.finish_raw_write(data_.n_));
    ObSSTableMergeRes base_res;
    ASSERT_EQ(OB_SUCCESS, seal_table(*base_, allocator_, false, writer.get_ivf_meta(), base_res));
    ASSERT_EQ(OB_SUCCESS, read_rows(*base_, base_rows_));
    ObTableReadInfo info;
    ASSERT_EQ(OB_SUCCESS, prepare_read_info(info));
    ObDatumRange range;
    range.set_whole_range();
    ObIMacroBlockIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, base_->scan_macro_block(range, info, allocator_, iter, false, true, true));
    ObDataMacroBlockMeta meta;
    ObMacroBlockDesc macro;
    macro.macro_meta_ = &meta;
    int rc = OB_SUCCESS;
    source_macros_.clear();
    while (OB_SUCCESS == (rc = iter->get_next_macro_block(macro))) {
      const int64_t type = macro.range_.end_key_.datums_[0].get_int();
      ASSERT_GE(type, 0);
      ASSERT_LT(type, IVF_ROW_TYPE_MAX);
      source_macros_.push_back({macro.macro_block_id_, type});
    }
    ASSERT_EQ(OB_ITER_END, rc);
    iter->~ObIMacroBlockIterator(); allocator_.free(iter);

    ASSERT_EQ(OB_SUCCESS, reset_output(row_store_type));
    if (data_.delta_ == 0) {
      ObStorageSchema schema;
      ObTabletCreateSSTableParam param;
      ASSERT_EQ(OB_SUCCESS, schema.init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
      ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(ObTabletID(IVF_TABLET_ID), schema,
          IVF_SNAPSHOT_VERSION + 10, -1, false, false));
      param.table_key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
      param.table_key_.version_range_.base_version_ = IVF_SNAPSHOT_VERSION;
      param.table_key_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION + 10;
      param.ivf_meta_.set_empty();
      ObUncommitTxDesc tx(20261025, IVF_SNAPSHOT_VERSION + 10, ObUncommitTxDesc::COMMIT_VERSION);
      ASSERT_EQ(OB_SUCCESS, param.uncommit_tx_info_.push_back(tx));
      param.uncommit_tx_info_.set_info_status_record_seq();
      ASSERT_EQ(OB_SUCCESS, delta_->init(param, &allocator_));
    } else {
      // A real refresh segment has centers/postings too. The existing prior
      // capture rejects fabricated raw-only inputs with centroid_count == 0.
      DeltaStore delta_store(data_, duplicate_delta);
      ObIVFVectorReader delta_reader;
      ObArray<ObIVFVectorVID> delta_vids;
      ObArray<float> delta_center;
      ASSERT_EQ(OB_SUCCESS, delta_reader.init(delta_store));
      ASSERT_EQ(OB_SUCCESS, delta_store.get_all_vids(delta_vids));
      for (int64_t d = 0; d < DIM; ++d) {
        ASSERT_EQ(OB_SUCCESS, delta_center.push_back(data_.center(data_.primary(0, true), d) + (distinct_delta && d == 0 ? 0.5F : 0.0F)));
      }
      ObIVFMergeSSTableWriter delta_writer(allocator_);
      ASSERT_EQ(OB_SUCCESS, delta_writer.init(whole_desc_.get_desc(), sstable_builder_, window,
          IVF_SNAPSHOT_VERSION, DIM, VIDA_L2));
      ASSERT_EQ(OB_SUCCESS, delta_writer.quantize_cluster(delta_reader, delta_center,
          1, 0, delta_vids, nullptr));
      ASSERT_EQ(OB_SUCCESS, delta_writer.finish_quantization(data_.delta_, 1, data_.delta_));
      ASSERT_EQ(OB_SUCCESS, delta_writer.write_raw_vectors(delta_reader, delta_vids, 0, delta_vids.count(), 0));
      ASSERT_EQ(OB_SUCCESS, delta_writer.finish_raw_write(data_.delta_));
      ObSSTableMergeRes delta_res;
      ASSERT_EQ(OB_SUCCESS, seal_table(*delta_, allocator_, true, delta_writer.get_ivf_meta(), delta_res));
    }
  }

  int configure_context(ObAsyncIndexMergeCtx &ctx, ObIVFMergeInputAdapter &input,
      ObIAllocator &arena, const std::vector<uint64_t> &deleted)
  {
    int ret = OB_SUCCESS;
    ctx.static_param_.is_ivf_index_ = true;
    ctx.static_param_.version_range_.base_version_ = 0;
    ctx.static_param_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION + 10;
    ctx.static_param_.concurrent_cnt_ = 1;
    ObTableHandleV2 base_handle, delta_handle;
    ObRoaringBitmap *bitmap = OB_NEWx(ObRoaringBitmap, &arena, &arena);
    if (bitmap == nullptr) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    else if (OB_FAIL(ctx.read_info_.init(arena, table_schema_.get_column_count(),
        table_schema_.get_rowkey_column_num(), false, whole_desc_.get_desc().get_full_stored_col_descs()))) {
    } else if (OB_FAIL(base_handle.set_sstable_with_tablet(base_.get()))
        || OB_FAIL(delta_handle.set_sstable_with_tablet(delta_.get()))
        || OB_FAIL(ctx.static_param_.tables_handle_.add_table(base_handle))
        || OB_FAIL(ctx.static_param_.tables_handle_.add_table(delta_handle))
        || OB_FAIL(ctx.deletion_bitmap_handle_.init_from_bitmap(arena, bitmap))) {
    }
    for (auto vid : deleted) { if (OB_SUCC(ret)) { ret = bitmap->value_add(vid); } }
    input.index_table_id_ = IVF_TABLE_ID; input.index_tablet_id_ = ObTabletID(IVF_TABLET_ID);
    const ObHcaTrainParam training;
    input.vector_dim_ = DIM;
    input.max_sample_count_ = training.max_local_k_ * training.samples_per_cluster_;
    input.dist_type_ = VIDA_L2;
    if (OB_SUCC(ret) && OB_FAIL(input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION))) {
    } else if (OB_SUCC(ret)) { ret = input.prepare_source_(ctx); }
    return ret;
  }

  int run_merge(Result &result, const std::vector<uint64_t> &deleted = {},
      ObRowStoreType output_type = CS_ENCODING_ROW_STORE, ObMergeType merge_type = MAJOR_MERGE,
      ObIVFProcessType selected = ObIVFProcessType::INSERTION, int64_t parent_threshold = 0)
  {
    int ret = reset_output(output_type, IVF_SNAPSHOT_VERSION + 10, merge_type);
    if (OB_FAIL(ret)) { return ret; }
    ObArenaAllocator arena("IVFABRun");
    ObTabletMergeDagParam dag_param(merge_type, ObLSID(IVF_LS_ID), ObTabletID(IVF_TABLET_ID), -1);
    dag_param.merge_version_ = IVF_SNAPSHOT_VERSION + 10;
    ObAsyncIndexMergeCtx ctx(dag_param, arena);
    ObIVFMergeInputAdapter input(arena);
    ObMacroSeqWindow window;
    if (OB_FAIL(ObVersionLocalMacroSeq::build_window(0, window))) { return ret; }
    const int64_t started = ObTimeUtility::current_time();
    if (OB_FAIL(configure_context(ctx, input, arena, deleted))) { return ret; }
    if (is_inc_major_merge(merge_type)) { ctx.deletion_bitmap_handle_.reset(); }
    result.eligible_ = true;
    ObIVFMergeCaptureCtx capture;
    if (OB_FAIL(capture.init(ctx, input, false))) { return ret; }
    for (int64_t t = 0; OB_SUCC(ret) && t < capture.get_task_count(); ++t) { ret = capture.scan(t); }
    if (OB_SUCC(ret)) { ret = capture.finish(); }
    if (OB_FAIL(ret)) { return ret; }
    result.capture_rows_ = input.locators_.count();
    for (const auto &locator : input.locators_) { result.old_capture_rows_ += locator.table_idx_ == 0; }
    result.stats_.removed_base_raw_rows_ = data_.n_ - result.old_capture_rows_;
    result.replay_reserved_ = input.replay_memory_reservation_.get_reserved_bytes();
    for (int64_t i = 0; i < input.replay_inputs_.count(); ++i) {
      result.replay_hold_ += input.replay_inputs_.at(i)->get_mem_hold();
      result.spill_bytes_ += input.replay_inputs_.at(i)->get_file_size();
    }
    if (input.is_empty()) {
      ObIvfSSTableMeta empty;
      empty.set_empty();
      ret = sstable_builder_.set_ivf_meta(empty);
    } else {
      // This fixture exercises insertion itself, including a single surviving
      // vector. Heuristic strategy selection is covered separately.
      EventItem force, reset;
      force.error_code_ = OB_ERR_UNEXPECTED; force.occur_ = 1;
      const char *event = selected == ObIVFProcessType::CONCATENATION
          ? "ERRSIM_IVF_FORCE_CONCAT" : "ERRSIM_IVF_FORCE_INSERTION";
      if (OB_FAIL(EventTable::set_event(event, force))) { return ret; }
      ret = input.capture_prior_(ctx);
      const int reset_ret = EventTable::set_event(event, reset);
      if (OB_FAIL(ret)) { return ret; }
      if (reset_ret != OB_SUCCESS) { return reset_ret; }
      ObIVFDDLHcaAdapter algorithm;
      ObIVFMergeSSTableWriter output(arena);
      ObIVFMergeProcessor processor;
      ObIVFInsertionPlanner insertion;
      ObIVFConcatPlanner concat;
      ObIVFMergePlanner &planner = selected == ObIVFProcessType::CONCATENATION
          ? static_cast<ObIVFMergePlanner &>(concat) : static_cast<ObIVFMergePlanner &>(insertion);
      if (OB_FAIL(algorithm.init(ObIVFHcaAlgorithmParam()))
          || OB_FAIL(output.init(whole_desc_.get_desc(), sstable_builder_, window,
              IVF_SNAPSHOT_VERSION + 10, DIM, VIDA_L2))) { return ret; }
      algorithm.prepare_insertion_source(input, input.get_prior(), VIDA_L2, base_->get_ivf_meta().leaf_cid_base());
      if (OB_FAIL(processor.init(arena, input.get_merge_input(), algorithm, output, selected, &input.get_prior()))
          || OB_FAIL(processor.configure_ddl_runtime(algorithm, 1))
          || OB_FAIL(selected == ObIVFProcessType::CONCATENATION
              ? concat.init(arena, processor) : insertion.init(arena, processor))) { return ret; }
      if (parent_threshold > 0 && OB_FAIL(ObIVFMergeTestPeer::set_parent_build_threshold(processor, parent_threshold))) {
        return ret;
      }
      const ObIVFTaskPlan *plan = nullptr;
      ret = planner.start(plan);
      for (int64_t step = 0; OB_SUCC(ret) && plan != nullptr && step < 128; ++step) {
        for (int64_t t = 0; OB_SUCC(ret) && t < plan->get_task_count(); ++t) { ret = planner.process_task(*plan, t); }
        const ObIVFTaskPlan *next = nullptr;
        if (OB_SUCC(ret)) { ret = planner.finish_task_plan(*plan, next); }
        plan = next;
      }
      if (OB_SUCC(ret) && (plan != nullptr || !output.is_finished())) { ret = OB_STATE_NOT_MATCH; }
    }
    if (OB_FAIL(ret)) { return ret; }
    result.old_vids_ = result.old_capture_rows_;
    result.meta_ = sstable_builder_.get_ivf_meta();
    ObSSTableMergeRes res;
    ObIvfSSTable table;
    const int64_t close_started = ObTimeUtility::current_time();
    ret = seal_table(table, arena, is_inc_major_merge(merge_type), result.meta_, res, IVF_SNAPSHOT_VERSION + 10);
    result.close_us_ = ObTimeUtility::current_time() - close_started;
    result.elapsed_us_ = ObTimeUtility::current_time() - started;
    if (OB_FAIL(ret)) { return ret; }
    result.run_arena_bytes_ = arena.total();
    result.rss_before_readback_ = resident_bytes();
    for (int64_t i = 0; i < res.data_block_ids_.count(); ++i) {
      for (const auto &source : source_macros_) {
        if (res.data_block_ids_.at(i) == source.id_) {
          ++result.reused_ids_[source.type_];
        }
      }
    }
    const int64_t read_started = ObTimeUtility::current_time();
    ret = read_rows(table, result.rows_);
    result.readback_us_ = ObTimeUtility::current_time() - read_started;
    return ret;
  }

  void verify_concat(const Result &result, const std::vector<Record> &delta_rows,
      const ObIvfSSTableMeta &delta_meta, const std::vector<uint64_t> &deleted)
  {
    using Key = std::pair<std::string, uint64_t>;
    std::map<Key, std::string> expected, actual;
    std::map<uint64_t, std::string> expected_raw, actual_raw;
    std::set<std::string> expected_centers, actual_centers;
    const auto collect = [&](const std::vector<Record> &rows, const ObIvfSSTableMeta &meta,
        bool source, std::map<Key, std::string> &postings, std::map<uint64_t, std::string> &raw,
        std::set<std::string> &centers) {
      std::map<int64_t, std::string> names;
      std::map<int64_t, uint32_t> stored_counts;
      std::map<int64_t, int64_t> counts;
      for (const auto &row : rows) {
        if (row.type_ != IVF_ROW_TYPE_CENTROID) { continue; }
        int64_t cid = -1, group = -1;
        ASSERT_EQ(OB_SUCCESS, meta.decode_centroid_key(row.cid_, row.vid_, cid, group));
        if (meta.is_hierarchical() && group == 0) { continue; }
        const float *center = nullptr; uint32_t children = 0;
        ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(
            ObString(row.payload_.size(), row.payload_.data()), DIM, center, children));
        std::string name(reinterpret_cast<const char *>(center), DIM * sizeof(float));
        if (names.count(cid)) { EXPECT_EQ(names.at(cid), name); EXPECT_EQ(stored_counts.at(cid), children); }
        else { names[cid] = name; stored_counts[cid] = children; }
        centers.insert(name);
      }
      for (const auto &row : rows) {
        if (row.type_ == IVF_ROW_TYPE_CENTROID) { continue; }
        if (source && std::find(deleted.begin(), deleted.end(), row.vid_) != deleted.end()) { continue; }
        if (row.type_ == IVF_ROW_TYPE_RAW_VECTOR) { EXPECT_TRUE(raw.emplace(row.vid_, row.payload_).second); }
        else {
          ASSERT_EQ(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, row.type_);
          ASSERT_EQ(1, names.count(row.cid_));
          const Key key(names.at(row.cid_), row.vid_);
          const auto found = postings.find(key);
          if (found != postings.end()) { EXPECT_TRUE(source); EXPECT_EQ(found->second, row.payload_); }
          else { postings.emplace(key, row.payload_); }
          ++counts[row.cid_];
        }
      }
      if (!source) { for (const auto &entry : stored_counts) { EXPECT_EQ(entry.second, counts[entry.first]); } }
    };
    ASSERT_NO_FATAL_FAILURE(collect(base_rows_, base_->get_ivf_meta(), true, expected, expected_raw, expected_centers));
    ASSERT_NO_FATAL_FAILURE(collect(delta_rows, delta_meta, true, expected, expected_raw, expected_centers));
    ASSERT_NO_FATAL_FAILURE(collect(result.rows_, result.meta_, false, actual, actual_raw, actual_centers));
    EXPECT_EQ(expected, actual); EXPECT_EQ(expected_raw, actual_raw);
    EXPECT_EQ(expected_raw.size(), result.meta_.vector_count_);
    if (expected_raw.empty()) { EXPECT_TRUE(result.meta_.is_empty()); EXPECT_TRUE(result.rows_.empty()); }
    else { EXPECT_EQ(expected_centers, actual_centers); EXPECT_EQ(expected_centers.size(), result.meta_.centroid_count_); }
    for (int64_t type = IVF_ROW_TYPE_CENTROID; type < IVF_ROW_TYPE_MAX; ++type) { EXPECT_EQ(0, result.reused_ids_[type]); }
  }

  void verify(Result &result, const std::vector<uint64_t> &deleted = {})
  {
    const int64_t started = ObTimeUtility::current_time();
    const ObIvfSSTableMeta &meta = result.meta_;
    std::vector<int64_t> counts(data_.k_, 0), stored_counts(data_.k_, -1);
    std::vector<int64_t> delta_primary(data_.delta_, 0), delta_secondary(data_.delta_, 0);
    int64_t raw = 0, center_records = 0;
    const auto removed = [&](uint64_t vid) { return std::find(deleted.begin(), deleted.end(), vid) != deleted.end(); };
    float x[DIM];
    for (const auto &row : result.rows_) {
      if (row.type_ == IVF_ROW_TYPE_CENTROID) {
        ++center_records;
        int64_t self = -1, group = -1;
        ASSERT_EQ(OB_SUCCESS, meta.decode_centroid_key(row.cid_, row.vid_, self, group));
        if (meta.is_hierarchical() && group == 0) { continue; }
        const int64_t c = self - meta.leaf_cid_base();
        ASSERT_GE(c, 0);
        ASSERT_LT(c, data_.k_);
        const float *center = nullptr;
        uint32_t count = 0;
        ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(
            ObString(row.payload_.size(), row.payload_.data()), DIM, center, count));
        if (stored_counts[c] >= 0) { EXPECT_EQ(stored_counts[c], count); }
        stored_counts[c] = count; // Every parent replica must carry the same final leaf count.
        for (int64_t d = 0; d < DIM; ++d) { x[d] = data_.center(c, d); }
        EXPECT_EQ(0, MEMCMP(x, center, sizeof(x)));
      } else {
        ASSERT_FALSE(removed(row.vid_));
        const bool is_delta = (row.vid_ & 1) != 0;
        const int64_t i = is_delta ? (row.vid_ - 2 * data_.n_ - 1) / 2 : row.vid_ / 2 - 1;
        ASSERT_GE(i, 0);
        ASSERT_LT(i, is_delta ? data_.delta_ : data_.n_);
        if (row.type_ == IVF_ROW_TYPE_RAW_VECTOR) {
          data_.vector(i, is_delta, x);
          ASSERT_EQ(sizeof(x), row.payload_.size());
          EXPECT_EQ(0, MEMCMP(x, row.payload_.data(), sizeof(x))); ++raw;
        } else {
          ASSERT_EQ(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, row.type_);
          const int64_t c = row.cid_ - meta.leaf_cid_base();
          ASSERT_GE(c, 0);
          ASSERT_LT(c, data_.k_);
          ++counts[c];
          if (!is_delta) {
            Record source_key = row;
            source_key.cid_ = base_->get_ivf_meta().leaf_storage_cid(c);
            const auto old = std::lower_bound(base_rows_.begin(), base_rows_.end(), source_key,
                [](const Record &a, const Record &b) { return a.key() < b.key(); });
            ASSERT_NE(base_rows_.end(), old);
            ASSERT_EQ(source_key.key(), old->key());
            EXPECT_EQ(old->payload_, row.payload_);
          } else {
            int64_t p = -1, s = -1;
            data_.vector(i, true, x); data_.reference(x, p, s);
            EXPECT_TRUE(c == p || c == s);
            if (c == p) { ++delta_primary[i]; }
            if (c == s) { ++delta_secondary[i]; }
          }
        }
      }
    }
    int64_t expected = data_.n_ + data_.delta_;
    for (auto vid : deleted) {
      if ((vid % 2 == 0 && vid >= 2 && vid <= static_cast<uint64_t>(2 * data_.n_))
          || (vid % 2 == 1 && vid > static_cast<uint64_t>(2 * data_.n_)
              && vid <= data_.delta_vid(data_.delta_ - 1))) { --expected; }
    }
    EXPECT_EQ(expected, raw); EXPECT_EQ(expected, result.meta_.vector_count_);
    if (expected == 0) { EXPECT_TRUE(result.meta_.is_empty()); EXPECT_TRUE(result.rows_.empty()); }
    else {
      EXPECT_GE(center_records, data_.k_);
      for (int64_t c = 0; c < data_.k_; ++c) { EXPECT_EQ(counts[c], stored_counts[c]); }
      for (const auto &old : base_rows_) {
        if (old.type_ != IVF_ROW_TYPE_LOW_PRECISION_VECTOR || removed(old.vid_)) { continue; }
        Record target = old;
        const int64_t c = old.cid_ - base_->get_ivf_meta().leaf_cid_base();
        target.cid_ = meta.leaf_storage_cid(c);
        const auto kept = std::lower_bound(result.rows_.begin(), result.rows_.end(), target,
            [](const Record &a, const Record &b) { return a.key() < b.key(); });
        ASSERT_NE(result.rows_.end(), kept);
        EXPECT_EQ(target, *kept);
      }
      for (int64_t i = 0; i < data_.delta_; ++i) {
        EXPECT_EQ(removed(data_.delta_vid(i)) ? 0 : 1, delta_primary[i]);
        EXPECT_EQ(removed(data_.delta_vid(i)) ? 0 : 1, delta_secondary[i]);
      }
    }
    for (int64_t type = IVF_ROW_TYPE_CENTROID; type < IVF_ROW_TYPE_MAX; ++type) {
      EXPECT_EQ(0, result.reused_ids_[type]);

    }
    result.verify_us_ = ObTimeUtility::current_time() - started;
    result.rss_after_validation_ = resident_bytes();
  }


  std::unique_ptr<ObIvfSSTable> base_, delta_;
  std::vector<Record> base_rows_;
  std::vector<SourceMacro> source_macros_;
  int64_t saved_replay_bytes_ = 0;
  bool inc_source_ = false;
};

TEST_F(TestIVFInsertionStorage, concat_remaps_and_deduplicates_centers_preserving_encoded_rows)
{
  data_.n_ = 512; data_.delta_ = 64;
  for (const ObRowStoreType format : {CS_ENCODING_ROW_STORE, FLAT_ROW_STORE, ENCODING_ROW_STORE}) {
  for (const bool grouped : {false, true}) {
  for (const bool distinct_delta : {false, true}) {
    SCOPED_TRACE(format);
    SCOPED_TRACE(grouped);
    SCOPED_TRACE(distinct_delta);
    ASSERT_NO_FATAL_FAILURE(build_inputs(false, format, grouped, distinct_delta));
    std::vector<Record> delta_rows;
    ASSERT_EQ(OB_SUCCESS, read_rows(*delta_, delta_rows));
    const auto delta_meta = delta_->get_ivf_meta();
    const std::vector<uint64_t> deleted = {data_.base_vid(3), data_.base_vid(19), data_.delta_vid(0)};
    Result output;
    ASSERT_EQ(OB_SUCCESS, run_merge(output, deleted, format, MAJOR_MERGE,
        ObIVFProcessType::CONCATENATION, grouped ? 1 : 0));
    ASSERT_NO_FATAL_FAILURE(verify_concat(output, delta_rows, delta_meta, deleted));
    EXPECT_EQ(data_.k_ + distinct_delta, output.meta_.centroid_count_);
    EXPECT_EQ(grouped, output.meta_.is_hierarchical());
    EXPECT_EQ(data_.n_ + data_.delta_ - deleted.size(), output.capture_rows_);
  }
  }
  }
}

TEST_F(TestIVFInsertionStorage, concat_preserves_incremental_visibility_and_empty_input_semantics)
{
  data_.n_ = 512;
  for (const ObMergeType merge_type : {MAJOR_MERGE, INC_MAJOR_MERGE}) {
  for (const int64_t delta : {0, 32}) {
    SCOPED_TRACE(merge_type);
    SCOPED_TRACE(delta);
    inc_source_ = is_inc_major_merge(merge_type); data_.delta_ = delta;
    ASSERT_NO_FATAL_FAILURE(build_inputs());
    std::vector<Record> delta_rows;
    ASSERT_EQ(OB_SUCCESS, read_rows(*delta_, delta_rows));
    const auto delta_meta = delta_->get_ivf_meta();
    std::vector<uint64_t> deleted = {data_.base_vid(0)};
    Result output;
    ASSERT_EQ(OB_SUCCESS, run_merge(output, deleted, CS_ENCODING_ROW_STORE, merge_type, ObIVFProcessType::CONCATENATION));
    ASSERT_NO_FATAL_FAILURE(verify_concat(output, delta_rows, delta_meta, inc_source_ ? std::vector<uint64_t>{} : deleted));
    if (!inc_source_ && delta == 0) {
      deleted.clear();
      for (int64_t i = 0; i < data_.n_; ++i) { deleted.push_back(data_.base_vid(i)); }
      Result empty;
      ASSERT_EQ(OB_SUCCESS, run_merge(empty, deleted, CS_ENCODING_ROW_STORE, merge_type, ObIVFProcessType::CONCATENATION));
      ASSERT_NO_FATAL_FAILURE(verify_concat(empty, delta_rows, delta_meta, deleted));
    }
  }
  }
}

TEST_F(TestIVFInsertionStorage, insertion_preserves_geometry_and_codes)
{
  ASSERT_NO_FATAL_FAILURE(build_inputs());
  Result optimized;
  ASSERT_EQ(OB_SUCCESS, run_merge(optimized));
  ASSERT_TRUE(optimized.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(optimized));
  EXPECT_EQ(data_.n_, optimized.old_capture_rows_);
  EXPECT_EQ(data_.n_ + data_.delta_, optimized.capture_rows_);
  EXPECT_EQ(0, optimized.reused_ids_[IVF_ROW_TYPE_LOW_PRECISION_VECTOR]);
  EXPECT_EQ(0, optimized.reused_ids_[IVF_ROW_TYPE_RAW_VECTOR]);
}



TEST_F(TestIVFInsertionStorage, deletion_cleans_raw_and_both_postings_without_double_subtraction)
{
  data_.n_ = 2048; data_.delta_ = 64;
  ASSERT_NO_FATAL_FAILURE(build_inputs());
  const std::vector<uint64_t> deleted = {data_.base_vid(3), data_.base_vid(19), data_.delta_vid(0)};
  Result optimized;
  ASSERT_EQ(OB_SUCCESS, run_merge(optimized, deleted));
  ASSERT_TRUE(optimized.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(optimized, deleted));
  EXPECT_EQ(2, optimized.stats_.removed_base_raw_rows_);
  EXPECT_EQ(data_.n_ - 2, optimized.old_capture_rows_);
}

TEST_F(TestIVFInsertionStorage, flat_and_pax_sources_preserve_rows_codes_and_deletion_semantics)
{
  data_.n_ = 2048; data_.delta_ = 64;
  for (const ObRowStoreType format : {FLAT_ROW_STORE, ENCODING_ROW_STORE}) {
    SCOPED_TRACE(format);
    ASSERT_NO_FATAL_FAILURE(build_inputs(false, format));
    Result optimized;
    ASSERT_EQ(OB_SUCCESS, run_merge(optimized, {}, format));
    ASSERT_TRUE(optimized.eligible_);
    ASSERT_NO_FATAL_FAILURE(verify(optimized));
    const std::vector<uint64_t> deleted = {data_.base_vid(3), data_.delta_vid(0)};
    Result cleaned;
    ASSERT_EQ(OB_SUCCESS, run_merge(cleaned, deleted, format));
    ASSERT_TRUE(cleaned.eligible_);
    ASSERT_NO_FATAL_FAILURE(verify(cleaned, deleted));
    EXPECT_EQ(1, cleaned.stats_.removed_base_raw_rows_);
    EXPECT_EQ(data_.n_ - 1, cleaned.old_capture_rows_);
  }
}

TEST_F(TestIVFInsertionStorage, grouped_source_keeps_leaf_geometry_codes_and_counts)
{
  data_.n_ = 2048; data_.delta_ = 64;
  ASSERT_NO_FATAL_FAILURE(build_inputs(false, CS_ENCODING_ROW_STORE, true));
  ASSERT_TRUE(base_->get_ivf_meta().is_hierarchical());
  ASSERT_EQ(2, base_->get_ivf_meta().parent_centroid_count_);
  Result optimized;
  ASSERT_EQ(OB_SUCCESS, run_merge(optimized));
  ASSERT_TRUE(optimized.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(optimized));
  // Leaf ordinal 0 has two parent records; its deleted vector also has SOAR.
  const std::vector<uint64_t> deleted = {data_.base_vid(0), data_.delta_vid(0)};
  Result cleaned;
  ASSERT_EQ(OB_SUCCESS, run_merge(cleaned, deleted));
  ASSERT_TRUE(cleaned.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(cleaned, deleted));
  EXPECT_EQ(1, cleaned.stats_.removed_base_raw_rows_);
}

TEST_F(TestIVFInsertionStorage, empty_delta_preserves_records_and_empty_leaf_accepts_new_vectors)
{
  data_.n_ = 2048; data_.delta_ = 0;
  ASSERT_NO_FATAL_FAILURE(build_inputs());
  Result unchanged;
  ASSERT_EQ(OB_SUCCESS, run_merge(unchanged));
  ASSERT_TRUE(unchanged.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(unchanged));
  EXPECT_EQ(base_rows_, unchanged.rows_);
  data_.delta_ = 64; data_.empty_leaf_ = true;
  ASSERT_NO_FATAL_FAILURE(build_inputs());
  Result filled;
  ASSERT_EQ(OB_SUCCESS, run_merge(filled));
  ASSERT_TRUE(filled.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(filled));
}

TEST_F(TestIVFInsertionStorage, all_deleted_produces_empty_output)
{
  data_.n_ = 2048; data_.delta_ = 0;
  ASSERT_NO_FATAL_FAILURE(build_inputs());
  std::vector<uint64_t> deleted;
  for (int64_t i = 0; i < data_.n_ - 1; ++i) { deleted.push_back(data_.base_vid(i)); }
  Result one_survivor;
  ASSERT_EQ(OB_SUCCESS, run_merge(one_survivor, deleted));
  ASSERT_TRUE(one_survivor.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(one_survivor, deleted));
  EXPECT_EQ(1, one_survivor.meta_.vector_count_);
  EXPECT_EQ(data_.k_, one_survivor.meta_.centroid_count_);
  EXPECT_FALSE(one_survivor.meta_.is_empty());
  deleted.push_back(data_.base_vid(data_.n_ - 1));
  Result result;
  ASSERT_EQ(OB_SUCCESS, run_merge(result, deleted));
  ASSERT_TRUE(result.eligible_);
  ASSERT_NO_FATAL_FAILURE(verify(result, deleted));
}



TEST_F(TestIVFInsertionStorage, insertion_has_major_and_incremental_delete_semantics)
{
  data_.n_ = 2048; data_.delta_ = 64;
  for (const auto type : {MAJOR_MERGE, INC_MAJOR_MERGE}) {
    inc_source_ = is_inc_major_merge(type);
    ASSERT_NO_FATAL_FAILURE(build_inputs());
    if (inc_source_) { ASSERT_TRUE(base_->is_inc_ivf_sstable()); }
    const std::vector<uint64_t> deleted = {data_.base_vid(3), data_.delta_vid(0)};
    Result first;
    ASSERT_EQ(OB_SUCCESS, run_merge(first, deleted, CS_ENCODING_ROW_STORE, type));
    const auto visible_deletes = inc_source_ ? std::vector<uint64_t>{} : deleted;
    ASSERT_NO_FATAL_FAILURE(verify(first, visible_deletes));
    EXPECT_EQ(data_.n_ + data_.delta_ - (inc_source_ ? 0 : 2), first.meta_.vector_count_);
  }
}

TEST_F(TestIVFInsertionStorage, duplicate_vid_fails_without_publishing_or_damaging_source)
{
  data_.n_ = 2048; data_.delta_ = 64;
  ASSERT_NO_FATAL_FAILURE(build_inputs(true));
  Result failed;
  EXPECT_NE(OB_SUCCESS, run_merge(failed));
  // Common RAW capture rejects duplicates before producing merge output.
  EXPECT_TRUE(failed.rows_.empty());
  EXPECT_FALSE(sstable_builder_.get_ivf_meta().is_valid());
  ASSERT_EQ(OB_SUCCESS, reset_output()); // Discard the failed attempt's roots.
  std::vector<Record> surviving_source;
  ASSERT_EQ(OB_SUCCESS, read_rows(*base_, surviving_source));
  EXPECT_EQ(base_rows_, surviving_source);
}


class TestRADatumStoreReader : public ::testing::Test
{
protected:
  using Store = sql::ObRADatumStore;

  void SetUp() override
  {
    ASSERT_NE(nullptr, ObTenantEnv::get_tenant());
    ASSERT_NE(nullptr, MTL(tmp_file::ObTenantTmpFileManager *));
    THIS_WORKER.set_timeout_ts(INT64_MAX);
  }

  static int append_row(Store &store, const int64_t row_id, const int64_t payload_size)
  {
    ObSEArray<ObDatum, 2> cells;
    ObDatum id;
    id.set_string(reinterpret_cast<const char *>(&row_id), sizeof(row_id));
    const std::string payload(payload_size, static_cast<char>('a' + row_id % 26));
    ObDatum data;
    data.set_string(payload.data(), payload.size());
    int ret = cells.push_back(id);
    if (OB_SUCC(ret)) {
      ret = cells.push_back(data);
    }
    if (OB_SUCC(ret)) {
      ret = store.add_row(cells);
    }
    return ret;
  }

  static bool matches(const Store::StoredRow *row,
                      const int64_t row_id, const int64_t payload_size)
  {
    if (nullptr == row || 2 != row->cnt_ || row_id != row->cells()[0].get_int()
        || payload_size != row->cells()[1].len_) {
      return false;
    }
    const char expected = static_cast<char>('a' + row_id % 26);
    const char *data = row->cells()[1].ptr_;
    for (int64_t i = 0; i < payload_size; ++i) {
      if (expected != data[i]) {
        return false;
      }
    }
    return true;
  }

  void check_parallel_readers(Store &store, const int64_t payload_size)
  {
    const int64_t worker_count = 8;
    const int64_t row_count = store.get_row_cnt();
    const int64_t baseline = store.get_mem_hold();
    ObTenantBase *tenant = ObTenantEnv::get_tenant();
    std::atomic<bool> start(false);
    std::atomic<bool> release(false);
    std::atomic<int64_t> ready(0);
    std::atomic<int64_t> failures(0);
    int64_t reader_holds[worker_count] = {};
    std::vector<std::thread> workers;
    for (int64_t worker_id = 0; worker_id < worker_count; ++worker_id) {
      workers.emplace_back([&, worker_id]() {
        ObTenantEnv::set_tenant(tenant);
        THIS_WORKER.set_timeout_ts(INT64_MAX);
        while (!start.load()) {
          std::this_thread::yield();
        }
        {
          Store::Reader reader(store);
          const Store::StoredRow *row = nullptr;
          int ret = reader.get_row(0, row);
          if (OB_SUCCESS != ret || !matches(row, 0, payload_size)) {
            failures.fetch_add(1);
          }
          reader_holds[worker_id] = reader.buf_.capacity()
              + (reader.buf_.is_inited() ? sizeof(Store::LinkNode) : 0)
              + reader.idx_buf_.capacity()
              + (reader.idx_buf_.is_inited() ? sizeof(Store::LinkNode) : 0);
          ready.fetch_add(1);
          while (!release.load()) {
            std::this_thread::yield();
          }
          for (int64_t round = 0; round < 96 && 0 == failures.load(); ++round) {
            // Shared rows, reverse traversal across index blocks, and independent
            // rows exercise both buffer reuse and concurrent replacement.
            const int64_t row_id = 0 == round % 3 ? 0
                : (1 == round % 3 ? row_count - 1 - round % row_count
                   : (worker_id * 97 + round * 31) % row_count);
            ret = reader.get_row(row_id, row);
            if (OB_SUCCESS != ret || !matches(row, row_id, payload_size)) {
              failures.fetch_add(1);
            }
            if (0 == round % 8) {
              reader.reuse();
            }
          }
        }
        ObTenantEnv::set_tenant(nullptr);
      });
    }
    start.store(true);
    while (ready.load() != worker_count) {
      std::this_thread::yield();
    }
    int64_t expected_hold = baseline;
    for (int64_t i = 0; i < worker_count; ++i) {
      expected_hold += reader_holds[i];
    }
    EXPECT_EQ(expected_hold, store.get_mem_hold());
    release.store(true);
    for (std::thread &worker : workers) {
      worker.join();
    }
    EXPECT_EQ(0, failures.load());
    EXPECT_EQ(baseline, store.get_mem_hold());
  }
};

TEST_F(TestRADatumStoreReader, parallel_spilled_readers_cross_index_blocks_and_release_buffers)
{
  Store store;
  const int64_t payload_size = (128L << 10) + 1;
  // More than half a data block per row forces natural block switching. This
  // reaches the second on-disk index block without mocking the file reader.
  const int64_t row_count = Store::IndexBlock::capacity() + 8;
  ASSERT_EQ(OB_SUCCESS, store.init(1L << 20, MTL_ID()));
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, append_row(store, i, payload_size));
  }
  ASSERT_EQ(OB_SUCCESS, store.finish_add_row());
  ASSERT_EQ(OB_SUCCESS, store.dump(true));
  ASSERT_EQ(0, store.get_inmemory_rows());
  ASSERT_GE(store.get_file_size(), row_count * payload_size);
  ASSERT_GE(store.blocks_.count(), 2);
  for (int64_t i = 0; i < store.blocks_.count(); ++i) {
    ASSERT_TRUE(store.blocks_.at(i).is_idx_block_);
    ASSERT_TRUE(store.blocks_.at(i).on_disk_);
  }
  check_parallel_readers(store, payload_size);
}

TEST_F(TestRADatumStoreReader, parallel_readers_mix_memory_and_temporary_file)
{
  Store store;
  const int64_t payload_size = 2048;
  const int64_t row_count = 4096;
  ASSERT_EQ(OB_SUCCESS, store.init(1L << 20, MTL_ID()));
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, append_row(store, i, payload_size));
  }
  ASSERT_EQ(OB_SUCCESS, store.finish_add_row());
  ASSERT_GT(store.get_file_size(), 0);
  ASSERT_GT(store.get_inmemory_rows(), 0);
  ASSERT_LT(store.get_inmemory_rows(), row_count);
  check_parallel_readers(store, payload_size);
}

class RecordingRAMemoryCallback final : public sql::ObSqlMemoryCallback
{
public:
  RecordingRAMemoryCallback() : allocated_(0), freed_(0), dumped_(0) {}
  void alloc(const int64_t size) override { allocated_ += size; }
  void free(const int64_t size) override { freed_ += size; }
  void dumped(const int64_t size) override { dumped_ += size; }
  int64_t allocated_;
  int64_t freed_;
  int64_t dumped_;
};

TEST_F(TestRADatumStoreReader, existing_reader_survives_append_dump_and_reuse)
{
  RecordingRAMemoryCallback callback;
  Store store;
  store.set_mem_stat(&callback);
  ASSERT_EQ(OB_SUCCESS, store.init(1L << 20, MTL_ID()));
  Store::Reader reader(store);
  const Store::StoredRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, append_row(store, 0, 1024));
  ASSERT_EQ(OB_SUCCESS, reader.get_row(0, row));
  ASSERT_TRUE(matches(row, 0, 1024));
  ASSERT_EQ(OB_SUCCESS, append_row(store, 1, 1024));
  ASSERT_EQ(OB_SUCCESS, reader.get_row(1, row));
  ASSERT_TRUE(matches(row, 1, 1024));
  ASSERT_EQ(OB_SUCCESS, store.dump(true));
  ASSERT_EQ(OB_SUCCESS, reader.get_row(0, row));
  ASSERT_TRUE(matches(row, 0, 1024));
  ASSERT_EQ(OB_SUCCESS, append_row(store, 2, 1024));
  ASSERT_EQ(OB_SUCCESS, reader.get_row(2, row));
  ASSERT_TRUE(matches(row, 2, 1024));
  ASSERT_EQ(OB_SUCCESS, store.dump(true));
  ASSERT_EQ(OB_SUCCESS, store.get_row(1, row));
  ASSERT_TRUE(matches(row, 1, 1024));
  EXPECT_EQ(store.get_mem_hold(), callback.allocated_ - callback.freed_);
  EXPECT_EQ(store.get_file_size(), callback.dumped_);
  EXPECT_EQ(OB_INDEX_OUT_OF_RANGE, reader.get_row(3, row));
  // Release external reader buffers before resetting the store's accounting.
  reader.reuse();
  store.reuse();
  EXPECT_EQ(0, store.get_row_cnt());
  EXPECT_EQ(0, store.get_file_size());
  ASSERT_EQ(OB_SUCCESS, append_row(store, 0, 4096));
  ASSERT_EQ(OB_SUCCESS, reader.get_row(0, row));
  ASSERT_TRUE(matches(row, 0, 4096));
  reader.reset();
  store.reset();
  EXPECT_EQ(0, store.get_mem_hold());
  EXPECT_GT(callback.allocated_, 0);
  EXPECT_EQ(callback.allocated_, callback.freed_);
}

TEST_F(TestRADatumStoreReader, iteration_age_keeps_rows_until_next_age)
{
  Store store;
  const int64_t payload_size = (128L << 10) + 1;
  ASSERT_EQ(OB_SUCCESS, store.init(1L << 20, MTL_ID()));
  for (int64_t i = 0; i < 6; ++i) {
    ASSERT_EQ(OB_SUCCESS, append_row(store, i, payload_size));
  }
  ASSERT_EQ(OB_SUCCESS, store.dump(true));
  const int64_t baseline = store.get_mem_hold();
  {
    Store::IterationAge age = {};
    Store::Reader reader(store);
    reader.set_iteration_age(&age);
    const Store::StoredRow *first = nullptr;
    const Store::StoredRow *second = nullptr;
    ASSERT_EQ(OB_SUCCESS, reader.get_row(0, first));
    ASSERT_EQ(OB_SUCCESS, reader.get_row(1, second));
    ASSERT_TRUE(matches(first, 0, payload_size));
    ASSERT_TRUE(matches(second, 1, payload_size));
    const int64_t two_buffers = store.get_mem_hold();
    age.inc();
    const Store::StoredRow *third = nullptr;
    ASSERT_EQ(OB_SUCCESS, reader.get_row(2, third));
    ASSERT_TRUE(matches(third, 2, payload_size));
    // Only the preceding buffer and current buffer remain after expiration.
    EXPECT_EQ(two_buffers, store.get_mem_hold());
    reader.reuse();
    EXPECT_EQ(baseline, store.get_mem_hold());
  }
  EXPECT_EQ(baseline, store.get_mem_hold());
}

class FailingRAReadAllocator final : public ObIAllocator
{
public:
  FailingRAReadAllocator() : fail_(false) {}
  void *alloc(const int64_t size) override
  { return alloc(size, ObMemAttr(MTL_ID(), "RAReadFailUT")); }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  { return fail_ ? nullptr : allocator_.alloc(size, attr); }
  void free(void *ptr) override { allocator_.free(ptr); }
  bool fail_;
private:
  DefaultPageAllocator allocator_;
};

TEST_F(TestRADatumStoreReader, failed_reader_allocation_and_timeout_release_buffers)
{
  FailingRAReadAllocator allocator;
  Store store(&allocator);
  ASSERT_EQ(OB_SUCCESS, store.init(1L << 20, MTL_ID()));
  ASSERT_EQ(OB_SUCCESS, append_row(store, 0, 1024));
  ASSERT_EQ(OB_SUCCESS, append_row(store, 1, 300L << 10));
  ASSERT_EQ(OB_SUCCESS, store.dump(true));
  const int64_t baseline = store.get_mem_hold();
  {
    Store::Reader reader(store);
    const Store::StoredRow *row = nullptr;
    allocator.fail_ = true;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, reader.get_row(0, row));
    EXPECT_EQ(baseline, store.get_mem_hold());
    allocator.fail_ = false;
    ASSERT_EQ(OB_SUCCESS, reader.get_row(0, row));
    ASSERT_TRUE(matches(row, 0, 1024));
    allocator.fail_ = true;
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, reader.get_row(1, row));
    EXPECT_EQ(baseline, store.get_mem_hold());
    allocator.fail_ = false;
    ASSERT_EQ(OB_SUCCESS, reader.get_row(1, row));
    ASSERT_TRUE(matches(row, 1, 300L << 10));
    reader.reuse();
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() - 1);
    EXPECT_EQ(OB_TIMEOUT, reader.get_row(0, row));
    THIS_WORKER.set_timeout_ts(INT64_MAX);
  }
  EXPECT_EQ(baseline, store.get_mem_hold());
}

namespace {
class RefineSpillStore final : public ObIVFVectorStore
{
public:
  static constexpr int64_t ROWS = 4097;
  static constexpr int64_t DIM = 512;
  static constexpr int64_t CENTERS = 64;
  bool is_valid() const override { return true; }
  int64_t get_vector_count() const override { return ROWS; }
  int64_t get_dimension() const override { return DIM; }
  static float value(int64_t row, int64_t dim)
  { return static_cast<float>((row * 137 + dim * 71) % 997) / 997.0F; }
  int get_all_vids(ObIArray<ObIVFVectorVID> &vids) const override
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < ROWS; ++i) { ret = vids.push_back(i); }
    return ret;
  }
  int batch_get_by_vid(const ObIArray<ObIVFVectorVID> &vids, int64_t begin, int64_t end,
      ObIArray<float> &values) const override
  {
    int ret = OB_SUCCESS;
    values.reuse();
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      for (int64_t d = 0; OB_SUCC(ret) && d < DIM; ++d) { ret = values.push_back(value(vids.at(i), d)); }
    }
    return ret;
  }
};
}

TEST_F(TestRADatumStoreReader, refinement_shared_budget_spill_matches_resident_training)
{
  RefineSpillStore source;
  ObArray<ObIVFVectorVID> vids;
  ObArray<float> centers, expected;
  ObArray<int64_t> assignments;
  ASSERT_EQ(OB_SUCCESS, source.get_all_vids(vids));
  for (int64_t cid = 0; cid < source.CENTERS; ++cid) {
    for (int64_t d = 0; d < source.DIM; ++d) {
      ASSERT_EQ(OB_SUCCESS, centers.push_back(source.value(cid, d)));
    }
  }
  for (int64_t i = 0; i < source.ROWS; ++i) { ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % source.CENTERS)); }
  ObIVFVectorReader reader;
  ASSERT_EQ(OB_SUCCESS, reader.init(source));
  for (int64_t samples_per_cluster : {32, 3, 1}) {
    ObIVFHcaAlgorithmParam param;
    param.refine_param_.samples_per_cluster_ = samples_per_cluster;
    param.refine_param_.random_seed_ = 7919;
    ObIVFHcaAlgorithmAdapter reference;
    ASSERT_EQ(OB_SUCCESS, reference.init(param));
    ASSERT_EQ(OB_SUCCESS, reference.refine_train(
        reader, vids, centers, assignments, nullptr, source.CENTERS, expected));
    const int64_t payload = source.CENTERS * param.refine_param_.samples_per_cluster_ * source.DIM * sizeof(float);
    ObTenantBase *tenant = ObTenantEnv::get_tenant();
    // The same leftover reservation covers every shard, rather than granting
    // the full budget to each worker. Include a partially resident store.
    for (int64_t budget : {INT64_MAX, payload * 3 / 4, int64_t(0)}) {
      // Mixed residency needs enough rows to fill more than one store block.
      if (budget != 0 && budget != INT64_MAX && payload < 4 * 65536) { continue; }
      SCOPED_TRACE(budget);
      ObArray<int64_t> initial_fds, sample_fds, current_fds;
      ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), initial_fds));
      ObIVFDDLHcaAdapter algorithm;
      ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
      ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(source.ROWS, source.DIM, centers,
          assignments, nullptr, budget, 4));
      const auto &samples = *algorithm.refine_sample_indices();
      EXPECT_EQ(source.CENTERS * samples_per_cluster, samples.count());
      EXPECT_EQ(4, algorithm.refine_sample_shards());
      EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
      auto load_range = [&](int64_t shard, int64_t begin, int64_t end) -> int {
        int ret = OB_SUCCESS;
        for (int64_t offset = begin; OB_SUCC(ret) && offset < end; offset += 97) {
          ObArray<int64_t> positions;
          ObArray<float> values;
          for (int64_t i = offset; OB_SUCC(ret) && i < MIN(end, offset + 97); ++i) {
            // Source order deliberately reverses the trainer's vector order.
            const int64_t pos = samples.count() - 1 - i;
            if (OB_FAIL(positions.push_back(pos))) { break; }
            for (int64_t d = 0; OB_SUCC(ret) && d < source.DIM; ++d) {
              ret = values.push_back(source.value(samples.at(pos), d));
            }
          }
          if (OB_SUCC(ret)) { ret = algorithm.load_refine_samples(positions, values, shard); }
        }
        return ret;
      };
      // Simulate a failed gather after it has already appended rows to disk.
      ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_sample_shard(0));
      ASSERT_EQ(OB_SUCCESS, load_range(0, 0, samples.count() / 8));
      EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
      std::vector<std::thread> loaders;
      int results[4] = {};
      for (int64_t shard = 0; shard < 4; ++shard) {
        loaders.emplace_back([&, shard] {
          ObTenantEnv::set_tenant(tenant);
          THIS_WORKER.set_timeout_ts(INT64_MAX);
          int &ret = results[shard];
          if (OB_SUCCESS == (ret = algorithm.begin_refine_sample_shard(shard))
              && OB_SUCCESS == (ret = load_range(shard, samples.count() * shard / 4,
                  samples.count() * (shard + 1) / 4))) {
            ret = algorithm.finish_refine_sample_shard(shard);
          }
          ObTenantEnv::set_tenant(nullptr);
        });
      }
      for (auto &loader : loaders) { loader.join(); }
      for (int ret : results) { ASSERT_EQ(OB_SUCCESS, ret); }
      // Each shard has destroyed its sorter; repeated completion is harmless,
      // and the retained stores must still supply the following training pass.
      for (int64_t shard = 0; shard < 4; ++shard) {
        ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_sample_shard(shard));
      }
      ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
      EXPECT_TRUE(algorithm.refine_sample_indices()->empty());
      if (budget == INT64_MAX) {
        EXPECT_EQ(0, algorithm.refine_sample_file_size());
        EXPECT_GE(algorithm.refine_sample_memory_hold(), 2 * payload);
      } else {
        if (budget == 0 || samples_per_cluster == 32) {
          EXPECT_GT(algorithm.refine_sample_file_size(), 0);
        }
        // Small tail-wrap pools can fit entirely in the retained write blocks.
        // Each store can retain a 256 KiB write block plus a 64 KiB block/index
        // outside its quota; use enough rows above to actually exercise spilling.
        EXPECT_LE(algorithm.refine_sample_memory_hold(), budget + 4 * (320L << 10));
        if (budget != 0) { EXPECT_GT(algorithm.refine_sample_memory_hold(), 0); }
      }
      ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), current_fds));
      for (int64_t i = 0; i < current_fds.count(); ++i) {
        bool existed = false;
        for (int64_t j = 0; j < initial_fds.count(); ++j) { existed |= current_fds.at(i) == initial_fds.at(j); }
        if (!existed) { ASSERT_EQ(OB_SUCCESS, sample_fds.push_back(current_fds.at(i))); }
      }
      const int64_t stored_file_bytes = algorithm.refine_sample_file_size();
      int64_t read_bytes = 0, read_requests = 0;
      if (budget == 0) {
        ASSERT_FALSE(sample_fds.empty());
        int64_t rows = 0;
        THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() - 1);
        const int read_ret = algorithm.refine_train_prepare(reader, vids, centers,
            assignments, nullptr, source.CENTERS, rows);
        THIS_WORKER.set_timeout_ts(INT64_MAX);
        EXPECT_EQ(OB_TIMEOUT, read_ret);
      }
      ObArray<float> actual;
      bool finished = false;
      int batches = 0;
      for (; !finished && batches < 40; ++batches) {
        int64_t rows = 0;
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(reader, vids, centers,
            assignments, nullptr, source.CENTERS, rows));
        EXPECT_EQ(source.CENTERS * 2, rows);
        EXPECT_EQ((batches + 1) * rows, algorithm.refine_sample_read_rows());
        read_bytes = 0;
        read_requests = 0;
        for (int64_t i = 0; i < sample_fds.count(); ++i) {
          tmp_file::ObSNTmpFileInfo sn_info;
          tmp_file::ObTmpFileBaseInfo *info = &sn_info;
#ifdef OB_BUILD_SHARED_STORAGE
          tmp_file::ObSSTmpFileInfo ss_info;
          if (GCTX.is_shared_storage_mode()) { info = &ss_info; }
#endif
          ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_info(
              MTL_ID(), sample_fds.at(i), info));
          read_bytes += info->get_read_info().total_read_size_;
          read_requests += info->get_read_info().read_req_cnt_;
        }
        // Count temporary-file requests even when the lower cache serves them.
        // Across all batches, each shard is read once, with small index overhead.
        EXPECT_LE(read_bytes, 2 * stored_file_bytes);
        std::vector<std::thread> workers;
        for (int64_t t = 0; t < 4; ++t) {
          workers.emplace_back([&, t] {
            results[t] = algorithm.refine_train_distances(rows * t / 4, rows * (t + 1) / 4);
          });
        }
        for (auto &worker : workers) { worker.join(); }
        for (int ret : results) { ASSERT_EQ(OB_SUCCESS, ret); }
        ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_finish_batch(actual, finished));
      }
      ASSERT_TRUE(finished);
      if (budget == 0) { EXPECT_GT(read_bytes, 0); }
      fprintf(stdout, "REFINE_BATCH_IO samples_per_cluster=%ld budget=%ld file_bytes=%ld "
          "read_bytes=%ld read_requests=%ld batches=%d\n",
          samples_per_cluster, budget, stored_file_bytes, read_bytes, read_requests, batches);
      current_fds.reuse();
      ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), current_fds));
      EXPECT_EQ(initial_fds.count(), current_fds.count());
      EXPECT_EQ(2 * ((samples_per_cluster + 1) / 2), batches);
      ASSERT_EQ(expected.count(), actual.count());
      EXPECT_EQ(0, MEMCMP(&expected.at(0), &actual.at(0), expected.count() * sizeof(float)));
      EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
      EXPECT_EQ(0, algorithm.refine_sample_memory_hold());
      EXPECT_EQ(0, algorithm.refine_sample_file_size());
    }
  }
}

TEST_F(TestRADatumStoreReader, refinement_batch_layout_survives_external_sort)
{
  // Three epochs exceed the single-shard sort workspace (128 MiB). Source
  // order is reversed; the first training batch must still match its schedule.
  const int64_t n = 8193, k = 64, dim = 2048;
  ObArray<int64_t> initial_fds, final_fds;
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), initial_fds));
  ObArray<float> centers, values;
  ObArray<int64_t> assignments, positions;
  ObIVFHcaAlgorithmParam param;
  param.refine_param_.samples_per_cluster_ = 128;
  param.refine_param_.max_iterations_ = 3;
  for (int64_t i = 0; i < n; ++i) { ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % k)); }
  for (int64_t i = 0; i < k * dim; ++i) {
    ASSERT_EQ(OB_SUCCESS, centers.push_back(RefineSpillStore::value(i / dim, i % dim)));
  }
  ObIVFDDLHcaAdapter algorithm;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(param));
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(n, dim, centers, assignments, nullptr, 0, 1));
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_sample_shard(0));
  const auto &samples = *algorithm.refine_sample_indices();
  ASSERT_EQ(8192, samples.count());
  for (int64_t end = samples.count(); end > 0;) {
    const int64_t begin = MAX(0L, end - 64);
    positions.reuse();
    values.reuse();
    for (int64_t pos = end; pos-- > begin;) {
      ASSERT_EQ(OB_SUCCESS, positions.push_back(pos));
      for (int64_t d = 0; d < dim; ++d) {
        ASSERT_EQ(OB_SUCCESS, values.push_back(RefineSpillStore::value(samples.at(pos), d)));
      }
    }
    ASSERT_EQ(OB_SUCCESS, algorithm.load_refine_samples(positions, values));
    end = begin;
  }
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_sample_shard(0));
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_samples());
  EXPECT_GE(algorithm.refine_sample_file_size(), 3 * 8192L * dim * sizeof(float));
  EXPECT_LT(algorithm.refine_sample_memory_hold(), 1L << 20);
  ObIVFVectorReader unused_reader;
  ObArray<ObIVFVectorVID> unused_vids;
  int64_t rows = 0;
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(
      unused_reader, unused_vids, centers, assignments, nullptr, k, rows));
  EXPECT_EQ(2 * k, rows);
  EXPECT_EQ(rows, algorithm.refine_sample_read_rows());
  ASSERT_EQ(OB_SUCCESS, algorithm.refine_train_prepare(
      unused_reader, unused_vids, centers, assignments, nullptr, k, rows));
  EXPECT_EQ(rows, algorithm.refine_sample_read_rows()); // Prepared batch is reused.
  algorithm.cancel(OB_CANCELED);
  algorithm.release_samples();
  EXPECT_EQ(0, algorithm.refine_sample_file_size());
  ASSERT_EQ(OB_SUCCESS, FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(MTL_ID(), final_fds));
  EXPECT_EQ(initial_fds.count(), final_fds.count());
}

TEST_F(TestRADatumStoreReader, refinement_spill_cancel_releases_partial_shards)
{
  RefineSpillStore source;
  ObArray<float> centers;
  ObArray<int64_t> assignments;
  for (int64_t i = 0; i < source.ROWS; ++i) { ASSERT_EQ(OB_SUCCESS, assignments.push_back(i % 2)); }
  for (int64_t cid = 0; cid < 2; ++cid) {
    for (int64_t d = 0; d < source.DIM; ++d) { ASSERT_EQ(OB_SUCCESS, centers.push_back(source.value(cid, d))); }
  }
  ObIVFDDLHcaAdapter algorithm;
  ASSERT_EQ(OB_SUCCESS, algorithm.init(ObIVFHcaAlgorithmParam()));
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_samples(source.ROWS, source.DIM,
      centers, assignments, nullptr, 0, 2));
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.begin_refine_sample_shard(2));
  ASSERT_EQ(OB_SUCCESS, algorithm.begin_refine_sample_shard(0));
  const auto &samples = *algorithm.refine_sample_indices();
  ObArray<int64_t> positions;
  ObArray<float> values;
  for (int64_t i = 0; i < samples.count() / 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, positions.push_back(i));
    for (int64_t d = 0; d < source.DIM; ++d) {
      ASSERT_EQ(OB_SUCCESS, values.push_back(source.value(samples.at(i), d)));
    }
  }
  ASSERT_EQ(OB_SUCCESS, algorithm.load_refine_samples(positions, values, 0));
  ASSERT_EQ(OB_SUCCESS, algorithm.finish_refine_sample_shard(0));
  EXPECT_GT(algorithm.refine_sample_file_size(), 0);
  EXPECT_EQ(OB_STATE_NOT_MATCH, algorithm.finish_refine_samples());
  algorithm.cancel(OB_TIMEOUT);
  EXPECT_EQ(OB_TIMEOUT, algorithm.begin_refine_sample_shard(1));
  EXPECT_EQ(OB_TIMEOUT, algorithm.load_refine_samples(positions, values, 1));
  algorithm.release_samples();
  EXPECT_EQ(nullptr, algorithm.refine_sample_indices());
  EXPECT_EQ(0, algorithm.refine_sample_file_size());
}

TEST_F(TestRADatumStoreReader, ddl_resident_source_and_mixed_spill_preserve_batch_positions)
{
  const int64_t dim = 4;
  const int64_t rows_per_slice = 65;
  const int64_t total_rows = rows_per_slice * 2;
  const auto vid_of = [](int64_t rank) -> uint64_t {
    return rank == 0 ? 0 : (1ULL << 48) + rank * 3;
  };
  for (const bool spill_second_slice : {false, true}) {
    SCOPED_TRACE(spill_second_slice);
    ObArenaAllocator allocator("DDLResidentUT");
    ObDDLTabletContext tablet;
    tablet.ivf_ctx_ = OB_NEW(ObIVFTabletContext, "DDLResidentUT");
    ASSERT_NE(nullptr, tablet.ivf_ctx_);
    ASSERT_EQ(OB_SUCCESS, tablet.ivf_ctx_->init_rotator(allocator, dim));
    for (int64_t slice_idx = 0; slice_idx < 2; ++slice_idx) {
      auto *slice = OB_NEW(ObIVFSliceInput, "IVFSlice", slice_idx);
      ASSERT_NE(nullptr, slice);
      ASSERT_EQ(OB_SUCCESS, tablet.ivf_ctx_->register_slice(slice));
      const int64_t limit = spill_second_slice && slice_idx == 1 ? 1 : 64L << 20;
      ASSERT_EQ(OB_SUCCESS, slice->replay_.init(MTL_ID(), dim, limit, 8, 123, slice_idx, true));
      for (int64_t row = 0; row < rows_per_slice; ++row) {
        const int64_t rank = ((slice_idx * rows_per_slice + row) * 37) % total_rows;
        const float values[] = {float(rank), float(rank) + 0.25F, -float(rank), 1.0F};
        ASSERT_EQ(OB_SUCCESS, slice->replay_.add_row(vid_of(rank),
            ObString(sizeof(values), reinterpret_cast<const char *>(values))));
      }
    }
    {
      ObIVFDDLInput unsealed(allocator);
      EXPECT_FALSE(unsealed.is_fully_memory_resident());
      EXPECT_EQ(OB_STATE_NOT_MATCH, unsealed.init(tablet, 64, VIDA_L2));
      EXPECT_FALSE(unsealed.is_fully_memory_resident());
    }
    for (int64_t i = 0; i < 2; ++i) {
      auto &replay = tablet.ivf_ctx_->slice_inputs_.at(i)->replay_;
      ASSERT_EQ(OB_SUCCESS, replay.seal());
      if (spill_second_slice && i == 1) {
        ASSERT_GT(replay.get_file_size(), 0);
        ASSERT_EQ(0, replay.get_inmemory_row_count());
      } else {
        ASSERT_EQ(0, replay.get_file_size());
        ASSERT_EQ(rows_per_slice, replay.get_inmemory_row_count());
      }
    }
    ObIVFDDLInput input(allocator);
    ASSERT_EQ(OB_SUCCESS, input.init(tablet, 64, VIDA_L2));
    ASSERT_TRUE(input.is_valid());
    EXPECT_EQ(!spill_second_slice, input.is_fully_memory_resident());
    input.release_capture_metadata();
    for (int64_t i = 0; i < 2; ++i) {
      const auto &replay = tablet.ivf_ctx_->slice_inputs_.at(i)->replay_;
      EXPECT_EQ(0, replay.get_sample_count());
      EXPECT_EQ(0, replay.sample_allocator_.total());
    }
    // The merged references own their candidate metadata; retiring the
    // per-slice heaps must not invalidate first-level sample reads.
    ObArray<ObIVFVectorVID> sample_vids;
    ObArray<float> sample_values;
    ASSERT_GT(input.get_sample_count(), 0);
    ASSERT_GT(input.sample_allocator_.total(), 0);
    ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(
        0, input.get_sample_count(), sample_vids, sample_values));
    input.release_first_level_samples();
    input.release_first_level_samples();
    EXPECT_EQ(0, input.sample_allocator_.total());
    EXPECT_EQ(0, input.get_sample_count());
    EXPECT_TRUE(input.is_valid());
    EXPECT_EQ(OB_STATE_NOT_MATCH, input.batch_get_samples(0, 1, sample_vids, sample_values));
    ObArray<ObIVFVectorVID> vids;
    ASSERT_EQ(OB_SUCCESS, input.get_all_vids(vids));
    ASSERT_EQ(total_rows, vids.count());
    for (int64_t i = 0; i < total_rows; ++i) { EXPECT_EQ(vid_of(i), vids.at(i)); }
    // Training may revisit one VID in a wrapped batch. Read in an order that
    // crosses slice boundaries and differs from both source and VID order.
    ObArray<ObIVFVectorVID> selected;
    const int64_t ranks[] = {129, 0, 17, 129, 65, 1};
    for (const int64_t rank : ranks) { ASSERT_EQ(OB_SUCCESS, selected.push_back(vid_of(rank))); }
    ObArray<float> values;
    for (int repeat = 0; repeat < 2; ++repeat) {
      ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(selected, 0, selected.count(), values));
      ASSERT_EQ(selected.count() * dim, values.count());
      for (int64_t i = 0; i < selected.count(); ++i) {
        EXPECT_FLOAT_EQ(float(ranks[i]), values.at(i * dim));
        EXPECT_FLOAT_EQ(float(ranks[i]) + 0.25F, values.at(i * dim + 1));
        EXPECT_FLOAT_EQ(-float(ranks[i]), values.at(i * dim + 2));
      }
      EXPECT_EQ(!spill_second_slice, input.is_fully_memory_resident());
    }
  }
}

TEST_F(TestRADatumStoreReader, merge_samples_survive_fallback_and_release_before_raw)
{
  for (bool spill : {false, true}) {
    for (const auto strategy : {ObIVFProcessType::FULL_REBUILD,
         ObIVFProcessType::CONCATENATION, ObIVFProcessType::INSERTION}) {
      ObArenaAllocator allocator("MergeSampleUT");
      ObCompactionMemoryContext memory_context(allocator);
      ObIVFMergeInputAdapter input(allocator, &memory_context);
      input.vector_dim_ = 2;
      input.max_sample_count_ = 256;
      input.dist_type_ = VIDA_L2;
      ASSERT_EQ(OB_SUCCESS, input.row_desc_.init(IVF_SSTABLE_FORMAT_VERSION));
      auto *replay = OB_NEWx(ObIVFReplayInput, &allocator, allocator);
      ASSERT_NE(nullptr, replay);
      ASSERT_EQ(OB_SUCCESS, input.replay_inputs_.push_back(replay));
      ASSERT_EQ(OB_SUCCESS, replay->init(MTL_ID(), 2, spill ? 1 : 64L << 20));
      ASSERT_EQ(OB_SUCCESS, input.replay_row_offsets_.push_back(0));
      ASSERT_EQ(OB_SUCCESS, input.replay_row_offsets_.push_back(512));
      for (int64_t row = 0; row < 512; ++row) {
        const float values[] = {float(row), float(row) + 0.5F};
        ASSERT_EQ(OB_SUCCESS, replay->add_row(row,
            ObString(sizeof(values), reinterpret_cast<const char *>(values))));
        ASSERT_EQ(OB_SUCCESS, input.locators_.push_back(ObAsyncIVFSourceLocator(row, 0, 0, row)));
        ASSERT_EQ(OB_SUCCESS, offer_ivf_sample_candidate(input.sample_heap_, 256, 123, row, 0, row));
      }
      ASSERT_EQ(OB_SUCCESS, replay->seal());
      ASSERT_GT(input.sample_heap_allocator_.total(), 0);
      ASSERT_EQ(OB_SUCCESS, input.finish_source_());
      EXPECT_EQ(0, input.sample_heap_allocator_.total());
      ASSERT_GT(input.sample_locator_allocator_.total(), 0);
      ASSERT_EQ(256, input.get_sample_count());
      ObArray<ObIVFVectorVID> sample_vids;
      ObArray<float> values;
      ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(0, 16, sample_vids, values));

      // Exercise the production retirement policy without transferring input
      // ownership to the DAG; detach on all exits, including failed ASSERTs.
      ObIVFMergeDagCtx dag;
      struct BorrowedInputGuard
      {
        ObIVFMergeDagCtx &dag_;
        ~BorrowedInputGuard() { dag_.input_adapter_ = nullptr; }
      } input_guard{dag};
      dag.input_adapter_ = &input;
      dag.process_type_ = strategy;
      ObIVFMergeStepPerf finished;
      finished.stage_ = ObIVFMergeStage::HIERARCHY;
      finished.step_ = ObIVFMergeStep::HIERARCHY_ASSIGN;
      finished.depth_ = 0;
      // A failed prior/root-quality check routes back to root clustering.
      dag.current_step_perf_.stage_ = ObIVFMergeStage::HIERARCHY;
      dag.current_step_perf_.step_ = ObIVFMergeStep::HIERARCHY_CLUSTER;
      dag.current_step_perf_.depth_ = 0;
      dag.release_samples_after_step_(finished);
      EXPECT_FALSE(input.samples_released_);
      ASSERT_EQ(OB_SUCCESS, input.batch_get_samples(0, 16, sample_vids, values));
      EXPECT_EQ(256, input.get_sample_count());

      if (strategy == ObIVFProcessType::FULL_REBUILD) {
        finished.step_ = ObIVFMergeStep::HIERARCHY_CLUSTER;
        dag.current_step_perf_.step_ = ObIVFMergeStep::HIERARCHY_ASSIGN;
      } else {
        dag.current_step_perf_.step_ = ObIVFMergeStep::HIERARCHY_UPDATE;
      }
      dag.release_samples_after_step_(finished);
      dag.release_samples_after_step_(finished);
      EXPECT_TRUE(input.samples_released_);
      EXPECT_EQ(0, input.sample_locator_allocator_.total());
      EXPECT_TRUE(input.is_valid());
      EXPECT_EQ(OB_STATE_NOT_MATCH, input.batch_get_samples(0, 1, sample_vids, values));
      ObArray<ObIVFVectorVID> vids;
      ASSERT_EQ(OB_SUCCESS, vids.push_back(511));
      ASSERT_EQ(OB_SUCCESS, vids.push_back(0));
      ASSERT_EQ(OB_SUCCESS, input.batch_get_by_vid(vids, 0, vids.count(), values));
      ASSERT_EQ(4, values.count());
      EXPECT_FLOAT_EQ(511.0F, values.at(0));
      EXPECT_FLOAT_EQ(511.5F, values.at(1));
      EXPECT_FLOAT_EQ(0.0F, values.at(2));
      EXPECT_EQ(!spill, input.is_fully_memory_resident());
      ObArray<int64_t> indexes;
      ASSERT_EQ(OB_SUCCESS, indexes.push_back(0));
      ASSERT_EQ(OB_SUCCESS, indexes.push_back(511));
      ASSERT_EQ(OB_SUCCESS, input.batch_get_by_source(indexes, values));
      EXPECT_FLOAT_EQ(0.0F, values.at(0));
      EXPECT_FLOAT_EQ(511.0F, values.at(2));
    }
  }
}

TEST_F(TestRADatumStoreReader, replay_sampling_metadata_releases_without_payload)
{
  for (bool spill : {false, true}) {
    ObArenaAllocator allocator("IVFSampleUT");
    ObIVFReplayInput replay(allocator);
    ASSERT_EQ(OB_SUCCESS, replay.init(MTL_ID(), 2, spill ? 1 : 64L << 20, 256, 123, 0, true));
    for (int64_t row = 0; row < 512; ++row) {
      const float values[] = {float(row), float(row) + 0.5F};
      ASSERT_EQ(OB_SUCCESS, replay.add_row(row,
          ObString(sizeof(values), reinterpret_cast<const char *>(values))));
    }
    ASSERT_EQ(OB_SUCCESS, replay.seal());
    ASSERT_EQ(256, replay.get_sample_count());
    ASSERT_GT(replay.sample_allocator_.total(), 0);
    replay.release_sample_candidates();
    replay.release_sample_candidates();
    EXPECT_EQ(0, replay.get_sample_count());
    EXPECT_EQ(0, replay.sample_allocator_.total());
    EXPECT_EQ(512, replay.get_row_count());
    uint64_t vid = 0;
    ASSERT_EQ(OB_SUCCESS, replay.get_captured_vid(511, vid));
    EXPECT_EQ(511, vid);
    const float *values = nullptr;
    ASSERT_EQ(OB_SUCCESS, replay.get_row_at(511, vid, values));
    ASSERT_NE(nullptr, values);
    EXPECT_EQ(511, vid);
    EXPECT_FLOAT_EQ(511.0F, values[0]);
    EXPECT_FLOAT_EQ(511.5F, values[1]);
  }
}

TEST_F(TestRADatumStoreReader, merge_input_release_waits_for_success_and_returns_quota)
{
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode); // resident, spilled, empty
    ObIVFReplayMemoryMgr budget;
    ObArenaAllocator arena("IVFMergeEndUT");
    ObCompactionMemoryContext memory_context(arena);
    ObIVFMergeInputAdapter input(arena, &memory_context);
    ASSERT_EQ(OB_SUCCESS, budget.reserve(64L << 20, 1, input.replay_memory_reservation_));
    auto *replay = OB_NEWx(ObIVFReplayInput, &arena, arena);
    ASSERT_NE(nullptr, replay);
    ASSERT_EQ(OB_SUCCESS, input.replay_inputs_.push_back(replay));
    ASSERT_EQ(OB_SUCCESS, replay->init(MTL_ID(), 2, mode == 1 ? 1 : 64L << 20));
    for (int64_t row = 0; row < (mode == 2 ? 0 : 128); ++row) {
      const float values[] = {float(row), float(row) + 0.5F};
      ASSERT_EQ(OB_SUCCESS, replay->add_row(row,
          ObString(sizeof(values), reinterpret_cast<const char *>(values))));
      ASSERT_EQ(OB_SUCCESS, input.locators_.push_back(ObAsyncIVFSourceLocator(row, 0, 0, row)));
    }
    ASSERT_EQ(OB_SUCCESS, replay->seal());
    ASSERT_EQ(OB_SUCCESS, input.prior_.postings_.push_back(ObIVFPriorPosting(0, 0)));
    ObIVFMergeDagCtx dag;
    struct BorrowedInputGuard
    {
      ObIVFMergeDagCtx &dag_;
      ~BorrowedInputGuard() { dag_.input_adapter_ = nullptr; dag_.input_allocator_ = nullptr; }
    } input_guard{dag};
    dag.input_adapter_ = &input;
    dag.input_allocator_ = &arena;
    dag.release_finished_input_();
    EXPECT_EQ(1, input.replay_inputs_.count());
    EXPECT_EQ(1, budget.get_active_build_count());
    dag.build_finished_ = true;
    dag.release_finished_input_(); // monitoring/publication handoff not complete
    EXPECT_EQ(1, input.replay_inputs_.count());
    EXPECT_GT(arena.total(), 0);
    dag.build_perf_finished_ = true;
    dag.release_finished_input_();
    dag.release_finished_input_();
    EXPECT_TRUE(input.replay_inputs_.empty());
    EXPECT_TRUE(input.locators_.empty());
    EXPECT_TRUE(input.prior_.postings_.empty());
    EXPECT_EQ(0, input.prior_posting_allocator_.total());
    EXPECT_EQ(0, arena.total());
    EXPECT_EQ(0, budget.get_active_build_count());
    EXPECT_EQ(0, budget.get_reserved_bytes());
  }
}

TEST_F(TestRADatumStoreReader, completed_ddl_tablet_releases_input_and_quota_before_merge)
{
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode); // resident, spilled, empty
    ObIVFReplayMemoryMgr budget;
    ObArenaAllocator rotator_allocator("IVFReleaseUT");
    ObColumnClusteredDag owner;
    owner.is_inited_ = true;
    const ObTabletID tablet_id(1001);
    ASSERT_EQ(OB_SUCCESS, owner.tablet_context_map_.create(2, ObMemAttr(MTL_ID(), "IVFReleaseUT")));
    auto *tablet = OB_NEWx(ObDDLTabletContext, &owner.arena_);
    ASSERT_NE(nullptr, tablet);
    ASSERT_EQ(OB_SUCCESS, owner.tablet_context_map_.set_refactored(tablet_id, tablet));
    tablet->ivf_ctx_ = OB_NEW(ObIVFTabletContext, "IVFCtx");
    ASSERT_NE(nullptr, tablet->ivf_ctx_);
    auto &ivf = *tablet->ivf_ctx_;
    ASSERT_EQ(OB_SUCCESS, ivf.init_rotator(rotator_allocator, 2));
    ASSERT_EQ(OB_SUCCESS, budget.reserve(64L << 20, 1, ivf.replay_memory_reservation_));
    ivf.capture_slice_count_ = 1;
    auto *slice = OB_NEW(ObIVFSliceInput, "IVFSlice", 0);
    ASSERT_NE(nullptr, slice);
    ASSERT_EQ(OB_SUCCESS, ivf.register_slice(slice));
    ASSERT_EQ(OB_SUCCESS, slice->replay_.init(MTL_ID(), 2, mode == 1 ? 1 : 64L << 20,
        16, 123, 0, true));
    for (int64_t row = 0; row < (mode == 2 ? 0 : 128); ++row) {
      const float values[] = {float(row), float(row) + 0.25F};
      ASSERT_EQ(OB_SUCCESS, slice->replay_.add_row(row,
          ObString(sizeof(values), reinterpret_cast<const char *>(values))));
    }
    ASSERT_EQ(OB_SUCCESS, slice->replay_.seal());
    if (mode == 1) { ASSERT_GT(slice->replay_.get_file_size(), 0); }
    ivf.ivf_ddl_dag_ctx_ = OB_NEW(ObIVFDDLDagCtx, "IVFDDLCtx");
    ASSERT_NE(nullptr, ivf.ivf_ddl_dag_ctx_);
    auto &ctx = *ivf.ivf_ddl_dag_ctx_;
    ASSERT_EQ(OB_SUCCESS, ctx.init(owner, ObIVFProcessType::INITIAL_BUILD, tablet_id));
    void *memory = ctx.runtime_allocator_.alloc(sizeof(ObIVFDDLInput));
    ASSERT_NE(nullptr, memory);
    ctx.input_adapter_ = new (memory) ObIVFDDLInput(ctx.runtime_allocator_);
    ASSERT_EQ(OB_SUCCESS, ctx.input_adapter_->init(*tablet, 64, VIDA_L2));
    ctx.runtime_initialized_ = true;
    ctx.input_adapter_->release_capture_metadata();
    // Cancellation/incomplete publication must not free borrowed inputs.
    EXPECT_EQ(OB_STATE_NOT_MATCH, ctx.release_build_resources());
    EXPECT_EQ(1, ivf.slice_inputs_.count());
    EXPECT_EQ(1, budget.get_active_build_count());
    EXPECT_TRUE(ctx.runtime_initialized_);

    // Model the terminal handoff: metadata has been copied for physical merge.
    ivf.ivf_meta_.set_empty();
    ivf.is_built_ = true;
    ctx.build_finished_ = true;
    ctx.build_perf_finished_ = true;
    ASSERT_EQ(OB_SUCCESS, ctx.release_build_resources());
    EXPECT_EQ(nullptr, ctx.input_adapter_);
    EXPECT_EQ(nullptr, ctx.planner_);
    EXPECT_EQ(nullptr, ctx.dispatch_task_plan_);
    EXPECT_FALSE(ctx.runtime_initialized_);
    EXPECT_EQ(0, ctx.runtime_arena_.total());
    EXPECT_EQ(0, ivf.slice_inputs_.count());
    EXPECT_FALSE(ivf.replay_memory_reservation_.is_reserved());
    EXPECT_EQ(0, budget.get_reserved_bytes());
    EXPECT_EQ(0, budget.get_active_build_count());
    EXPECT_TRUE(ivf.is_built_);
    EXPECT_TRUE(ivf.ivf_meta_.is_valid());
    EXPECT_EQ(2, ivf.get_rotation_dim());
    EXPECT_EQ(&ctx, ivf.ivf_ddl_dag_ctx_);
    // The retained tablet/DagCtx shells and their later destructors are safe.
    EXPECT_EQ(OB_SUCCESS, ctx.release_build_resources());
    ivf.release_capture_inputs();
    EXPECT_EQ(0, budget.get_active_build_count());
  }
}

TEST_F(TestRADatumStoreReader, replay_incomplete_captured_vids_reject_append_and_seal)
{
  ObArenaAllocator allocator("ReplayVidUT");
  ObIVFReplayInput replay(allocator);
  const float input[] = {1.0F, 2.0F};
  const ObString bytes(sizeof(input), reinterpret_cast<const char *>(input));
  ASSERT_EQ(OB_SUCCESS, replay.init(MTL_ID(), 2, 1L << 20, 4, 123, 0, true));
  ASSERT_EQ(OB_SUCCESS, replay.add_row(17, bytes));
  ASSERT_EQ(1, replay.get_row_count());
  ASSERT_EQ(1, replay.get_sample_scan_count());
  // Simulate an append that persisted its payload but could not retain its VID.
  // No later append or seal may publish an incomplete locator input.
  replay.captured_vids_.reuse();
  EXPECT_EQ(OB_STATE_NOT_MATCH, replay.add_row(23, bytes));
  EXPECT_EQ(1, replay.get_row_count());
  EXPECT_EQ(1, replay.get_sample_scan_count());
  EXPECT_NE(OB_SUCCESS, replay.seal());
  EXPECT_FALSE(replay.is_sealed());
  uint64_t vid = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, replay.get_captured_vid(0, vid));
}

TEST_F(TestRADatumStoreReader, compact_sample_ranges_memory_and_spill)
{
  for (const int64_t budget : {1L, 64L * 1024 * 1024}) {
    ObArenaAllocator allocator("ClusterSampleUT");
    ObIVFReplayInput replay(allocator);
    ASSERT_EQ(OB_SUCCESS, replay.init(MTL_ID(), 3, budget));
    ObArray<int64_t> ids; ObArray<float> values;
    EXPECT_EQ(OB_STATE_NOT_MATCH, replay.copy_row_range(0, 1, ids, values));
    for (int64_t i = 0; i < 10001; ++i) {
      float payload[3] = {float(i), float(i + 1), float(i + 2)};
      ASSERT_EQ(OB_SUCCESS, replay.add_row(i * 7,
          ObString(sizeof(payload), reinterpret_cast<char *>(payload))));
    }
    ASSERT_EQ(OB_SUCCESS, replay.seal());
    EXPECT_EQ(budget == 1 ? 0 : 10001, replay.get_inmemory_row_count());
    const int64_t baseline = replay.get_mem_hold();
    ObTenantBase *tenant = ObTenantEnv::get_tenant();
    std::atomic<int64_t> errors(0);
    std::vector<std::thread> workers;
    for (int64_t worker = 0; worker < 4; ++worker) {
      workers.emplace_back([&, worker]() {
        ObTenantEnv::set_tenant(tenant); THIS_WORKER.set_timeout_ts(INT64_MAX);
        ObArray<int64_t> ids; ObArray<float> values;
        const int64_t begin = worker * 2000, end = begin + 2500;
        int ret = replay.copy_row_range(begin, end, ids, values);
        if (ret != OB_SUCCESS || ids.count() != end - begin || values.count() != 3 * ids.count()) {
          ++errors;
        } else {
          for (int64_t i = 0; i < ids.count(); ++i) {
            if (ids.at(i) != (begin + i) * 7 || values.at(i * 3) != float(begin + i)
                || values.at(i * 3 + 2) != float(begin + i + 2)) { ++errors; break; }
          }
        }
        ObTenantEnv::set_tenant(nullptr);
      });
    }
    for (auto &worker : workers) { worker.join(); }
    EXPECT_EQ(0, errors.load()); EXPECT_EQ(baseline, replay.get_mem_hold());
    EXPECT_EQ(OB_INVALID_ARGUMENT, replay.copy_row_range(-1, 3, ids, values));
    EXPECT_EQ(OB_INVALID_ARGUMENT, replay.copy_row_range(0, 10002, ids, values));
    EXPECT_EQ(OB_INVALID_ARGUMENT, replay.copy_row_range(3, 3, ids, values));
  }
}

TEST_F(TestRADatumStoreReader, replay_batches_read_spilled_rows_concurrently)
{
  const int64_t dim = 768;
  const int64_t row_count = 2048;
  const int64_t batch_count = 16;
  const uint64_t first_vid = 50000;
  ObArenaAllocator allocator("RAReplayUT");
  ObIVFReplayInput replay(allocator);
  ASSERT_EQ(OB_SUCCESS, replay.init(MTL_ID(), dim,
      ObIVFReplayInput::NO_CACHE_MEMORY_LIMIT, 16, 123, 7, true));
  float input[dim];
  for (int64_t row_id = 0; row_id < row_count; ++row_id) {
    for (int64_t d = 0; d < dim; ++d) {
      input[d] = static_cast<float>(row_id) + static_cast<float>(d) / 1024;
    }
    ASSERT_EQ(OB_SUCCESS, replay.add_row(first_vid + (row_id * 37) % row_count,
        ObString(sizeof(input), reinterpret_cast<char *>(input))));
  }
  ASSERT_EQ(OB_SUCCESS, replay.seal());
  ASSERT_EQ(0, replay.get_inmemory_row_count());
  ASSERT_GT(replay.get_file_size(), 0);
  const int64_t baseline = replay.get_mem_hold();
  ASSERT_FALSE(replay.reader_.buf_.is_inited());
  ASSERT_FALSE(replay.reader_.idx_buf_.is_inited());
  for (int64_t row_id = row_count - 1; row_id >= 0; --row_id) {
    uint64_t vid = 0;
    ASSERT_EQ(OB_SUCCESS, replay.get_captured_vid(row_id, vid));
    EXPECT_EQ(first_vid + (row_id * 37) % row_count, vid);
  }
  // Enumerating VIDs must not load either RA data blocks or index blocks.
  EXPECT_FALSE(replay.reader_.buf_.is_inited());
  EXPECT_FALSE(replay.reader_.idx_buf_.is_inited());
  EXPECT_EQ(baseline, replay.get_mem_hold());
  replay.release_captured_vids();
  EXPECT_EQ(0, replay.captured_vids_.count());
  EXPECT_EQ(0, replay.captured_vids_.get_capacity());
  uint64_t captured_vid = 0;
  EXPECT_EQ(OB_STATE_NOT_MATCH, replay.get_captured_vid(0, captured_vid));
  const ObIArray<ObIVFSampleCandidate> &samples = replay.get_sample_candidates();
  ASSERT_EQ(16, samples.count());
  ObSEArray<ObIVFReplayReadRequest, 16> sample_requests;
  ObSEArray<float, 16> sample_vectors;
  ASSERT_EQ(OB_SUCCESS, sample_vectors.prepare_allocate(samples.count() * dim));
  for (int64_t i = 0; i < samples.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sample_requests.push_back(ObIVFReplayReadRequest(
        samples.at(i).row_id_, samples.at(i).vid_, i)));
  }
  ASSERT_EQ(OB_SUCCESS, replay.batch_copy_rows(
      sample_requests, 0, sample_requests.count(), sample_vectors));
  for (int64_t i = 0; i < samples.count(); ++i) {
    for (int64_t d = 0; d < dim; ++d) {
      EXPECT_FLOAT_EQ(static_cast<float>(samples.at(i).row_id_)
          + static_cast<float>(d) / 1024, sample_vectors.at(i * dim + d));
    }
  }
  EXPECT_EQ(baseline, replay.get_mem_hold());
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  std::atomic<bool> start(false);
  std::atomic<int64_t> failures(0);
  std::vector<std::thread> workers;
  for (int64_t worker_id = 0; worker_id < 8; ++worker_id) {
    workers.emplace_back([&, worker_id]() {
      ObTenantEnv::set_tenant(tenant);
      THIS_WORKER.set_timeout_ts(INT64_MAX);
      while (!start.load()) {
        std::this_thread::yield();
      }
      for (int64_t round = 0; round < 64 && 0 == failures.load(); ++round) {
        ObSEArray<ObIVFReplayReadRequest, batch_count> requests;
        ObSEArray<float, 16> vectors;
        int ret = vectors.prepare_allocate(batch_count * dim);
        for (int64_t i = 0; OB_SUCCESS == ret && i < batch_count; ++i) {
          const int64_t row_id = 0 == i % 4 ? 0 :
              (worker_id * 31 + round * 97 + i * 113) % row_count;
          ret = requests.push_back(ObIVFReplayReadRequest(
              row_id, first_vid + (row_id * 37) % row_count, batch_count - 1 - i));
        }
        if (OB_SUCCESS == ret) {
          ret = replay.batch_copy_rows(requests, 0, batch_count, vectors);
        }
        for (int64_t i = 0; OB_SUCCESS == ret && i < batch_count; ++i) {
          for (int64_t d = 0; OB_SUCCESS == ret && d < dim; ++d) {
            const float expected = static_cast<float>(requests.at(i).row_id_)
                + static_cast<float>(d) / 1024;
            if (expected != vectors.at(requests.at(i).output_idx_ * dim + d)) {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        }
        if (OB_SUCCESS != ret) {
          failures.fetch_add(1);
        }
      }
      ObTenantEnv::set_tenant(nullptr);
    });
  }
  start.store(true);
  for (std::thread &worker : workers) {
    worker.join();
  }
  EXPECT_EQ(0, failures.load());
  EXPECT_EQ(baseline, replay.get_mem_hold());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase;
  system("rm -f test_ivf_sstable.log*");
  OB_LOGGER.set_file_name("test_ivf_sstable.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  // Production capture reserves replay quota through this tenant module.
  MTL_BIND2(mtl_new_default,
      oceanbase::share::ObIVFReplayMemoryMgr::mtl_init,
      nullptr, nullptr, nullptr, mtl_destroy_default);
  if (OB_SUCCESS != oceanbase::MockTenantModuleEnv::get_instance().init()) {
    return 1;
  }
#ifndef OB_USE_ASAN
  // One snapshot producer for all memory assertions in this test process.
  auto &memory_dump = oceanbase::common::ObMemoryDump::get_instance();
  if (OB_SUCCESS != memory_dump.init()) { return 1; }
  memory_dump.stop();
  memory_dump.wait();
#endif
  const int ret = RUN_ALL_TESTS();
#ifndef OB_USE_ASAN
  memory_dump.destroy();
#endif
  oceanbase::MockTenantModuleEnv::get_instance().destroy();
  // Skip remaining static destructors. MockTenantModuleEnv teardown cannot
  // stop every background thread before ObServer destroys OB_LOGGER, which
  // otherwise floods stderr with alloc_log_item / stop_append_log errors.
  std::_Exit(ret);
}
