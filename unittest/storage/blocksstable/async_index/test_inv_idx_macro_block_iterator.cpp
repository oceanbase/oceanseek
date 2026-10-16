/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
// unittest/storage/blocksstable/async_index/test_inv_idx_macro_block_iterator.cpp
//
// ObInvIndexBlockMacroIterator（倒排索引宏块迭代器）及其产出的 ObMacroBlockDesc
// 描述符的测试。
//
// 测试范围说明
// -------------
// 端到端多宏块迭代——对真实携带数据的 inv sstable 执行 open() 并通过
// ObIndexBlockTreeCursor 在 MACRO 深度遍历索引树——需要一个完整物化的
// ObInvSSTable，其宏块可通过 IO 层读取。mtlenv 单测环境无法提供此条件：
// ObInvIdxMacroBlockWriter 在内存中构建宏块，但其 close() 提交真实 IO，
// 对携带数据的 block 会失败（参见 test_inv_idx_macro_block_writer.cpp:557-562），
// 因此这里无法组装出可扫描的 inv sstable。同目录下的 writer/micro-iterator 测试
// 也受限于此，只能直接读取内存缓冲区。
//
// 本测试因此覆盖无需 IO 即可测试的部分：
//   * ObMacroBlockDesc：reset / reuse / is_valid / 字段保持。
//   * ObInvIndexBlockMacroIterator：参数校验、init-twice 守卫，以及
//     空 sstable 提前退出路径（no_data_to_read() => 立即 is_iter_end，
//     get_next_macro_block 在不触碰索引树的情况下返回 OB_ITER_END）。
//   * ObIndexBlockMacroIterator：直接调用也不能绕过 INV 的 reverse/record
//     能力边界，且参数合法性检查保持优先。
//
// 完整的多宏块迭代（范围裁剪、反向、跨宏块 move_forward、范围连续性）将在
// 有可扫描 inv sstable fixture 后通过集成测试验证（参照 MR 458 的
// mittest/test_sstable_posting_scanner 方式），并在 ObInvSSTableMergeIter
// 消费此迭代器时再次验证。

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>

#define private public
#define protected public

#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/access/ob_table_read_info.h"
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

class TestInvIdxMacroBlockIterator : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // init_for_empty_major_sstable() 会访问 GET_MIN_DATA_VERSION(MTL_ID())，
    // 需要租户上下文和可解析的 data version（与 test_inv_sstable.cpp 相同的设置）。
    static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
    ObTenantEnv::set_tenant(&tenant_ctx);
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
    }
    return ret;
  }

  int build_empty_inv_sstable(ObInvSSTable &sstable)
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

  ObArenaAllocator allocator_;
};

// =============================================================================
// ObMacroBlockDesc
// =============================================================================
TEST_F(TestInvIdxMacroBlockIterator, desc_default_is_invalid)
{
  ObMacroBlockDesc desc;
  // reset()（由构造函数调用）之后，macro_block_id_ 和 range_ 无效。
  EXPECT_FALSE(desc.is_valid());
  EXPECT_EQ(0, desc.start_row_offset_);
  EXPECT_EQ(0, desc.schema_version_);
  EXPECT_EQ(0, desc.row_count_);
}

TEST_F(TestInvIdxMacroBlockIterator, desc_reset_clears_fields)
{
  ObMacroBlockDesc desc;
  desc.start_row_offset_ = 42;
  desc.schema_version_ = 5;
  desc.row_count_ = 99;
  desc.row_store_type_ = FLAT_ROW_STORE;
  desc.reset();
  EXPECT_FALSE(desc.is_valid());
  EXPECT_EQ(0, desc.start_row_offset_);
  EXPECT_EQ(0, desc.schema_version_);
  EXPECT_EQ(0, desc.row_count_);
}

TEST_F(TestInvIdxMacroBlockIterator, desc_reuse_equals_reset)
{
  ObMacroBlockDesc desc;
  desc.row_count_ = 123;
  desc.start_row_offset_ = 7;
  desc.reuse();
  EXPECT_FALSE(desc.is_valid());
  EXPECT_EQ(0, desc.row_count_);
  EXPECT_EQ(0, desc.start_row_offset_);
}

// =============================================================================
// ObInvIndexBlockMacroIterator — argument validation & lifecycle
// =============================================================================
TEST_F(TestInvIdxMacroBlockIterator, open_invalid_range_rejected)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_empty_inv_sstable(sstable));
  ASSERT_TRUE(sstable.is_valid());

  ObInvIndexBlockMacroIterator iter;
  ObReadInfoStruct read_info;
  ObDatumRange range;  // 默认构造 => 无效
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(sstable, range, read_info, allocator_));
  EXPECT_FALSE(iter.is_iter_end());
}

TEST_F(TestInvIdxMacroBlockIterator, open_init_twice_rejected)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_empty_inv_sstable(sstable));

  ObInvIndexBlockMacroIterator iter;
  ObReadInfoStruct read_info;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, iter.open(sstable, range, read_info, allocator_));
  // 第二次 open 必须失败并返回 OB_INIT_TWICE（未 reset 的情况下）。
  EXPECT_EQ(OB_INIT_TWICE, iter.open(sstable, range, read_info, allocator_));
  iter.reset();
}

TEST_F(TestInvIdxMacroBlockIterator, generic_iterator_rejects_reverse_scan_but_allows_record_micro_info)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_empty_inv_sstable(sstable));

  ObReadInfoStruct read_info;
  ObDatumRange whole_range;
  whole_range.set_whole_range();

  // need_record_micro_info=true 现已被允许：clustered index 整块复用需要靠它
  // 取回 get_current_clustered_index_info()。是否真的记录取决于树类型（非
  // clustered 树在 locate_macro_block() 之后静默降级为不记录），open() 本身
  // 不再因此拒绝。空 sstable 走 no_data_to_read() 提前退出，树类型无从判断，
  // 直接返回 OB_SUCCESS 且立即 is_iter_end()。
  ObIndexBlockMacroIterator record_iter;
  EXPECT_EQ(OB_SUCCESS, record_iter.open(
      sstable, whole_range, read_info, allocator_,
      false /* is_reverse */, true /* need_record_micro_info */));
  EXPECT_TRUE(record_iter.is_iter_end());

  // is_reverse 仍是唯一在 open() 层就被无条件拒绝的组合，无论
  // need_record_micro_info 取值。
  ObIndexBlockMacroIterator reverse_iter;
  EXPECT_EQ(OB_NOT_SUPPORTED, reverse_iter.open(
      sstable, whole_range, read_info, allocator_,
      true /* is_reverse */, false /* need_record_micro_info */));

  ObIndexBlockMacroIterator reverse_record_iter;
  EXPECT_EQ(OB_NOT_SUPPORTED, reverse_record_iter.open(
      sstable, whole_range, read_info, allocator_,
      true /* is_reverse */, true /* need_record_micro_info */));
}

TEST_F(TestInvIdxMacroBlockIterator, generic_iterator_validates_arguments_before_inv_capability)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_empty_inv_sstable(sstable));

  ObIndexBlockMacroIterator iter;
  ObReadInfoStruct read_info;
  ObDatumRange invalid_range;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.open(
      sstable, invalid_range, read_info, allocator_,
      true /* is_reverse */, true /* need_record_micro_info */));
}

// =============================================================================
// ObInvIndexBlockMacroIterator — 空 sstable 提前退出路径
//
// 空的 inv sstable（无数据宏块）报告 no_data_to_read()==true，因此 open()
// 必须短路：is_iter_end() 立刻变为 true，首次 get_next_macro_block() 返回
// OB_ITER_END，且从不初始化树游标（树游标需要有效的 root_block_info，而
// 空 sstable 没有）。
// =============================================================================
TEST_F(TestInvIdxMacroBlockIterator, empty_sstable_early_exit)
{
  ObInvSSTable sstable;
  ASSERT_EQ(OB_SUCCESS, build_empty_inv_sstable(sstable));
  ASSERT_TRUE(sstable.is_valid());
  ASSERT_TRUE(sstable.no_data_to_read())
      << "empty inv sstable must report no_data_to_read";

  ObInvIndexBlockMacroIterator iter;
  ObReadInfoStruct read_info;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, iter.open(sstable, range, read_info, allocator_));
  EXPECT_TRUE(iter.is_iter_end());

  ObMacroBlockDesc desc;
  // 重复 get_next 必须持续返回 OB_ITER_END，不崩溃，也不把 is_iter_end 翻回 false。
  EXPECT_EQ(OB_ITER_END, iter.get_next_macro_block(desc));
  EXPECT_EQ(OB_ITER_END, iter.get_next_macro_block(desc));
  EXPECT_TRUE(iter.is_iter_end());
  EXPECT_FALSE(desc.is_valid());

  iter.reset();
  EXPECT_FALSE(iter.is_iter_end());
}

// open 之前调用 get_next_macro_block 必须返回 OB_NOT_INIT。
TEST_F(TestInvIdxMacroBlockIterator, get_next_before_open_not_init)
{
  ObInvIndexBlockMacroIterator iter;
  ObMacroBlockDesc desc;
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_macro_block(desc));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_macro_block_iterator.log*");
  OB_LOGGER.set_file_name("test_inv_idx_macro_block_iterator.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
