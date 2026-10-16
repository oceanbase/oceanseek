/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <signal.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/checksum/ob_crc64.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"

// private/public 宏必须出现在声明被测类的头文件之前，以便测试代码直接驱动
// 无 IO 的解析入口（init_from_macro_buf_）并检查私有分帧状态（data_buf_、
// posting_entries_）。单测环境无法执行真正的宏块 IO（close() 对携带数据的
// block 会失败），因此带索引的读路径（init_from_macro_buf_ + next）已在
// test_inv_idx_macro_block_writer.cpp 的 path_b 用例中通过 writer 的内存宏块
// 缓冲区覆盖。本文件单独覆盖 prescan_region_ 分帧层：它喂入手工构造的
// [ObMicroBlockHeader][payload] 数据区域，断言 prescan 接受/拒绝的情况。
#ifndef private
#define private public
#endif
#ifndef protected
#define protected public
#endif

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

// =============================================================================
// ObInvIndexBlockMicroIterator — prescan_region_ 分帧单元测试
//
// prescan_region_ 遍历宏块数据区域中每个以 ObMicroBlockHeader 分帧的微块，
// 校验分帧格式，并记录 posting data block（跳过 posting value block）。
// 本测试用通用物理头包装任意 payload 字节，直接驱动 prescan_region_（仅数据区域，
// 无 common/macro header 包装、无 term-index），断言：
//   L1  posting data block 按序记录；posting value block 跳过
//   L3  非法缓冲区被拒绝（null / 空 / 截断 / 校验和错误 / 越界 /
//        无 posting data）
// =============================================================================

class TestInvIndexBlockMicroIterator : public ::testing::Test
{
public:
  TestInvIndexBlockMicroIterator() : allocator_(ObModIds::TEST) {}
  virtual ~TestInvIndexBlockMicroIterator() {}

protected:
  struct MicroSpec
  {
    MicroSpec()
      : is_value_(false), payload_(nullptr), payload_size_(0),
        corrupt_checksum_(false), set_compressed_(false) {}
    MicroSpec(const bool is_value, const char *payload, const int64_t size)
      : is_value_(is_value), payload_(payload), payload_size_(size),
        corrupt_checksum_(false), set_compressed_(false) {}
    TO_STRING_KV(K_(is_value), K_(payload_size));
    // 倒排微块类型由 ObMicroBlockHeader 的 inverted-index flag/type 表达；
    // rowkey_column_count_ 只作为一致性校验。
    bool is_value_;
    const char *payload_;
    int64_t payload_size_;
    bool corrupt_checksum_;
    bool set_compressed_;
  };

  // 将一系列 [ObMicroBlockHeader][payload] 微块拼接为单个倒排数据微块区域。
  // 缓冲区在 allocator_ 中分配。
  int build_region(const ObIArray<MicroSpec> &specs, const char *&region, int64_t &region_size)
  {
    int ret = OB_SUCCESS;
    // 所有 spec 用相同的 column_count(1)/无列校验和，header 序列化大小恒定。
    ObMicroBlockHeader tmpl_header;
    if (OB_FAIL(tmpl_header.init(0 /*data_version unused by init*/, 1, 0,
            common::INV_IDX_ROW_STORE, false /*has_column_checksum*/))) {
    } else {
      const int64_t header_size = tmpl_header.get_serialize_size();
      int64_t total = 0;
      for (int64_t i = 0; i < specs.count(); ++i) {
        total += header_size + specs.at(i).payload_size_;
      }
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(total)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t pos = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < specs.count(); ++i) {
          const MicroSpec &spec = specs.at(i);
          // Both posting and posting value micro blocks carry no rowkey column (payload opaque),
          // so the rowkey_column_count fed to the test header is uniformly 0 to match the read-side
          // guard in ObInvIndexBlockMicroIterator.
          const int64_t rowkey_cnt = 0;
          ObMicroBlockHeader header;
          int64_t hdr_pos = 0;
          if (OB_FAIL(header.init(0 /*data_version unused*/, 1, rowkey_cnt,
                  common::INV_IDX_ROW_STORE, false))) {
          } else {
            header.inverted_index_type_ = static_cast<uint8_t>(spec.is_value_
                ? MICRO_BLOCK_INV_IDX_POSTING_VALUE
                : MICRO_BLOCK_INV_IDX_POSTING);
            header.data_length_ = spec.set_compressed_
                ? static_cast<int32_t>(spec.payload_size_ + 1)
                : static_cast<int32_t>(spec.payload_size_);
            header.data_zlength_ = static_cast<int32_t>(spec.payload_size_);
            header.original_length_ = static_cast<int32_t>(spec.payload_size_);
            const uint64_t real_checksum = ob_crc64_sse42(0, spec.payload_, spec.payload_size_);
            header.data_checksum_ = spec.corrupt_checksum_ ? (real_checksum ^ 0xFFFFFFFFULL) : real_checksum;
            header.set_header_checksum();
            if (OB_FAIL(header.serialize(buf + pos, total - pos, hdr_pos))) {
            } else {
              pos += hdr_pos;
              MEMCPY(buf + pos, spec.payload_, spec.payload_size_);
              pos += spec.payload_size_;
            }
          }
        }
        if (OB_SUCC(ret)) {
          region = buf;
          region_size = total;
        }
      }
    }
    return ret;
  }

  // 直接将原始数据区域写入迭代器的私有 data_buf_/data_size_（无 common/macro
  // header 包装），驱动 prescan_region_。
  int prescan_region(ObInvIndexBlockMicroIterator &iter,
                     const char *region, const int64_t region_size)
  {
    iter.data_buf_ = region;
    iter.data_size_ = region_size;
    iter.posting_entries_.reset();
    return iter.prescan_region_(0 /*region_macro_base*/);
  }

protected:
  ObArenaAllocator allocator_;
};

TEST_F(TestInvIndexBlockMicroIterator, allocators_belong_to_current_tenant)
{
  static const uint64_t TEST_TENANT_ID = 1002;
  lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
  ASSERT_NE(nullptr, malloc_allocator);
  // 同二进制其它 case 若已创建过该租户 allocator，会返回 OB_ENTRY_EXIST，容忍之。
  int create_ret = malloc_allocator->create_and_add_tenant_allocator(TEST_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == create_ret || OB_ENTRY_EXIST == create_ret);
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(TEST_TENANT_ID, 1L << 30));

  share::ObTenantBase tenant_base(TEST_TENANT_ID);
  share::ObTenantEnv::set_tenant(&tenant_base);
  {
    ObInvIndexBlockMicroIterator iter;
    EXPECT_EQ(TEST_TENANT_ID, iter.posting_entries_.get_block_allocator().get_attr().tenant_id_);
    EXPECT_EQ(TEST_TENANT_ID, iter.value_entries_.get_block_allocator().get_attr().tenant_id_);
    EXPECT_EQ(TEST_TENANT_ID, iter.index_alloc_.get_arena().get_tenant_id());
    EXPECT_EQ(TEST_TENANT_ID, iter.index_entries_.get_block_allocator().get_attr().tenant_id_);

    ObInvIndexBlockMicroIterator::IndexEntry *entry = iter.index_entries_.alloc_place_holder();
    EXPECT_NE(nullptr, entry);
    if (nullptr != entry) {
      // ObSEArray 没有公开 allocator getter；推入第 5 个元素，覆盖使用显式
      // 传入的当前租户 allocator 越过内联缓冲后的分配路径。
      ObInvIdxLogicalBlockMeta meta;
      for (int64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(OB_SUCCESS, entry->metas_.push_back(meta));
      }
      EXPECT_EQ(5, entry->metas_.count());
    }
  }
  share::ObTenantEnv::set_tenant(nullptr);
}

// =============================================================================
// L1. prescan 按序记录 posting data block，跳过 posting value block
// =============================================================================

TEST_F(TestInvIndexBlockMicroIterator, prescan_records_posting_data_and_skips_value)
{
  const char pd0[] = "posting0";
  const char pv0[] = "value0";
  const char pd1[] = "posting-block-1";
  const char pv1[] = "v1";
  const char pd2[] = "pd2";
  const char pv2[] = "value-block-2";
  ObArray<MicroSpec> specs;
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(false /*is_value*/, pd0, 8)));
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(true /*is_value*/, pv0, 6)));
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(false /*is_value*/, pd1, 15)));
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(true /*is_value*/, pv1, 2)));
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(false /*is_value*/, pd2, 3)));
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(true /*is_value*/, pv2, 13)));

  const char *region = nullptr;
  int64_t region_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_region(specs, region, region_size));

  ObInvIndexBlockMicroIterator iter;
  ASSERT_EQ(OB_SUCCESS, prescan_region(iter, region, region_size));
  EXPECT_EQ(3, iter.posting_entries_.count());  // 仅 posting data block 被记录

  const char *expect_payload[] = {pd0, pd1, pd2};
  const int64_t expect_size[] = {8, 15, 3};
  for (int64_t i = 0; i < 3; ++i) {
    const auto &entry = iter.posting_entries_.at(i);
    EXPECT_EQ(expect_size[i], entry.payload_size_);
    EXPECT_EQ(0, MEMCMP(entry.payload_buf_, expect_payload[i], expect_size[i])) << "entry " << i;
    // macro_offset 是每个 block 的 header 起始偏移（每 block 的 header_size + 前面的 payload 之和）
    EXPECT_GE(entry.macro_offset_, 0);
  }
}

// =============================================================================
// L3. prescan 拒绝非法输入缓冲区
// =============================================================================

TEST_F(TestInvIndexBlockMicroIterator, prescan_handles_null_and_empty)
{
  // prescan_region_ 本身不做 data_buf_ 的 null/empty 检查（那是调用方的职责
  // ——init_from_macro_buf_ 校验 data/index 布局）。非空但短于一个 header 的
  // 尺寸被当作截断拒绝；空区域则产出零个 posting block。
  ObInvIndexBlockMicroIterator iter;
  const char dummy[] = "x";
  // nullptr 直接交给 ObMicroBlockHeader::deserialize_and_check_header，后者以
  // OB_INVALID_ARGUMENT 拒绝。
  EXPECT_EQ(OB_INVALID_ARGUMENT, prescan_region(iter, nullptr, 16));
  EXPECT_EQ(OB_SUCCESS, prescan_region(iter, dummy, 0));            // 空 -> 0 条目
  EXPECT_EQ(0, iter.posting_entries_.count());
}

TEST_F(TestInvIndexBlockMicroIterator, prescan_rejects_truncated_header)
{
  // 短于一个物理 header 的区域：反序列化失败，任何非成功错误码均可接受。
  char buf[4] = {0};
  ObInvIndexBlockMicroIterator iter;
  EXPECT_NE(OB_SUCCESS, prescan_region(iter, buf, sizeof(buf)));
}

TEST_F(TestInvIndexBlockMicroIterator, prescan_rejects_bad_checksum)
{
  // 通用 ObMicroBlockHeader 不再携带独立 version/type 字段；header 完整性由
  // header_checksum 保证，payload 完整性由 data_checksum_ 保证。这里损坏 payload
  // 校验和，使 materialize_payload_ 中的 check_payload_checksum 失败。
  const char pd[] = "payload";
  MicroSpec spec(false /*is_value*/, pd, 7);
  spec.corrupt_checksum_ = true;
  ObArray<MicroSpec> specs;
  ASSERT_EQ(OB_SUCCESS, specs.push_back(spec));
  const char *region = nullptr;
  int64_t region_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_region(specs, region, region_size));

  ObInvIndexBlockMicroIterator iter;
  EXPECT_EQ(OB_PHYSIC_CHECKSUM_ERROR, prescan_region(iter, region, region_size));
}

TEST_F(TestInvIndexBlockMicroIterator, prescan_rejects_compressed)
{
  const char pd[] = "payload";
  MicroSpec spec(false /*is_value*/, pd, 7);
  spec.set_compressed_ = true;
  ObArray<MicroSpec> specs;
  ASSERT_EQ(OB_SUCCESS, specs.push_back(spec));
  const char *region = nullptr;
  int64_t region_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_region(specs, region, region_size));

  ObInvIndexBlockMicroIterator iter;
  // data_length_ != data_zlength_ 标记此块为压缩态（is_compressed_data() == true）。
  // prescan_region_ 随后调 materialize_payload_ 解压；该 iter 未经 init_from_macro_buf_，
  // 成员 compressor_type_ 保持 INVALID_COMPRESSOR，decompress_data_with_prealloc_buf 在
  // 压缩器查找时失败并返回 OB_NOT_SUPPORTED。
  EXPECT_EQ(OB_NOT_SUPPORTED, prescan_region(iter, region, region_size));
}

TEST_F(TestInvIndexBlockMicroIterator, prescan_rejects_payload_overrun)
{
  // 手工构造一个 header，其 data_zlength_ 超出区域末尾：prescan_region_ 的越界
  // 检查在 materialize（校验和）之前执行，故以 OB_ERR_UNEXPECTED 拒绝。
  ObMicroBlockHeader header;
  int64_t hdr_pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.init(0 /*data_version unused*/, 1, 0,
      common::INV_IDX_ROW_STORE, false));
  const int64_t header_size = header.get_serialize_size();
  const int64_t region_size = header_size + 4;
  char *region = static_cast<char *>(allocator_.alloc(region_size));
  ASSERT_NE(nullptr, region);
  header.data_length_ = 1000;   // 远超末尾 4 字节
  header.data_zlength_ = 1000;
  header.original_length_ = 1000;
  header.data_checksum_ = 0;
  header.inverted_index_type_ = static_cast<uint8_t>(MICRO_BLOCK_INV_IDX_POSTING);
  header.set_header_checksum();
  ASSERT_EQ(OB_SUCCESS, header.serialize(region, region_size, hdr_pos));
  ASSERT_EQ(header_size, hdr_pos);

  ObInvIndexBlockMicroIterator iter;
  EXPECT_EQ(OB_ERR_UNEXPECTED, prescan_region(iter, region, region_size));
}

TEST_F(TestInvIndexBlockMicroIterator, prescan_rejects_region_without_posting_data)
{
  // 仅包含一个 posting value 微块的区域
  const char pv[] = "value-only";
  ObArray<MicroSpec> specs;
  ASSERT_EQ(OB_SUCCESS, specs.push_back(MicroSpec(true /*is_value*/, pv, 10)));
  const char *region = nullptr;
  int64_t region_size = 0;
  ASSERT_EQ(OB_SUCCESS, build_region(specs, region, region_size));

  ObInvIndexBlockMicroIterator iter;
  // prescan 本身成功（posting value 是合法分帧 block，只是被跳过），
  // 但 posting_entries_ 为空——调用方（init_from_macro_buf_）会将其视为
  // OB_ERR_UNEXPECTED。这里直接断言空结果。
  EXPECT_EQ(OB_SUCCESS, prescan_region(iter, region, region_size));
  EXPECT_EQ(0, iter.posting_entries_.count());
}

// =============================================================================
// L3. 未 init：游标/边界查询在 init 前安全
// =============================================================================

TEST_F(TestInvIndexBlockMicroIterator, not_inited_state)
{
  ObInvIndexBlockMicroIterator iter;
  EXPECT_FALSE(iter.is_left_border());
  EXPECT_FALSE(iter.is_right_border());
  const ObMicroBlock *mb = nullptr;
  const ObIArray<ObInvIdxLogicalBlockMeta> *metas = nullptr;
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_term_entry(mb, metas));
  EXPECT_EQ(nullptr, mb);
  EXPECT_EQ(nullptr, metas);
}

} // end namespace blocksstable
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_micro_block_iterator.log*");
  OB_LOGGER.set_file_name("test_inv_idx_micro_block_iterator.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
