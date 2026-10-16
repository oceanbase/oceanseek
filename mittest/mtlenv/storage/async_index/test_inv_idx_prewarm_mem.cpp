/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST

#include "mtlenv/storage/blocksstable/test_inv_idx_sstable_prepare.h"
#include "share/cache/ob_kvcache_pre_warmer.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;
using namespace unittest;

namespace storage
{

// Tier-2 coverage for the async inverted-index cache pre-warm (non-SS, MEM_PRE_WARM):
//   - build_pre_warmer selection: the n-1 index side is auto-upgraded to MEM_PRE_WARM for a user
//     tablet even when PRE_WARM_TYPE_NONE is specified.
//   - end-to-end insertion: after close, the n-1 index micro block lands in the index block cache,
//     keyed by the real pre-allocated macro id + on-disk offset + compressed total size.
class ObInvIdxPrewarmMemTest : public TestInvIdxSSTablePrepare
{
};

// 1. PRE_WARM_TYPE_NONE on a user tablet: the n-1 index side is auto-upgraded to MEM_PRE_WARM,
//    mirroring the general rule in ObMacroBlockWriter::build_pre_warmer
//    ("user tablet & index macro -> mem pre warm"). The fixture's TABLET_ID (200001) is a user
//    tablet (> OB_MAX_INNER_TABLE_ID), so the upgrade fires.
TEST_F(ObInvIdxPrewarmMemTest, pre_warm_type_none_user_tablet)
{
  prepare_inv_idx_schema(table_schema_, true /*has_pos_list*/);
  OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
  ASSERT_TRUE(inv_idx_row_desc_.is_valid());
  prepare_desc();
  ASSERT_TRUE(whole_desc_.get_desc().get_tablet_id().is_user_tablet());

  ObInvIdxMacroBlockWriter writer;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObSSTablePrivateObjectCleaner cleaner;
  OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
      seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());
  EXPECT_NE(nullptr, writer.index_block_builder_.index_pre_warmer_);
  OK(writer.close());
}

// 2. MEM_PRE_WARM: the n-1 index pre-warmer is created and inserts the index micro block into cache.
TEST_F(ObInvIdxPrewarmMemTest, mem_pre_warm_inserts_index_into_cache)
{
  prepare_inv_idx_schema(table_schema_, true /*has_pos_list*/);
  OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
  ASSERT_TRUE(inv_idx_row_desc_.is_valid());
  prepare_desc();

  ObInvIdxMacroBlockWriter writer;
  ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
  pre_warm_param.fixed_percentage_ = 100;  // deterministic: always reserve
  ObSSTablePrivateObjectCleaner cleaner;
  OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
      seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());

  // selection: concrete pre-warmer type and its cache target
  ASSERT_NE(nullptr, writer.index_block_builder_.index_pre_warmer_);
  ObIndexBlockCachePreWarmer *idx_pw = dynamic_cast<ObIndexBlockCachePreWarmer *>(writer.index_block_builder_.index_pre_warmer_);
  ASSERT_NE(nullptr, idx_pw);
  EXPECT_EQ(static_cast<ObIMicroBlockCache *>(&OB_STORE_CACHE.get_index_block_cache()), idx_pw->cache_);

  // Feed enough rows to flush several data micro blocks. close() flushes the partial macro block,
  // including the single n-1 index micro block, and triggers index pre-warm.
  ObInvIdxRowFeeder feeder(inv_idx_row_desc_, mv_col_cnt_);
  OK(feeder.feed_n(writer, 5000));
  OK(writer.close());

  // close() hands the data macro ids off to the index tree root's data_write_ctx_
  // (ObMacroBlocksWriteCtx::set moves and clears the source), so writer.get_macro_block_write_ctx()
  // is empty here by design — read the flushed macro id from the move destination instead.
  ObMacroBlocksWriteCtx *data_write_ctx = writer.index_block_builder_.index_tree_root_ctx_->data_write_ctx_;
  ASSERT_NE(nullptr, data_write_ctx);
  ASSERT_FALSE(data_write_ctx->is_empty());
  const MacroBlockId macro_id = data_write_ctx->get_macro_block_list().at(0);
  ASSERT_TRUE(macro_id.is_valid());

  // (a) n-1 index micro block hits the index block cache, keyed by the real macro id +
  //     index_block_offset_ / index_block_size_ (compressed total size = buf_size_ + header_size).
  ObMicroBlockCacheKey idx_key;
  idx_key.set(MTL_ID(), macro_id,
              writer.index_block_builder_.index_block_offset_,
              writer.index_block_builder_.index_block_size_);
  ObMicroBlockBufferHandle idx_handle;
  EXPECT_EQ(OB_SUCCESS,
            OB_STORE_CACHE.get_index_block_cache().get_cache_block(idx_key, idx_handle));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_prewarm_mem.log*");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
