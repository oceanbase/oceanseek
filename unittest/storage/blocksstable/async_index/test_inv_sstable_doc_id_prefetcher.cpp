/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "mtlenv/mock_tenant_module_env.h"

#define private public
#include "storage/blocksstable/async_index/ob_inv_sstable_doc_id_prefetcher.h"
#undef private

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace blocksstable
{

namespace
{
void prepare_slot(
    const MacroBlockId &macro_id,
    const int32_t block_offset,
    const int64_t first_block_idx,
    const int64_t last_block_idx,
    ObInvSSTableDocIdPrefetcher::Slot &slot)
{
  slot.data_handle_.tenant_id_ = MTL_ID();
  slot.data_handle_.macro_block_id_ = macro_id;
  slot.data_handle_.micro_info_.set(
      block_offset, 4096, ObLogicMicroBlockId(), 0);
  slot.first_block_idx_ = first_block_idx;
  slot.last_block_idx_ = last_block_idx;
}
} // namespace

class TestInvSSTableDocIdPrefetcher : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
};

TEST_F(TestInvSSTableDocIdPrefetcher, five_slots_transfer_handles_in_fifo_order_across_wrap)
{
  const MacroBlockId macro_id(1, 1, 0);
  ObInvSSTableDocIdPrefetcher prefetcher;
  EXPECT_TRUE(prefetcher.is_empty());

  prefetcher.fetch_idx_ = 3;
  prefetcher.prefetch_idx_ = 8;
  for (int64_t seq = 3; seq < 8; ++seq) {
    prepare_slot(
        macro_id, static_cast<int32_t>(128 + seq), seq * 10, seq * 10 + 9,
        prefetcher.slots_[seq % prefetcher.MAX_PREFETCH_DEPTH]);
  }
  EXPECT_TRUE(prefetcher.is_full());

  for (int64_t seq = 3; seq < 8; ++seq) {
    storage::ObMicroBlockDataHandle data_handle;
    ObInvSSTableDocIdPrefetcher::Slot &slot = prefetcher.slots_[seq % prefetcher.MAX_PREFETCH_DEPTH];
    EXPECT_TRUE(slot.data_handle_.match(macro_id, 128 + seq, 4096));
    EXPECT_EQ(seq, prefetcher.fetch_idx_);
    ASSERT_EQ(OB_SUCCESS, prefetcher.take_front_handle(data_handle));
    EXPECT_TRUE(data_handle.match(macro_id, 128 + seq, 4096));
  }
  EXPECT_TRUE(prefetcher.is_empty());
  ObInvSSTableDocIdPrefetcher::FrontView front;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, prefetcher.peek_front(front));
  EXPECT_FALSE(front.is_valid());
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
