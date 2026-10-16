/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include "storage/compaction/ob_compaction_trans_cache.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace share;
using namespace transaction;

namespace unittest
{

TEST(TestCompactionTransCache, cache_trans_state_with_end_scn)
{
  ObArenaAllocator allocator;
  ObCachedTransStateMgr mgr(allocator);
  ObMergeCachedTransState trans_state;
  SCN trans_version;
  SCN end_scn;

  trans_version.convert_from_ts(100);
  end_scn.convert_from_ts(120);

  ASSERT_EQ(OB_SUCCESS, mgr.init(16));
  ASSERT_EQ(OB_SUCCESS, mgr.add_trans_state(ObTransID(1),
                                            ObTxSEQ::cast_from_int(1),
                                            trans_version.get_val_for_tx(),
                                            storage::ObTxData::COMMIT,
                                            true,
                                            end_scn));
  ASSERT_EQ(OB_SUCCESS, mgr.get_trans_state(ObTransID(1),
                                            ObTxSEQ::cast_from_int(1),
                                            trans_state));
  EXPECT_EQ(storage::ObTxData::COMMIT, trans_state.trans_state_);
  EXPECT_EQ(trans_version.get_val_for_tx(), trans_state.trans_version_);
  EXPECT_EQ(true, trans_state.can_read_);
  EXPECT_EQ(end_scn, trans_state.end_scn_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_compaction_trans_cache.log");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
