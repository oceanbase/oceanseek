/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/ob_tablet_create_mds_ctx.h"
#include "storage/multi_data_source/ob_start_transfer_in_mds_ctx.h"
#include "storage/multi_data_source/ob_finish_transfer_in_mds_ctx.h"
#include "storage/multi_data_source/compile_utility/compile_mapper.h"
#include "storage/ob_relative_table.h"
#include "storage/tx/ob_trans_id.h"

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestMdsNewCtxDeserialized: public ::testing::Test
{
public:
  TestMdsNewCtxDeserialized() {};
  virtual ~TestMdsNewCtxDeserialized() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsNewCtxDeserialized);
};

TEST_F(TestMdsNewCtxDeserialized, deserialized_from_mds_ctx) {
  MdsCtx old_ctx;
  ObRelativeTable relative_table;
  old_ctx.set_writer(MdsWriter(transaction::ObTransID(1)));
  old_ctx.set_binding_type_id(TupleTypeIdx<mds::BufferCtxTupleHelper, MdsCtx>::value);
  old_ctx.set_relative_table(&relative_table);
  char buffer[1024];
  for (auto &ch : buffer)
    ch = 0xff;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_ctx.serialize(buffer, 1024, pos));
  int64_t buffer_len = pos;
  pos = 0;
  ObTabletCreateMdsCtx new_ctx1;
  ASSERT_EQ(OB_SUCCESS, new_ctx1.deserialize(buffer, buffer_len, pos));
  ASSERT_EQ(nullptr, new_ctx1.get_relative_table());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_ctx1.deserialize(buffer, buffer_len + 1, pos));
  pos = 0;
  for (int idx = buffer_len; idx < 1024; ++idx)
    buffer[idx] = 0;
  ASSERT_EQ(OB_SUCCESS, new_ctx1.deserialize(buffer, buffer_len + 10, pos));
  pos = 0;
  ObStartTransferInMdsCtx new_ctx2;
  ASSERT_EQ(OB_SUCCESS, new_ctx2.deserialize(buffer, buffer_len, pos));
  pos = 0;
  ObFinishTransferInMdsCtx new_ctx3;
  ASSERT_EQ(OB_SUCCESS, new_ctx3.deserialize(buffer, buffer_len, pos));
}

TEST_F(TestMdsNewCtxDeserialized, relative_table_pointer_is_ephemeral) {
  MdsCtx ctx;
  ObRelativeTable relative_table;
  ObRelativeTable nested_relative_table;

  ASSERT_EQ(nullptr, ctx.get_relative_table());
  {
    ObMdsRelativeTableGuard guard(ctx, relative_table);
    ASSERT_EQ(&relative_table, ctx.get_relative_table());
    {
      ObMdsRelativeTableGuard nested_guard(ctx, nested_relative_table);
      ASSERT_EQ(&relative_table, ctx.get_relative_table());
    }
    ASSERT_EQ(&relative_table, ctx.get_relative_table());
  }
  ASSERT_EQ(nullptr, ctx.get_relative_table());
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_new_ctx_deserialized.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_new_ctx_deserialized.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}