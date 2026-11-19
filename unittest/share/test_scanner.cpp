/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/ob_scanner.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "storage/lob/common/ob_outtable_lob_control_key.h"
using namespace oceanbase::common;
using oceanbase::storage::ObOuttableLobControlKey;

namespace
{
ObOuttableLobControlKey make_control_key(const int64_t sequence)
{
  ObOuttableLobControlKey key;
  key.macro_id_ = oceanbase::blocksstable::MacroBlockId(
      oceanbase::blocksstable::MacroBlockId::DEFAULT_FIRST_ID, sequence, 0, 0, 0);
  key.hash_ = sequence * 3;
  return key;
}

void expect_control_keys(const ObScanner &scanner, const int64_t count, const int64_t sequence)
{
  const ObIArray<ObOuttableLobControlKey> *keys = scanner.get_outtable_lob_control_keys();
  ASSERT_NE(nullptr, keys);
  ASSERT_EQ(count, keys->count());
  for (int64_t i = 0; i < count; ++i) {
    const ObOuttableLobControlKey expected = make_control_key(sequence + i);
    EXPECT_EQ(expected.macro_id_, keys->at(i).macro_id_);
    EXPECT_EQ(expected.hash_, keys->at(i).hash_);
  }
}
}

class TestScanner: public ::testing::Test
{
public:
  TestScanner();
  virtual ~TestScanner();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestScanner);
protected:
  // function members
protected:
  // data members
};

TestScanner::TestScanner()
{
}

TestScanner::~TestScanner()
{
}

void TestScanner::SetUp()
{
}

void TestScanner::TearDown()
{
}

TEST_F(TestScanner, basic_test)
{
  ObScanner scanner;
  ObObj objs[3];
  ObNewRow row;
  row.cells_ = objs;
  row.count_ = 3;
  row.cells_[0].set_int(1);
  row.cells_[1].set_int(2);
  row.cells_[2].set_int(3);

  for (int i = 0; i < 1024; ++i) {
    OK(scanner.add_row(row));
  }
  ASSERT_EQ(1024, scanner.get_row_count());
  ASSERT_EQ(3, scanner.get_col_count());

  ObScanner::Iterator it = scanner.begin();
  ObNewRow row2;
  row2.cells_ = objs;
  row2.count_ = 2;
  BAD(it.get_next_row(row2));
  row2.count_ = 3;
  for (int i = 0; i < 1024; ++i) {
    OK(it.get_next_row(row2));
    ASSERT_EQ(3, row2.count_);
    ObCStringHelper helper;
    _OB_LOG(INFO, "row=%s", helper.convert(row2));
  }
}

TEST_F(TestScanner, serialization)
{
  ObScanner scanner;
  scanner.set_affected_rows(10);
  scanner.set_last_insert_id_to_client(111);
  scanner.set_last_insert_id_session(121);
  scanner.set_last_insert_id_changed(true);
  scanner.set_found_rows(100);
  scanner.set_err_code(OB_ERR_UNEXPECTED);
  scanner.store_err_msg("how are you");
  scanner.set_row_matched_count(1000);
  scanner.set_row_duplicated_count(2000);
  scanner.set_extend_info("fine,thank you, and you");
  scanner.set_is_result_accurate(false);
  ASSERT_EQ(OB_SUCCESS, scanner.init(1024));

  ObObj objs[3];
  ObNewRow row;
  row.cells_ = objs;
  row.count_ = 3;
  row.cells_[0].set_int(1);
  row.cells_[1].set_int(2);
  row.cells_[2].set_int(3);

  for (int i = 0; i < 10; ++i) {
    OK(scanner.add_row(row));
  }

  ModuleArena allocator;
  int64_t buf_size = scanner.get_serialize_size();
  char *buf = (char *)allocator.alloc(buf_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, scanner.serialize(buf, buf_size, pos));
  ASSERT_EQ(buf_size, pos);

  ObScanner out_scanner;
  pos = 0;
  ObString err_msg(scanner.get_err_msg());
  ObString err_msg_1("how are you");
  ASSERT_EQ(OB_SUCCESS, out_scanner.deserialize(buf, buf_size, pos));
  ASSERT_EQ(scanner.get_mem_size_limit(), 1024);
  ASSERT_EQ(scanner.get_affected_rows(), 10);
  ASSERT_EQ(scanner.get_last_insert_id_to_client(), 111);
  ASSERT_EQ(scanner.get_last_insert_id_session(), 121);
  ASSERT_EQ(scanner.get_last_insert_id_changed(), true);
  ASSERT_EQ(scanner.get_found_rows(), 100);
  ASSERT_EQ(scanner.get_err_code(), OB_ERR_UNEXPECTED);
  ASSERT_EQ(err_msg.compare(err_msg_1), 0);
  ASSERT_EQ(scanner.get_row_matched_count(), 1000);
  ASSERT_EQ(scanner.get_row_duplicated_count(), 2000);
  ASSERT_EQ(scanner.get_extend_info(), "fine,thank you, and you");
  ASSERT_EQ(scanner.is_result_accurate(), false);

  ObScanner::Iterator it = out_scanner.begin();
  ObNewRow row2;
  row2.cells_ = objs;
  row2.count_ = 2;
  BAD(it.get_next_row(row2));
  row2.count_ = 3;
  for (int i = 0; i < 10; ++i) {
    OK(it.get_next_row(row2));
    ASSERT_EQ(3, row2.count_);
    ObCStringHelper helper;
    _OB_LOG(INFO, "row=%s", helper.convert(row2));
  }
}

TEST_F(TestScanner, outtable_control_keys_round_trip_without_rows)
{
  ObScanner source;
  EXPECT_EQ(nullptr, source.get_outtable_lob_control_keys());
  ASSERT_EQ(OB_SUCCESS, source.init());
  source.set_affected_rows(2);
  ASSERT_EQ(OB_SUCCESS, source.set_row_matched_count(2));
  ASSERT_EQ(OB_SUCCESS, source.add_outtable_lob_control_key(make_control_key(2026111701)));
  ASSERT_EQ(OB_SUCCESS, source.add_outtable_lob_control_key(make_control_key(2026111702)));
  EXPECT_EQ(0, source.get_row_count());

  ObArenaAllocator allocator("ScannerLobTest");
  const int64_t size = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  ASSERT_EQ(size, pos);
  ObScanner target;
  int64_t decoded_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, size, decoded_pos));
  EXPECT_EQ(size, decoded_pos);
  EXPECT_EQ(0, target.get_row_count());
  EXPECT_EQ(2, target.get_affected_rows());
  EXPECT_EQ(2, target.get_row_matched_count());
  expect_control_keys(target, 2, 2026111701);
}

TEST_F(TestScanner, outtable_control_keys_empty_packet_clears_previous_keys)
{
  ObScanner source;
  ASSERT_EQ(OB_SUCCESS, source.init());
  source.set_affected_rows(3);
  // Reusing the receiver for an empty result must discard keys from the preceding response.
  ObArenaAllocator allocator("ScannerEmptyLob");
  const int64_t size = source.get_serialize_size();
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  ASSERT_EQ(size, pos);

  ObScanner target;
  ASSERT_EQ(OB_SUCCESS, target.add_outtable_lob_control_key(make_control_key(2026111703)));
  int64_t decoded_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, size, decoded_pos));
  EXPECT_EQ(size, decoded_pos);
  EXPECT_EQ(3, target.get_affected_rows());
  const ObIArray<ObOuttableLobControlKey> *keys = target.get_outtable_lob_control_keys();
  EXPECT_TRUE(nullptr == keys || keys->empty());
}

TEST_F(TestScanner, outtable_control_keys_reuse_and_reset)
{
  ObScanner scanner;
  ASSERT_EQ(OB_SUCCESS, scanner.init());
  const int64_t empty_size = scanner.get_serialize_size();
  ASSERT_EQ(OB_SUCCESS, scanner.add_outtable_lob_control_key(make_control_key(2026111704)));
  EXPECT_GT(scanner.get_serialize_size(), empty_size);
  scanner.reuse();
  const ObIArray<ObOuttableLobControlKey> *keys = scanner.get_outtable_lob_control_keys();
  EXPECT_TRUE(nullptr == keys || keys->empty());
  EXPECT_EQ(empty_size, scanner.get_serialize_size());

  ASSERT_EQ(OB_SUCCESS, scanner.add_outtable_lob_control_key(make_control_key(2026111705)));
  expect_control_keys(scanner, 1, 2026111705);
  scanner.reset();
  keys = scanner.get_outtable_lob_control_keys();
  EXPECT_TRUE(nullptr == keys || keys->empty());
  ASSERT_EQ(OB_SUCCESS, scanner.add_outtable_lob_control_key(make_control_key(2026111706)));
  expect_control_keys(scanner, 1, 2026111706);
}

TEST_F(TestScanner, outtable_control_keys_assign_owns_copy_and_clears_old_keys)
{
  ObScanner source;
  ObScanner target;
  ASSERT_EQ(OB_SUCCESS, source.init());
  ASSERT_EQ(OB_SUCCESS, source.add_outtable_lob_control_key(make_control_key(2026111707)));
  ASSERT_EQ(OB_SUCCESS, target.add_outtable_lob_control_key(make_control_key(2026111708)));
  ASSERT_EQ(OB_SUCCESS, target.assign(source));
  source.reset_outtable_lob_control_keys();
  expect_control_keys(target, 1, 2026111707);
  ASSERT_EQ(OB_SUCCESS, target.assign(source));
  const ObIArray<ObOuttableLobControlKey> *keys = target.get_outtable_lob_control_keys();
  EXPECT_TRUE(nullptr == keys || keys->empty());
}

TEST_F(TestScanner, outtable_control_keys_large_result_round_trip)
{
  const int64_t key_count = 90000;
  const int64_t first_sequence = 2026111800;
  ObArenaAllocator allocator("ScannerLargeLob");
  ObScanner source;
  ObScanner target;
  ASSERT_EQ(OB_SUCCESS, source.init());
  source.set_affected_rows(key_count);
  ASSERT_EQ(OB_SUCCESS, source.set_row_matched_count(key_count));
  for (int64_t i = 0; i < key_count; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        source.add_outtable_lob_control_key(make_control_key(first_sequence + i)));
  }
  const int64_t size = source.get_serialize_size();
  ASSERT_GT(size, 4L * 1024L * 1024L);
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));
  ASSERT_EQ(size, pos);
  int64_t decoded_pos = 0;
  ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, size, decoded_pos));
  EXPECT_EQ(size, decoded_pos);
  EXPECT_EQ(0, target.get_row_count());
  EXPECT_EQ(key_count, target.get_affected_rows());
  EXPECT_EQ(key_count, target.get_row_matched_count());
  expect_control_keys(target, key_count, first_sequence);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
