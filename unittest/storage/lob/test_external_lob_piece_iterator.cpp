/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include "lib/worker.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/store/ob_lob_store_common.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;

namespace unittest
{

class ExternalLobStatusWorker : public lib::Worker
{
public:
  explicit ExternalLobStatusWorker(int64_t cancel_on) : cancel_on_(cancel_on), checks_(0) {}
  int check_status() override
  {
    return ++checks_ >= cancel_on_ ? OB_ERR_QUERY_INTERRUPTED : OB_SUCCESS;
  }
  int64_t checks() const { return checks_; }

private:
  int64_t cancel_on_;
  int64_t checks_;
};

class ExternalLobStatusScope
{
public:
  explicit ExternalLobStatusScope(lib::Worker &worker)
      : tenant_(OB_SYS_TENANT_ID, 0), old_tenant_(share::ObTenantEnv::get_tenant()),
        old_worker_(lib::Worker::self_)
  {
    share::ObTenantEnv::set_tenant(&tenant_);
    lib::Worker::set_worker_to_thread_local(&worker);
  }

  ~ExternalLobStatusScope()
  {
    lib::Worker::set_worker_to_thread_local(old_worker_);
    share::ObTenantEnv::set_tenant(old_tenant_);
  }

private:
  share::ObTenantBase tenant_;
  share::ObTenantBase *old_tenant_;
  lib::Worker *old_worker_;
};

static void check_empty_external_range(int64_t cancel_on, int expected_ret, int64_t expected_checks)
{
  ExternalLobStatusWorker worker(cancel_on);
  ExternalLobStatusScope scope(worker);
  ObLobReadContext read_ctx;
  read_ctx.coll_type_ = CS_TYPE_BINARY;
  read_ctx.byte_size_ = 0;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.kind_ = ObExternalBlobKind::REFERENCE;
  info.has_length_ = false;
  ObObjectStorageInfo access_info;
  ObExternalLobQueryIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.open(read_ctx, info, 1001, 1, access_info));
  char data[1];
  ObString buffer;
  buffer.assign_buffer(data, sizeof(data));
  EXPECT_EQ(expected_ret, iter.get_next_row(buffer));
  EXPECT_EQ(expected_checks, worker.checks());
  EXPECT_EQ(0, buffer.length());
}

TEST(TestExternalLobPieceIterator, canceled_before_reading)
{
  check_empty_external_range(1, OB_ERR_QUERY_INTERRUPTED, 1);
}

TEST(TestExternalLobPieceIterator, empty_range_without_cancellation)
{
  check_empty_external_range(2, OB_ITER_END, 1);
}

TEST(TestExternalLobPieceIterator, clipped_first_and_last_piece)
{
  const int64_t mib = 1024 * 1024;
  const int64_t expected_offsets[] = {3, 4, 6, 8, 10, 12};
  const int64_t expected_sizes[] = {1, 2, 2, 2, 2, 1};
  ObExternalLobMacroIterator iter;
  MacroBlockId id;
  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 77, 3 * mib, 10 * mib, 0));
  for (int64_t i = 0; i < 6; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
    EXPECT_TRUE(id.is_valid());
    EXPECT_TRUE(id.is_id_mode_share());
    EXPECT_TRUE(id.is_in_object_storage());
    EXPECT_EQ(ObStorageObjectType::SHARED_EXTERNAL_LOB_DATA_MACRO, id.storage_object_type());
    EXPECT_EQ(1001, id.second_id());
    EXPECT_EQ(77, id.third_id());
    EXPECT_EQ(expected_offsets[i] * mib, id.fourth_id());
    EXPECT_EQ(expected_sizes[i] * mib, id.fifth_id());
  }
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST(TestExternalLobPieceIterator, boundary_and_single_piece)
{
  const uint64_t size = ObExternalLobMacroIterator::PIECE_SIZE;
  ObExternalLobMacroIterator iter;
  MacroBlockId id;
  EXPECT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 1, size, size, 0));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  EXPECT_EQ(size, static_cast<uint64_t>(id.fourth_id()));
  EXPECT_EQ(size, static_cast<uint64_t>(id.fifth_id()));
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));
  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 1, 7, 3, 0));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  EXPECT_EQ(7, id.fourth_id());
  EXPECT_EQ(3, id.fifth_id());
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST(TestExternalLobPieceIterator, stable_id_and_distinct_insert_identity)
{
  ObExternalLobMacroIterator first;
  ObExternalLobMacroIterator reread;
  ObExternalLobMacroIterator copied;
  MacroBlockId first_id;
  MacroBlockId reread_id;
  MacroBlockId copied_id;
  ASSERT_EQ(OB_SUCCESS, first.init(1001, 7, 3, 10, 0));
  ASSERT_EQ(OB_SUCCESS, reread.init(1001, 7, 3, 10, 0));
  ASSERT_EQ(OB_SUCCESS, copied.init(1001, 8, 3, 10, 0));
  ASSERT_EQ(OB_SUCCESS, first.get_next(first_id));
  ASSERT_EQ(OB_SUCCESS, reread.get_next(reread_id));
  ASSERT_EQ(OB_SUCCESS, copied.get_next(copied_id));
  EXPECT_EQ(first_id, reread_id);
  EXPECT_FALSE(first_id == copied_id);
}

TEST(TestExternalLobPieceIterator, seek_near_end_of_large_range)
{
  const uint64_t size = ObExternalLobMacroIterator::PIECE_SIZE;
  const uint64_t offset = 1;
  const uint64_t end_offset = INT64_MAX;
  const uint64_t length = end_offset - offset;
  const uint64_t last_piece_size = end_offset % size;
  ObExternalLobMacroIterator iter;
  MacroBlockId id;
  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 1, offset, length, length - 1));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  EXPECT_TRUE(id.is_valid());
  EXPECT_EQ(end_offset - last_piece_size, static_cast<uint64_t>(id.fourth_id()));
  EXPECT_EQ(last_piece_size, static_cast<uint64_t>(id.fifth_id()));
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST(TestExternalLobPieceIterator, seek_preserves_clipped_piece_boundaries)
{
  const uint64_t size = ObExternalLobMacroIterator::PIECE_SIZE;
  const uint64_t offset = size + 7;
  const uint64_t length = 2 * size;
  ObExternalLobMacroIterator iter;
  MacroBlockId id;

  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 1, offset, length, 1));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  EXPECT_EQ(offset, static_cast<uint64_t>(id.fourth_id()));
  EXPECT_EQ(size - 7, static_cast<uint64_t>(id.fifth_id()));

  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 1, offset, length, size - 7));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  EXPECT_EQ(2 * size, static_cast<uint64_t>(id.fourth_id()));
  EXPECT_EQ(size, static_cast<uint64_t>(id.fifth_id()));
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  EXPECT_EQ(3 * size, static_cast<uint64_t>(id.fourth_id()));
  EXPECT_EQ(7, id.fifth_id());
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));

  iter.reset();
  ASSERT_EQ(OB_SUCCESS, iter.init(1001, 1, offset, length, length));
  EXPECT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST(TestExternalLobPieceIterator, empty_range_has_no_pieces)
{
  ObExternalLobMacroIterator invalid_iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_iter.init(1001, 7, UINT64_MAX, 0, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_iter.init(1001, 7, uint64_t(INT64_MAX) + 1, 0, 0));
  const uint64_t offsets[] = {0, 1, INT64_MAX};
  for (uint64_t offset : offsets) {
    ObExternalLobMacroIterator iter;
    MacroBlockId id;
    ASSERT_EQ(OB_SUCCESS, iter.init(1001, 7, offset, 0, 0));
    EXPECT_EQ(OB_ITER_END, iter.get_next(id));
  }
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.kind_ = ObExternalBlobKind::REFERENCE;
  info.offset_ = INT64_MAX;
  EXPECT_FALSE(info.is_valid());
  info.offset_ = 0;
  EXPECT_FALSE(info.is_valid());
  info.offset_ = INT64_MAX;
  info.has_length_ = false;
  EXPECT_FALSE(info.is_valid());
  info.offset_ = 0;
  EXPECT_TRUE(info.is_valid());
  info.offset_ = 1;
  EXPECT_FALSE(info.is_valid());
}

TEST(TestExternalLobPieceIterator, null_length_descriptor_round_trip)
{
  ObArenaAllocator allocator;
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.kind_ = ObExternalBlobKind::REFERENCE;
  info.has_length_ = false;
  const int64_t size = ObExternalBlobInfoLayout::get_size(info);
  void *buf = allocator.alloc(size);
  ASSERT_NE(nullptr, buf);
  auto *layout = new (buf) ObExternalBlobInfoLayout();
  layout->assign(info);
  ASSERT_TRUE(layout->is_valid());
  ObExternalBlobInfo decoded;
  layout->get_info(decoded);
  EXPECT_FALSE(decoded.has_length_);
  EXPECT_EQ(0, decoded.length_);
  EXPECT_EQ(info.url_, decoded.url_);
  ObLobMetaExtraInfo extra;
  ObLobMetaExtraInfo restored;
  ObString json;
  extra.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  ASSERT_EQ(OB_SUCCESS, extra.set_external_lob_info(allocator, decoded));
  ASSERT_EQ(OB_SUCCESS, extra.serialize(allocator, json));
  ASSERT_EQ(OB_SUCCESS, restored.deserialize(allocator, json));
  EXPECT_FALSE(restored.external_lob_info_.has_length_);
  info.length_ = 0;
  info.has_length_ = true;
  EXPECT_FALSE(info.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, extra.set_external_lob_info(allocator, info));
  info.length_ = 1;
  info.offset_ = INT64_MAX - 1;
  ASSERT_EQ(OB_SUCCESS, extra.set_external_lob_info(allocator, info));
  ASSERT_EQ(OB_SUCCESS, extra.serialize(allocator, json));
  ASSERT_EQ(OB_SUCCESS, restored.deserialize(allocator, json));
  EXPECT_TRUE(restored.external_lob_info_.has_length_);
  EXPECT_EQ(INT64_MAX - 1, restored.external_lob_info_.offset_);
  EXPECT_EQ(1, restored.external_lob_info_.length_);
}

TEST(TestExternalLobPieceIterator, summary_requires_has_length)
{
  ObArenaAllocator allocator;
  ObLobMetaExtraInfo restored;
  const ObString missing_has_length = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"EXTERNAL_REFERENCE\",\"chunk_count\":0,"
      "\"external_lob_url\":\"s3://bucket/object\",\"external_lob_offset\":0,"
      "\"external_lob_length\":7,\"external_lob_kind\":0}");
  EXPECT_EQ(OB_INVALID_ARGUMENT, restored.deserialize(allocator, missing_has_length));
}

TEST(TestExternalLobPieceIterator, invalid_identity_and_io_range)
{
  ObExternalLobMacroIterator iter;
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(0, 1, 0, 1, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(1001, 0, 0, 1, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(1001, 1, INT64_MAX, 1, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(1001, 1, UINT64_MAX, 1, 0));
  EXPECT_EQ(OB_SUCCESS, iter.init(1001, 1, INT64_MAX - 1, 1, 0));
  EXPECT_EQ(OB_INIT_TWICE, iter.init(1001, 1, 0, 1, 0));
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_external_lob_piece_iterator.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
