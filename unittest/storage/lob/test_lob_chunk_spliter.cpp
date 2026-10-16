/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstring>

#define protected public
#define private public

#include "lib/allocator/page_arena.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_persistent_iterator.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/lob/store/ob_lob_store_common.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace unittest
{

class TestLobChunkSpliter : public ::testing::Test
{
public:
  TestLobChunkSpliter() : lob_manager_(OB_SYS_TENANT_ID) {}
  virtual ~TestLobChunkSpliter() {}
  virtual void SetUp() { lob_manager_.is_inited_ = true; }
  virtual void TearDown() {}

  int init_source_ctx(
      ObLobLocatorV2 &source_locator,
      ObIAllocator &allocator,
      const ObCollationType coll_type,
      const uint64_t source_byte_len,
      ObLobReadContext &read_ctx,
      ObLobSourceReadCtx &source_ctx)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(read_ctx.build(source_locator, allocator, 0, UINT64_MAX))) {
      STORAGE_LOG(WARN, "failed to build source lob query parameter", KR(ret), K(source_locator));
    } else {
      read_ctx.coll_type_ = coll_type;
      read_ctx.byte_size_ = source_byte_len;
      source_ctx.read_ctx_ = &read_ctx;
      source_ctx.lob_manager_ = &lob_manager_;
      source_ctx.source_byte_len_ = source_byte_len;
    }
    return ret;
  }

  ObLobManager lob_manager_;
};

// fill an allocator-backed buffer with repeated unit string
static ObString make_data(ObIAllocator &allocator, const char *unit, int64_t repeat)
{
  ObString result;
  const int64_t unit_len = STRLEN(unit);
  const int64_t total = unit_len * repeat;
  char *buf = static_cast<char *>(allocator.alloc(total <= 0 ? 1 : total));
  EXPECT_NE(nullptr, buf);
  for (int64_t i = 0; i < repeat; ++i) {
    MEMCPY(buf + i * unit_len, unit, unit_len);
  }
  result.assign_ptr(buf, static_cast<int32_t>(total));
  return result;
}

//==================== ObLobMetaExtraInfo ====================
TEST_F(TestLobChunkSpliter, lob_meta_extra_info_old_summary_defaults_to_insert)
{
  const ObString old_summary = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"IN_TABLE\",\"chunk_count\":3}");
  ObLobMetaExtraInfo extra_info;
  extra_info.last_write_type_ = ObLobLastWriteType::APPEND;

  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(old_summary));
  ASSERT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, extra_info.extra_info_type_);
  ASSERT_EQ(ObLobLastWriteType::INSERT, extra_info.last_write_type_);
}

TEST_F(TestLobChunkSpliter, lob_meta_extra_info_rejects_invalid_last_write_type)
{
  const ObString invalid_summary = ObString::make_string(
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"IN_TABLE\","
      "\"chunk_count\":3,\"last_write_type\":99}");
  ObLobMetaExtraInfo extra_info;

  ASSERT_EQ(OB_INVALID_ARGUMENT, extra_info.deserialize(invalid_summary));
}

//==================== ObLobChunkType ====================
TEST_F(TestLobChunkSpliter, chunk_type_state)
{
  ObLobChunkType type;
  ASSERT_FALSE(type.is_valid());
  ASSERT_TRUE(type.in_table()); // 见 ObLobChunkType::in_table() 的实现和注释

  type.set_in_table();
  ASSERT_TRUE(type.is_valid());
  ASSERT_TRUE(type.in_table());

  type.reset();
  ASSERT_FALSE(type.is_valid());
}

//==================== ObLobChunk::to_meta / to_piece ====================
TEST_F(TestLobChunkSpliter, chunk_to_meta_invalid_type)
{
  ObLobChunk chunk;
  ObArenaAllocator allocator;
  ObLobMetaInfo meta;
  // type not set -> invalid
  ASSERT_EQ(OB_INVALID_ARGUMENT, chunk.to_meta(allocator, meta));
}

TEST_F(TestLobChunkSpliter, chunk_to_meta_fields)
{
  ObArenaAllocator allocator;
  const char seq[] = "\x01\x00\x00\x01\x00";
  const char *block = "block-id";
  const char *data = "chunk-data";

  ObLobChunk chunk;
  chunk.lob_col_idx_ = 3;
  chunk.lob_id_.assign(chunk.lob_col_idx_, 1001, 2002);
  chunk.seq_id_.assign_ptr(const_cast<char*>(seq), sizeof(seq) - 1);
  chunk.byte_len_ = 100;
  chunk.char_len_ = 80;
  chunk.lob_extra_info_.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  chunk.lob_extra_info_.lob_chunk_type_.set_in_table();
  chunk.lob_block_id_.assign_ptr(block, STRLEN(block));
  chunk.lob_data_.assign_ptr(data, STRLEN(data));

  ObLobMetaInfo meta;
  ASSERT_EQ(OB_SUCCESS, chunk.to_meta(allocator, meta));
  ASSERT_EQ(3, meta.lob_col_idx_);
  ASSERT_EQ(chunk.lob_id_, meta.lob_id_);
  ASSERT_EQ(100u, meta.byte_len_);
  ASSERT_EQ(80u, meta.char_len_);
  ASSERT_EQ(0, MEMCMP(meta.seq_id_.ptr(), seq, sizeof(seq) - 1));
  ASSERT_EQ(0, MEMCMP(meta.lob_block_id_.ptr(), block, STRLEN(block)));

  ObLobPieceInfo piece;
  ASSERT_EQ(OB_SUCCESS, chunk.to_piece(piece));
  ASSERT_EQ(3, piece.lob_col_idx_);
  ASSERT_EQ(chunk.lob_id_, piece.lob_id_);
  ASSERT_EQ(0, MEMCMP(piece.seq_id_.ptr(), seq, sizeof(seq) - 1));
  ASSERT_EQ(0, MEMCMP(piece.lob_data_.ptr(), data, STRLEN(data)));
}

TEST_F(TestLobChunkSpliter, chunk_reset)
{
  ObLobChunk chunk;
  chunk.lob_extra_info_.lob_chunk_type_.set_in_table();
  chunk.lob_col_idx_ = 5;
  chunk.byte_len_ = 99;
  chunk.reset();
  ASSERT_FALSE(chunk.lob_extra_info_.lob_chunk_type_.is_valid());
  ASSERT_EQ(OB_INVALID_INDEX_INT64, chunk.lob_col_idx_);
  ASSERT_EQ(0u, chunk.byte_len_);
}

//==================== streaming chunk / piece iterators ====================
TEST_F(TestLobChunkSpliter, query_iter_returns_multibyte_chunks)
{
  ObArenaAllocator allocator;
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  ObString data = make_data(allocator, "\xe4\xb8\xad", 512);
  ObLobInRowQueryIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(
      data, 0, static_cast<uint32_t>(data.length()), CS_TYPE_UTF8MB4_GENERAL_CI));

  ObLobChunk chunk;
  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(1023u, chunk.byte_len_);
  ASSERT_EQ(341u, chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), data.ptr(), chunk.byte_len_));

  ASSERT_EQ(OB_SUCCESS, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_EQ(513u, chunk.byte_len_);
  ASSERT_EQ(171u, chunk.char_len_);
  ASSERT_EQ(0, MEMCMP(chunk.lob_data_.ptr(), data.ptr() + 1023, chunk.byte_len_));

  ASSERT_EQ(OB_ITER_END, query_iter.get_next_chunk(chunk, chunk_size));
  ASSERT_TRUE(query_iter.is_end());
  ASSERT_FALSE(chunk.lob_extra_info_.lob_chunk_type_.is_valid());
}

TEST_F(TestLobChunkSpliter, piece_iter_streams_and_deep_copies_meta)
{
  ObArenaAllocator allocator;
  const int64_t chunk_size = OB_MIN_LOB_CHUNK_SIZE;
  ObString data = make_data(allocator, "a", chunk_size + 17);
  ObLobInRowQueryIter query_iter;
  ASSERT_EQ(OB_SUCCESS, query_iter.open(
      data, 0, static_cast<uint32_t>(data.length()), CS_TYPE_BINARY));

  ObLobId lob_id;
  lob_id.assign(2, 1001, 2002);
  ObArray<ObLobMetaInfo> metas;
  ObLobWriteStats stats;
  ObLobInTablePieceInsertIter piece_iter;
  ASSERT_EQ(OB_SUCCESS, piece_iter.init(
      &query_iter, &allocator, chunk_size, 2, lob_id, &metas, &stats));

  blocksstable::ObDatumRow *row = nullptr;
  ASSERT_EQ(OB_SUCCESS, piece_iter.get_next_row(row));
  ASSERT_NE(nullptr, row);
  ASSERT_EQ(1, metas.count());
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), stats.byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(chunk_size), stats.char_len_);
  ASSERT_EQ(1u, stats.chunk_count_);
  ASSERT_EQ(ObLobSeqId::LOB_SEQ_ID_MIN_LEN, metas.at(0).seq_id_.length());
  char first_seq_id[sizeof(uint32_t)];
  MEMCPY(first_seq_id, metas.at(0).seq_id_.ptr(), sizeof(first_seq_id));

  ASSERT_EQ(OB_SUCCESS, piece_iter.get_next_row(row));
  ASSERT_NE(nullptr, row);
  ASSERT_EQ(2, metas.count());
  ASSERT_EQ(static_cast<uint64_t>(data.length()), stats.byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(data.length()), stats.char_len_);
  ASSERT_EQ(2u, stats.chunk_count_);
  ASSERT_EQ(0, MEMCMP(first_seq_id, metas.at(0).seq_id_.ptr(), sizeof(first_seq_id)));
  ASSERT_NE(0, metas.at(0).seq_id_.compare(metas.at(1).seq_id_));
  ASSERT_NE(metas.at(0).seq_id_.ptr(), metas.at(1).seq_id_.ptr());
  ASSERT_NE(metas.at(0).lob_extra_info_.ptr(), metas.at(1).lob_extra_info_.ptr());

  ASSERT_EQ(OB_ITER_END, piece_iter.get_next_row(row));
  ASSERT_EQ(nullptr, row);
  ASSERT_TRUE(piece_iter.is_end());
}

//==================== ObLobWriteIter ====================
TEST_F(TestLobChunkSpliter, lob_write_iter_emits_piece_then_meta)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
  ObString data = make_data(source_allocator, "a", chunk_size + 17);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 3;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2002);
  target_ctx.seq_no_ = transaction::ObTxSEQ(10, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_STATE_NOT_MATCH, write_iter.get_pending_locator(pending_locator));

  ObArray<ObString> piece_seq_ids;
  ObLobWriteResult result;
  int ret = OB_SUCCESS;
  int64_t piece_count = 0;
  int64_t meta_count = 0;
  int64_t data_offset = 0;
  bool summary_seen = false;
  while (OB_SUCCESS == (ret = write_iter.get_next_row(result))) {
    if (result.is_piece()) {
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(target_ctx.lob_col_idx_, result.piece_info_.lob_col_idx_);
      ASSERT_EQ(target_ctx.lob_id_, result.piece_info_.lob_id_);
      ASSERT_EQ(0, MEMCMP(data.ptr() + data_offset,
                          result.piece_info_.lob_data_.ptr(),
                          result.piece_info_.lob_data_.length()));
      data_offset += result.piece_info_.lob_data_.length();
      ObString seq_id;
      ASSERT_EQ(OB_SUCCESS, ob_write_string(
          source_allocator, result.piece_info_.seq_id_, seq_id));
      ASSERT_EQ(OB_SUCCESS, piece_seq_ids.push_back(seq_id));
      ++piece_count;
      if (1 == piece_count) {
        ASSERT_EQ(OB_STATE_NOT_MATCH, write_iter.get_pending_locator(pending_locator));
      }
    } else if (result.is_meta_summary()) {
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(2, piece_count);
      ASSERT_EQ(target_ctx.lob_col_idx_, result.meta_info_.lob_col_idx_);
      ASSERT_EQ(target_ctx.lob_id_, result.meta_info_.lob_id_);
      char summary_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
      ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
      ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_summary_seq_id(summary_seq_id));
      ASSERT_EQ(0, result.meta_info_.seq_id_.compare(summary_seq_id));
      ASSERT_EQ(static_cast<uint64_t>(data.length()), result.meta_info_.byte_len_);
      ASSERT_EQ(static_cast<uint64_t>(data.length()), result.meta_info_.char_len_);
      ObLobMetaExtraInfo extra_info;
      ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(result.meta_info_.lob_extra_info_));
      ASSERT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, extra_info.extra_info_type_);
      ASSERT_EQ(static_cast<uint64_t>(piece_count), extra_info.chunk_count_);
      ASSERT_TRUE(extra_info.lob_chunk_type_.in_table());
      summary_seen = true;
      ++meta_count;
    } else {
      ASSERT_TRUE(result.is_meta_chunk());
      ASSERT_TRUE(summary_seen);
      ASSERT_LT(meta_count - 1, piece_seq_ids.count());
      ASSERT_EQ(target_ctx.lob_col_idx_, result.meta_info_.lob_col_idx_);
      ASSERT_EQ(target_ctx.lob_id_, result.meta_info_.lob_id_);
      ASSERT_EQ(0, result.meta_info_.seq_id_.compare(piece_seq_ids.at(meta_count - 1)));
      ++meta_count;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(data.length(), data_offset);
  ASSERT_EQ(2, piece_count);
  ASSERT_EQ(3, meta_count);
  ASSERT_TRUE(write_iter.is_finished());

  ASSERT_EQ(OB_SUCCESS, write_iter.get_pending_locator(pending_locator));
  ASSERT_TRUE(pending_locator.is_outrow_disk_lob_locator());
  ObString disk_locator;
  ASSERT_EQ(OB_SUCCESS, pending_locator.get_disk_locator(disk_locator));
  ObLobDiskLocatorWrapper locator_wrapper;
  ASSERT_EQ(OB_SUCCESS, locator_wrapper.init(disk_locator.ptr(), disk_locator.length()));
  ASSERT_EQ(target_ctx.lob_id_, locator_wrapper.get_lob_id());
  ASSERT_EQ(target_ctx.seq_no_, locator_wrapper.get_seq_no());

  write_iter.reset();
  result_allocator.reuse();
  ASSERT_EQ(target_ctx.lob_id_, locator_wrapper.get_lob_id());
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_piece());
  ASSERT_EQ(0, result.piece_info_.seq_id_.compare(piece_seq_ids.at(0)));
  write_iter.reset();
}

TEST_F(TestLobChunkSpliter, lob_write_iter_accumulates_utf8mb4_char_len)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
  const int64_t emoji_count = chunk_size / 4 * 2 + 17;
  ObString data = make_data(source_allocator, "\xf0\x9f\x98\x80", emoji_count);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_UTF8MB4_GENERAL_CI,
      data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
  target_ctx.lob_col_idx_ = 6;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2007);
  target_ctx.seq_no_ = transaction::ObTxSEQ(15, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  int ret = OB_SUCCESS;
  int64_t piece_count = 0;
  int64_t meta_chunk_count = 0;
  bool summary_seen = false;
  while (OB_SUCCESS == (ret = write_iter.get_next_row(result))) {
    if (result.is_piece()) {
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(0, result.piece_info_.lob_data_.length() % 4);
      ++piece_count;
    } else if (result.is_meta_summary()) {
      ObLobMetaExtraInfo extra_info;
      ASSERT_FALSE(summary_seen);
      ASSERT_EQ(static_cast<uint64_t>(data.length()), result.meta_info_.byte_len_);
      ASSERT_EQ(static_cast<uint64_t>(emoji_count), result.meta_info_.char_len_);
      ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(result.meta_info_.lob_extra_info_));
      ASSERT_EQ(static_cast<uint64_t>(piece_count), extra_info.chunk_count_);
      summary_seen = true;
    } else {
      ASSERT_TRUE(result.is_meta_chunk());
      ASSERT_TRUE(summary_seen);
      ++meta_chunk_count;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_EQ(3, piece_count);
  ASSERT_EQ(piece_count, meta_chunk_count);
  ASSERT_TRUE(summary_seen);
  ASSERT_EQ(static_cast<uint64_t>(data.length()), write_iter.write_byte_len_);
  ASSERT_EQ(static_cast<uint64_t>(emoji_count), write_iter.write_char_len_);
}

TEST_F(TestLobChunkSpliter, lob_write_iter_rebuilds_target_seq_id)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  ObString data = make_data(source_allocator, "a", 17);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 3;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2005);
  target_ctx.seq_no_ = transaction::ObTxSEQ(13, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));

  char source_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString source_seq_id(sizeof(source_seq_buf), source_seq_buf);
  ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_seq_id(9, source_seq_id));
  ObLobChunk source_chunk;
  source_chunk.seq_id_ = source_seq_id;
  source_chunk.byte_len_ = data.length();
  source_chunk.char_len_ = data.length();
  source_chunk.lob_data_ = data;

  ObLobWriteResult result;
  ASSERT_EQ(OB_SUCCESS, write_iter.build_chunk_write_result_(source_chunk, result));
  ASSERT_TRUE(result.is_piece());
  ASSERT_EQ(target_ctx.lob_col_idx_, result.piece_info_.lob_col_idx_);
  ASSERT_EQ(target_ctx.lob_id_, result.piece_info_.lob_id_);
  ASSERT_NE(0, source_seq_id.compare(result.piece_info_.seq_id_));
  ASSERT_EQ(1, write_iter.chunk_meta_infos_.count());
  ASSERT_EQ(0, result.piece_info_.seq_id_.compare(
      write_iter.chunk_meta_infos_.at(0).seq_id_));
}

TEST_F(TestLobChunkSpliter, lob_write_iter_empty_lob_has_summary_only)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(source_allocator, nullptr, 0));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, 0, read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 1;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2003);
  target_ctx.seq_no_ = transaction::ObTxSEQ(11, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_meta_summary());
  ASSERT_EQ(0u, result.meta_info_.byte_len_);
  ASSERT_EQ(0u, result.meta_info_.char_len_);
  ObLobMetaExtraInfo extra_info;
  ASSERT_EQ(OB_SUCCESS, extra_info.deserialize(result.meta_info_.lob_extra_info_));
  ASSERT_EQ(0u, extra_info.chunk_count_);
  ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));

  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_pending_locator(pending_locator));
}

TEST_F(TestLobChunkSpliter, lob_write_iter_failure_does_not_publish_locator)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  const int64_t chunk_size = ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
  ObString data = make_data(source_allocator, "a", chunk_size + 1);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 2;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2004);
  target_ctx.seq_no_ = transaction::ObTxSEQ(12, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_piece());
  ASSERT_EQ(OB_SUCCESS, write_iter.get_next_row(result));
  ASSERT_TRUE(result.is_piece());

  ++write_iter.source_byte_len_;
  ASSERT_EQ(OB_ERR_UNEXPECTED, write_iter.get_next_row(result));
  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_STATE_NOT_MATCH, write_iter.get_pending_locator(pending_locator));
}

TEST_F(TestLobChunkSpliter, lob_write_iter_reset_clears_finished_state)
{
  ObArenaAllocator source_allocator;
  ObArenaAllocator locator_allocator;
  ObArenaAllocator result_allocator;
  ObString data = make_data(source_allocator, "a", 17);
  ObLobDiskLocatorBuilder source_locator_builder;
  ObLobLocatorV2 source_locator;
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.init_inrow(
      source_allocator, data.ptr(), data.length()));
  ASSERT_EQ(OB_SUCCESS, source_locator_builder.to_locator(source_locator));

  ObLobReadContext read_ctx;
  ObLobSourceReadCtx source_ctx;
  ASSERT_EQ(OB_SUCCESS, init_source_ctx(
      source_locator, source_allocator, CS_TYPE_BINARY, data.length(), read_ctx, source_ctx));
  ObLobTargetWriteCtx target_ctx;
  target_ctx.locator_allocator_ = &locator_allocator;
  target_ctx.result_allocator_ = &result_allocator;
  target_ctx.coll_type_ = CS_TYPE_BINARY;
  target_ctx.lob_col_idx_ = 4;
  target_ctx.lob_id_.assign(target_ctx.lob_col_idx_, 1001, 2006);
  target_ctx.seq_no_ = transaction::ObTxSEQ(14, 0);

  ObLobWriteIter write_iter;
  ASSERT_EQ(OB_SUCCESS, write_iter.open(source_ctx, target_ctx));
  ObLobWriteResult result;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = write_iter.get_next_row(result))) {
  }
  ASSERT_EQ(OB_ITER_END, ret);
  ASSERT_TRUE(write_iter.is_finished());
  ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));
  ASSERT_EQ(OB_ITER_END, write_iter.get_next_row(result));

  ObLobLocatorV2 pending_locator;
  ASSERT_EQ(OB_SUCCESS, write_iter.get_pending_locator(pending_locator));
  ASSERT_TRUE(pending_locator.is_outrow_disk_lob_locator());

  write_iter.reset();
  ASSERT_FALSE(write_iter.is_finished());
  ASSERT_EQ(OB_NOT_INIT, write_iter.get_next_row(result));
  ASSERT_EQ(OB_NOT_INIT, write_iter.get_pending_locator(pending_locator));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_chunk_spliter.log*");
  OB_LOGGER.set_file_name("test_lob_chunk_spliter.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObCharset::init_charset();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
