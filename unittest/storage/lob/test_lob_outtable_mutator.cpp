/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstdlib>
#include <memory>
#include <rapidjson/document.h>
#include <string>
#include "lib/allocator/ob_malloc.h"
#include "lib/checksum/ob_crc64.h"
#include "share/ob_admin_dump_helper.h"
#define private public
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_dml_param.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx/ob_tx_log.h"
#define private public
#include "storage/tx/ob_tx_replay_executor.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace blocksstable;
using namespace memtable;
using namespace storage;
using namespace transaction;

struct OuttableReplayIOState
{
  std::string events_;
  std::string written_data_;
  MacroBlockId head_macro_id_;
  MacroBlockId written_macro_id_;
  int64_t ls_id_ = 0;
  bool object_exists_ = false;
  int head_ret_ = OB_SUCCESS;
  int write_ret_ = OB_SUCCESS;
  int row_ret_ = OB_SUCCESS;
  int head_count_ = 0;
  int write_count_ = 0;
  int row_count_ = 0;
  MutatorType last_row_type_ = MutatorType::MUTATOR_ROW;
};

static thread_local OuttableReplayIOState *replay_io_state = nullptr;
} // namespace unittest

namespace unittest
{
class TestLobOuttableMutator : public ::testing::Test
{
protected:
  void SetUp() override
  {
    key_obj_.set_int(1);
    ASSERT_EQ(OB_SUCCESS, row_.rowkey_.assign(&key_obj_, 1));
    row_.table_id_ = 0;
    row_.table_version_ = 9;
    row_.new_row_.set("new meta row", 12);
    row_.dml_flag_ = ObDmlFlag::DF_INSERT;
    row_.update_seq_ = 2;
    row_.acc_checksum_ = 12345;
    row_.version_ = 8;
    row_.seq_no_ = ObTxSEQ(100, 0);
    row_.column_cnt_ = 7;

    payload_.lob_col_idx_ = 0;
    payload_.lob_id_.assign(1, 200, 300);
    payload_.seq_id_ = ObString("outtable seq id");
    payload_.lob_piece_table_id_ = 50001;
    payload_.lob_operation_seq_no_ = row_.seq_no_;
    payload_.macro_id_.set_version_v1();
    payload_.macro_id_.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    payload_.macro_id_.set_storage_object_type(
        static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
    payload_.macro_id_.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
    payload_.macro_id_.set_second_id(100);
    payload_.macro_id_.set_third_id(200);
    payload_.macro_id_.set_fourth_id(300);
    payload_.macro_id_.set_fifth_id(0);
    payload_.data_ = ObString("outtable data");
    payload_.byte_len_ = payload_.data_.length();
    ASSERT_TRUE(payload_.is_valid());
    ASSERT_EQ(OB_SUCCESS, encrypt_info_.init());
  }

  void serialize_native_row(std::string &raw, const MutatorType type)
  {
    raw.assign(4096, '\0');
    int64_t capacity = raw.size();
    int64_t pos = 0;
    ObMutatorRowHeader header;
    header.mutator_type_ = type;
    header.tablet_id_ = ObTabletID(200);
    ASSERT_EQ(OB_SUCCESS, header.serialize(&raw[0], capacity, pos));
    ASSERT_EQ(OB_SUCCESS, row_.serialize(&raw[0], capacity, pos, nullptr, encrypt_info_));
    raw.resize(pos);
  }

  void prepare_redo(RedoDataNode &redo, ObITransCallback *callback)
  {
    ObMemtableKey key;
    ASSERT_EQ(OB_SUCCESS, key.encode(&row_.rowkey_));
    redo.set(&key, row_.old_row_, row_.new_row_, row_.dml_flag_, row_.update_seq_,
        row_.acc_checksum_, row_.version_, row_.flag_, row_.seq_no_, ObTabletID(200),
        row_.column_cnt_);
    redo.set_callback(callback);
  }

  void append_native_row(ObMutatorWriter &writer)
  {
    std::string raw;
    ASSERT_NO_FATAL_FAILURE(serialize_native_row(raw, MutatorType::MUTATOR_ROW));
    ASSERT_EQ(OB_SUCCESS, writer.append_row_buf(raw.data(), raw.size()));
  }

  void open_iterator(ObMutatorWriter &writer, std::string &buf,
      ObMemtableMutatorIterator &iter, int64_t &length)
  {
    ASSERT_EQ(OB_SUCCESS, writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, iter.deserialize(buf.data(), length, pos, encrypt_info_));
    EXPECT_EQ(length, pos);
  }

  void check_row(ObMemtableMutatorIterator &iter, const ObDmlFlag flag)
  {
    const ObMemtableMutatorRow &decoded = iter.get_mutator_row();
    EXPECT_EQ(MutatorType::MUTATOR_LOB_OUTTABLE_ROW, iter.get_row_head().mutator_type_);
    EXPECT_EQ(ObTabletID(200), iter.get_row_head().tablet_id_);
    EXPECT_EQ(row_.rowkey_, decoded.rowkey_);
    EXPECT_EQ(row_.table_id_, decoded.table_id_);
    EXPECT_EQ(row_.table_version_, decoded.table_version_);
    EXPECT_EQ(row_.new_row_, decoded.new_row_);
    EXPECT_EQ(row_.old_row_, decoded.old_row_);
    EXPECT_EQ(flag, decoded.dml_flag_);
    EXPECT_EQ(row_.update_seq_, decoded.update_seq_);
    EXPECT_EQ(row_.acc_checksum_, decoded.acc_checksum_);
    EXPECT_EQ(row_.version_, decoded.version_);
    EXPECT_EQ(row_.seq_no_, decoded.seq_no_);
    EXPECT_EQ(row_.column_cnt_, decoded.get_column_cnt());
    EXPECT_EQ(row_.seq_no_, iter.get_row_seq_no());
  }

  void check_payload(const ObLobOutTableRedoPayload &decoded)
  {
    ASSERT_TRUE(decoded.is_valid());
    EXPECT_EQ(payload_.lob_col_idx_, decoded.lob_col_idx_);
    EXPECT_EQ(payload_.lob_id_, decoded.lob_id_);
    EXPECT_EQ(payload_.seq_id_, decoded.seq_id_);
    EXPECT_EQ(payload_.lob_piece_table_id_, decoded.lob_piece_table_id_);
    EXPECT_EQ(payload_.lob_operation_seq_no_, decoded.lob_operation_seq_no_);
    EXPECT_EQ(payload_.macro_id_, decoded.macro_id_);
    EXPECT_EQ(payload_.is_logging_, decoded.is_logging_);
    EXPECT_EQ(payload_.byte_len_, decoded.byte_len_);
    EXPECT_EQ(payload_.data_, decoded.data_);
  }

  void check_empty_payload(const ObMemtableMutatorIterator &iter)
  {
    EXPECT_TRUE(iter.get_lob_outtable_payload_buf().empty());
    const ObLobOutTableRedoPayload &payload = iter.get_lob_outtable_payload();
    EXPECT_FALSE(payload.is_valid());
    EXPECT_TRUE(payload.data_.empty());
    EXPECT_EQ(nullptr, payload.data_.ptr());
    EXPECT_TRUE(payload.seq_id_.empty());
    EXPECT_EQ(nullptr, payload.seq_id_.ptr());
  }

  void check_borrowed_data(const ObString &data, const std::string &buf, const int64_t length)
  {
    ASSERT_FALSE(data.empty());
    ASSERT_NE(nullptr, data.ptr());
    ASSERT_GT(length, data.length());
    // The serialized object string ends with its terminator.
    EXPECT_EQ(buf.data() + length - data.length() - 1, data.ptr());
    EXPECT_EQ(std::string(payload_.data_.ptr(), payload_.data_.length()),
        std::string(data.ptr(), data.length()));
  }

  void check_roundtrip(const ObDmlFlag flag)
  {
    row_.dml_flag_ = flag;
    if (ObDmlFlag::DF_UPDATE == flag) {
      row_.old_row_.set("old meta row", 12);
    }
    std::string buf(4096, '\0');
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
    ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
    ASSERT_EQ(1, writer.get_meta().get_row_count());
    ObMemtableMutatorIterator iter;
    ObEncryptRowBuf row_buf;
    int64_t length = 0;
    ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
    ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
    ASSERT_NO_FATAL_FAILURE(check_row(iter, flag));
    ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));

    // The new header and attachment must not alter the native Meta row bytes or its length.
    std::string expected;
    ASSERT_NO_FATAL_FAILURE(serialize_native_row(expected, MutatorType::MUTATOR_LOB_OUTTABLE_ROW));
    ASSERT_GT(length, writer.get_meta().get_meta_size() + expected.size());
    EXPECT_EQ(expected,
        std::string(buf.data() + writer.get_meta().get_meta_size(), expected.size()));
    if (!payload_.data_.empty()) {
      ASSERT_NO_FATAL_FAILURE(
          check_borrowed_data(iter.get_lob_outtable_payload().data_, buf, length));
    }
    ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
    ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  }

  void serialize_raw_lob_row(std::string &raw, const ObLobOutTableRedoPayload &payload)
  {
    ASSERT_NO_FATAL_FAILURE(serialize_native_row(raw, MutatorType::MUTATOR_LOB_OUTTABLE_ROW));
    const int64_t row_length = raw.size();
    raw.resize(row_length + payload.get_serialize_size());
    int64_t pos = row_length;
    ASSERT_EQ(OB_SUCCESS, payload.serialize(&raw[0], raw.size(), pos));
    ASSERT_EQ(raw.size(), pos);
  }

  void check_bad_tail(std::string &raw)
  {
    std::string buf(raw.size() + 4096, '\0');
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
    ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
    // Rebuild the outer CRC so rejection must come from the row/tail decoder.
    ASSERT_EQ(OB_SUCCESS, writer.append_row_buf(raw.data(), raw.size()));
    ObMemtableMutatorIterator iter;
    ObEncryptRowBuf row_buf;
    int64_t length = 0;
    ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
    ASSERT_EQ(2, iter.get_meta().get_row_count());
    ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
    ASSERT_TRUE(iter.get_lob_outtable_payload().is_valid());
    const int decode_ret = iter.iterate_next_row(row_buf, encrypt_info_);
    EXPECT_NE(OB_SUCCESS, decode_ret);
    EXPECT_NE(OB_ITER_END, decode_ret);
    ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
    EXPECT_FALSE(iter.get_row_seq_no().is_valid());
    // A failed decode must not consume only the native part and expose it as a valid row.
    EXPECT_EQ(decode_ret, iter.iterate_next_row(row_buf, encrypt_info_));
  }

  void serialize_bad_payload_frame(std::string &raw, const int64_t declared_length)
  {
    ASSERT_NO_FATAL_FAILURE(serialize_native_row(raw, MutatorType::MUTATOR_LOB_OUTTABLE_ROW));
    int64_t pos = raw.size();
    raw.resize(raw.size() + 32);
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(&raw[0], raw.size(), pos, 1));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(&raw[0], raw.size(), pos, declared_length));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_i8(&raw[0], raw.size(), pos, 0));
    raw.resize(pos);
  }

  void attach_payload(RedoDataNode &redo);
  void append_lob_row(ObMutatorWriter &writer, const int expected_ret = OB_SUCCESS);

  ObObj key_obj_;
  ObMemtableMutatorRow row_;
  ObLobOutTableRedoPayload payload_;
  ObCLogEncryptInfo encrypt_info_;
  std::string payload_buf_;
private:
  friend class TestLobOuttableMutator_lob_digest_covers_contents_and_object_identity_Test;
  static uint64_t calc_payload_digest_(const ObLobOutTableRedoPayload &payload);
};

TEST_F(TestLobOuttableMutator, insert_preserves_native_row_encoding)
{
  EXPECT_EQ(4, static_cast<int>(MutatorType::MUTATOR_LOB_OUTTABLE_ROW));
  ASSERT_NO_FATAL_FAILURE(check_roundtrip(ObDmlFlag::DF_INSERT));
}

TEST_F(TestLobOuttableMutator, update_preserves_native_row_encoding)
{
  ASSERT_NO_FATAL_FAILURE(check_roundtrip(ObDmlFlag::DF_UPDATE));
}

TEST_F(TestLobOuttableMutator, mixed_native_and_lob_rows_clear_payload_between_rows)
{
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  ASSERT_EQ(3, iter.get_meta().get_row_count());
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
  EXPECT_EQ(row_.column_cnt_, iter.get_mutator_row().get_column_cnt());
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
}

TEST_F(TestLobOuttableMutator, two_mib_payload_retries_without_publishing_partial_row)
{
  std::string object_data(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'x');
  payload_.data_.assign_ptr(object_data.data(), object_data.size());
  payload_.byte_len_ = object_data.size();
  ASSERT_TRUE(payload_.is_valid());
  std::string small_buf(1024, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&small_buf[0], small_buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer, OB_BUF_NOT_ENOUGH));
  EXPECT_EQ(0, writer.get_meta().get_row_count());
  int64_t length = 0;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST,
      writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));

  std::string buf(object_data.size() + 4096, '\0');
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  EXPECT_EQ(1, writer.get_meta().get_row_count());
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
  ASSERT_NO_FATAL_FAILURE(check_borrowed_data(iter.get_lob_outtable_payload().data_, buf, length));
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, failed_tail_does_not_advance_existing_writer_position)
{
  std::string object_data(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'x');
  payload_.data_.assign_ptr(object_data.data(), object_data.size());
  payload_.byte_len_ = object_data.size();
  std::string buf(1024, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer, OB_BUF_NOT_ENOUGH));
  EXPECT_EQ(1, writer.get_meta().get_row_count());
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  ASSERT_EQ(2, iter.get_meta().get_row_count());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
    EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
    EXPECT_EQ(row_.new_row_, iter.get_mutator_row().new_row_);
    ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  }
  EXPECT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, nologging_retains_identity_and_length_without_object_data)
{
  payload_.is_logging_ = false;
  payload_.byte_len_ = ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE;
  payload_.data_.reset();
  ASSERT_TRUE(payload_.is_valid());
  ASSERT_NO_FATAL_FAILURE(check_roundtrip(ObDmlFlag::DF_INSERT));
}

TEST_F(TestLobOuttableMutator, truncated_payload_is_not_a_valid_native_row)
{
  std::string raw;
  ASSERT_NO_FATAL_FAILURE(serialize_raw_lob_row(raw, payload_));
  raw.resize(raw.size() - 1);
  ASSERT_NO_FATAL_FAILURE(check_bad_tail(raw));
}

TEST_F(TestLobOuttableMutator, truncated_packet_cannot_extend_iterator_past_input)
{
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  int64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t pos = 0;
  // Preserve the original declared length; only the received packet is one byte shorter.
  EXPECT_EQ(OB_DESERIALIZE_ERROR, iter.deserialize(buf.data(), length - 1, pos, encrypt_info_));
  EXPECT_EQ(0, pos);
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  EXPECT_EQ(OB_NOT_INIT, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_EQ(OB_SUCCESS, iter.deserialize(buf.data(), length, pos, encrypt_info_));
  EXPECT_EQ(length, pos);
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
}

TEST_F(TestLobOuttableMutator, invalid_payload_fields_do_not_publish_native_meta_row)
{
  ObLobOutTableRedoPayload invalid_payload = payload_;
  ++invalid_payload.byte_len_;
  ASSERT_FALSE(invalid_payload.is_valid());
  std::string raw;
  ASSERT_NO_FATAL_FAILURE(serialize_raw_lob_row(raw, invalid_payload));
  std::string buf(raw.size() + 4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_EQ(OB_SUCCESS, writer.append_row_buf(raw.data(), raw.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  // A complete UNIS frame is not sufficient if the object data contradicts its byte length.
  EXPECT_EQ(OB_INVALID_LOG, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  EXPECT_FALSE(iter.get_row_seq_no().is_valid());
  EXPECT_EQ(ObDmlFlag::DF_NOT_EXIST, iter.get_mutator_row().dml_flag_);
  EXPECT_EQ(0, iter.get_mutator_row().new_row_.size_);
  EXPECT_EQ(OB_INVALID_LOG, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, invalid_payload_frame_length_is_rejected)
{
  // Include addition overflow, large but non-overflowing lengths, and one missing body byte.
  const int64_t invalid_lengths[] = {-1, INT64_MAX, INT32_MAX, 2};
  std::string raw;
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid_lengths); ++i) {
    SCOPED_TRACE(invalid_lengths[i]);
    ASSERT_NO_FATAL_FAILURE(serialize_bad_payload_frame(raw, invalid_lengths[i]));
    ASSERT_NO_FATAL_FAILURE(check_bad_tail(raw));
  }
}

TEST_F(TestLobOuttableMutator, reset_drops_borrowed_payload)
{
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_TRUE(iter.get_lob_outtable_payload().is_valid());
  iter.reset();
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  EXPECT_EQ(OB_NOT_INIT, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, deserialize_reuse_clears_previous_payload)
{
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, iter.deserialize(buf.data(), length, pos, encrypt_info_));
  EXPECT_EQ(length, pos);
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
}

TEST_F(TestLobOuttableMutator, invalid_append_arguments_do_not_publish_rows)
{
  ObITransCallback callback;
  RedoDataNode redo;
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
  ObMutatorWriter writer;
  EXPECT_EQ(OB_NOT_INIT, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
  std::string buf(4096, '\0');
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  redo.tablet_id_ = ObTabletID();
  EXPECT_EQ(OB_ERR_UNEXPECTED, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
  EXPECT_EQ(0, writer.get_meta().get_row_count());
}

TEST_F(TestLobOuttableMutator, encryption_is_rejected_before_serializing_plaintext_tail)
{
  ObITransCallback callback;
  RedoDataNode redo;
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ObTxEncryptMeta encrypt_meta;
  encrypt_meta.table_id_ = 1;
  encrypt_meta.meta_.encrypt_algorithm_ = 1;
  encrypt_meta.meta_.master_key_version_ = 1;
  ASSERT_EQ(OB_SUCCESS, encrypt_meta.meta_.master_key_.set_content("master", 6));
  ASSERT_EQ(OB_SUCCESS, encrypt_meta.meta_.table_key_.set_content("table", 5));
  ASSERT_EQ(OB_SUCCESS, encrypt_meta.meta_.encrypted_table_key_.set_content("encrypted", 9));
  ASSERT_TRUE(encrypt_meta.is_valid());
  EXPECT_EQ(OB_NOT_SUPPORTED,
      writer.append_row_kv(row_.table_version_, redo, &encrypt_meta, encrypt_info_));
  EXPECT_EQ(0, writer.get_meta().get_row_count());
}

TEST_F(TestLobOuttableMutator, row_kv_bridge_roundtrips_insert_update_and_nologging)
{
  ObITransCallback callback;
  for (int64_t i = 0; i < 3; ++i) {
    SCOPED_TRACE(i);
    row_.dml_flag_ = 1 == i ? ObDmlFlag::DF_UPDATE : ObDmlFlag::DF_INSERT;
    row_.old_row_.reset();
    if (1 == i) {
      row_.old_row_.set("old meta row", 12);
    } else if (2 == i) {
      payload_.is_logging_ = false;
      payload_.data_.reset();
    }
    RedoDataNode redo;
    ASSERT_TRUE(redo.outtable_redo_payload_buf_.empty());
    ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
    ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
    std::string buf(4096, '\0');
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
    ASSERT_EQ(OB_SUCCESS, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
    ASSERT_EQ(1, writer.get_meta().get_row_count());
    ObMemtableMutatorIterator iter;
    ObEncryptRowBuf row_buf;
    int64_t length = 0;
    ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
    ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
    ASSERT_NO_FATAL_FAILURE(check_row(iter, row_.dml_flag_));
    ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
    EXPECT_EQ(redo.outtable_redo_payload_buf_, iter.get_lob_outtable_payload_buf());
    ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
  }
}

TEST_F(TestLobOuttableMutator, row_kv_without_payload_preserves_native_bytes)
{
  ObITransCallback callback;
  for (int64_t i = 0; i < 2; ++i) {
    SCOPED_TRACE(i);
    RedoDataNode redo;
    ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, 0 == i ? nullptr : &callback));
    std::string expected;
    if (0 == i) {
      expected.assign(4096, '\0');
      int64_t capacity = expected.size();
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, row_.serialize(&expected[0], capacity, pos, nullptr, encrypt_info_));
      expected.resize(pos);
    } else {
      ASSERT_NO_FATAL_FAILURE(serialize_native_row(expected, MutatorType::MUTATOR_ROW));
    }
    std::string buf(4096, '\0');
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
    ASSERT_EQ(OB_SUCCESS, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
    int64_t length = 0;
    ASSERT_EQ(OB_SUCCESS, writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));
    ASSERT_EQ(1, writer.get_meta().get_row_count());
    ASSERT_EQ(writer.get_meta().get_meta_size() + expected.size(), length);
    EXPECT_EQ(expected,
        std::string(buf.data() + writer.get_meta().get_meta_size(), expected.size()));
  }
}

TEST_F(TestLobOuttableMutator, redo_set_clears_previous_outtable_payload)
{
  ObITransCallback callback;
  RedoDataNode redo;
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_TRUE(redo.outtable_redo_payload_buf_.empty());
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_EQ(OB_SUCCESS, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, invalid_row_kv_attachment_does_not_advance_writer)
{
  ObITransCallback callback;
  RedoDataNode redo;
  ObTxEncryptMeta encrypt_meta;
  encrypt_meta.table_id_ = 1;
  encrypt_meta.meta_.encrypt_algorithm_ = 1;
  encrypt_meta.meta_.master_key_version_ = 1;
  ASSERT_EQ(OB_SUCCESS, encrypt_meta.meta_.master_key_.set_content("master", 6));
  ASSERT_EQ(OB_SUCCESS, encrypt_meta.meta_.table_key_.set_content("table", 5));
  ASSERT_EQ(OB_SUCCESS, encrypt_meta.meta_.encrypted_table_key_.set_content("encrypted", 9));
  ASSERT_TRUE(encrypt_meta.is_valid());
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  int64_t length = 0;
  ASSERT_EQ(OB_SUCCESS, writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));
  const std::string prefix(buf.data(), length);
  const int expected_errors[] = {
      OB_INVALID_ARGUMENT, OB_NOT_SUPPORTED, OB_NOT_SUPPORTED};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_errors); ++i) {
    SCOPED_TRACE(i);
    ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, 0 == i ? nullptr : &callback));
    ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
    EXPECT_EQ(expected_errors[i], writer.append_row_kv(row_.table_version_, redo,
        2 == i ? &encrypt_meta : nullptr, encrypt_info_, 1 == i));
    EXPECT_EQ(1, writer.get_meta().get_row_count());
    ASSERT_EQ(OB_SUCCESS, writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));
    EXPECT_EQ(prefix, std::string(buf.data(), length));
  }
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, buf, iter, length));
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
    EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
  }
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, row_kv_two_mib_retry_preserves_preceding_rows)
{
  std::string object_data(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'x');
  payload_.data_.assign_ptr(object_data.data(), object_data.size());
  payload_.byte_len_ = object_data.size();
  ObITransCallback callback;
  RedoDataNode redo;
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
  std::string small_buf(1024, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&small_buf[0], small_buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH,
      writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
  EXPECT_EQ(1, writer.get_meta().get_row_count());
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t length = 0;
  ASSERT_NO_FATAL_FAILURE(open_iterator(writer, small_buf, iter, length));
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
    EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
    ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  }
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
  std::string buf(object_data.size() + 4096, '\0');
  ObMutatorWriter retry_writer;
  ASSERT_EQ(OB_SUCCESS, retry_writer.set_buffer(&buf[0], buf.size()));
  ASSERT_EQ(OB_SUCCESS,
      retry_writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
  EXPECT_EQ(1, retry_writer.get_meta().get_row_count());
  ASSERT_NO_FATAL_FAILURE(open_iterator(retry_writer, buf, iter, length));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_row(iter, ObDmlFlag::DF_INSERT));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
  ASSERT_NO_FATAL_FAILURE(check_borrowed_data(iter.get_lob_outtable_payload().data_, buf, length));
  ASSERT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
}

TEST_F(TestLobOuttableMutator, two_mib_mutator_extends_native_tx_log_block)
{
  std::string object_data(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'x');
  payload_.data_.assign_ptr(object_data.data(), object_data.size());
  payload_.byte_len_ = object_data.size();
  ObITransCallback callback;
  RedoDataNode redo;
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
  const ObAddr scheduler(ObAddr::VER::IPV4, "127.0.0.1", 2882);
  ObTxLogBlock block;
  block.get_header().init(1, DATA_CURRENT_VERSION, 0, ObTransID(1), scheduler);
  ASSERT_EQ(OB_SUCCESS, block.init_for_fill(ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE));
  ObTxRedoLog fill_redo(DATA_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, block.prepare_mutator_buf(fill_redo));
  ASSERT_LT(fill_redo.get_mutator_size(), object_data.size());
  {
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS,
        writer.set_buffer(fill_redo.get_mutator_buf(), fill_redo.get_mutator_size()));
    EXPECT_EQ(OB_BUF_NOT_ENOUGH,
        writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
    EXPECT_EQ(0, writer.get_meta().get_row_count());
  }
  // Follow the submitter's empty-buffer retry without using legacy BIG_ROW framing.
  ASSERT_EQ(OB_SUCCESS, block.finish_mutator_buf(fill_redo, 0));
  ASSERT_EQ(OB_SUCCESS, block.extend_log_buf());
  ASSERT_EQ(OB_SUCCESS, block.prepare_mutator_buf(fill_redo));
  ASSERT_GT(fill_redo.get_mutator_size(), object_data.size());
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS,
      writer.set_buffer(fill_redo.get_mutator_buf(), fill_redo.get_mutator_size()));
  ASSERT_EQ(OB_SUCCESS,
      writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  EXPECT_EQ(2, writer.get_meta().get_row_count());
  int64_t mutator_length = 0;
  ASSERT_EQ(OB_SUCCESS,
      writer.serialize(ObTransRowFlag::NORMAL_ROW, mutator_length, encrypt_info_));
  ASSERT_EQ(OB_SUCCESS, block.finish_mutator_buf(fill_redo, mutator_length));
  ASSERT_EQ(OB_SUCCESS, block.add_new_log(fill_redo));
  ASSERT_EQ(OB_SUCCESS, block.seal(1));
  ASSERT_GT(block.get_size(), static_cast<int64_t>(ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE));
  ASSERT_LE(block.get_size(), static_cast<int64_t>(ObTxAdaptiveLogBuf::BIG_LOG_BUF_SIZE));

  ObTxLogBlock replay_block;
  ObTxLogHeader header;
  ObTxRedoLogTempRef temp_ref;
  ObTxRedoLog replay_redo(temp_ref);
  ASSERT_EQ(OB_SUCCESS, replay_block.init_for_replay(block.get_buf(), block.get_size()));
  ASSERT_EQ(OB_SUCCESS, replay_block.get_next_log(header));
  EXPECT_EQ(ObTxLogType::TX_REDO_LOG, header.get_tx_log_type());
  ASSERT_EQ(OB_SUCCESS, replay_block.deserialize_log_body(replay_redo));
  EXPECT_EQ(mutator_length, replay_redo.get_mutator_size());
  ObMemtableMutatorIterator iter;
  ObEncryptRowBuf row_buf;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, iter.deserialize(replay_redo.get_replay_mutator_buf(),
      replay_redo.get_mutator_size(), pos, encrypt_info_));
  EXPECT_EQ(mutator_length, pos);
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  ASSERT_NO_FATAL_FAILURE(check_row(iter, ObDmlFlag::DF_INSERT));
  ASSERT_NO_FATAL_FAILURE(check_payload(iter.get_lob_outtable_payload()));
  ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
  EXPECT_EQ(MutatorType::MUTATOR_ROW, iter.get_row_head().mutator_type_);
  EXPECT_EQ(row_.new_row_, iter.get_mutator_row().new_row_);
  ASSERT_NO_FATAL_FAILURE(check_empty_payload(iter));
  EXPECT_EQ(OB_ITER_END, iter.iterate_next_row(row_buf, encrypt_info_));
  EXPECT_EQ(OB_ITER_END, replay_block.get_next_log(header));
}

class OuttableCallbackMemtableCtx : public ObMemtableCtx
{
public:
  void *old_row_alloc(const int64_t size) override
  {
    ADD_FAILURE() << "LOB redo must use its independent allocation";
    return nullptr;
  }
  void old_row_free(void *ptr) override
  {
    ADD_FAILURE() << "LOB redo must not use old_row_free";
  }
  void inc_pending_log_size(const int64_t size) override { pending_delta_ += size; }
  void *alloc_mvcc_row_callback() override
  {
    void *ptr = fail_callback_alloc_ || callback_alloc_count_ == fail_callback_after_
        ? nullptr : std::malloc(sizeof(ObMvccRowCallback));
    if (nullptr != ptr) {
      ++callback_alloc_count_;
      static_cast<ObITransCallback *>(ptr)->owner_ = 1;
    }
    return ptr;
  }
  void free_mvcc_row_callback(ObITransCallback *cb) override
  {
    if (nullptr != cb) { cb->~ObITransCallback(); std::free(cb); ++callback_free_count_; }
  }
  int append_callback(ObITransCallback *cb) override { return append_callback(cb, cb, 1); }
  int append_callback(ObITransCallback *head, ObITransCallback *tail, const int64_t length) override
  {
    int ret = append_ret_;
    ++append_calls_;
    for (int64_t i = 0; OB_SUCC(ret) && i < length; ++i) {
      ObMvccRowCallback *cb = static_cast<ObMvccRowCallback *>(head);
      head = head->get_next();
      RedoDataNode redo;
      EXPECT_EQ(OB_SUCCESS, cb->get_redo(redo));
      if (submit_on_append_) {
        EXPECT_EQ(OB_SUCCESS, cb->before_append(false));
        share::SCN scn;
        EXPECT_EQ(OB_SUCCESS, scn.convert_for_logservice(100));
        ObIMemtable *last = cb->get_memtable();
        EXPECT_EQ(OB_SUCCESS, cb->log_submitted_cb(scn, last));
        EXPECT_EQ(OB_SUCCESS, cb->clean());
        free_mvcc_row_callback(cb);
      } else {
        ret = published_.push_back(cb);
      }
    }
    return ret;
  }
  bool fail_callback_alloc_ = false;
  bool submit_on_append_ = false;
  int append_ret_ = OB_SUCCESS;
  int64_t append_calls_ = 0;
  int64_t pending_delta_ = 0;
  int64_t callback_alloc_count_ = 0;
  int64_t callback_free_count_ = 0;
  int64_t fail_callback_after_ = -1;
  ObSEArray<ObMvccRowCallback *, 8> published_;
};

class OuttableCallbackMemtable : public ObMemtable
{
public:
  OuttableCallbackMemtable() { key_.tablet_id_ = ObTabletID(200); }
  using ObMemtable::mvcc_engine_;
};

class OuttableNodeBuilder : public ObMvccEngine
{
public:
  OuttableNodeBuilder(ObIAllocator &allocator, ObMTKVBuilder &builder)
  {
    engine_allocator_ = &allocator;
    kv_builder_ = &builder;
  }
  using ObMvccEngine::build_tx_node_for_write_;
  using ObMvccEngine::init_tx_node_for_write_;
  using ObMvccEngine::build_tx_node_for_replay_;
};

class TestLobOuttableCallback : public TestLobOuttableMutator
{
protected:
  TestLobOuttableCallback() : engine_(allocator_, builder_) {}
  void SetUp() override
  {
    TestLobOuttableMutator::SetUp();
    param_.is_non_unique_local_index_ = true;
    store_ctx_.mvcc_acc_ctx_.mem_ctx_ = &ctx_;
    ctx_.tenant_id_ = OB_SYS_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, ctx_.ObIMvccCtx::init(
        ObTransID(), OB_SYS_TENANT_ID, 0, share::ObLSID(), ObAddr()));
    ASSERT_EQ(OB_SYS_TENANT_ID, ctx_.get_tenant_id());
    memtable_.mvcc_engine_ = &engine_;
    reset_mem_leak_checker_label("LobRedoPayload@1");
  }
  void TearDown() override
  {
    for (int64_t i = 0; i < ctx_.published_.count(); ++i) {
      ObMvccRowCallback *cb = ctx_.published_.at(i);
      EXPECT_EQ(OB_SUCCESS, cb->clean());
      ctx_.free_mvcc_row_callback(cb);
    }
    EXPECT_EQ(0, payload_live_count());
    EXPECT_EQ(ctx_.callback_alloc_count_, ctx_.callback_free_count_);
    for (int64_t i = 0; i < values_.count(); ++i) {
      ObMvccRow *value = values_.at(i);
      value->reset();
      value->~ObMvccRow();
    }
    memtable_.mvcc_engine_ = nullptr;
    reset_mem_leak_checker_label("NONE");
  }
  void prepare_row(const ObLobOutTableRedoPayload &payload, ObTxNodeArg &arg,
      ObMvccWriteResult &res, const bool lob = true, const bool replay = false)
  {
    ObObj cells[3];
    cells[0].set_int(payload.lob_col_idx_);
    cells[1].set_varbinary(payload.lob_id_.get_string());
    cells[2].set_varbinary(payload.seq_id_);
    ObStoreRowkey source;
    ASSERT_EQ(OB_SUCCESS, source.assign(cells, 3));
    ObStoreRowkey *key = new(allocator_.alloc(sizeof(ObStoreRowkey))) ObStoreRowkey();
    ASSERT_EQ(OB_SUCCESS, source.deep_copy(*key, allocator_));
    ASSERT_EQ(OB_SUCCESS, res.mtk_.encode(key));
    ObMemtableData *data = new(allocator_.alloc(sizeof(ObMemtableData)))
        ObMemtableData(row_.dml_flag_, row_.new_row_.size_, row_.new_row_.data_);
    if (lob && replay) {
      data->set_payload_kind(ObMemtablePayloadKind::ROW_WITH_LOB_REDO);
      const int64_t size = payload.get_serialize_size();
      char *bytes = static_cast<char *>(allocator_.alloc(size));
      ASSERT_NE(nullptr, bytes);
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, payload.serialize(bytes, size, pos));
      data->lob_redo_info()->init_replay(ObString(pos, bytes));
    }
    arg.data_ = data;
    res.value_ = new(allocator_.alloc(sizeof(ObMvccRow))) ObMvccRow();
    ASSERT_EQ(OB_SUCCESS, values_.push_back(res.value_));
    arg.modify_count_ = row_.update_seq_;
    arg.memstore_version_ = row_.version_;
    arg.seq_no_ = row_.seq_no_;
    arg.write_epoch_ = 0;
    arg.column_cnt_ = row_.column_cnt_;
  }
  int bind_payload(ObTxNodeArg &arg, ObMvccWriteResult &res,
      const ObMemtableSetArg &set_arg)
  {
    return ObMemtable::bind_lob_redo_(set_arg, res.mtk_,
        *const_cast<ObMemtableData *>(arg.data_), arg);
  }
  int bind_payload(ObTxNodeArg &arg, ObMvccWriteResult &res,
      const ObLobOutTableRedoCtx *redo_ctx)
  {
    const ObMemtableSetArg set_arg(nullptr, nullptr, nullptr, nullptr, nullptr,
        1, false, nullptr, redo_ctx);
    return bind_payload(arg, res, set_arg);
  }
  int build_node(ObTxNodeArg &arg, ObMvccWriteResult &res,
      const bool preallocated = false, const bool replay = false)
  {
    int ret = OB_SUCCESS;
    if (replay) {
      ret = engine_.build_tx_node_for_replay_(arg, res.tx_node_);
    } else if (preallocated) {
      res.tx_node_ = static_cast<ObMvccTransNode *>(
          allocator_.alloc(sizeof(ObMvccTransNode) + arg.data_->dup_size()));
      ret = engine_.init_tx_node_for_write_(store_ctx_, arg, res.tx_node_);
    } else {
      ret = engine_.build_tx_node_for_write_(store_ctx_, arg, res.tx_node_);
    }
    if (OB_SUCC(ret)) {
      ObMvccTransNode *next = nullptr;
      ret = res.value_->insert_trans_node(ctx_, *res.tx_node_, allocator_, next);
      res.can_insert_ = true;
      res.need_insert_ = true;
    }
    return ret;
  }
  ObLobOutTableRedoInfo *info(ObMvccWriteResult &res)
  {
    return reinterpret_cast<ObMemtableDataHeader *>(res.tx_node_->buf_)->lob_redo_info();
  }
  int register_row(ObTxNodeArg &arg, ObMvccWriteResult &res,
      const ObLobOutTableRedoCtx *redo_ctx = nullptr)
  {
    int ret = bind_payload(arg, res, redo_ctx);
    if (OB_SUCC(ret)) { ret = build_node(arg, res); }
    if (OB_SUCC(ret)) { ret = ctx_.register_row_commit_cb(param_, arg, res, &memtable_); }
    if (OB_FAIL(ret)) { memtable_.mvcc_undo_(res); }
    return ret;
  }
  int register_batch(ObTxNodeArgs &args, ObMvccWriteResults &results,
      const ObLobOutTableRedoCtx &redo_ctx)
  {
    const ObMemtableSetArg set_arg(nullptr, nullptr, nullptr, nullptr, nullptr,
        results.count(), false, nullptr, &redo_ctx);
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < results.count(); ++i) {
      ret = bind_payload(args.at(i), results.at(i), set_arg);
      if (OB_SUCC(ret)) { ret = build_node(args.at(i), results.at(i), true); }
    }
    if (OB_SUCC(ret)) { ret = ctx_.register_row_commit_cb(param_, args, results, &memtable_); }
    if (OB_FAIL(ret)) { memtable_.mvcc_undo_(results); }
    return ret;
  }
  static int64_t payload_live_count();
  OuttableCallbackMemtableCtx ctx_;
  OuttableCallbackMemtable memtable_;
  ObTableIterParam param_;
  ObStoreCtx store_ctx_;
  ObArenaAllocator allocator_;
  ObMTKVBuilder builder_;
  OuttableNodeBuilder engine_;
  ObSEArray<ObMvccRow *, 8> values_;
private:
  friend class TestLobOuttableCallback_replay_digest_matches_leader_without_retaining_body_Test;
  static void bind_callback_(ObMvccRowCallback &cb, ObMvccWriteResult &res, const ObTxNodeArg &arg);
};

TEST_F(TestLobOuttableCallback, plain_rows_keep_native_layout_and_callback)
{
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res, false));
  ASSERT_EQ(OB_SUCCESS, register_row(arg, res));
  EXPECT_EQ(nullptr, info(res));
  EXPECT_EQ(sizeof(ObMemtableDataHeader) + row_.new_row_.size_, res.tx_node_->get_data_size());
  RedoDataNode redo;
  ASSERT_EQ(OB_SUCCESS, ctx_.published_.at(0)->get_redo(redo));
  EXPECT_TRUE(redo.outtable_redo_payload_buf_.empty());
  EXPECT_EQ(row_.new_row_, redo.new_row_);
  EXPECT_EQ(0, payload_live_count());
}

TEST_F(TestLobOuttableCallback, owned_payload_survives_source_reuse)
{
  for (int64_t i = 0; i < 3; ++i) {
    row_.dml_flag_ = 1 == i ? ObDmlFlag::DF_UPDATE : ObDmlFlag::DF_INSERT;
    std::string source(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'x');
    payload_.is_logging_ = 2 != i;
    payload_.byte_len_ = source.size();
    payload_.data_.assign_ptr(source.data(), payload_.is_logging_ ? source.size() : 0);
    ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
    ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
    ObLobOutTableRedoCtx redo_ctx(payloads);
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ASSERT_EQ(OB_SUCCESS, register_row(arg, res, &redo_ctx));
    EXPECT_FALSE(info(res)->checksum_ready_);
    std::string expected(payload_.get_serialize_size(), '\0');
    int64_t expected_pos = 0;
    ASSERT_EQ(OB_SUCCESS, payload_.serialize(&expected[0], expected.size(), expected_pos));
    ASSERT_EQ(expected.size(), expected_pos);
    source.assign(source.size(), 'z');
    RedoDataNode redo;
    ASSERT_EQ(OB_SUCCESS, ctx_.published_.at(i)->get_redo(redo));
    const ObString &wire = redo.outtable_redo_payload_buf_;
    ASSERT_NE(nullptr, info(res)->redo_buf_);
    EXPECT_EQ(info(res)->redo_buf_, wire.ptr());
    ASSERT_EQ(expected_pos, wire.length());
    EXPECT_EQ(expected_pos, info(res)->redo_size_);
    EXPECT_EQ(expected, std::string(wire.ptr(), wire.length()));
    ObLobOutTableRedoPayload decoded;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire.ptr(), wire.length(), pos));
    ASSERT_EQ(wire.length(), pos);
    EXPECT_EQ(payload_.seq_id_, decoded.seq_id_);
    EXPECT_EQ(payload_.macro_id_, decoded.macro_id_);
    EXPECT_EQ(payload_.byte_len_, decoded.byte_len_);
    EXPECT_EQ(payload_.is_logging_, decoded.is_logging_);
    if (payload_.is_logging_) {
      EXPECT_EQ(std::string(source.size(), 'x'),
          std::string(decoded.data_.ptr(), decoded.data_.length()));
    } else {
      EXPECT_TRUE(decoded.data_.empty());
    }
    EXPECT_EQ(row_.new_row_, redo.new_row_);
    // A failed log-buffer fill can retry without losing or reallocating bytes.
    const char *first_payload = wire.ptr();
    ASSERT_EQ(OB_SUCCESS, ctx_.published_.at(i)->get_redo(redo));
    EXPECT_EQ(first_payload, wire.ptr());
    EXPECT_EQ(expected_pos, wire.length());
    EXPECT_EQ(expected, std::string(wire.ptr(), wire.length()));
    EXPECT_EQ(i + 1, payload_live_count());
    EXPECT_EQ(sizeof(ObMemtableDataHeader) + row_.new_row_.size_ + payload_.get_serialize_size(),
        ctx_.published_.at(i)->get_data_size());
  }
}

TEST_F(TestLobOuttableCallback, submission_releases_body_preserves_checksum_and_accounting)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
  ASSERT_EQ(OB_SUCCESS, register_row(arg, res, &redo_ctx));
  ObMvccRowCallback *cb = ctx_.published_.at(0);
  const int64_t size = cb->get_data_size();
  EXPECT_EQ(1, payload_live_count());
  TxChecksum before, after;
  EXPECT_FALSE(info(res)->checksum_ready_);
  ASSERT_EQ(OB_SUCCESS, cb->calc_checksum(share::SCN::min_scn(), &before));
  EXPECT_TRUE(info(res)->checksum_ready_);
  ASSERT_EQ(OB_SUCCESS, cb->before_append(false));
  share::SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_logservice(100));
  ObIMemtable *last = &memtable_;
  ASSERT_EQ(OB_SUCCESS, cb->log_submitted_cb(scn, last));
  EXPECT_EQ(0, payload_live_count());
  EXPECT_EQ(nullptr, info(res)->redo_buf_);
  EXPECT_EQ(size, cb->get_data_size());
  ASSERT_EQ(OB_SUCCESS, cb->calc_checksum(share::SCN::min_scn(), &after));
  EXPECT_EQ(before.calc(), after.calc());
  RedoDataNode redo;
  EXPECT_EQ(OB_STATE_NOT_MATCH, cb->get_redo(redo));
  EXPECT_TRUE(redo.outtable_redo_payload_buf_.empty());
  ASSERT_EQ(OB_SUCCESS, cb->log_submitted_cb(scn, last));
  EXPECT_EQ(0, payload_live_count());
  // Sync failure only rolls back the node, never reads the freed redo body.
  ASSERT_EQ(OB_SUCCESS, cb->log_sync_fail(scn));
  EXPECT_TRUE(res.value_->is_empty());
}

TEST_F(TestLobOuttableCallback, release_before_checksum_caches_binary_digest)
{
  std::string data(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, '\0');
  data.front() = 'a';
  data.back() = 'z';
  for (int64_t logging = 0; logging < 2; ++logging) {
    payload_.is_logging_ = logging;
    payload_.byte_len_ = data.size();
    payload_.data_.assign_ptr(data.data(), logging ? data.size() : 0);
    for (int64_t cleanup = 0; cleanup < 3; ++cleanup) {
      SCOPED_TRACE(cleanup + logging * 3);
      ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
      ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
      ObLobOutTableRedoCtx redo_ctx(payloads);
      ObTxNodeArg arg;
      ObMvccWriteResult res;
      ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
      ASSERT_EQ(OB_SUCCESS, register_row(arg, res, &redo_ctx));
      ObMvccRowCallback *cb = ctx_.published_.at(0);
      RedoDataNode redo;
      ASSERT_EQ(OB_SUCCESS, cb->get_redo(redo));
      std::string log(data.size() + 4096, '\0');
      ObMutatorWriter writer;
      ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&log[0], log.size()));
      ASSERT_EQ(OB_SUCCESS, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
      ObMemtableMutatorIterator iter;
      ObEncryptRowBuf row_buf;
      int64_t length = 0;
      ASSERT_NO_FATAL_FAILURE(open_iterator(writer, log, iter, length));
      ASSERT_EQ(OB_SUCCESS, iter.iterate_next_row(row_buf, encrypt_info_));
      const ObString &wire = iter.get_lob_outtable_payload_buf();
      const int64_t redo_size = info(res)->redo_size_;
      ASSERT_EQ(payload_.get_serialize_size(), redo_size);
      ASSERT_EQ(redo_size, wire.length());
      EXPECT_EQ(info(res)->redo_buf_, redo.outtable_redo_payload_buf_.ptr());
      EXPECT_EQ(redo.outtable_redo_payload_buf_, wire);
      const uint64_t digest = ob_crc64(wire.ptr(), wire.length());
      ObLobOutTableRedoInfo replay;
      replay.init_replay(wire);
      // Replay retains no borrowed bytes after this buffer is reused.
      log.assign(log.size(), 'x');
      EXPECT_FALSE(info(res)->checksum_ready_);
      if (0 == cleanup) {
        ASSERT_EQ(OB_SUCCESS, cb->before_append(false));
        share::SCN scn;
        ASSERT_EQ(OB_SUCCESS, scn.convert_for_logservice(100));
        ObIMemtable *last = &memtable_;
        ASSERT_EQ(OB_SUCCESS, cb->log_submitted_cb(scn, last));
      } else if (1 == cleanup) {
        ASSERT_EQ(OB_SUCCESS, cb->before_append(false));
        ASSERT_EQ(OB_SUCCESS, cb->clean_unlog_cb());
      } else {
        ASSERT_EQ(OB_SUCCESS, cb->clean());
        ctx_.free_mvcc_row_callback(cb);
      }
      EXPECT_EQ(nullptr, info(res)->redo_buf_);
      EXPECT_EQ(redo_size, info(res)->redo_size_);
      EXPECT_TRUE(info(res)->checksum_ready_);
      EXPECT_EQ(digest, info(res)->checksum_);
      EXPECT_EQ(digest, replay.checksum_);
      ObBatchChecksum expected, actual, replayed;
      expected.fill(&digest, sizeof(digest));
      info(res)->checksum(actual);
      replay.checksum(replayed);
      EXPECT_EQ(expected.calc(), actual.calc());
      EXPECT_EQ(expected.calc(), replayed.calc());
      if (cleanup != 2) {
        TxChecksum tx_expected, tx_actual;
        res.tx_node_->checksum(tx_expected);
        reinterpret_cast<ObMemtableDataHeader *>(res.tx_node_->buf_)->checksum(tx_expected);
        tx_expected.fill(&digest, sizeof(digest));
        ASSERT_EQ(OB_SUCCESS, cb->calc_checksum(share::SCN::min_scn(), &tx_actual));
        EXPECT_EQ(tx_expected.calc(), tx_actual.calc());
        ASSERT_EQ(OB_SUCCESS, cb->clean());
        ctx_.free_mvcc_row_callback(cb);
      }
      ctx_.published_.reset();
      info(res)->release();
      EXPECT_EQ(0, payload_live_count());
    }
  }
}

TEST_F(TestLobOuttableCallback, rollback_accounts_for_payload_and_destruction_frees_it)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
  ASSERT_EQ(OB_SUCCESS, register_row(arg, res, &redo_ctx));
  ObMvccRowCallback *cb = ctx_.published_.at(0);
  ctx_.inc_pending_log_size(cb->get_data_size());
  ASSERT_EQ(OB_SUCCESS, cb->rollback_callback());
  EXPECT_EQ(0, ctx_.pending_delta_);
  EXPECT_TRUE(res.value_->is_empty());
  ctx_.free_mvcc_row_callback(cb);
  ctx_.published_.reset();
  EXPECT_EQ(0, payload_live_count());
  EXPECT_EQ(nullptr, info(res)->redo_buf_);
}

TEST_F(TestLobOuttableCallback, publication_can_submit_and_free_callback_before_return)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
  ctx_.submit_on_append_ = true;
  ASSERT_EQ(OB_SUCCESS, register_row(arg, res, &redo_ctx));
  EXPECT_EQ(0, payload_live_count());
  EXPECT_EQ(1, ctx_.callback_free_count_);
  EXPECT_EQ(nullptr, info(res)->redo_buf_);
}

TEST_F(TestLobOuttableCallback, unlogged_callback_cleanup_releases_node_payload)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
  ASSERT_EQ(OB_SUCCESS, register_row(arg, res, &redo_ctx));
  ObMvccRowCallback *cb = ctx_.published_.at(0);
  ASSERT_EQ(OB_SUCCESS, cb->before_append(false));
  ASSERT_EQ(OB_SUCCESS, cb->clean_unlog_cb());
  EXPECT_EQ(0, payload_live_count());
  EXPECT_EQ(nullptr, info(res)->redo_buf_);
  ctx_.free_mvcc_row_callback(cb);
  ctx_.published_.reset();
  EXPECT_EQ(0, payload_live_count());
}

TEST_F(TestLobOuttableCallback, write_failure_before_node_insertion_releases_payload)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  store_ctx_.mvcc_acc_ctx_.snapshot_.version_ = share::SCN::min_scn();
  for (int64_t preallocated = 0; preallocated < 2; ++preallocated) {
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ASSERT_EQ(OB_SUCCESS, bind_payload(arg, res, &redo_ctx));
    res.value_->update_max_trans_version(share::SCN::max_scn(), ObTransID(1));
    if (preallocated) {
      void *buf = allocator_.alloc(sizeof(ObMvccTransNode) + arg.data_->dup_size());
      ASSERT_EQ(OB_TRANSACTION_SET_VIOLATION,
          engine_.mvcc_write(store_ctx_, *res.value_, arg, false, buf, res));
    } else {
      ASSERT_EQ(OB_TRANSACTION_SET_VIOLATION,
          engine_.mvcc_write(store_ctx_, *res.value_, arg, false, res));
    }
    EXPECT_EQ(nullptr, res.tx_node_);
    EXPECT_TRUE(res.value_->is_empty());
    EXPECT_EQ(0, payload_live_count());
    EXPECT_EQ(0, ctx_.callback_alloc_count_);
  }
}

TEST_F(TestLobOuttableCallback, put_node_binds_payload_during_construction)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  row_.dml_flag_ = ObDmlFlag::DF_PUT;
  for (int64_t preallocated = 0; preallocated < 2; ++preallocated) {
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ASSERT_EQ(OB_SUCCESS, bind_payload(arg, res, &redo_ctx));
    ASSERT_EQ(OB_SUCCESS, build_node(arg, res, preallocated));
    ASSERT_NE(nullptr, info(res)->redo_buf_);
    ASSERT_EQ(payload_.get_serialize_size(), info(res)->redo_size_);
    ObLobOutTableRedoPayload decoded;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        decoded.deserialize(info(res)->redo_buf_, info(res)->redo_size_, pos));
    ASSERT_EQ(info(res)->redo_size_, pos);
    ASSERT_NO_FATAL_FAILURE(check_payload(decoded));
    // Already-undone nodes must still release the node-owned payload.
    res.value_->mvcc_undo();
    res.is_mvcc_undo_ = true;
    EXPECT_FALSE(res.has_insert());
    memtable_.mvcc_undo_(res);
    EXPECT_EQ(nullptr, info(res)->redo_buf_);
    EXPECT_EQ(0, payload_live_count());
  }
}

TEST_F(TestLobOuttableCallback, batch_undo_releases_active_and_already_undone_payloads_once)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObMvccWriteResults results;
  for (int64_t i = 0; i < 2; ++i) {
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ASSERT_EQ(OB_SUCCESS, bind_payload(arg, res, &redo_ctx));
    ASSERT_EQ(OB_SUCCESS, build_node(arg, res, true));
    if (0 == i) {
      res.value_->mvcc_undo();
      res.is_mvcc_undo_ = true;
    }
    ASSERT_NE(nullptr, info(res)->redo_buf_);
    ASSERT_EQ(OB_SUCCESS, results.push_back(res));
  }
  ASSERT_EQ(OB_SUCCESS, results.push_back(ObMvccWriteResult()));
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    memtable_.mvcc_undo_(results);
    for (int64_t i = 0; i < 2; ++i) {
      EXPECT_TRUE(results.at(i).is_mvcc_undo_);
      EXPECT_FALSE(results.at(i).has_insert());
      EXPECT_TRUE(results.at(i).value_->is_empty());
      EXPECT_EQ(nullptr, info(results.at(i))->redo_buf_);
      EXPECT_TRUE(info(results.at(i))->checksum_ready_);
    }
    EXPECT_EQ(nullptr, results.at(2).tx_node_);
    EXPECT_EQ(0, payload_live_count());
  }
}

TEST_F(TestLobOuttableCallback, both_callback_allocators_run_payload_destructor)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObMemtableCtxCbAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(OB_SYS_TENANT_ID));
  ObMemtableCtxObjPool pool(allocator);
  ObTransCallbackMgr manager(ctx_, allocator, pool);
  for (int64_t parallel = 0; parallel < 2; ++parallel) {
    manager.parallel_stat_ = parallel ? ObTransCallbackMgr::PARALLEL_STMT : 0;
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ASSERT_EQ(OB_SUCCESS, bind_payload(arg, res, &redo_ctx));
    ASSERT_EQ(OB_SUCCESS, build_node(arg, res));
    void *buf = manager.alloc_mvcc_row_callback();
    ASSERT_NE(nullptr, buf);
    ObMvccRowCallback *cb = new(buf) ObMvccRowCallback(ctx_, *res.value_, &memtable_);
    EXPECT_EQ(parallel, cb->owner_);
    ObRowData empty;
    cb->set(&res.mtk_, res.tx_node_, res.tx_node_->get_data_size(), empty,
        false, arg.seq_no_, arg.column_cnt_, true);
    manager.free_mvcc_row_callback(cb);
    EXPECT_EQ(0, payload_live_count());
    EXPECT_EQ(nullptr, info(res)->redo_buf_);
  }
  pool.reset();
  allocator.reset();
}

TEST_F(TestLobOuttableCallback, allocation_and_append_failures_leave_cleanup_to_writer)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  for (int64_t i = 1; i < 3; ++i) {
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ctx_.fail_callback_alloc_ = 1 == i;
    ctx_.append_ret_ = 2 == i ? OB_EAGAIN : OB_SUCCESS;
    EXPECT_EQ(2 == i ? OB_EAGAIN : OB_ALLOCATE_MEMORY_FAILED, register_row(arg, res, &redo_ctx));
    EXPECT_EQ(nullptr, info(res)->redo_buf_);
    EXPECT_TRUE(ctx_.published_.empty());
    EXPECT_EQ(0, payload_live_count());
    EXPECT_TRUE(res.value_->is_empty());
    EXPECT_TRUE(res.is_mvcc_undo_);
  }
}

TEST_F(TestLobOuttableCallback, batch_matches_full_keys_and_cleans_partial_failure)
{
  ObLobOutTableRedoPayload second = payload_;
  ++second.lob_col_idx_;
  second.macro_id_.set_second_id(101);
  for (int64_t fail = 0; fail < 2; ++fail) {
    ObSEArray<ObLobOutTableRedoPayload, 2> payloads;
    if (!fail) { ASSERT_EQ(OB_SUCCESS, payloads.push_back(second)); }
    ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
    ObLobOutTableRedoCtx redo_ctx(payloads);
    ObTxNodeArgs args;
    ObMvccWriteResults results;
    for (int64_t i = 0; i < 2; ++i) {
      ObTxNodeArg arg;
      ObMvccWriteResult res;
      ASSERT_NO_FATAL_FAILURE(prepare_row(0 == i ? payload_ : second, arg, res));
      ASSERT_EQ(OB_SUCCESS, args.push_back(arg));
      ASSERT_EQ(OB_SUCCESS, results.push_back(res));
    }
    EXPECT_EQ(fail ? OB_ENTRY_NOT_EXIST : OB_SUCCESS, register_batch(args, results, redo_ctx));
    if (!fail) {
      for (int64_t i = 0; i < 2; ++i) {
        RedoDataNode redo;
        ASSERT_EQ(OB_SUCCESS, ctx_.published_.at(i)->get_redo(redo));
        ObLobOutTableRedoPayload decoded;
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, decoded.deserialize(redo.outtable_redo_payload_buf_.ptr(),
            redo.outtable_redo_payload_buf_.length(), pos));
        EXPECT_EQ(0 == i ? payload_.macro_id_ : second.macro_id_, decoded.macro_id_);
      }
    } else {
      EXPECT_EQ(nullptr, info(results.at(0))->redo_buf_);
      EXPECT_EQ(nullptr, results.at(1).tx_node_);
      EXPECT_EQ(nullptr, args.at(1).lob_redo_payload_);
    }
  }
}

TEST_F(TestLobOuttableCallback, sub_batches_and_retries_share_read_only_payloads)
{
  ObLobOutTableRedoPayload second = payload_;
  second.seq_id_ = ObString("second seq");
  second.macro_id_.set_second_id(101);
  ObLobOutTableRedoPayload third = payload_;
  ++third.lob_col_idx_;
  third.macro_id_.set_second_id(102);
  ObSEArray<ObLobOutTableRedoPayload, 3> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(third));
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(second));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    ObTxNodeArgs args;
    ObMvccWriteResults results;
    for (int64_t i = 0; i < 2; ++i) {
      ObTxNodeArg arg;
      ObMvccWriteResult res;
      ASSERT_NO_FATAL_FAILURE(prepare_row(0 == i ? second : third, arg, res));
      ASSERT_EQ(OB_SUCCESS, args.push_back(arg));
      ASSERT_EQ(OB_SUCCESS, results.push_back(res));
    }
    ASSERT_EQ(OB_SUCCESS, register_batch(args, results, redo_ctx));
    EXPECT_EQ(&payloads.at(2), args.at(0).lob_redo_payload_);
    EXPECT_EQ(&payloads.at(0), args.at(1).lob_redo_payload_);
    for (int64_t i = 0; i < 2; ++i) {
      const ObLobOutTableRedoPayload &expected = (0 == i) ? second : third;
      ObLobOutTableRedoInfo *redo_info = info(results.at(i));
      ASSERT_NE(nullptr, redo_info->redo_buf_);
      ASSERT_EQ(expected.get_serialize_size(), redo_info->redo_size_);
      ObLobOutTableRedoPayload decoded;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, decoded.deserialize(redo_info->redo_buf_, redo_info->redo_size_, pos));
      ASSERT_EQ(redo_info->redo_size_, pos);
      EXPECT_EQ(expected.macro_id_, decoded.macro_id_);
      EXPECT_EQ(expected.lob_col_idx_, decoded.lob_col_idx_);
      EXPECT_EQ(expected.seq_id_, decoded.seq_id_);
      EXPECT_EQ(expected.data_, decoded.data_);
      EXPECT_NE(args.at(i).lob_redo_payload_->seq_id_.ptr(), decoded.seq_id_.ptr());
      EXPECT_NE(args.at(i).lob_redo_payload_->data_.ptr(), decoded.data_.ptr());
    }
    EXPECT_NE(info(results.at(0))->redo_buf_, info(results.at(1))->redo_buf_);
    // Roll back this attempt, then reuse the same input collection unchanged.
    for (int64_t i = 0; i < ctx_.published_.count(); ++i) {
      ObMvccRowCallback *cb = ctx_.published_.at(i);
      ctx_.inc_pending_log_size(cb->get_data_size());
      ASSERT_EQ(OB_SUCCESS, cb->rollback_callback());
      ctx_.free_mvcc_row_callback(cb);
    }
    ctx_.published_.reset();
    EXPECT_EQ(0, payload_live_count());
  }
  EXPECT_EQ(3, payloads.count());
  EXPECT_EQ(second.data_, payloads.at(2).data_);

  // An ordinary write on the same store context cannot inherit the LOB batch.
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res, false));
  ASSERT_EQ(OB_SUCCESS, register_row(arg, res));
  EXPECT_EQ(nullptr, arg.lob_redo_payload_);
  EXPECT_EQ(nullptr, info(res));
}

TEST_F(TestLobOuttableCallback, write_and_replay_builders_reject_wrong_payload_inputs)
{
  ObTxNodeArg arg;
  ObMvccWriteResult res;
  ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ASSERT_EQ(OB_SUCCESS, bind_payload(arg, res, &redo_ctx));
  EXPECT_EQ(OB_INVALID_ARGUMENT, engine_.build_tx_node_for_replay_(arg, res.tx_node_));
  EXPECT_EQ(nullptr, res.tx_node_);
  arg.lob_redo_payload_ = nullptr;
  for (int64_t preallocated = 0; preallocated < 2; ++preallocated) {
    EXPECT_EQ(OB_INVALID_ARGUMENT, build_node(arg, res, preallocated));
    EXPECT_EQ(nullptr, info(res)->redo_buf_);
  }
  ObLobOutTableRedoPayload invalid_payload = payload_;
  ++invalid_payload.byte_len_;
  arg.lob_redo_payload_ = &invalid_payload;
  for (int64_t preallocated = 0; preallocated < 2; ++preallocated) {
    EXPECT_EQ(OB_INVALID_ARGUMENT, build_node(arg, res, preallocated));
    EXPECT_EQ(nullptr, info(res)->redo_buf_);
  }
  for (int64_t i = 0; i < 2; ++i) {
    row_.dml_flag_ = 0 == i ? ObDmlFlag::DF_DELETE : ObDmlFlag::DF_LOCK;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, res));
    ASSERT_EQ(OB_SUCCESS, bind_payload(arg, res, &redo_ctx));
    for (int64_t preallocated = 0; preallocated < 2; ++preallocated) {
      EXPECT_EQ(OB_NOT_SUPPORTED, build_node(arg, res, preallocated));
      EXPECT_EQ(nullptr, info(res)->redo_buf_);
    }
  }
  EXPECT_EQ(0, payload_live_count());
}

TEST_F(TestLobOuttableCallback, replay_digest_matches_leader_without_retaining_body)
{
  const ObString object_data = payload_.data_;
  for (int64_t logging = 0; logging < 2; ++logging) {
    SCOPED_TRACE(logging);
    payload_.is_logging_ = (0 != logging);
    payload_.data_ = payload_.is_logging_ ? object_data : ObString();
    ObTxNodeArg arg, replay_arg, plain_arg;
    ObMvccWriteResult leader, follower, plain;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, arg, leader));
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, replay_arg, follower, true, true));
    ASSERT_NO_FATAL_FAILURE(prepare_row(payload_, plain_arg, plain, false));
    ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
    ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
    ObLobOutTableRedoCtx redo_ctx(payloads);
    ASSERT_EQ(OB_SUCCESS, bind_payload(arg, leader, &redo_ctx));
    ASSERT_EQ(OB_SUCCESS, build_node(arg, leader));
    ASSERT_EQ(OB_SUCCESS, build_node(replay_arg, follower, false, true));
    ASSERT_EQ(OB_SUCCESS, build_node(plain_arg, plain));
    ObMvccRowCallback a(ctx_, *leader.value_, &memtable_);
    ObMvccRowCallback b(ctx_, *follower.value_, &memtable_);
    ObMvccRowCallback c(ctx_, *plain.value_, &memtable_);
    bind_callback_(a, leader, arg);
    bind_callback_(b, follower, arg);
    bind_callback_(c, plain, arg);
    TxChecksum ac, bc, cc;
    ASSERT_EQ(OB_SUCCESS, a.calc_checksum(share::SCN::min_scn(), &ac));
    ASSERT_EQ(OB_SUCCESS, b.calc_checksum(share::SCN::min_scn(), &bc));
    ASSERT_EQ(OB_SUCCESS, c.calc_checksum(share::SCN::min_scn(), &cc));
    EXPECT_EQ(ac.calc(), bc.calc());
    EXPECT_NE(ac.calc(), cc.calc());
    EXPECT_EQ(nullptr, info(follower)->redo_buf_);
    // Row accumulated checksums exclude the LOB attachment on both sides.
    ObBatchChecksum row_a, row_b, row_c;
    reinterpret_cast<ObMemtableDataHeader *>(leader.tx_node_->buf_)->checksum(row_a);
    reinterpret_cast<ObMemtableDataHeader *>(follower.tx_node_->buf_)->checksum(row_b);
    reinterpret_cast<ObMemtableDataHeader *>(plain.tx_node_->buf_)->checksum(row_c);
    EXPECT_EQ(row_a.calc(), row_b.calc());
    EXPECT_EQ(row_a.calc(), row_c.calc());
  }
}

TEST_F(TestLobOuttableCallback, partial_batch_callback_allocation_releases_all_bodies)
{
  ObLobOutTableRedoPayload second = payload_;
  ++second.lob_col_idx_;
  ObSEArray<ObLobOutTableRedoPayload, 2> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(second));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObTxNodeArgs args;
  ObMvccWriteResults results;
  for (int64_t i = 0; i < 2; ++i) {
    ObTxNodeArg arg;
    ObMvccWriteResult res;
    ASSERT_NO_FATAL_FAILURE(prepare_row(payloads.at(i), arg, res));
    ASSERT_EQ(OB_SUCCESS, args.push_back(arg));
    ASSERT_EQ(OB_SUCCESS, results.push_back(res));
  }
  ctx_.fail_callback_after_ = 1;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, register_batch(args, results, redo_ctx));
  EXPECT_EQ(0, ctx_.append_calls_);
  EXPECT_EQ(0, payload_live_count());
  EXPECT_EQ(1, ctx_.callback_free_count_);
  EXPECT_EQ(nullptr, info(results.at(0))->redo_buf_);
  EXPECT_EQ(nullptr, info(results.at(1))->redo_buf_);
}

TEST_F(TestLobOuttableMutator, lob_digest_covers_contents_and_object_identity)
{
  const uint64_t original = calc_payload_digest_(payload_);
  ObLobOutTableRedoPayload changed = payload_;
  changed.data_ = ObString("OUTTABLE DATA");
  EXPECT_NE(original, calc_payload_digest_(changed));
  changed = payload_; changed.macro_id_.set_second_id(101);
  EXPECT_NE(original, calc_payload_digest_(changed));
  changed = payload_; changed.lob_operation_seq_no_.set_branch(1);
  EXPECT_NE(original, calc_payload_digest_(changed));
  changed = payload_; changed.lob_piece_table_id_++;
  EXPECT_NE(original, calc_payload_digest_(changed));
  changed = payload_; changed.seq_id_ = ObString("different seq");
  EXPECT_NE(original, calc_payload_digest_(changed));
}

TEST_F(TestLobOuttableMutator, redo_context_rejects_malformed_keys)
{
  ObSEArray<ObLobOutTableRedoPayload, 1> payloads;
  ASSERT_EQ(OB_SUCCESS, payloads.push_back(payload_));
  ObLobOutTableRedoCtx redo_ctx(payloads);
  ObStoreRowkey key;
  const ObLobOutTableRedoPayload *found = &payload_;
  EXPECT_EQ(OB_INVALID_ARGUMENT, redo_ctx.find_payload(key, found));
  EXPECT_EQ(nullptr, found);
  ObObj cells[3];
  cells[0].set_int(payload_.lob_col_idx_);
  cells[1].set_varbinary(payload_.lob_id_.get_string());
  cells[2].set_varbinary(payload_.seq_id_);
  ASSERT_EQ(OB_SUCCESS, key.assign(cells, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, redo_ctx.find_payload(key, found));
  EXPECT_EQ(nullptr, found);
  ASSERT_EQ(OB_SUCCESS, key.assign(cells, 3));
  for (int64_t i = 0; i < 3; ++i) {
    const ObObj saved = cells[i];
    if (0 == i) {
      cells[i].set_varbinary(ObString("invalid column index"));
    } else {
      cells[i].set_int(0);
    }
    found = &payload_;
    EXPECT_EQ(OB_INVALID_ARGUMENT, redo_ctx.find_payload(key, found));
    EXPECT_EQ(nullptr, found);
    cells[i] = saved;
  }
  ASSERT_EQ(OB_SUCCESS, redo_ctx.find_payload(key, found));
  EXPECT_EQ(&payloads.at(0), found);
}

class OuttableMutatorReplayExecutor : public ObTxReplayExecutor
{
public:
  OuttableMutatorReplayExecutor(
      const logservice::ObLogBaseHeader &header, OuttableReplayIOState &io_state)
    : ObTxReplayExecutor(nullptr, share::ObLSID(1), 1001, nullptr, palf::LSN(1),
        share::SCN::min_scn(), header), io_state_(io_state) {}

  int replay_one_row_in_memtable_(ObMutatorRowHeader &row, ObMemtableMutatorIterator *iter) override
  {
    io_state_.events_ += 'M';
    ++io_state_.row_count_;
    io_state_.last_row_type_ = row.mutator_type_;
    EXPECT_EQ(ObTabletID(200), row.tablet_id_);
    EXPECT_EQ(12345U, iter->get_mutator_row().acc_checksum_);
    EXPECT_EQ(7, iter->get_mutator_row().get_column_cnt());
    if (MutatorType::MUTATOR_LOB_OUTTABLE_ROW == row.mutator_type_) {
      EXPECT_TRUE(iter->get_lob_outtable_payload().is_valid());
      if (iter->get_lob_outtable_payload().is_logging_) {
        EXPECT_TRUE(io_state_.object_exists_);
      }
    } else {
      EXPECT_EQ(MutatorType::MUTATOR_ROW, row.mutator_type_);
      EXPECT_FALSE(iter->get_lob_outtable_payload().is_valid());
    }
    return io_state_.row_ret_;
  }

private:
  OuttableReplayIOState &io_state_;
};

class TestLobOuttableMutatorReplay : public TestLobOuttableMutator
{
protected:
  TestLobOuttableMutatorReplay() : executor_(base_header_, io_state_), redo_(redo_ref_) {}

  void SetUp() override
  {
    TestLobOuttableMutator::SetUp();
    ASSERT_EQ(nullptr, replay_io_state);
    replay_io_state = &io_state_;
  }

  void TearDown() override
  {
    replay_io_state = nullptr;
  }

  void prepare_redo(ObMutatorWriter &writer, const std::string &mutator_buf)
  {
    int64_t mutator_length = 0;
    ASSERT_EQ(OB_SUCCESS,
        writer.serialize(ObTransRowFlag::NORMAL_ROW, mutator_length, encrypt_info_));
    redo_buf_.assign(mutator_length + 4096, '\0');
    ObTxRedoLog encoded(DATA_CURRENT_VERSION);
    ASSERT_EQ(OB_SUCCESS, encoded.set_mutator_buf(&redo_buf_[0]));
    ASSERT_EQ(OB_SUCCESS, encoded.set_mutator_size(redo_buf_.size(), false));
    ASSERT_GE(encoded.get_mutator_size(), mutator_length);
    MEMCPY(encoded.get_mutator_buf(), mutator_buf.data(), mutator_length);
    ASSERT_EQ(OB_SUCCESS, encoded.set_mutator_size(mutator_length, true));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, encoded.serialize(&redo_buf_[0], redo_buf_.size(), pos));
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, redo_.deserialize(redo_buf_.data(), pos, read_pos));
    ASSERT_EQ(pos, read_pos);
  }

  void prepare_lob_redo()
  {
    std::string buf(payload_.get_serialize_size() + 4096, '\0');
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
    ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
    ASSERT_NO_FATAL_FAILURE(prepare_redo(writer, buf));
  }

  OuttableReplayIOState io_state_;
  logservice::ObLogBaseHeader base_header_;
  OuttableMutatorReplayExecutor executor_;
  ObTxRedoLogTempRef redo_ref_;
  ObTxRedoLog redo_;
  std::string redo_buf_;
};

TEST_F(TestLobOuttableMutatorReplay, object_is_written_before_native_meta_row)
{
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  ObTxSEQ max_seq;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HWM", io_state_.events_);
  EXPECT_EQ(1, io_state_.row_count_);
  EXPECT_EQ(MutatorType::MUTATOR_LOB_OUTTABLE_ROW, io_state_.last_row_type_);
  EXPECT_EQ(payload_.macro_id_, io_state_.head_macro_id_);
  EXPECT_EQ(payload_.macro_id_, io_state_.written_macro_id_);
  EXPECT_EQ(1, io_state_.ls_id_);
  EXPECT_EQ(std::string(payload_.data_.ptr(), payload_.data_.length()), io_state_.written_data_);
  EXPECT_EQ(row_.seq_no_, max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, head_failure_blocks_meta_and_retries_from_same_row)
{
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  io_state_.head_ret_ = OB_TIMEOUT;
  ObTxSEQ max_seq;
  EXPECT_EQ(OB_EAGAIN, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("H", io_state_.events_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(0, io_state_.row_count_);
  EXPECT_FALSE(max_seq.is_valid());

  io_state_.head_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HHWM", io_state_.events_);
  EXPECT_EQ(1, io_state_.row_count_);
  EXPECT_EQ(row_.seq_no_, max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, upload_failure_blocks_meta_and_retries_object)
{
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  io_state_.write_ret_ = OB_OBJECT_STORAGE_IO_ERROR;
  ObTxSEQ max_seq;
  EXPECT_EQ(OB_EAGAIN, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HW", io_state_.events_);
  EXPECT_EQ(0, io_state_.row_count_);
  EXPECT_FALSE(io_state_.object_exists_);
  EXPECT_FALSE(max_seq.is_valid());

  io_state_.write_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HWHWM", io_state_.events_);
  EXPECT_EQ(2, io_state_.write_count_);
  EXPECT_EQ(1, io_state_.row_count_);
  EXPECT_EQ(row_.seq_no_, max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, nonretryable_object_failure_is_preserved)
{
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  io_state_.head_ret_ = OB_NOT_SUPPORTED;
  ObTxSEQ max_seq;
  EXPECT_EQ(OB_NOT_SUPPORTED, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("H", io_state_.events_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(0, io_state_.row_count_);
  EXPECT_FALSE(max_seq.is_valid());
}

TEST_F(TestLobOuttableMutatorReplay, existing_object_skips_upload_on_repeated_replay)
{
  row_.dml_flag_ = ObDmlFlag::DF_UPDATE;
  row_.old_row_.set("old meta row", 12);
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  io_state_.object_exists_ = true;
  ObTxSEQ max_seq;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HMHM", io_state_.events_);
  EXPECT_EQ(2, io_state_.head_count_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(2, io_state_.row_count_);
  EXPECT_EQ(row_.seq_no_, max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, meta_retry_reuses_object_uploaded_on_previous_attempt)
{
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  io_state_.row_ret_ = OB_EAGAIN;
  ObTxSEQ max_seq;
  EXPECT_EQ(OB_EAGAIN, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HWM", io_state_.events_);
  EXPECT_TRUE(io_state_.object_exists_);
  EXPECT_FALSE(max_seq.is_valid());

  io_state_.row_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HWMHM", io_state_.events_);
  EXPECT_EQ(1, io_state_.write_count_);
  EXPECT_EQ(2, io_state_.row_count_);
  EXPECT_EQ(row_.seq_no_, max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, nologging_replays_meta_without_object_io)
{
  payload_.is_logging_ = false;
  payload_.data_.reset();
  ASSERT_TRUE(payload_.is_valid());
  ASSERT_NO_FATAL_FAILURE(prepare_lob_redo());
  ObTxSEQ max_seq;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("M", io_state_.events_);
  EXPECT_EQ(0, io_state_.head_count_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(1, io_state_.row_count_);
  EXPECT_EQ(row_.seq_no_, max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, native_row_has_no_object_io_or_final_sequence_tracking)
{
  std::string buf(4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ASSERT_NO_FATAL_FAILURE(prepare_redo(writer, buf));
  ObTxSEQ max_seq(999, 0);
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, false, max_seq));
  EXPECT_EQ("M", io_state_.events_);
  EXPECT_EQ(MutatorType::MUTATOR_ROW, io_state_.last_row_type_);
  EXPECT_EQ(0, io_state_.head_count_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(1, io_state_.row_count_);
  EXPECT_FALSE(max_seq.is_valid());
}

TEST_F(TestLobOuttableMutatorReplay, serial_final_tracks_maximum_across_mixed_row_types)
{
  std::string buf(8192, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  row_.seq_no_ = ObTxSEQ(200, 0);
  payload_.lob_operation_seq_no_ = row_.seq_no_;
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  row_.seq_no_ = ObTxSEQ(300, 0);
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  row_.seq_no_ = ObTxSEQ(100, 0);
  payload_.lob_operation_seq_no_ = row_.seq_no_;
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  ASSERT_NO_FATAL_FAILURE(prepare_redo(writer, buf));

  ObTxSEQ max_seq;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("HWMMHM", io_state_.events_);
  EXPECT_EQ(2, io_state_.head_count_);
  EXPECT_EQ(1, io_state_.write_count_);
  EXPECT_EQ(3, io_state_.row_count_);
  EXPECT_EQ(ObTxSEQ(300, 0), max_seq);
}

TEST_F(TestLobOuttableMutatorReplay, invalid_payload_blocks_object_io_and_meta_replay)
{
  ObLobOutTableRedoPayload invalid_payload = payload_;
  ++invalid_payload.byte_len_;
  ASSERT_FALSE(invalid_payload.is_valid());
  std::string raw;
  ASSERT_NO_FATAL_FAILURE(serialize_raw_lob_row(raw, invalid_payload));
  std::string buf(raw.size() + 4096, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  // Keep the UNIS envelope and outer CRC valid to exercise payload validation during replay.
  ASSERT_EQ(OB_SUCCESS, writer.append_row_buf(raw.data(), raw.size()));
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ASSERT_NO_FATAL_FAILURE(prepare_redo(writer, buf));

  ObTxSEQ max_seq;
  EXPECT_EQ(OB_INVALID_LOG, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_TRUE(io_state_.events_.empty());
  EXPECT_EQ(0, io_state_.head_count_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(0, io_state_.row_count_);
  EXPECT_FALSE(max_seq.is_valid());
}

TEST_F(TestLobOuttableMutatorReplay, object_failure_keeps_only_successful_prefix_sequence)
{
  std::string buf(8192, '\0');
  ObMutatorWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
  row_.seq_no_ = ObTxSEQ(100, 0);
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  row_.seq_no_ = ObTxSEQ(200, 0);
  payload_.lob_operation_seq_no_ = row_.seq_no_;
  ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
  row_.seq_no_ = ObTxSEQ(300, 0);
  ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
  ASSERT_NO_FATAL_FAILURE(prepare_redo(writer, buf));

  io_state_.head_ret_ = OB_TIMEOUT;
  ObTxSEQ max_seq;
  EXPECT_EQ(OB_EAGAIN, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("MH", io_state_.events_);
  EXPECT_EQ(1, io_state_.head_count_);
  EXPECT_EQ(0, io_state_.write_count_);
  EXPECT_EQ(1, io_state_.row_count_);
  EXPECT_EQ(ObTxSEQ(100, 0), max_seq);

  // A retry replays the same prefix and reaches the previously blocked suffix.
  io_state_.head_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, executor_.replay_redo_in_memtable_(redo_, true, max_seq));
  EXPECT_EQ("MHMHWMM", io_state_.events_);
  EXPECT_EQ(2, io_state_.head_count_);
  EXPECT_EQ(1, io_state_.write_count_);
  EXPECT_EQ(4, io_state_.row_count_);
  EXPECT_EQ(ObTxSEQ(300, 0), max_seq);
}

TEST_F(TestLobOuttableMutator, ob_admin_dump_accepts_lob_and_native_rows)
{
  ObDatumRow datum_row;
  ASSERT_EQ(OB_SUCCESS, datum_row.init(1));
  datum_row.storage_datums_[0].set_int(1);
  datum_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  ObRowWriterV0 row_writer;
  char *row_buf = nullptr;
  int64_t row_size = 0;
  ASSERT_EQ(OB_SUCCESS, row_writer.write(1, datum_row, row_buf, row_size));
  row_.new_row_.set(row_buf, row_size);
  row_.column_cnt_ = 1;

  for (int64_t mixed = 0; mixed < 2; ++mixed) {
    SCOPED_TRACE(mixed);
    std::string buf(4096, '\0');
    ObMutatorWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.set_buffer(&buf[0], buf.size()));
    ASSERT_NO_FATAL_FAILURE(append_lob_row(writer));
    if (mixed) {
      ASSERT_NO_FATAL_FAILURE(append_native_row(writer));
    }
    int64_t length = 0;
    ASSERT_EQ(OB_SUCCESS, writer.serialize(ObTransRowFlag::NORMAL_ROW, length, encrypt_info_));
    ObTxRedoLog redo(DATA_CURRENT_VERSION);
    redo.replay_mutator_buf_ = buf.data();
    redo.mutator_size_ = length;
    ObMemtableMutatorIterator iter;
    rapidjson::StringBuffer json;
    share::ObAdminLogJsonDumper dumper(json);
    share::ObLogStat stat;
    share::ObAdminMutatorStringArg arg;
    std::string scratch(4096, '\0');
    arg.buf_ = &scratch[0];
    arg.buf_len_ = scratch.size();
    arg.pos_ = 0;
    arg.writer_ptr_ = &dumper;
    arg.log_stat_ = &stat;
    bool has_dumped_tx_id = false;
    ASSERT_EQ(OB_SUCCESS, dumper.start_object());
    ASSERT_EQ(OB_SUCCESS, redo.ob_admin_dump(&iter, arg, "test", palf::LSN(1), 1,
        share::SCN::min_scn(), has_dumped_tx_id));
    ASSERT_EQ(OB_SUCCESS, dumper.end_object());
    const std::string output(json.GetString(), json.GetSize());
    rapidjson::Document document;
    document.Parse(json.GetString());
    ASSERT_FALSE(document.HasParseError());
    ASSERT_TRUE(document.HasMember("###<TxRedoLog>"));
    ASSERT_TRUE(document["###<TxRedoLog>"].HasMember("MutatorRows"));
    const rapidjson::Value &rows = document["###<TxRedoLog>"]["MutatorRows"];
    ASSERT_TRUE(rows.HasMember("LOB_OUTTABLE_ROW"));
    const rapidjson::Value &lob_row = rows["LOB_OUTTABLE_ROW"];
    ASSERT_TRUE(lob_row.HasMember("LobPayloadLength"));
    ASSERT_TRUE(lob_row["LobPayloadLength"].IsInt64());
    EXPECT_EQ(static_cast<int64_t>(payload_buf_.size()), lob_row["LobPayloadLength"].GetInt64());
    ASSERT_TRUE(lob_row.HasMember("LobPayloadCrc64"));
    ASSERT_TRUE(lob_row["LobPayloadCrc64"].IsUint64());
    EXPECT_EQ(ob_crc64(payload_buf_.data(), payload_buf_.size()),
        lob_row["LobPayloadCrc64"].GetUint64());
    EXPECT_EQ(1 + mixed, stat.normal_row_count_);
    EXPECT_NE(std::string::npos, output.find("LOB_OUTTABLE_ROW"));
    EXPECT_NE(std::string::npos, output.find("LobPayload"));
    EXPECT_NE(std::string::npos, output.find("data_length"));
    EXPECT_EQ(std::string::npos, output.find("unknown mutator type"));
    EXPECT_EQ(std::string::npos, output.find("outtable data"));
    if (mixed) {
      EXPECT_NE(std::string::npos, output.find("NORMAL_ROW"));
      ASSERT_TRUE(rows.HasMember("NORMAL_ROW"));
      EXPECT_FALSE(rows["NORMAL_ROW"].HasMember("LobPayloadLength"));
      EXPECT_FALSE(rows["NORMAL_ROW"].HasMember("LobPayloadCrc64"));
    }
  }
}

uint64_t TestLobOuttableMutator::calc_payload_digest_(const ObLobOutTableRedoPayload &payload)
{
  ObLobOutTableRedoInfo info;
  EXPECT_EQ(OB_SUCCESS, info.init(OB_SYS_TENANT_ID, payload));
  info.release();
  return info.checksum_;
}

void TestLobOuttableCallback::bind_callback_(
    ObMvccRowCallback &cb, ObMvccWriteResult &res, const ObTxNodeArg &arg)
{
  ObRowData empty;
  cb.set(&res.mtk_, res.tx_node_, res.tx_node_->get_data_size(), empty,
      false, arg.seq_no_, arg.column_cnt_, true);
  cb.set_is_link();
}

void TestLobOuttableMutator::attach_payload(RedoDataNode &redo)
{
  payload_buf_.assign(payload_.get_serialize_size(), '\0');
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, payload_.serialize(&payload_buf_[0], payload_buf_.size(), pos));
  redo.outtable_redo_payload_buf_.assign_ptr(payload_buf_.data(), pos);
}

void TestLobOuttableMutator::append_lob_row(ObMutatorWriter &writer, const int expected_ret)
{
  ObITransCallback callback;
  RedoDataNode redo;
  ASSERT_NO_FATAL_FAILURE(prepare_redo(redo, &callback));
  ASSERT_NO_FATAL_FAILURE(attach_payload(redo));
  ASSERT_EQ(expected_ret, writer.append_row_kv(row_.table_version_, redo, nullptr, encrypt_info_));
}

int64_t TestLobOuttableCallback::payload_live_count()
{
  return get_mem_leak_checker().malloc_info_.size();
}

} // namespace unittest

namespace blocksstable
{
using namespace common;

// Mock the external object manager boundary so optimization cannot bypass I/O interception.
int ObObjectManager::ss_is_exist_object(
    const MacroBlockId &macro_id, const int64_t ls_epoch, bool &exists, const int64_t ls_id)
{
  int ret = OB_SUCCESS;
  unittest::OuttableReplayIOState *state = unittest::replay_io_state;
  exists = false;
  if (nullptr == state) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    state->events_ += 'H';
    ++state->head_count_;
    state->head_macro_id_ = macro_id;
    state->ls_id_ = ls_id;
    exists = state->object_exists_;
    ret = state->head_ret_;
  }
  return ret;
}

int ObObjectManager::async_write_object(const MacroBlockId &macro_id,
    const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  unittest::OuttableReplayIOState *state = unittest::replay_io_state;
  if (nullptr == state) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    state->events_ += 'W';
    ++state->write_count_;
    state->written_macro_id_ = macro_id;
    state->written_data_.assign(write_info.buffer_, write_info.size_);
    ret = state->write_ret_;
    if (OB_SUCCESS == ret) {
      state->object_exists_ = true;
    }
  }
  return ret;
}

int ObStorageObjectHandle::wait()
{
  return nullptr == unittest::replay_io_state ? OB_ERR_UNEXPECTED : OB_SUCCESS;
}
} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  // Reset the initial INT64_MAX permit count together with the rate to avoid overflow.
  oceanbase::common::ObSimpleRateLimiter &limiter = oceanbase::common::ObMemLeakChecker::rl_;
  limiter.~ObSimpleRateLimiter();
  new (&limiter) oceanbase::common::ObSimpleRateLimiter(1000000);
  if (0 != oceanbase::common::get_mem_leak_checker().init()) {
    return 1;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
