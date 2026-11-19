#ifndef OCEANBASE_UNITTEST_CDC_OUTTABLE_LOB_TEST_HELPER_H_
#define OCEANBASE_UNITTEST_CDC_OUTTABLE_LOB_TEST_HELPER_H_

#include <string>
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/tx/ob_clog_encrypt_info.h"

namespace oceanbase { namespace libobcdc {

// Convenience constructors only; the serialized representation is the production payload.
struct OuttableTestPayload : public storage::ObLobOutTableRedoPayload
{
  OuttableTestPayload() = default;
  OuttableTestPayload(uint64_t table_id, const transaction::ObTxSEQ &seq,
      const common::ObString &piece_seq, const common::ObLobId &lob_id,
      const blocksstable::MacroBlockId &macro_id, bool logging, const common::ObString &data)
  {
    lob_col_idx_ = lob_id.get_column_idx();
    lob_id_ = lob_id;
    seq_id_ = piece_seq;
    lob_piece_table_id_ = table_id;
    lob_operation_seq_no_ = seq;
    macro_id_ = macro_id;
    is_logging_ = logging;
    byte_len_ = logging ? data.length() : 1;
    data_ = data;
  }
  uint64_t get_lob_aux_table_id() const { return lob_piece_table_id_; }
  const transaction::ObTxSEQ &get_lob_operation_seq_no() const { return lob_operation_seq_no_; }
  const common::ObLobId &get_lob_id() const { return lob_id_; }
  const blocksstable::MacroBlockId &get_macro_id() const { return macro_id_; }
  const common::ObString &get_seq_id() const { return seq_id_; }
  const common::ObString &get_data() const { return data_; }
};

inline int serialize_outtable_test_row(const storage::ObLobOutTableRedoPayload &payload,
    std::string &raw, bool append_compat_tail = false, bool with_payload = true,
    uint64_t tablet_id = 1001)
{
  int ret = OB_SUCCESS;
  transaction::ObCLogEncryptInfo encrypt_info;
  memtable::ObMemtableMutatorRow row;
  common::ObObj keys[3];
  keys[0].set_int(payload.lob_col_idx_);
  keys[1].set_varchar(common::ObString(sizeof(payload.lob_id_),
      reinterpret_cast<const char *>(&payload.lob_id_)));
  keys[2].set_varchar(payload.seq_id_);
  row.rowkey_.assign(keys, 3);
  row.table_id_ = 0;
  row.table_version_ = 9;
  // Deliberately not a decoded Meta column row: payload-only CDC must never open it.
  row.new_row_.set("native meta bytes", 17);
  row.dml_flag_ = blocksstable::ObDmlFlag::DF_INSERT;
  row.seq_no_ = payload.lob_operation_seq_no_;
  row.column_cnt_ = 7;
  memtable::ObMutatorRowHeader header;
  header.mutator_type_ = with_payload ? memtable::MutatorType::MUTATOR_LOB_OUTTABLE_ROW
      : memtable::MutatorType::MUTATOR_ROW;
  header.tablet_id_ = common::ObTabletID(tablet_id);
  raw.assign(payload.get_serialize_size() + 4096, '\0');
  int64_t capacity = raw.size();
  int64_t pos = 0;
  if (OB_FAIL(encrypt_info.init())) {
  } else if (OB_FAIL(header.serialize(&raw[0], raw.size(), pos))) {
  } else if (OB_FAIL(row.serialize(&raw[0], capacity, pos, nullptr, encrypt_info))) {
  } else if (with_payload) {
    const int64_t payload_start = pos;
    if (OB_FAIL(payload.serialize(&raw[0], raw.size(), pos))) {
    } else if (append_compat_tail) {
      int64_t read_pos = payload_start;
      int64_t version = 0;
      int64_t payload_length = 0;
      if (OB_FAIL(common::serialization::decode_vi64(raw.data(), pos, read_pos, &version))) {
      } else {
        const int64_t length_pos = read_pos;
        if (OB_FAIL(common::serialization::decode_vi64(raw.data(), pos, read_pos, &payload_length))) {
        } else {
          raw[pos++] = 'T';
          int64_t rewrite_pos = length_pos;
          ret = common::serialization::encode_fixed_bytes_i64(
              &raw[0], raw.size(), rewrite_pos, payload_length + 1);
        }
      }
    }
  }
  if (OB_SUCC(ret)) { raw.resize(pos); }
  return ret;
}

inline int wrap_outtable_test_mutator(const std::string &raw, std::string &mutator)
{
  int ret = OB_SUCCESS;
  memtable::ObMutatorWriter writer;
  transaction::ObCLogEncryptInfo encrypt_info;
  mutator.assign(raw.size() + 4096, '\0');
  int64_t size = 0;
  if (OB_FAIL(encrypt_info.init())) {
  } else if (OB_FAIL(writer.set_buffer(&mutator[0], mutator.size()))) {
  } else if (OB_FAIL(writer.append_row_buf(raw.data(), raw.size()))) {
  } else if (OB_FAIL(writer.serialize(memtable::ObTransRowFlag::NORMAL_ROW, size, encrypt_info))) {
  } else { mutator.resize(size); }
  return ret;
}

inline int decode_outtable_test_row(const char *buf, int64_t len, int64_t &pos,
    storage::ObLobOutTableRedoPayload &payload)
{
  int ret = OB_SUCCESS;
  memtable::ObMutatorRowHeader header;
  int32_t row_size = 0;
  if (OB_FAIL(header.deserialize(buf, len, pos))) {
  } else if (header.mutator_type_ != memtable::MutatorType::MUTATOR_LOB_OUTTABLE_ROW) {
    ret = OB_INVALID_DATA;
  } else {
    const int64_t row_start = pos;
    if (OB_FAIL(common::serialization::decode_i32(buf, len, pos, &row_size))) {
    } else if (row_size <= 0 || row_size > len - row_start) {
      ret = OB_INVALID_DATA;
    } else {
      pos = row_start + row_size;
      ret = payload.deserialize(buf, len, pos);
    }
  }
  return ret;
}

} }
#endif
