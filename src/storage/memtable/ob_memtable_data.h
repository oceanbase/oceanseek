/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_

#include <cstddef>
#include "lib/utility/ob_print_utils.h"
#include "storage/ob_i_store.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"
#include "storage/lob/store/ob_lob_outtable_redo_payload.h"

namespace oceanbase
{
namespace memtable
{

// What ObMemtableDataHeader::buf_[0 .. buf_len_-1] holds after the header.
enum class ObMemtablePayloadKind : uint8_t
{
  ROW_SERIALIZED = 0, // raw memtable row bytes (ObCompatRowReader path)
  MDS_LIVE_VALUE = 1, // [ObMdsValueMeta][V live object] at buf_
  ROW_WITH_LOB_REDO = 2, // row bytes, alignment padding, ObLobOutTableRedoInfo
};

// The structure actually written to MvccTransNode::buf_
class ObMemtableDataHeader
{
public:
  ObMemtableDataHeader(blocksstable::ObDmlFlag dml_flag,
                       int64_t buf_len,
                       const ObMemtablePayloadKind payload_kind = ObMemtablePayloadKind::ROW_SERIALIZED)
      : dml_flag_(dml_flag),
        payload_kind_(static_cast<uint8_t>(payload_kind)),
        buf_len_(buf_len)
  {}
  ~ObMemtableDataHeader() {}
  TO_STRING_KV(K_(dml_flag), K_(buf_len), K_(payload_kind));
  inline ObMemtablePayloadKind get_payload_kind() const
  {
    return static_cast<ObMemtablePayloadKind>(payload_kind_);
  }
  inline bool is_mds_payload() const
  {
    return ObMemtablePayloadKind::MDS_LIVE_VALUE == get_payload_kind();
  }
  inline bool is_row_payload() const { return !is_mds_payload(); }
  bool with_lob_payload() const;
  storage::ObLobOutTableRedoInfo *lob_redo_info();
  const storage::ObLobOutTableRedoInfo *lob_redo_info() const;
  // Serialized row bytes for either row kind; MDS returns nullptr.
  inline const char *row_serialized_ptr() const { return is_row_payload() ? buf_ : nullptr; }
  inline char *row_serialized_ptr() { return is_row_payload() ? buf_ : nullptr; }
  inline int64_t row_serialized_len() const { return is_row_payload() ? buf_len_ : 0; }
  // Live V object when MDS_LIVE_VALUE; otherwise nullptr.
  inline const void *mds_inner_value_ptr() const
  {
    return is_mds_payload() ? static_cast<const void *>(buf_ + sizeof(storage::ObMdsValueMeta)) : nullptr;
  }
  inline void *mds_inner_value_ptr()
  {
    // Must call const overload; plain this->mds_inner_value_ptr() would recurse here.
    return const_cast<void *>(static_cast<const ObMemtableDataHeader *>(this)->mds_inner_value_ptr());
  }
  inline const storage::ObMdsValueMeta *mds_meta_ptr() const
  {
    return is_mds_payload() ? reinterpret_cast<const storage::ObMdsValueMeta *>(buf_) : nullptr;
  }
  inline storage::ObMdsValueMeta *mds_meta_ptr()
  {
    return const_cast<storage::ObMdsValueMeta *>(
        static_cast<const ObMemtableDataHeader *>(this)->mds_meta_ptr());
  }

  inline int64_t dup_size() const
  {
    return sizeof(ObMemtableDataHeader) + (with_lob_payload()
        ? ALIGN_UP(buf_len_, 8) + sizeof(storage::ObLobOutTableRedoInfo) : buf_len_);
  }
  inline int checksum(common::ObBatchChecksum &bc) const
  {
    int ret = common::OB_SUCCESS;
    if (buf_len_ <= 0) {
      ret = common::OB_NOT_INIT;
    } else {
      bc.fill(&dml_flag_, sizeof(dml_flag_));
      bc.fill(&buf_len_, sizeof(buf_len_));
      // LOB attachments are not table columns. Preserve the row checksum; the
      // callback includes the immutable LOB digest in the transaction checksum.
      const uint8_t row_kind = static_cast<uint8_t>(ObMemtablePayloadKind::ROW_SERIALIZED);
      bc.fill(with_lob_payload() ? &row_kind : &payload_kind_, sizeof(payload_kind_));
      // MDS_LIVE_VALUE: buf_ holds live inner value; do not checksum object bits.
      if (is_row_payload()) {
        bc.fill(buf_, buf_len_);
      }
    }
    return ret;
  }

  // The source is normally ObMemtableData. A live LOB body belongs to one node
  // and cannot be shared by copying its header.
  template<class T>
  static int build(ObMemtableDataHeader *new_data, const T *data)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(data->buf_len_ < 0)) {
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(data->buf_) || 0 == data->buf_len_) {
      // do nothing
    } else if (ObMemtablePayloadKind::ROW_WITH_LOB_REDO == data->get_payload_kind()
        && nullptr != data->lob_redo_info()->redo_buf_) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_ISNULL(new(new_data) ObMemtableDataHeader(data->dml_flag_,
                                                            data->buf_len_,
                                                            data->get_payload_kind()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(new_data->buf_, data->buf_, data->buf_len_);
      if (new_data->with_lob_payload()) {
        new(new_data->lob_redo_info()) storage::ObLobOutTableRedoInfo(*data->lob_redo_info());
      }
    }
    return ret;
  }
  // Keep payload_kind_ next to dml_flag_ so FAM buf_ is 8-byte aligned.
  // MDS_LIVE_VALUE places ObMdsValueMeta at buf_; unaligned ATOMIC_LOAD of
  // patch_seq_ SIGBUS on AArch64 (x86 tolerates the same layout).
  blocksstable::ObDmlFlag dml_flag_;
  uint8_t payload_kind_;
  int64_t buf_len_;
  char buf_[0];
};

static_assert(offsetof(ObMemtableDataHeader, buf_) % 8 == 0,
              "ObMemtableDataHeader::buf_ must be 8-byte aligned");
static_assert(offsetof(ObMemtableDataHeader, buf_) % alignof(storage::ObMdsValueMeta) == 0,
              "ObMemtableDataHeader::buf_ must be aligned for ObMdsValueMeta");
static_assert(alignof(storage::ObLobOutTableRedoInfo) <= 8,
              "LOB redo info requires 8-byte alignment");

// only used for more conveniently passing parameters
class ObMemtableData
{
public:
  ObMemtableData()
    : dml_flag_(blocksstable::ObDmlFlag::DF_MAX),
      payload_kind_(static_cast<uint8_t>(ObMemtablePayloadKind::ROW_SERIALIZED)),
      buf_len_(0),
      buf_(nullptr)
  {}
  ObMemtableData(blocksstable::ObDmlFlag dml_flag, int64_t buf_len, const char *buf)
      : dml_flag_(dml_flag),
        payload_kind_(static_cast<uint8_t>(ObMemtablePayloadKind::ROW_SERIALIZED)),
        buf_len_(buf_len),
        buf_(buf)
  {}
  ~ObMemtableData() {}
  TO_STRING_KV(K_(dml_flag), K_(buf_len), K_(payload_kind));
  inline ObMemtablePayloadKind get_payload_kind() const
  {
    return static_cast<ObMemtablePayloadKind>(payload_kind_);
  }
  inline void set_payload_kind(const ObMemtablePayloadKind k) { payload_kind_ = static_cast<uint8_t>(k); }
  storage::ObLobOutTableRedoInfo *lob_redo_info();
  const storage::ObLobOutTableRedoInfo *lob_redo_info() const;
  void reset()
  {
    dml_flag_ = blocksstable::ObDmlFlag::DF_MAX;
    payload_kind_ = static_cast<uint8_t>(ObMemtablePayloadKind::ROW_SERIALIZED);
    buf_len_ = 0;
    buf_ = nullptr;
    lob_redo_info_ = storage::ObLobOutTableRedoInfo();
  }
  void set(blocksstable::ObDmlFlag dml_flag, const int64_t data_len, char *buf)
  {
    dml_flag_ = dml_flag;
    buf_len_ = data_len;
    buf_ = buf;
  }
  // must use the size of ObMemtableDataHeader, since the actual structure
  // involved in dup is always ObMemtableDataHeader
  inline int64_t dup_size() const
  {
    return sizeof(ObMemtableDataHeader)
        + ((ObMemtablePayloadKind::ROW_WITH_LOB_REDO == get_payload_kind())
            ? ALIGN_UP(buf_len_, 8) + sizeof(storage::ObLobOutTableRedoInfo) : buf_len_);
  }

  blocksstable::ObDmlFlag dml_flag_;
  uint8_t payload_kind_;
  int64_t buf_len_;
  const char *buf_;
private:
  storage::ObLobOutTableRedoInfo lob_redo_info_;
};

}
}

#endif // OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_
