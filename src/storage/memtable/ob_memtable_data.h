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

namespace oceanbase
{
namespace memtable
{

// What ObMemtableDataHeader::buf_[0 .. buf_len_-1] holds after the header.
enum class ObMemtablePayloadKind : uint8_t
{
  ROW_SERIALIZED = 0, // raw memtable row bytes (ObCompatRowReader path)
  MDS_LIVE_VALUE = 1, // [ObMdsValueMeta][V live object] at buf_
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
  // Serialized row blob for ROW_SERIALIZED; do not use for MDS (returns nullptr).
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

  inline int64_t dup_size() const { return (sizeof(ObMemtableDataHeader) + buf_len_); }
  inline int checksum(common::ObBatchChecksum &bc) const
  {
    int ret = common::OB_SUCCESS;
    if (buf_len_ <= 0) {
      ret = common::OB_NOT_INIT;
    } else {
      bc.fill(&dml_flag_, sizeof(dml_flag_));
      bc.fill(&buf_len_, sizeof(buf_len_));
      bc.fill(&payload_kind_, sizeof(payload_kind_));
      // MDS_LIVE_VALUE: buf_ holds live inner value; do not checksum object bits.
      if (is_row_payload()) {
        bc.fill(buf_, buf_len_);
      }
    }
    return ret;
  }

  // the template parameter T supports ObMemtableData and ObMemtableDataHeader,
  // but in practice only ObMemtableDataHeader is involved in building.
  template<class T>
  static int build(ObMemtableDataHeader *new_data, const T *data)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(data->buf_len_ < 0)) {
      ret = OB_NOT_INIT;
    } else if (OB_ISNULL(data->buf_) || 0 == data->buf_len_) {
      // do nothing
    } else if (OB_ISNULL(new(new_data) ObMemtableDataHeader(data->dml_flag_,
                                                            data->buf_len_,
                                                            data->get_payload_kind()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(new_data->buf_, data->buf_, data->buf_len_);
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
  void reset()
  {
    dml_flag_ = blocksstable::ObDmlFlag::DF_MAX;
    payload_kind_ = static_cast<uint8_t>(ObMemtablePayloadKind::ROW_SERIALIZED);
    buf_len_ = 0;
    buf_ = nullptr;
  }
  void set(blocksstable::ObDmlFlag dml_flag, const int64_t data_len, char *buf)
  {
    dml_flag_ = dml_flag;
    buf_len_ = data_len;
    buf_ = buf;
  }
  // must use the size of ObMemtableDataHeader, since the actual structure
  // involved in dup is always ObMemtableDataHeader
  inline int64_t dup_size() const { return (sizeof(ObMemtableDataHeader) + buf_len_); }

  blocksstable::ObDmlFlag dml_flag_;
  uint8_t payload_kind_;
  int64_t buf_len_;
  const char *buf_;
};

}
}

#endif // OCEANBASE_MEMTABLE_OB_MEMTABLE_DATA_
