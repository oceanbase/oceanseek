/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 *
 * Payload for ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH (V1 only).
 * - Each row: (mds_unit_id, meta, serialized value blob).
 * - The relocation type identifies transfer, split, or fork and remains unchanged across batches.
 * - Target tablet_id and destination MDS row key live in the batch target list
 *   and are broadcast to every row in the batch at apply time. Transfer uses
 *   its start SCN as the key; fork uses the destination reorganization SCN.
 * - Source tablet-status rows (ObTabletCreateDeleteMdsUserData / DummyKey) are NOT shipped;
 *   a transfer or fork final batch writes an ObTabletMdsRelocStatus marker row
 *   for the destination generation instead of touching the tablet-status unit.
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_LS_MDS_TABLET_RELOC_BATCH_ARG_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_LS_MDS_TABLET_RELOC_BATCH_ARG_H_

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"

namespace oceanbase
{
namespace storage
{

static const int64_t LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION = 1;

enum class ObLsMdsTabletRelocType : int8_t
{
  INVALID = 0,
  TRANSFER,
  SPLIT,
  FORK,
  MAX_TYPE,
};

OB_INLINE bool is_valid_reloc_type(const ObLsMdsTabletRelocType type)
{
  return type > ObLsMdsTabletRelocType::INVALID
      && type < ObLsMdsTabletRelocType::MAX_TYPE;
}

struct ObLsMdsTabletRelocTarget final
{
  OB_UNIS_VERSION(1);

public:
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;

  TO_STRING_KV(K_(tablet_id), K_(transfer_scn));

  void reset()
  {
    tablet_id_.reset();
    transfer_scn_.reset();
  }
};

struct ObLsMdsTabletRelocRowStorage final
{
  OB_UNIS_VERSION(1);

public:
  uint8_t mds_unit_id_;
  ObMdsValueMeta meta_;
  common::ObString ser_payload_;
  TO_STRING_KV(K_(mds_unit_id), K_(meta), "serialize_len", ser_payload_.length());
  void reset()
  {
    mds_unit_id_ = UINT8_MAX;
    meta_.reset();
    ser_payload_.reset();
  }
};

// 2-D skip bitmap: row_count x target_count, backed by a flat uint64_t array.
// Each uint64_t covers 64 targets.  Bit layout (linear):
//   bit_index = row_idx * target_count + tgt_idx
//   words_[bit_index / 64] & (1ULL << (bit_index % 64))
//
// All elements use fixed-length encode_i64 (8 bytes) so that serialize,
// rewrite_inplace, and get_serialize_size are always consistent.
// deserialize records a raw pointer (words_data_ptr_) to the first word
// in the buffer; rewrite_inplace overwrites words at that pointer directly.
class ObRelocSkipBitmap final
{
public:
  ObRelocSkipBitmap() : row_count_(0), target_count_(0), words_data_ptr_(nullptr) {}
  void reset();
  int init(const int64_t row_count, const int64_t target_count);
  bool is_inited() const { return row_count_ > 0 && target_count_ > 0; }
  void set_skip(const int64_t row_idx, const int64_t tgt_idx);
  bool is_skipped(const int64_t row_idx, const int64_t tgt_idx) const;
  bool has_any_skip() const;

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos,
                  const int64_t row_count, const int64_t target_count);
  int64_t get_serialize_size() const;
  // Overwrite the words in the original buffer at words_data_ptr_.
  // words_data_ptr_ must have been set by a prior deserialize().
  int rewrite_inplace() const;

  TO_STRING_KV(K_(row_count), K_(target_count), "total_words", words_.count(),
               KP_(words_data_ptr));

private:
  int64_t row_count_;
  int64_t target_count_;
  // Points into the redo buffer at the first word's position.
  // Set by deserialize(); used by rewrite_inplace().
  // Valid only while the buffer is alive (during on_register / on_replay).
  char *words_data_ptr_;
  common::ObSEArray<uint64_t, 4> words_;
};

class ObLsMdsTabletRelocBatchArg final
{
  OB_UNIS_VERSION(1);

public:
  ObLsMdsTabletRelocBatchArg();
  void reset();
  void reset_rows();
  bool is_valid() const;
  int push_row(const ObLsMdsTabletRelocRowStorage &row);
  void pop_back_row();
  int64_t estimate_serialize_size_after_push_row(const int64_t incoming_row_serialize_size);

  TO_STRING_KV(K_(version), K_(reloc_type), K_(ls_id),
               "target_cnt", targets_.count(), "row_cnt", rows_.count(),
               K_(skip_bitmap), K_(is_final));

public:
  int64_t version_;
  ObLsMdsTabletRelocType reloc_type_;
  share::ObLSID ls_id_;
  common::ObSEArray<ObLsMdsTabletRelocTarget, 4> targets_;
  common::ObSEArray<ObLsMdsTabletRelocRowStorage, 16> rows_;
  ObRelocSkipBitmap skip_bitmap_;
  // Last batch of a reloc transaction.  A final batch may contain no rows.
  bool is_final_;
private:
  void init_fixed_serialize_size();
  int64_t fixed_ser_size_;
  int64_t rows_ser_size_;
};

} // namespace storage
} // namespace oceanbase

#endif
