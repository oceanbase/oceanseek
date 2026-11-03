/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 *
 * Payload for ObTxDataSourceType::LS_MDS_TABLET_RELOC_BATCH.
 * - Each row: (mds_unit_id, meta, serialized value blob).
 * - The relocation type identifies transfer, split, or fork and remains unchanged across batches.
 * - Target tablet_id and destination MDS row key live in the batch target list
 *   and are broadcast to every row in the batch at apply time. Transfer uses
 *   its start SCN as the key; fork uses the destination reorganization SCN.
 * - Source tablet-status rows (ObTabletCreateDeleteMdsUserData / DummyKey) are NOT shipped.
 * - V2 empty non-final batch / final batch are the marker mutex INSERT and fence
 *   PUT. See ObTabletMdsRelocStatus for the marker protocol.
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_BATCH_ARG_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_BATCH_ARG_H_

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
// V2: op-semantic reloc.  Op-type rows are NOT carried in
// rows_; instead each replayer pulls them locally from the source LS so the dest
// row-set stays self-consistent with its own major progress. Transfer uses the
// persisted transfer task's data_version; split/fork use their DDL task version.
static const int64_t LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2 = 2;

enum class ObTabletMdsRelocType : int8_t
{
  INVALID = 0,
  TRANSFER,
  SPLIT,
  FORK,
  MAX_TYPE,
};

OB_INLINE bool is_valid_reloc_type(const ObTabletMdsRelocType type)
{
  return type > ObTabletMdsRelocType::INVALID
      && type < ObTabletMdsRelocType::MAX_TYPE;
}

// Route reloc events into the driving operation's existing SERVER_EVENT module
// (__all_server_event_history), so operators find them alongside that
// operation's other events.  The replay/pull path has no application-task
// context, so routing by reloc_type here keeps a single source of truth.
OB_INLINE const char *reloc_event_module(const ObTabletMdsRelocType type)
{
  const char *module = "MDS_RELOC";
  switch (type) {
    case ObTabletMdsRelocType::TRANSFER:
      module = "TRANSFER";
      break;
    case ObTabletMdsRelocType::SPLIT:
    case ObTabletMdsRelocType::FORK:
      module = "ddl";
      break;
    default:
      break;
  }
  return module;
}

struct ObTabletMdsRelocTarget final
{
  OB_UNIS_VERSION(1);

public:
  common::ObTabletID tablet_id_;    // dest tablet id
  // dest MDS row key: transfer uses transfer-start SCN, fork uses dest
  // reorganization SCN, split uses SCN::min_scn().
  share::SCN transfer_scn_;
  // V2-only op-semantic fields.  For op-type rows the replayer pulls from the
  // source tablet locally instead of reading them out of the batch.  These carry
  // the source key for locating op rows and the source reorganization SCN for
  // replay-side generation validation (src_reorg_scn_ must still match the source
  // tablet reorg SCN before op rows are pulled).  For transfer,
  // src_tablet_id_ == tablet_id_; for split/fork they differ.  They serialize as
  // UNIS-appended fields (invalid/default in V1) so old readers skip them.
  common::ObTabletID src_tablet_id_;
  share::SCN src_reorg_scn_;

  TO_STRING_KV(K_(tablet_id), K_(transfer_scn), K_(src_tablet_id), K_(src_reorg_scn));

  void reset()
  {
    tablet_id_.reset();
    transfer_scn_.reset();
    src_tablet_id_.reset();
    src_reorg_scn_.reset();
  }
};

struct ObTabletMdsRelocRowStorage final
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
class ObTabletMdsRelocSkipBitmap final
{
public:
  ObTabletMdsRelocSkipBitmap() : row_count_(0), target_count_(0), words_data_ptr_(nullptr) {}
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

class ObTabletMdsRelocBatchArg final
{
  OB_UNIS_VERSION(1);

public:
  ObTabletMdsRelocBatchArg();
  void reset();
  void reset_rows();
  bool is_valid() const;
  int push_row(const ObTabletMdsRelocRowStorage &row);
  void pop_back_row();
  int64_t estimate_serialize_size_after_push_row(const int64_t incoming_row_serialize_size);

  bool is_v2() const { return LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2 == version_; }
  // No extra serialized flag is needed: V1 never emits an empty non-final
  // batch, so V2 can reserve that wire-compatible shape for acquiring the
  // marker mutex before any payload/op row is written.
  bool is_marker_lock_batch() const;

  TO_STRING_KV(K_(version), K_(reloc_type), K_(ls_id), K_(src_ls_id), K_(read_snapshot),
               "target_cnt", targets_.count(), "row_cnt", rows_.count(),
               K_(skip_bitmap), K_(is_final));

public:
  int64_t version_;
  ObTabletMdsRelocType reloc_type_;
  share::ObLSID ls_id_;         // dest ls id (the MDS txn is registered here)
  // V2-only fields.  On replay each replica pulls op-type rows from the local
  // source LS replica at read_snapshot_.  Invalid in V1 and serialized only
  // when version_ == V2; decoding rejects versions other than V1 and V2.
  // Compatibility with an old binary is guaranteed by
  // the min-data-version upgrade barrier; these fields are not tail-appended
  // fields that a V1 BatchArg reader could skip.  read_snapshot_ is
  // SCN::max_scn() for transfer (source is frozen at transfer-out) and the
  // split/fork snapshot otherwise. All submitters select the protocol from
  // the data version pinned in their transfer or DDL task.
  share::ObLSID src_ls_id_;
  share::SCN read_snapshot_;
  common::ObSEArray<ObTabletMdsRelocTarget, 4> targets_;
  common::ObSEArray<ObTabletMdsRelocRowStorage, 16> rows_;
  ObTabletMdsRelocSkipBitmap skip_bitmap_;
  // Last batch of a reloc transaction.  A final batch may contain no rows and,
  // in V2, updates the marker inserted by is_marker_lock_batch().
  bool is_final_;
private:
  void init_fixed_serialize_size();
  int64_t fixed_ser_size_;
  int64_t rows_ser_size_;
};

} // namespace storage
} // namespace oceanbase

#endif
