/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_LOB_OUTTABLE_REDO_PAYLOAD_H_
#define OCEANBASE_STORAGE_LOB_OUTTABLE_REDO_PAYLOAD_H_

#include "common/object/ob_object.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace common
{
class ObStoreRowkey;
}
namespace storage
{

// Object data serialized alongside its native Meta row redo, not stored as a Meta table column.
struct ObLobOutTableRedoPayload
{
  OB_UNIS_VERSION(1);
public:
  ObLobOutTableRedoPayload();
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(lob_col_idx), K_(lob_id), K_(seq_id), K_(lob_piece_table_id),
      K_(lob_operation_seq_no), K_(macro_id), K_(is_logging), K_(byte_len),
      "data_length", data_.length());

  int64_t lob_col_idx_;
  common::ObLobId lob_id_;
  common::ObString seq_id_;
  uint64_t lob_piece_table_id_;
  transaction::ObTxSEQ lob_operation_seq_no_;
  blocksstable::MacroBlockId macro_id_;
  bool is_logging_;
  int64_t byte_len_;
  common::ObString data_;
};

// Stored after the aligned row bytes in a LOB trans node. The memstore owns this
// descriptor and holds the separately allocated body until submission or cleanup.
struct ObLobOutTableRedoInfo
{
  ObLobOutTableRedoInfo();
  int init(const uint64_t tenant_id, const ObLobOutTableRedoPayload &payload);
  void init_replay(const common::ObString &serialized_payload);
  void release();
  void checksum(common::ObBatchChecksum &bc);

  // The node owns these serialized bytes until submission or cleanup.
  char *redo_buf_;
  int64_t redo_size_;
  uint64_t checksum_;
  bool checksum_ready_;
private:
  void ensure_checksum_();
};

struct ObLobOutTableRedoCtx
{
  explicit ObLobOutTableRedoCtx(const common::ObIArray<ObLobOutTableRedoPayload> &payloads);
  int find_payload(
      const common::ObStoreRowkey &rowkey,
      const ObLobOutTableRedoPayload *&payload) const;

  // Borrowed for this Meta DML; the writer copies bytes before callback publication.
  const common::ObIArray<ObLobOutTableRedoPayload> &payloads_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OUTTABLE_REDO_PAYLOAD_H_
