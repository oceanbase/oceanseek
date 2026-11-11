/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/oblog/ob_log_print_kv.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_direct_load_type.h"
#include "storage/tx/ob_trans_id.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace storage
{

// The transaction module holds ObDDLIncLogBasic by value, while ob_ddl_inc_clog.h depends on ObITable::TableType.
// Move the basic structure to a lightweight header to avoid circular dependencies among transaction definitions, DDL clog, and ObITable.
class ObDDLIncLogBasic final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncLogBasic();
  ~ObDDLIncLogBasic() = default;
  int init(const ObTabletID &tablet_id,
          const ObTabletID &lob_meta_tablet_id,
          const ObTabletID &lob_piece_tablet_id,
          const ObDirectLoadType direct_load_type,
          const transaction::ObTransID &trans_id,
          const transaction::ObTxSEQ &seq_no,
          const int64_t snapshot_version,
          const uint64_t data_format_version);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  void reset()
  {
    tablet_id_.reset();
    lob_meta_tablet_id_.reset();
    lob_piece_tablet_id_.reset();
    direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
    trans_id_.reset();
    seq_no_.reset();
    snapshot_version_ = 0;
    data_format_version_ = 0;
  }
  bool operator ==(const ObDDLIncLogBasic &other) const
  {
    return tablet_id_ == other.get_tablet_id()
              && lob_meta_tablet_id_ == other.get_lob_meta_tablet_id()
              && lob_piece_tablet_id_ == other.get_lob_piece_tablet_id()
              && direct_load_type_ == other.get_direct_load_type()
              && trans_id_ == other.get_trans_id()
              && seq_no_ == other.get_seq_no();
  }
  bool is_valid() const
  {
    return tablet_id_.is_valid();
  }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  const ObTabletID &get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  const ObTabletID &get_lob_piece_tablet_id() const { return lob_piece_tablet_id_; }
  const ObDirectLoadType &get_direct_load_type() const { return direct_load_type_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  const transaction::ObTxSEQ &get_seq_no() const { return seq_no_; }
  const int64_t &get_snapshot_version() const { return snapshot_version_; }
  const uint64_t &get_data_format_version() const { return data_format_version_; }
  TO_STRING_KV(K_(tablet_id), K_(lob_meta_tablet_id), K_(lob_piece_tablet_id), K_(direct_load_type),
      K_(trans_id), K_(seq_no), K_(snapshot_version), K_(data_format_version));
private:
  ObTabletID tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  ObTabletID lob_piece_tablet_id_;
  ObDirectLoadType direct_load_type_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
  int64_t snapshot_version_;
  uint64_t data_format_version_;
};

} // namespace storage
} // namespace oceanbase
