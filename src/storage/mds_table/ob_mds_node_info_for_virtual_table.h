/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_NODE_INFO_FOR_VIRTUAL_TABLE_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_NODE_INFO_FOR_VIRTUAL_TABLE_H

#include "lib/ob_errno.h"
#include "common/ob_tablet_id.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/multi_data_source/mds_writer.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
// Row-shaped snapshot for __all_virtual_mds_node_stat and tablet::fill_virtual_info paths.
// Fields not filled by a particular producer remain default / invalid.
struct MdsNodeInfoForVirtualTable {
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  uint8_t unit_id_;
  common::ObStringHolder user_key_;
  common::ObStringHolder user_data_;
  int64_t version_idx_;
  MdsWriter writer_;
  transaction::ObTxSEQ seq_no_;
  share::SCN redo_scn_;
  share::SCN end_scn_;
  share::SCN trans_version_;
  MdsNodeType node_type_;
  TwoPhaseCommitState state_;
  NodePosition position_;

  MdsNodeInfoForVirtualTable()
      : ls_id_(),
        tablet_id_(),
        unit_id_(0),
        version_idx_(0),
        writer_(),
        seq_no_(),
        redo_scn_(),
        end_scn_(),
        trans_version_(),
        node_type_(MdsNodeType::UNKNOWN_NODE),
        state_(TwoPhaseCommitState::STATE_INIT),
        position_(NodePosition::POSITION_END)
  {}

  // ObArray relocation uses construct_assign / copy_assign with assign(const T&), not copy ctor
  // (ObStringHolder is non-copy-constructible).
  int assign(const MdsNodeInfoForVirtualTable &rhs)
  {
    int ret = OB_SUCCESS;
    if (this != &rhs) {
      ls_id_ = rhs.ls_id_;
      tablet_id_ = rhs.tablet_id_;
      unit_id_ = rhs.unit_id_;
      if (OB_FAIL(user_key_.assign(rhs.user_key_))) {
      } else if (OB_FAIL(user_data_.assign(rhs.user_data_))) {
      } else {
        version_idx_ = rhs.version_idx_;
        writer_ = rhs.writer_;
        seq_no_ = rhs.seq_no_;
        redo_scn_ = rhs.redo_scn_;
        end_scn_ = rhs.end_scn_;
        trans_version_ = rhs.trans_version_;
        node_type_ = rhs.node_type_;
        state_ = rhs.state_;
        position_ = rhs.position_;
      }
    }
    return ret;
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    (void)common::databuff_printf(buf, buf_len, pos, "{ls_id:");
    if (buf_len > pos) {
      pos += ls_id_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " tablet_id:");
    if (buf_len > pos) {
      pos += tablet_id_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf,
        buf_len,
        pos,
        " unit_id:%u version_idx:%ld node_type:%s state:%s position:%s user_key:",
        static_cast<unsigned>(unit_id_),
        version_idx_,
        obj_to_string(node_type_),
        obj_to_string(state_),
        obj_to_string(position_));
    if (buf_len > pos) {
      pos += user_key_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " user_data:");
    if (buf_len > pos) {
      pos += user_data_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " writer:");
    if (buf_len > pos) {
      pos += writer_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " seq_no:");
    if (buf_len > pos) {
      pos += seq_no_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " redo_scn:");
    if (buf_len > pos) {
      pos += redo_scn_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " end_scn:");
    if (buf_len > pos) {
      pos += end_scn_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, " trans_version:");
    if (buf_len > pos) {
      pos += trans_version_.to_string(buf + pos, buf_len - pos);
    }
    (void)common::databuff_printf(buf, buf_len, pos, "}");
    return pos;
  }
};

} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_NODE_INFO_FOR_VIRTUAL_TABLE_H
