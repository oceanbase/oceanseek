/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "ob_memtable_data.h"

namespace oceanbase
{
namespace memtable
{

storage::ObLobOutTableRedoInfo *ObMemtableDataHeader::lob_redo_info()
{
  return with_lob_payload()
      ? reinterpret_cast<storage::ObLobOutTableRedoInfo *>(buf_ + ALIGN_UP(buf_len_, 8)) : nullptr;
}

const storage::ObLobOutTableRedoInfo *ObMemtableDataHeader::lob_redo_info() const
{
  return const_cast<ObMemtableDataHeader *>(this)->lob_redo_info();
}

storage::ObLobOutTableRedoInfo *ObMemtableData::lob_redo_info()
{
  return &lob_redo_info_;
}

const storage::ObLobOutTableRedoInfo *ObMemtableData::lob_redo_info() const
{
  return &lob_redo_info_;
}

bool ObMemtableDataHeader::with_lob_payload() const
{
  return ObMemtablePayloadKind::ROW_WITH_LOB_REDO == get_payload_kind();
}

} // namespace memtable
} // namespace oceanbase
