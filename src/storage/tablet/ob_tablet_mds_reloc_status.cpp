/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "ob_tablet_mds_reloc_status.h"

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObTabletMdsRelocStatus, reloc_done_, redo_scn_);
// redo_scn_ is a UNIS-appended tail field kept at OB_UNIS_VERSION(1):
// OB_UNIS_DECODE stops at the object length header when the payload is
// exhausted, so a new binary reading a legacy V1 marker (which encodes only
// reloc_done_) deserializes successfully and leaves redo_scn_ at its default
// (invalid) value.  New binaries also leave redo_scn_ invalid when executing
// the V1 protocol; only V2 initializes and captures the redo fence.  Pinned by
// unit tests in
// unittest/storage/mds_table/test_mds_table.cpp.

} // namespace storage
} // namespace oceanbase
