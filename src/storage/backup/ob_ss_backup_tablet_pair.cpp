/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_ss_backup_tablet_pair.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace backup
{

OB_SERIALIZE_MEMBER(ObSSBackupTabletPair, tablet_id_, transfer_scn_);

ObSSBackupTabletPair::ObSSBackupTabletPair()
  : tablet_id_(),
    transfer_scn_()
{
}

ObSSBackupTabletPair::~ObSSBackupTabletPair()
{
}

bool ObSSBackupTabletPair::is_valid() const
{
  return tablet_id_.is_valid() && transfer_scn_.is_valid();
}

void ObSSBackupTabletPair::reset()
{
  tablet_id_.reset();
  transfer_scn_.reset();
}

bool ObSSBackupTabletPair::operator==(const ObSSBackupTabletPair &other) const
{
  return tablet_id_ == other.tablet_id_ && transfer_scn_ == other.transfer_scn_;
}

bool ObSSBackupTabletPair::operator!=(const ObSSBackupTabletPair &other) const
{
  return !(*this == other);
}

int ObSSBackupTabletPair::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = 0;
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = common::murmurhash(&transfer_scn_, sizeof(transfer_scn_), hash_val);
  return ret;
}

} // namespace backup
} // namespace oceanbase
