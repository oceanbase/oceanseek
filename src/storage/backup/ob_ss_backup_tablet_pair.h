/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BACKUP_SS_BACKUP_TABLET_PAIR_H_
#define OCEANBASE_STORAGE_BACKUP_SS_BACKUP_TABLET_PAIR_H_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace backup
{

struct ObSSBackupTabletPair final
{
  OB_UNIS_VERSION(1);
public:
  ObSSBackupTabletPair();
  ~ObSSBackupTabletPair();
  bool is_valid() const;
  void reset();
  bool operator==(const ObSSBackupTabletPair &other) const;
  bool operator!=(const ObSSBackupTabletPair &other) const;
  int hash(uint64_t &hash_val) const;
  TO_STRING_KV(K_(tablet_id), K_(transfer_scn));
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
};

} // namespace backup
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BACKUP_SS_BACKUP_TABLET_PAIR_H_
