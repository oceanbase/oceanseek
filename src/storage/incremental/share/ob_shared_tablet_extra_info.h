/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_TABLET_EXTRA_INFO_
#define OCEANBASE_STORAGE_OB_SHARED_TABLET_EXTRA_INFO_
#include "common/log/ob_log_constants.h"
#include "share/ob_define.h"
#include "share/scn.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_attr.h"
#include "storage/meta_store/ob_startup_accelerate_info.h"
#include "storage/slog/ob_storage_log.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"

namespace oceanbase
{
namespace storage
{

class ObSSTabletAttr final
{
  OB_UNIS_VERSION(1);
public:
  ObSSTabletAttr();
  ~ObSSTabletAttr() = default;
  void reset();
  bool is_valid() const;
  int convert_to_tablet_attr(
      const share::SCN &row_scn,
      ObTabletAttr &tablet_attr) const;

  TO_STRING_KV(K_(iter_attr),
               K_(ha_status),
               K_(all_sstable_data_occupy_size),
               K_(all_sstable_data_required_size),
               K_(tablet_meta_size),
               K_(backup_bytes),
               K_(ss_public_sstable_occupy_size));

public:
  ObTabletFastIterAttr iter_attr_;
  int64_t ha_status_;
  int64_t all_sstable_data_occupy_size_;
  // all sstable data requred_size, data_block_count * 2MB, include major sstable
  int64_t all_sstable_data_required_size_;
  // meta_size in shared_nothing, meta_block_count * 2MB
  int64_t tablet_meta_size_;
  // major sstable data occupy_size
  // which is same as major_sstable_required_size_;
  // because the alignment size is 1B in object_storage.
  int64_t backup_bytes_;
  int64_t ss_public_sstable_occupy_size_;
};

struct ObSSUpdateTabletLog final
{
  OB_UNIS_VERSION(1);
public:
  ObSSUpdateTabletLog();
  ~ObSSUpdateTabletLog() = default;
  bool is_valid() const;
  void reset();
  int convert_to_update_log(
      const share::SCN &row_scn,
      ObUpdateTabletLog &update_log) const;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(reorg_scn), K_(ss_tablet_attr), K_(accelerate_info),
      K_(deleted_scn), K_(must_cache_size), K_(try_cache_size), K_(transfer_scn),
      K_(tablet_serialize_size));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorg_scn_;
  storage::ObSSTabletAttr ss_tablet_attr_;
  storage::ObStartupTabletAccelerateInfo accelerate_info_;
  share::SCN deleted_scn_;
  int64_t must_cache_size_;
  int64_t try_cache_size_;
  share::SCN transfer_scn_;
  int64_t tablet_serialize_size_;
};

} //storage
} //oceanbase


#endif
