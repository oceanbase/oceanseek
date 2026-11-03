/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_UTILS_
#define OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_UTILS_

#include "storage/incremental/ob_inc_ss_define.h"
#include "share/scn.h"
#include "storage/ls/ob_ls.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase {
using namespace share;
namespace storage
{

class ObSSTransferBackfillUtils
{
public:
  static int check_tablet_need_upload(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &need_upload);
  static int get_and_check_source_tablet(
      ObLS &ls, const ObTabletBackfillInfo &tablet_info, ObTabletHandle &tablet_handle);
  static int finish_tablet_upload(ObLS &ls, const ObTabletBackfillInfo &tablet_info);
  static int check_tablet_need_backfill_tx(
      const ObTabletHandle &tablet_handle,
      const share::SCN &backfill_scn,
      bool &need_backfill_tx);
  static int check_tablet_need_replace_table(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &need_replace_table);
  static int check_self_is_sswriter(
      const ObSSWriterType type,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      bool &is_sswriter);
  static int notify_sswriter_do_backfill(
      const ObSSWriterType type,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id);
  static int check_ss_tablet_ready(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      bool &is_ready);
  static int check_self_is_sswriter(
      const ObSSWriterType type,
      const ObLSID &ls_id,
      bool &is_sswriter);
  static int get_ss_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &reorg_scn,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle,
      const bool read_mark_delete);
  static int get_ss_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &reorg_scn,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle,
      share::SCN &meta_scn,
      const bool read_mark_delete);
  static int create_ss_tablet(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id);
  static int get_ss_ls_meta(
      const ObLSID &ls_id,
      ObSSLSMeta &ls_meta);
  static int check_ss_src_tablet_ready(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &reorg_scn,
      const share::SCN &backfill_scn,
      bool &is_tablet_exist,
      bool &is_data_uploaded,
      bool &is_data_backfilled);

};

} //storage
} //oceanbase
#endif
