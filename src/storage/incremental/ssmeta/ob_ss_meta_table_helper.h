/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_TABLE_HELPER_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_TABLE_HELPER_H_

#include "share/scn.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSMetaTableHelper
{
public:
  ObSSMetaTableHelper() {}
  ~ObSSMetaTableHelper() {}

  static int get_meta_info(
      const ObSSMetaReadParamV2 &read_param,
      const share::SCN &read_snapshot,
      common::ObArenaAllocator &allocator,
      ObSSRawMetaRow &row);
  static int get_meta_info(
      const ObSSMetaReadParamV2 &read_param,
      const share::SCN &read_snapshot,
      common::ObArenaAllocator &allocator,
      ObSSMetaOpBuffer &meta_value,
      share::SCN &meta_scn);

  static int get_tablet(
      const ObSSMetaReadParamV2 &read_param,
      const share::SCN &read_snapshot,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle,
      ObSSMetaUpdateMetaInfo &update_meta_info,
      share::SCN &meta_scn);
  static int get_tablet(
      const ObSSMetaReadParamV2 &read_param,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &tablet_handle);
  static int get_tablet_scn(
      const ObSSMetaReadParamV2 &read_param,
      const share::SCN &read_snapshot,
      share::SCN &meta_scn);
  static int get_tablet_last_major_snapshot_version(
      const ObSSMetaReadParamV2 &read_param,
      int64_t &last_major_snapshot_version,
      bool &is_mark_delete,
      ObSSMetaOpBuffer *meta_value = nullptr);
  static int get_ls(
      const ObSSMetaReadParamV2 &read_param,
      const share::SCN &read_snapshot,
      ObDefaultSSMetaValue<ObSSLSMeta> &ls_value,
      share::SCN &meta_scn);
  static int get_ls(
      const ObSSMetaReadParamV2 &read_param,
      ObDefaultSSMetaValue<ObSSLSMeta> &ls_value);

  static int delete_meta_info(ObSSMetaDeleteParamV2 &delete_param);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaTableHelper);
};

} // end namespace storage
} // end namespace oceanbase
#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_TABLE_HELPER_H_ */
