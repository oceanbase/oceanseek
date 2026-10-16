/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INC_SS_DAG_UTIL_H_
#define OCEANBASE_STORAGE_INC_SS_DAG_UTIL_H_

#include "storage/ob_storage_struct.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase {
namespace share {
class ObIDag;
class ObIDagNet;
}  // namespace share
namespace compaction {
struct ObTabletMergeDagParam;
}  // namespace compaction
namespace storage {
class ObTabletHandle;
class ObIncSSTableUploadParam;
struct ObIncReplaceUploadParam;
struct ObIncUploadDagNetParam;
class ObIncSSDagUtil {
public:
  static int schedule_inc_sst_upload_dag(ObIncSSTableUploadParam &param, const bool is_emergency = false);
  static int schedule_replace_upload_dag(ObIncReplaceUploadParam &param, const bool is_emergency = false);
  static int generate_replace_upload_dags(
      share::ObIDag *parent_dag,
      const ObIncReplaceUploadParam &param,
      const bool is_ha);
  static int generate_replace_upload_prepare_dags(
      share::ObIDagNet *dag_net,
      share::ObIDag *parent_dag,
      const ObIncReplaceUploadParam &param,
      const bool is_ha,
      const bool is_emergency = false);
  static share::ObDagId get_dag_net_id(share::ObIDag *dag);
private:
  static compaction::ObTabletMergeDagParam build_replace_minor_merge_param_(
      const ObIncReplaceUploadParam &param,
      const int32_t private_transfer_epoch,
      const bool is_ha);
  static int build_merge_tables_result_(
      const ObIncReplaceUploadParam &param,
      ObTabletHandle &tablet_handle,
      ObGetMergeTablesResult &result);
  static int do_schedule_upload_dag_net_(ObIncUploadDagNetParam &dag_net_param, const bool is_emergency);
};
}  // namespace storage
}  // namespace oceanbase

#endif
