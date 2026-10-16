// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#include "storage/incremental/ob_ss_inc_checkpoint.h"

#include "storage/compaction/ob_compaction_util.h"
#include "logservice/ob_log_base_type.h"

namespace oceanbase {

using namespace share;
using namespace compaction;
using namespace logservice;

namespace storage {

ObSSIncCheckpoint::ObSSIncCheckpoint()
{
  // Make sure merge type is not changed
  STATIC_ASSERT(ObMergeType::INVALID_MERGE_TYPE == 0,       "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::MINOR_MERGE == 1,              "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::HISTORY_MINOR_MERGE == 2,      "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::META_MAJOR_MERGE == 3,         "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::MINI_MERGE == 4,               "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::MAJOR_MERGE == 5,              "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::MEDIUM_MERGE == 6,             "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::DDL_KV_MERGE == 7,             "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::BACKFILL_TX_MERGE == 8,        "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::BATCH_EXEC == 9,               "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::CONVERT_CO_MAJOR_MERGE == 10, "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::INC_MAJOR_MERGE == 11,        "MergeType Changed. Please Check It.");
  STATIC_ASSERT(ObMergeType::MERGE_TYPE_MAX == 12,         "MergeType Changed. Please Check It.");

  // Make sure ObLogBaseType
  STATIC_ASSERT(ObLogBaseType::INVALID_LOG_BASE_TYPE == 0,                                "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE == 1,                          "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TABLET_OP_LOG_BASE_TYPE == 2,                              "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::STORAGE_SCHEMA_LOG_BASE_TYPE == 3,                         "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TABLET_SEQ_SYNC_LOG_BASE_TYPE == 4,                        "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DDL_LOG_BASE_TYPE == 5,                                    "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::KEEP_ALIVE_LOG_BASE_TYPE == 6,                             "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TIMESTAMP_LOG_BASE_TYPE == 7,                              "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TRANS_ID_LOG_BASE_TYPE == 8,                               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::GC_LS_LOG_BASE_TYPE == 9,                                  "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::MAJOR_FREEZE_LOG_BASE_TYPE == 10,                          "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::PRIMARY_LS_SERVICE_LOG_BASE_TYPE == 11,                    "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::RECOVERY_LS_SERVICE_LOG_BASE_TYPE == 12,                   "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::STANDBY_TIMESTAMP_LOG_BASE_TYPE == 13,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::GAIS_LOG_BASE_TYPE == 14,                                  "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DAS_ID_LOG_BASE_TYPE == 15,                                "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::RESTORE_SERVICE_LOG_BASE_TYPE == 16,                       "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::RESERVED_SNAPSHOT_LOG_BASE_TYPE == 17,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::MEDIUM_COMPACTION_LOG_BASE_TYPE == 18,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::ARB_GARBAGE_COLLECT_SERVICE_LOG_BASE_TYPE == 19,           "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DATA_DICT_LOG_BASE_TYPE == 20,                             "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::ARBITRATION_SERVICE_LOG_BASE_TYPE == 21,                   "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::NET_STANDBY_TNT_SERVICE_LOG_BASE_TYPE == 22,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::NET_ENDPOINT_INGRESS_LOG_BASE_TYPE == 23,                  "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::HEARTBEAT_SERVICE_LOG_BASE_TYPE == 24,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::PADDING_LOG_BASE_TYPE == 25,                               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DUP_TABLE_LOG_BASE_TYPE == 26,                             "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::OBJ_LOCK_GARBAGE_COLLECT_SERVICE_LOG_BASE_TYPE == 27,      "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TENANT_TRANSFER_SERVICE_LOG_BASE_TYPE == 28,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TENANT_BALANCE_SERVICE_LOG_BASE_TYPE == 29,                "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::BALANCE_EXECUTE_SERVICE_LOG_BASE_TYPE == 30,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::BACKUP_TASK_SCHEDULER_LOG_BASE_TYPE == 31,                 "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::BACKUP_DATA_SERVICE_LOG_BASE_TYPE == 32,                   "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::BACKUP_CLEAN_SERVICE_LOG_BASE_TYPE == 33,                  "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::BACKUP_ARCHIVE_SERVICE_LOG_BASE_TYPE == 34,                "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TRANSFER_HANDLER_LOG_BASE_TYPE == 35,                      "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::COMMON_LS_SERVICE_LOG_BASE_TYPE == 36,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::LS_BLOCK_TX_SERVICE_LOG_BASE_TYPE == 37,                   "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::WORKLOAD_REPOSITORY_SERVICE_LOG_BASE_TYPE == 38,           "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TTL_LOG_BASE_TYPE == 39,                                   "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TABLE_LOAD_RESOURCE_SERVICE_LOG_BASE_TYPE == 40,           "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::SNAPSHOT_SCHEDULER_LOG_BASE_TYPE == 41,                    "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::CLONE_SCHEDULER_LOG_BASE_TYPE == 42,                       "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::MVIEW_MAINTENANCE_SERVICE_LOG_BASE_TYPE == 43,             "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::SHARE_STORAGE_NRT_THROT_LOG_BASE_TYPE == 44,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DBMS_SCHEDULER_LOG_BASE_TYPE == 45,                        "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::SHARED_STORAGE_PRE_WARM_LOG_BASE_TYPE == 46,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::SHARE_STORAGE_PUBLIC_BLOCK_GC_SERVICE_LOG_BASE_TYPE == 47, "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::VEC_INDEX_LOG_BASE_TYPE == 48,                             "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TABLE_LOCK_LOG_BASE_TYPE == 49,                            "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DBMS_SCHEDULER_GC_LOG_BASE_TYPE == 50,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::SHARED_STORAGE_SSWRITER_LOG_BASE_TYPE == 51,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::SYS_DDL_SCHEDULER_LOG_BASE_TYPE == 52,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DISASTER_RECOVERY_SERVICE_LOG_BASE_TYPE == 53,             "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::DDL_SERVICE_LAUNCHER_LOG_BASE_TYPE == 54,                  "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TABLE_SESS_ID_LOG_BASE_TYPE == 58,                         "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::BACKUP_VALIDATE_SERVICE_LOG_BASE_TYPE == 59,               "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::VEC_INDEX_SERVICE_LOG_BASE_TYPE == 60,                     "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::TTL_SERVICE_LOG_BASE_TYPE == 62,                          "LogBaseType Changed. Please Check it.");
  STATIC_ASSERT(ObLogBaseType::MAX_LOG_BASE_TYPE,                                         "LogBaseType Changed. Please Check it.");
}


const char *ObSSIncCheckpoint::get_inc_ckpt_string(int64_t type_idx)
{
  STATIC_ASSERT(int64_t(SSIncCheckpointType::SS_INC_META) == 0, "invalid ss inc type");
  STATIC_ASSERT(int64_t(SSIncCheckpointType::SS_INC_SSTABLE) == 1, "invalid ss inc type");
  STATIC_ASSERT(int64_t(SSIncCheckpointType::MAX_SS_INC_CHECKPOINT_TYPE) == 2, "invalid ss inc type");
  const static char META_TYPE_TO_CHAR[SSIncMetaType::MAX_INC_META_TYPE + 1][32] = {
      "SS_INC_META", "SS_INC_SSTABLE", "MAX_SS_INC_CHECKPOINT_TYPE"};
  return META_TYPE_TO_CHAR[type_idx];
}

}  // namespace storage
}  // namespace oceanbase
