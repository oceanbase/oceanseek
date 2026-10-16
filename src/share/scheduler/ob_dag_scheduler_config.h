/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifdef DAG_SCHEDULER_DAG_NET_TYPE_DEF
// DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_ENUM, DAG_NET_TYPE_STR)
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_COMPLETE_MIGARTION, "DAG_NET_COMPLETE_MIGRATION")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_TRANSFER, "DAG_NET_TRANSFER")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_BACKUP, "DAG_NET_BACKUP")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_BACKUP_CLEAN, "DAG_NET_TYPE_BACKUP_CLEAN")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_CO_MAJOR, "DAG_NET_TYPE_CO_MAJOR")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TRANSFER_BACKFILL_TX, "DAG_NET_TRANSFER_BACKFILL_TX")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_SS_MIGRATION, "DAG_NET_SS_MIGRATION")
#ifdef OB_BUILD_SHARED_STORAGE
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_SS_TRANSFER_BACKFILL_TX, "DAG_NET_SS_TRANSFER_BACKFILL_TX")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_SS_HA_MACRO_COPY, "DAG_NET_SS_HA_MACRO_COPY")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_SS_HA_MACRO_DELETE, "DAG_NET_SS_HA_MACRO_DELETE")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_SS_INC_UPLOAD, "DAG_NET_SS_INC_UPLOAD")
#endif
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_BACKUP_VALIDATE, "DAG_NET_TYPE_BACKUP_VALIDATE")
DAG_SCHEDULER_DAG_NET_TYPE_DEF(DAG_NET_TYPE_MAX, "DAG_NET_TYPE_MAX")
#endif

#ifdef DAG_SCHEDULER_DAG_PRIO_DEF
#define DEFAULT_COMPACTION_HIGH_THREAD_CNT 6
#define DEFAULT_SS_DATA_CHECK_THREAD_SCORE 3
// DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_ENUM, DAG_PRIORITY_SCORE, DAG_PRIORITY_STR, DAG_FUNCTION_TYPE)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_COMPACTION_HIGH,   DEFAULT_COMPACTION_HIGH_THREAD_CNT, "PRIO_COMPACTION_HIGH", PRIO_COMPACTION_HIGH)
#ifdef OB_BUILD_SHARED_STORAGE
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_INC_SSTABLE_UPLOAD, DEFAULT_COMPACTION_HIGH_THREAD_CNT, "PRIO_INC_SSTABLE_UPLOAD", PRIO_INC_SSTABLE_UPLOAD)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_LINK_SHARED_SSTABLE, 6, "PRIO_LINK_SHARED_SSTABLE", PRIO_LINK_SHARED_SSTABLE)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_MARK_SWEEP, 1, "PRIO_MARK_SWEEP", PRIO_SS_GC)
#endif
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_HA_HIGH,        8, "PRIO_HA_HIGH", PRIO_HA_HIGH)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_LS_CKPT,       16, "PRIO_LS_CKPT", PRIO_LS_CKPT)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_COMPACTION_MID, 6, "PRIO_COMPACTION_MID", PRIO_COMPACTION_MID)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_HA_MID,         5, "PRIO_HA_MID", PRIO_HA_MID)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_COMPACTION_LOW, 6, "PRIO_COMPACTION_LOW", PRIO_COMPACTION_LOW)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_HA_LOW,         2, "PRIO_HA_LOW", PRIO_HA_LOW)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_DDL,            2, "PRIO_DDL", PRIO_DDL)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_FORK_DATABASE, 4, "PRIO_FORK_DATABASE", PRIO_DDL)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_DDL_HIGH,       6, "PRIO_DDL_HIGH", PRIO_DDL_HIGH)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_TTL,            2, "PRIO_TTL", PRIO_KV_TTL)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_SS_GC_EXEC,     1, "PRIO_SS_GC_EXEC", PRIO_SS_GC)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_LTABLE_BG,      2, "PRIO_LTABLE_BG", PRIO_LTABLE_BG)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_ASYNC_INDEX_MERGE_MID, 6, "PRIO_ASYNC_INDEX_MERGE_MID", PRIO_ASYNC_INDEX_MERGE_MID)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_ASYNC_INDEX_MERGE_LOW, 4, "PRIO_ASYNC_INDEX_MERGE_LOW", PRIO_ASYNC_INDEX_MERGE_LOW)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_VECTOR_INDEX,   16, "PRIO_VECTOR_INDEX", PRIO_VECTOR_INDEX)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_SS_GC_GEN,      3, "PRIO_SS_GC_GEN", PRIO_SS_GC)
DAG_SCHEDULER_DAG_PRIO_DEF(
    DAG_PRIO_SS_DATA_CHECK, DEFAULT_SS_DATA_CHECK_THREAD_SCORE,
    "PRIO_SS_DATA_CHECK", PRIO_SS_GC)
DAG_SCHEDULER_DAG_PRIO_DEF(DAG_PRIO_MAX,            0, "INVALID", MAX_FUNCTION_NUM)
/*
* Attention! need apply for new therad & new parameter to manage new DAG_PRIO_QUEUE
*/
#undef DEFAULT_COMPACTION_HIGH_THREAD_CNT
#undef DEFAULT_SS_DATA_CHECK_THREAD_SCORE
#endif

#ifdef DAG_SCHEDULER_DAG_TYPE_DEF
// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_ENUM, DAG_DEFAULT_PRIO, SYS_TASK_TYPE, DAG_TYPE_STR, DAG_MODULE_STR, DIAGNOSE_WITH_COMMENT, DIAGNOSE_PRIORITY, DIAGNOSE_INT_INFO_CNT, DIAGNOSE_INT_FMT_STR)
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MINI_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::SSTABLE_MINI_MERGE_TASK, "MINI_MERGE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MERGE_EXECUTE, ObDagPrio::DAG_PRIO_COMPACTION_MID, ObSysTaskType::SSTABLE_MINOR_MERGE_TASK, "MINOR_EXECUTE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MAJOR_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "MAJOR_MERGE/MEDIUM_MERGE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_EXECUTE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_EXECUTE", "COMPACTION",
    true, 5, {"ls_id", "tablet_id", "compaction_scn", "range_count", "persisted_range_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_PREPARE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_PREPARE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_CO_MERGE_SCHEDULE, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "CO_MERGE_SCHEDULE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TX_TABLE_MERGE, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::SPECIAL_TABLE_MERGE_TASK, "TX_TABLE_MERGE", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VERIFY_CKM, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "VERIFY_CKM", "COMPACTION",
    false, 2, {"ls_id", "tablet_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BATCH_FREEZE_TABLETS, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::BATCH_FREEZE_TABLET_TASK, "BATCH_FREEZE", "COMPACTION",
    false, 2, {"ls_id", "tablet_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_FIRST_PERSIST, ObDagPrio::DAG_PRIO_COMPACTION_HIGH, ObSysTaskType::TABLET_FIRST_PERSIST_TASK, "TABLET_FIRST_PERSIST", "STORAGE",
    false, 2, {"ls_id", "tablet_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_UPDATE_SKIP_MAJOR, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "UPDATE_SKIP_MAJOR", "COMPACTION",
    false, 2, {"ls_id", "tablet_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_REFRESH_SSTABLES, ObDagPrio::DAG_PRIO_COMPACTION_LOW, ObSysTaskType::SSTABLE_MAJOR_MERGE_TASK, "REFRESH_MAJOR", "COMPACTION",
    false, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_ASYNC_INDEX_INC_MERGE, ObDagPrio::DAG_PRIO_ASYNC_INDEX_MERGE_MID, ObSysTaskType::ASYNC_INDEX_INC_MERGE_TASK, "ASYNC_INDEX_INC_MERGE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_ASYNC_INDEX_MAJOR_MERGE, ObDagPrio::DAG_PRIO_ASYNC_INDEX_MERGE_LOW, ObSysTaskType::ASYNC_INDEX_MAJOR_MERGE_TASK, "ASYNC_INDEX_MAJOR_MERGE", "COMPACTION",
    true, 3, {"ls_id", "tablet_id", "compaction_scn"})
/*
 * NOTICE: if you add/delete a compaction dag type here, remember to alter function is_compaction_dag and get_diagnose_tablet_type in ob_tenant_dag_scheduler.h
 * AND update check_ls_compaction_dag_exist_with_cancel
*/

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DDL, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "DDL_COMPLEMENT", "DDL",
    true, 7, {"ls_id", "source_tablet_id", "dest_tablet_id", "data_table_id", "target_table_id", "schema_version", "snapshot_version"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_UNIQUE_CHECKING, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "UNIQUE_CHECK", "DDL",
    true, 2, {"tablet_id", "index_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SQL_BUILD_INDEX, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "SQL_BUILD_INDEX", "DDL",
    true, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DDL_KV_MERGE, ObDagPrio::DAG_PRIO_DDL_HIGH, ObSysTaskType::DDL_KV_MERGE_TASK, "DDL_KV_MERGE", "DDL",
    true, 3, {"ls_id", "tablet_id", "rec_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DDL_DEL_LOB_META, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "DDL_DEL_LOB_META", "DROP_VEC_INDEX",
    true, 7, {"ls_id", "source_tablet_id", "dest_tablet_id", "data_table_id", "target_table_id", "schema_version", "snapshot_version"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_SPLIT, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TABLET_SPLIT, "DDL_TABLET_SPLIT", "DDL",
    true, 2, {"ls_id", "source_tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_LOB_SPLIT, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TABLET_SPLIT, "DDL_TABLET_SPLIT", "DDL",
    true, 2, {"ls_id", "source_tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_DDL_INDEPENDENT, ObDagPrio::DAG_PRIO_DDL, ObSysTaskType::DDL_TASK, "DDL_INDEPENDENT", "DDL",
    true, 7, {"ls_id", "source_tablet_id", "dest_tablet_id", "data_table_id", "target_table_id", "schema_version", "snapshot_version"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FORK_DATABASE_PREPARE, ObDagPrio::DAG_PRIO_FORK_DATABASE,
    ObSysTaskType::DDL_TASK, "FORK_DATABASE_PREPARE", "FORK_DATABASE", true, 5,
    {"tenant_id", "parent_task_id", "worker_count", "finished_table_count", "snapshot_version"})

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_GC_EXEC, ObDagPrio::DAG_PRIO_SS_GC_EXEC, ObSysTaskType::SS_GC_EXEC_TASK, "SS_GC_EXEC", "SS_GC",
    false, 2, {"tenant_id", "batch_row_count"})


// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MIGRATE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "MIGRATE", "MIGRATE")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_COMPLETE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "INITIAL_COMPLETE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MIGRATE_WARMUP, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "MIGRATE_WARMUP", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VECTOR_INDEX_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "VECTOR_INDEX_MIGRATION", "MIGRATE",
    true, 4, {"tenant_id", "ls_id", "task_id", "adaptor_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VECTOR_INDEX_ADAPTOR_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "VECTOR_INDEX_ADAPTOR_MIGRATION", "MIGRATE",
    true, 4, {"tenant_id", "ls_id", "data_tablet_id", "task_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VECTOR_INDEX_SERIALIZE, ObDagPrio::DAG_PRIO_VECTOR_INDEX, ObSysTaskType::MIGRATION_TASK, "VECTOR_INDEX_SERIALIZE", "MIGRATE",
    true, 4, {"tenant_id", "ls_id", "data_tablet_id", "processor_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_WAIT_DATA_READY, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "WAIT_DATA_READY", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_COMPLETE_MIGRATION, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "FINISH_COMPLETE_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
#ifdef OB_BUILD_SHARED_STORAGE
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_INITIAL_MIGRATION, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::MIGRATION_TASK, "SS_INITIAL_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_START_MIGRATION, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::MIGRATION_TASK, "SS_START_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_FINISH_MIGRATION, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::MIGRATION_TASK, "SS_FINISH_MIGRATION", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_CREATE_TABLETS_CONSUMER, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::MIGRATION_TASK, "SS_CREATE_TABLETS_CONSUMER", "MIGRATE",
    true, 3, {"tenant_id", "ls_id", "op_type"})
#endif
// DAG_TYPE_MIGRATE END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FAST_MIGRATE, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::MIGRATION_TASK, "FAST_MIGRATE", "MIGRATE",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VALIDATE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::MIGRATION_TASK, "VALIDATE", "MIGRATE",
    false, 0, {})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::BACKFILL_TX_TASK, "BACKFILL_TX", "BACKFILL_TX")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TABLET_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::BACKFILL_TX_TASK, "TABLET_BACKFILL_TX", "BACKFILL_TX",
    true, 2, {"ls_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::BACKFILL_TX_TASK, "FINISH_BACKFILL_TX", "BACKFILL_TX",
    true, 1, {"ls_id"})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP", "BACKUP")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_META, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_META", "BACKUP",
    false, 1, {"ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_PREPARE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_PREPARE", "BACKUP",
    false, 5, {"tenant_id", "backup_set_id", "ls_id", "turn_id", "retry_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_FINISH, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_FINISH", "BACKUP",
    false, 3, {"tenant_id", "backup_set_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_DATA, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_DATA", "BACKUP",
    false, 7, {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id", "task_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_PREFETCH_BACKUP_INFO, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "PREFETCH_BACKUP_INFO", "BACKUP",
    false, 7, {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id", "task_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_INDEX_REBUILD, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_INDEX_REBUILD", "BACKUP",
    false, 6, {"tenant_id", "backup_set_id", "backup_data_type", "ls_id", "turn_id", "retry_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_LS_LOG_GROUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_COMPLEMENT_LOG_LS_GROUP", "BACKUP",
    false, 3, {"tenant_id", "backup_set_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_LS_LOG_GROUP_FINISH, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_COMPLEMENT_LOG_LS_GROUP", "BACKUP",
    false, 3, {"tenant_id", "backup_set_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_LS_LOG, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_COMPLEMENT_LOG_LS", "BACKUP",
    false, 3, {"tenant_id", "backup_set_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_BACKUP_FUSE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "INITIAL_BACKUP_FUSE", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_BACKUP_FUSE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "START_BACKUP_FUSE", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_TABLET_FUSE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_TABLET_FUSE", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_BACKUP_FUSE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "FINISH_BACKUP_FUSE", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_BACKUP_META, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_SS_META", "BACKUP",
    true, 2, {"tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_INITIAL_LS_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "INITIAL_LS_BACKUP", "BACKUP",
    true, 2, {"tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_START_LS_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "START_LS_BACKUP", "BACKUP",
    true, 2, {"tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_BACKUP_SYS_TABLETS, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_SYS_TABLETS", "BACKUP",
    true, 2, {"tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_BACKUP_DATA_TABLETS, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "BACKUP_DATA_TABLETS", "BACKUP",
    true, 2, {"tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_GROUP_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "TABLET_GROUP_BACKUP", "BACKUP",
    true, 3, {"tenant_id", "ls_id", "first_tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "TABLET_BACKUP", "BACKUP",
    true, 3, {"tenant_id", "ls_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_FINISH_LS_BACKUP, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "FINISH_LS_BACKUP", "BACKUP",
    true, 2, {"tenant_id", "ls_id"})
// DAG_TYPE_BACKUP END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_BACKUPSET, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_BACKUPSET_TASK, "BACKUP_BACKUPSET", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_ARCHIVELOG, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_ARCHIVELOG_TASK, "BACKUP_ARCHIVELOG", "BACKUP",
    false, 0, {})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_VALIDATE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_VALIDATE_TASK, "BACKUP_VALIDATE", "BACKUP_VALIDATE",
    true, 5, {"tenant_id", "task_id", "validate_id", "ls_id", "validate_dag_type"})

#ifdef OB_BUILD_SHARED_STORAGE
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INITIAL_MACRO_COPY, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::SS_HA_MACRO_COPY_TASK, "INITIAL_MACRO_COPY", "SSHA_MC",
    true, 4, {"parent_task_id", "ls_id", "task_id", "task_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_MACRO_COPY, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::SS_HA_MACRO_COPY_TASK, "START_MACRO_COPY", "SSHA_MC",
    true, 4, {"parent_task_id", "ls_id", "task_id", "task_type"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_FINISH_MACRO_COPY, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::SS_HA_MACRO_COPY_TASK, "FINISH_MACRO_COPY", "SSHA_MC",
    true, 4, {"parent_task_id", "ls_id", "task_id", "task_type"})
#endif
    // DAG_TYPE_RESTORE END

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_BACKUP_CLEAN, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_CLEAN_TASK, "BACKUP_CLEAN", "BACKUP_CLEAN",
    true, 4, {"tenant_id", "task_id", "ls_id", "id"})

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_REMOVE_MEMBER, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::REMOVE_MEMBER_TASK, "REMOVE_MEMBER", "REMOVE_MEMBER",
    true, 1, {"ls_id"})

// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER", "TRANSFER")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER_BACKFILL_TX", "TRANSFER",
    true, 3, {"tenant_id", "src_ls_id", "start_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER_REPLACE_TABLE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER_REPLACE_TABLE", "TRANSFER",
    true, 2, {"tenant_id", "desc_ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TRANSFER_BUILD_TABLET_INFO, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "TRANSFER_BUILD_TABLET_INFO", "TRANSFER",
    false, 0, {})
// DAG_TYPE_TRANSFER END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TTL, ObDagPrio::DAG_PRIO_TTL, ObSysTaskType::TABLE_API_TTL_TASK, "TTL_DELTE_DAG", "TTL",
    false, 4, {"tenant_id", "ls_id", "table_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TENANT_SNAPSHOT_CREATE, ObDagPrio::DAG_PRIO_HA_MID, ObSysTaskType::TENANT_SNAPSHOT_CREATE_TASK, "TENANT_SNAPSHOT_CREATE", "TSNAP_CR8",
    false, 1, {"tsnap_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_TENANT_SNAPSHOT_GC, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::TENANT_SNAPSHOT_GC_TASK, "TENANT_SNAPSHOT_GC","TSNAP_GC",
    false, 1, {"tsnap_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_VECTOR_INDEX, ObDagPrio::DAG_PRIO_TTL, ObSysTaskType::VECTOR_INDEX_TASK, "VECTOR_INDEX_DAG", "VECTOR_INDEX",
    false, 4, {"tenant_id", "ls_id", "table_id", "tablet_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_LTABLE_BG_DELETE, ObDagPrio::DAG_PRIO_LTABLE_BG, ObSysTaskType::LTABLE_BG_DELETE_TASK, "LTABLE_BG_DELETE_DAG", "LTABLE_BG",
    false, 4, {"tenant_id", "collection_id", "namespace_id", "ltable_id"})

#ifdef OB_BUILD_SHARED_STORAGE
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INC_SSTABLE_UPLOAD, ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD, ObSysTaskType::INC_SSTABLE_UPLOAD_TASK, "INC_SSTABLE_UPLOAD","INC_UPLOAD",
    false, 7,
    {"ls_id", "tablet_id", "table_type", "start_scn",
     "end_scn", "transfer_scn", "reason"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_LINK_SHARED_SSTABLE, ObDagPrio::DAG_PRIO_LINK_SHARED_SSTABLE, ObSysTaskType::LINK_SHARED_SSTABLE_TASK, "LINK_SHARED_SSTABLE", "LINK_SHARED_SSTABLE",
    false, 3, {"ls_id", "tablet_id", "version"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_LINK_MAJOR_SSTABLE, ObDagPrio::DAG_PRIO_LINK_SHARED_SSTABLE, ObSysTaskType::LINK_MAJOR_SSTABLE_TASK, "LINK_MAJOR_SSTABLE", "LINK_MAJOR_SSTABLE",
    false, 3, {"ls_id", "tablet_id", "merge_version"})
// DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TRANSFER, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TRANSFER", "TRANSFER")
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_START_TRANSFER_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_START_TRANSFER_BACKFILL_TX", "TRANSFER",
    true, 3, {"backfill_job_type", "tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_FINISH_TRANSFER_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_FINISH_TRANSFER_BACKFILL_TX", "TRANSFER",
    true, 3, {"backfill_job_type", "tenant_id", "ls_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_BACKFILL_SCHEDULE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TABLET_BACKFILL_TX_SCHEDULE", "TRANSFER",
    true, 3, {"ls_id", "tablet_id", "backfill_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_START_MACRO_DELETE, ObDagPrio::DAG_PRIO_HA_LOW, ObSysTaskType::BACKUP_TASK, "START_MACRO_DELETE", "BACKUP_CLEAN",
    true, 3, {"tenant_id", "ls_id", "task_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_BACKFILL_UPLOAD, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TABLET_BACKFILL_UPLOAD", "TRANSFER",
    true, 3, {"ls_id", "tablet_id", "backfill_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_BACKFILL_TX, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TABLET_BACKFILL_TX", "TRANSFER",
    true, 3, {"ls_id", "tablet_id", "backfill_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_REPLACE_TABLE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TABLET_REPLACE_TABLE", "TRANSFER",
    true, 3, {"ls_id", "tablet_id", "backfill_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_REFRESH_TABLE, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TABLET_REFRESH_TABLE", "TRANSFER",
    true, 3, {"ls_id", "tablet_id", "backfill_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_TABLET_UPDATE_INFO, ObDagPrio::DAG_PRIO_HA_HIGH, ObSysTaskType::TRANSFER_TASK, "SS_TABLET_UPDATE_INFO", "TRANSFER",
    true, 3, {"ls_id", "tablet_id", "backfill_scn"})
// DAG_TYPE_SS_TRANSFER END

DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MARK_SWEEP, ObDagPrio::DAG_PRIO_MARK_SWEEP, ObSysTaskType::MARK_SWEEP_TASK, "MARK_SWEEP", "SS_GC",
    true, 1, {"tenant_id"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_LS_CKPT, ObDagPrio::DAG_PRIO_LS_CKPT,
    ObSysTaskType::LS_CKPT_TASK, "SS_LS_CKPT", "SS_META",
    true, 1, {"mode"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INC_REPLACE_UPLOAD_PREPARE, ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD, ObSysTaskType::INC_SSTABLE_UPLOAD_TASK, "INC_REPLACE_UPLOAD_PREPARE", "INC_REPLACE_PREPARE",
    false, 3, {"ls_id", "tablet_id", "transfer_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INC_REPLACE_UPLOAD, ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD, ObSysTaskType::INC_SSTABLE_UPLOAD_TASK, "INC_REPLACE_UPLOAD", "INC_REPLACE_UPLOAD",
    false, 3, {"ls_id", "tablet_id", "transfer_scn"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_INC_REPLACE_UPLOAD_COMPLETE, ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD, ObSysTaskType::INC_SSTABLE_UPLOAD_TASK, "INC_REPLACE_UPLOAD_COMPLETE", "INC_REPLACE_COMPLETE",
    false, 3, {"ls_id", "tablet_id", "transfer_scn"})
// DAG_TYPE_INC_REPLACE_UPLOAD END
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_FAIL_GC, ObDagPrio::DAG_PRIO_SS_GC_GEN, ObSysTaskType::SS_FAIL_GC_TASK, "SS_FAIL_GC", "SS_GC",
    true, 5, {"tenant_id", "server_id", "ls_count", "first_error_code", "failed_ls_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_LOCK_ROW_GC, ObDagPrio::DAG_PRIO_SS_GC_GEN, ObSysTaskType::SS_LOCK_ROW_GC_TASK, "SS_LOCK_ROW_GC", "SS_GC",
    true, 5, {"tenant_id", "server_id", "ls_count", "first_error_code", "failed_ls_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_SS_LS_META_GC, ObDagPrio::DAG_PRIO_SS_GC_GEN, ObSysTaskType::SS_LS_META_GC_TASK, "SS_LS_META_GC", "SS_GC",
    true, 5, {"tenant_id", "server_id", "ls_count", "first_error_code", "failed_ls_count"})
DAG_SCHEDULER_DAG_TYPE_DEF(
    DAG_TYPE_SS_DATA_CHECK,
    ObDagPrio::DAG_PRIO_SS_DATA_CHECK,
    ObSysTaskType::SS_DATA_CHECK_TASK,
    "SS_DATA_CHECK", "SS_DATA_CHECK",
    true, 2, {"tenant_id", "first_error_code"})

#endif
DAG_SCHEDULER_DAG_TYPE_DEF(DAG_TYPE_MAX, ObDagPrio::DAG_PRIO_MAX, ObSysTaskType::MAX_SYS_TASK_TYPE, "DAG_TYPE_MAX", "INVALID", false, 0, {})
#endif

#ifdef DIAGNOSE_INFO_PRIORITY_DEF
DIAGNOSE_INFO_PRIORITY_DEF(DIAGNOSE_PRIORITY_LOW)
DIAGNOSE_INFO_PRIORITY_DEF(DIAGNOSE_PRIORITY_MID)
DIAGNOSE_INFO_PRIORITY_DEF(DIAGNOSE_PRIORITY_HIGH)
#endif

#ifndef SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_CONFIG_H_
#define SRC_SHARE_SCHEDULER_OB_DAG_SCHEDULER_CONFIG_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_sys_task_stat.h"
#include "share/resource_manager/ob_resource_plan_info.h"

namespace oceanbase
{
namespace share
{

enum class ObDiagnoseInfoPrio : uint32_t
{
#define DIAGNOSE_INFO_PRIORITY_DEF(diagnose_info_prio) diagnose_info_prio,
#include "ob_dag_scheduler_config.h"
#undef DIAGNOSE_INFO_PRIORITY_DEF
};

struct ObDagPrioStruct
{
  int64_t score_;
  const char *dag_prio_str_;
  const ObFunctionType function_type_;
  TO_STRING_KV(K_(score), K_(dag_prio_str));
};

struct ObDagPrio
{
  enum ObDagPrioEnum
  {
#define DAG_SCHEDULER_DAG_PRIO_DEF(dag_prio, score, dag_prio_str, dag_function_type) dag_prio,
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_PRIO_DEF
  };
};

static constexpr ObDagPrioStruct OB_DAG_PRIOS[] = {
#define DAG_SCHEDULER_DAG_PRIO_DEF(dag_prio, score, dag_prio_str, dag_function_type) \
    {score, dag_prio_str, ObFunctionType::dag_function_type},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_PRIO_DEF
};

struct ObDagNetTypeStruct
{
  const char *dag_net_type_str_;
  TO_STRING_KV(K_(dag_net_type_str));
};

struct ObDagNetType
{
  enum ObDagNetTypeEnum
  {
#define DAG_SCHEDULER_DAG_NET_TYPE_DEF(dag_net_type, dag_net_type_str) dag_net_type,
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_NET_TYPE_DEF
  };
};

static constexpr ObDagNetTypeStruct OB_DAG_NET_TYPES[] = {
#define DAG_SCHEDULER_DAG_NET_TYPE_DEF(dag_net_type, dag_net_type_str) \
    {dag_net_type_str},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_NET_TYPE_DEF
};

struct ObDagTypeStruct
{
  ObDagPrio::ObDagPrioEnum init_dag_prio_;
  ObSysTaskType sys_task_type_;
  const char *dag_type_str_;
  const char *dag_module_str_;
  TO_STRING_KV(K_(init_dag_prio), K_(sys_task_type), K_(dag_type_str), K_(dag_module_str));
};

struct ObDagType
{
  enum ObDagTypeEnum : uint8_t
  {
#define DAG_SCHEDULER_DAG_TYPE_DEF(dag_type, init_dag_prio, sys_task_type, dag_type_str, dag_module_str, diagnose_with_comment, diagnose_priority, diagnose_int_info_cnt, ...) dag_type,
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_TYPE_DEF
  };
};

static constexpr ObDagTypeStruct OB_DAG_TYPES[] = {
#define DAG_SCHEDULER_DAG_TYPE_DEF(dag_type, init_dag_prio, sys_task_type, dag_type_str, dag_module_str, diagnose_with_comment, diagnose_priority, diagnose_int_info_cnt, ...) \
    {init_dag_prio, sys_task_type, dag_type_str, dag_module_str},
#include "ob_dag_scheduler_config.h"
#undef DAG_SCHEDULER_DAG_TYPE_DEF
};

inline bool is_compaction_dag(ObDagType::ObDagTypeEnum dag_type)
{
  return ObDagType::DAG_TYPE_CO_MERGE_PREPARE == dag_type ||
         ObDagType::DAG_TYPE_CO_MERGE_SCHEDULE == dag_type ||
         ObDagType::DAG_TYPE_CO_MERGE_EXECUTE == dag_type ||
         ObDagType::DAG_TYPE_MAJOR_MERGE == dag_type ||
         ObDagType::DAG_TYPE_MINI_MERGE == dag_type ||
         ObDagType::DAG_TYPE_MERGE_EXECUTE == dag_type ||
         ObDagType::DAG_TYPE_TX_TABLE_MERGE == dag_type ||
         ObDagType::DAG_TYPE_ASYNC_INDEX_MAJOR_MERGE == dag_type ||
         ObDagType::DAG_TYPE_ASYNC_INDEX_INC_MERGE == dag_type;
}

inline bool is_batch_exec_dag(ObDagType::ObDagTypeEnum dag_type)
{
  return ObDagType::DAG_TYPE_VERIFY_CKM == dag_type
      || ObDagType::DAG_TYPE_BATCH_FREEZE_TABLETS == dag_type
      || ObDagType::DAG_TYPE_UPDATE_SKIP_MAJOR == dag_type;
}

inline bool is_diagnose_dag(ObDagType::ObDagTypeEnum dag_type)
{
  return is_compaction_dag(dag_type)
      || is_batch_exec_dag(dag_type);
}

inline bool is_valid_dag_priority(ObDagPrio::ObDagPrioEnum priority)
{
  return priority < ObDagPrio::DAG_PRIO_MAX && 
         priority >= ObDagPrio::DAG_PRIO_COMPACTION_HIGH;
}

inline bool is_ha_prio_dag(ObDagPrio::ObDagPrioEnum priority)
{
  return ObDagPrio::DAG_PRIO_HA_HIGH == priority ||
         ObDagPrio::DAG_PRIO_HA_MID == priority ||
         ObDagPrio::DAG_PRIO_HA_LOW == priority;
}
} // namespace share
} // namespace oceanbase
#endif
