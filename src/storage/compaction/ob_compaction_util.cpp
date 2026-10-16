/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/compaction/ob_compaction_util.h"
#include "share/ob_define.h"
#include "storage/tablet/ob_tablet.h"
namespace oceanbase
{
namespace compaction
{

const static char * ObMergeTypeStr[] = {
    "INVALID_MERGE_TYPE",
    "MINOR_MERGE",
    "HISTORY_MINOR_MERGE",
    "META_MAJOR_MERGE",
    "MINI_MERGE",
    "MAJOR_MERGE",
    "MEDIUM_MERGE",
    "DDL_KV_MERGE",
    "BACKFILL_TX_MERGE",
    "BATCH_EXEC",
    "CONVERT_CO_MAJOR_MERGE",
    "INC_MAJOR_MERGE",
    "EMPTY_MERGE_TYPE"
};

const char *merge_type_to_str(const ObMergeType &merge_type)
{
  STATIC_ASSERT(static_cast<int64_t>(MERGE_TYPE_MAX + 1) == ARRAYSIZEOF(ObMergeTypeStr), "merge type str len is mismatch");
  const char *str = "";
  if (is_valid_merge_type(merge_type)) {
    str = ObMergeTypeStr[merge_type];
  } else {
    str = "invalid_merge_type";
  }
  return str;
}

const static char * ObMergeLevelStr[] = {
    "MACRO_BLOCK_LEVEL",
    "MICRO_BLOCK_LEVEL"
};

const char *merge_level_to_str(const ObMergeLevel &merge_level)
{
  STATIC_ASSERT(static_cast<int64_t>(MERGE_LEVEL_MAX) == ARRAYSIZEOF(ObMergeLevelStr), "merge level str len is mismatch");
  const char *str = "";
  if (is_valid_merge_level(merge_level)) {
    str = ObMergeLevelStr[merge_level];
  } else {
    str = "invalid_merge_level";
  }
  return str;
}

const static char * ObExecModeStr[] = {
  "EXEC_MODE_LOCAL",
  "EXEC_MODE_OUTPUT",
  "EXEC_MODE_VALIDATE",
  "EXEC_MODE_LOCAL_WITH_SHARED_BLOCK",
  "EXEC_MODE_UPLOAD_MINOR",
};

const char *exec_mode_to_str(const ObExecMode &exec_mode)
{
  STATIC_ASSERT(static_cast<int64_t>(EXEC_MODE_MAX) == ARRAYSIZEOF(ObExecModeStr), "exec mode str len is mismatch");
  const char *str = "";
  if (is_valid_exec_mode(exec_mode)) {
    str = ObExecModeStr[exec_mode];
  } else {
    str = "invalid_exec_mode";
  }
  return str;
}

int check_minor_merge_input_sstables_exist(
    const storage::ObTablet &tablet,
    const storage::ObTablesHandleArray &input_tables_handle)
{
  int ret = OB_SUCCESS;
  storage::ObTabletMemberWrapper<storage::ObTabletTableStore> table_store_wrapper;
  const storage::ObTabletTableStore *table_store = nullptr;
  const common::ObTabletID &tablet_id = tablet.get_tablet_id();
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store for input tables check", KR(ret), K(tablet_id));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is nullptr", KR(ret), K(tablet_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_tables_handle.get_count(); ++i) {
    const storage::ObITable *table = input_tables_handle.get_table(i);
    storage::ObSSTableWrapper tmp_wrapper;
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input table is nullptr", KR(ret), K(i), K(tablet_id));
    } else if (!table->is_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input table is not sstable", KR(ret), K(i), K(tablet_id));
    } else if (OB_FAIL(table_store->get_sstable(table->get_key(), tmp_wrapper))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_NEED_RETRY;
        LOG_WARN("input sstable no longer exists in table-store, minor merge aborted",
                 KR(ret), K(tablet_id), K(table->get_key()));
      } else {
        LOG_WARN("fail to get sstable from table store", KR(ret), K(tablet_id), K(table->get_key()));
      }
    } else {
      const blocksstable::ObSSTable *input_sstable = static_cast<const blocksstable::ObSSTable *>(table);
      const blocksstable::ObSSTable *store_sstable = tmp_wrapper.get_sstable();
      if (OB_ISNULL(store_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("store sstable is nullptr", KR(ret), K(tablet_id), K(table->get_key()));
      } else if (input_sstable->get_filled_tx_scn() != store_sstable->get_filled_tx_scn()) {
        ret = OB_NEED_RETRY;
        LOG_WARN("input sstable meta filled_tx_scn mismatch with table-store, minor merge aborted",
                 KR(ret), K(tablet_id), K(table->get_key()),
                 K(input_sstable->get_filled_tx_scn()), K(store_sstable->get_filled_tx_scn()));
      }
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
