/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_util.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

namespace oceanbase
{
namespace storage
{

int ObSSMetaCtrlUtil::delete_rows(const uint64_t tenant_id, volatile bool &is_stopped)
{
  int ret = OB_SUCCESS;
  int64_t total_affected_rows = 0;
  int64_t sql_timeout_us = ObSSGCUtil::DEFAULT_SS_GC_SQL_TIMEOUT_US;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObSSMetaCtrlTableOperator table_operator;
  ObSSMetaCtrlRowkeyIterator rowkey_iterator;
  ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "SSMetaCtrlDel"));

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObSSGCUtil::get_shared_gc_sql_timeout_us(tenant_id, sql_timeout_us))) {
    LOG_WARN("failed to get shared gc sql timeout", KR(ret), K(tenant_id), K(sql_timeout_us));
  } else if (OB_FAIL(table_operator.init(*sql_proxy, sql_timeout_us))) {
    LOG_WARN("failed to init meta ctrl table operator", KR(ret), K(tenant_id), K(sql_timeout_us));
  } else if (OB_FAIL(rowkey_iterator.init(tenant_id, *sql_proxy, sql_timeout_us))) {
    LOG_WARN("failed to init rowkey iterator", KR(ret), K(tenant_id));
  } else {
    uint64_t row_tenant_id = OB_INVALID_ID;
    share::ObLSID row_ls_id;
    blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
    ObString meta_key;
    int64_t affected_rows = 0;
    bool is_finish = false;

    while (OB_SUCC(ret) && !is_finish) {
      allocator.reset();
      row_tenant_id = OB_INVALID_ID;
      row_ls_id.reset();
      meta_type = blocksstable::ObStorageObjectType::MAX;
      meta_key.reset();
      affected_rows = 0;
      if (is_stopped) {
        ret = OB_SERVICE_STOPPED;
        LOG_WARN("delete rows is stopped", KR(ret), K(tenant_id));
      } else if (OB_FAIL(rowkey_iterator.get_next(
          allocator, row_tenant_id, meta_type, row_ls_id, meta_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_finish = true;
        } else {
          LOG_WARN("failed to get next rowkey", KR(ret), K(tenant_id));
        }
      } else if (OB_FAIL(table_operator.delete_rows_from_rowkey(row_tenant_id,
                                                                meta_type,
                                                                row_ls_id,
                                                                meta_key,
                                                                DELETE_ROWS_BATCH_LIMIT,
                                                                affected_rows))) {
        LOG_WARN("failed to delete rows from rowkey", KR(ret), K(row_tenant_id), K(row_ls_id),
            K(meta_type), K(meta_key), K(total_affected_rows));
      } else if (0 == affected_rows) {
        is_finish = true;
      } else {
        total_affected_rows += affected_rows;
        if (OB_FAIL(skip_rowkey_iterator_after_delete_(
            tenant_id, affected_rows, is_stopped, rowkey_iterator))) {
          LOG_WARN("failed to skip rowkey iterator after delete batch", KR(ret), K(tenant_id),
              K(affected_rows));
        } else if (affected_rows < DELETE_ROWS_BATCH_LIMIT) {
          // Fewer than a full batch means no rows remain at or after this rowkey.
          is_finish = true;
        }
      }
    }
  }

  LOG_INFO("finish delete_rows", KR(ret), K(tenant_id), K(is_stopped), K(total_affected_rows));
  return ret;
}

int ObSSMetaCtrlUtil::skip_rowkey_iterator_after_delete_(
    const uint64_t tenant_id,
    const int64_t affected_rows,
    volatile bool &is_stopped,
    ObSSMetaCtrlRowkeyIterator &rowkey_iterator)
{
  int ret = OB_SUCCESS;
  // get_next() already consumed the first row of this batch; skip the rest
  // deleted by delete_rows_from_rowkey so the iterator stays aligned.
  for (int64_t i = 1; OB_SUCC(ret) && i < affected_rows; ++i) {
    if (is_stopped) {
      ret = OB_SERVICE_STOPPED;
      LOG_WARN("delete rows is stopped when skip rowkey iterator", KR(ret),
          K(tenant_id), K(i), K(affected_rows));
      break;
    } else if (OB_FAIL(rowkey_iterator.get_next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to skip rowkey iterator after delete batch", KR(ret),
            K(tenant_id), K(i), K(affected_rows));
      }
    }
  }
  return ret;
}

} // storage
} // oceanbase
