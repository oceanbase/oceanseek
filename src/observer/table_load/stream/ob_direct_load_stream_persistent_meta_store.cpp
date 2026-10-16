/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/stream/ob_direct_load_stream_persistent_meta_store.h"

#include "lib/allocator/page_arena.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::sqlclient;
using namespace share;

namespace
{

ERRSIM_POINT_DEF(ERRSIM_DIRECT_LOAD_STREAM_FINISH_IN_TRANS_FAIL);

const char *DIRECT_LOAD_STREAM_SELECT_COLUMNS =
    "time_to_usec(gmt_create) AS create_ts, "
    "time_to_usec(gmt_modified) AS modified_ts, "
    "tenant_id, stream_id, user_id, table_id, target_schema_version, status, "
    "queue_list, owner_server, session_id, session_create_ts, result_code, expire_ts";

bool is_same_queue_entries(
    const common::ObIArray<table::ObDirectLoadStreamQueueEntry> &lhs,
    const common::ObIArray<table::ObDirectLoadStreamQueueEntry> &rhs)
{
  bool same = lhs.count() == rhs.count();
  for (int64_t i = 0; same && i < lhs.count(); ++i) {
    same = lhs.at(i).server_addr_ == rhs.at(i).server_addr_
        && lhs.at(i).queue_count_ == rhs.at(i).queue_count_;
  }
  return same;
}

bool is_same_created_open_meta(const int64_t stream_id,
                               const ObDirectLoadStreamCreateParam &param,
                               const ObDirectLoadStreamMeta &meta)
{
  return meta.stream_id_ == stream_id
      && meta.tenant_id_ == param.tenant_id_
      && meta.user_id_ == param.user_id_
      && meta.table_id_ == param.table_id_
      && meta.target_schema_version_ == param.target_schema_version_
      && DIRECT_LOAD_STREAM_STATUS_OPEN == meta.status_
      && !meta.owner_server_.is_valid()
      && OB_INVALID_ID == meta.session_id_
      && 0 == meta.session_create_ts_
      && OB_SUCCESS == meta.result_code_
      && meta.expire_ts_ == param.expire_ts_
      && is_same_queue_entries(meta.queue_entries_, param.queue_entries_);
}

int copy_persistent_stream_meta(const ObDirectLoadStreamMeta &src,
                                ObDirectLoadStreamMeta &dest)
{
  int ret = OB_SUCCESS;
  dest.stream_id_ = src.stream_id_;
  dest.tenant_id_ = src.tenant_id_;
  dest.user_id_ = src.user_id_;
  dest.table_id_ = src.table_id_;
  dest.target_schema_version_ = src.target_schema_version_;
  dest.status_ = src.status_;
  dest.owner_server_ = src.owner_server_;
  dest.session_id_ = src.session_id_;
  dest.session_create_ts_ = src.session_create_ts_;
  dest.result_code_ = src.result_code_;
  dest.create_ts_ = src.create_ts_;
  dest.modified_ts_ = src.modified_ts_;
  dest.expire_ts_ = src.expire_ts_;
  dest.queue_entries_.reset();
  dest.queue_entries_.set_attr(ObMemAttr(src.tenant_id_, "DLStrQueue"));
  if (OB_FAIL(dest.queue_entries_.assign(src.queue_entries_))) {
    LOG_WARN("failed to copy persistent direct load stream queue entries", KR(ret), K(src));
  }
  return ret;
}

int extract_stream_meta(ObMySQLResult &result,
                        const uint64_t tenant_id,
                        ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  int64_t physical_tenant_id = 0;
  int64_t status = DIRECT_LOAD_STREAM_STATUS_MAX;
  ObString queue_list;
  ObString owner_server;
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "DLStrMetaRead"));
  ObDirectLoadStreamMeta tmp_meta;
  tmp_meta.queue_entries_.set_attr(ObMemAttr(tenant_id, "DLStrQueue"));

  EXTRACT_INT_FIELD_MYSQL(result, "create_ts", tmp_meta.create_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "modified_ts", tmp_meta.modified_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", physical_tenant_id, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "stream_id", tmp_meta.stream_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "user_id", tmp_meta.user_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "table_id", tmp_meta.table_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "target_schema_version",
                          tmp_meta.target_schema_version_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "status", status, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "queue_list", queue_list);
  EXTRACT_VARCHAR_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "owner_server", owner_server, true, false, "");
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "session_id", tmp_meta.session_id_, uint64_t,
      true, false, OB_INVALID_ID);
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "session_create_ts", tmp_meta.session_create_ts_, int64_t,
      true, false, 0);
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(
      result, "result_code", tmp_meta.result_code_, int,
      true, false, OB_SUCCESS);
  EXTRACT_INT_FIELD_MYSQL(result, "expire_ts", tmp_meta.expire_ts_, int64_t);

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to extract persistent direct load stream metadata", KR(ret));
  } else if (OB_UNLIKELY(physical_tenant_id != static_cast<int64_t>(
                 share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected physical tenant id in direct load stream metadata",
             KR(ret), K(physical_tenant_id), K(tenant_id));
  } else if (OB_UNLIKELY(status < DIRECT_LOAD_STREAM_STATUS_OPEN
                         || status >= DIRECT_LOAD_STREAM_STATUS_MAX)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid persistent direct load stream status", KR(ret), K(status));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::decode(
                 queue_list, allocator, tmp_meta.queue_entries_))) {
    LOG_WARN("failed to decode persistent direct load stream queue list", KR(ret), K(queue_list));
  } else if (!owner_server.empty()
             && OB_FAIL(tmp_meta.owner_server_.parse_from_string(owner_server))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("failed to parse persistent direct load stream owner", KR(ret), K(owner_server));
  } else {
    tmp_meta.tenant_id_ = tenant_id;
    tmp_meta.status_ = static_cast<ObDirectLoadStreamStatus>(status);
    if (OB_FAIL(copy_persistent_stream_meta(tmp_meta, meta))) {
      LOG_WARN("failed to copy extracted persistent direct load stream metadata", KR(ret));
    }
  }
  return ret;
}

int resolve_single_row_write(const int64_t affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_single_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows for direct load stream metadata write",
             KR(ret), K(affected_rows));
  }
  return ret;
}

} // namespace

ObDirectLoadStreamPersistentMetaStore::ObDirectLoadStreamPersistentMetaStore(
    const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    sql_client_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadStreamPersistentMetaStore::~ObDirectLoadStreamPersistentMetaStore()
{
  reset();
}

int ObDirectLoadStreamPersistentMetaStore::init(common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    sql_client_ = &sql_client;
    is_inited_ = true;
  }
  return ret;
}

void ObDirectLoadStreamPersistentMetaStore::reset()
{
  is_inited_ = false;
  sql_client_ = nullptr;
}

uint64_t ObDirectLoadStreamPersistentMetaStore::get_sql_tenant_id_() const
{
  return share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_);
}

int ObDirectLoadStreamPersistentMetaStore::create_stream(
    const int64_t stream_id,
    const ObDirectLoadStreamCreateParam &param,
    ObDirectLoadStreamMetaCreateOutcome &outcome)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObString queue_list;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "DLStrMetaSQL"));
  outcome = DIRECT_LOAD_STREAM_META_NOT_CREATED;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || param.tenant_id_ != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(check_direct_load_stream_create_param(param))) {
    LOG_WARN("invalid persistent direct load stream create param", KR(ret), K(stream_id), K(param));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::encode(
                 param.queue_entries_, allocator, queue_list))) {
    LOG_WARN("failed to encode persistent direct load stream queue list", KR(ret), K(param));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", stream_id))
             || OB_FAIL(dml.add_column("user_id", param.user_id_))
             || OB_FAIL(dml.add_column("table_id", param.table_id_))
             || OB_FAIL(dml.add_column("target_schema_version", param.target_schema_version_))
             || OB_FAIL(dml.add_column("status", DIRECT_LOAD_STREAM_STATUS_OPEN))
             || OB_FAIL(dml.add_column("queue_list", ObHexEscapeSqlStr(queue_list)))
             || OB_FAIL(dml.add_column("expire_ts", param.expire_ts_))) {
    LOG_WARN("failed to build persistent direct load stream insert columns", KR(ret), K(param));
  } else if (OB_FAIL(dml.splice_insert_sql(share::OB_ALL_DIRECT_LOAD_STREAM_TNAME, sql))) {
    LOG_WARN("failed to build persistent direct load stream insert sql", KR(ret));
  } else {
    ret = sql_client_->write(tenant_id_, sql.ptr(), affected_rows);
    if (OB_SUCCESS == ret && is_single_row(affected_rows)) {
      outcome = DIRECT_LOAD_STREAM_META_CREATED;
    } else {
      const int write_ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
      if (OB_SUCCESS == ret) {
        LOG_WARN("unexpected affected rows for persistent direct load stream insert",
                 KR(write_ret), K(affected_rows), K(stream_id));
      } else {
        LOG_WARN("persistent direct load stream insert failed",
                 KR(write_ret), K(affected_rows), K(stream_id));
      }
      ret = resolve_create_write_result_(stream_id, param, write_ret, outcome);
    }
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::resolve_create_write_result_(
    const int64_t stream_id,
    const ObDirectLoadStreamCreateParam &param,
    const int write_ret,
    ObDirectLoadStreamMetaCreateOutcome &outcome)
{
  int ret = write_ret;
  ObDirectLoadStreamMeta meta;
  const int read_ret = get_stream(stream_id, meta);
  outcome = DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN;
  if (OB_SUCCESS == read_ret && is_same_created_open_meta(stream_id, param, meta)) {
    ret = OB_SUCCESS;
    outcome = DIRECT_LOAD_STREAM_META_CREATED;
    LOG_INFO("resolved persistent direct load stream create by matching metadata row",
             K(stream_id), K(write_ret));
  } else {
    // write() 已经发起后，任何错误都不能证明 INSERT 未提交。
    LOG_WARN("persistent direct load stream create result remains unknown",
             KR(write_ret), KR(read_ret), K(stream_id), K(meta));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::get_stream(
    const int64_t stream_id, ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT %s FROM %s WHERE tenant_id = %lu AND stream_id = %ld",
                 DIRECT_LOAD_STREAM_SELECT_COLUMNS,
                 share::OB_ALL_DIRECT_LOAD_STREAM_TNAME,
                 get_sql_tenant_id_(), stream_id))) {
    LOG_WARN("failed to build persistent direct load stream select sql", KR(ret), K(stream_id));
  } else if (OB_FAIL(read_one_stream_(sql, stream_id, meta))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to read persistent direct load stream metadata", KR(ret), K(stream_id));
    }
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::read_one_stream_(
    const ObSqlString &sql, const int64_t stream_id,
    ObDirectLoadStreamMeta &meta)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObISQLClient::ReadResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql_client_->read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("failed to execute persistent direct load stream read",
               KR(ret), K_(tenant_id), K(stream_id), "operation", "get_stream");
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ITER_END == (ret = result->next())) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to advance persistent direct load stream result",
               KR(ret), K_(tenant_id), K(stream_id), "operation", "get_stream");
    } else if (OB_FAIL(extract_stream_meta(*result, tenant_id_, meta))) {
      LOG_WARN("failed to extract persistent direct load stream row",
               KR(ret), K_(tenant_id), K(stream_id), "operation", "get_stream");
    } else {
      const int next_ret = result->next();
      if (OB_ITER_END != next_ret) {
        ret = OB_SUCCESS == next_ret ? OB_ERR_UNEXPECTED : next_ret;
        LOG_WARN("unexpected duplicate persistent direct load stream rows",
                 KR(ret), K_(tenant_id), K(stream_id), "operation", "get_stream");
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::scan_streams(
    const ObDirectLoadStreamScanParam &param,
    common::ObIArray<ObDirectLoadStreamMeta> &metas)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  metas.reset();
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_direct_load_stream_scan_param(param))) {
    LOG_WARN("invalid persistent direct load stream scan param", KR(ret), K(param));
  } else if (DIRECT_LOAD_STREAM_STATUS_FINISHED == param.status_) {
    if (OB_FAIL(sql.assign_fmt(
            "SELECT %s FROM %s WHERE tenant_id = %lu AND status = %ld "
            "AND gmt_modified <= usec_to_time(%ld) AND stream_id > %ld "
            "ORDER BY stream_id LIMIT %ld",
            DIRECT_LOAD_STREAM_SELECT_COLUMNS,
            share::OB_ALL_DIRECT_LOAD_STREAM_TNAME,
            get_sql_tenant_id_(), static_cast<int64_t>(param.status_),
            param.upper_bound_ts_, param.start_after_stream_id_, param.limit_))) {
      LOG_WARN("failed to build finished direct load stream scan sql", KR(ret), K(param));
    }
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT %s FROM %s WHERE tenant_id = %lu AND status = %ld "
                 "AND expire_ts <= %ld AND stream_id > %ld ORDER BY stream_id LIMIT %ld",
                 DIRECT_LOAD_STREAM_SELECT_COLUMNS,
                 share::OB_ALL_DIRECT_LOAD_STREAM_TNAME,
                 get_sql_tenant_id_(), static_cast<int64_t>(param.status_),
                 param.upper_bound_ts_, param.start_after_stream_id_, param.limit_))) {
    LOG_WARN("failed to build expired direct load stream scan sql", KR(ret), K(param));
  }
  if (OB_SUCC(ret) && OB_FAIL(read_streams_(sql, metas))) {
    LOG_WARN("failed to scan persistent direct load stream metadata", KR(ret), K(param));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::scan_gc_streams(
    const int64_t now,
    common::ObIArray<ObDirectLoadStreamMeta> &metas)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  metas.reset();
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT %s FROM %s WHERE tenant_id = %lu AND "
                 "((status IN (%ld, %ld) AND expire_ts < %ld "
                 "AND owner_server IS NULL AND session_id IS NULL "
                 "AND session_create_ts IS NULL) OR status = %ld) "
                 "ORDER BY stream_id",
                 DIRECT_LOAD_STREAM_SELECT_COLUMNS,
                 share::OB_ALL_DIRECT_LOAD_STREAM_TNAME,
                 get_sql_tenant_id_(),
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_OPEN),
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_SEALED),
                 now,
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_FINISHED)))) {
    LOG_WARN("failed to build direct load stream gc scan sql", KR(ret), K(now));
  } else if (OB_FAIL(read_streams_(sql, metas))) {
    LOG_WARN("failed to scan direct load stream gc candidates", KR(ret), K(now));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::finish_expired_unclaimed_stream(
    const int64_t stream_id, const int64_t now, bool &finished)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  finished = false;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || now <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", stream_id))
             || OB_FAIL(dml.add_column("status", DIRECT_LOAD_STREAM_STATUS_FINISHED))
             || OB_FAIL(dml.add_column("result_code", OB_TIMEOUT))
             || OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status IN (%ld, %ld) AND result_code IS NULL AND expire_ts < %ld "
                 "AND owner_server IS NULL AND session_id IS NULL "
                 "AND session_create_ts IS NULL",
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_OPEN),
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_SEALED), now))
             || OB_FAIL(dml.splice_update_sql(
                 share::OB_ALL_DIRECT_LOAD_STREAM_TNAME, sql))) {
    LOG_WARN("failed to build expired unclaimed stream finish sql",
             KR(ret), K(stream_id), K(now));
  } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to finish expired unclaimed direct load stream",
             KR(ret), K(stream_id), K(now));
  } else if (is_single_row(affected_rows)) {
    finished = true;
  } else if (!is_zero_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows when finishing expired unclaimed stream",
             KR(ret), K(stream_id), K(now), K(affected_rows));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::read_streams_(
    const ObSqlString &sql, common::ObIArray<ObDirectLoadStreamMeta> &metas)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObISQLClient::ReadResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql_client_->read(res, tenant_id_, sql.ptr()))) {
      LOG_WARN("failed to execute persistent direct load stream scan",
               KR(ret), K_(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      while (OB_SUCC(ret)) {
        ObDirectLoadStreamMeta meta;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(extract_stream_meta(*result, tenant_id_, meta))) {
          LOG_WARN("failed to extract scanned persistent direct load stream row", KR(ret));
        } else if (OB_FAIL(metas.push_back(meta))) {
          LOG_WARN("failed to append scanned persistent direct load stream row", KR(ret), K(meta));
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::seal_stream(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", stream_id))
             || OB_FAIL(dml.add_column("status", DIRECT_LOAD_STREAM_STATUS_SEALED))
             || OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status = %ld", static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_OPEN)))
             || OB_FAIL(dml.splice_update_sql(share::OB_ALL_DIRECT_LOAD_STREAM_TNAME, sql))) {
    LOG_WARN("failed to build persistent direct load stream seal sql", KR(ret), K(stream_id));
  } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to seal persistent direct load stream", KR(ret), K(stream_id));
  } else if (is_zero_row(affected_rows)) {
    ret = resolve_zero_affected_seal_(stream_id);
  } else if (OB_FAIL(resolve_single_row_write(affected_rows))) {
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::resolve_zero_affected_seal_(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  if (OB_FAIL(get_stream(stream_id, meta))) {
  } else if (DIRECT_LOAD_STREAM_STATUS_SEALED != meta.status_) {
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::claim_stream(
    const ObDirectLoadStreamClaimParam &param)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  char owner_buf[OB_IP_PORT_STR_BUFF] = "";
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(check_direct_load_stream_claim_param(param))) {
    LOG_WARN("invalid persistent direct load stream claim param", KR(ret), K(param));
  } else if (OB_FAIL(param.owner_server_.ip_port_to_string(owner_buf, sizeof(owner_buf)))) {
    LOG_WARN("failed to stringify persistent direct load stream owner", KR(ret), K(param));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", param.stream_id_))
             || OB_FAIL(dml.add_column(
                 "owner_server", ObHexEscapeSqlStr(ObString::make_string(owner_buf))))
             || OB_FAIL(dml.add_column("session_id", param.session_id_))
             || OB_FAIL(dml.add_column("session_create_ts", param.session_create_ts_))
             || OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status IN (%ld, %ld) AND owner_server IS NULL "
                 "AND session_id IS NULL AND session_create_ts IS NULL",
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_OPEN),
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_SEALED)))
             || OB_FAIL(dml.splice_update_sql(share::OB_ALL_DIRECT_LOAD_STREAM_TNAME, sql))) {
    LOG_WARN("failed to build persistent direct load stream claim sql", KR(ret), K(param));
  } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to claim persistent direct load stream", KR(ret), K(param));
  } else if (is_zero_row(affected_rows)) {
    ret = resolve_zero_affected_claim_(param.stream_id_);
  } else if (OB_FAIL(resolve_single_row_write(affected_rows))) {
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::resolve_zero_affected_claim_(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  if (OB_FAIL(get_stream(stream_id, meta))) {
  } else {
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::build_finish_stream_sql_(
    const int64_t stream_id, const int result_code, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString full_table_name;
  if (OB_UNLIKELY(!is_valid_stream_id(stream_id))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", stream_id))
             || OB_FAIL(dml.add_column("status", DIRECT_LOAD_STREAM_STATUS_FINISHED))
             || OB_FAIL(dml.add_column("result_code", result_code))) {
    LOG_WARN("failed to build persistent direct load stream finish columns",
             KR(ret), K(stream_id), K(result_code));
  } else if (OB_SUCCESS == result_code
             && OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status = %ld AND result_code IS NULL",
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_SEALED)))) {
    LOG_WARN("failed to build persistent direct load stream success finish condition", KR(ret));
  } else if (OB_SUCCESS != result_code
             && OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status IN (%ld, %ld) AND result_code IS NULL",
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_OPEN),
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_SEALED)))) {
    LOG_WARN("failed to build persistent direct load stream error finish condition", KR(ret));
  } else if (OB_FAIL(full_table_name.assign_fmt(
                 "%s.%s", OB_SYS_DATABASE_NAME, share::OB_ALL_DIRECT_LOAD_STREAM_TNAME))) {
    LOG_WARN("failed to build persistent direct load stream full table name", KR(ret));
  } else if (OB_FAIL(dml.splice_update_sql(full_table_name.ptr(), sql))) {
    LOG_WARN("failed to build persistent direct load stream finish sql", KR(ret));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::finish_stream(
    const int64_t stream_id, const int result_code)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(build_finish_stream_sql_(stream_id, result_code, sql))) {
  } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to finish persistent direct load stream", KR(ret), K(stream_id), K(result_code));
  } else if (is_zero_row(affected_rows)) {
    ret = resolve_zero_affected_finish_(stream_id);
  } else if (OB_FAIL(resolve_single_row_write(affected_rows))) {
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::finish_success_in_trans(
    const int64_t stream_id,
    common::sqlclient::ObISQLConnection &sql_connection)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id_))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ERRSIM_DIRECT_LOAD_STREAM_FINISH_IN_TRANS_FAIL)) {
    LOG_WARN("errsim failed transaction-bound direct load stream finish",
             KR(ret), K(stream_id));
  } else if (OB_FAIL(build_finish_stream_sql_(stream_id, OB_SUCCESS, sql))) {
  } else if (OB_FAIL(sql_connection.execute_write(
                 tenant_id_, sql.string(), affected_rows))) {
    LOG_WARN("failed to finish persistent direct load stream in transaction",
             KR(ret), K(stream_id));
  } else if (OB_FAIL(resolve_single_row_write(affected_rows))) {
    LOG_WARN("transaction-bound direct load stream finish did not update exactly one row",
             KR(ret), K(stream_id), K(affected_rows));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::finish_stream_if_open(
    const int64_t stream_id, const int result_code, bool &finished)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  finished = false;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || OB_SUCCESS == result_code)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", stream_id))
             || OB_FAIL(dml.add_column("status", DIRECT_LOAD_STREAM_STATUS_FINISHED))
             || OB_FAIL(dml.add_column("result_code", result_code))
             || OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status = %ld AND result_code IS NULL",
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_OPEN)))
             || OB_FAIL(dml.splice_update_sql(share::OB_ALL_DIRECT_LOAD_STREAM_TNAME, sql))) {
    LOG_WARN("failed to build persistent open stream finish sql",
             KR(ret), K(stream_id), K(result_code));
  } else {
    const int write_ret = sql_client_->write(tenant_id_, sql.ptr(), affected_rows);
    if (OB_SUCCESS != write_ret) {
      LOG_WARN("failed to finish persistent open direct load stream",
               KR(write_ret), K(stream_id), K(result_code));
      ret = resolve_finish_if_open_write_result_(
          stream_id, result_code, write_ret, finished);
    } else if (is_single_row(affected_rows)) {
      finished = true;
    } else {
      const int reconcile_ret = is_zero_row(affected_rows)
          ? OB_STATE_NOT_MATCH : OB_ERR_UNEXPECTED;
      if (!is_zero_row(affected_rows)) {
        LOG_WARN("unexpected affected rows when finishing persistent open stream",
                 KR(reconcile_ret), K(stream_id), K(result_code), K(affected_rows));
      }
      ret = resolve_finish_if_open_write_result_(
          stream_id, result_code, reconcile_ret, finished);
    }
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::resolve_finish_if_open_write_result_(
    const int64_t stream_id, const int result_code,
    const int write_ret, bool &finished)
{
  int ret = write_ret;
  ObDirectLoadStreamMeta meta;
  const int read_ret = get_stream(stream_id, meta);
  finished = false;
  if (OB_SUCCESS == read_ret
      && DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_
      && result_code == meta.result_code_) {
    ret = OB_SUCCESS;
    finished = true;
    LOG_INFO("resolved persistent open stream finish by matching metadata row",
             K(stream_id), K(result_code), K(write_ret));
  } else if (OB_SUCCESS == read_ret
             && (DIRECT_LOAD_STREAM_STATUS_SEALED == meta.status_
                 || DIRECT_LOAD_STREAM_STATUS_FINISHED == meta.status_)) {
    ret = OB_STATE_NOT_MATCH;
  } else {
    // write() 发起后仍为 OPEN 或点查失败时，
    // 保留原始错误表示提交结果不确定。
    LOG_WARN("persistent open stream finish result remains unknown",
             KR(write_ret), KR(read_ret), K(stream_id), K(result_code), K(meta));
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::resolve_zero_affected_finish_(const int64_t stream_id)
{
  int ret = OB_SUCCESS;
  ObDirectLoadStreamMeta meta;
  if (OB_FAIL(get_stream(stream_id, meta))) {
  } else {
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

int ObDirectLoadStreamPersistentMetaStore::delete_stream(
    const int64_t stream_id, const int64_t max_modified_ts)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(sql_client_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_valid_stream_id(stream_id) || max_modified_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", get_sql_tenant_id_()))
             || OB_FAIL(dml.add_pk_column("stream_id", stream_id))
             || OB_FAIL(dml.get_extra_condition().assign_fmt(
                 "status = %ld AND gmt_modified <= usec_to_time(%ld)",
                 static_cast<int64_t>(DIRECT_LOAD_STREAM_STATUS_FINISHED),
                 max_modified_ts))
             || OB_FAIL(dml.splice_delete_sql(share::OB_ALL_DIRECT_LOAD_STREAM_TNAME, sql))) {
    LOG_WARN("failed to build guarded direct load stream delete sql", KR(ret), K(stream_id));
  } else if (OB_FAIL(sql_client_->write(tenant_id_, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to delete persistent direct load stream", KR(ret), K(stream_id));
  } else if (is_zero_row(affected_rows)) {
    ObDirectLoadStreamMeta meta;
    const int get_ret = get_stream(stream_id, meta);
    if (OB_ENTRY_NOT_EXIST == get_ret) {
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS == get_ret) {
      ret = OB_STATE_NOT_MATCH;
    } else {
      ret = get_ret;
    }
  } else if (OB_FAIL(resolve_single_row_write(affected_rows))) {
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
