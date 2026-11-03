/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/spfresh/ob_spfresh_table_helper.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_single_connection_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "common/ob_timeout_ctx.h"
#include "share/ob_share_util.h"
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"
#include "lib/encode/ob_base64_encode.h"
#include "share/logical_table/ob_logical_table_decoder.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_lite_scan.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_trans_service.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include <cstring>

namespace oceanbase
{
namespace share
{

int ObSpfreshTableHelper::set_spfresh_timeout_ctx_(ObTimeoutCtx &timeout_ctx,
                                                   const bool set_trx_timeout)
{
  int ret = OB_SUCCESS;
  // ObTimeoutCtx inherits the enclosing context. Keep an existing transaction
  // timeout and let set_default_timeout_ctx() preserve an existing context or
  // worker deadline instead of extending a caller's shorter timeout.
  if (set_trx_timeout && !timeout_ctx.is_trx_timeout_set()
      && OB_FAIL(timeout_ctx.set_trx_timeout_us(SPFRESH_TIMEOUT_US))) {
    LOG_WARN("failed to set SPFRESH transaction timeout", K(ret),
             K(SPFRESH_TIMEOUT_US));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(
                 timeout_ctx, SPFRESH_TIMEOUT_US))) {
    LOG_WARN("failed to set SPFRESH SQL timeout", K(ret),
             K(SPFRESH_TIMEOUT_US));
  }
  return ret;
}

// File-local helper: convert binary data to hex string (uppercase)
static void bin_to_hex_(const char *data, int64_t data_len, char *hex_out)
{
  static const char hex_chars[] = "0123456789ABCDEF";
  for (int64_t i = 0; i < data_len; ++i) {
    hex_out[i * 2] = hex_chars[(static_cast<unsigned char>(data[i]) >> 4) & 0x0F];
    hex_out[i * 2 + 1] = hex_chars[static_cast<unsigned char>(data[i]) & 0x0F];
  }
  hex_out[data_len * 2] = '\0';
}

static bool identifier_has_backtick_(const char *s)
{
  bool found = false;
  if (OB_NOT_NULL(s)) {
    for (const char *p = s; '\0' != *p; ++p) {
      if ('`' == *p) {
        found = true;
        break;
      }
    }
  }
  return found;
}

// Encode logical-table kv_key prefix (25B) to uppercase hex for X'...' literals.
static int encode_logical_key_prefix_hex_(const uint64_t namespace_id,
                                          const uint64_t logical_table_id,
                                          const uint64_t index_seq,
                                          const uint8_t aux_type,
                                          char *hex_out,
                                          const int64_t hex_buf_len)
{
  int ret = OB_SUCCESS;
  char buf[OB_LOGICAL_TABLE_KEY_PREFIX_LEN];
  int64_t pos = 0;
  unsigned char *dst = reinterpret_cast<unsigned char *>(buf);
  const int64_t need_hex_len = OB_LOGICAL_TABLE_KEY_PREFIX_LEN * 2 + 1;
  if (OB_UNLIKELY(hex_buf_len < need_hex_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("hex buffer too small", K(ret), K(hex_buf_len), K(need_hex_len));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
                 namespace_id, dst, OB_LOGICAL_TABLE_KEY_PREFIX_LEN, pos))) {
    LOG_WARN("encode namespace_id failed", K(ret), K(namespace_id));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
                 logical_table_id, dst, OB_LOGICAL_TABLE_KEY_PREFIX_LEN, pos))) {
    LOG_WARN("encode logical_table_id failed", K(ret), K(logical_table_id));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
                 index_seq, dst, OB_LOGICAL_TABLE_KEY_PREFIX_LEN, pos))) {
    LOG_WARN("encode index_seq failed", K(ret), K(index_seq));
  } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint8(
                 aux_type, dst, OB_LOGICAL_TABLE_KEY_PREFIX_LEN, pos))) {
    LOG_WARN("encode aux_type failed", K(ret), K(aux_type));
  } else if (OB_UNLIKELY(pos != OB_LOGICAL_TABLE_KEY_PREFIX_LEN)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected encoded prefix length", K(ret), K(pos));
  } else {
    bin_to_hex_(buf, pos, hex_out);
  }
  return ret;
}

// --- ObSpfreshKvTableResolver ---

int ObSpfreshKvTableResolver::resolve(uint64_t tenant_id, uint64_t index_table_id,
                                       ObSpfreshKvTableInfo &kv_info)
{
  int ret = OB_SUCCESS;
  kv_info.reset();

  if (OB_INVALID_ID == tenant_id || OB_INVALID_ID == index_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(index_table_id));
  } else {
    schema::ObSchemaGetterGuard schema_guard;
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
        .get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id,
               index_schema))) {
      LOG_WARN("failed to get index schema", K(ret), K(index_table_id));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index schema is null", K(ret), K(index_table_id));
    } else {
      const uint64_t data_table_id = index_schema->get_data_table_id();
      const ObTableSchema *data_table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id,
                 data_table_schema))) {
        LOG_WARN("failed to get data table schema", K(ret), K(data_table_id));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table schema is null", K(ret), K(data_table_id));
      } else {
        const uint64_t database_id = data_table_schema->get_database_id();
        const ObDatabaseSchema *db_schema = nullptr;
        if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id,
                     db_schema))) {
          LOG_WARN("failed to get database schema", K(ret), K(database_id));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database schema is null", K(ret), K(database_id));
        } else if (db_schema->get_database_name_str().prefix_match("__recycle_$")) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("table in recyclebin, skip spfresh task", K(ret));
        } else {
          const ObString &db_name_str = db_schema->get_database_name_str();
          if (db_name_str.length() >= OB_MAX_DATABASE_NAME_BUF_LENGTH) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("database name too long", K(ret), K(db_name_str));
          } else {
            MEMCPY(kv_info.db_name_, db_name_str.ptr(), db_name_str.length());
            kv_info.db_name_[db_name_str.length()] = '\0';

            // Use tenant_id as namespace_id and logical_table_id (will be
            // overridden by task executor with session-level values).
            kv_info.namespace_id_ = tenant_id;
            kv_info.logical_table_id_ = data_table_id;
            kv_info.centroid_index_seq_ = 0;
            kv_info.cid_vector_index_seq_ = 0;
            kv_info.rowid_cid_index_seq_ = 0;

            // Find kv_data_table and aux index tables in same database.
            // Different aux types have different physical index tables, and
            // DML uses each table's physical table_id as the index_seq in the
            // kv_key prefix. We must resolve all three to match DML's encoding.
            const uint64_t tg_id = data_table_schema->get_tablegroup_id();
            schema::ObSchemaLiteScanSpec lite_spec;
            schema::ObSchemaLiteTableIterator lite_iter;
            const schema::ObSchemaLiteTableResult *row = nullptr;
            lite_spec.table_spec_.tenant_id_ = tenant_id;
            lite_spec.table_spec_.kind_ = schema::ObSchemaScanKind::BY_DATABASE;
            lite_spec.table_spec_.filter_id_ = database_id;
            lite_spec.table_spec_.select_flags_ =
                schema::OB_SCHEMA_SCAN_SELECT_TABLE_ID
                | schema::OB_SCHEMA_SCAN_SELECT_TABLE_TYPE
                | schema::OB_SCHEMA_SCAN_SELECT_INDEX_TYPE
                | schema::OB_SCHEMA_SCAN_SELECT_TABLEGROUP_ID
                | schema::OB_SCHEMA_SCAN_SELECT_DATA_TABLE_ID
                | schema::OB_SCHEMA_SCAN_SELECT_TABLE_NAME;
            lite_spec.caller_tag_ = "spfresh_kv_resolver";
            if (OB_FAIL(schema_guard.scan_lite_tables(lite_spec, lite_iter))) {
              LOG_WARN("failed to scan lite tables in database", K(ret), K(database_id));
            } else {
              bool found_kv = false;
              bool found_vec_col = false;
              while (OB_SUCC(ret)) {
                if (OB_FAIL(lite_iter.get_next(row))) {
                  if (OB_ITER_END == ret) {
                    ret = OB_SUCCESS;
                  } else {
                    LOG_WARN("fail to get next lite table", K(ret), K(database_id));
                  }
                  break;
                } else if (OB_ISNULL(row)) {
                  // skip
                } else {
                  // Aux index tables have tablegroup_id=-1 and is_vec_index()=true.
                  // Resolve per-aux-type index_seq from each corresponding physical table.
                  if (row->is_vec_index()
                      && row->data_table_id_ == data_table_id) {
                    if (0 == kv_info.centroid_index_seq_
                        && row->is_vec_ivf_centroid_index()) {
                      kv_info.centroid_index_seq_ = row->table_id_;
                    }
                    if (0 == kv_info.cid_vector_index_seq_
                        && row->is_vec_ivfflat_cid_vector_index()) {
                      kv_info.cid_vector_index_seq_ = row->table_id_;
                    }
                    if (0 == kv_info.rowid_cid_index_seq_
                        && row->is_vec_ivfflat_rowkey_cid_index()) {
                      kv_info.rowid_cid_index_seq_ = row->table_id_;
                    }
                    if (!found_vec_col) {
                      for (int64_t j = 0; OB_SUCC(ret)
                           && j < data_table_schema->get_column_count(); ++j) {
                        const ObColumnSchemaV2 *col =
                            data_table_schema->get_column_schema_by_idx(j);
                        if (OB_NOT_NULL(col)
                            && col->is_vec_ivf_data_vector_column()) {
                          const ObString &col_name = col->get_column_name_str();
                          if (col_name.length() >= OB_MAX_COLUMN_NAME_BUF_LENGTH) {
                            ret = OB_SIZE_OVERFLOW;
                            LOG_WARN("vector column name too long",
                                     K(ret), K(col_name));
                          } else {
                            MEMCPY(kv_info.vector_col_name_, col_name.ptr(),
                                   col_name.length());
                            kv_info.vector_col_name_[col_name.length()] = '\0';
                            found_vec_col = true;
                          }
                          break;
                        }
                      }
                    }
                  }
                  if (!found_kv && row->tablegroup_id_ == tg_id) {
                    const ObString &name = row->table_name_;
                    const char *kv_tag = "kv_data_table";
                    const int64_t kv_tag_len = 13; // strlen("kv_data_table")
                    bool name_matched = false;
                    if (name.length() >= kv_tag_len) {
                      for (int64_t p = 0; !name_matched && p + kv_tag_len <= name.length(); ++p) {
                        name_matched = (0 == MEMCMP(name.ptr() + p, kv_tag, kv_tag_len));
                      }
                    }
                    if (name_matched) {
                      if (name.length() >= OB_MAX_TABLE_NAME_BUF_LENGTH) {
                        ret = OB_SIZE_OVERFLOW;
                        LOG_WARN("kv table name too long", K(ret), K(name));
                      } else {
                        kv_info.kv_data_table_id_ = row->table_id_;
                        MEMCPY(kv_info.kv_table_name_, name.ptr(), name.length());
                        kv_info.kv_table_name_[name.length()] = '\0';
                        found_kv = true;
                      }
                    }
                  }
                }
              }
              if (OB_SUCC(ret) && !found_kv) {
                ret = OB_ENTRY_NOT_EXIST;
                LOG_WARN("kv_data_table not found in tablegroup", K(ret), K(tg_id));
              } else if (OB_SUCC(ret)) {
                if (0 == kv_info.centroid_index_seq_) {
                  LOG_TRACE("[SPFRESH_TRACE] centroid aux index not found, centroid_index_seq=0", 
                           K(data_table_id));
                }
                if (0 == kv_info.cid_vector_index_seq_) {
                  LOG_TRACE("[SPFRESH_TRACE] cid_vector aux index not found, cid_vector_index_seq=0",
                           K(data_table_id));
                }
                if (0 == kv_info.rowid_cid_index_seq_) {
                  LOG_TRACE("[SPFRESH_TRACE] rowid_cid aux index not found, rowid_cid_index_seq=0",
                           K(data_table_id));
                }
                // Resolve centroid column names from centroid aux table schema
                if (0 != kv_info.centroid_index_seq_) {
                  const ObTableSchema *centroid_schema = nullptr;
                  if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                               kv_info.centroid_index_seq_, centroid_schema))) {
                    LOG_WARN("failed to get centroid aux table schema", K(ret),
                             K(kv_info.centroid_index_seq_));
                  } else if (OB_ISNULL(centroid_schema)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("centroid aux table schema is null", K(ret));
                  } else {
                    bool found_centroid_vec = false;
                    bool found_centroid_state = false;
                    for (int64_t j = 0; OB_SUCC(ret)
                         && j < centroid_schema->get_column_count(); ++j) {
                      const ObColumnSchemaV2 *col =
                          centroid_schema->get_column_schema_by_idx(j);
                      if (OB_NOT_NULL(col)) {
                        const ObString &col_name = col->get_column_name_str();
                        if (!found_centroid_vec
                            && col_name.prefix_match(
                                OB_VEC_IVF_CENTER_VECTOR_COLUMN_NAME_PREFIX)) {
                          if (col_name.length() >= OB_MAX_COLUMN_NAME_BUF_LENGTH) {
                            ret = OB_SIZE_OVERFLOW;
                            LOG_WARN("centroid vector column name too long",
                                     K(ret), K(col_name));
                          } else {
                            MEMCPY(kv_info.centroid_vector_col_name_,
                                   col_name.ptr(), col_name.length());
                            kv_info.centroid_vector_col_name_[col_name.length()] = '\0';
                            found_centroid_vec = true;
                          }
                        }
                        if (!found_centroid_state
                            && col_name.prefix_match(
                                OB_VEC_IVF_CENTER_STATE_COLUMN_NAME_PREFIX)) {
                          if (col_name.length() >= OB_MAX_COLUMN_NAME_BUF_LENGTH) {
                            ret = OB_SIZE_OVERFLOW;
                            LOG_WARN("centroid state column name too long",
                                     K(ret), K(col_name));
                          } else {
                            MEMCPY(kv_info.centroid_state_col_name_,
                                   col_name.ptr(), col_name.length());
                            kv_info.centroid_state_col_name_[col_name.length()] = '\0';
                            found_centroid_state = true;
                          }
                        }
                      }
                    }
                    if (OB_SUCC(ret)
                        && (!found_centroid_vec || !found_centroid_state)) {
                      ret = OB_ERR_UNEXPECTED;
                      LOG_WARN("centroid vector/state column not found",
                               K(ret), K(found_centroid_vec),
                               K(found_centroid_state));
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

// --- ObSpfreshTableHelper ---

ObSpfreshTableHelper::ObSpfreshTableHelper()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    sql_proxy_(nullptr),
    centroid_index_seq_(0),
    cid_vector_index_seq_(0),
    rowid_cid_index_seq_(0),
    centroid_tablet_id_(common::OB_INVALID_ID),
    kv_data_table_id_(OB_INVALID_ID),
    batch_conn_(nullptr),
    batch_conn_proxy_(nullptr)
{
  db_name_[0] = '\0';
  kv_data_table_[0] = '\0';
  vector_col_name_[0] = '\0';
}

ObSpfreshTableHelper::~ObSpfreshTableHelper()
{
  destroy();
}

int ObSpfreshTableHelper::get_raw_vector_expected_bytes(
    int64_t dimension,
    int64_t &expected_bytes)
{
  int ret = OB_SUCCESS;
  expected_bytes = 0;
  const int64_t element_bytes = static_cast<int64_t>(sizeof(float));
  if (OB_UNLIKELY(dimension <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid raw vector dimension", K(ret), K(dimension));
  } else if (OB_UNLIKELY(dimension > INT64_MAX / element_bytes)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("raw vector byte length would overflow", K(ret), K(dimension), K(element_bytes));
  } else {
    expected_bytes = dimension * element_bytes;
  }
  return ret;
}

int ObSpfreshTableHelper::init(uint64_t tenant_id,
                               common::ObMySQLProxy *sql_proxy,
                               const ObSpfreshKvTableInfo &kv_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSpfreshTableHelper init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || OB_ISNULL(sql_proxy) || !kv_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(sql_proxy),
             K(kv_info.is_valid()));
  } else {
    const int64_t db_name_len = strlen(kv_info.db_name_);
    const int64_t tbl_name_len = strlen(kv_info.kv_table_name_);
    if (db_name_len >= OB_MAX_DATABASE_NAME_BUF_LENGTH
        || tbl_name_len >= OB_MAX_TABLE_NAME_BUF_LENGTH) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("name too long", K(ret), K(db_name_len), K(tbl_name_len));
    } else {
      MEMCPY(db_name_, kv_info.db_name_, db_name_len + 1);
      MEMCPY(kv_data_table_, kv_info.kv_table_name_, tbl_name_len + 1);
      const int64_t col_name_len = strlen(kv_info.vector_col_name_);
      if (col_name_len >= OB_MAX_COLUMN_NAME_BUF_LENGTH) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("vector column name too long", K(ret), K(col_name_len));
      } else {
        MEMCPY(vector_col_name_, kv_info.vector_col_name_, col_name_len + 1);
      }
      if (OB_SUCC(ret)) {
        const int64_t centro_vec_len = strlen(kv_info.centroid_vector_col_name_);
        const int64_t centro_state_len = strlen(kv_info.centroid_state_col_name_);
        if (centro_vec_len >= OB_MAX_COLUMN_NAME_BUF_LENGTH
            || centro_state_len >= OB_MAX_COLUMN_NAME_BUF_LENGTH) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("centroid column name too long", K(ret),
                   K(centro_vec_len), K(centro_state_len));
        } else {
          MEMCPY(centroid_vector_col_name_, kv_info.centroid_vector_col_name_,
                 centro_vec_len + 1);
          MEMCPY(centroid_state_col_name_, kv_info.centroid_state_col_name_,
                 centro_state_len + 1);
        }
      }
      if (OB_SUCC(ret)) {
        key_prefix_.namespace_id_ = kv_info.namespace_id_;
        key_prefix_.logical_table_id_ = kv_info.logical_table_id_;
        key_prefix_.index_seq_ = 0;
        centroid_index_seq_ = kv_info.centroid_index_seq_;
        cid_vector_index_seq_ = kv_info.cid_vector_index_seq_;
        rowid_cid_index_seq_ = kv_info.rowid_cid_index_seq_;
        centroid_tablet_id_ = kv_info.centroid_tablet_id_;
        kv_data_table_id_ = kv_info.kv_data_table_id_;
        tenant_id_ = tenant_id;
        sql_proxy_ = sql_proxy;
        batch_conn_ = nullptr;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

void ObSpfreshTableHelper::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    sql_proxy_ = nullptr;
    db_name_[0] = '\0';
    kv_data_table_[0] = '\0';
    vector_col_name_[0] = '\0';
    centroid_vector_col_name_[0] = '\0';
    centroid_state_col_name_[0] = '\0';
    key_prefix_.reset();
    centroid_tablet_id_ = common::OB_INVALID_ID;
    kv_data_table_id_ = OB_INVALID_ID;
    batch_conn_ = nullptr;
    if (OB_NOT_NULL(batch_conn_proxy_)) {
      delete batch_conn_proxy_;
      batch_conn_proxy_ = nullptr;
    }
  }
}

// --- KV Encoding Helpers ---

int ObSpfreshTableHelper::build_center_id_datum_(uint64_t center_id,
                                                  common::ObIAllocator &allocator,
                                                  common::ObDatum &datum)
{
  int ret = OB_SUCCESS;
  ObCenterId cid(centroid_tablet_id_, center_id);
  ObString str;
  if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(cid, str, &allocator))) {
    LOG_WARN("failed to set center_id to string", K(ret), K(cid),
             K_(centroid_tablet_id));
  } else {
    datum.set_string(str);
  }
  return ret;
}

int ObSpfreshTableHelper::build_kv_key_(ObLogicalTableAuxType aux_type,
                                         const common::ObIArray<common::ObDatum> &datums,
                                         common::ObIAllocator &allocator,
                                         common::ObString &key)
{
  int ret = OB_SUCCESS;
  ObLogicalTableKeyPrefix prefix = key_prefix_;
  prefix.aux_type_ = aux_type;
  prefix.index_seq_ = get_index_seq_(aux_type);
  if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, datums, allocator, key))) {
    LOG_WARN("encode kv_key failed", K(ret), K(prefix), K(datums.count()));
  }
  return ret;
}

uint64_t ObSpfreshTableHelper::get_index_seq_(ObLogicalTableAuxType aux_type) const
{
  uint64_t index_seq = 0;
  switch (aux_type) {
  case LT_AUX_IVF_CID_VECTOR: {
    index_seq = centroid_index_seq_;
    break;
  }
  case LT_AUX_IVF_CID_ROWID: {
    index_seq = cid_vector_index_seq_;
    break;
  }
  case LT_AUX_IVF_ROWID_CID: {
    index_seq = rowid_cid_index_seq_;
    break;
  }
  default: {
    index_seq = 0;
    break;
  }
  }
  return index_seq;
}

int64_t ObSpfreshTableHelper::get_scan_parallelism_() const
{
  int ret = OB_SUCCESS;
  int64_t parallelism = 1;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    parallelism = 1;
    LOG_WARN("tenant config is invalid, use serial scan", K(ret), K(tenant_id_));
  } else {
    int64_t user_config_parallelism = tenant_config->vector_index_optimization_concurrency;
    double min_cpu = 0.0;
    double max_cpu = 0.0;
    if (OB_ISNULL(GCTX.omt_) || OB_FAIL(GCTX.omt_->get_tenant_cpu(tenant_id_, min_cpu, max_cpu))) {
      parallelism = 1;
      LOG_WARN("fail to get tenant cpu, use serial scan", K(ret), K(tenant_id_));
    } else {
      int64_t tenant_max_cpu = static_cast<int64_t>(max_cpu);
      parallelism = tenant_max_cpu < 2 ? 1 : user_config_parallelism;
    }
  }
  LOG_INFO("spfresh scan parallelism", K(ret), K(parallelism), K(tenant_id_));
  return parallelism;
}

int ObSpfreshTableHelper::build_kv_value_(const common::ObIArray<common::ObString> &col_names,
                                           const common::ObIArray<common::ObObjMeta> &metas,
                                           const common::ObIArray<common::ObDatum> &datums,
                                           common::ObIAllocator &allocator,
                                           common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (col_names.count() != metas.count() || col_names.count() != datums.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count mismatch", K(ret), K(col_names.count()),
             K(metas.count()), K(datums.count()));
  } else if (OB_FAIL(ObLogicalTableEncoder::encode_value(col_names, metas, datums,
                         allocator, value))) {
    LOG_WARN("encode kv_value failed", K(ret));
  }
  return ret;
}

int ObSpfreshTableHelper::append_raw_cid_vector_(
    const char *kv_value,
    int64_t kv_value_len,
    int64_t expected_vector_bytes,
    common::ObIAllocator &allocator,
    const ObSpfreshVectorRow &key_row,
    ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(expected_vector_bytes <= 0
      || 0 != expected_vector_bytes % static_cast<int64_t>(sizeof(float)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expected raw vector byte length", K(ret), K(expected_vector_bytes));
  } else if (0 == kv_value_len) {
    // A zero-length value is the only NULL sentinel in the released raw format.
    // NULL postings are intentionally not exposed to SPFresh maintenance.
  } else if (OB_ISNULL(kv_value) || kv_value_len != expected_vector_bytes) {
    ret = OB_INVALID_DATA;
    LOG_WARN("raw cid rowid vector dimension mismatch", K(ret), KP(kv_value),
             K(kv_value_len), K(expected_vector_bytes));
  } else {
    ObSpfreshVectorRow row = key_row;
    char *vector_buf = static_cast<char *>(allocator.alloc(expected_vector_bytes));
    if (OB_ISNULL(vector_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc raw cid rowid vector failed", K(ret), K(expected_vector_bytes));
    } else {
      MEMCPY(vector_buf, kv_value, expected_vector_bytes);
      row.vector_ = vector_buf;
      row.vector_len_ = expected_vector_bytes;
      if (OB_FAIL(vectors.push_back(row))) {
        LOG_WARN("push raw vector row failed", K(ret));
      } else {
        ObSpfreshVectorRow &last = vectors.at(vectors.count() - 1);
        last.rowkey_ = last.rowkey_buf_;
      }
    }
  }
  return ret;
}

void ObSpfreshTableHelper::finalize_vector_scan_result_(
    int scan_ret,
    ObIArray<ObSpfreshVectorRow> &vectors)
{
  if (OB_SUCCESS == scan_ret) {
    // ObArray resizing moves rows without rebasing their self-referential
    // rowkey_ pointers. Repair every row only after the complete scan succeeds.
    for (int64_t i = 0; i < vectors.count(); ++i) {
      vectors.at(i).rowkey_ = vectors.at(i).rowkey_buf_;
    }
  } else {
    // Never publish a partial scan. Besides preserving failure atomicity, this
    // prevents callers from observing rowkey_ pointers into released storage
    // after a prior ObArray expansion.
    vectors.reset();
  }
}

int ObSpfreshTableHelper::decode_kv_row_(const char *kv_key, int64_t kv_key_len,
                                          const char *kv_value, int64_t kv_value_len,
                                          int64_t expected_vector_bytes,
                                          common::ObIAllocator &allocator,
                                          ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  ObSpfreshVectorRow row;
  bool row_appended = false;

  // Step 1: decode key prefix (25 bytes)
  ObLogicalTableKeyPrefix decoded_prefix;
  int64_t pos = 0;
  common::ObSEArray<common::ObDatum, 2> key_datums;
  if (OB_FAIL(ObLogicalTableDecoder::decode_key_prefix(kv_key, kv_key_len, pos,
                                                        decoded_prefix))) {
    LOG_WARN("decode key prefix failed", K(ret));
  } else if (OB_FAIL(ObLogicalTableDecoder::decode_key_body(
                 decoded_prefix.aux_type_, kv_key, kv_key_len, pos,
                 allocator, key_datums))) {
    LOG_WARN("decode key body failed", K(ret), K(decoded_prefix.aux_type_));
  } else {
    // Step 3: extract center_id and rowid from decoded datums
    int64_t cid_idx = 0;
    int64_t rowid_idx = -1;
    if (share::LT_AUX_IVF_CID_VECTOR == decoded_prefix.aux_type_) {
      // aux_type=4: [center_id(16B)] — no rowid
      cid_idx = 0;
    } else if (share::LT_AUX_IVF_CID_ROWID == decoded_prefix.aux_type_) {
      // aux_type=5: [center_id(16B)][rowid(8B)]
      cid_idx = 0;
      rowid_idx = 1;
    } else if (share::LT_AUX_IVF_ROWID_CID == decoded_prefix.aux_type_) {
      // aux_type=6: [rowid(8B)][center_id(16B)]
      cid_idx = 1;
      rowid_idx = 0;
    }

    // Decode center_id from 16B ObCenterId string
    if (cid_idx < key_datums.count()) {
      ObCenterId cid;
      if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
              cid, key_datums.at(cid_idx).get_string(),
              ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
        LOG_WARN("get_center_id_from_string failed", K(ret), K(cid_idx));
      } else {
        row.center_id_ = cid.center_id_;
      }
    }

    // Decode rowid if present
    if (OB_SUCC(ret) && rowid_idx >= 0 && rowid_idx < key_datums.count()) {
      uint64_t raw_rowid = key_datums.at(rowid_idx).get_uint();
      row.set_rowkey_from_int(static_cast<int64_t>(raw_rowid));
    }
  }

  // Step 4: CID_ROWID values use the final release format only: either an
  // exact float32[dimension] payload or the zero-length NULL sentinel. There
  // is deliberately no parser for pre-release formats; if the release or
  // rolling-upgrade premises change, this contract must be reassessed first.
  if (OB_SUCC(ret) && share::LT_AUX_IVF_CID_ROWID == decoded_prefix.aux_type_) {
    row_appended = true;
    if (OB_FAIL(append_raw_cid_vector_(kv_value, kv_value_len,
                                      expected_vector_bytes, allocator,
                                      row, vectors))) {
      LOG_WARN("append raw cid rowid vector failed", K(ret), K(kv_value_len),
               K(expected_vector_bytes));
    }
  }
  if (OB_SUCC(ret)
      && share::LT_AUX_IVF_CID_ROWID != decoded_prefix.aux_type_
      && kv_value_len > 0
      && OB_NOT_NULL(kv_value)) {
    common::ObIJsonBase *root = nullptr;
    common::ObString value_binary(kv_value_len, kv_value);
    if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
            &allocator, value_binary,
            common::ObJsonInType::JSON_BIN,
            common::ObJsonInType::JSON_TREE,
            root))) {
      LOG_WARN("parse kv_value as JSON_BIN failed", K(ret));
    } else if (OB_ISNULL(root)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parsed json root is null", K(ret));
    } else if (root->json_type() == common::ObJsonNodeType::J_OBJECT) {
      common::ObString vec_data;
      if (OB_FAIL(ObLogicalTableDecoder::extract_column_from_value(
              root, common::ObString::make_string(vector_col_name_), vec_data))) {
        LOG_WARN("extract vector column from value failed",
                 K(ret), K_(vector_col_name));
      } else {
        row.vector_ = vec_data.ptr();
        row.vector_len_ = vec_data.length();
      }
    } else if (root->json_type() == common::ObJsonNodeType::J_STRING) {
      // DML legacy format: "base64:typeXX:<base64_data>"
      common::ObString b64_str = root->get_data();
      const char *prefix_str = "base64:";
      const int64_t prefix_len = 7;
      if (b64_str.length() < prefix_len + 6
          || 0 != MEMCMP(b64_str.ptr(), prefix_str, prefix_len)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid base64 prefix in kv_value", K(b64_str));
      } else {
        const char *data_start = b64_str.ptr() + prefix_len;
        int64_t remaining = b64_str.length() - prefix_len;
        const char *colon = static_cast<const char *>(
            MEMCHR(data_start, ':', remaining));
        if (OB_ISNULL(colon) || colon - b64_str.ptr() + 1 >= b64_str.length()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid base64 type prefix");
        } else {
          data_start = colon + 1;
          remaining = b64_str.length() - (data_start - b64_str.ptr());
          int64_t max_decoded_len =
              common::ObBase64Encoder::needed_decoded_length(remaining);
          char *decoded_buf = static_cast<char *>(allocator.alloc(max_decoded_len));
          if (OB_ISNULL(decoded_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc base64 decode buf failed", K(ret));
          } else {
            int64_t decode_pos = 0;
            if (OB_FAIL(common::ObBase64Encoder::decode(
                    data_start, remaining,
                    reinterpret_cast<uint8_t *>(decoded_buf),
                    max_decoded_len, decode_pos))) {
              LOG_WARN("base64 decode failed", K(ret));
            } else {
              const char *name = vector_col_name_;
              const int64_t name_len = strlen(vector_col_name_);
              const char *found = nullptr;
              for (int64_t i = 0; i <= decode_pos - name_len; ++i) {
                if (0 == MEMCMP(decoded_buf + i, name, name_len)) {
                  found = decoded_buf + i;
                  break;
                }
              }
              if (OB_ISNULL(found)) {
                ret = OB_INVALID_DATA;
                LOG_WARN("vector column not found in decoded binary",
                         K_(vector_col_name));
              } else {
                const int64_t after_name = found - decoded_buf + name_len;
                if (after_name + 2 > decode_pos) {
                  ret = OB_INVALID_DATA;
                  LOG_WARN("decoded binary too short for data_vector header");
                } else {
                  const int64_t data_len = static_cast<unsigned char>(
                      decoded_buf[after_name + 1]);
                  const int64_t vec_start = after_name + 2;
                  if (vec_start + data_len > decode_pos) {
                    ret = OB_INVALID_DATA;
                    LOG_WARN("vector data exceeds decoded buffer",
                             K(vec_start), K(data_len), K(decode_pos));
                  } else {
                    row.vector_ = decoded_buf + vec_start;
                    row.vector_len_ = data_len;
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !row_appended) {
    if (OB_FAIL(vectors.push_back(row))) {
      LOG_WARN("push vector row failed", K(ret));
    } else {
      ObSpfreshVectorRow &last = vectors.at(vectors.count() - 1);
      last.rowkey_ = last.rowkey_buf_;
    }
  }
  return ret;
}

int ObSpfreshTableHelper::decode_centroid_value_(common::ObIAllocator &allocator,
                                                const char *kv_value,
                                                int64_t kv_value_len,
                                                const char *&vector_data,
                                                int64_t &vector_len,
                                                ObIvfCentroidState &state)
{
  int ret = OB_SUCCESS;
  vector_data = nullptr;
  vector_len = 0;
  state = ObIvfCentroidState::NORMAL;
  if (kv_value_len <= 0 || OB_ISNULL(kv_value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty kv_value for centroid", K(ret));
  } else {
    common::ObIJsonBase *root = nullptr;
    if (OB_FAIL(ObLogicalTableDecoder::decode_value(
            allocator, common::ObString(kv_value_len, kv_value), root))) {
      LOG_WARN("decode kv_value failed", K(ret));
    } else if (OB_ISNULL(root) || root->json_type() != common::ObJsonNodeType::J_OBJECT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("centroid kv_value is not object", K(ret));
    } else {
      common::ObIJsonBase *state_node = nullptr;
      common::ObIJsonBase *vec_node = nullptr;
      ObString state_key(centroid_state_col_name_);
      ObString vec_key(centroid_vector_col_name_);
      if (OB_FAIL(root->get_object_value(vec_key, vec_node))) {
        LOG_WARN("center_vector not in value", K(ret));
      } else if (OB_ISNULL(vec_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("center_vector node is null", K(ret));
      } else {
        vector_data = vec_node->get_data();
        vector_len = static_cast<int64_t>(vec_node->get_data_length());
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(root->get_object_value(state_key, state_node))) {
          LOG_WARN("center_state not in value", K(ret));
        } else if (OB_ISNULL(state_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("center_state node is null", K(ret));
        } else {
          state = static_cast<ObIvfCentroidState>(state_node->get_int());
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::decode_centroid_kv_row_(const char *kv_key,
                                                   int64_t kv_key_len,
                                                   const char *kv_value,
                                                   int64_t kv_value_len,
                                                   common::ObIAllocator &allocator,
                                                   ObSpfreshCentroidRow &centroid)
{
  int ret = OB_SUCCESS;
  centroid.reset();
  ObLogicalTableKeyPrefix decoded_prefix;
  int64_t pos = 0;
  common::ObSEArray<common::ObDatum, 1> key_datums;
  if (OB_FAIL(ObLogicalTableDecoder::decode_key_prefix(kv_key, kv_key_len, pos, decoded_prefix))) {
    LOG_WARN("decode key prefix failed", K(ret));
  } else if (share::LT_AUX_IVF_CID_VECTOR != decoded_prefix.aux_type_) {
    // The namespace-level centroid scan uses a broad key range (ltable_id
    // from 1 to UINT64_MAX) which may capture non-centroid rows (e.g.,
    // LT_AUX_PRIMARY data vectors). Skip them silently — centroid stays
    // reset and the caller filters by vector_!=nullptr.
  } else if (OB_FAIL(ObLogicalTableDecoder::decode_key_body(
                 decoded_prefix.aux_type_, kv_key, kv_key_len, pos, allocator, key_datums))) {
    LOG_WARN("decode key body failed", K(ret));
  } else {
    centroid.ltable_id_ = decoded_prefix.logical_table_id_;
    if (key_datums.count() > 0) {
      ObCenterId cid;
      if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
              cid, key_datums.at(0).get_string(),
              ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
        LOG_WARN("get_center_id_from_string failed", K(ret));
      } else {
        centroid.center_id_ = cid.center_id_;
      }
    }
  }
  if (OB_SUCC(ret) && share::LT_AUX_IVF_CID_VECTOR == decoded_prefix.aux_type_) {
    const char *vec_ptr = nullptr;
    int64_t vec_len = 0;
    ObIvfCentroidState state = ObIvfCentroidState::NORMAL;
    if (OB_FAIL(decode_centroid_value_(allocator, kv_value, kv_value_len, vec_ptr, vec_len, state))) {
      LOG_WARN("decode centroid value failed", K(ret));
    } else {
      char *vec_copy = static_cast<char *>(allocator.alloc(vec_len));
      if (OB_ISNULL(vec_copy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc centroid vector copy failed", K(ret), K(vec_len));
      } else {
        MEMCPY(vec_copy, vec_ptr, vec_len);
        centroid.vector_ = vec_copy;
        centroid.vector_len_ = vec_len;
        centroid.state_ = state;
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::execute_inner_sql_with_centroid_result_(
    const char *sql,
    common::ObIAllocator &allocator,
    ObIArray<ObSpfreshCentroidRow> &centroids,
    bool is_get_visiable /* = false */)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  centroids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null sql_proxy or sql", K(ret), KP(sql_proxy_), KP(sql));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH centroid query", K(ret));
  } else {
    MTL_SWITCH(tenant_id_) {
      ret = OB_SUCCESS;
      SMART_VAR(ObISQLClient::ReadResult, result) {
        common::sqlclient::ObMySQLResult *res = nullptr;
        if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql))) {
          LOG_WARN("execute read sql failed", K(ret), K_(tenant_id), K(sql));
        } else if (OB_ISNULL(res = result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(res->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              }
              LOG_WARN("get next result failed", K(ret));
            } else {
              ObString kv_key_raw;
              ObString kv_value_raw;
              if (OB_FAIL(res->get_varchar("kv_key", kv_key_raw))) {
                LOG_WARN("get kv_key failed", K(ret));
              } else if (OB_FAIL(res->get_varchar("kv_value", kv_value_raw))) {
                ret = OB_SUCCESS;
                kv_value_raw.reset();
              }
              if (OB_SUCC(ret)) {
                ObSpfreshCentroidRow row;
                if (OB_FAIL(decode_centroid_kv_row_(kv_key_raw.ptr(), kv_key_raw.length(),
                                                    kv_value_raw.ptr(), kv_value_raw.length(),
                                                    allocator, row))) {
                  LOG_WARN("decode centroid kv row failed", K(ret));
                } else if (nullptr == row.vector_) {
                  // non-centroid row from broad key range, skip silently
                } else if (is_get_visiable && ObCentroidInfo::is_visible_stat(row.state_) && OB_FAIL(centroids.push_back(row))) {
                  LOG_WARN("push centroid row failed", K(ret));
                } else if (!is_get_visiable && OB_FAIL(centroids.push_back(row))) {
                  LOG_WARN("push centroid row failed", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::scan_centroids_by_kv_key_range_(
    uint64_t namespace_id,
    const char *start_hex,
    const char *end_hex,
    const bool end_inclusive,
    common::ObIAllocator &allocator,
    ObIArray<ObSpfreshCentroidRow> &centroids,
    const bool is_get_visiable /* = false */)
{
  int ret = OB_SUCCESS;
  centroids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(start_hex) || OB_ISNULL(end_hex)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null kv key range", K(ret), KP(start_hex), KP(end_hex));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT kv_key, kv_value FROM `%s`.`%s` "
            "WHERE namespace_id = %lu AND kv_key >= X'%s' AND kv_key %s X'%s'",
            db_name_, kv_data_table_, namespace_id, start_hex,
            end_inclusive ? "<=" : "<", end_hex))) {
      LOG_WARN("assign scan centroids by kv key range sql failed", K(ret),
               K(namespace_id), K(start_hex), K(end_hex), K(end_inclusive));
    } else if (OB_FAIL(execute_inner_sql_with_centroid_result_(sql.ptr(),
                                                              allocator,
                                                              centroids,
                                                              is_get_visiable))) {
      LOG_WARN("execute scan centroids by kv key range failed", K(ret), K(sql));
    }
  }
  return ret;
}

int ObSpfreshTableHelper::scan_centroids_by_namespace(uint64_t namespace_id,
                                                     common::ObIAllocator &allocator,
                                                     ObIArray<ObSpfreshCentroidRow> &centroids)
{
  int ret = OB_SUCCESS;
  centroids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT kv_key, kv_value FROM `%s`.`%s` "
            "WHERE namespace_id = %lu AND LENGTH(kv_key) >= 25 "
            "AND HEX(SUBSTRING(kv_key, 25, 1)) = '04'",
            db_name_, kv_data_table_, namespace_id))) {
      LOG_WARN("assign scan centroids sql failed", K(ret));
    } else if (OB_FAIL(execute_inner_sql_with_centroid_result_(sql.ptr(), allocator, centroids))) {
      LOG_WARN("execute scan centroids failed", K(ret), K(sql));
    }
  }
  return ret;
}

int ObSpfreshTableHelper::scan_centroids_by_ltable(uint64_t namespace_id,
                                                   uint64_t ltable_id,
                                                   common::ObIAllocator &allocator,
                                                   ObIArray<ObSpfreshCentroidRow> &centroids)
{
  int ret = OB_SUCCESS;
  centroids.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else {
    char start_hex[OB_LOGICAL_TABLE_KEY_PREFIX_LEN * 2 + 1];
    char end_hex[OB_LOGICAL_TABLE_KEY_PREFIX_LEN * 2 + 1];
    if (OB_FAIL(encode_logical_key_prefix_hex_(
            namespace_id, ltable_id, centroid_index_seq_,
            static_cast<uint8_t>(LT_AUX_IVF_CID_VECTOR),
            start_hex, sizeof(start_hex)))) {
      LOG_WARN("encode centroid scan start prefix failed", K(ret),
               K(namespace_id), K(ltable_id));
    } else if (OB_FAIL(encode_logical_key_prefix_hex_(
                   namespace_id, ltable_id, centroid_index_seq_,
                   static_cast<uint8_t>(LT_AUX_IVF_CID_ROWID),
                   end_hex, sizeof(end_hex)))) {
      LOG_WARN("encode centroid scan end prefix failed", K(ret),
               K(namespace_id), K(ltable_id));
    } else if (OB_FAIL(scan_centroids_by_kv_key_range_(namespace_id, start_hex, end_hex,
                                                       false /*end_inclusive*/,
                                                       allocator, centroids,
                                                       true /*is_get_visiable*/))) {
      LOG_WARN("scan centroids by ltable failed", K(ret),
               K(namespace_id), K(ltable_id));
    }
  }
  return ret;
}

int ObSpfreshTableHelper::get_max_center_id_from_kv(uint64_t &max_center_id)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  max_center_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH max center id query", K(ret));
  } else {
    // Build [lower, upper] = [(tablet, 0), (tablet, UINT64_MAX)] full kv_keys so the
    // range is scoped to this centroid tablet (center_id is unique per tablet).
    ObArenaAllocator allocator("SpfreshMC", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    ObString lower_key, upper_key;
    ObDatum lower_datum, upper_datum;
    ObSEArray<ObDatum, 1> lower_kd, upper_kd;
    if (OB_FAIL(build_center_id_datum_(0, allocator, lower_datum))) {
      LOG_WARN("build lower center_id datum failed", K(ret));
    } else if (OB_FAIL(lower_kd.push_back(lower_datum))) {
      LOG_WARN("push lower datum failed", K(ret));
    } else if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, lower_kd, allocator, lower_key))) {
      LOG_WARN("build lower kv_key failed", K(ret));
    } else if (OB_FAIL(build_center_id_datum_(UINT64_MAX, allocator, upper_datum))) {
      LOG_WARN("build upper center_id datum failed", K(ret));
    } else if (OB_FAIL(upper_kd.push_back(upper_datum))) {
      LOG_WARN("push upper datum failed", K(ret));
    } else if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, upper_kd, allocator, upper_key))) {
      LOG_WARN("build upper kv_key failed", K(ret));
    } else {
      static const char hex_chars[] = "0123456789ABCDEF";
      const int64_t lower_hex_len = lower_key.length() * 2 + 1;
      const int64_t upper_hex_len = upper_key.length() * 2 + 1;
      char *lower_hex = static_cast<char *>(allocator.alloc(lower_hex_len));
      char *upper_hex = static_cast<char *>(allocator.alloc(upper_hex_len));
      if (OB_ISNULL(lower_hex) || OB_ISNULL(upper_hex)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc hex buf failed", K(ret));
      } else {
        for (int64_t i = 0; i < lower_key.length(); ++i) {
          const unsigned char b = static_cast<unsigned char>(lower_key.ptr()[i]);
          lower_hex[i * 2] = hex_chars[(b >> 4) & 0x0F];
          lower_hex[i * 2 + 1] = hex_chars[b & 0x0F];
        }
        lower_hex[lower_key.length() * 2] = '\0';
        for (int64_t i = 0; i < upper_key.length(); ++i) {
          const unsigned char b = static_cast<unsigned char>(upper_key.ptr()[i]);
          upper_hex[i * 2] = hex_chars[(b >> 4) & 0x0F];
          upper_hex[i * 2 + 1] = hex_chars[b & 0x0F];
        }
        upper_hex[upper_key.length() * 2] = '\0';

        // Reject backtick in identifiers before splicing into SQL.
        ObSqlString sql;
        if (OB_UNLIKELY(identifier_has_backtick_(db_name_)
                        || identifier_has_backtick_(kv_data_table_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("db_name or kv_data_table contains backtick, reject",
                   K(ret), KCSTRING(db_name_), KCSTRING(kv_data_table_));
        } else if (OB_FAIL(sql.assign_fmt(
                "SELECT kv_key FROM `%s`.`%s` "
                "WHERE namespace_id = %lu AND kv_key >= X'%s' AND kv_key <= X'%s' "
                "ORDER BY kv_key DESC LIMIT 1",
                db_name_, kv_data_table_, key_prefix_.namespace_id_,
                lower_hex, upper_hex))) {
          LOG_WARN("assign max center_id sql failed", K(ret));
        } else {
          ret = OB_ERR_UNEXPECTED;
          MTL_SWITCH(tenant_id_) {
            ret = OB_SUCCESS;
            SMART_VAR(ObISQLClient::ReadResult, result) {
              common::sqlclient::ObMySQLResult *res = nullptr;
              if (OB_FAIL(sql_proxy_->read(result, tenant_id_, sql.ptr()))) {
                LOG_WARN("execute max center_id sql failed", K(ret), K(sql));
              } else if (OB_ISNULL(res = result.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get mysql result failed", K(ret));
              } else if (OB_FAIL(res->next())) {
                if (OB_ITER_END == ret) {
                  // No centroid row in KV for this tablet → bootstrap (max=0).
                  ret = OB_SUCCESS;
                } else {
                  LOG_WARN("get next result failed", K(ret));
                }
              } else {
                ObString kv_key_raw;
                if (OB_FAIL(res->get_varchar("kv_key", kv_key_raw))) {
                  LOG_WARN("get kv_key failed", K(ret));
                } else {
                  ObLogicalTableKeyPrefix decoded_prefix;
                  int64_t pos = 0;
                  ObSEArray<ObDatum, 1> key_datums;
                  if (OB_FAIL(ObLogicalTableDecoder::decode_key_prefix(
                          kv_key_raw.ptr(), kv_key_raw.length(), pos, decoded_prefix))) {
                    LOG_WARN("decode key prefix failed", K(ret));
                  } else if (OB_FAIL(ObLogicalTableDecoder::decode_key_body(
                          decoded_prefix.aux_type_, kv_key_raw.ptr(),
                          kv_key_raw.length(), pos, allocator, key_datums))) {
                    LOG_WARN("decode key body failed", K(ret));
                  } else if (key_datums.count() > 0) {
                    ObCenterId cid;
                    if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                            cid, key_datums.at(0).get_string(),
                            ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
                      LOG_WARN("get_center_id_from_string failed", K(ret));
                    } else {
                      max_center_id = cid.center_id_;
                    }
                  } else {
                    // A centroid row with empty key body must not be treated as
                    // bootstrap (max=0), or callers may re-allocate existing IDs.
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("centroid kv_key decoded to empty key_datums", K(ret),
                             K(kv_key_raw.length()));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// --- SQL Execution ---

int ObSpfreshTableHelper::execute_inner_sql_(const char *sql, SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null sql_proxy or sql", K(ret), KP(sql_proxy_), KP(sql));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, true /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH inner SQL", K(ret));
  } else if (OB_NOT_NULL(batch_conn_)) {
    // In batch mode: execute on the existing connection, no tx management
    ret = execute_sql_no_tx_(sql);
  } else {
    ret = OB_ERR_UNEXPECTED;
    MTL_SWITCH(tenant_id_) {
      ret = OB_SUCCESS;
      int64_t affected_rows = 0;
      common::ObSingleConnectionProxy single_conn_proxy;
      sqlclient::ObISQLConnection *conn = nullptr;
      if (OB_FAIL(single_conn_proxy.connect(tenant_id_, 0, sql_proxy_))) {
        LOG_WARN("single conn connect failed", K(ret), K_(tenant_id));
      } else if (OB_ISNULL(conn = single_conn_proxy.get_connection())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("connection is null", K(ret));
      } else if (OB_FAIL(conn->start_transaction(tenant_id_, false))) {
        LOG_WARN("failed to start transaction", K(ret), K_(tenant_id));
      } else if (OB_FAIL(conn->execute_write(tenant_id_, sql, affected_rows))) {
        LOG_WARN("failed to execute inner sql", K(ret), K(sql));
      } else {
        int tmp_ret = conn->commit();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("failed to commit transaction", K(tmp_ret));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        } else {
          commit_scn = SCN::min_scn();
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::execute_sql_no_tx_(const char *sql)
{
  int64_t affected_rows = 0;
  return execute_sql_no_tx_(sql, affected_rows);
}

int ObSpfreshTableHelper::execute_sql_no_tx_(const char *sql, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  ObTimeoutCtx timeout_ctx;
  if (OB_ISNULL(batch_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch_conn_ is null, call start_batch_transaction first", K(ret));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH batch SQL", K(ret));
  } else if (OB_FAIL(batch_conn_->execute_write(tenant_id_, sql, affected_rows))) {
    // The connection owns the transaction context established by
    // start_batch_transaction().  Switching MTL again while that transaction
    // is active is invalid on vector recovery workers; use the owning
    // connection directly and pass its tenant id explicitly.
    LOG_WARN("failed to execute batch sql", K(ret), K(sql));
  }
  return ret;
}

int ObSpfreshTableHelper::start_batch_transaction()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null sql_proxy", K(ret));
  } else if (OB_NOT_NULL(batch_conn_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("batch transaction already started", K(ret));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, true /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH batch transaction", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    MTL_SWITCH(tenant_id_) {
      ret = OB_SUCCESS;
      common::ObSingleConnectionProxy *proxy = new common::ObSingleConnectionProxy();
      if (OB_ISNULL(proxy)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc batch conn proxy", K(ret));
      } else if (OB_FAIL(proxy->connect(tenant_id_, 0, sql_proxy_))) {
        LOG_WARN("batch conn connect failed", K(ret), K_(tenant_id));
        delete proxy;
      } else if (OB_ISNULL(batch_conn_ = proxy->get_connection())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch connection is null", K(ret));
        delete proxy;
      } else {
        // Some batch reads execute directly without creating a local ObTimeoutCtx.
        batch_conn_->set_user_timeout(SPFRESH_TIMEOUT_US);
        if (OB_FAIL(batch_conn_->start_transaction(tenant_id_, false))) {
          LOG_WARN("failed to start batch transaction", K(ret), K_(tenant_id));
          delete proxy;
          batch_conn_ = nullptr;
        } else {
          batch_conn_proxy_ = proxy;
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::lock_batch_tablet(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  if (IS_NOT_INIT || OB_ISNULL(batch_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet lock requires an active batch transaction", K(ret));
  } else if (!tablet_id.is_valid() || OB_INVALID_ID == kv_data_table_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid physical tablet lock target", K(ret), K(tablet_id), K_(kv_data_table_id));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for batch tablet lock", K(ret));
  } else if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_tablet(
      tenant_id_, kv_data_table_id_, tablet_id, transaction::tablelock::EXCLUSIVE,
      0 /*try lock*/, static_cast<observer::ObInnerSQLConnection *>(batch_conn_)))) {
    if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret || OB_TRY_LOCK_ROW_CONFLICT == ret) {
      // The caller must roll back the batch before retrying or skipping this attempt.
      ret = OB_EAGAIN;
      LOG_TRACE("SPFresh tablet busy; retry later", K(ret), K(tablet_id));
    } else {
      LOG_WARN("failed to lock SPFresh tablet", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObSpfreshTableHelper::end_batch_transaction(const bool commit, SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  commit_scn = SCN::min_scn();
  if (OB_ISNULL(batch_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch transaction not started", K(ret));
  } else {
    const int timeout_ret = set_spfresh_timeout_ctx_(
        timeout_ctx, true /*set_trx_timeout*/);
    if (OB_SUCCESS != timeout_ret) {
      // Timeout setup is best-effort and must not prevent an active transaction
      // from being explicitly committed or rolled back.
      LOG_WARN("failed to set timeout for ending SPFRESH batch transaction",
               K(timeout_ret));
    }
    if (commit) {
      int tmp_ret = batch_conn_->commit();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to commit batch transaction", K(tmp_ret));
        ret = tmp_ret;
      }
    } else {
      int tmp_ret = batch_conn_->rollback();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to rollback batch transaction", K(tmp_ret));
        ret = tmp_ret;
      }
    }
    batch_conn_ = nullptr;
    if (OB_NOT_NULL(batch_conn_proxy_)) {
      delete batch_conn_proxy_;
      batch_conn_proxy_ = nullptr;
    }
  }
  return ret;
}

int ObSpfreshTableHelper::execute_inner_sql_with_result_(
    const char *sql,
    int64_t expected_vector_bytes,
    ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  vectors.reset();
  ObArenaAllocator allocator("SpfreshExR", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  if (OB_FAIL(execute_inner_sql_with_result_(
      sql, expected_vector_bytes, allocator, vectors))) {
    LOG_WARN("execute_inner_sql_with_result failed", K(ret));
  }
  return ret;
}

int ObSpfreshTableHelper::execute_inner_sql_with_result_(
    const char *sql,
    int64_t expected_vector_bytes,
    common::ObIAllocator &allocator,
    ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  vectors.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null sql_proxy or sql", K(ret), KP(sql_proxy_), KP(sql));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH vector query", K(ret));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      common::sqlclient::ObMySQLResult *res = nullptr;
      if (OB_NOT_NULL(batch_conn_)) {
        const ObString sql_str = ObString::make_string(sql);
        if (OB_FAIL(batch_conn_->execute_read(tenant_id_, sql_str, result))) {
          LOG_WARN("execute batch read failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        MTL_SWITCH(tenant_id_) {
          ret = sql_proxy_->read(result, tenant_id_, sql);
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("execute read sql failed", K(ret), K_(tenant_id));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(res->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
            LOG_WARN("get next result failed", K(ret));
          } else {
            ObString kv_key_raw;
            ObString kv_value_raw;
            if (OB_FAIL(res->get_varchar("kv_key", kv_key_raw))) {
              LOG_WARN("get kv_key failed", K(ret));
            } else if (OB_FAIL(res->get_varchar("kv_value", kv_value_raw))) {
              // Both callers add LENGTH(kv_value) > 0. Any extraction failure is
              // therefore a real read error and must abort the maintenance batch.
              LOG_WARN("get kv_value failed", K(ret));
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(decode_kv_row_(kv_key_raw.ptr(), kv_key_raw.length(),
                                          kv_value_raw.ptr(), kv_value_raw.length(),
                                          expected_vector_bytes,
                                          allocator, vectors))) {
                LOG_WARN("decode kv row failed", K(ret));
              }
            }
          }
        }
      }
    }
  }

  finalize_vector_scan_result_(ret, vectors);
  return ret;
}

// --- Centroid Operations (aux_type=4) ---

int ObSpfreshTableHelper::insert_centroid(uint64_t center_id,
                                           const char *vector,
                                           int64_t vector_len,
                                           ObIvfCentroidState state,
                                           SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(vector) || vector_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(vector), K(vector_len));
  } else {
    // Build key: aux_type=4, key_columns=[center_id(16B)]
    ObArenaAllocator allocator("SpfreshI", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    ObSEArray<common::ObDatum, 1> key_datums;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else if (OB_FAIL(key_datums.push_back(center_id_datum))) {
      LOG_WARN("push key datum failed", K(ret));
    } else {
      common::ObString kv_key;
      if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, key_datums, allocator, kv_key))) {
        LOG_WARN("build kv_key failed", K(ret));
      } else {
        // Build value: columns=[center_vector(VARBINARY), center_state(INT)]
        common::ObString col_names_arr[2];
        col_names_arr[0] = common::ObString::make_string(centroid_vector_col_name_);
        col_names_arr[1] = common::ObString::make_string(centroid_state_col_name_);
        common::ObSEArray<common::ObString, 2> col_names;
        if (OB_FAIL(col_names.push_back(col_names_arr[0]))
            || OB_FAIL(col_names.push_back(col_names_arr[1]))) {
          LOG_WARN("push col names failed", K(ret));
        } else {
          common::ObObjMeta metas_arr[2];
          metas_arr[0].set_varbinary();
          metas_arr[1].set_int();
          common::ObSEArray<common::ObObjMeta, 2> metas;
          if (OB_FAIL(metas.push_back(metas_arr[0]))
              || OB_FAIL(metas.push_back(metas_arr[1]))) {
            LOG_WARN("push metas failed", K(ret));
          } else {
            int64_t state_val = static_cast<int64_t>(state);
            common::ObDatum val_datums_arr[2];
            val_datums_arr[0].set_string(common::ObString(vector_len, vector));
            val_datums_arr[1] = common::ObDatum(reinterpret_cast<const char *>(&state_val),
                                                sizeof(int64_t), false);
            common::ObSEArray<common::ObDatum, 2> val_datums;
            if (OB_FAIL(val_datums.push_back(val_datums_arr[0]))
                || OB_FAIL(val_datums.push_back(val_datums_arr[1]))) {
              LOG_WARN("push val datums failed", K(ret));
            } else {
              common::ObString kv_value;
              if (OB_FAIL(build_kv_value_(col_names, metas, val_datums, allocator, kv_value))) {
                LOG_WARN("build kv_value failed", K(ret));
              } else {
                // Build hex-encoded key and value for SQL INSERT
                static const char hex_chars[] = "0123456789ABCDEF";
                char *key_hex = static_cast<char *>(ob_malloc(kv_key.length() * 2 + 1, "SpfreshKHex"));
                char *val_hex = static_cast<char *>(ob_malloc(kv_value.length() * 2 + 1, "SpfreshVHex"));
                if (OB_ISNULL(key_hex) || OB_ISNULL(val_hex)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc hex buf failed", K(ret));
                } else {
                  for (int64_t i = 0; i < kv_key.length(); ++i) {
                    key_hex[i * 2] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) >> 4) & 0x0F];
                    key_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) & 0x0F)];
                  }
                  key_hex[kv_key.length() * 2] = '\0';
                  for (int64_t i = 0; i < kv_value.length(); ++i) {
                    val_hex[i * 2] = hex_chars[(static_cast<unsigned char>(kv_value.ptr()[i]) >> 4) & 0x0F];
                    val_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(kv_value.ptr()[i]) & 0x0F)];
                  }
                  val_hex[kv_value.length() * 2] = '\0';

                  ObSqlString sql;
                  if (OB_FAIL(sql.assign_fmt(
                          "INSERT INTO `%s`.`%s` (namespace_id, kv_key, kv_value) "
                          "VALUES (%lu, X'%s', X'%s')",
                          db_name_, kv_data_table_, key_prefix_.namespace_id_,
                          key_hex, val_hex))) {
                    LOG_WARN("assign insert sql failed", K(ret));
                  } else if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
                    LOG_WARN("execute insert failed", K(ret), K(sql));
                  }
                }
                if (OB_NOT_NULL(key_hex)) { ob_free(key_hex); }
                if (OB_NOT_NULL(val_hex)) { ob_free(val_hex); }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::update_centroid_state(uint64_t center_id,
                                                 ObIvfCentroidState new_state,
                                                 ObIvfCentroidState expected_old_state,
                                                 SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH centroid state update", K(ret));
  } else {
    ObArenaAllocator allocator("SpfreshUS", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    // Build key for this centroid
    common::ObDatum center_id_datum;
    ObSEArray<common::ObDatum, 1> key_datums;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else if (OB_FAIL(key_datums.push_back(center_id_datum))) {
      LOG_WARN("push key datum failed", K(ret));
    } else {
      common::ObString kv_key;
      if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, key_datums, allocator, kv_key))) {
        LOG_WARN("build kv_key failed", K(ret));
      } else {
        // Hex encode key
        static const char hex_chars[] = "0123456789ABCDEF";
        char *key_hex = static_cast<char *>(ob_malloc(kv_key.length() * 2 + 1, "SpfreshKH2"));
        if (OB_ISNULL(key_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc key hex failed", K(ret));
        } else {
          for (int64_t i = 0; i < kv_key.length(); ++i) {
            key_hex[i * 2] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) >> 4) & 0x0F];
            key_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) & 0x0F)];
          }
          key_hex[kv_key.length() * 2] = '\0';

          // READ: SELECT kv_value to verify old state and capture old vector data
          const char *old_vec_data = nullptr;
          int64_t old_vec_len = 0;
          ObSqlString select_sql;
          if (OB_FAIL(select_sql.assign_fmt(
                  "SELECT kv_value FROM `%s`.`%s` "
                  "WHERE namespace_id = %lu AND kv_key = X'%s'",
                  db_name_, kv_data_table_, key_prefix_.namespace_id_, key_hex))) {
            LOG_WARN("assign select sql failed", K(ret));
          } else {
            SMART_VAR(ObISQLClient::ReadResult, result) {
              common::sqlclient::ObMySQLResult *mysql_res = nullptr;
              if (OB_FAIL(sql_proxy_->read(result, tenant_id_, select_sql.ptr()))) {
                LOG_WARN("execute select failed", K(ret), K(select_sql));
              } else if (OB_ISNULL(mysql_res = result.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("mysql result is null", K(ret));
              } else {
                bool found = false;
                while (OB_SUCC(ret) && OB_SUCC(mysql_res->next())) {
                  if (found) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("multiple centroids for same center_id", K(ret));
                  } else {
                    found = true;
                    ObString kv_value_raw;
                    if (OB_FAIL(mysql_res->get_varchar("kv_value", kv_value_raw))) {
                      LOG_WARN("get kv_value failed", K(ret));
                    } else {
                      // Decode value to check state and capture old vector data
                      common::ObIJsonBase *root = nullptr;
                      if (OB_FAIL(ObLogicalTableDecoder::decode_value(
                              allocator, kv_value_raw, root))) {
                        LOG_WARN("decode kv_value failed", K(ret));
                      } else if (OB_NOT_NULL(root)
                                 && root->json_type() == common::ObJsonNodeType::J_OBJECT) {
                        common::ObIJsonBase *state_node = nullptr;
                        common::ObIJsonBase *vec_node = nullptr;
                        ObString state_key(centroid_state_col_name_);
                        ObString vec_key(centroid_vector_col_name_);
                        if (OB_FAIL(root->get_object_value(state_key, state_node))) {
                          LOG_WARN("center_state not in value", K(ret));
                        } else if (OB_ISNULL(state_node)) {
                          ret = OB_ERR_UNEXPECTED;
                          LOG_WARN("center_state node is null", K(ret));
                        } else {
                          int64_t current_state = state_node->get_int();
                          if (expected_old_state < ObIvfCentroidState::MAX_TYPE
                                     && current_state
                                            != static_cast<int64_t>(expected_old_state)) {
                            ret = OB_STATE_NOT_MATCH;
                            LOG_WARN("centroid state mismatch", K(ret), K(current_state),
                                     K(static_cast<int64_t>(expected_old_state)));
                          }
                        }
                        // Capture old vector data for the re-insert
                        if (OB_SUCC(ret)) {
                          if (OB_FAIL(root->get_object_value(vec_key, vec_node))) {
                            LOG_WARN("center_vector not in value", K(ret));
                          } else if (OB_ISNULL(vec_node)) {
                            ret = OB_ERR_UNEXPECTED;
                            LOG_WARN("center_vector node is null", K(ret));
                          } else {
                            old_vec_data = vec_node->get_data();
                            old_vec_len = static_cast<int64_t>(vec_node->get_data_length());
                          }
                        }
                      }
                    }
                  }
                }
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                }
                if (OB_SUCC(ret) && !found) {
                  ret = OB_ENTRY_NOT_EXIST;
                  LOG_WARN("centroid not found", K(ret), K(center_id));
                }
              }
            }
          }

          // DELETE old row + INSERT new row with updated state
          bool batch_tx_local = false;
          if (OB_SUCC(ret) && OB_ISNULL(batch_conn_)) {
            if (OB_FAIL(start_batch_transaction())) {
              LOG_WARN("start batch tx for update_centroid_state failed", K(ret));
            } else {
              batch_tx_local = true;
            }
          }

          if (OB_SUCC(ret)) {
            ObSqlString delete_sql;
            if (OB_FAIL(delete_sql.assign_fmt(
                    "DELETE FROM `%s`.`%s` WHERE namespace_id = %lu AND kv_key = X'%s'",
                    db_name_, kv_data_table_, key_prefix_.namespace_id_, key_hex))) {
              LOG_WARN("assign delete sql failed", K(ret));
            } else if (OB_FAIL(execute_inner_sql_(delete_sql.ptr(), commit_scn))) {
              LOG_WARN("execute delete failed", K(ret), K(delete_sql));
            } else {
              // INSERT new row with updated state, preserving old vector
              common::ObString col_names_arr[2];
              col_names_arr[0] = common::ObString::make_string(centroid_vector_col_name_);
              col_names_arr[1] = common::ObString::make_string(centroid_state_col_name_);
              common::ObSEArray<common::ObString, 2> col_names;
              common::ObObjMeta metas_arr[2];
              metas_arr[0].set_varbinary();
              metas_arr[1].set_int();
              common::ObSEArray<common::ObObjMeta, 2> metas;
              int64_t new_state_val = static_cast<int64_t>(new_state);
              common::ObDatum val_datums_arr[2];
              val_datums_arr[0].set_string(
                  common::ObString(old_vec_len, old_vec_data));
              val_datums_arr[1] = common::ObDatum(reinterpret_cast<const char *>(&new_state_val),
                                                  sizeof(int64_t), false);
              common::ObSEArray<common::ObDatum, 2> val_datums;
              if (OB_FAIL(col_names.push_back(col_names_arr[0]))
                  || OB_FAIL(col_names.push_back(col_names_arr[1]))
                  || OB_FAIL(metas.push_back(metas_arr[0]))
                  || OB_FAIL(metas.push_back(metas_arr[1]))
                  || OB_FAIL(val_datums.push_back(val_datums_arr[0]))
                  || OB_FAIL(val_datums.push_back(val_datums_arr[1]))) {
                LOG_WARN("push column metadata failed", K(ret));
              } else {
                common::ObString kv_value_new;
                if (OB_FAIL(build_kv_value_(col_names, metas, val_datums, allocator,
                                            kv_value_new))) {
                  LOG_WARN("build value failed", K(ret));
                } else {
                  char *val_hex = static_cast<char *>(
                      ob_malloc(kv_value_new.length() * 2 + 1, "SpfreshVH2"));
                  if (OB_ISNULL(val_hex)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("alloc val hex failed", K(ret));
                  } else {
                    for (int64_t i = 0; i < kv_value_new.length(); ++i) {
                      val_hex[i * 2] = hex_chars[
                          (static_cast<unsigned char>(kv_value_new.ptr()[i]) >> 4) & 0x0F];
                      val_hex[i * 2 + 1] = hex_chars[
                          static_cast<unsigned char>(kv_value_new.ptr()[i]) & 0x0F];
                    }
                    val_hex[kv_value_new.length() * 2] = '\0';
                    ObSqlString insert_sql;
                    if (OB_FAIL(insert_sql.assign_fmt(
                            "INSERT INTO `%s`.`%s` (namespace_id, kv_key, kv_value) "
                            "VALUES (%lu, X'%s', X'%s')",
                            db_name_, kv_data_table_, key_prefix_.namespace_id_,
                            key_hex, val_hex))) {
                      LOG_WARN("assign insert sql failed", K(ret));
                    } else if (OB_FAIL(execute_inner_sql_(insert_sql.ptr(), commit_scn))) {
                      LOG_WARN("execute insert failed", K(ret), K(insert_sql));
                    }
                    ob_free(val_hex);
                  }
                }
              }
            }
          }

          if (batch_tx_local) {
            int end_ret = OB_SUCCESS;
            SCN end_scn;
            if (OB_SUCCESS != (end_ret = end_batch_transaction(OB_SUCC(ret), end_scn))) {
              LOG_WARN("end batch tx for update_centroid_state failed", K(end_ret));
              if (OB_SUCC(ret)) {
                ret = end_ret;
              }
            } else if (OB_SUCC(ret)) {
              commit_scn = end_scn;
            }
          }
          ob_free(key_hex);
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::delete_centroid(uint64_t center_id, SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else {
    ObArenaAllocator allocator("SpfreshDC", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    ObSEArray<common::ObDatum, 1> key_datums;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else if (OB_FAIL(key_datums.push_back(center_id_datum))) {
      LOG_WARN("push key datum failed", K(ret));
    } else {
      common::ObString kv_key;
      if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, key_datums, allocator, kv_key))) {
        LOG_WARN("build kv_key failed", K(ret));
      } else {
        static const char hex_chars[] = "0123456789ABCDEF";
        char *key_hex = static_cast<char *>(ob_malloc(kv_key.length() * 2 + 1, "SpfreshKH3"));
        if (OB_ISNULL(key_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc key hex failed", K(ret));
        } else {
          for (int64_t i = 0; i < kv_key.length(); ++i) {
            key_hex[i * 2] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) >> 4) & 0x0F];
            key_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) & 0x0F)];
          }
          key_hex[kv_key.length() * 2] = '\0';
          ObSqlString sql;
          if (OB_FAIL(sql.assign_fmt(
                  "DELETE FROM `%s`.`%s` WHERE namespace_id = %lu AND kv_key = X'%s'",
                  db_name_, kv_data_table_, key_prefix_.namespace_id_, key_hex))) {
            LOG_WARN("assign delete sql failed", K(ret));
          } else if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
            LOG_WARN("execute delete failed", K(ret), K(sql));
          }
          ob_free(key_hex);
        }
      }
    }
  }

  return ret;
}

// --- CID-Vector Operations (aux_type=5) ---

int ObSpfreshTableHelper::scan_cid_vector_by_center(
    uint64_t center_id,
    int64_t dimension,
    common::ObIAllocator &allocator,
    ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  int64_t expected_vector_bytes = 0;
  vectors.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_FAIL(get_raw_vector_expected_bytes(
                 dimension, expected_vector_bytes))) {
    LOG_WARN("invalid dimension for cid vector scan", K(ret), K(dimension));
  } else {
    ObArenaAllocator key_allocator("SpfreshSC", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    ObSEArray<common::ObDatum, 1> scan_key_datums;
    if (OB_FAIL(build_center_id_datum_(center_id, key_allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else if (OB_FAIL(scan_key_datums.push_back(center_id_datum))) {
      LOG_WARN("push key datum failed", K(ret));
    } else {
      // Build range keys: kv_key LIKE prefix+center_id+%
      // Use prefix encoding: build start_key with [center_id, min_rowid]
      // and end_key with [center_id, max_rowid]
      ObLogicalTableKeyPrefix prefix = key_prefix_;
      prefix.aux_type_ = LT_AUX_IVF_CID_ROWID;
      prefix.index_seq_ = get_index_seq_(LT_AUX_IVF_CID_ROWID);

      // Build start key: prefix + center_id + min_rowid(1)
      // Skip rowid=0 to avoid centroid metadata rows from being scanned as vectors.
      // Must call set_uint() so encode_key_column_ uses variable-length encoding
      // matching the stored key format (LT_AUX_IVF_CID_ROWID col descriptor has no fixed_length_).
      common::ObString start_key;
      {
        uint64_t rowid_val = 1;
        common::ObDatum rowid_datum;
        rowid_datum.ptr_ = reinterpret_cast<const char *>(&rowid_val);
        rowid_datum.set_uint(rowid_val);
        ObSEArray<common::ObDatum, 2> start_datums;
        if (OB_FAIL(start_datums.push_back(center_id_datum))
            || OB_FAIL(start_datums.push_back(rowid_datum))) {

          LOG_WARN("push start datums failed", K(ret));
        } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, start_datums,
                    key_allocator, start_key))) {
          LOG_WARN("encode start key failed", K(ret));
        }
      }

      // Build end key: prefix + center_id + max_rowid
      common::ObString end_key;
      if (OB_SUCC(ret)) {
        uint64_t max_rowid_val = UINT64_MAX;
        common::ObDatum rowid_max_datum;
        rowid_max_datum.ptr_ = reinterpret_cast<const char *>(&max_rowid_val);
        rowid_max_datum.set_uint(UINT64_MAX);
        ObSEArray<common::ObDatum, 2> end_datums;
        if (OB_FAIL(end_datums.push_back(center_id_datum))
            || OB_FAIL(end_datums.push_back(rowid_max_datum))) {

          LOG_WARN("push end datums failed", K(ret));
        } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, end_datums,
                    key_allocator, end_key))) {
          LOG_WARN("encode end key failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        static const char hex_chars[] = "0123456789ABCDEF";
        char *start_hex = static_cast<char *>(ob_malloc(start_key.length() * 2 + 1, "SpfreshSH1"));
        char *end_hex = static_cast<char *>(ob_malloc(end_key.length() * 2 + 1, "SpfreshEH1"));
        if (OB_ISNULL(start_hex) || OB_ISNULL(end_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc hex buf failed", K(ret));
        } else {
          for (int64_t i = 0; i < start_key.length(); ++i) {
            start_hex[i * 2] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) >> 4) & 0x0F];
            start_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) & 0x0F)];
          }
          start_hex[start_key.length() * 2] = '\0';
          for (int64_t i = 0; i < end_key.length(); ++i) {
            end_hex[i * 2] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) >> 4) & 0x0F];
            end_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) & 0x0F)];
          }
          end_hex[end_key.length() * 2] = '\0';

          ObSqlString sql;
          int64_t parallelism = OB_NOT_NULL(batch_conn_) ? 1 : get_scan_parallelism_();
          if (OB_FAIL(sql.assign_fmt(
                  "SELECT /*+ parallel(%ld) */ kv_key, kv_value FROM `%s`.`%s` "
                  "WHERE namespace_id = %lu AND kv_key >= X'%s' AND kv_key < X'%s' "
                  "AND LENGTH(kv_value) > 0",
                  parallelism, db_name_, kv_data_table_, key_prefix_.namespace_id_,
                  start_hex, end_hex))) {
            LOG_WARN("assign scan sql failed", K(ret));
          } else {
            LOG_TRACE("[SPFRESH_TRACE] scan_cid_vector_by_center",
                     "ns_id", key_prefix_.namespace_id_,
                     "lt_id", key_prefix_.logical_table_id_,
                     "idx_seq", cid_vector_index_seq_,
                     "aux_type", static_cast<int>(LT_AUX_IVF_CID_ROWID),
                     "ctid", centroid_tablet_id_,
                     K(center_id), "start_hex", start_hex, "end_hex", end_hex,
                     "sql_len", sql.length());
            if (OB_FAIL(execute_inner_sql_with_result_(
                sql.ptr(), expected_vector_bytes, allocator, vectors))) {
              LOG_WARN("execute scan failed", K(ret), K(sql));
            }
          }
        }
        if (OB_NOT_NULL(start_hex)) { ob_free(start_hex); }
        if (OB_NOT_NULL(end_hex)) { ob_free(end_hex); }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::scan_cid_vector_by_center_limit(
    uint64_t center_id,
    const char *last_rowkey,
    int64_t last_rowkey_len,
    int64_t limit,
    int64_t dimension,
    common::ObIAllocator &allocator,
    ObIArray<ObSpfreshVectorRow> &vectors)
{
  int ret = OB_SUCCESS;
  int64_t expected_vector_bytes = 0;
  vectors.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (limit <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid limit", K(ret), K(limit));
  } else if (OB_FAIL(get_raw_vector_expected_bytes(
                 dimension, expected_vector_bytes))) {
    LOG_WARN("invalid dimension for limited cid vector scan", K(ret), K(dimension));
  } else {
    ObArenaAllocator key_allocator("SpfreshSL", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(center_id, key_allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      ObLogicalTableKeyPrefix prefix = key_prefix_;
      prefix.aux_type_ = LT_AUX_IVF_CID_ROWID;
      prefix.index_seq_ = get_index_seq_(LT_AUX_IVF_CID_ROWID);
      common::ObString start_key;
      common::ObString end_key;

      // Start key: prefix + center_id + [last_rowkey or 1]
      // Skip rowid=0 to avoid centroid metadata rows from being scanned as vectors.
      // Must call set_uint() so encode_key_column_ uses variable-length encoding
      // matching the stored key format.
      {
        uint64_t rowid_val = 0;
        if (OB_ISNULL(last_rowkey) || last_rowkey_len <= 0) {
          rowid_val = 1;
        } else if (last_rowkey_len < 8) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("last_rowkey too short for pagination", K(ret), K(last_rowkey_len));
        } else {
          uint64_t be_val = 0;
          MEMCPY(&be_val, last_rowkey, 8);
          rowid_val = ntohll(be_val) + 1;
        }
        if (OB_SUCC(ret)) {
          common::ObDatum rowid_datum;
          rowid_datum.ptr_ = reinterpret_cast<const char *>(&rowid_val);
          rowid_datum.set_uint(rowid_val);
          ObSEArray<common::ObDatum, 2> start_datums;
          if (OB_FAIL(start_datums.push_back(center_id_datum))
              || OB_FAIL(start_datums.push_back(rowid_datum))) {

            LOG_WARN("push start datums failed", K(ret));
          } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, start_datums,
                      key_allocator, start_key))) {
            LOG_WARN("encode start key failed", K(ret));
          }
        }
      }

      // End key: prefix + center_id + max_rowid
      if (OB_SUCC(ret)) {
        uint64_t rowid_max_val = UINT64_MAX;
        common::ObDatum rowid_max;
        rowid_max.ptr_ = reinterpret_cast<const char *>(&rowid_max_val);
        rowid_max.set_uint(UINT64_MAX);
        ObSEArray<common::ObDatum, 2> end_datums;
        if (OB_FAIL(end_datums.push_back(center_id_datum))
            || OB_FAIL(end_datums.push_back(rowid_max))) {

          LOG_WARN("push end datums failed", K(ret));
        } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, end_datums,
                    key_allocator, end_key))) {
          LOG_WARN("encode end key failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        static const char hex_chars[] = "0123456789ABCDEF";
        char *start_hex = static_cast<char *>(ob_malloc(start_key.length() * 2 + 1, "SpfreshSH2"));
        char *end_hex = static_cast<char *>(ob_malloc(end_key.length() * 2 + 1, "SpfreshEH2"));
        if (OB_ISNULL(start_hex) || OB_ISNULL(end_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc hex buf failed", K(ret));
        } else {
          for (int64_t i = 0; i < start_key.length(); ++i) {
            start_hex[i * 2] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) >> 4) & 0x0F];
            start_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) & 0x0F)];
          }
          start_hex[start_key.length() * 2] = '\0';
          for (int64_t i = 0; i < end_key.length(); ++i) {
            end_hex[i * 2] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) >> 4) & 0x0F];
            end_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) & 0x0F)];
          }
          end_hex[end_key.length() * 2] = '\0';

          ObSqlString sql;
          int64_t parallelism = OB_NOT_NULL(batch_conn_) ? 1 : get_scan_parallelism_();
          if (OB_FAIL(sql.assign_fmt(
                  "SELECT /*+ parallel(%ld) */ kv_key, kv_value FROM `%s`.`%s` "
                  "WHERE namespace_id = %lu AND kv_key >= X'%s' AND kv_key < X'%s' "
                  "AND LENGTH(kv_value) > 0 "
                  "ORDER BY kv_key LIMIT %ld%s",
                  parallelism, db_name_, kv_data_table_, key_prefix_.namespace_id_,
                  start_hex, end_hex, limit, OB_NOT_NULL(batch_conn_) ? " FOR UPDATE" : ""))) {
            LOG_WARN("assign scan limit sql failed", K(ret));
          } else if (OB_FAIL(execute_inner_sql_with_result_(
                         sql.ptr(), expected_vector_bytes, allocator, vectors))) {
            LOG_WARN("execute scan limit failed", K(ret), K(sql));
          }
        }
        if (OB_NOT_NULL(start_hex)) { ob_free(start_hex); }
        if (OB_NOT_NULL(end_hex)) { ob_free(end_hex); }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::delete_from_cid_vector(uint64_t center_id,
                                                  const ObIArray<ObString> &rowkeys,
                                                  SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (rowkeys.empty()) {
    // nothing to delete
  } else {
    ObArenaAllocator allocator("SpfreshDel", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      static const char hex_chars[] = "0123456789ABCDEF";
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
              "DELETE FROM `%s`.`%s` WHERE namespace_id = %lu AND kv_key IN (",
              db_name_, kv_data_table_, key_prefix_.namespace_id_))) {
        LOG_WARN("assign delete sql failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
          const ObString &rk = rowkeys.at(i);
          if (i > 0) {
            if (OB_FAIL(sql.append(", "))) {
              LOG_WARN("append comma failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            // Rowkey is in network byte order (set by set_rowkey_from_int).
            // Decode to host order, then init ObDatum with set_uint so that
            // encode_key_column_ -> get_uint() returns the correct value.
            uint64_t rk_raw = 0;
            if (rk.length() >= 8) {
              MEMCPY(&rk_raw, rk.ptr(), 8);
            }
            uint64_t rowid_val = ntohll(rk_raw);
            common::ObDatum rowid_datum;
            rowid_datum.ptr_ = reinterpret_cast<const char *>(&rowid_val);
            rowid_datum.set_uint(rowid_val);
            ObSEArray<common::ObDatum, 2> key_datums;
            if (OB_FAIL(key_datums.push_back(center_id_datum))
                || OB_FAIL(key_datums.push_back(rowid_datum))) {

              LOG_WARN("push key datums failed", K(ret));
            } else {
              common::ObString kv_key;
              if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_ROWID, key_datums, allocator, kv_key))) {
                LOG_WARN("build kv_key failed", K(ret));
              } else {
                char *key_hex = static_cast<char *>(ob_malloc(kv_key.length() * 2 + 1, "SpfreshKhD"));
                if (OB_ISNULL(key_hex)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc key hex failed", K(ret));
                } else {
                  for (int64_t j = 0; j < kv_key.length(); ++j) {
                    key_hex[j * 2] = hex_chars[
                        (static_cast<unsigned char>(kv_key.ptr()[j]) >> 4) & 0x0F];
                    key_hex[j * 2 + 1] = hex_chars[
                        static_cast<unsigned char>(kv_key.ptr()[j]) & 0x0F];
                  }
                  key_hex[kv_key.length() * 2] = '\0';
                  if (OB_FAIL(sql.append_fmt("X'%s'", key_hex))) {
                    LOG_WARN("append key hex failed", K(ret));
                  }
                  ob_free(key_hex);
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sql.append(")"))) {
            LOG_WARN("append close paren failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
          LOG_WARN("execute delete failed", K(ret), K(sql));
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::touch_cid_vector(
    uint64_t center_id,
    const common::ObString &rowkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(batch_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch transaction not started", K(ret));
  } else if (rowkey.length() < static_cast<int64_t>(sizeof(uint64_t))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey for tablet intent", K(ret), K(rowkey.length()));
  } else if (identifier_has_backtick_(db_name_)
             || identifier_has_backtick_(kv_data_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsafe identifier for tablet intent SQL", K(ret));
  } else {
    // The aux05 key format is fixed and order preserving:
    //   25B logical prefix + 16B ObCenterId + 8B rowid.
    // Build the exact point key on the stack.  This barrier also runs from
    // recovery workers, so it must not depend on an implicit MTL allocator.
    static constexpr int64_t CID_LEN = OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH;
    static constexpr int64_t KEY_LEN =
        OB_LOGICAL_TABLE_KEY_PREFIX_LEN + CID_LEN + sizeof(uint64_t);
    char key_buf[KEY_LEN];
    int64_t pos = 0;
    ObLogicalTableKeyPrefix prefix = key_prefix_;
    prefix.aux_type_ = LT_AUX_IVF_CID_ROWID;
    prefix.index_seq_ = get_index_seq_(LT_AUX_IVF_CID_ROWID);
    if (OB_FAIL(prefix.encode(key_buf, KEY_LEN, pos))) {
      LOG_WARN("encode aux05 prefix for tablet intent failed", K(ret), K(prefix));
    } else {
      const uint64_t tablet_id_be = htonll(centroid_tablet_id_);
      const uint64_t center_id_be = htonll(center_id);
      MEMCPY(key_buf + pos, &tablet_id_be, sizeof(tablet_id_be));
      pos += sizeof(tablet_id_be);
      MEMCPY(key_buf + pos, &center_id_be, sizeof(center_id_be));
      pos += sizeof(center_id_be);
      MEMCPY(key_buf + pos, rowkey.ptr(), sizeof(uint64_t));
      pos += sizeof(uint64_t);
      if (OB_UNLIKELY(KEY_LEN != pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected aux05 point key length", K(ret), K(pos), K(KEY_LEN));
      } else {
        char key_hex[KEY_LEN * 2 + 1];
        bin_to_hex_(key_buf, KEY_LEN, key_hex);
        ObSqlString sql;
        if (OB_FAIL(sql.assign_fmt(
            "SELECT kv_key FROM `%s`.`%s` "
            "WHERE namespace_id = %lu AND kv_key = X'%s' FOR UPDATE",
            db_name_, kv_data_table_, key_prefix_.namespace_id_, key_hex))) {
          LOG_WARN("build tablet intent SQL failed", K(ret));
        } else {
          // A no-op UPDATE may report zero changed rows even when it matched.
          // Lock and verify the exact source posting in the owning transaction.
          SMART_VAR(ObISQLClient::ReadResult, result) {
            common::sqlclient::ObMySQLResult *mysql_res = nullptr;
            ObString sql_str(sql.length(), sql.ptr());
            if (OB_FAIL(batch_conn_->execute_read(tenant_id_, sql_str, result))) {
              LOG_WARN("register physical tablet intent failed", K(ret), K(center_id));
            } else if (OB_ISNULL(mysql_res = result.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tablet intent SQL result is null", K(ret), K(center_id));
            } else if (OB_FAIL(mysql_res->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_ENTRY_NOT_EXIST;
                LOG_WARN("source posting missing for tablet intent", K(ret), K(center_id));
              } else {
                LOG_WARN("failed to read source posting for tablet intent", K(ret), K(center_id));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::delete_from_rowid_cid(uint64_t center_id,
                                                 const ObIArray<ObString> &rowkeys,
                                                 SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (rowkeys.empty()) {
    // nothing to delete
  } else {
    ObArenaAllocator allocator("SpfreshDR", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
              "DELETE FROM `%s`.`%s` WHERE namespace_id = %lu AND kv_key IN (",
              db_name_, kv_data_table_, key_prefix_.namespace_id_))) {
        LOG_WARN("assign delete sql failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
          const ObString &rk = rowkeys.at(i);
          if (i > 0) {
            if (OB_FAIL(sql.append(", "))) {
              LOG_WARN("append comma failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            uint64_t rk_raw = 0;
            if (rk.length() >= 8) {
              MEMCPY(&rk_raw, rk.ptr(), 8);
            }
            uint64_t rowid_val = ntohll(rk_raw);
            common::ObDatum rowid_datum;
            rowid_datum.ptr_ = reinterpret_cast<const char *>(&rowid_val);
            rowid_datum.set_uint(rowid_val);
            // Key order: [rowid][center_id] (different from aux_type=5)
            ObSEArray<common::ObDatum, 2> key_datums;
            if (OB_FAIL(key_datums.push_back(rowid_datum))
                || OB_FAIL(key_datums.push_back(center_id_datum))) {
              LOG_WARN("push key datums failed", K(ret));
            } else {
              common::ObString kv_key;
              if (OB_FAIL(build_kv_key_(LT_AUX_IVF_ROWID_CID, key_datums,
                                        allocator, kv_key))) {
                LOG_WARN("build kv_key failed", K(ret));
              } else {
                char *key_hex = static_cast<char *>(
                    ob_malloc(kv_key.length() * 2 + 1, "SpfreshKhDR"));
                if (OB_ISNULL(key_hex)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc key hex failed", K(ret));
                } else {
                  bin_to_hex_(kv_key.ptr(), kv_key.length(), key_hex);
                  if (OB_FAIL(sql.append_fmt("X'%s'", key_hex))) {
                    LOG_WARN("append key hex failed", K(ret));
                  }
                  ob_free(key_hex);
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(sql.append(")"))) {
            LOG_WARN("append close paren failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
          LOG_WARN("execute delete from rowid_cid failed", K(ret), K(sql));
        }
      }
    }
  }

  return ret;
}


int ObSpfreshTableHelper::validate_cid_vector_rows_(
    const ObIArray<ObSpfreshVectorRow> &vectors) const
{
  int ret = OB_SUCCESS;
  int64_t expected_vector_len = 0;
  if (!vectors.empty()) {
    expected_vector_len = vectors.at(0).vector_len_;
    if (expected_vector_len <= 0
        || 0 != expected_vector_len % static_cast<int64_t>(sizeof(float))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expected cid rowid vector length", K(ret), K(expected_vector_len));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
      const ObSpfreshVectorRow &row = vectors.at(i);
      if (OB_ISNULL(row.vector_) || expected_vector_len != row.vector_len_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("inconsistent cid rowid vector length",
                 K(ret), K(i), KP(row.vector_), K(row.vector_len_), K(expected_vector_len));
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::insert_to_cid_vector(uint64_t center_id,
                                                const ObIArray<ObSpfreshVectorRow> &vectors,
                                                SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (vectors.empty()) {
    // nothing to insert
  } else if (OB_FAIL(validate_cid_vector_rows_(vectors))) {
    LOG_WARN("invalid cid rowid vectors to insert", K(ret), K(vectors.count()));
  } else {
    ObArenaAllocator allocator("SpfreshInsV", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      static const char hex_chars[] = "0123456789ABCDEF";
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
              "INSERT INTO `%s`.`%s` (namespace_id, kv_key, kv_value) VALUES ",
              db_name_, kv_data_table_))) {
        LOG_WARN("assign insert sql failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
          const ObSpfreshVectorRow &row = vectors.at(i);
          if (i > 0) {
            if (OB_FAIL(sql.append(", "))) {
              LOG_WARN("append comma failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            // Rowkey is in network byte order (set by set_rowkey_from_int).
            // Decode to host order, then init ObDatum with set_uint so that
            // encode_key_column_ -> get_uint() returns the correct value.
            uint64_t rk_raw = 0;
            if (row.rowkey_len_ >= 8) {
              MEMCPY(&rk_raw, row.rowkey_, 8);
            }
            uint64_t rowid_val = ntohll(rk_raw);
            common::ObDatum rowid_datum;
            rowid_datum.ptr_ = reinterpret_cast<const char *>(&rowid_val);
            rowid_datum.set_uint(rowid_val);
            ObSEArray<common::ObDatum, 2> key_datums;
            if (OB_FAIL(key_datums.push_back(center_id_datum))
                || OB_FAIL(key_datums.push_back(rowid_datum))) {

              LOG_WARN("push key datums failed", K(ret));
            } else {
              common::ObString kv_key;
              if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_ROWID, key_datums, allocator, kv_key))) {
                LOG_WARN("build kv_key failed", K(ret));
              } else {
                common::ObString kv_value(row.vector_len_, row.vector_);
                if (OB_UNLIKELY(row.vector_len_ <= 0 || OB_ISNULL(row.vector_))) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("invalid raw cid rowid vector", K(ret), K(row.vector_len_));
                } else {
                  char *key_hex = static_cast<char *>(
                      ob_malloc(kv_key.length() * 2 + 1, "SpfreshKhI"));
                  char *val_hex = static_cast<char *>(
                      ob_malloc(kv_value.length() * 2 + 1, "SpfreshVhI"));
                  if (OB_ISNULL(key_hex) || OB_ISNULL(val_hex)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("alloc hex buf failed", K(ret));
                  } else {
                    for (int64_t j = 0; j < kv_key.length(); ++j) {
                      key_hex[j * 2] = hex_chars[
                          (static_cast<unsigned char>(kv_key.ptr()[j]) >> 4) & 0x0F];
                      key_hex[j * 2 + 1] = hex_chars[
                          static_cast<unsigned char>(kv_key.ptr()[j]) & 0x0F];
                    }
                    key_hex[kv_key.length() * 2] = '\0';
                    for (int64_t j = 0; j < kv_value.length(); ++j) {
                      val_hex[j * 2] = hex_chars[
                          (static_cast<unsigned char>(kv_value.ptr()[j]) >> 4) & 0x0F];
                      val_hex[j * 2 + 1] = hex_chars[
                          static_cast<unsigned char>(kv_value.ptr()[j]) & 0x0F];
                    }
                    val_hex[kv_value.length() * 2] = '\0';
                    if (OB_FAIL(sql.append_fmt("(%lu, X'%s', X'%s')",
                            key_prefix_.namespace_id_, key_hex, val_hex))) {
                      LOG_WARN("append values failed", K(ret));
                    }
                  }
                  if (OB_NOT_NULL(key_hex)) { ob_free(key_hex); }
                  if (OB_NOT_NULL(val_hex)) { ob_free(val_hex); }
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
          LOG_WARN("execute insert failed", K(ret), K(sql));
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::insert_to_rowid_cid(uint64_t center_id,
                                               const ObIArray<ObSpfreshVectorRow> &vectors,
                                               SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (vectors.empty()) {
    // nothing to insert
  } else {
    ObArenaAllocator allocator("SpfreshIR", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      // Pre-build center_id value (16B big-endian, same for all rows in batch)
      char cid_buf[16] = {};
      {
        uint64_t cid_be = htonll(center_id);
        MEMSET(cid_buf, 0, 8);
        MEMCPY(cid_buf + 8, &cid_be, 8);
      }
      common::ObString cid_val(16, cid_buf);
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
              "INSERT INTO `%s`.`%s` (namespace_id, kv_key, kv_value) VALUES ",
              db_name_, kv_data_table_))) {
        LOG_WARN("assign insert sql failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
          const ObSpfreshVectorRow &row = vectors.at(i);
          if (i > 0) {
            if (OB_FAIL(sql.append(", "))) {
              LOG_WARN("append comma failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            uint64_t rk_raw = 0;
            if (row.rowkey_len_ >= 8) {
              MEMCPY(&rk_raw, row.rowkey_, 8);
            }
            uint64_t rowid_val = ntohll(rk_raw);
            common::ObDatum rowid_datum;
            rowid_datum.ptr_ = reinterpret_cast<const char *>(&rowid_val);
            rowid_datum.set_uint(rowid_val);
            // Key order: [rowid][center_id] (different from aux_type=5)
            ObSEArray<common::ObDatum, 2> key_datums;
            if (OB_FAIL(key_datums.push_back(rowid_datum))
                || OB_FAIL(key_datums.push_back(center_id_datum))) {
              LOG_WARN("push key datums failed", K(ret));
            } else {
              common::ObString kv_key;
              if (OB_FAIL(build_kv_key_(LT_AUX_IVF_ROWID_CID, key_datums,
                                        allocator, kv_key))) {
                LOG_WARN("build kv_key failed", K(ret));
              } else {
                // Build value: column="center_id", value=16B big-endian center_id
                common::ObString col_names_arr[1];
                col_names_arr[0] = common::ObString::make_string("center_id");
                common::ObSEArray<common::ObString, 1> col_names;
                common::ObObjMeta metas_arr[1];
                metas_arr[0].set_varbinary();
                common::ObSEArray<common::ObObjMeta, 1> metas;
                common::ObDatum val_datums_arr[1];
                val_datums_arr[0].set_string(cid_val);
                common::ObSEArray<common::ObDatum, 1> val_datums;
                common::ObString kv_value;
                if (OB_FAIL(col_names.push_back(col_names_arr[0]))
                    || OB_FAIL(metas.push_back(metas_arr[0]))
                    || OB_FAIL(val_datums.push_back(val_datums_arr[0]))) {
                  LOG_WARN("push column metadata failed", K(ret));
                } else if (OB_FAIL(build_kv_value_(col_names, metas, val_datums,
                                                   allocator, kv_value))) {
                  LOG_WARN("build kv_value failed", K(ret));
                } else {
                  char *key_hex = static_cast<char *>(
                      ob_malloc(kv_key.length() * 2 + 1, "SpfreshKhIR"));
                  char *val_hex = static_cast<char *>(
                      ob_malloc(kv_value.length() * 2 + 1, "SpfreshVhIR"));
                  if (OB_ISNULL(key_hex) || OB_ISNULL(val_hex)) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("alloc hex buf failed", K(ret));
                  } else {
                    bin_to_hex_(kv_key.ptr(), kv_key.length(), key_hex);
                    bin_to_hex_(kv_value.ptr(), kv_value.length(), val_hex);
                    if (OB_FAIL(sql.append_fmt("(%lu, X'%s', X'%s')",
                            key_prefix_.namespace_id_, key_hex, val_hex))) {
                      LOG_WARN("append values failed", K(ret));
                    }
                  }
                  if (OB_NOT_NULL(key_hex)) { ob_free(key_hex); }
                  if (OB_NOT_NULL(val_hex)) { ob_free(val_hex); }
                }
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
          LOG_WARN("execute insert to rowid_cid failed", K(ret), K(sql));
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::update_centroid_value_cas(uint64_t center_id,
                                                     ObIvfCentroidState expected_state,
                                                     ObIvfCentroidState new_state,
                                                     int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx timeout_ctx;
  affected_rows = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(batch_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch transaction not started, call start_batch_transaction first", K(ret));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH centroid CAS", K(ret));
  } else {
    ObArenaAllocator allocator("SpfreshCV", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    ObSEArray<common::ObDatum, 1> key_datums;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else if (OB_FAIL(key_datums.push_back(center_id_datum))) {
      LOG_WARN("push key datum failed", K(ret));
    } else {
      common::ObString kv_key;
      if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, key_datums, allocator, kv_key))) {
        LOG_WARN("build kv_key failed", K(ret));
      } else {
        // Hex encode key
        static const char hex_chars[] = "0123456789ABCDEF";
        char *key_hex = static_cast<char *>(ob_malloc(kv_key.length() * 2 + 1, "SpfreshCKh"));
        if (OB_ISNULL(key_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc key hex failed", K(ret));
        } else {
          for (int64_t i = 0; i < kv_key.length(); ++i) {
            key_hex[i * 2] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) >> 4) & 0x0F];
            key_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(kv_key.ptr()[i]) & 0x0F)];
          }
          key_hex[kv_key.length() * 2] = '\0';

          // Step 1: SELECT kv_value within batch tx
          const char *old_vec_data = nullptr;
          int64_t old_vec_len = 0;
          common::ObString old_kv_value_raw;
          common::ObString old_kv_value_owned;
          bool state_matched = false;
          ObSqlString select_sql;
          if (OB_FAIL(select_sql.assign_fmt(
                  "SELECT kv_value FROM `%s`.`%s` "
                  "WHERE namespace_id = %lu AND kv_key = X'%s'%s",
                  db_name_, kv_data_table_, key_prefix_.namespace_id_, key_hex,
                  expected_state == new_state ? " FOR UPDATE" : ""))) {
            LOG_WARN("assign select sql failed", K(ret));
          } else {
            SMART_VAR(ObISQLClient::ReadResult, result) {
              common::sqlclient::ObMySQLResult *mysql_res = nullptr;
              ObString select_sql_str(select_sql.length(), select_sql.ptr());
              if (OB_FAIL(batch_conn_->execute_read(tenant_id_, select_sql_str, result))) {
                LOG_WARN("execute select failed", K(ret), K(select_sql));
              } else if (OB_ISNULL(mysql_res = result.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("mysql result is null", K(ret));
              } else if (OB_FAIL(mysql_res->next())) {
                if (OB_ITER_END == ret) {
                  ret = OB_ENTRY_NOT_EXIST;
                  LOG_WARN("centroid not found in KV", K(ret), K(center_id));
                } else {
                  LOG_WARN("mysql next failed", K(ret));
                }
              } else if (OB_FAIL(mysql_res->get_varchar("kv_value", old_kv_value_raw))) {
                LOG_WARN("get kv_value failed", K(ret));
              } else if (OB_FAIL(ob_write_string(allocator, old_kv_value_raw, old_kv_value_owned))) {
                LOG_WARN("deep copy kv_value failed", K(ret));
              } else {
                // Step 2: decode and check state
                common::ObIJsonBase *root = nullptr;
                if (OB_FAIL(ObLogicalTableDecoder::decode_value(
                        allocator, old_kv_value_owned, root))) {
                  LOG_WARN("decode kv_value failed", K(ret));
                } else if (OB_NOT_NULL(root)
                           && root->json_type() == common::ObJsonNodeType::J_OBJECT) {
                  common::ObIJsonBase *state_node = nullptr;
                  common::ObIJsonBase *vec_node = nullptr;
                  ObString state_key(centroid_state_col_name_);
                  ObString vec_key(centroid_vector_col_name_);
                  if (OB_FAIL(root->get_object_value(state_key, state_node))) {
                    LOG_WARN("center_state not in value", K(ret));
                  } else if (OB_ISNULL(state_node)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("center_state node is null", K(ret));
                  } else if (static_cast<ObIvfCentroidState>(state_node->get_int())
                             != expected_state) {
                    affected_rows = 0;
                    LOG_TRACE("[SPFRESH_TRACE] centroid state mismatch",
                             K(center_id), "current", state_node->get_int(),
                             "expected", static_cast<int>(expected_state));
                  } else if (OB_FAIL(root->get_object_value(vec_key, vec_node))) {
                    LOG_WARN("center_vector not in value", K(ret));
                  } else if (OB_ISNULL(vec_node)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("center_vector node is null", K(ret));
                  } else {
                    old_vec_data = vec_node->get_data();
                    old_vec_len = static_cast<int64_t>(vec_node->get_data_length());
                    state_matched = true;
                  }
                }
              }
            }
          }

          // A resumed REASSIGN may already have its target state. The locking
          // read above confirms that state until this transaction ends; a
          // same-value UPDATE would instead report zero changed rows.
          if (OB_SUCC(ret) && state_matched && expected_state == new_state) {
            affected_rows = 1;
          }
          // State transitions still require the conditional UPDATE's real row count.
          if (OB_SUCC(ret) && state_matched && expected_state != new_state) {
            // Build new kv_value with new state, old vector
            common::ObString col_names_arr[2];
            col_names_arr[0] = common::ObString::make_string(centroid_vector_col_name_);
            col_names_arr[1] = common::ObString::make_string(centroid_state_col_name_);
            common::ObSEArray<common::ObString, 2> col_names;
            common::ObObjMeta metas_arr[2];
            metas_arr[0].set_varbinary();
            metas_arr[1].set_int();
            common::ObSEArray<common::ObObjMeta, 2> metas;
            int64_t new_state_val = static_cast<int64_t>(new_state);
            common::ObDatum val_datums_arr[2];
            val_datums_arr[0].set_string(common::ObString(old_vec_len, old_vec_data));
            val_datums_arr[1] = common::ObDatum(
                reinterpret_cast<const char *>(&new_state_val), sizeof(int64_t), false);
            common::ObSEArray<common::ObDatum, 2> val_datums;
            if (OB_FAIL(col_names.push_back(col_names_arr[0]))
                || OB_FAIL(col_names.push_back(col_names_arr[1]))
                || OB_FAIL(metas.push_back(metas_arr[0]))
                || OB_FAIL(metas.push_back(metas_arr[1]))
                || OB_FAIL(val_datums.push_back(val_datums_arr[0]))
                || OB_FAIL(val_datums.push_back(val_datums_arr[1]))) {
              LOG_WARN("push column metadata failed", K(ret));
            } else {
              common::ObString kv_value_new;
              if (OB_FAIL(build_kv_value_(col_names, metas, val_datums, allocator,
                                          kv_value_new))) {
                LOG_WARN("build new kv_value failed", K(ret));
              } else {
                // Hex encode old and new kv_value
                char *old_hex = static_cast<char *>(
                    ob_malloc(old_kv_value_owned.length() * 2 + 1, "SpfreshCOh"));
                char *new_hex = static_cast<char *>(
                    ob_malloc(kv_value_new.length() * 2 + 1, "SpfreshCNh"));
                if (OB_ISNULL(old_hex) || OB_ISNULL(new_hex)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc hex buf failed", K(ret));
                } else {
                  for (int64_t i = 0; i < old_kv_value_owned.length(); ++i) {
                    old_hex[i * 2] = hex_chars[
                        (static_cast<unsigned char>(old_kv_value_owned.ptr()[i]) >> 4) & 0x0F];
                    old_hex[i * 2 + 1] = hex_chars[
                        static_cast<unsigned char>(old_kv_value_owned.ptr()[i]) & 0x0F];
                  }
                  old_hex[old_kv_value_owned.length() * 2] = '\0';
                  for (int64_t i = 0; i < kv_value_new.length(); ++i) {
                    new_hex[i * 2] = hex_chars[
                        (static_cast<unsigned char>(kv_value_new.ptr()[i]) >> 4) & 0x0F];
                    new_hex[i * 2 + 1] = hex_chars[
                        static_cast<unsigned char>(kv_value_new.ptr()[i]) & 0x0F];
                  }
                  new_hex[kv_value_new.length() * 2] = '\0';

                  ObSqlString update_sql;
                  if (OB_FAIL(update_sql.assign_fmt(
                          "UPDATE `%s`.`%s` SET kv_value = X'%s' "
                          "WHERE namespace_id = %lu AND kv_key = X'%s' AND kv_value = X'%s'",
                          db_name_, kv_data_table_, new_hex,
                          key_prefix_.namespace_id_, key_hex, old_hex))) {
                    LOG_WARN("assign update sql failed", K(ret));
                  } else if (OB_FAIL(execute_sql_no_tx_(update_sql.ptr(), affected_rows))) {
                    LOG_WARN("execute update failed", K(ret), K(update_sql));
                  } else if (affected_rows > 1) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("centroid CAS updated unexpected rows", K(ret), K(affected_rows), K(center_id));
                  }
                }
                if (OB_NOT_NULL(old_hex)) {
                  ob_free(old_hex);
                }
                if (OB_NOT_NULL(new_hex)) {
                  ob_free(new_hex);
                }
              }
            }
          }
          ob_free(key_hex);
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::read_centroid_state_in_tx_(uint64_t center_id,
                                                       ObIvfCentroidState &state)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SpfreshCR", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
  common::ObDatum center_id_datum;
  ObSEArray<common::ObDatum, 1> key_datums;
  common::ObString kv_key;
  if (IS_NOT_INIT || OB_ISNULL(batch_conn_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch transaction is not ready", K(ret), K(center_id));
  } else if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
    LOG_WARN("build center id datum failed", K(ret), K(center_id));
  } else if (OB_FAIL(key_datums.push_back(center_id_datum))) {
    LOG_WARN("push center id datum failed", K(ret), K(center_id));
  } else if (OB_FAIL(build_kv_key_(LT_AUX_IVF_CID_VECTOR, key_datums, allocator, kv_key))) {
    LOG_WARN("build centroid key failed", K(ret), K(center_id));
  } else if (OB_UNLIKELY(identifier_has_backtick_(db_name_)
                         || identifier_has_backtick_(kv_data_table_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("db_name or kv_data_table contains backtick, reject",
             K(ret), KCSTRING(db_name_), KCSTRING(kv_data_table_));
  } else {
    char *key_hex = static_cast<char *>(allocator.alloc(kv_key.length() * 2 + 1));
    if (OB_ISNULL(key_hex)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate centroid key hex failed", K(ret), K(center_id));
    } else {
      bin_to_hex_(kv_key.ptr(), kv_key.length(), key_hex);
      ObSqlString sql;
      if (OB_FAIL(sql.assign_fmt(
              "SELECT kv_value FROM `%s`.`%s` WHERE namespace_id = %lu AND kv_key = X'%s'",
              db_name_, kv_data_table_, key_prefix_.namespace_id_, key_hex))) {
        LOG_WARN("assign centroid state query failed", K(ret), K(center_id));
      } else {
        SMART_VAR(ObISQLClient::ReadResult, result) {
          common::sqlclient::ObMySQLResult *mysql_res = nullptr;
          ObString sql_str(sql.length(), sql.ptr());
          ObString kv_value;
          const char *vector_data = nullptr;
          int64_t vector_len = 0;
          if (OB_FAIL(batch_conn_->execute_read(tenant_id_, sql_str, result))) {
            LOG_WARN("read centroid state failed", K(ret), K(center_id));
          } else if (OB_ISNULL(mysql_res = result.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("centroid state result is null", K(ret), K(center_id));
          } else if (OB_FAIL(mysql_res->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_ENTRY_NOT_EXIST;
            }
            LOG_WARN("centroid state row not found", K(ret), K(center_id));
          } else if (OB_FAIL(mysql_res->get_varchar("kv_value", kv_value))) {
            LOG_WARN("get centroid value failed", K(ret), K(center_id));
          } else if (OB_FAIL(decode_centroid_value_(allocator, kv_value.ptr(), kv_value.length(),
                                                    vector_data, vector_len, state))) {
            LOG_WARN("decode centroid state failed", K(ret), K(center_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::rollback_centroid_state(uint64_t center_id,
                                                   ObIvfCentroidState locked_state,
                                                   ObIvfCentroidState target_state,
                                                   SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  bool target_confirmed = false;
  commit_scn = SCN::min_scn();
  if (OB_FAIL(start_batch_transaction())) {
    LOG_WARN("start rollback transaction failed", K(ret), K(center_id));
  } else {
    if (OB_FAIL(update_centroid_value_cas(center_id, locked_state,
                                          target_state, affected_rows))) {
      LOG_WARN("conditional centroid rollback failed", K(ret), K(center_id));
    } else if (1 == affected_rows) {
      target_confirmed = true;
    } else {
      ObIvfCentroidState current_state = ObIvfCentroidState::MAX_TYPE;
      if (OB_FAIL(read_centroid_state_in_tx_(center_id, current_state))) {
        LOG_WARN("confirm centroid rollback target failed", K(ret), K(center_id));
      } else if (target_state == current_state) {
        target_confirmed = true;
      } else {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("centroid changed to a third state during rollback", K(ret), K(center_id),
                 K(locked_state), K(target_state), K(current_state));
      }
    }

    int end_ret = OB_SUCCESS;
    if (OB_SUCCESS != (end_ret = end_batch_transaction(OB_SUCC(ret) && target_confirmed,
                                                        commit_scn))) {
      LOG_WARN("end rollback transaction failed", K(end_ret), K(center_id));
      if (OB_SUCC(ret)) {
        ret = end_ret;
      }
    }
  }
  return ret;
}

int ObSpfreshTableHelper::validate_raw_vector_count_result_(
    int64_t valid_count,
    int64_t invalid_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(valid_count < 0 || invalid_count < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("negative raw vector count", K(ret), K(valid_count), K(invalid_count));
  } else if (OB_UNLIKELY(invalid_count > 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("malformed non-empty raw vector payload found",
             K(ret), K(valid_count), K(invalid_count));
  }
  return ret;
}

int ObSpfreshTableHelper::count_vectors_by_center(
    uint64_t center_id,
    int64_t dimension,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  int64_t expected_vector_bytes = 0;
  int64_t local_count = 0;
  int64_t invalid_count = 0;
  ObTimeoutCtx timeout_ctx;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_FAIL(get_raw_vector_expected_bytes(
                 dimension, expected_vector_bytes))) {
    LOG_WARN("invalid dimension for cid vector count", K(ret), K(dimension));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH vector count query", K(ret));
  } else {
    ObArenaAllocator allocator("SpfreshCnt", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      // Build range for aux_type=5 + center_id
      uint64_t rowid_min_val = 0;
      uint64_t rowid_max_val = UINT64_MAX;
      common::ObDatum rowid_min(reinterpret_cast<const char *>(&rowid_min_val), sizeof(uint64_t), false);
      common::ObDatum rowid_max(reinterpret_cast<const char *>(&rowid_max_val), sizeof(uint64_t), false);
      ObLogicalTableKeyPrefix prefix = key_prefix_;
      prefix.aux_type_ = LT_AUX_IVF_CID_ROWID;
      prefix.index_seq_ = get_index_seq_(LT_AUX_IVF_CID_ROWID);

      common::ObString start_key;
      common::ObString end_key;
      {
        ObSEArray<common::ObDatum, 2> start_datums;
        if (OB_FAIL(start_datums.push_back(center_id_datum))
            || OB_FAIL(start_datums.push_back(rowid_min))) {

          LOG_WARN("push start datums failed", K(ret));
        } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, start_datums,
                    allocator, start_key))) {
          LOG_WARN("encode start key failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObSEArray<common::ObDatum, 2> end_datums;
        if (OB_FAIL(end_datums.push_back(center_id_datum))
            || OB_FAIL(end_datums.push_back(rowid_max))) {

          LOG_WARN("push end datums failed", K(ret));
        } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, end_datums,
                    allocator, end_key))) {
          LOG_WARN("encode end key failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        static const char hex_chars[] = "0123456789ABCDEF";
        char *start_hex = static_cast<char *>(ob_malloc(start_key.length() * 2 + 1, "SpfreshCH1"));
        char *end_hex = static_cast<char *>(ob_malloc(end_key.length() * 2 + 1, "SpfreshCH2"));
        if (OB_ISNULL(start_hex) || OB_ISNULL(end_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc hex buf failed", K(ret));
        } else {
          for (int64_t i = 0; i < start_key.length(); ++i) {
            start_hex[i * 2] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) >> 4) & 0x0F];
            start_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) & 0x0F)];
          }
          start_hex[start_key.length() * 2] = '\0';
          for (int64_t i = 0; i < end_key.length(); ++i) {
            end_hex[i * 2] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) >> 4) & 0x0F];
            end_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) & 0x0F)];
          }
          end_hex[end_key.length() * 2] = '\0';

          ObSqlString sql;
          SMART_VAR(ObMySQLProxy::MySQLResult, res) {
            common::sqlclient::ObMySQLResult *result = nullptr;
            if (OB_FAIL(sql.assign_fmt(
                    "SELECT "
                    "COUNT(CASE WHEN LENGTH(kv_value) = %ld THEN 1 END) AS cnt, "
                    "COUNT(CASE WHEN LENGTH(kv_value) <> %ld THEN 1 END) AS invalid_cnt "
                    "FROM `%s`.`%s` "
                    "WHERE namespace_id = %lu AND kv_key >= X'%s' AND kv_key < X'%s' "
                    "AND LENGTH(kv_value) > 0",
                    expected_vector_bytes, expected_vector_bytes,
                    db_name_, kv_data_table_, key_prefix_.namespace_id_,
                    start_hex, end_hex))) {
              LOG_WARN("assign count sql failed", K(ret));
            } else if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
              LOG_WARN("execute count failed", K(ret), K(sql));
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result is null", K(ret));
            } else if (OB_FAIL(result->next())) {
              LOG_WARN("get next row failed", K(ret));
            } else {
              EXTRACT_INT_FIELD_MYSQL(*result, "cnt", local_count, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "invalid_cnt", invalid_count, int64_t);
              if (OB_SUCC(ret) && OB_FAIL(validate_raw_vector_count_result_(
                                      local_count, invalid_count))) {
                LOG_WARN("invalid raw vector payload found while counting center",
                         K(ret), K(center_id), K(expected_vector_bytes),
                         K(local_count), K(invalid_count));
              }
            }
          }
        }
        if (OB_NOT_NULL(start_hex)) { ob_free(start_hex); }
        if (OB_NOT_NULL(end_hex)) { ob_free(end_hex); }
      }
    }
  }

  if (OB_SUCC(ret)) {
    count = local_count;
  }
  return ret;
}

int ObSpfreshTableHelper::publish_grouped_counts_(
    const common::ObIArray<std::pair<uint64_t, int64_t>> &local_counts,
    common::ObArray<std::pair<uint64_t, int64_t>> &center_counts)
{
  // ObArray::assign allocates and fills replacement storage before destroying
  // the old buffer, so allocation/copy failure leaves center_counts unchanged.
  return center_counts.assign(local_counts);
}

int ObSpfreshTableHelper::count_vectors_group_by_center(
    int64_t dimension,
    common::ObArray<std::pair<uint64_t, int64_t>> &center_counts)
{
  int ret = OB_SUCCESS;
  int64_t expected_vector_bytes = 0;
  ObTimeoutCtx timeout_ctx;
  common::ObSEArray<std::pair<uint64_t, int64_t>, 64> local_counts;
  local_counts.set_attr(ObMemAttr(tenant_id_, "SpfreshGCntOut"));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_FAIL(get_raw_vector_expected_bytes(
                 dimension, expected_vector_bytes))) {
    LOG_WARN("invalid dimension for grouped cid vector count", K(ret), K(dimension));
  } else if (OB_FAIL(set_spfresh_timeout_ctx_(timeout_ctx, false /*set_trx_timeout*/))) {
    LOG_WARN("failed to set timeout for SPFRESH grouped vector count query", K(ret));
  } else {
    ObArenaAllocator allocator("SpfreshGCnt", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum cid_min;
    common::ObDatum cid_max;
    uint64_t rowid_min_val = 0;
    uint64_t rowid_max_val = UINT64_MAX;
    common::ObDatum rowid_min(reinterpret_cast<const char *>(&rowid_min_val), sizeof(uint64_t), false);
    common::ObDatum rowid_max(reinterpret_cast<const char *>(&rowid_max_val), sizeof(uint64_t), false);
    ObLogicalTableKeyPrefix prefix = key_prefix_;
    prefix.aux_type_ = LT_AUX_IVF_CID_ROWID;
    prefix.index_seq_ = get_index_seq_(LT_AUX_IVF_CID_ROWID);

    common::ObString start_key;
    common::ObString end_key;
    if (OB_FAIL(build_center_id_datum_(0, allocator, cid_min))) {
      LOG_WARN("build min center_id datum failed", K(ret));
    } else if (OB_FAIL(build_center_id_datum_(UINT64_MAX, allocator, cid_max))) {
      LOG_WARN("build max center_id datum failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      ObSEArray<common::ObDatum, 2> start_datums;
      if (OB_FAIL(start_datums.push_back(cid_min))
          || OB_FAIL(start_datums.push_back(rowid_min))) {
        LOG_WARN("push start datums failed", K(ret));
      } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, start_datums,
                  allocator, start_key))) {
        LOG_WARN("encode start key failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<common::ObDatum, 2> end_datums;
      if (OB_FAIL(end_datums.push_back(cid_max))
          || OB_FAIL(end_datums.push_back(rowid_max))) {
        LOG_WARN("push end datums failed", K(ret));
      } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, end_datums,
                  allocator, end_key))) {
        LOG_WARN("encode end key failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      // aux_type=5 key layout (ObLogicalTableEncoder::encode_key_column):
      //   prefix(OB_LOGICAL_TABLE_KEY_PREFIX_LEN) + ObCenterId(16B) + rowid(8B)
      const int64_t min_cid_rowid_key_len =
          OB_LOGICAL_TABLE_KEY_PREFIX_LEN + common::OB_DOC_ID_COLUMN_BYTE_LENGTH
          + static_cast<int64_t>(sizeof(uint64_t));
      const int64_t key_len = start_key.length();
      if (OB_UNLIKELY(key_len != min_cid_rowid_key_len || key_len != end_key.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected encoded key length", K(ret), K(key_len), K(end_key.length()),
                 K(min_cid_rowid_key_len));
      } else {
        // ObCenterId starts right after prefix; SUBSTRING uses 1-based hex char offset.
        const int64_t cid_hex_off = OB_LOGICAL_TABLE_KEY_PREFIX_LEN * 2 + 1;
        const int64_t cid_hex_len = common::OB_DOC_ID_COLUMN_BYTE_LENGTH * 2;
        static const char hex_chars[] = "0123456789ABCDEF";
        char *start_hex = static_cast<char *>(ob_malloc(start_key.length() * 2 + 1, "SpfreshGCH1"));
        char *end_hex = static_cast<char *>(ob_malloc(end_key.length() * 2 + 1, "SpfreshGCH2"));
        if (OB_ISNULL(start_hex) || OB_ISNULL(end_hex)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc hex buf failed", K(ret));
        } else {
          for (int64_t i = 0; i < start_key.length(); ++i) {
            start_hex[i * 2] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) >> 4) & 0x0F];
            start_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(start_key.ptr()[i]) & 0x0F)];
          }
          start_hex[start_key.length() * 2] = '\0';
          for (int64_t i = 0; i < end_key.length(); ++i) {
            end_hex[i * 2] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) >> 4) & 0x0F];
            end_hex[i * 2 + 1] = hex_chars[(static_cast<unsigned char>(end_key.ptr()[i]) & 0x0F)];
          }
          end_hex[end_key.length() * 2] = '\0';

          ObSqlString sql;
          SMART_VAR(ObMySQLProxy::MySQLResult, res) {
            common::sqlclient::ObMySQLResult *result = nullptr;
            if (OB_FAIL(sql.assign_fmt(
                    "SELECT UNHEX(SUBSTRING(HEX(kv_key), %ld, %ld)) AS center_id_str, "
                    "COUNT(CASE WHEN LENGTH(kv_value) = %ld THEN 1 END) AS cnt, "
                    "COUNT(CASE WHEN LENGTH(kv_value) <> %ld THEN 1 END) AS invalid_cnt "
                    "FROM `%s`.`%s` "
                    "WHERE namespace_id = %lu AND kv_key >= X'%s' AND kv_key <= X'%s' "
                    "AND LENGTH(kv_value) > 0 "
                    "GROUP BY center_id_str",
                    cid_hex_off, cid_hex_len,
                    expected_vector_bytes, expected_vector_bytes,
                    db_name_, kv_data_table_, key_prefix_.namespace_id_,
                    start_hex, end_hex))) {
              LOG_WARN("assign group count sql failed", K(ret));
            } else {
              if (OB_FAIL(sql_proxy_->read(res, tenant_id_, sql.ptr()))) {
                LOG_WARN("execute group count failed", K(ret), K(sql));
              } else if (OB_ISNULL(result = res.get_result())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("result is null", K(ret));
              } else {
                while (OB_SUCC(ret) && OB_SUCC(result->next())) {
                  common::ObString cid_str;
                  int64_t cnt = 0;
                  int64_t invalid_count = 0;
                  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "center_id_str", cid_str);
                  EXTRACT_INT_FIELD_MYSQL(*result, "cnt", cnt, int64_t);
                  EXTRACT_INT_FIELD_MYSQL(*result, "invalid_cnt", invalid_count, int64_t);
                  if (OB_SUCC(ret) && OB_FAIL(validate_raw_vector_count_result_(
                                          cnt, invalid_count))) {
                    LOG_WARN("invalid raw vector payload found while grouping centers",
                             K(ret), K(expected_vector_bytes), K(cnt), K(invalid_count));
                  } else if (OB_SUCC(ret)) {
                    ObCenterId cent_id;
                    if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                            cent_id, cid_str, ObVectorClusterHelper::IVF_PARSE_CENTER_ID))) {
                      LOG_WARN("get_center_id_from_string failed", K(ret), K(cid_str.length()));
                    } else if (OB_FAIL(local_counts.push_back(
                                   std::make_pair(cent_id.center_id_, cnt)))) {
                      LOG_WARN("push local center count failed", K(ret),
                               K(cent_id.center_id_), K(cnt));
                    }
                  }
                }
                if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
              }
            }
          }
        }
        if (OB_NOT_NULL(start_hex)) { ob_free(start_hex); }
        if (OB_NOT_NULL(end_hex)) { ob_free(end_hex); }
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(publish_grouped_counts_(local_counts, center_counts))) {
    LOG_WARN("publish grouped count output failed", K(ret), K(local_counts.count()));
  }
  return ret;
}

// --- Rowkey-CID Operations (aux_type=6) ---

int ObSpfreshTableHelper::update_rowkey_cid(const char *rowkey,
                                             int64_t rowkey_len,
                                             uint64_t new_center_id,
                                             SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSpfreshTableHelper not init", K(ret));
  } else if (OB_ISNULL(rowkey) || rowkey_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(rowkey), K(rowkey_len));
  } else {
    ObArenaAllocator allocator("SpfreshRK", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id_);
    common::ObDatum center_id_datum;
    if (OB_FAIL(build_center_id_datum_(new_center_id, allocator, center_id_datum))) {
      LOG_WARN("build center_id datum failed", K(ret));
    } else {
      // Build rowid from rowkey
      uint64_t rk_uint = 0;
      const int64_t copy_len = (rowkey_len < 8) ? rowkey_len : 8;
      MEMCPY(&rk_uint, rowkey, static_cast<size_t>(copy_len));
      rk_uint = ntohll(rk_uint);
      uint64_t rowid_val = rk_uint;
      common::ObDatum rowid_datum(reinterpret_cast<const char *>(&rowid_val), sizeof(uint64_t), false);

      ObSEArray<common::ObDatum, 2> key_datums;
      // aux_type=6: [rowid(8B)][center_id(16B)]
      if (OB_FAIL(key_datums.push_back(rowid_datum))
          || OB_FAIL(key_datums.push_back(center_id_datum))) {

        LOG_WARN("push key datums failed", K(ret));
      } else {
        common::ObString kv_key;
        if (OB_FAIL(build_kv_key_(LT_AUX_IVF_ROWID_CID, key_datums, allocator, kv_key))) {
          LOG_WARN("build kv_key failed", K(ret));
        } else {
          // Build value: columns=[center_id]
          common::ObString col_names_arr[1];
          col_names_arr[0] = common::ObString::make_string("center_id");
          common::ObDatum val_datums_arr[1];
          char cid_buf[16] = {};
          {
            uint64_t cid_be = htonll(new_center_id);
            MEMSET(cid_buf, 0, 8);
            MEMCPY(cid_buf + 8, &cid_be, 8);
          }
          val_datums_arr[0].set_string(common::ObString(16, cid_buf));
          common::ObObjMeta metas_arr[1];
          metas_arr[0].set_varbinary();
          common::ObSEArray<common::ObString, 1> col_names;
          common::ObSEArray<common::ObObjMeta, 1> metas;
          common::ObSEArray<common::ObDatum, 1> val_datums;
          if (OB_FAIL(col_names.push_back(col_names_arr[0]))
              || OB_FAIL(metas.push_back(metas_arr[0]))
              || OB_FAIL(val_datums.push_back(val_datums_arr[0]))) {

            LOG_WARN("push arrays failed", K(ret));
          } else {
            common::ObString kv_value;
            if (OB_FAIL(build_kv_value_(col_names, metas, val_datums, allocator, kv_value))) {
              LOG_WARN("build kv_value failed", K(ret));
            } else {
              static const char hex_chars[] = "0123456789ABCDEF";
              char *key_hex = static_cast<char *>(
                  ob_malloc(kv_key.length() * 2 + 1, "SpfreshKhRK"));
              char *val_hex = static_cast<char *>(
                  ob_malloc(kv_value.length() * 2 + 1, "SpfreshVhRK"));
              if (OB_ISNULL(key_hex) || OB_ISNULL(val_hex)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc hex buf failed", K(ret));
              } else {
                for (int64_t i = 0; i < kv_key.length(); ++i) {
                  key_hex[i * 2] = hex_chars[
                      (static_cast<unsigned char>(kv_key.ptr()[i]) >> 4) & 0x0F];
                  key_hex[i * 2 + 1] = hex_chars[
                      static_cast<unsigned char>(kv_key.ptr()[i]) & 0x0F];
                }
                key_hex[kv_key.length() * 2] = '\0';
                for (int64_t i = 0; i < kv_value.length(); ++i) {
                  val_hex[i * 2] = hex_chars[
                      (static_cast<unsigned char>(kv_value.ptr()[i]) >> 4) & 0x0F];
                  val_hex[i * 2 + 1] = hex_chars[
                      static_cast<unsigned char>(kv_value.ptr()[i]) & 0x0F];
                }
                val_hex[kv_value.length() * 2] = '\0';

                // Use REPLACE INTO for upsert behavior
                ObSqlString sql;
                if (OB_FAIL(sql.assign_fmt(
                        "REPLACE INTO `%s`.`%s` (namespace_id, kv_key, kv_value) "
                        "VALUES (%lu, X'%s', X'%s')",
                        db_name_, kv_data_table_, key_prefix_.namespace_id_,
                        key_hex, val_hex))) {
                  LOG_WARN("assign replace sql failed", K(ret));
                } else if (OB_FAIL(execute_inner_sql_(sql.ptr(), commit_scn))) {
                  LOG_WARN("execute replace failed", K(ret), K(sql));
                }
              }
              if (OB_NOT_NULL(key_hex)) { ob_free(key_hex); }
              if (OB_NOT_NULL(val_hex)) { ob_free(val_hex); }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObSpfreshTableHelper::get_commit_scn_from_trans(ObMySQLTransaction &trans, SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  commit_scn = SCN::min_scn();

  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", K(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT))) {
      LOG_WARN("fail to set default timeout ctx", K(ret));
    } else if (OB_FAIL(txs->get_read_snapshot_version(
                   timeout_ctx.get_abs_timeout(), commit_scn))) {
      LOG_WARN("get read snapshot version failed", K(ret));
    }
  }

  return ret;
}

}  // namespace share
}  // namespace oceanbase
