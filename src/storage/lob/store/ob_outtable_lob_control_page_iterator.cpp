/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"

#include "common/ob_smart_var.h"
#include "lib/utility/utility.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/lob/store/ob_lob_outtable_io.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

int ObOuttableLobControlUtil::get_lob_ctrl_macro_id(
    const blocksstable::ObDatumRow &row, blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  macro_id.reset();
  if (!row.is_valid() || 1 != row.count_ || row.storage_datums_[0].is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid projected lob control row", KR(ret), K(row));
  } else if (OB_FAIL(ObLobOutTableIO::deserialize_macro_id(
      row.storage_datums_[0].get_string(), macro_id))) {
    LOG_WARN("failed to decode lob control macro id", KR(ret));
  } else if (row.storage_datums_[0].len_ != macro_id.get_serialize_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected control macro id length", KR(ret), K(row), K(macro_id));
  }
  return ret;
}

int ObOuttableLobControlUtil::append_macro_id_literal(
    const blocksstable::MacroBlockId &macro_id,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  char buf[ObOuttableLobControlOperator::MAX_MACRO_ID_LENGTH];
  int64_t pos = 0;
  if (OB_UNLIKELY(!macro_id.is_valid() || macro_id.get_serialize_size() > sizeof(buf))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", K(ret), K(macro_id));
  } else if (OB_FAIL(macro_id.serialize(buf, sizeof(buf), pos))) {
    LOG_WARN("failed to serialize macro id", K(ret), K(macro_id));
  } else if (OB_FAIL(sql_append_hex_escape_str(ObString(pos, buf), sql))) {
    LOG_WARN("failed to append macro id", K(ret), K(macro_id));
  }
  return ret;
}

ObOuttableLobControlPageIterator::ObOuttableLobControlPageIterator()
  : is_inited_(false),
    is_finished_(false),
    sql_proxy_(nullptr),
    sql_timeout_us_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    page_size_(0),
    upper_macro_id_(),
    upper_hash_(0),
    has_cursor_(false),
    last_macro_id_(),
    last_hash_(0),
    current_page_row_count_(0),
    result_(nullptr)
{
}

ObOuttableLobControlPageIterator::~ObOuttableLobControlPageIterator()
{
  reset();
}

void ObOuttableLobControlPageIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    share::mtl_free(result_);
    result_ = nullptr;
  }
}

void ObOuttableLobControlPageIterator::reset_cursor_()
{
  has_cursor_ = false;
  last_macro_id_.reset();
  last_hash_ = 0;
}

void ObOuttableLobControlPageIterator::reset()
{
  release_result_();
  is_inited_ = false;
  is_finished_ = false;
  sql_proxy_ = nullptr;
  sql_timeout_us_ = 0;
  tenant_id_ = OB_INVALID_TENANT_ID;
  page_size_ = 0;
  upper_macro_id_.reset();
  upper_hash_ = 0;
  reset_cursor_();
  current_page_row_count_ = 0;
}

int ObOuttableLobControlPageIterator::init(
    common::ObMySQLProxy &sql_proxy,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("outtable lob control page iterator init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || sql_timeout_us <= 0
      || page_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for initializing outtable lob control page iterator",
        K(ret), K(tenant_id), K(sql_timeout_us), K(page_size));
  } else {
    sql_proxy_ = &sql_proxy;
    sql_timeout_us_ = sql_timeout_us;
    tenant_id_ = tenant_id;
    page_size_ = page_size;
    if (OB_FAIL(capture_upper_bound_())) {
      LOG_WARN("failed to capture outtable lob control upper bound", K(ret), K(tenant_id));
      reset();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObOuttableLobControlPageIterator::capture_upper_bound_()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  upper_macro_id_.reset();
  upper_hash_ = 0;
  HEAP_VAR(ObISQLClient::ReadResult, read_result) {
    ObObj macro_id_obj;
    int64_t pos = 0;
    if (OB_FAIL(sql.assign_fmt(
        "SELECT /*+ QUERY_TIMEOUT(%ld) */ macro_id, hash FROM %s "
        "ORDER BY macro_id DESC, hash DESC LIMIT 1",
        sql_timeout_us_, share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME))) {
      LOG_WARN("failed to build outtable lob control upper bound sql", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(read_result, tenant_id_, sql.ptr(), share::OBCG_STORAGE))) {
      LOG_WARN("failed to read outtable lob control upper bound", K(ret), K_(tenant_id), K(sql));
    } else if (OB_ISNULL(read_result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("outtable lob control upper bound result is null", K(ret), K_(tenant_id));
    } else if (OB_FAIL(read_result.get_result()->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_finished_ = true;
      } else {
        LOG_WARN("failed to get outtable lob control upper bound", K(ret), K_(tenant_id));
      }
    } else if (OB_FAIL(read_result.get_result()->get_obj("macro_id", macro_id_obj))) {
      LOG_WARN("failed to get upper macro id", K(ret), K_(tenant_id));
    } else if (macro_id_obj.is_null()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upper macro id is null", K(ret), K_(tenant_id));
    } else if (OB_FAIL(upper_macro_id_.deserialize(
        macro_id_obj.get_string().ptr(), macro_id_obj.get_string().length(), pos))) {
      LOG_WARN("failed to deserialize upper macro id", K(ret), K_(tenant_id));
    } else if (OB_UNLIKELY(pos != macro_id_obj.get_string().length()
        || !upper_macro_id_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid serialized upper macro id", K(ret), K_(tenant_id), K(pos),
          "length", macro_id_obj.get_string().length());
    } else if (OB_FAIL(read_result.get_result()->get_uint("hash", upper_hash_))) {
      LOG_WARN("failed to get upper hash", K(ret), K_(tenant_id));
    }
  }
  if (OB_FAIL(ret)) {
    upper_macro_id_.reset();
    upper_hash_ = 0;
  }
  return ret;
}

int ObOuttableLobControlPageIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!upper_macro_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable lob control upper bound is not initialized", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ macro_id, hash, status, "
      "last_modify_time, info FROM %s",
      sql_timeout_us_, share::OB_ALL_OUTTABLE_LOB_CONTROL_TNAME))) {
    LOG_WARN("failed to build outtable lob control select sql", K(ret));
  } else if (has_cursor_) {
    if (OB_FAIL(sql.append(" WHERE (macro_id > "))) {
      LOG_WARN("failed to append macro id cursor condition", K(ret));
    } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(last_macro_id_, sql))) {
      LOG_WARN("failed to append macro id cursor", K(ret), K_(last_macro_id));
    } else if (OB_FAIL(sql.append(" OR (macro_id = "))) {
      LOG_WARN("failed to append macro id cursor condition", K(ret));
    } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(last_macro_id_, sql))) {
      LOG_WARN("failed to append macro id cursor", K(ret), K_(last_macro_id));
    } else if (OB_FAIL(sql.append_fmt(" AND hash > %lu))", last_hash_))) {
      LOG_WARN("failed to append hash cursor", K(ret), K_(last_hash));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append(has_cursor_ ? " AND (macro_id < " : " WHERE (macro_id < "))) {
      LOG_WARN("failed to append upper macro id condition", K(ret));
    } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(upper_macro_id_, sql))) {
      LOG_WARN("failed to append upper macro id", K(ret), K_(upper_macro_id));
    } else if (OB_FAIL(sql.append(" OR (macro_id = "))) {
      LOG_WARN("failed to append upper macro id condition", K(ret));
    } else if (OB_FAIL(ObOuttableLobControlUtil::append_macro_id_literal(upper_macro_id_, sql))) {
      LOG_WARN("failed to append upper macro id", K(ret), K_(upper_macro_id));
    } else if (OB_FAIL(sql.append_fmt(" AND hash <= %lu))", upper_hash_))) {
      LOG_WARN("failed to append upper hash", K(ret), K_(upper_hash));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(" ORDER BY macro_id, hash LIMIT %ld", page_size_))) {
    LOG_WARN("failed to append order and limit", K(ret), K_(page_size));
  }
  return ret;
}

int ObOuttableLobControlPageIterator::open_next_page_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObSqlString sql;
  if (OB_NOT_NULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable lob control page is already open", K(ret));
  } else if (OB_ISNULL(buf = share::mtl_malloc(sizeof(ObMySQLProxy::ReadResult), "LobCtrlPgIt"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate read result", K(ret));
  } else if (FALSE_IT(result_ = new (buf) ObMySQLProxy::ReadResult())) {
  } else if (OB_FAIL(build_select_sql_(sql))) {
    LOG_WARN("failed to build outtable lob control page sql", K(ret));
  } else if (OB_FAIL(sql_proxy_->read(*result_, tenant_id_, sql.ptr(), share::OBCG_STORAGE))) {
    LOG_WARN("failed to read outtable lob control page", K(ret), K_(tenant_id), K(sql));
  } else if (OB_ISNULL(result_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("outtable lob control page result is null", K(ret), K_(tenant_id));
  } else {
    current_page_row_count_ = 0;
  }
  if (OB_FAIL(ret)) {
    release_result_();
  }
  return ret;
}

int ObOuttableLobControlPageIterator::parse_row_(
    common::sqlclient::ObMySQLResult &result,
    common::ObIAllocator &allocator,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  ObObj macro_id_obj;
  ObObj info_obj;
  int64_t pos = 0;
  row.reset();
  if (OB_FAIL(result.get_obj("macro_id", macro_id_obj))) {
    LOG_WARN("failed to get macro id", K(ret));
  } else if (macro_id_obj.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro id is null", K(ret));
  } else if (OB_FAIL(row.key_.macro_id_.deserialize(
      macro_id_obj.get_string().ptr(), macro_id_obj.get_string().length(), pos))) {
    LOG_WARN("failed to deserialize macro id", K(ret));
  } else if (OB_UNLIKELY(
      pos != macro_id_obj.get_string().length() || !row.key_.macro_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid serialized macro id", K(ret), K(pos),
        "length", macro_id_obj.get_string().length());
  } else if (OB_FAIL(result.get_uint("hash", row.key_.hash_))) {
    LOG_WARN("failed to get hash", K(ret));
  } else if (OB_FAIL(result.get_int("status", row.status_))) {
    LOG_WARN("failed to get status", K(ret));
  } else if (OB_FAIL(result.get_timestamp("last_modify_time", nullptr, row.last_modify_time_))) {
    LOG_WARN("failed to get last modify time", K(ret));
  } else if (OB_FAIL(result.get_obj("info", info_obj))) {
    LOG_WARN("failed to get info", K(ret));
  } else if (!info_obj.is_null() && info_obj.get_string().empty()) {
    row.info_.assign_ptr(ObOuttableLobControlOperator::EMPTY_INFO, 0);
  } else if (!info_obj.is_null()
      && OB_FAIL(ob_write_string(allocator, info_obj.get_string(), row.info_))) {
    LOG_WARN("failed to copy info", K(ret), "info_length", info_obj.get_string().length());
  } else if (OB_UNLIKELY(!row.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid outtable lob control row", K(ret), K(row));
  }
  if (OB_FAIL(ret)) {
    row.reset();
  }
  return ret;
}

int ObOuttableLobControlPageIterator::get_next(
    common::ObIAllocator &allocator,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control page iterator is not initialized", K(ret));
  } else if (is_finished_) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret)) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_ISNULL(result_) && OB_FAIL(open_next_page_())) {
        if (OB_ITER_END == ret) {
          is_finished_ = true;
        } else {
          LOG_WARN("failed to open outtable lob control page", K(ret));
        }
      } else if (OB_ISNULL(result_) || OB_ISNULL(result = result_->get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("outtable lob control page result is null", K(ret), K_(tenant_id));
        release_result_();
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          release_result_();
          is_finished_ = true;
        } else {
          LOG_WARN("failed to get next outtable lob control row", K(ret));
          release_result_();
        }
      } else if (OB_FAIL(parse_row_(*result, allocator, row))) {
        LOG_WARN("failed to parse outtable lob control row", K(ret));
        release_result_();
      } else {
        ++current_page_row_count_;
        if (OB_UNLIKELY(current_page_row_count_ > page_size_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("outtable lob control page row count exceeds page size", K(ret),
              K_(current_page_row_count), K_(page_size));
          row.reset();
          release_result_();
        } else {
          has_cursor_ = true;
          last_macro_id_ = row.key_.macro_id_;
          last_hash_ = row.key_.hash_;
          if (current_page_row_count_ == page_size_) {
            release_result_();
          }
          break;
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
