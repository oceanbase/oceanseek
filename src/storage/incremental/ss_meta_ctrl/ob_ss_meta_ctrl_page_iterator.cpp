/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_page_iterator.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

ObSSMetaCtrlPageScanParam::ObSSMetaCtrlPageScanParam()
  : scan_mode_(ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES),
    page_size_(0),
    single_meta_type_(blocksstable::ObStorageObjectType::MAX),
    meta_key_prefix_()
{
}

void ObSSMetaCtrlPageScanParam::reset()
{
  scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  page_size_ = 0;
  single_meta_type_ = blocksstable::ObStorageObjectType::MAX;
  meta_key_prefix_.reset();
}

ObSSMetaCtrlPageIterator::ObSSMetaCtrlPageIterator()
  : is_inited_(false),
    is_finished_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    scan_mode_(ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES),
    page_size_(0),
    meta_key_prefix_(),
    ctrl_types_(),
    meta_type_idx_(0),
    has_meta_key_cursor_(false),
    last_meta_key_(),
    page_buffer_(),
    page_pos_(0),
    returned_row_count_(0),
    fetched_page_count_(0),
    page_allocator_(ObMemAttr(MTL_ID(), "SSMCPgIt")),
    operator_(),
    result_(nullptr)
{
}

ObSSMetaCtrlPageIterator::~ObSSMetaCtrlPageIterator()
{
  reset();
}

void ObSSMetaCtrlPageIterator::reset()
{
  release_result_();
  is_inited_ = false;
  is_finished_ = false;
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  scan_mode_ = ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES;
  page_size_ = 0;
  meta_key_prefix_.reset();
  ctrl_types_.reset();
  meta_type_idx_ = 0;
  reset_key_cursor_();
  page_buffer_.reset();
  page_pos_ = 0;
  returned_row_count_ = 0;
  fetched_page_count_ = 0;
  page_allocator_.reuse();
  operator_.reset();
}

void ObSSMetaCtrlPageIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    share::mtl_free(result_);
    result_ = nullptr;
  }
}

void ObSSMetaCtrlPageIterator::reset_key_cursor_()
{
  has_meta_key_cursor_ = false;
  last_meta_key_.reset();
}

bool ObSSMetaCtrlPageIterator::is_lock_row_scan_mode_() const
{
  return ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE == scan_mode_;
}

int ObSSMetaCtrlPageIterator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlPageScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl page iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || scan_param.page_size_ <= 0
      || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl page iterator get invalid argument",
        KR(ret), K(tenant_id), K(ls_id), K(scan_param), K(sql_timeout_us));
  } else if (ObSSMetaCtrlPageScanMode::SINGLE_META_TYPE == scan_param.scan_mode_
      && (!is_valid_ss_meta_ctrl_type(scan_param.single_meta_type_)
          || is_ss_meta_ctrl_lock_type(scan_param.single_meta_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid single meta type for page iterator", KR(ret), K(scan_param));
  } else if (ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE == scan_param.scan_mode_
      && (!is_valid_ss_meta_ctrl_type(scan_param.single_meta_type_)
          || !is_ss_meta_ctrl_lock_type(scan_param.single_meta_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid single lock meta type for page iterator", KR(ret), K(scan_param));
  } else if (OB_FAIL(operator_.init(sql_client, sql_timeout_us))) {
    LOG_WARN("failed to init table operator", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    scan_mode_ = scan_param.scan_mode_;
    page_size_ = scan_param.page_size_;
    meta_type_idx_ = 0;
    reset_key_cursor_();
    page_buffer_.reset();
    page_pos_ = 0;
    returned_row_count_ = 0;
    fetched_page_count_ = 0;
    is_finished_ = false;
    ctrl_types_.reset();
    if (ObSSMetaCtrlPageScanMode::ALL_CTRL_VALUE_TYPES == scan_mode_) {
      if (OB_FAIL(ObSSMetaCtrlType::get_all_ctrl_value_types(ctrl_types_))) {
        LOG_WARN("failed to get all ctrl value types", KR(ret));
      }
    } else if (OB_FAIL(ctrl_types_.push_back(scan_param.single_meta_type_))) {
      LOG_WARN("failed to push back single meta type", KR(ret), K(scan_param));
    }
    if (OB_SUCC(ret) && !scan_param.meta_key_prefix_.empty()
        && OB_FAIL(meta_key_prefix_.assign(scan_param.meta_key_prefix_.ptr(),
                                            scan_param.meta_key_prefix_.length()))) {
      LOG_WARN("failed to deep copy meta key prefix", KR(ret), K(scan_param));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::build_select_sql_(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  const blocksstable::ObStorageObjectType cur_meta_type = ctrl_types_.at(meta_type_idx_);
  if (OB_FAIL(operator_.get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ * FROM %s "
      "WHERE tenant_id = %lu AND ls_id = %ld AND meta_type = %ld",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id_,
      ls_id_.id(),
      static_cast<int64_t>(cur_meta_type)))) {
    LOG_WARN("failed to assign select sql", KR(ret), K_(tenant_id), K_(ls_id), K(cur_meta_type));
  } else if (has_meta_key_cursor_ && !last_meta_key_.empty()) {
    const ObString last_meta_key(last_meta_key_.length(), last_meta_key_.ptr());
    if (OB_FAIL(operator_.append_meta_key_gt_(last_meta_key, sql))) {
      LOG_WARN("failed to append meta key gt", KR(ret), K(last_meta_key));
    }
  }
  if (OB_SUCC(ret) && !meta_key_prefix_.empty()) {
    if (OB_FAIL(sql.append(" AND meta_key"))) {
      LOG_WARN("failed to append meta_key column ref", KR(ret));
    } else if (OB_FAIL(operator_.append_meta_key_like_prefix_(meta_key_prefix_.string(), sql))) {
      LOG_WARN("failed to append meta key like prefix", KR(ret), K_(meta_key_prefix));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(" ORDER BY meta_key LIMIT %ld", page_size_))) {
    LOG_WARN("failed to append order by limit", KR(ret), K_(page_size));
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::parse_ctrl_row_(
    common::ObMySQLProxy::MySQLResult &result,
    common::ObIAllocator &allocator,
    PageRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  uint64_t row_tenant_id = OB_INVALID_ID;
  share::ObLSID row_ls_id;
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key;
  ObString meta_value;
  int64_t key_pos = 0;
  int64_t value_pos = 0;
  if (OB_FAIL(operator_.parse_result_whole_row_(result, allocator,
          unused_gmt_create, unused_gmt_modified,
          row_tenant_id, row_ls_id, meta_type, meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to parse result whole row", KR(ret));
    }
  } else if (OB_FAIL(row.ctrl_key_.deserialize_meta_key(meta_key.ptr(), meta_key.length(), key_pos))) {
    LOG_WARN("failed to deserialize ctrl key", KR(ret), K(meta_type), K(meta_key));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(row_ls_id, meta_type, row.ctrl_key_))) {
    LOG_WARN("ctrl key mismatch with row columns", KR(ret), K(meta_type), K(meta_key), K(row_ls_id), K(row.ctrl_key_));
  } else if (!row.ctrl_key_.is_valid() || row.ctrl_key_.is_lock_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctrl key is not valid or type is lock row, unexpected", KR(ret),
        K(row.ctrl_key_), K(meta_key), K(meta_type));
  } else if (OB_FAIL(row.ctrl_value_.deserialize(meta_value.ptr(), meta_value.length(), value_pos))) {
    LOG_WARN("failed to deserialize ctrl value", KR(ret), K(meta_type), K(meta_key));
  } else if (!row.ctrl_value_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctrl value is not valid, unexpected", KR(ret),
        K(meta_type), K(meta_key), K(row.ctrl_key_), K(row.ctrl_value_));
  } else if (OB_FAIL(ob_write_string(allocator, meta_key, row.meta_key_))) {
    LOG_WARN("failed to deep copy meta key", KR(ret), K(meta_key));
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::parse_lock_row_(
    common::ObMySQLProxy::MySQLResult &result,
    common::ObIAllocator &allocator,
    PageRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  uint64_t row_tenant_id = OB_INVALID_ID;
  share::ObLSID row_ls_id;
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key;
  ObString meta_value;
  int64_t key_pos = 0;
  int64_t value_pos = 0;
  if (OB_FAIL(operator_.parse_result_whole_row_(result, allocator,
          unused_gmt_create, unused_gmt_modified,
          row_tenant_id, row_ls_id, meta_type, meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to parse result whole row", KR(ret));
    }
  } else if (OB_FAIL(row.ctrl_key_.deserialize_meta_key(meta_key.ptr(), meta_key.length(), key_pos))) {
    LOG_WARN("failed to deserialize lock key", KR(ret), K(meta_type), K(meta_key));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(row_ls_id, meta_type, row.ctrl_key_))) {
    LOG_WARN("lock key mismatch with row columns", KR(ret), K(meta_type), K(meta_key), K(row_ls_id), K(row.ctrl_key_));
  } else if (!row.ctrl_key_.is_valid() || !row.ctrl_key_.is_lock_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock key is not valid or type is not lock row, unexpected", KR(ret),
        K(row.ctrl_key_), K(meta_key), K(meta_type));
  } else if (OB_FAIL(row.lock_row_value_.deserialize(meta_value.ptr(), meta_value.length(), value_pos))) {
    LOG_WARN("failed to deserialize lock row value", KR(ret), K(meta_type), K(meta_key));
  } else if (!row.lock_row_value_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row value is not valid, unexpected", KR(ret),
        K(meta_type), K(meta_key), K(row.ctrl_key_), K(row.lock_row_value_));
  } else if (OB_FAIL(ob_write_string(allocator, meta_key, row.meta_key_))) {
    LOG_WARN("failed to deep copy meta key", KR(ret), K(meta_key));
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::fetch_one_page_from_table_()
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  void *buf = nullptr;
  ObSqlString sql;
  page_buffer_.reset();
  page_pos_ = 0;
  page_allocator_.reuse();
  release_result_();
  if (meta_type_idx_ >= ctrl_types_.count()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(buf = share::mtl_malloc(sizeof(ObMySQLProxy::ReadResult), "SSMCPgIt"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read result", KR(ret));
  } else if (FALSE_IT(result_ = new (buf) ObMySQLProxy::ReadResult())) {
  } else if (OB_FAIL(operator_.get_real_tenant_id_(tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(build_select_sql_(sql))) {
    LOG_WARN("failed to build select sql", KR(ret));
  } else if (OB_FAIL(operator_.do_sql_read_(real_tenant_id, sql.ptr(), *result_))) {
    LOG_WARN("failed to execute select sql", KR(ret), K(real_tenant_id), K(sql));
  } else {
    while (OB_SUCC(ret) && page_buffer_.count() < page_size_) {
      PageRow row;
      if (is_lock_row_scan_mode_()) {
        ret = parse_lock_row_(*result_, page_allocator_, row);
      } else {
        ret = parse_ctrl_row_(*result_, page_allocator_, row);
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to parse page row", KR(ret), K_(tenant_id), K_(ls_id),
            "meta_type", ctrl_types_.at(meta_type_idx_), K_(scan_mode));
      } else if (OB_FAIL(page_buffer_.push_back(row))) {
        LOG_WARN("failed to push back page row", KR(ret));
      }
    }
    release_result_();
    if (OB_SUCC(ret)) {
      if (page_buffer_.empty()) {
        ret = OB_ITER_END;
      } else {
        ++fetched_page_count_;
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::refill_page_buffer_()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !is_finished_) {
    if (meta_type_idx_ >= ctrl_types_.count()) {
      is_finished_ = true;
      ret = OB_ITER_END;
    } else if (OB_FAIL(fetch_one_page_from_table_())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        ++meta_type_idx_;
        reset_key_cursor_();
      } else {
        LOG_WARN("failed to fetch one page from storage", KR(ret), K_(meta_type_idx));
      }
    } else if (!page_buffer_.empty()) {
      page_pos_ = 0;
      break;
    }
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::consume_next_page_row_(PageRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl page iterator do not init", KR(ret));
  } else if (is_finished_ && page_pos_ >= page_buffer_.count()) {
    ret = OB_ITER_END;
  } else {
    while (OB_SUCC(ret)) {
      if (page_pos_ < page_buffer_.count()) {
        if (OB_FAIL(row.assign(page_buffer_.at(page_pos_)))) {
          LOG_WARN("failed to assign page row", KR(ret), K_(page_pos));
        } else if (OB_FAIL(last_meta_key_.assign(row.meta_key_.ptr(), row.meta_key_.length()))) {
          LOG_WARN("failed to update last meta key cursor", KR(ret), K(row));
        } else {
          has_meta_key_cursor_ = true;
          ++page_pos_;
          ++returned_row_count_;
          break;
        }
      } else if (OB_FAIL(refill_page_buffer_())) {
        if (OB_ITER_END == ret) {
          is_finished_ = true;
        } else {
          LOG_WARN("failed to refill page buffer", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::get_next(
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaCtrlKey &ctrl_key,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  ctrl_key.reset();
  ctrl_value.reset();
  if (is_lock_row_scan_mode_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next(ctrl) called on lock row page iterator", KR(ret), K_(scan_mode));
  } else {
    PageRow row;
    if (OB_FAIL(consume_next_page_row_(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to consume next page row", KR(ret));
      }
    } else if (OB_FAIL(ctrl_key.assign(row.ctrl_key_))) {
      LOG_WARN("failed to assign ctrl key", KR(ret));
    } else {
      ctrl_value = row.ctrl_value_;
      tenant_id = tenant_id_;
      ls_id = ls_id_;
    }
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::get_next(
    ObSSMetaCtrlKey &ctrl_key,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  uint64_t unused_tenant_id = OB_INVALID_ID;
  share::ObLSID unused_ls_id;
  if (OB_FAIL(get_next(unused_tenant_id, unused_ls_id, ctrl_key, ctrl_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next ctrl key and value", KR(ret));
    }
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::get_next(
    ObSSMetaCtrlKey &lock_key,
    ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  lock_key.reset();
  lock_row_value.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl page iterator do not init", KR(ret));
  } else if (!is_lock_row_scan_mode_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next(lock) called on non-lock page iterator", KR(ret), K_(scan_mode));
  } else {
    PageRow row;
    if (OB_FAIL(consume_next_page_row_(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to consume next lock page row", KR(ret));
      }
    } else if (OB_FAIL(lock_key.assign(row.ctrl_key_))) {
      LOG_WARN("failed to assign lock key", KR(ret));
    } else {
      lock_row_value = row.lock_row_value_;
    }
  }
  return ret;
}

int ObSSMetaCtrlPageIterator::get_next_tablet(
    common::ObTabletID &tablet_id,
    share::SCN &reorg_scn)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  reorg_scn.reset();
  ObSSMetaCtrlKey lock_key;
  ObSSMetaCtrlLockRowValue lock_row_value;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl page iterator do not init", KR(ret));
  } else if (!is_lock_row_scan_mode_()
      || 1 != ctrl_types_.count()
      || blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK != ctrl_types_.at(0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next_tablet called on invalid page iterator mode", KR(ret),
        K_(scan_mode), K_(ctrl_types));
  } else if (OB_FAIL(get_next(lock_key, lock_row_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next tablet lock row", KR(ret));
    }
  } else if (OB_UNLIKELY(!lock_key.is_lock_row() || !lock_key.is_tablet_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-tablet lock row from TABLET_LOCK_ROW filter",
        KR(ret), K(lock_key));
  } else {
    tablet_id = lock_key.tablet_key_.tablet_id_;
    reorg_scn = lock_key.tablet_key_.reorg_scn_;
  }
  return ret;
}
