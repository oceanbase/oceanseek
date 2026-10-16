/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_ctrl_table_iterator.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

ObSSMetaCtrlTableIterator::ObSSMetaCtrlTableIterator()
  : is_inited_(false),
    opened_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    iter_type_(ObSSMetaCtrlTableIteratorType::INVALID),
    meta_key_prefix_(),
    result_(nullptr),
    operator_(),
    returned_row_count_(0)
{
}

ObSSMetaCtrlTableIterator::~ObSSMetaCtrlTableIterator()
{
  reset();
}

void ObSSMetaCtrlTableIterator::reset()
{
  release_result_();
  is_inited_ = false;
  opened_ = false;
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  iter_type_.reset();
  meta_key_prefix_.reset();
  returned_row_count_ = 0;
  operator_.reset();
}

void ObSSMetaCtrlTableIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    share::mtl_free(result_);
    result_ = nullptr;
  }
}

int ObSSMetaCtrlTableIterator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlTableIteratorType &iter_type,
    const common::ObString &meta_key_prefix)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl table iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || !iter_type.is_valid()
      || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl table iterator get invalid argument",
        KR(ret), K(tenant_id), K(ls_id), K(iter_type), K(sql_timeout_us));
  } else if (OB_FAIL(operator_.init(sql_client, sql_timeout_us))) {
    LOG_WARN("failed to init inner operator", K(ret));
  } else if (!meta_key_prefix.empty()
      && OB_FAIL(meta_key_prefix_.assign(meta_key_prefix))) {
    LOG_WARN("failed to deep copy meta key prefix", KR(ret), K(meta_key_prefix));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    iter_type_ = iter_type;
    opened_ = false;
    returned_row_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlTableIterator::build_select_sql_(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  // Note: ORDER BY meta_key gives stable iteration order, which makes
  // resumable / paginated callers easier to reason about.
  if (OB_FAIL(operator_.get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ * FROM %s "
      "WHERE tenant_id = %lu AND ls_id = %ld",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id_,
      ls_id_.id()))) {
    LOG_WARN("failed to assign fmt", KR(ret), K_(tenant_id), K_(ls_id));
  } else if (OB_FAIL(build_filter_condition_(sql))) {
    LOG_WARN("failed to bu build filter condition", KR(ret), K_(tenant_id), K_(ls_id));
  } else if (!meta_key_prefix_.empty()) {
    if (OB_FAIL(sql.append(" AND meta_key"))) {
      LOG_WARN("failed to append meta_key column ref", KR(ret));
    } else if (OB_FAIL(operator_.append_meta_key_like_prefix_(
                          meta_key_prefix_.string(), sql))) {
      LOG_WARN("failed to append like prefix", KR(ret), K_(meta_key_prefix));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append(" ORDER BY meta_key"))) {
    LOG_WARN("failed to append order by", KR(ret));
  }
  return ret;
}

int ObSSMetaCtrlTableIterator::open_()
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  void *buf = nullptr;
  ObSqlString sql;

  if (OB_NOT_NULL(result_)) {
    // already opened; should not happen
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator already opened", KR(ret));
  } else if (OB_ISNULL(buf = share::mtl_malloc(sizeof(ObMySQLProxy::ReadResult),
                                               "SSMetaCtrlIt"))) {
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
    opened_ = true;
    LOG_TRACE("ss meta ctrl table iterator opened", K_(tenant_id), K(real_tenant_id),
        K_(ls_id), K_(iter_type), K(sql));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSSMetaCtrlTableIterator::get_next(
    common::ObIAllocator &allocator,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    blocksstable::ObStorageObjectType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  return get_next(allocator, unused_gmt_create, unused_gmt_modified,
                  tenant_id, ls_id, meta_type, meta_key, meta_value);
}

int ObSSMetaCtrlTableIterator::get_next(
    common::ObIAllocator &allocator,
    int64_t &gmt_create,
    int64_t &gmt_modified,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    blocksstable::ObStorageObjectType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_type = blocksstable::ObStorageObjectType::MAX;
  meta_key.reset();
  meta_value.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table iterator do not init", KR(ret));
  } else if (!opened_ && OB_FAIL(open_())) {
    LOG_WARN("failed to open iterator", KR(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after open", KR(ret));
  } else if (OB_FAIL(operator_.parse_result_whole_row_(*result_, allocator,
                                                      gmt_create, gmt_modified,
                                                      tenant_id, ls_id, meta_type,
                                                      meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to parse result whole row", KR(ret), K_(returned_row_count));
    }
  } else {
    ++returned_row_count_;
  }
  return ret;
}

int ObSSMetaCtrlTableIterator::build_filter_condition_(
    common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  ObArray<blocksstable::ObStorageObjectType> lock_types;
  if (OB_FAIL(ObSSMetaCtrlType::get_all_lock_types(lock_types))) {
    LOG_WARN("failed to get lock types", KR(ret));
  } else if (ObSSMetaCtrlTableIteratorType::WHOLE_VALUE == iter_type_.type_) {
    //do nothing
  } else if (ObSSMetaCtrlTableIteratorType::MAJOR_SSTABLE_VALUE == iter_type_.type_) {
    if (OB_FAIL(sql.append_fmt(" AND meta_type = %ld",
        static_cast<int64_t>(blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL)))) {
      LOG_WARN("failed to append major sstable meta type", KR(ret), K(iter_type_));
    }
  } else if (ObSSMetaCtrlTableIteratorType::TABLET_LOCK_VALUE == iter_type_.type_) {
    if (OB_FAIL(sql.append_fmt(" AND meta_type = %ld",
        static_cast<int64_t>(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK)))) {
      LOG_WARN("failed to append tablet lock row meta type", KR(ret), K(iter_type_));
    }
  } else if (ObSSMetaCtrlTableIteratorType::LOCK_VALUE == iter_type_.type_) {
    if (OB_FAIL(sql.append(" AND ("))) {
      LOG_WARN("failed to append sql", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < lock_types.count(); ++i) {
      if (0 == i) {
        if (OB_FAIL(sql.append_fmt("meta_type = %ld", static_cast<int64_t>(lock_types.at(i))))) {
          LOG_WARN("failed to append meta type", KR(ret), "type", lock_types.at(i), K(iter_type_));
        }
      } else {
        if (OB_FAIL(sql.append_fmt(" OR meta_type = %ld", static_cast<int64_t>(lock_types.at(i))))) {
          LOG_WARN("failed to append meta type", KR(ret), "type", lock_types.at(i), K(iter_type_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append(")"))) {
        LOG_WARN("failed to append sql", KR(ret));
      }
    }
  } else if (ObSSMetaCtrlTableIteratorType::CTRL_VALUE == iter_type_.type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < lock_types.count(); ++i) {
      if (OB_FAIL(sql.append_fmt(" AND meta_type != %ld", static_cast<int64_t>(lock_types.at(i))))) {
        LOG_WARN("failed to append meta type", KR(ret), "type", lock_types.at(i), K(iter_type_));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("build filter condition get unexpected type", KR(ret), K(iter_type_));
  }
  return ret;
}

/******************ObSSMetaCtrlValueIterator*********************/
ObSSMetaCtrlValueIterator::ObSSMetaCtrlValueIterator()
  : is_inited_(false),
    table_iterator_()
{
}

ObSSMetaCtrlValueIterator::~ObSSMetaCtrlValueIterator()
{
}

int ObSSMetaCtrlValueIterator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObString &meta_key_prefix)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::CTRL_VALUE);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl value iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl value iterator get invalid argument",
        KR(ret), K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else if (OB_FAIL(table_iterator_.init(
      sql_client, sql_timeout_us, tenant_id, ls_id, iter_type, meta_key_prefix))) {
    LOG_WARN("failed to init inner operator", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

/******************ObSSMetaCtrlMajorSSTableValueIterator*********************/
ObSSMetaCtrlMajorSSTableValueIterator::ObSSMetaCtrlMajorSSTableValueIterator()
  : is_inited_(false),
    table_iterator_()
{
}

ObSSMetaCtrlMajorSSTableValueIterator::~ObSSMetaCtrlMajorSSTableValueIterator()
{
}

int ObSSMetaCtrlMajorSSTableValueIterator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObString &meta_key_prefix)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlTableIteratorType iter_type(
      ObSSMetaCtrlTableIteratorType::MAJOR_SSTABLE_VALUE);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl major sstable value iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl major sstable value iterator get invalid argument",
        KR(ret), K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else if (OB_FAIL(table_iterator_.init(
      sql_client, sql_timeout_us, tenant_id, ls_id, iter_type, meta_key_prefix))) {
    LOG_WARN("failed to init inner operator", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlMajorSSTableValueIterator::get_next(
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaCtrlKey &ctrl_key,
    ObSSMetaCtrlValue &ctrl_value)
{
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  return get_next(unused_gmt_create, unused_gmt_modified, tenant_id, ls_id, ctrl_key, ctrl_value);
}

int ObSSMetaCtrlMajorSSTableValueIterator::get_next(
    int64_t &gmt_create,
    int64_t &gmt_modified,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaCtrlKey &ctrl_key,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  ctrl_key.reset();
  ctrl_value.reset();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "major_value"));
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key;
  ObString meta_value;
  int64_t key_pos = 0;
  int64_t value_pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl major sstable value iterator do not init", KR(ret));
  } else if (OB_FAIL(table_iterator_.get_next(allocator, gmt_create, gmt_modified,
                                              tenant_id, ls_id, meta_type, meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next", KR(ret), K(table_iterator_));
    }
  } else if (OB_FAIL(ctrl_key.deserialize_meta_key(meta_key.ptr(), meta_key.length(), key_pos))) {
    LOG_WARN("failed to deserialize ctrl key", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ls_id, meta_type, ctrl_key))) {
    LOG_WARN("ctrl key mismatch with row columns", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (!ctrl_key.is_valid() || ctrl_key.is_lock_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctrl key is not valid or type is lock row, unexpected", KR(ret), K(ctrl_key), K(meta_key), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ctrl_value.deserialize(meta_value.ptr(), meta_value.length(), value_pos))) {
    LOG_WARN("failed to deserialize ctrl value", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id));
  } else if (!ctrl_value.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctrl value is not valid, unexpected", KR(ret), K(meta_type), K(meta_key), K(tenant_id),
        K(ls_id), K(ctrl_key), K(ctrl_value));
  }
  return ret;
}

int ObSSMetaCtrlMajorSSTableValueIterator::get_next(
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

int ObSSMetaCtrlValueIterator::get_next(
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaCtrlKey &ctrl_key,
    ObSSMetaCtrlValue &ctrl_value)
{
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  return get_next(unused_gmt_create, unused_gmt_modified, tenant_id, ls_id, ctrl_key, ctrl_value);
}

int ObSSMetaCtrlValueIterator::get_next(
    int64_t &gmt_create,
    int64_t &gmt_modified,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaCtrlKey &ctrl_key,
    ObSSMetaCtrlValue &ctrl_value)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  ctrl_key.reset();
  ctrl_value.reset();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "ctrl_value"));
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key;
  ObString meta_value;
  int64_t key_pos = 0;
  int64_t value_pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl value iterator do not init", KR(ret));
  } else if (OB_FAIL(table_iterator_.get_next(allocator, gmt_create, gmt_modified,
                                              tenant_id, ls_id, meta_type, meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next", KR(ret), K(table_iterator_));
    }
  } else if (OB_FAIL(ctrl_key.deserialize_meta_key(meta_key.ptr(), meta_key.length(), key_pos))) {
    LOG_WARN("failed to deserialize ctrl key", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ls_id, meta_type, ctrl_key))) {
    LOG_WARN("ctrl key mismatch with row columns", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id), K(ctrl_key));
  } else if (!ctrl_key.is_valid() || ctrl_key.is_lock_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctrl key is not valid or type is lock row, unexpected", KR(ret), K(ctrl_key), K(meta_key), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ctrl_value.deserialize(meta_value.ptr(), meta_value.length(), value_pos))) {
    LOG_WARN("failed to deserialize ctrl value", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id));
  } else if (!ctrl_value.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctrl value is not valid, unexpected", KR(ret), K(meta_type), K(meta_key), K(tenant_id),
        K(ls_id), K(ctrl_key), K(ctrl_value));
  }
  return ret;
}

int ObSSMetaCtrlValueIterator::get_next(
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


/******************ObSSMetaCtrlLockValueIterator*********************/
ObSSMetaCtrlLockValueIterator::ObSSMetaCtrlLockValueIterator()
  : is_inited_(false),
    table_iterator_()
{
}

ObSSMetaCtrlLockValueIterator::~ObSSMetaCtrlLockValueIterator()
{
}

int ObSSMetaCtrlLockValueIterator::init(
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObString &meta_key_prefix)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::LOCK_VALUE);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl lock value iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid()
      || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl value iterator get invalid argument",
        K(ret), K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else if (OB_FAIL(table_iterator_.init(
      sql_client, sql_timeout_us, tenant_id, ls_id, iter_type, meta_key_prefix))) {
    LOG_WARN("failed to init inner operator", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlLockValueIterator::get_next(
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaCtrlKey &meta_key,
    ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_key.reset();
  lock_row_value.reset();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "lock_value"));
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key_str;
  ObString meta_value;
  int64_t key_pos = 0;
  int64_t value_pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl lock value iterator do not init", KR(ret));
  } else if (OB_FAIL(table_iterator_.get_next(allocator, tenant_id, ls_id, meta_type, meta_key_str, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next", KR(ret), K(table_iterator_));
    }
  } else if (OB_FAIL(meta_key.deserialize_meta_key(meta_key_str.ptr(), meta_key_str.length(), key_pos))) {
    LOG_WARN("failed to deserialize meta key", KR(ret), K(meta_type), K(meta_key_str), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ls_id, meta_type, meta_key))) {
    LOG_WARN("meta key mismatch with row columns", KR(ret), K(meta_type), K(meta_key_str), K(tenant_id), K(ls_id), K(meta_key));
  } else if (!meta_key.is_valid() || !meta_key.is_lock_row()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta key is not valid or type is not lock row, unexpected", KR(ret), K(meta_key), K(meta_key_str), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(lock_row_value.deserialize(meta_value.ptr(), meta_value.length(), value_pos))) {
    LOG_WARN("failed to deserialize lock row value", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id));
  } else if (!lock_row_value.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock row value is not valid, unexpected", KR(ret), K(meta_type), K(meta_key), K(tenant_id),
        K(ls_id), K(lock_row_value));
  }
  return ret;
}

int ObSSMetaCtrlLockValueIterator::get_next(ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  uint64_t unused_tenant_id = OB_INVALID_ID;
  share::ObLSID unused_ls_id;
  ObSSMetaCtrlKey unused_meta_key;
  if (OB_FAIL(get_next(unused_tenant_id, unused_ls_id, unused_meta_key, lock_row_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next lock row value", KR(ret));
    }
  }
  return ret;
}


/******************ObSSMetaCtrlTabletLockValueIterator*********************/
ObSSMetaCtrlTabletLockValueIterator::ObSSMetaCtrlTabletLockValueIterator()
  : is_inited_(false),
    table_iterator_()
{
}

ObSSMetaCtrlTabletLockValueIterator::~ObSSMetaCtrlTabletLockValueIterator()
{
}

int ObSSMetaCtrlTabletLockValueIterator::init(
    ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObString &meta_key_prefix)
{
  int ret = OB_SUCCESS;
  const ObSSMetaCtrlTableIteratorType iter_type(ObSSMetaCtrlTableIteratorType::TABLET_LOCK_VALUE);

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl tablet lock value iterator init twice", KR(ret));
  } else if (OB_UNLIKELY((OB_INVALID_ID == tenant_id) || !ls_id.is_valid() || sql_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init ss meta ctrl tablet lock value iterator",
        KR(ret), K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else if (OB_FAIL(table_iterator_.init(
      sql_client, sql_timeout_us, tenant_id, ls_id, iter_type, meta_key_prefix))) {
    LOG_WARN("fail to init inner operator", KR(ret), K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlTabletLockValueIterator::get_next(
    uint64_t &tenant_id,
    ObLSID &ls_id,
    ObSSMetaCtrlKey &meta_key,
    ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_key.reset();
  lock_row_value.reset();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "tablet_lock"));
  blocksstable::ObStorageObjectType meta_type = blocksstable::ObStorageObjectType::MAX;
  ObString meta_key_str;
  ObString meta_value;
  int64_t key_pos = 0;
  int64_t value_pos = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl tablet lock value iterator do not init", KR(ret));
  } else if (OB_FAIL(table_iterator_.get_next(allocator, tenant_id, ls_id, meta_type, meta_key_str, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next", KR(ret), K_(table_iterator));
    }
  } else if (OB_UNLIKELY(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK != meta_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non tablet lock row meta type",
        KR(ret), K(meta_type), K(meta_key_str), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(meta_key.deserialize_meta_key(meta_key_str.ptr(), meta_key_str.length(), key_pos))) {
    LOG_WARN("failed to deserialize meta key", KR(ret), K(meta_type), K(meta_key_str), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(validate_ss_meta_ctrl_row_key(ls_id, meta_type, meta_key))) {
    LOG_WARN("meta key mismatch with row columns", KR(ret), K(meta_type), K(meta_key_str), K(tenant_id), K(ls_id), K(meta_key));
  } else if (OB_UNLIKELY(!meta_key.is_valid()
      || (blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK != meta_key.type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta key is not valid or type is not tablet lock row, unexpected",
        KR(ret), K(meta_key), K(meta_key_str), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(lock_row_value.deserialize(meta_value.ptr(), meta_value.length(), value_pos))) {
    LOG_WARN("failed to deserialize tablet lock row value", KR(ret), K(meta_type), K(meta_key), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(!lock_row_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet lock row value is not valid, unexpected", KR(ret), K(meta_type), K(meta_key), K(tenant_id),
        K(ls_id), K(lock_row_value));
  }
  return ret;
}

int ObSSMetaCtrlTabletLockValueIterator::get_next(
    ObSSMetaCtrlKey &meta_key,
    ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  uint64_t unused_tenant_id = OB_INVALID_ID;
  ObLSID unused_ls_id;
  if (OB_FAIL(get_next(unused_tenant_id, unused_ls_id, meta_key, lock_row_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next tablet lock row value", KR(ret));
    }
  }
  return ret;
}

int ObSSMetaCtrlTabletLockValueIterator::get_next(ObSSMetaCtrlLockRowValue &lock_row_value)
{
  int ret = OB_SUCCESS;
  ObSSMetaCtrlKey unused_meta_key;
  if (OB_FAIL(get_next(unused_meta_key, lock_row_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next tablet lock row value", KR(ret));
    }
  }
  return ret;
}


/******************ObSSMetaCtrlTableKeyIterator*********************/
ObSSMetaCtrlTableKeyIterator::ObSSMetaCtrlTableKeyIterator()
  : is_inited_(false),
    opened_(false),
    tenant_id_(OB_INVALID_ID),
    column_type_(INVALID_COLUMN_TYPE),
    distinct_(false),
    column_name_("SSMCKColumn"),
    extra_where_clause_(),
    result_(nullptr),
    operator_(),
    returned_row_count_(0)
{
}

ObSSMetaCtrlTableKeyIterator::~ObSSMetaCtrlTableKeyIterator()
{
  reset();
}

void ObSSMetaCtrlTableKeyIterator::reset()
{
  release_result_();
  is_inited_ = false;
  opened_ = false;
  tenant_id_ = OB_INVALID_ID;
  column_type_ = INVALID_COLUMN_TYPE;
  distinct_ = false;
  column_name_.reset();
  extra_where_clause_.reset();
  returned_row_count_ = 0;
  operator_.reset();
}

void ObSSMetaCtrlTableKeyIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    share::mtl_free(result_);
    result_ = nullptr;
  }
}

int ObSSMetaCtrlTableKeyIterator::init(
    const uint64_t tenant_id,
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us,
    const ColumnType column_type,
    const bool distinct,
    const common::ObString &column_name,
    const common::ObString &extra_where_clause)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl table key iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id
      || column_name.empty()
      || (INT_COLUMN != column_type && VARCHAR_COLUMN != column_type)
      || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl table key iterator get invalid argument",
        K(ret), K(tenant_id), K(column_type), K(column_name), K(sql_timeout_us));
  } else if (OB_FAIL(operator_.init(sql_client, sql_timeout_us))) {
    LOG_WARN("failed to init inner operator", K(ret));
  } else if (OB_FAIL(column_name_.assign(column_name))) {
    LOG_WARN("failed to deep copy column name", KR(ret), K(column_name));
  } else if (!extra_where_clause.empty() && OB_FAIL(extra_where_clause_.assign(extra_where_clause))) {
    LOG_WARN("failed to deep copy extra where clause", KR(ret), K(extra_where_clause));
  } else {
    tenant_id_ = tenant_id;
    column_type_ = column_type;
    distinct_ = distinct;
    opened_ = false;
    returned_row_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlTableKeyIterator::build_select_sql_(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_FAIL(operator_.get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ %s%.*s FROM %s",
      timeout_us,
      distinct_ ? "DISTINCT " : "",
      column_name_.length(),
      column_name_.ptr(),
      share::OB_ALL_SHARED_META_CONTROL_TNAME))) {
    LOG_WARN("failed to assign fmt", KR(ret), K_(tenant_id), "column_name", column_name_.string());
  } else if (!extra_where_clause_.empty()
      && OB_FAIL(sql.append_fmt(" WHERE %.*s",
                                extra_where_clause_.length(),
                                extra_where_clause_.ptr()))) {
    LOG_WARN("failed to append extra where clause", KR(ret), K_(extra_where_clause));
  } else if (OB_FAIL(sql.append_fmt(" ORDER BY %.*s",
                                    column_name_.length(),
                                    column_name_.ptr()))) {
    LOG_WARN("failed to append order by", KR(ret), "column_name", column_name_.string());
  }
  return ret;
}

int ObSSMetaCtrlTableKeyIterator::open_()
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  void *buf = nullptr;
  ObSqlString sql;

  if (OB_NOT_NULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator already opened", KR(ret));
  } else if (OB_ISNULL(buf = share::mtl_malloc(sizeof(ObMySQLProxy::ReadResult),
                                               "SSMetaCtrlIt"))) {
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
    opened_ = true;
    LOG_TRACE("ss meta ctrl table key iterator opened", K_(tenant_id), K(real_tenant_id),
        "column_name", column_name_.string(), K(sql));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSSMetaCtrlTableKeyIterator::get_next(int64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table key iterator do not init", KR(ret));
  } else if (OB_UNLIKELY(INT_COLUMN != column_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next(int64_t) called on non-int column iterator",
        KR(ret), K_(column_type));
  } else if (!opened_ && OB_FAIL(open_())) {
    LOG_WARN("failed to open iterator", KR(ret));
  } else if (OB_ISNULL(result_) || OB_ISNULL(result_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after open", KR(ret));
  } else if (OB_FAIL(result_->get_result()->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to next result", KR(ret), K_(returned_row_count));
    }
  } else if (OB_FAIL(result_->get_result()->get_int(column_name_.ptr(), value))) {
    LOG_WARN("failed to get column value", KR(ret), "column_name", column_name_.string());
  } else {
    ++returned_row_count_;
  }
  return ret;
}

int ObSSMetaCtrlTableKeyIterator::get_next(
    common::ObIAllocator &allocator,
    common::ObString &value)
{
  int ret = OB_SUCCESS;
  value.reset();
  ObString tmp_value;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl table key iterator do not init", KR(ret));
  } else if (OB_UNLIKELY(VARCHAR_COLUMN != column_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next(varchar) called on non-varchar column iterator",
        KR(ret), K_(column_type));
  } else if (!opened_ && OB_FAIL(open_())) {
    LOG_WARN("failed to open iterator", KR(ret));
  } else if (OB_ISNULL(result_) || OB_ISNULL(result_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after open", KR(ret));
  } else if (OB_FAIL(result_->get_result()->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to next result", KR(ret), K_(returned_row_count));
    }
  } else if (OB_FAIL(result_->get_result()->get_varchar(column_name_.ptr(), tmp_value))) {
    LOG_WARN("failed to get column value", KR(ret), "column_name", column_name_.string());
  } else if (OB_FAIL(ob_write_string(allocator, tmp_value, value))) {
    LOG_WARN("failed to deep copy column value", KR(ret), K(tmp_value));
  } else {
    ++returned_row_count_;
  }
  return ret;
}


/******************ObSSMetaCtrlTenantIterator*********************/
ObSSMetaCtrlTenantIterator::ObSSMetaCtrlTenantIterator()
  : is_inited_(false),
    is_sys_input_(false),
    sql_client_(nullptr),
    sql_timeout_us_(0),
    self_tenant_id_(OB_INVALID_ID),
    self_emitted_(false),
    key_iterator_(),
    user_key_iterator_(),
    user_iter_opened_(false)
{
}

ObSSMetaCtrlTenantIterator::~ObSSMetaCtrlTenantIterator()
{
}

void ObSSMetaCtrlTenantIterator::reset()
{
  key_iterator_.reset();
  user_key_iterator_.reset();
  is_inited_ = false;
  is_sys_input_ = false;
  sql_client_ = nullptr;
  sql_timeout_us_ = 0;
  self_tenant_id_ = OB_INVALID_ID;
  self_emitted_ = false;
  user_iter_opened_ = false;
}

int ObSSMetaCtrlTenantIterator::init(
    const uint64_t tenant_id,
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl tenant iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl tenant iterator get invalid argument", K(ret), K(tenant_id), K(sql_timeout_us));
  } else if (is_sys_tenant(tenant_id)) {
    // sys input: enumerate distinct tenant_id from the sys storage table; for
    // each meta tenant found there, drill down into that meta tenant's table to
    // enumerate the user tenants that actually have rows.
    if (OB_FAIL(key_iterator_.init(tenant_id, sql_client, sql_timeout_us,
                                   ObSSMetaCtrlTableKeyIterator::INT_COLUMN,
                                   true /*distinct*/,
                                   ObString::make_string("tenant_id")/*column name*/,
                                   ObString()/*extra where clause*/))) {
      LOG_WARN("failed to init key iterator", KR(ret), K(tenant_id));
    } else {
      is_sys_input_ = true;
      sql_client_ = &sql_client;
      sql_timeout_us_ = sql_timeout_us;
      is_inited_ = true;
    }
  } else if (is_meta_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    // meta / user input: only itself, no SQL needed.
    is_sys_input_ = false;
    self_tenant_id_ = tenant_id;
    self_emitted_ = false;
    is_inited_ = true;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected tenant type for ss meta ctrl tenant iterator", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSMetaCtrlTenantIterator::open_user_iter_(const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  // Initing the inner key iterator with the user tenant id routes its SELECT,
  // via get_real_tenant_id_ (user->meta), to the meta tenant table that
  // physically stores this user tenant's rows.
  user_key_iterator_.reset();
  if (OB_ISNULL(sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql client is null", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(user_tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a user tenant id for drill-down", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(user_key_iterator_.init(user_tenant_id, *sql_client_, sql_timeout_us_,
                                             ObSSMetaCtrlTableKeyIterator::INT_COLUMN,
                                             true /*distinct*/,
                                             ObString::make_string("tenant_id")/*column name*/,
                                             ObString()/*extra where clause*/))) {
    LOG_WARN("failed to init user key iterator", KR(ret), K(user_tenant_id));
  } else {
    user_iter_opened_ = true;
  }
  return ret;
}

int ObSSMetaCtrlTenantIterator::get_next(uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl tenant iterator do not init", KR(ret));
  } else if (!is_sys_input_) {
    // meta / user input: emit the single id exactly once.
    if (self_emitted_) {
      ret = OB_ITER_END;
    } else {
      tenant_id = self_tenant_id_;
      self_emitted_ = true;
    }
  } else {
    // sys input: two-level enumeration. Drain the inner user-tenant iterator
    // first; when it is exhausted, advance the outer sys-table iterator and emit
    // that id, opening a fresh inner iterator whenever a meta tenant shows up.
    bool got = false;
    while (OB_SUCC(ret) && !got) {
      if (user_iter_opened_) {
        int64_t user_value = 0;
        if (OB_FAIL(user_key_iterator_.get_next(user_value))) {
          if (OB_ITER_END == ret) {
            // this meta tenant's user rows are drained, back to the outer loop.
            user_key_iterator_.reset();
            user_iter_opened_ = false;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get next user tenant", KR(ret), K(user_key_iterator_));
          }
        } else {
          tenant_id = static_cast<uint64_t>(user_value);
          got = true;
        }
      } else {
        int64_t value = 0;
        if (OB_FAIL(key_iterator_.get_next(value))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next", KR(ret), K(key_iterator_));
          }
        } else {
          const uint64_t stored_tenant_id = static_cast<uint64_t>(value);
          tenant_id = stored_tenant_id;
          got = true;
          if (is_meta_tenant(stored_tenant_id)) {
            // open the drill-down iterator for the paired user tenant; its rows
            // will be emitted on subsequent calls before the outer iterator
            // advances again.
            if (OB_FAIL(open_user_iter_(gen_user_tenant_id(stored_tenant_id)))) {
              LOG_WARN("failed to open user iterator", KR(ret), K(stored_tenant_id));
            }
          }
        }
      }
    }
  }
  return ret;
}


/******************ObSSMetaCtrlLSIterator*********************/
ObSSMetaCtrlLSIterator::ObSSMetaCtrlLSIterator()
  : is_inited_(false),
    where_clause_(),
    key_iterator_()
{
}

ObSSMetaCtrlLSIterator::~ObSSMetaCtrlLSIterator()
{
}

void ObSSMetaCtrlLSIterator::reset()
{
  key_iterator_.reset();
  where_clause_.reset();
  is_inited_ = false;
}

int ObSSMetaCtrlLSIterator::init(
    const uint64_t tenant_id,
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl ls iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl ls iterator get invalid argument", K(ret), K(tenant_id), K(sql_timeout_us));
  } else if (OB_FAIL(where_clause_.assign_fmt("tenant_id = %lu and meta_type = %ld",
      tenant_id, static_cast<int64_t>(blocksstable::ObStorageObjectType::SHARED_LS_LOCK)))) {
    LOG_WARN("failed to build where clause", K(ret), K(tenant_id));
  } else if (OB_FAIL(key_iterator_.init(tenant_id, sql_client, sql_timeout_us,
                                        ObSSMetaCtrlTableKeyIterator::INT_COLUMN,
                                        true /*distinct*/,
                                        ObString::make_string("ls_id")/*column name*/,
                                        where_clause_.string()/*extra where clause*/))) {
    LOG_WARN("failed to init key iterator", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlLSIterator::get_next(share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  int64_t value = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl ls iterator do not init", KR(ret));
  } else if (OB_FAIL(key_iterator_.get_next(value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next", KR(ret), K(key_iterator_));
    }
  } else {
    ls_id = value;
  }
  return ret;
}


/******************ObSSMetaCtrlTabletIterator*********************/
ObSSMetaCtrlTabletIterator::ObSSMetaCtrlTabletIterator()
  : is_inited_(false),
    where_clause_(),
    key_iterator_()
{
}

ObSSMetaCtrlTabletIterator::~ObSSMetaCtrlTabletIterator()
{
}

void ObSSMetaCtrlTabletIterator::reset()
{
  key_iterator_.reset();
  where_clause_.reset();
  is_inited_ = false;
}

int ObSSMetaCtrlTabletIterator::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl tablet iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl tablet iterator get invalid argument",
        K(ret), K(tenant_id), K(ls_id), K(sql_timeout_us));
  } else if (OB_FAIL(where_clause_.assign_fmt(
      "tenant_id = %lu AND ls_id = %ld AND meta_type = %ld",
      tenant_id,
      ls_id.id(),
      static_cast<int64_t>(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK)))) {
    LOG_WARN("failed to build where clause", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(key_iterator_.init(tenant_id, sql_client, sql_timeout_us,
                                        ObSSMetaCtrlTableKeyIterator::VARCHAR_COLUMN,
                                        false /*distinct*/,
                                        ObString::make_string("meta_key") /*column name*/,
                                        where_clause_.string() /*extra where clause*/))) {
    LOG_WARN("failed to init key iterator", KR(ret), K(tenant_id), K(ls_id));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlTabletIterator::get_next(common::ObTabletID &tablet_id, SCN &reorg_scn)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  reorg_scn.reset();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SSTabletIt"));
  ObString meta_key_str;
  ObSSMetaCtrlKey meta_key;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl tablet iterator do not init", KR(ret));
  } else if (OB_FAIL(key_iterator_.get_next(allocator, meta_key_str))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next meta key", KR(ret), K(key_iterator_));
    }
  } else if (OB_FAIL(meta_key.deserialize_meta_key(meta_key_str.ptr(),
                                                  meta_key_str.length(),
                                                  pos))) {
    LOG_WARN("failed to deserialize meta key", KR(ret), K(meta_key_str));
  } else if (OB_UNLIKELY(!meta_key.is_lock_row() || !meta_key.is_tablet_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-tablet meta type from TABLET_LOCK_ROW filter",
        KR(ret), K(meta_key), K(meta_key_str));
  } else {
    tablet_id = meta_key.tablet_key_.tablet_id_;
    reorg_scn = meta_key.tablet_key_.reorg_scn_;
  }
  return ret;
}


/******************ObSSMetaCtrlRowkeyIterator*********************/
ObSSMetaCtrlRowkeyIterator::ObSSMetaCtrlRowkeyIterator()
  : is_inited_(false),
    opened_(false),
    tenant_id_(OB_INVALID_ID),
    result_(nullptr),
    operator_(),
    returned_row_count_(0)
{
}

ObSSMetaCtrlRowkeyIterator::~ObSSMetaCtrlRowkeyIterator()
{
  reset();
}

void ObSSMetaCtrlRowkeyIterator::reset()
{
  release_result_();
  is_inited_ = false;
  opened_ = false;
  tenant_id_ = OB_INVALID_ID;
  returned_row_count_ = 0;
  operator_.reset();
}

void ObSSMetaCtrlRowkeyIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    share::mtl_free(result_);
    result_ = nullptr;
  }
}

int ObSSMetaCtrlRowkeyIterator::init(
    const uint64_t tenant_id,
    common::ObISQLClient &sql_client,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta ctrl rowkey iterator init twice", KR(ret));
  } else if (OB_INVALID_ID == tenant_id || sql_timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta ctrl rowkey iterator get invalid argument",
        KR(ret), K(tenant_id), K(sql_timeout_us));
  } else if (OB_FAIL(operator_.init(sql_client, sql_timeout_us))) {
    LOG_WARN("failed to init inner operator", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    opened_ = false;
    returned_row_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaCtrlRowkeyIterator::build_select_sql_(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  if (OB_FAIL(operator_.get_sql_timeout_us_(timeout_us))) {
    LOG_WARN("failed to get sql timeout", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT /*+ QUERY_TIMEOUT(%ld) */ * FROM %s "
      "WHERE tenant_id = %lu ORDER BY meta_type, ls_id, meta_key",
      timeout_us,
      share::OB_ALL_SHARED_META_CONTROL_TNAME,
      tenant_id_))) {
    LOG_WARN("failed to assign fmt", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSMetaCtrlRowkeyIterator::open_()
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  void *buf = nullptr;
  ObSqlString sql;

  if (OB_NOT_NULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator already opened", KR(ret));
  } else if (OB_ISNULL(buf = share::mtl_malloc(sizeof(ObMySQLProxy::ReadResult),
                                               "SSMetaCtrlRKIt"))) {
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
    opened_ = true;
    LOG_TRACE("ss meta ctrl rowkey iterator opened", K_(tenant_id), K(real_tenant_id), K(sql));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSSMetaCtrlRowkeyIterator::get_next(
    common::ObIAllocator &allocator,
    uint64_t &tenant_id,
    blocksstable::ObStorageObjectType &meta_type,
    share::ObLSID &ls_id,
    common::ObString &meta_key)
{
  int64_t unused_gmt_create = 0;
  int64_t unused_gmt_modified = 0;
  ObString unused_meta_value;
  return get_next(allocator, unused_gmt_create, unused_gmt_modified,
                  tenant_id, meta_type, ls_id, meta_key, unused_meta_value);
}

int ObSSMetaCtrlRowkeyIterator::get_next(
    common::ObIAllocator &allocator,
    int64_t &gmt_create,
    int64_t &gmt_modified,
    uint64_t &tenant_id,
    blocksstable::ObStorageObjectType &meta_type,
    share::ObLSID &ls_id,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  gmt_create = 0;
  gmt_modified = 0;
  tenant_id = OB_INVALID_ID;
  meta_type = blocksstable::ObStorageObjectType::MAX;
  ls_id.reset();
  meta_key.reset();
  meta_value.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl rowkey iterator do not init", KR(ret));
  } else if (!opened_ && OB_FAIL(open_())) {
    LOG_WARN("failed to open iterator", KR(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after open", KR(ret));
  } else if (OB_FAIL(operator_.parse_result_whole_row_(*result_, allocator,
                                                       gmt_create, gmt_modified,
                                                       tenant_id, ls_id, meta_type,
                                                       meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to parse result whole row", KR(ret), K_(returned_row_count));
    }
  } else {
    ++returned_row_count_;
  }
  return ret;
}

int ObSSMetaCtrlRowkeyIterator::get_next()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta ctrl rowkey iterator do not init", KR(ret));
  } else if (!opened_ && OB_FAIL(open_())) {
    LOG_WARN("failed to open iterator", KR(ret));
  } else if (OB_ISNULL(result_) || OB_ISNULL(result_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after open", KR(ret));
  } else if (OB_FAIL(result_->get_result()->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to advance rowkey iterator", KR(ret), K_(returned_row_count));
    }
  } else {
    ++returned_row_count_;
  }
  return ret;
}
