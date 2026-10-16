/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_meta_reserved_iterator.h"

#include "lib/oblog/ob_log_module.h"
#include "observer/ob_server_struct.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_define.h"
#include "share/ob_share_util.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;

namespace storage
{
ObSharedMetaReservedIterator::ObSharedMetaReservedIterator()
  : is_inited_(false),
    opened_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    real_tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr),
    result_(nullptr),
    sql_timeout_us_(0),
    returned_row_count_(0)
{
}

ObSharedMetaReservedIterator::~ObSharedMetaReservedIterator()
{
  reset();
}

void ObSharedMetaReservedIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    mtl_free(result_);
    result_ = nullptr;
  }
}

void ObSharedMetaReservedIterator::reset()
{
  release_result_();
  is_inited_ = false;
  opened_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  real_tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = nullptr;
  sql_timeout_us_ = 0;
  returned_row_count_ = 0;
}

int ObSharedMetaReservedIterator::init(
    const uint64_t tenant_id,
    const int64_t sql_timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("shared meta reserved iterator init twice", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init get invalid argument", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(sql_timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init get invalid sql timeout", KR(ret), K(sql_timeout_us));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObSharedMetaReservedUtil::get_real_tenant_id(tenant_id, real_tenant_id_))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = GCTX.sql_proxy_;
    sql_timeout_us_ = sql_timeout_us;
    is_inited_ = true;
  }
  return ret;
}

int ObSharedMetaReservedIterator::build_select_sql_(ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ tenant_id, type, meta_scn, owner FROM %s "
      "WHERE tenant_id = %lu AND type != '%s' "
      "ORDER BY type, meta_scn",
      sql_timeout_us_,
      OB_ALL_SHARED_META_RESERVED_SCN_TNAME,
      tenant_id_,
      shared_meta_reserved_type_to_string(ObSharedMetaReservedTypeEnum::MAX_RESERVED)))) {
    LOG_WARN("fail to assign fmt", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedIterator::open_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObSqlString sql;

  if (OB_NOT_NULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator already opened", KR(ret));
  } else if (OB_ISNULL(buf = mtl_malloc(sizeof(ObMySQLProxy::ReadResult),
                                         "SMResvIter"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read result", KR(ret));
  } else if (FALSE_IT(result_ = new (buf) ObMySQLProxy::ReadResult())) {
  } else if (OB_FAIL(build_select_sql_(sql))) {
    LOG_WARN("failed to build select sql", KR(ret));
  } else if (OB_FAIL(sql_proxy_->read(*result_, real_tenant_id_, sql.ptr(),
                                       OBCG_STORAGE))) {
    LOG_WARN("failed to read sql", KR(ret), K_(real_tenant_id), K(sql));
  } else if (OB_ISNULL(result_->get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after read", KR(ret));
  } else {
    opened_ = true;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSharedMetaReservedIterator::get_next(ObSharedMetaReservedInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved iterator not init", KR(ret));
  } else if (!opened_ && OB_FAIL(open_())) {
    LOG_WARN("failed to open iterator", KR(ret));
  } else if (OB_FAIL(result_->get_result()->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next row", KR(ret));
    } else {
      LOG_TRACE("iterator reached end", KR(ret), K_(returned_row_count));
    }
  } else if (OB_FAIL(ObSharedMetaReservedOperator::parse_row_(
      *result_->get_result(), info))) {
    LOG_WARN("failed to parse row", KR(ret));
  } else {
    ++returned_row_count_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
