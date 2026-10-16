/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_table_iterator.h"
#include "ob_ss_meta_info_util.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/share/ob_shared_meta_util.h"
#include "storage/blocksstable/ob_ss_obj_util.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

/******************ObSSMetaBaseIterator*********************/

ObSSMetaBaseIterator::ObSSMetaBaseIterator()
  : version_filter_(),
    last_logical_prefix_(),
    is_inited_(false),
    opened_(false),
    iter_end_(false),
    has_cursor_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    meta_type_(),
    meta_key_prefix_(),
    result_(nullptr),
    operator_(),
    returned_row_count_(0),
    current_page_row_count_(0),
    page_size_(DEFAULT_PAGE_SIZE),
    last_meta_key_(),
    last_ls_id_(),
    last_part_(0)
{
}

ObSSMetaBaseIterator::~ObSSMetaBaseIterator()
{
  reset();
}

void ObSSMetaBaseIterator::reset()
{
  release_result_();
  is_inited_ = false;
  opened_ = false;
  iter_end_ = false;
  has_cursor_ = false;
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  meta_type_.reset();
  meta_key_prefix_.reset();
  end_meta_key_.reset();
  returned_row_count_ = 0;
  current_page_row_count_ = 0;
  page_size_ = DEFAULT_PAGE_SIZE;
  last_meta_key_.reset();
  last_ls_id_.reset();
  last_part_ = 0;
  operator_.reset();
  version_filter_.reset();
  last_logical_prefix_.reset();
}

int ObSSMetaBaseIterator::set_page_size(const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(page_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss meta iterator page size", K(ret), K(page_size));
  } else if (OB_UNLIKELY(opened_ || OB_NOT_NULL(result_))) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("cannot change page size while iterator page is open",
        K(ret), K(page_size), K_(opened), KP_(result));
  } else {
    page_size_ = page_size;
  }
  return ret;
}

void ObSSMetaBaseIterator::close_current_page()
{
  release_result_();
  opened_ = false;
  current_page_row_count_ = 0;
}

void ObSSMetaBaseIterator::release_result_()
{
  if (OB_NOT_NULL(result_)) {
    result_->~ReadResult();
    share::mtl_free(result_);
    result_ = nullptr;
  }
}

int ObSSMetaBaseIterator::init(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const common::ObString &meta_key_prefix,
    const ObSSMetaVersionFilter &version_filter)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta base iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id
            || !version_filter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta base iterator get invalid argument",
             K(ret), K(tenant_id), K(ls_id), K(meta_type), K(version_filter));
  } else if (OB_FAIL(operator_.init(sql_client, get_table_name_()))) {
    LOG_WARN("failed to init inner operator", K(ret));
  } else if (operator_.is_meta_type_in_rowkey() && !meta_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta base iterator get invalid argument",
             K(ret), K(tenant_id), K(ls_id), K(meta_type), K(version_filter));
  } else if (!meta_key_prefix.empty() && OB_FAIL(meta_key_prefix_.assign(meta_key_prefix))) {
    LOG_WARN("failed to deep copy meta key prefix", K(ret), K(meta_key_prefix));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    meta_type_ = meta_type;
    version_filter_ = version_filter;
    opened_ = false;
    iter_end_ = false;
    has_cursor_ = false;
    returned_row_count_ = 0;
    current_page_row_count_ = 0;
    last_meta_key_.reset();
    last_ls_id_.reset();
    last_part_ = 0;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && !is_inited_) {
    operator_.reset();
  }
  return ret;
}

int ObSSMetaBaseIterator::init(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const common::ObString &start_meta_key,
    const common::ObString &end_meta_key,
    const ObSSMetaVersionFilter &version_filter)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss meta base iterator init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id
            || !version_filter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta base iterator get invalid argument",
             K(ret), K(tenant_id), K(ls_id), K(meta_type), K(version_filter));
  } else if (OB_FAIL(operator_.init(sql_client, get_table_name_()))) {
    LOG_WARN("failed to init inner operator", K(ret));
  } else if (operator_.is_meta_type_in_rowkey() && !meta_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss meta base iterator get invalid argument",
             K(ret), K(tenant_id), K(ls_id), K(meta_type), K(version_filter));
  } else if (!start_meta_key.empty() && OB_FAIL(meta_key_prefix_.assign(start_meta_key))) {
    LOG_WARN("failed to deep copy meta key start prefix", K(ret), K(start_meta_key));
  } else if (!end_meta_key.empty() && OB_FAIL(end_meta_key_.assign(end_meta_key))) {
    LOG_WARN("failed to deep copy meta key end prefix", K(ret), K(end_meta_key));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    meta_type_ = meta_type;
    version_filter_ = version_filter;
    opened_ = false;
    iter_end_ = false;
    has_cursor_ = false;
    returned_row_count_ = 0;
    current_page_row_count_ = 0;
    last_meta_key_.reset();
    last_ls_id_.reset();
    last_part_ = 0;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && !is_inited_) {
    operator_.reset();
  }
  return ret;
}

int64_t ObSSMetaBaseIterator::get_next_page_limit_() const
{
  int64_t limit = page_size_;
  if (version_filter_.has_limit()) {
    const int64_t remain = version_filter_.get_limit() - returned_row_count_;
    if (remain < limit) {
      limit = remain;
    }
  }
  return limit;
}

int ObSSMetaBaseIterator::build_select_sql_(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const int64_t timeout_us = operator_.get_sql_timeout_us_(tenant_id_);
  const bool is_desc = version_filter_.is_desc_order();
  const char *cmp_op = is_desc ? "<" : ">";
  const char *order = is_desc ? "DESC" : "ASC";
  const int64_t page_limit = get_next_page_limit_();

  if (OB_FAIL(sql.assign_fmt(
      "SELECT /*+ QUERY_TIMEOUT(%ld) */ * FROM %s "
      "WHERE tenant_id = %lu",
      timeout_us,
      get_table_name_(),
      tenant_id_))) {
    LOG_WARN("failed to assign fmt", K(ret), K_(tenant_id));
  } else if (ls_id_.is_valid() && OB_FAIL(sql.append_fmt(" AND ls_id = %ld", ls_id_.id()))) {
    LOG_WARN("failed to append ls_id filter", K(ret), K_(ls_id));
  } else if (meta_type_.is_valid() && OB_FAIL(sql.append_fmt(" AND meta_type = %ld", meta_type_.get_type_val()))) {
    LOG_WARN("failed to append meta_type filter", K(ret), K_(meta_type));
  } else if (!meta_key_prefix_.empty() && !end_meta_key_.empty()) {
    // end_meta_key_补充'\xff'，使得类似scn00100.T20这样的格式能被完整迭代到
    // 因为end_meta_key_是没有带'.T20'这样的后缀的
    if (OB_FAIL(sql.append_fmt(" AND meta_key >= '%s' AND meta_key <= '%s\xff'",
                               meta_key_prefix_.ptr(), end_meta_key_.ptr()))) {
      LOG_WARN("failed to append range filter", K(ret), K_(meta_key_prefix), K_(end_meta_key));
    }
  } else if (!meta_key_prefix_.empty()) {
    ObSqlString escaped_prefix;
    if (OB_FAIL(ObSharedMetaUtil::escape_str_for_like(
        meta_key_prefix_.string(), escaped_prefix))) {
      LOG_WARN("failed to escape meta key prefix", K(ret), K_(meta_key_prefix));
    } else if (OB_FAIL(sql.append_fmt(
        " AND meta_key LIKE '%s%%' ESCAPE '!'", escaped_prefix.ptr()))) {
      LOG_WARN("failed to append like prefix", K(ret), K_(meta_key_prefix));
    }
  }

  if (OB_SUCC(ret) && has_cursor_) {
    // 游标续查：下一页须从最后一条原始行之后继续。
    // support_part_column 表同一 meta_key 有多 part 行，且 ORDER BY 固定为
    // "..., part ASC"，因此无论 meta_key 升降序，同一 meta_key 内的 part 续查
    // 永远是 part > last_part_；否则翻页游标只比 meta_key 会丢弃未读完的 part。
    if (ls_id_.is_valid()) { //迭代时 ls_id固定，只需要比较meta_key
      if (operator_.support_part_column()) {
        if (OB_FAIL(sql.append_fmt(" AND (meta_key %s '%.*s' OR (meta_key = '%.*s' AND part > %ld))",
            cmp_op, last_meta_key_.length(), last_meta_key_.ptr(),
            last_meta_key_.length(), last_meta_key_.ptr(), last_part_))) {
          LOG_WARN("failed to append meta key+part cursor", K(ret), K_(last_meta_key), K_(last_part));
        }
      } else if (OB_FAIL(sql.append_fmt(" AND meta_key %s '%.*s'",
            cmp_op, last_meta_key_.length(), last_meta_key_.ptr()))) {
        LOG_WARN("failed to append meta key cursor", K(ret), K_(last_meta_key));
      }
    } else { // 迭代时ls_id不固定，需要比较Ls_id和meta_key
      if (operator_.support_part_column()) {
        if (OB_FAIL(sql.append_fmt(
            " AND (ls_id %s %ld OR (ls_id = %ld AND (meta_key %s '%.*s' OR (meta_key = '%.*s' AND part > %ld))))",
            cmp_op, last_ls_id_.id(), last_ls_id_.id(), cmp_op,
            last_meta_key_.length(), last_meta_key_.ptr(),
            last_meta_key_.length(), last_meta_key_.ptr(), last_part_))) {
          LOG_WARN("failed to append ls+meta key+part cursor", K(ret),
                   K_(last_ls_id), K_(last_meta_key), K_(last_part));
        }
      } else if (OB_FAIL(sql.append_fmt(
          " AND (ls_id %s %ld OR (ls_id = %ld AND meta_key %s '%.*s'))",
          cmp_op, last_ls_id_.id(), last_ls_id_.id(), cmp_op,
          last_meta_key_.length(), last_meta_key_.ptr()))) {
        LOG_WARN("failed to append ls and meta key cursor", K(ret),
                 K_(last_ls_id), K_(last_meta_key));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (operator_.support_part_column()) {
      if (ls_id_.is_valid()) {
        if (OB_FAIL(sql.append_fmt(" ORDER BY meta_key %s, part ASC", order))) {
          LOG_WARN("failed to append order by", K(ret));
        }
      } else if (OB_FAIL(sql.append_fmt(" ORDER BY ls_id %s, meta_key %s, part ASC", order, order))) {
        LOG_WARN("failed to append order by", K(ret));
      }
    } else if (ls_id_.is_valid()) {
      if (OB_FAIL(sql.append_fmt(" ORDER BY meta_key %s", order))) {
        LOG_WARN("failed to append order by", K(ret));
      }
    } else if (OB_FAIL(sql.append_fmt(" ORDER BY ls_id %s, meta_key %s", order, order))) {
      LOG_WARN("failed to append order by", K(ret));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(" LIMIT %ld", page_limit))) {
    LOG_WARN("failed to append page limit", K(ret), K(page_limit));
  }
  return ret;
}

int ObSSMetaBaseIterator::open_next_page_()
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  void *buf = nullptr;
  ObSqlString sql;

  if (get_next_page_limit_() <= 0) {
    iter_end_ = true;
    ret = OB_ITER_END;
  } else if (OB_NOT_NULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterator page already opened", K(ret));
  } else if (OB_ISNULL(buf = share::mtl_malloc(
      sizeof(ObMySQLProxy::ReadResult), "SSMetaIt"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read result", K(ret));
  } else if (OB_ISNULL(result_ = new (buf) ObMySQLProxy::ReadResult())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to construct read result", K(ret));
  } else if (OB_FAIL(operator_.get_real_tenant_id_(tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", K(ret), K_(tenant_id));
  } else if (OB_FAIL(build_select_sql_(sql))) {
    LOG_WARN("failed to build select sql", K(ret));
  } else if (OB_FAIL(operator_.do_sql_read_(real_tenant_id, sql.ptr(), *result_))) {
    LOG_WARN("failed to execute select sql", K(ret), K(real_tenant_id), K(sql));
  } else {
    opened_ = true;
    current_page_row_count_ = 0;
    LOG_TRACE("ss meta base iterator opened next page", K_(tenant_id), K(real_tenant_id),
        K_(ls_id), K_(meta_type), K_(returned_row_count), K(sql));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    release_result_();
  }
  return ret;
}

int ObSSMetaBaseIterator::get_next_raw_row_(
    common::ObIAllocator &allocator,
    ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();

  // 分页读取流程：首次调用时打开第一页；当前页满后释放 result_，下次调用再按游标打开下一页；
  // 最后一页不足 page_size_ 时，读到该 ReadResult 末尾即可结束整个迭代。
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss meta base iterator not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (!opened_ && OB_FAIL(open_next_page_())) {
    // 首次读取或上一页已满时才会打开页面；打开成功后继续使用下面的 result_ 读取本页第一行。
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to open next iterator page", K(ret));
    }
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result is null after opening iterator page", K(ret));
  } else {
    ObSSMetaBaseType tmp_meta_type;
    int64_t part = 0;
    if (OB_FAIL(operator_.parse_result_whole_row_(
        *result_, allocator, row.tenant_id_, row.ls_id_, tmp_meta_type,
        row.meta_key_, row.meta_value_, part))) {
      if (OB_ITER_END == ret) {
        // 满页会在返回最后一行时主动释放 result_，因此在这里读到末尾，
        // 说明本页不足 page_size_（或续查结果为空），后面已经没有数据。
        release_result_();
        opened_ = false;
        iter_end_ = true;
      } else {
        LOG_WARN("failed to parse result whole row", K(ret), K_(returned_row_count));
      }
    } else if (OB_FAIL(last_meta_key_.assign(row.meta_key_))) {
      LOG_WARN("failed to save page cursor meta key", K(ret), K(row));
    } else {
      row.meta_type_ = tmp_meta_type.get_type_val();
      row.part_ = part;
      // 游标必须记录每一条 SQL 原始行；即使上层之后因版本过滤或去重跳过该行，
      // 下一页也应从该原始行之后继续，避免重复扫描。
      last_ls_id_ = row.ls_id_;
      last_part_ = part;
      has_cursor_ = true;
      // returned_row_count_ 统计整个迭代，current_page_row_count_ 只统计当前 SQL 页。
      ++returned_row_count_;
      ++current_page_row_count_;
      if (version_filter_.has_limit()
          && returned_row_count_ >= version_filter_.get_limit()) {
        // 已达到调用方指定的总 LIMIT，不再打开下一页。
        release_result_();
        opened_ = false;
        iter_end_ = true;
      } else if (current_page_row_count_ >= page_size_) {
        // 当前页已满；释放 ReadResult，下一次调用会使用保存的游标打开下一页。
        release_result_();
        opened_ = false;
      }
    }
  }
  return ret;
}

int ObSSMetaBaseIterator::is_in_version_range_(
    const ObString &meta_key, 
    bool &is_in_range) const
{
  int ret = OB_SUCCESS;
  is_in_range = true; // no version filter configured: keep the row by default
  if (version_filter_.cover_all_version()) {
    // is_in_range must be true
  } else if (version_filter_.has_version_range()) {
    uint64_t scn_val = 0;
    if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(meta_key, scn_val))) {
      LOG_WARN("fail to extract scn from meta_key", KR(ret), K(meta_key));
    } else {
      is_in_range =
          scn_val >= version_filter_.get_version_start().get_val_for_inner_table_field()
       && scn_val <= version_filter_.get_version_end().get_val_for_inner_table_field();
    }
  }
  return ret;
}

int ObSSMetaBaseIterator::get_next(
    common::ObIAllocator &allocator,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaBaseType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  ObSSRawMetaRow row;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_type.reset();
  meta_key.reset();
  meta_value.reset();
  
  ObArenaAllocator local_alloc("SSMetaDedup");

  while (OB_SUCC(ret)) {
    local_alloc.reuse();
    bool is_in_range = false;
    if (OB_FAIL(get_next_raw_row_(local_alloc, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next raw row", K(ret));
      }
    } else if (OB_FAIL(is_in_version_range_(row.meta_key_, is_in_range))) {
      LOG_WARN("fail to check version range", KR(ret), K(row));
    } else if (!is_in_range) {
      continue; // 按照版本范围过滤
    } else if (version_filter_.is_dedup_by_prefix() && is_latest_desc_()) {
      ObString logical_prefix; // 对同一个逻辑key去重，降序取最新版本
      if (OB_FAIL(ObSSMetaInfoUtil::extract_prefix(row.meta_key_, logical_prefix))) {
        LOG_WARN("fail to extract prefix", KR(ret), K(row));
      } else if (OB_UNLIKELY(logical_prefix.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("logical prefix should not be empty", KR(ret), K(row));
      } else if (!last_logical_prefix_.empty() && logical_prefix == last_logical_prefix_.string()) {
        continue; // 跳过同一个逻辑key的旧版本
      } else if (OB_FAIL(last_logical_prefix_.assign(logical_prefix.ptr(), logical_prefix.length()))) {
        LOG_WARN("failed to save logical prefix", K(ret), K(logical_prefix));
      }
    }
    break;
  }

  if (OB_SUCC(ret)) {
    tenant_id = row.tenant_id_;
    ls_id = row.ls_id_;
    meta_type.type_val_ = row.meta_type_;
    if (OB_FAIL(ob_write_string(allocator, row.meta_key_, meta_key))) {
      LOG_WARN("failed to copy meta_key to caller allocator", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, row.meta_value_, meta_value))) {
      LOG_WARN("failed to copy meta_value to caller allocator", K(ret));
    }
  }
  return ret;
}

/******************ObSSMetaTableIterator*********************/

const char *ObSSMetaTableIterator::get_table_name_() const
{
  return share::OB_ALL_SHARED_META_TNAME;
}

/******************ObSSTieredMetaTableIterator*********************/

ObSSTieredMetaTableIterator::ObSSTieredMetaTableIterator()
  : ObSSMetaBaseIterator(),
    has_pending_row_(false),
    pending_row_(),
    meta_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "TieredIterV2"))
{}

ObSSTieredMetaTableIterator::~ObSSTieredMetaTableIterator()
{
  reset();
}

void ObSSTieredMetaTableIterator::reset()
{
  ObSSMetaBaseIterator::reset();
  has_pending_row_ = false;
  pending_row_.reset();
  pending_meta_key_buf_.reset();
  pending_meta_value_buf_.reset();
  meta_allocator_.reset();
}

const char *ObSSTieredMetaTableIterator::get_table_name_() const
{
  return share::OB_ALL_SHARED_TIERED_META_TNAME;
}

bool ObSSTieredMetaTableIterator::is_same_logical_row_(const ObSSRawMetaRow &lhs, const ObSSRawMetaRow &rhs)
{
  return lhs.tenant_id_ == rhs.tenant_id_ && lhs.ls_id_ == rhs.ls_id_
      && lhs.meta_key_.length() == rhs.meta_key_.length()
      && (0 == rhs.meta_key_.length()
           || 0 == MEMCMP(lhs.meta_key_.ptr(), rhs.meta_key_.ptr(), rhs.meta_key_.length()));
}

// 将一行暂存到 pending 中（重组拆分部分时多读到的下一逻辑行）
int ObSSTieredMetaTableIterator::save_to_pending_(const ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  pending_row_.tenant_id_ = row.tenant_id_;
  pending_row_.ls_id_ = row.ls_id_;
  pending_row_.meta_type_ = row.meta_type_;
  pending_row_.part_ = row.part_;
  if (OB_FAIL(pending_meta_key_buf_.assign(row.meta_key_))) {
    LOG_WARN("failed to save pending meta key", K(ret), K(row.meta_key_));
  } else if (OB_FAIL(pending_meta_value_buf_.assign(row.meta_value_))) {
    LOG_WARN("failed to save pending meta value", K(ret), K(row.meta_value_.length()));
  } else {
    pending_row_.meta_key_.assign_ptr(pending_meta_key_buf_.ptr(),
        static_cast<ObString::obstr_size_t>(pending_meta_key_buf_.length()));
    pending_row_.meta_value_.assign_ptr(pending_meta_value_buf_.ptr(),
        static_cast<ObString::obstr_size_t>(pending_meta_value_buf_.length()));
    has_pending_row_ = true;
  }
  return ret;
}

// 加载当前行：pending 有则直接使用，否则从 DB 取一行
int ObSSTieredMetaTableIterator::load_current_row_(
    common::ObIAllocator &alloc, ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();

  if (has_pending_row_) {
    // pending_row_ 的 key/value 只是指向 owning buffer 的 view，而 owning buffer
    // 会被下一次 save_to_pending_ 复用覆盖；消费 pending 时必须深拷贝到 alloc，
    // 保证当前行在后续 lookahead 期间持有独立所有权。
    if (OB_FAIL(row.deep_copy(alloc, pending_row_))) {
      LOG_WARN("failed to deep copy pending row", K(ret), K_(pending_row));
    } else {
      has_pending_row_ = false;
    }
  } else {
    if (OB_FAIL(get_next_raw_row_(alloc, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next raw row from db", K(ret));
      }
    }
  }
  return ret;
}

// 收集拆分行后续部分的 value 到数组，直到离开当前逻辑键。
// 所有 part value 分配在 alloc 中，调用方需保证 alloc 生命周期覆盖后续拼接。
// first_row 用于校验 tenant_id、ls_id、meta_key、meta_type 与首个 part 一致，
// 同时保证 part 编号连续。
int ObSSTieredMetaTableIterator::reassemble_parts_(
    common::ObIAllocator &alloc,
    common::ObSqlString &merged_value,
    const ObSSRawMetaRow &first_row,
    int64_t next_part)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    ObSSRawMetaRow next_row;

    if (OB_FAIL(get_next_raw_row_(alloc, next_row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next part", K(ret));
      }
      break;
    }

    const bool is_same_logical_row = is_same_logical_row_(next_row, first_row);
    if (is_same_logical_row && (next_row.meta_type_ != first_row.meta_type_ || next_row.part_ != next_part
        || next_row.meta_value_.empty())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid tiered meta split row", K(ret),
               "tenant_id", first_row.tenant_id_, "ls_id", first_row.ls_id_,
               "meta_key", first_row.meta_key_, "first_meta_type", first_row.meta_type_,
               "next_meta_type", next_row.meta_type_, "next_part", next_row.part_,
               "expected_part", next_part, "next_value_length", next_row.meta_value_.length());
      break;
    } else if (!is_same_logical_row && 0 != next_row.part_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("next logical row does not start from part 0", K(ret),
               "first_tenant_id", first_row.tenant_id_, "first_ls_id", first_row.ls_id_,
               "first_meta_key", first_row.meta_key_, "first_meta_type", first_row.meta_type_,
               "next_tenant_id", next_row.tenant_id_, "next_ls_id", next_row.ls_id_,
               "next_meta_key", next_row.meta_key_, "next_meta_type", next_row.meta_type_,
               "next_part", next_row.part_, "expected_part", next_part,
               "next_value_length", next_row.meta_value_.length());
      break;
    } else if (!is_same_logical_row) {
      if (OB_FAIL(save_to_pending_(next_row))) {
        LOG_WARN("failed to save pending row", K(ret));
      }
      break;
    }

    if (OB_FAIL(merged_value.append(next_row.meta_value_.ptr(), next_row.meta_value_.length()))) {
      LOG_WARN("failed to append part value", K(ret), K(next_part));
    } else {
      ++next_part;
    }
  }
  return ret;
}

int ObSSTieredMetaTableIterator::handle_multi_part_row_(
    common::ObIAllocator &alloc,
    const ObSSRawMetaRow &in, ObSSRawMetaRow &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  ObSqlString merged_value;
  ObString stable_meta_key;

  if (OB_UNLIKELY(0 != in.part_ || in.meta_value_.empty())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid first tiered meta split row", K(ret),
             "tenant_id", in.tenant_id_, "ls_id", in.ls_id_,
             "meta_type", in.meta_type_, "meta_key", in.meta_key_, "part", in.part_,
             "value_length", in.meta_value_.length());
  } else if (OB_FAIL(ob_write_string(alloc, in.meta_key_, stable_meta_key))) {
    LOG_WARN("failed to copy meta key", K(ret));
  } else {
    // load_current_row_ 已将 meta_key 深拷贝到 alloc。
    // stable_meta_key 是同一 alloc 中的防御性副本，重组期间不会被复用。
    ObSSRawMetaRow stable_first_row;
    stable_first_row.tenant_id_ = in.tenant_id_;
    stable_first_row.ls_id_ = in.ls_id_;
    stable_first_row.meta_type_ = in.meta_type_;
    stable_first_row.part_ = in.part_;
    stable_first_row.meta_key_ = stable_meta_key;
    stable_first_row.meta_value_.reset();
    if (OB_FAIL(merged_value.append(in.meta_value_.ptr(), in.meta_value_.length()))) {
      LOG_WARN("failed to append first part value", K(ret));
    } else if (OB_FAIL(reassemble_parts_(alloc, merged_value, stable_first_row, in.part_ + 1))) {
      LOG_WARN("failed to reassemble parts", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    out.tenant_id_ = in.tenant_id_;
    out.ls_id_ = in.ls_id_;
    out.meta_type_ = in.meta_type_;
    out.part_ = 0;
    out.meta_key_ = stable_meta_key;

    if (OB_FAIL(ob_write_string(alloc,
               ObString(static_cast<ObString::obstr_size_t>(merged_value.length()),
                        merged_value.ptr()),
               out.meta_value_))) {
      LOG_WARN("failed to copy merged value", K(ret));
    }
  }
  return ret;
}

int ObSSTieredMetaTableIterator::get_next(
    common::ObIAllocator &allocator,
    uint64_t &tenant_id,
    share::ObLSID &ls_id,
    ObSSMetaBaseType &meta_type,
    common::ObString &meta_key,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc("SSTieredIt");
  ObSSRawMetaRow cur_row;
  ObSSRawMetaRow out_row;
  tenant_id = OB_INVALID_ID;
  ls_id.reset();
  meta_type.reset();
  meta_key.reset();
  meta_value.reset();

  const bool is_latest = version_filter_.is_dedup_by_prefix();
  
  while (OB_SUCC(ret)) {
    tmp_alloc.reuse();
    bool is_in_range = false;
    if (OB_FAIL(load_current_row_(tmp_alloc, cur_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to load current row", K(ret));
      }
    } else if (0 != cur_row.part_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("unexpected split part row without part 0", K(ret),
               "tenant_id", cur_row.tenant_id_, "ls_id", cur_row.ls_id_,
               "meta_type", cur_row.meta_type_, "meta_key", cur_row.meta_key_, "part", cur_row.part_,
               "value_length", cur_row.meta_value_.length());
    } else if (OB_FAIL(handle_multi_part_row_(tmp_alloc, cur_row, out_row))) {
      LOG_WARN("failed to handle multi-part row", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(is_in_version_range_(out_row.meta_key_, is_in_range))) {
      LOG_WARN("fail to check version range", KR(ret), K(out_row));
    } else if (!is_in_range) {
      continue;
    } else if (is_latest) {
      ObString logical_prefix;
      if (OB_FAIL(ObSSMetaInfoUtil::extract_prefix(out_row.meta_key_, logical_prefix))) {
        LOG_WARN("fail to extract prefix", KR(ret), K(out_row));
      } else if (OB_UNLIKELY(logical_prefix.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("logical prefix should not be empty", KR(ret), K(out_row));
      } else if (!last_logical_prefix_.empty() && logical_prefix == last_logical_prefix_.string()) {
        continue; // 对同一个逻辑key去重，降序取最新版本
      } else if (OB_FAIL(last_logical_prefix_.assign(logical_prefix.ptr(), logical_prefix.length()))) {
        LOG_WARN("failed to save logical prefix", K(ret), K(logical_prefix));
        break;
      }
    }
    break;
  }

  if (OB_SUCC(ret)) {
    tenant_id = out_row.tenant_id_;
    ls_id = out_row.ls_id_;
    meta_type.type_val_ = out_row.meta_type_;
    if (OB_FAIL(ob_write_string(allocator, out_row.meta_key_, meta_key))) {
      LOG_WARN("failed to copy meta_key to caller allocator", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, out_row.meta_value_, meta_value))) {
      LOG_WARN("failed to copy meta_value to caller allocator", K(ret));
    }
  }
  return ret;
}

int ObSSTieredMetaTableIterator::get_next(
    blocksstable::MacroBlockId &macro_block_id,
    common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString meta_key;

  meta_allocator_.reuse();
  if (OB_FAIL(get_next(meta_allocator_, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next", K(ret));
    }
  } else {
    SMART_VAR(char[common::MAX_PATH_SIZE], path_buf) {
      if (OB_FAIL(databuff_printf(path_buf, common::MAX_PATH_SIZE,
              "%.*s", meta_key.length(), meta_key.ptr()))) {
        LOG_WARN("fail to format path", K(ret), K(meta_key));
      } else if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
              ObStorageObjectType::SHARED_TABLET_SUB_META, path_buf, macro_block_id))) {
        LOG_WARN("fail to convert path to macro id", K(ret), K(meta_key));
      }
    }
  }
  return ret;
}
