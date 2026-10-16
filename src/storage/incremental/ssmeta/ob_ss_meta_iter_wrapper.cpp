/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "lib/utility/utility.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/blocksstable/index_block/ob_sstable_meta_info.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_table_store_util.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{

// ============================================================================
// ObSSMetaLSIDIter
// ============================================================================
ObSSMetaLSIDIter::ObSSMetaLSIDIter()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaLSIDIter),
      is_inited_(false),
      inner_iter_(),
      last_ls_id_(),
      tmp_alloc_("LSIDIt")
{
}

ObSSMetaLSIDIter::~ObSSMetaLSIDIter()
{
}

int ObSSMetaLSIDIter::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN &upper_bound)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    ObLSID invalid_ls;
    ObString empty_prefix;
    ObSSMetaVersionFilter version_filter;
    // 使用 DESC + dedup_by_prefix 模式：每个 logical_prefix 只返回最新行，
    // 大幅减少扫描行数，同时避免依赖 meta_key 字典序来保证 ls_id 连续
    version_filter.set_dedup_by_prefix();
    version_filter.set_desc_order();
    version_filter.set_upper_bound(upper_bound);

    if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, invalid_ls,
                                 meta_type, empty_prefix, version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(upper_bound));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaLSIDIter::get_next(ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
        break;
      }
      if (row_ls_id == last_ls_id_) {
        continue;
      }
      last_ls_id_ = row_ls_id;
      ls_id = row_ls_id;
      break;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaLSSCNIterator
// ============================================================================
ObSSMetaLSSCNIterator::ObSSMetaLSSCNIterator()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaLSSCNIterator),
      is_inited_(false),
      skip_mark_delete_(false),
      inner_iter_(),
      tmp_alloc_("LSSCNIt")
{
}

ObSSMetaLSSCNIterator::~ObSSMetaLSSCNIterator()
{
}

int ObSSMetaLSSCNIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    ObString empty_prefix;
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(upper_bound);

    if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                 meta_type, empty_prefix, version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
    } else {
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaLSSCNIterator::get_next(SCN &scn)
{
  int ret = OB_SUCCESS;
  scn.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
      } else if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
        } else if (is_deleted) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        uint64_t scn_val = 0;
        if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(row_meta_key, scn_val))) {
          LOG_WARN("fail to extract scn from meta_key", KR(ret), K(row_meta_key));
        } else if (OB_FAIL(scn.convert_for_inner_table_field(scn_val))) {
          LOG_WARN("fail to convert for inner table field", K(ret), K(scn));
        }
      }
      break;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaLSCkptKeyIter
// ============================================================================
ObSSMetaLSCkptKeyIter::ObSSMetaLSCkptKeyIter()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaLSCkptKeyIter),
      is_inited_(false),
      skip_mark_delete_(false),
      inner_iter_(),
      tmp_alloc_("LSCkptIt")
{
}

ObSSMetaLSCkptKeyIter::~ObSSMetaLSCkptKeyIter()
{
}

int ObSSMetaLSCkptKeyIter::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != tenant_id || !ls_id.is_valid()
             || !upper_bound.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    ObString empty_prefix;
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(upper_bound);
    version_filter.set_desc_order();

    if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                 meta_type, empty_prefix, version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
    } else {
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaLSCkptKeyIter::get_next(ObSSMetaLSCkptKey &key)
{
  int ret = OB_SUCCESS;
  key.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
      } else if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
        } else if (is_deleted) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        uint64_t scn_val = 0;
        if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(row_meta_key, scn_val))) {
          LOG_WARN("fail to extract scn from meta_key", KR(ret), K(row_meta_key));
        } else if (OB_FAIL(key.meta_scn_.convert_for_inner_table_field(scn_val))) {
          LOG_WARN("fail to convert for inner table field", K(ret), K(scn_val));
        } else {
          key.ls_id_ = row_ls_id;
        }
      }
      break;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaTabletKeyIterator
// ============================================================================
ObSSMetaTabletKeyIterator::ObSSMetaTabletKeyIterator()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaTabletKeyIterator),
      is_inited_(false),
      skip_mark_delete_(false),
      ls_id_(),
      inner_iter_(),
      tmp_alloc_("TKeyIt")
{
}

ObSSMetaTabletKeyIterator::~ObSSMetaTabletKeyIterator()
{
}

int ObSSMetaTabletKeyIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObString empty_prefix;
    ObSSMetaVersionFilter version_filter;
    version_filter.set_dedup_by_prefix();
    version_filter.set_desc_order();
    version_filter.set_upper_bound(upper_bound);

    if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                 meta_type, empty_prefix, version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
    } else {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletKeyIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const ObScnRange &range,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
             || !tablet_id.is_valid() || !range.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(range));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    ObSqlString prefix;
    // 返回 meta_scn 落在闭区间内的全部版本，供删除内部表前收集对应的对象存储 key。
    if (OB_FAIL(version_filter.set_version_range(range.start_scn_, range.end_scn_))) {
      LOG_WARN("invalid version range", K(ret), K(range));
    } else if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
                           ls_id, tablet_id, reorganize_scn, prefix))) {
      LOG_WARN("failed to build prefix", K(ret), K(ls_id), K(tablet_id), K(reorganize_scn));
    } else if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                        meta_type, prefix.string(), version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
               K(tablet_id), K(reorganize_scn), K(range));
    } else {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletKeyIterator::get_next(ObSSMetaTabletKey &key)
{
  int ret = OB_SUCCESS;
  key.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
        break;
      }
      if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
          break;
        }
        if (is_deleted) {
          continue;
        }
      }
      ObTabletID tablet_id;
      SCN reorgan_scn;
      SCN meta_scn;
      // 直接对完整 meta_key 反解析，兼容 inner_tablet(ls/...) 与普通 tablet(tablet/...)
      if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(row_meta_key, tablet_id,
                                                          reorgan_scn, meta_scn))) {
        LOG_WARN("failed to parse tablet meta_key", K(ret), K(row_meta_key));
      } else {
        key.ls_id_ = ls_id_;
        key.tablet_id_ = tablet_id;
        key.reorganization_scn_ = reorgan_scn;
        key.meta_scn_ = meta_scn;
      }
      break;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaTabletSCNIterator
// ============================================================================
ObSSMetaTabletSCNIterator::ObSSMetaTabletSCNIterator()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaTabletSCNIterator),
      is_inited_(false),
      skip_mark_delete_(false),
      inner_iter_(),
      tmp_alloc_("TSCNIt")
{
}

ObSSMetaTabletSCNIterator::~ObSSMetaTabletSCNIterator()
{
}

int ObSSMetaTabletSCNIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const SCN &upper_bound,
    const bool only_latest_row,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
             || !tablet_id.is_valid() || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id),
             K(tablet_id), K(reorganize_scn), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    if (only_latest_row) {
      version_filter.set_desc_order();
    }

    ObSqlString start_key;
    ObSqlString end_key;
    if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(ls_id, tablet_id,
                                                             reorganize_scn, start_key))) {
      LOG_WARN("failed to build prefix", K(ret), K(ls_id), K(tablet_id), K(reorganize_scn));
    } else if (only_latest_row && (!skip_mark_delete)) {
      // 由 SQL 范围 + LIMIT 下推,只取范围内的最新一行
      // 由于meta_key带了'.Txx'后缀，而start_key和end_key不带，因此底层sql会对end_key补充'\xFF'
      // 只有不跳mark_delete行，才能允许limit 1. 否则在get_next时判断mark_deleted，将取出的
      // 这一行过滤了，会导致返回ENTRY_NOT_EXIST
      version_filter.set_limit(1);
      if (OB_FAIL(end_key.assign(start_key.string()))) {
        LOG_WARN("failed to assign end_key", K(ret), K(start_key));
      } else if (OB_FAIL(end_key.append_fmt("/%s%020lu", SCN_KEY_STR,
                     static_cast<int64_t>(upper_bound.get_val_for_inner_table_field())))) {
        LOG_WARN("failed to append upper_bound to end_key", K(ret), K(upper_bound));
      } else if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id, meta_type,
                     start_key.string(), end_key.string(), version_filter))) {
        LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
                 K(tablet_id), K(reorganize_scn), K(upper_bound));
      }
    } else {
      // 全量枚举 <= (upper_bound) 的版本（GC 路径），保持原有 prefix + upper_bound 行为。
      version_filter.set_upper_bound(upper_bound);
      if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                     meta_type, start_key.string(), version_filter))) {
        LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
                 K(tablet_id), K(reorganize_scn), K(upper_bound));
      }
    }
    if (OB_SUCC(ret)) {
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletSCNIterator::get_next(SCN &scn)
{
  int ret = OB_SUCCESS;
  scn.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
      } else if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
        } else if (is_deleted) {
          continue;
        }
      }
      if (OB_SUCC(ret)) {
        uint64_t scn_val = 0;
        if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(row_meta_key, scn_val))) {
          LOG_WARN("fail to extract scn from meta_key", KR(ret), K(row_meta_key));
        } else if (OB_FAIL(scn.convert_for_inner_table_field(scn_val))) {
          LOG_WARN("fail to convert for inner table field", K(ret), K(scn));
        }
      }
      break;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaRawRowIter
// ============================================================================
ObSSMetaRawRowIter::ObSSMetaRawRowIter()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaRawRowIter),
      is_inited_(false),
      skip_mark_delete_(false),
      inner_iter_(),
      tmp_alloc_("RawIt")
{
}

ObSSMetaRawRowIter::~ObSSMetaRawRowIter()
{
}

int ObSSMetaRawRowIter::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObSSMetaBaseType &meta_type,
    const ObString &meta_key_prefix,
    const ObSSMetaVersionFilter &version_filter,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                      meta_type, meta_key_prefix, version_filter))) {
    LOG_WARN("failed to init inner_iter_", K(ret), K(tenant_id), K(ls_id), K(meta_type));
  } else {
    skip_mark_delete_ = skip_mark_delete;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaRawRowIter::set_page_size(const int64_t page_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("raw meta row iterator is not initialized", K(ret));
  } else if (OB_FAIL(inner_iter_.set_page_size(page_size))) {
    LOG_WARN("failed to set raw meta row iterator page size", K(ret), K(page_size));
  }
  return ret;
}

void ObSSMetaRawRowIter::close_current_page()
{
  inner_iter_.close_current_page();
}

int ObSSMetaRawRowIter::get_next(ObIAllocator &allocator, ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  row.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      row.reset();
      tmp_alloc_.reuse();
      ObSSMetaBaseType meta_type;
      ObSSRawMetaRow tmp_row;
      ret = inner_iter_.get_next(tmp_alloc_, tmp_row.tenant_id_, tmp_row.ls_id_,
                                 meta_type, tmp_row.meta_key_, tmp_row.meta_value_);
      if (OB_SUCC(ret)) {
        tmp_row.meta_type_ = meta_type.type_val_;
      }
      if (OB_FAIL(ret)) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get_next failed", K(ret));
        }
      } else if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(tmp_row.meta_value_, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
        } else if (is_deleted) {
          continue;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(row.deep_copy(allocator, tmp_row))) {
        LOG_WARN("failed to deep copy row", K(ret));
      }
      break;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaTabletIDIterator
// ============================================================================
ObSSMetaTabletIDIterator::ObSSMetaTabletIDIterator()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaTabletIDIter),
      is_inited_(false),
      inner_key_iter_()
{
}

ObSSMetaTabletIDIterator::~ObSSMetaTabletIDIterator()
{
}

int ObSSMetaTabletIDIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(inner_key_iter_.init(sql_client, tenant_id, ls_id,
                                           upper_bound, skip_mark_delete))) {
    LOG_WARN("failed to init inner key iter", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaTabletIDIterator::get_next(
    ObLSID &ls_id,
    ObTabletID &tablet_id,
    SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  tablet_id.reset();
  transfer_scn.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSSMetaTabletKey key;
    if (OB_FAIL(inner_key_iter_.get_next(key))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("inner get_next failed", K(ret));
      }
    } else {
      ls_id = key.ls_id_;
      tablet_id = key.tablet_id_;
      transfer_scn = key.reorganization_scn_;
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaTabletIter
// ============================================================================
ObSSMetaTabletIter::ObSSMetaTabletIter()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaTabletIter),
      is_inited_(false),
      skip_mark_delete_(false),
      ls_id_(),
      inner_iter_(),
      tmp_alloc_("TabIt")
{
}

ObSSMetaTabletIter::~ObSSMetaTabletIter()
{
}

int ObSSMetaTabletIter::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObString empty_prefix;
    ObSSMetaVersionFilter version_filter;
    version_filter.set_dedup_by_prefix();
    version_filter.set_desc_order();
    version_filter.set_upper_bound(upper_bound);

    if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                 meta_type, empty_prefix, version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
    } else {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletIter::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
             || !tablet_id.is_valid() || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id),
             K(tablet_id), K(reorganize_scn), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    version_filter.set_desc_order(); // 获取最新版本，所以默认desc排序

    ObSqlString start_key;
    if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(ls_id, tablet_id,
                                                             reorganize_scn, start_key))) {
      LOG_WARN("failed to build prefix", K(ret), K(ls_id), K(tablet_id), K(reorganize_scn));
    } else if (reorganize_scn.is_valid() && (!skip_mark_delete)) {
      // 精确 reorganize_scn：范围扫描 + LIMIT 1，只取范围内的最新一行
      // 由于meta_key带了'.Txx'后缀，而start_key和end_key不带，因此底层sql会对end_key补充'\xFF'
      // 只有不跳mark_delete行，才能允许limit 1. 否则在get_next时判断mark_deleted，将取出的
      // 这一行过滤了，会导致返回ENTRY_NOT_EXIST
      version_filter.set_limit(1);
      ObSqlString end_key;
      if (OB_FAIL(end_key.assign(start_key.string()))) {
        LOG_WARN("failed to assign end_key", K(ret), K(start_key));
      } else if (OB_FAIL(end_key.append_fmt("/%s%020lu", SCN_KEY_STR,
                 static_cast<int64_t>(upper_bound.get_val_for_inner_table_field())))) {
        LOG_WARN("failed to append upper_bound to end_key", K(ret), K(upper_bound));
      } else if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                          meta_type, start_key.string(), end_key.string(),
                                          version_filter))) {
        LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
                 K(tablet_id), K(reorganize_scn), K(upper_bound));
      }
    } else {
      // 前缀匹配扫描，且不限制reorganize_scn
      version_filter.set_dedup_by_prefix();
      version_filter.set_upper_bound(upper_bound);
      if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                    meta_type, start_key.string(), version_filter))) {
        LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
                 K(tablet_id), K(reorganize_scn), K(upper_bound));
      }
    }
    if (OB_SUCC(ret)) {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletIter::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const SCN &range_start,
    const SCN &range_end,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
             || !tablet_id.is_valid() || !range_start.is_valid() || !range_end.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id),
             K(tablet_id), K(reorganize_scn), K(range_start), K(range_end));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    // Search historical tablet versions from newest to oldest, so callers can
    // stop once the current version proves remaining older versions cannot match.
    version_filter.set_desc_order();

    ObSqlString prefix;
    if (OB_FAIL(version_filter.set_version_range(range_start, range_end))) {
      LOG_WARN("invalid version range", K(ret), K(range_start), K(range_end));
    } else if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(ls_id, tablet_id,
                                                                    reorganize_scn, prefix))) {
      LOG_WARN("failed to build prefix", K(ret), K(ls_id), K(tablet_id), K(reorganize_scn));
    } else if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                        meta_type, prefix.string(), version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
               K(tablet_id), K(reorganize_scn));
    } else {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletIter::get_next(
    ObArenaAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len,
    SCN &meta_scn,
    ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  tablet_raw_buf = nullptr;
  tablet_raw_buf_len = 0;
  meta_scn.reset();
  update_log.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
        break;
      }
      if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
          break;
        }
        if (is_deleted) {
          continue;
        }
      }
      ObTabletID tablet_id;
      SCN reorgan_scn;
      SCN parsed_meta_scn;
      // 直接对完整 meta_key 反解析，兼容 inner_tablet(ls/...) 与普通 tablet(tablet/...)
      if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(row_meta_key, tablet_id,
                                                          reorgan_scn, parsed_meta_scn))) {
        LOG_WARN("failed to parse tablet meta_key", K(ret), K(row_meta_key));
      } else {
        ObMetaDiskAddr tablet_addr;
        int64_t pos = 0;
        if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_addr(ls_id_, tablet_id, reorgan_scn, 0/*op_id*/,
                                                      row_meta_value.length(), tablet_addr))) {
          LOG_WARN("fail to gen tablet_addr", KR(ret), K_(ls_id), K(tablet_id));
        } else if (OB_FAIL(ObSSTabletCreateHelper::get_ss_meta_raw_tablet_buf_by_buf(tablet_addr,
                   row_meta_value.ptr(), row_meta_value.length(), pos,
                   tablet_raw_buf, tablet_raw_buf_len, update_log, allocator))) {
          LOG_WARN("failed to get raw tablet buf from meta_value", K(ret), K(tablet_id), K(tablet_addr));
        } else {
          meta_scn = parsed_meta_scn;
        }
      }
      break;
    }
  }
  return ret;
}

int ObSSMetaTabletIter::get_next(
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    SCN &reorgan_scn,
    SCN &meta_scn)
{
  ObSSMetaUpdateMetaInfo update_meta_info;
  ObSSUpdateTabletLog update_log;
  return get_next(allocator, tablet_handle, update_meta_info, reorgan_scn, meta_scn, update_log);
}

int ObSSMetaTabletIter::get_next(
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    SCN &meta_scn,
    ObSSUpdateTabletLog &update_log)
{
  ObSSMetaUpdateMetaInfo update_meta_info;
  SCN reorgan_scn;
  return get_next(allocator, tablet_handle, update_meta_info, reorgan_scn, meta_scn, update_log);
}

int ObSSMetaTabletIter::get_next(
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObSSMetaUpdateMetaInfo &update_meta_info,
    SCN &reorgan_scn,
    SCN &meta_scn)
{
  ObSSUpdateTabletLog update_log;
  return get_next(allocator, tablet_handle, update_meta_info, reorgan_scn, meta_scn, update_log);
}

int ObSSMetaTabletIter::get_next(
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObSSMetaUpdateMetaInfo &update_meta_info,
    SCN &reorgan_scn,
    SCN &meta_scn,
    ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  reorgan_scn.reset();
  meta_scn.reset();
  update_log.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id,
                                       row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
        break;
      }
      if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
          break;
        }
        if (is_deleted) {
          continue;
        }
      }
      ObTabletID tablet_id;
      SCN parsed_reorgan_scn;
      SCN parsed_meta_scn;
      // 直接对完整 meta_key 反解析，兼容 inner_tablet(ls/...) 与普通 tablet(tablet/...)
      if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(row_meta_key, tablet_id,
                                                          parsed_reorgan_scn, parsed_meta_scn))) {
        LOG_WARN("failed to parse tablet meta_key", K(ret), K(row_meta_key));
      } else {
        ObSSMetaHeadValue head_value;
        int64_t head_pos = 0;
        if (OB_FAIL(head_value.deserialize_value_info(row_meta_value.ptr(),
                                                      row_meta_value.length(),
                                                      head_pos))) {
          LOG_WARN("failed to deserialize head value info", K(ret), K(tablet_id));
        } else if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(ls_id_, tablet_id, parsed_reorgan_scn,
                   row_meta_value, allocator, tablet_handle, update_log))) {
          LOG_WARN("fail to build tablet handle", KR(ret), K_(ls_id), K(tablet_id), K(row_meta_value));
        } else {
          const ObSSMetaHeadValueInfo &head_info = head_value.get_meta_head_info();
          ObMetaUpdateReason reason;
          head_info.get_update_reason(reason);
          ObSSTabletTableStoreMetaInfo table_store_meta_info;
          head_info.get_table_store_meta(table_store_meta_info);
          if (OB_FAIL(update_meta_info.set(reason, parsed_meta_scn, -1 /*sstable_op_id*/,
                                           table_store_meta_info))) {
            LOG_WARN("failed to set update_meta_info", K(ret), K(reason), K(parsed_meta_scn));
          } else {
            reorgan_scn = parsed_reorgan_scn;
            meta_scn = parsed_meta_scn;
          }
        }
      }
      break;
    }
  }

  if (OB_FAIL(ret)) {
    tablet_handle.reset();
  }
  return ret;
}


int ObSSMetaTabletLockRowHelper::get_tablet_sub_meta_op_id_(
    const ObMetaDiskAddr &addr,
    const ObTabletID &tablet_id,
    int64_t &op_id)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  op_id = -1;
  if (!addr.is_block() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet sub meta address", K(ret), K(addr), K(tablet_id));
  } else if (OB_FAIL(addr.get_macro_block_id(macro_id))) {
    LOG_WARN("failed to get tablet sub meta macro id", K(ret), K(addr), K(tablet_id));
  } else if ((!macro_id.is_shared_tablet_sub_meta()
      && !macro_id.is_shared_tablet_sub_meta_in_table())
      || macro_id.second_id() != tablet_id.id()
      || macro_id.op_id() < 0) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid tablet sub meta macro id", K(ret), K(macro_id), K(tablet_id));
  } else {
    op_id = macro_id.op_id();
  }
  return ret;
}

// 按宏块类型分别恢复 SHARED_MINOR_LOCK 和 SHARED_INC_MAJOR_LOCK 高水位。
// DDL inc-major、major 等类型的 fourth_id_ 具有其它语义，不参与统计。
int ObSSMetaTabletLockRowHelper::get_sstable_macro_op_id_(
    const ObTabletID &tablet_id,
    const MacroBlockId &macro_id,
    int64_t &minor_op_id,
    int64_t &inc_major_op_id)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObStorageObjectType object_type = macro_id.storage_object_type();
  const bool is_minor_macro =
      blocksstable::ObStorageObjectType::SHARED_MINI_DATA_MACRO == object_type
      || blocksstable::ObStorageObjectType::SHARED_MINI_META_MACRO == object_type
      || blocksstable::ObStorageObjectType::SHARED_MINOR_DATA_MACRO == object_type
      || blocksstable::ObStorageObjectType::SHARED_MINOR_META_MACRO == object_type;
  const bool is_inc_major_macro =
      blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_DATA_MACRO == object_type
      || blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_COMPACTION_META_MACRO
          == object_type;
  minor_op_id = -1;
  inc_major_op_id = -1;
  if (!tablet_id.is_valid() || !macro_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for getting sstable macro op id",
        K(ret), K(tablet_id), K(macro_id));
  } else if (!macro_id.is_id_mode_share()
      || macro_id.second_id() != tablet_id.id()) {
    // 非共享对象或跨 tablet 引用的宏块，不参与统计。
  } else if (!is_minor_macro && !is_inc_major_macro) {
    // fourth_id_ 不表示这两个 lock op_id，不参与统计。
  } else if (macro_id.op_id() < 0) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid shared sstable macro op id",
        K(ret), K(tablet_id), K(macro_id));
  } else if (is_minor_macro) {
    minor_op_id = macro_id.op_id();
  } else {
    inc_major_op_id = macro_id.op_id();
  }
  return ret;
}

int ObSSMetaTabletLockRowHelper::accumulate_sstable_macro_op_id_(
    const ObTabletID &tablet_id,
    blocksstable::ObMacroIdIterator &macro_iter,
    int64_t &max_minor_op_id,
    int64_t &max_inc_major_op_id)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    MacroBlockId macro_id;
    int64_t minor_op_id = -1;
    int64_t inc_major_op_id = -1;
    if (OB_FAIL(macro_iter.get_next_macro_id(macro_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      LOG_WARN("failed to get next sstable macro id", K(ret), K(tablet_id));
    } else if (OB_FAIL(get_sstable_macro_op_id_(
        tablet_id, macro_id, minor_op_id, inc_major_op_id))) {
      LOG_WARN("failed to get sstable macro op id", K(ret), K(tablet_id), K(macro_id));
    } else {
      max_minor_op_id = MAX(max_minor_op_id, minor_op_id);
      max_inc_major_op_id = MAX(max_inc_major_op_id, inc_major_op_id);
    }
  }
  return ret;
}

int ObSSMetaTabletLockRowHelper::get_lock_row_info(
    const ObTablet &tablet,
    ObSSMetaTabletLockRowInfo &lock_row_info)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id = tablet.get_tablet_id();
  const ObMetaDiskAddr &table_store_addr = tablet.get_table_store_addr();
  int64_t tablet_op_id = -1;
  int64_t max_minor_op_id = -1;
  int64_t max_inc_major_op_id = -1;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator sstable_iter;
  lock_row_info.reset();

  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet", K(ret), K(tablet_id));
  } else if (table_store_addr.is_none()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("tablet has no table store address", K(ret), K(tablet_id), K(table_store_addr));
  } else if (OB_FAIL(get_tablet_sub_meta_op_id_(
      table_store_addr, tablet_id, tablet_op_id))) {
    LOG_WARN("failed to get table store op id", K(ret), K(tablet_id), K(table_store_addr));
  } else {
    lock_row_info.tablet_next_op_id_ = tablet_op_id + 1;
  }

  // 必须先 prewarm：它内部会把该 tablet 所有 SStable 的 meta 一次性预热进 storage meta
  // cache（get_table_store_and_prewarm_sstable_meta），否则下面逐个 sstable->get_meta()
  // 会各自触发一次 cache miss 去 IO。table_store_wrapper 本身用不到，别当成死代码删掉。
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet.fetch_table_store_with_prewarm(table_store_wrapper))) {
    LOG_WARN("failed to prewarm table store and sstable meta", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet.get_all_sstables(sstable_iter, false /*need_unpack*/))) {
    LOG_WARN("failed to get all sstables", K(ret), K(tablet_id));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(sstable_iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("failed to get next sstable", K(ret), K(tablet_id));
      } else if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null or non-sstable table", K(ret), K(tablet_id), KP(table));
      } else if (ObITable::TableType::MINI_SSTABLE == table->get_table_type()
          || ObITable::TableType::MINOR_SSTABLE == table->get_table_type()
          || table->is_inc_major_type_sstable()) {
        const bool is_inc_major_sstable = table->is_inc_major_type_sstable();
        blocksstable::ObSSTable *sstable =
            static_cast<blocksstable::ObSSTable *>(table);
        ObSSTableMetaHandle meta_handle;
        if (OB_FAIL(sstable->get_meta(meta_handle))) {
          LOG_WARN("failed to get sstable meta", K(ret), K(tablet_id), KPC(sstable));
        } else if (meta_handle.get_sstable_meta().is_empty()) {
          // 空 SStable 没有任何持久化宏块，也不消耗 SSTABLE_LOCK_ROW 的 op_id。
        } else {
          // inc-major SSTable 可能混用 DDL 与 compaction 宏块，需要同时扫描 OTHER 和 DATA。
          // MINI/MINOR 仅在 OTHER 未取得本 tablet 的 minor op_id 时回退 DATA，
          // 避免恢复阶段遍历大量数据宏块。
          const blocksstable::ObSSTableMacroInfo &macro_info =
              meta_handle.get_sstable_meta().get_macro_info();
          blocksstable::ObMacroIdIterator other_iter;
          int64_t cur_minor_op_id = -1;
          int64_t cur_inc_major_op_id = -1;
          if (OB_FAIL(macro_info.get_other_block_iter(other_iter))) {
            LOG_WARN("failed to get sstable other macro iterator",
                K(ret), K(tablet_id), KPC(sstable));
          } else if (OB_FAIL(accumulate_sstable_macro_op_id_(
              tablet_id, other_iter, cur_minor_op_id, cur_inc_major_op_id))) {
            LOG_WARN("failed to accumulate sstable other macro op id",
                K(ret), K(tablet_id), KPC(sstable));
          } else if (is_inc_major_sstable || cur_minor_op_id < 0) {
            blocksstable::ObMacroIdIterator data_iter;
            if (OB_FAIL(macro_info.get_data_block_iter(data_iter))) {
              LOG_WARN("failed to get sstable data macro iterator",
                  K(ret), K(tablet_id), KPC(sstable));
            } else if (OB_FAIL(accumulate_sstable_macro_op_id_(
                tablet_id, data_iter, cur_minor_op_id, cur_inc_major_op_id))) {
              LOG_WARN("failed to accumulate sstable data macro op id",
                  K(ret), K(tablet_id), KPC(sstable));
            }
          }
          if (OB_SUCC(ret)) {
            max_minor_op_id = MAX(max_minor_op_id, cur_minor_op_id);
            max_inc_major_op_id = MAX(max_inc_major_op_id, cur_inc_major_op_id);
          }
        }
      }
    }
  }

  // max 仍为 -1 是合法状态，对应 lock row 由后续协议按需懒创建。
  if (OB_SUCC(ret) && max_minor_op_id >= 0) {
    lock_row_info.minor_next_op_id_ = max_minor_op_id + 1;
  }
  if (OB_SUCC(ret) && max_inc_major_op_id >= 0) {
    lock_row_info.inc_major_next_op_id_ = max_inc_major_op_id + 1;
  }
  return ret;
}


// ============================================================================
// ObSSMetaTabletMacroIDIterator
//
// 【整体流程】
//   对 meta_scn_arr_ 中的每个版本，依次执行：
//     1. 从 __all_shared_meta 查询该版本的 tablet meta raw row
//     2. 从 raw row 中收集宏块 ID（两条路径）：
//        a. inline shared_meta 路径：解析 meta_value 中内联的 ObTabletMacroInfo，
//           获取 tablet 级别管理块（sstable 地址、table_store 地址等）
//        b. sstable 遍历路径（best-effort）：反序列化完整 tablet，遍历其所有
//           sstable，从每个 sstable 的 ObSSTableMacroInfo 中收集数据/元数据宏块
//        c. prewarm 文件路径：对 major sstable 按 snapshot/reorg_scn 生成预热文件对象 ID
//     3. 将收集到的所有 block_info 暂存到 cur_block_infos_ 数组
//     4. get_next() 从该数组逐条输出，通过 dedup_set_ 跨版本去重
//
// 【状态机】
//   get_next() 循环：
//     cur_block_idx_ < cur_block_infos_.count() → 输出当前版本的下一个块
//     cur_block_idx_ >= count → load_next_version_blocks_() 加载下一个版本
//     所有版本耗尽 → OB_ITER_END
// ============================================================================
ObSSMetaTabletMacroIDIterator::ObSSMetaTabletMacroIDIterator()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaTabletMacroIDIter),
      is_inited_(false),
      skip_mark_delete_(false),
      iter_end_(false),
      target_type_(ObTabletMacroType::MAX),  // 默认返回所有类型
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      tablet_id_(),
      reorganize_scn_(),
      sql_client_(nullptr),       // 生命周期由调用者保证
      meta_scn_arr_(),            // 待遍历的 tablet meta 版本列表
      cur_version_idx_(0),        // 当前遍历到第几个版本
      inner_raw_iter_(),          // 每个版本对应一次 SQL 查询
      tablet_alloc_("TMacroIDIt"),// 每个版本独立使用，版本切换时 reuse
      cur_block_infos_(),         // 当前版本预收集的所有宏块（两条路径合并）
      cur_block_idx_(0),          // get_next 在 cur_block_infos_ 中的游标
      dedup_set_()                // 跨版本去重 hash set
{
}

ObSSMetaTabletMacroIDIterator::~ObSSMetaTabletMacroIDIterator()
{
  dedup_set_.destroy();
}

// 版本切换时清理当前版本的临时数据，复用 allocator 内存
void ObSSMetaTabletMacroIDIterator::reset_current_version_()
{
  cur_block_infos_.reuse();
  cur_block_idx_ = 0;
  tablet_alloc_.reuse();
}

int ObSSMetaTabletMacroIDIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const ObIArray<SCN> &meta_scn_arr,
    const ObTabletMacroType target_type,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
             || !tablet_id.is_valid() || 0 == meta_scn_arr.count()
             || target_type <= ObTabletMacroType::INVALID_TYPE
             || target_type > ObTabletMacroType::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id),
             K(tablet_id), K(reorganize_scn), K(meta_scn_arr.count()), K(target_type));
  } else if (OB_FAIL(meta_scn_arr_.assign(meta_scn_arr))) {
    LOG_WARN("failed to copy meta_scn_arr", K(ret));
  } else if (OB_FAIL(dedup_set_.create(1024))) {
    LOG_WARN("failed to create dedup set", K(ret));
  } else {
    target_type_ = target_type;
    skip_mark_delete_ = skip_mark_delete;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    reorganize_scn_ = reorganize_scn;
    sql_client_ = &sql_client;
    cur_version_idx_ = 0;
    cur_block_idx_ = 0;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

// 路径 b：反序列化 tablet 后，遍历所有 sstable，从每个 sstable 的
// ObSSTableMacroInfo 中收集 data/other/linked block ID。
// 需要有效的 MTL 租户上下文（sstable->get_meta() 可能触发共享存储 IO）。
int ObSSMetaTabletMacroIDIterator::collect_blocks_from_sstables_(
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const bool fill_occupy_size,
    const bool include_prewarm,
    ObTablet &tablet,
    ObIArray<ObTabletBlockInfo> &out_blocks)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTableStoreIterator sstable_iter;
  if (OB_FAIL(tablet.fetch_table_store_with_prewarm(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store with sstable meta prewarm", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet.get_all_sstables(sstable_iter, true /*need_unpack*/))) {
    LOG_WARN("failed to get all sstables", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(sstable_iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        LOG_WARN("failed to get next sstable", K(ret));
      } else if (OB_ISNULL(table) || !table->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null or non-sstable table", K(ret), KP(table));
      } else {
        blocksstable::ObSSTable *sstable = static_cast<blocksstable::ObSSTable *>(table);
        ObSSTableMetaHandle meta_handle;
        if (include_prewarm
            && OB_FAIL(collect_prewarm_blocks_from_sstable_(tablet_id, reorganize_scn, *sstable, out_blocks))) {
          LOG_WARN("failed to collect prewarm block ids from sstable", K(ret), KPC(sstable));
        } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
          LOG_WARN("failed to get sstable meta", K(ret), KPC(sstable));
        } else if (sstable->is_small_sstable()) {
          // small_sstable 的数据宏块是嵌套在共享宏块中的，需要按 SHARED_DATA_BLOCK 类型收集，
          // 并记录 nested_size 作为 occupy_size，与 init_aggregated_info 保持一致。
          const blocksstable::ObSSTableMacroInfo &sst_macro_info =
              meta_handle.get_sstable_meta().get_macro_info();
          blocksstable::ObMacroIdIterator data_iter;
          if (OB_FAIL(sst_macro_info.get_data_block_iter(data_iter))) {
            LOG_WARN("failed to get data block iter for small sstable", K(ret), KPC(sstable));
          } else {
            MacroBlockId data_macro_id;
            if (OB_FAIL(data_iter.get_next_macro_id(data_macro_id))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get data macro id from small sstable", K(ret));
              }
            } else if (OB_UNLIKELY(!data_macro_id.is_valid())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("small sstable data macro id is invalid", K(ret), K(data_macro_id), KPC(sstable));
            } else {
              const int64_t occupy_size = fill_occupy_size ? sst_macro_info.get_nested_size() : 0;
              ObTabletBlockInfo info(data_macro_id, ObTabletMacroType::SHARED_DATA_BLOCK,
                                    occupy_size);
              if (OB_FAIL(out_blocks.push_back(info))) {
                LOG_WARN("failed to push small sstable data block", K(ret), K(info));
              }
            }
          }
          // small_sstable 的 other_block（meta block）正常收集
          blocksstable::ObMacroIdIterator other_iter;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(sst_macro_info.get_other_block_iter(other_iter))) {
            LOG_WARN("failed to get other block iter for small sstable", K(ret), KPC(sstable));
          } else {
            MacroBlockId block_id;
            while (OB_SUCC(ret)) {
              if (OB_FAIL(other_iter.get_next_macro_id(block_id))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                }
                LOG_WARN("failed to get next other block from small sstable", K(ret));
              } else if (OB_UNLIKELY(!block_id.is_valid())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("small sstable other block id is invalid", K(ret), K(block_id), KPC(sstable));
              } else {
                ObTabletBlockInfo info(block_id, ObTabletMacroType::META_BLOCK, 0);
                if (OB_FAIL(out_blocks.push_back(info))) {
                  LOG_WARN("failed to push small sstable other block", K(ret), K(info));
                }
              }
            }
          }
        } else {
          // occupy_size 仅用于备份进度/账目，无需 byte 精确：按 data macro 数分摊 sstable 级 occupy_size。
          int64_t per_data_occupy_size = 0;
          if (fill_occupy_size) {
            const int64_t data_macro_cnt = meta_handle.get_sstable_meta().get_data_macro_block_count();
            const int64_t total_occupy_size = meta_handle.get_sstable_meta().get_occupy_size();
            per_data_occupy_size = (data_macro_cnt > 0) ? (total_occupy_size / data_macro_cnt) : 0;
          }
          const blocksstable::ObSSTableMacroInfo &sst_macro_info =
              meta_handle.get_sstable_meta().get_macro_info();
          blocksstable::ObMacroIdIterator block_iter;
          if (OB_FAIL(sst_macro_info.get_all_block_iter(block_iter))) {
            LOG_WARN("failed to get block iter from sstable", K(ret));
          } else {
            MacroBlockId block_id;
            blocksstable::ObMacroIdIterator::Type block_type;
            while (OB_SUCC(ret)) {
              if (OB_FAIL(block_iter.get_next_macro_id(block_id, block_type))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                }
                LOG_WARN("failed to get next macro id from sstable", K(ret));
              } else if (block_id.is_valid()) {
                ObTabletMacroType tablet_block_type = ObTabletMacroType::INVALID_TYPE;
                int64_t occupy_size = 0;
                if (blocksstable::ObMacroIdIterator::Type::DATA_BLOCK == block_type) {
                  tablet_block_type = ObTabletMacroType::DATA_BLOCK;
                  occupy_size = per_data_occupy_size;
                } else if (blocksstable::ObMacroIdIterator::Type::OTHER_BLOCK == block_type) {
                  tablet_block_type = ObTabletMacroType::META_BLOCK;
                } else if (blocksstable::ObMacroIdIterator::Type::LINKED_BLOCK == block_type) {
                  tablet_block_type = ObTabletMacroType::LINKED_BLOCK;
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected sstable block type", K(ret), K(block_type), K(block_id));
                }
                if (OB_SUCC(ret)) {
                  ObTabletBlockInfo info(block_id, tablet_block_type, occupy_size);
                  if (OB_FAIL(out_blocks.push_back(info))) {
                    LOG_WARN("failed to push block info", K(ret), K(info));
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

int ObSSMetaTabletMacroIDIterator::collect_prewarm_blocks_from_sstable_(
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const blocksstable::ObSSTable &sstable,
    ObIArray<ObTabletBlockInfo> &out_blocks)
{
  int ret = OB_SUCCESS;
  if (!sstable.is_major_sstable()) {
    // Major compaction prewarm files are organized by major snapshot version.
  } else {
    const int64_t major_snapshot_version = sstable.get_snapshot_version();
    if (OB_UNLIKELY(major_snapshot_version < 0 || !reorganize_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid prewarm object key", K(ret), K(major_snapshot_version), K(reorganize_scn));
    } else if (OB_FAIL(push_prewarm_block_info_(tablet_id, reorganize_scn,
                   blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA,
                   ObTabletMacroType::DATA_BLOCK,
                   major_snapshot_version, out_blocks))) {
      LOG_WARN("failed to push major prewarm data block id", K(ret), K(major_snapshot_version));
    } else if (OB_FAIL(push_prewarm_block_info_(tablet_id, reorganize_scn,
                   blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX,
                   ObTabletMacroType::DATA_BLOCK,
                   major_snapshot_version, out_blocks))) {
      LOG_WARN("failed to push major prewarm data index block id", K(ret), K(major_snapshot_version));
    } else if (OB_FAIL(push_prewarm_block_info_(tablet_id, reorganize_scn,
                   blocksstable::ObStorageObjectType::MAJOR_PREWARM_META,
                   ObTabletMacroType::META_BLOCK,
                   major_snapshot_version, out_blocks))) {
      LOG_WARN("failed to push major prewarm meta block id", K(ret), K(major_snapshot_version));
    } else if (OB_FAIL(push_prewarm_block_info_(tablet_id, reorganize_scn,
                   blocksstable::ObStorageObjectType::MAJOR_PREWARM_META_INDEX,
                   ObTabletMacroType::META_BLOCK,
                   major_snapshot_version, out_blocks))) {
      LOG_WARN("failed to push major prewarm meta index block id", K(ret), K(major_snapshot_version));
    }
  }
  return ret;
}

int ObSSMetaTabletMacroIDIterator::push_prewarm_block_info_(
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const blocksstable::ObStorageObjectType object_type,
    const ObTabletMacroType block_type,
    const int64_t major_snapshot_version,
    ObIArray<ObTabletBlockInfo> &out_blocks)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId prewarm_block_id;
  blocksstable::ObStorageObjectOpt storage_opt;
  if (OB_UNLIKELY(major_snapshot_version < 0 || block_type <= ObTabletMacroType::INVALID_TYPE
      || block_type >= ObTabletMacroType::MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(object_type), K(block_type), K(major_snapshot_version));
  } else {
    storage_opt.set_ss_major_prewarm_opt(object_type,
                                         tablet_id.id(),
                                         major_snapshot_version,
                                         reorganize_scn.get_val_for_sql());
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.ss_get_object_id(storage_opt, prewarm_block_id))) {
      LOG_WARN("failed to get prewarm object id", K(ret), K(storage_opt), K(object_type));
    } else if (prewarm_block_id.is_valid()) {
      ObTabletBlockInfo info(prewarm_block_id, block_type, 0);
      if (OB_FAIL(out_blocks.push_back(info))) {
        LOG_WARN("failed to push prewarm block info", K(ret), K(info));
      }
    }
  }
  return ret;
}

// 路径 a：直接从 __all_shared_meta 的 meta_value 中解析内联的 ObTabletMacroInfo。
// 在 v2 持久化路径下，该 macro_info 仅包含 shared_meta 管理块
// （sstable meta 对象地址、table_store 地址、storage_schema 地址等），
// 不包含 sstable 内部的数据宏块。不需要额外 IO。
int ObSSMetaTabletMacroIDIterator::collect_inline_shared_meta_blocks_(
    ObArenaAllocator &allocator,
    const ObString &meta_value,
    ObIArray<ObTabletBlockInfo> &out_blocks)
{
  int ret = OB_SUCCESS;
  char *tablet_buf = nullptr;
  int64_t tablet_buf_len = 0;
  if (OB_UNLIKELY(meta_value.empty())) {
    // no data
  } else if (OB_FAIL(ObSSTabletMetaValue::get_tablet_raw_buffer(
                 meta_value.ptr(), meta_value.length(), allocator, tablet_buf, tablet_buf_len))) {
    LOG_WARN("failed to get tablet raw buffer", K(ret), K(meta_value.length()));
  } else {
    ObTabletBlockHeader header;
    int64_t pos = 0;
    if (OB_FAIL(header.deserialize(tablet_buf, tablet_buf_len, pos))) {
      LOG_WARN("failed to deserialize tablet block header", K(ret));
    } else if (0 == header.inline_meta_count_) {
      // no inline macro_info
    } else if (1 == header.inline_meta_count_
               && ObSecondaryMetaType::TABLET_MACRO_INFO == header.desc_array_[0].type_) {
      const int64_t macro_info_pos = pos + header.first_level_size_;
      if (OB_UNLIKELY(macro_info_pos < 0 || macro_info_pos >= tablet_buf_len)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid macro info position", K(ret), K(macro_info_pos), K(tablet_buf_len));
      } else {
        ObTabletMacroInfo *macro_info = nullptr;
        void *buf = allocator.alloc(sizeof(ObTabletMacroInfo));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc macro info", K(ret));
        } else {
          int64_t macro_pos = macro_info_pos;
          macro_info = new (buf) ObTabletMacroInfo();
          if (OB_FAIL(macro_info->deserialize(allocator, tablet_buf, tablet_buf_len, macro_pos))) {
            LOG_WARN("failed to deserialize inline macro info", K(ret));
            macro_info->~ObTabletMacroInfo();
            macro_info = nullptr;
          } else {
            ObMacroInfoIterator macro_iter;
            if (OB_FAIL(macro_iter.init(ObTabletMacroType::MAX, *macro_info))) {
              LOG_WARN("failed to init macro iter for inline shared_meta", K(ret));
            } else {
              ObTabletBlockInfo info;
              while (OB_SUCC(ret)) {
                if (OB_FAIL(macro_iter.get_next(info))) {
                  if (OB_ITER_END == ret) {
                    ret = OB_SUCCESS;
                    break;
                  }
                  LOG_WARN("failed to get next from inline macro_info", K(ret));
                } else if (OB_FAIL(out_blocks.push_back(info))) {
                  LOG_WARN("failed to push inline shared_meta block", K(ret), K(info));
                }
              }
            }
            macro_iter.destroy();
            macro_info->~ObTabletMacroInfo();
          }
        }
      }
    }
  }
  return ret;
}

// 对一个 tablet meta 版本的 raw meta_value，执行两条收集路径并合并结果。
// 调用后 cur_block_infos_ 包含该版本的完整宏块集合。
int ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_(const ObString &meta_value)
{
  cur_block_infos_.reuse();
  cur_block_idx_ = 0;
  // GC 依赖该迭代器生成完整的 KEPT 集合。预热文件不再生成，不加入 KEPT 集合。
  // sstable 路径失败必须上抛，避免漏标活块。
  return collect_blocks_from_tablet_meta_value(tablet_alloc_, ls_id_, tablet_id_,
      reorganize_scn_, meta_value, false /*fill_occupy_size*/, false /*include_prewarm*/, cur_block_infos_);
}

int ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
    ObArenaAllocator &allocator,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const ObString &meta_value,
    const bool fill_occupy_size,
    const bool include_prewarm,
    ObIArray<ObTabletBlockInfo> &out_blocks)
{
  int ret = OB_SUCCESS;

  // 路径 a) 收集 inline shared_meta 管理块（sstable 地址、table_store 地址等）
  if (OB_FAIL(collect_inline_shared_meta_blocks_(allocator, meta_value, out_blocks))) {
    LOG_WARN("failed to collect inline shared_meta blocks", K(ret));
  }

  // 路径 b) 反序列化完整 tablet，遍历 sstable 收集数据/元数据宏块。
  // 失败必须上抛，避免生成"成功但缺块"的不完整结果。
  if (OB_SUCC(ret)) {
    ObTabletHandle tablet_handle;
    if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(ls_id, tablet_id, reorganize_scn,
                                                      meta_value, allocator, tablet_handle))) {
      LOG_WARN("failed to build tablet handle", K(ret), K(ls_id), K(tablet_id), K(meta_value.length()));
    } else if (!tablet_handle.is_valid() || OB_ISNULL(tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle after deserialize", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(collect_blocks_from_sstables_(tablet_id, reorganize_scn,
                   fill_occupy_size, include_prewarm, *tablet_handle.get_obj(), out_blocks))) {
      LOG_WARN("failed to collect blocks from sstables", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

// 从 meta_scn_arr_ 中取下一个未处理的版本，查询 __all_shared_meta 获取该版本的
// raw meta_value，然后调用 collect_blocks_from_tablet_ 收集宏块到 cur_block_infos_。
// 指定版本不存在时返回错误；skip_mark_delete=true 时跳过被 mark_delete 的版本。
// 所有版本耗尽时返回 OB_ITER_END。
int ObSSMetaTabletMacroIDIterator::load_next_version_blocks_()
{
  int ret = OB_SUCCESS;
  reset_current_version_();

  bool loaded = false;
  while (OB_SUCC(ret) && !loaded && cur_version_idx_ < meta_scn_arr_.count()) {
    const SCN &cur_scn = meta_scn_arr_.at(cur_version_idx_);
    cur_version_idx_++;

    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    version_filter.set_dedup_by_prefix();
    version_filter.set_desc_order();
    ObSqlString prefix;
    if (OB_FAIL(version_filter.set_version_range(cur_scn, cur_scn))) {
      LOG_WARN("invalid version range", K(ret), K(cur_scn));
      break;
    } else if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(ls_id_, tablet_id_,
               reorganize_scn_, prefix))) {
      LOG_WARN("failed to build prefix", K(ret), K_(ls_id), K_(tablet_id), K_(reorganize_scn));
      break;
    } else if (FALSE_IT(inner_raw_iter_.reset())) {
    } else if (OB_FAIL(inner_raw_iter_.init(*sql_client_, tenant_id_, ls_id_,
               meta_type, prefix.string(), version_filter))) {
      LOG_WARN("failed to init inner raw iter", K(ret), K_(tenant_id), K_(ls_id),
               K_(tablet_id), K(cur_scn));
      break;
    }

    uint64_t row_tenant_id = OB_INVALID_ID;
    ObLSID row_ls_id;
    ObSSMetaBaseType row_meta_type;
    ObString row_meta_key;
    ObString row_meta_value;
    bool found = false;
    while (OB_SUCC(ret) && !found) {
      if (OB_FAIL(inner_raw_iter_.get_next(tablet_alloc_, row_tenant_id, row_ls_id,
                                           row_meta_type, row_meta_key, row_meta_value))) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("tablet meta version does not exist", K(ret), K_(tenant_id), K_(ls_id),
                   K_(tablet_id), K_(reorganize_scn), K(cur_scn));
        } else {
          LOG_WARN("inner raw iter get_next failed", K(ret), K(cur_scn));
        }
        break;
      }
      if (skip_mark_delete_) {
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(row_meta_value, is_deleted))) {
          LOG_WARN("check_mark_deleted failed", K(ret));
          break;
        }
        if (is_deleted) {
          break;
        }
      }
      found = true;
    }

    if (OB_SUCC(ret) && found) {
      if (OB_FAIL(collect_blocks_from_tablet_(row_meta_value))) {
        LOG_WARN("failed to collect blocks from tablet", K(ret), K(cur_scn));
      } else if (cur_block_infos_.count() > 0) {
        loaded = true;
      } else {
        reset_current_version_();
      }
    } else {
      reset_current_version_();
    }
  }

  if (OB_SUCC(ret) && !loaded) {
    ret = OB_ITER_END;
  }
  return ret;
}

// 对外接口：每次调用返回一个去重后的 ObTabletBlockInfo。
// 内部从 cur_block_infos_ 数组顺序取出，按 target_type_ 过滤，
// 通过 dedup_set_ 排除已返回过的 macro_id。当前版本耗尽时自动切换下一个版本。
//
// 【线程安全】非线程安全，调用方需保证单线程消费；详见类注释。
int ObSSMetaTabletMacroIDIterator::get_next(ObTabletBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  block_info.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    bool got_new = false;
    while (OB_SUCC(ret) && !got_new) {
      if (cur_block_idx_ >= cur_block_infos_.count()) {
        if (OB_FAIL(load_next_version_blocks_())) {
          if (OB_ITER_END == ret) {
            iter_end_ = true;
          } else {
            iter_end_ = true;
            LOG_WARN("failed to load next version blocks", K(ret),
                     K_(tenant_id), K_(ls_id), K_(tablet_id), K_(cur_version_idx));
          }
          break;
        }
      }
      const ObTabletBlockInfo &cur_info = cur_block_infos_.at(cur_block_idx_);
      cur_block_idx_++;

      if (target_type_ != ObTabletMacroType::MAX && cur_info.block_type() != target_type_) {
        continue;
      }
      int hash_ret = dedup_set_.exist_refactored(cur_info.macro_id());
      if (OB_HASH_EXIST == hash_ret) {
        continue;
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        if (OB_FAIL(dedup_set_.set_refactored(cur_info.macro_id()))) {
          LOG_WARN("failed to insert into dedup set", K(ret), K(cur_info));
        } else {
          block_info = cur_info;
          got_new = true;
        }
      } else {
        ret = hash_ret;
        LOG_WARN("dedup set exist check failed", K(ret), K(cur_info));
      }
    }
  }
  return ret;
}

// ============================================================================
// ObSSMetaTabletTinyInfoIterator
// ============================================================================
ObSSMetaTabletTinyInfoIterator::ObSSMetaTabletTinyInfoIterator()
    : ObSSMetaIterator(ObSSMetaIterator::ObSSMetaTabletTinyInfoIter),
      is_inited_(false),
      skip_mark_delete_(false),
      ls_id_(),
      inner_iter_(),
      tmp_alloc_("TTinyInfo")
{
}

ObSSMetaTabletTinyInfoIterator::~ObSSMetaTabletTinyInfoIterator()
{
}

int ObSSMetaTabletTinyInfoIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObString empty_prefix;
    ObSSMetaVersionFilter version_filter;
    version_filter.set_dedup_by_prefix();
    version_filter.set_desc_order();
    version_filter.set_upper_bound(upper_bound);
    if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                                 meta_type, empty_prefix, version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id), K(upper_bound));
    } else {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMetaTabletTinyInfoIterator::get_next(
    ObSSMetaTabletKey &key,
    ObSSMetaTabletTinyInfo &tiny_info)
{
  int ret = OB_SUCCESS;
  key.reset();
  tiny_info.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      tmp_alloc_.reuse();
      uint64_t row_tenant_id = OB_INVALID_ID;
      ObLSID row_ls_id;
      ObSSMetaBaseType row_meta_type;
      ObString row_meta_key;
      ObString row_meta_value;
      bool is_mark_delete = false;
      SCN head_deleted_scn;
      ObTabletID tablet_id;
      SCN reorgan_scn;
      SCN meta_scn;
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      if (OB_FAIL(inner_iter_.get_next(tmp_alloc_, row_tenant_id, row_ls_id, row_meta_type,
                                       row_meta_key, row_meta_value))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("inner get_next failed", K(ret));
        }
      } else if (OB_FAIL(ObSSMetaHeadValue::get_mark_delete_info(
                     row_meta_value, is_mark_delete, head_deleted_scn))) {
        LOG_WARN("get_mark_delete_info failed", K(ret));
      } else if (skip_mark_delete_ && is_mark_delete) {
        continue;
      } else if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(row_meta_key, tablet_id,
                  reorgan_scn, meta_scn))) {
        LOG_WARN("failed to parse tablet meta key", KR(ret), K(row_meta_key));
      } else if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(ls_id_, tablet_id, reorgan_scn,
                  row_meta_value, tmp_alloc_, tablet_handle))) {
        LOG_WARN("failed to build tablet handle", KR(ret), K(ls_id_), K(tablet_id));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet handle is null", KR(ret), K(ls_id_), K(tablet_id));
      } else {
        key.ls_id_ = ls_id_;
        key.tablet_id_ = tablet_id;
        key.reorganization_scn_ = reorgan_scn;
        key.meta_scn_ = meta_scn;
        tiny_info.is_mark_delete_ = is_mark_delete;
        tiny_info.deleted_scn_ = head_deleted_scn;
        tiny_info.max_replayed_scn_ = tablet->get_tablet_meta().get_max_replayed_scn();
        tiny_info.min_major_snapshot_ = tablet->get_min_major_snapshot_version();
        tiny_info.max_major_snapshot_ = tablet->get_last_major_snapshot_version();
      }
      break;
    }
  }
  return ret;
}

int ObSSMetaTabletTinyInfoIterator::init(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganize_scn,
    const SCN &upper_bound,
    const bool skip_mark_delete)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()
             || !tablet_id.is_valid() || !reorganize_scn.is_valid()
             || !upper_bound.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id),
             K(tablet_id), K(reorganize_scn), K(upper_bound));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    version_filter.set_upper_bound(upper_bound);

    ObSqlString start_key;
    // 使用单个 tablet 的 key 前缀，遍历该 tablet <= upper_bound 的所有版本行。
    if (OB_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(ls_id, tablet_id,
                                                             reorganize_scn, start_key))) {
      LOG_WARN("failed to build prefix", K(ret), K(ls_id), K(tablet_id), K(reorganize_scn));
    } else if (OB_FAIL(inner_iter_.init(sql_client, tenant_id, ls_id,
                   meta_type, start_key.string(), version_filter))) {
      LOG_WARN("failed to init inner iterator", K(ret), K(tenant_id), K(ls_id),
               K(tablet_id), K(reorganize_scn), K(upper_bound));
    } else {
      ls_id_ = ls_id;
      skip_mark_delete_ = skip_mark_delete;
      is_inited_ = true;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
