/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_table_helper.h"
#include "ob_ss_meta_iter_wrapper.h"
#include "ob_ss_meta_info_util.h"
#include "observer/ob_server_struct.h"
#include "lib/string/ob_sql_string.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace
{
bool has_valid_table_store_meta_info(const ObSSTabletTableStoreMetaInfo &table_store_meta_info)
{
  // is_inited_ is intentionally not serialized, so judge by serialized fields.
  return table_store_meta_info.major_tables_cnt_ >= 0
      && table_store_meta_info.minor_tables_cnt_ >= 0
      && table_store_meta_info.ddl_tables_cnt_ >= 0
      && table_store_meta_info.meta_major_tables_cnt_ >= 0
      && table_store_meta_info.uncommitted_inc_major_cnt_ >= 0;
}
}

int ObSSMetaTableHelper::get_meta_info(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObArenaAllocator &allocator,
    ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  ObSSMetaType meta_type;
  ObSSMetaVersionFilter version_filter;
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  version_filter.set_upper_bound(read_snapshot);
  ObSqlString prefix;
  ObSSMetaRawRowIter iter;

  const bool is_ls_type = (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META
                           || read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);
  if (OB_UNLIKELY(!read_param.is_valid() || !read_param.read_prefix())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else {
    if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    } else if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    } else {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
    LOG_WARN("fail to gen meta key prefix", KR(ret), K(read_param));
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, MTL_ID(),
                                is_ls_type ? read_param.ls_key_.ls_id_ : read_param.tablet_key_.ls_id_,
                                meta_type, prefix.string(), version_filter))) {
    LOG_WARN("fail to init raw row iter", KR(ret), K(read_param));
  } else if (OB_FAIL(iter.get_next(allocator, row))) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get next row from iter", KR(ret), K(read_param));
  }
  LOG_TRACE("get_meta_info", KR(ret), K(read_param), K(read_snapshot));
  return ret;
}

int ObSSMetaTableHelper::get_meta_info(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObArenaAllocator &allocator,
    ObSSMetaOpBuffer &meta_value,
    share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  meta_scn.reset();
  ObSSRawMetaRow row;
  if (OB_FAIL(get_meta_info(read_param, read_snapshot, allocator, row))) {
    LOG_WARN("fail to get meta info row", KR(ret), K(read_param), K(read_snapshot));
  } else {
    uint64_t scn_val = 0;
    if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val))) {
      LOG_WARN("fail to extract scn from meta_key", KR(ret), K(row.meta_key_));
    } else {
      meta_scn.convert_for_logservice(scn_val);
      const int64_t data_len = row.meta_value_.length();
      char *buf = static_cast<char *>(share::mtl_malloc(data_len, "MetaInfoBuf"));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc meta_value buf", KR(ret), K(data_len));
      } else {
        MEMCPY(buf, row.meta_value_.ptr(), data_len);
        meta_value.assign(buf, data_len);
      }
    }
  }
  LOG_TRACE("get_meta_info", KR(ret), K(read_param), K(meta_scn));
  return ret;
}

int ObSSMetaTableHelper::get_tablet(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObSSMetaUpdateMetaInfo &update_meta_info,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  meta_scn.reset();
  ObSSMetaTabletIter iter;
  SCN reorganization_scn;
  SCN meta_version;

  if (OB_UNLIKELY(!read_param.is_valid() || !read_param.read_prefix())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_,
                                read_param.tablet_key_.tablet_id_,
                                read_param.tablet_key_.reorganization_scn_,
                                read_snapshot,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet iter", KR(ret), K(read_snapshot), K(read_param));
  } else if (OB_FAIL(iter.get_next(allocator, tablet_handle, update_meta_info, reorganization_scn, meta_version))) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get next tablet from iter", KR(ret), K(read_param));
  } else {
    meta_scn = meta_version;
  }
  LOG_TRACE("get_tablet", KR(ret), K(read_param), K(meta_scn));
  return ret;
}

int ObSSMetaTableHelper::get_tablet(
    const ObSSMetaReadParamV2 &read_param,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc("GetTablet");
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  if (OB_UNLIKELY(!read_param.is_valid() || !read_param.read_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else {
    ObSSMetaTabletKey tablet_key;
    tablet_key.ls_id_ = read_param.tablet_key_.ls_id_;
    tablet_key.tablet_id_ = read_param.tablet_key_.tablet_id_;
    tablet_key.reorganization_scn_ = read_param.tablet_key_.reorganization_scn_;
    tablet_key.meta_scn_ = read_param.tablet_key_.meta_scn_;
    blocksstable::MacroBlockId macro_id = tablet_key.convert_to_macro_id(true/*is_in_table*/);
    char path_buf[SSMETA_PATH_MAX_LEN] = {0};
    int64_t pos = 0;
    ObSSMetaTableOperator op;
    if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
            macro_id, path_buf, sizeof(path_buf), pos))) {
      LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id), K(tablet_key));
    } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
      LOG_WARN("fail to init ss meta operator", KR(ret));
    } else {
      ObString meta_key(pos, path_buf);
      ObString meta_value;
      if (OB_FAIL(op.read_row(MTL_ID(), read_param.tablet_key_.ls_id_, meta_type,
                              meta_key, tmp_alloc, meta_value))) {
        LOG_WARN("fail to read tablet meta row", KR(ret), K(read_param), K(meta_key));
      } else if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(
                     read_param.tablet_key_.ls_id_,
                     read_param.tablet_key_.tablet_id_,
                     read_param.tablet_key_.reorganization_scn_,
                     meta_value, allocator, tablet_handle))) {
        LOG_WARN("fail to build tablet handle", KR(ret), K(read_param));
      }
    }
  }
  LOG_TRACE("get_tablet by key", KR(ret), K(read_param));
  return ret;
}

int ObSSMetaTableHelper::get_tablet_scn(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  meta_scn.reset();
  ObSSMetaTabletSCNIterator iter;
  SCN scn;

  if (OB_UNLIKELY(!read_param.is_valid() || !read_param.read_prefix())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_,
                                read_param.tablet_key_.tablet_id_,
                                read_param.tablet_key_.reorganization_scn_,
                                read_snapshot,
                                true /*desc_scn_order*/,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet scn iter", KR(ret), K(read_snapshot), K(read_param));
  } else if (OB_FAIL(iter.get_next(scn))) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get next scn from iter", KR(ret), K(read_param));
  } else {
    meta_scn = scn;
  }
  LOG_INFO("get_tablet_scn", KR(ret), K(read_param), K(meta_scn));
  return ret;
}

int ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
    const ObSSMetaReadParamV2 &read_param,
    int64_t &last_major_snapshot_version,
    bool &is_mark_delete,
    ObSSMetaOpBuffer *meta_value)
{
  int ret = OB_SUCCESS;
  last_major_snapshot_version = -1;
  is_mark_delete = false;
  ObArenaAllocator tmp_alloc("GetMajSnap");
  ObSSMetaType meta_type;
  ObSSMetaVersionFilter version_filter;
  ObSqlString prefix;
  ObSSMetaRawRowIter iter;
  ObSSRawMetaRow row;

  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  version_filter.set_dedup_by_prefix();
  version_filter.set_desc_order();
  version_filter.set_upper_bound(SCN::max_scn());

  if (OB_UNLIKELY(!read_param.is_valid()
                  || ObAtomicProtocolTypeEnum::TABLET_META != read_param.protocol_type_
                  || !read_param.tablet_key_.ls_id_.is_valid()
                  || !read_param.tablet_key_.tablet_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
    LOG_WARN("fail to gen tablet meta key prefix", KR(ret), K(read_param));
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_,
                                meta_type,
                                prefix.string(),
                                version_filter,
                                !read_param.read_mark_delete_ /*skip_mark_delete*/))) {
    LOG_WARN("fail to init raw row iter", KR(ret), K(read_param), K(prefix));
  } else if (OB_FAIL(iter.get_next(tmp_alloc, row))) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    LOG_WARN("fail to get latest tablet meta row", KR(ret), K(read_param), K(prefix));
  } else {
    int64_t pos = 0;
    ObSSMetaHeadValue head_value;
    ObSSTabletTableStoreMetaInfo table_store_meta_info;
    if (OB_FAIL(head_value.deserialize_value_info(row.meta_value_.ptr(), row.meta_value_.length(), pos))) {
      LOG_WARN("fail to deserialize tablet meta head value", KR(ret), K(row.meta_value_.length()));
    } else {
      is_mark_delete = head_value.get_meta_head_info().is_mark_deleted();
      head_value.get_meta_head_info().get_table_store_meta(table_store_meta_info);
      if (has_valid_table_store_meta_info(table_store_meta_info)) {
        last_major_snapshot_version = table_store_meta_info.last_major_snapshot_version_;
      } else {
        ObTabletHandle tablet_handle;
        if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(read_param.tablet_key_.ls_id_,
                                                          read_param.tablet_key_.tablet_id_,
                                                          read_param.tablet_key_.reorganization_scn_,
                                                          row.meta_value_,
                                                          tmp_alloc,
                                                          tablet_handle))) {
          LOG_WARN("fail to build tablet handle", KR(ret), K(read_param), K(row));
        } else if (OB_ISNULL(tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null tablet", KR(ret), K(read_param));
        } else {
          last_major_snapshot_version = tablet_handle.get_obj()->get_last_major_snapshot_version();
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(meta_value)) {
        if (OB_NOT_NULL(meta_value->ori_buf_)) {
          share::mtl_free(meta_value->ori_buf_);
          meta_value->reset();
        }
        char *buf = static_cast<char *>(share::mtl_malloc(row.meta_value_.length(), "GetMajSnap"));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc meta value buf", KR(ret), K(row.meta_value_.length()));
        } else {
          MEMCPY(buf, row.meta_value_.ptr(), row.meta_value_.length());
          meta_value->assign(buf, row.meta_value_.length());
        }
      }
    }
  }
  LOG_INFO("get_tablet_last_major_snapshot_version", KR(ret), K(read_param),
           K(last_major_snapshot_version), K(is_mark_delete));
  return ret;
}

int ObSSMetaTableHelper::get_ls(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObDefaultSSMetaValue<ObSSLSMeta> &ls_value,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  meta_scn.reset();
  ObArenaAllocator tmp_alloc("GetLS");
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObSSMetaVersionFilter version_filter;
  version_filter.set_desc_order();
  version_filter.set_dedup_by_prefix();
  version_filter.set_upper_bound(read_snapshot);
  ObSqlString prefix;
  ObSSMetaRawRowIter iter;

  if (OB_UNLIKELY(!read_param.is_valid() || !read_param.read_prefix())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
    LOG_WARN("fail to gen meta key prefix", KR(ret), K(read_param));
  } else if (OB_FAIL(iter.init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.ls_key_.ls_id_,
                                meta_type, prefix.string(), 
                                version_filter, !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init raw row iter", KR(ret), K(read_param));
  } else {
    ObSSRawMetaRow row;
    if (OB_FAIL(iter.get_next(tmp_alloc, row))) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      LOG_WARN("fail to get next row from iter", KR(ret), K(read_param));
    } else {
      uint64_t scn_val = 0;
      if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val))) {
        LOG_WARN("fail to extract scn from meta_key", KR(ret), K(row.meta_key_));
      } else {
        int64_t pos = 0;
        if (OB_FAIL(ls_value.deserialize(row.meta_value_.ptr(), row.meta_value_.length(), pos))) {
          LOG_WARN("fail to deserialize ls meta value", KR(ret), K(row.meta_value_.length()));
        } else {
          meta_scn.convert_for_logservice(scn_val);
        }
      }
    }
  }
  LOG_TRACE("get_ls", KR(ret), K(read_param), K(meta_scn));
  return ret;
}

int ObSSMetaTableHelper::get_ls(
    const ObSSMetaReadParamV2 &read_param,
    ObDefaultSSMetaValue<ObSSLSMeta> &ls_value)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc("GetLS");
  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  if (OB_UNLIKELY(!read_param.is_valid() || !read_param.read_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else {
    ObSSMetaLSKey ls_key;
    ls_key.ls_id_ = read_param.ls_key_.ls_id_;
    ls_key.meta_scn_ = read_param.ls_key_.meta_scn_;
    blocksstable::MacroBlockId macro_id = ls_key.convert_to_macro_id(true/*is_in_table*/);
    char path_buf[SSMETA_PATH_MAX_LEN] = {0};
    int64_t pos = 0;
    ObSSMetaTableOperator op;
    if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
            macro_id, path_buf, sizeof(path_buf), pos))) {
      LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id), K(ls_key));
    } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
      LOG_WARN("fail to init ss meta operator", KR(ret));
    } else {
      ObString meta_key(pos, path_buf);
      ObString meta_value;
      if (OB_FAIL(op.read_row(MTL_ID(), read_param.ls_key_.ls_id_, meta_type,
                              meta_key, tmp_alloc, meta_value))) {
        LOG_WARN("fail to read ls meta row", KR(ret), K(read_param), K(meta_key));
      } else {
        int64_t pos = 0;
        if (OB_FAIL(ls_value.deserialize(meta_value.ptr(), meta_value.length(), pos))) {
          LOG_WARN("fail to deserialize ls meta value", KR(ret), K(meta_value.length()));
        }
      }
    }
  }
  LOG_TRACE("get_ls by key", KR(ret), K(read_param));
  return ret;
}

int ObSSMetaTableHelper::delete_meta_info(ObSSMetaDeleteParamV2 &delete_param)
{
  int ret = OB_SUCCESS;
  ObSSMetaType meta_type;
  ObSSMetaTableOperator op;
  ObSqlString key_buf;
  char path_buf[SSMETA_PATH_MAX_LEN] = {0};
  ObString meta_key;
  const bool is_prefix = delete_param.read_prefix();
  const bool is_ls_type = (delete_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META
                           || delete_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);
  delete_param.affected_rows_ = 0;

  if (OB_UNLIKELY(!delete_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(delete_param));
  } else {
    if (delete_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    } else if (delete_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    } else {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
    LOG_WARN("fail to init ss meta operator", KR(ret));
  } else {
    switch (delete_param.read_param_type_) {
    case ObSSMetaReadParamType::TABLET_KEY: {
      ObSSMetaTabletKey tablet_key;
      tablet_key.ls_id_ = delete_param.tablet_key_.ls_id_;
      tablet_key.tablet_id_ = delete_param.tablet_key_.tablet_id_;
      tablet_key.reorganization_scn_ = delete_param.tablet_key_.reorganization_scn_;
      tablet_key.meta_scn_ = delete_param.tablet_key_.meta_scn_;
      blocksstable::MacroBlockId macro_id = tablet_key.convert_to_macro_id(true/*is_in_table*/);
      int64_t pos = 0;
      if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
              macro_id, path_buf, sizeof(path_buf), pos))) {
        LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id), K(delete_param));
      } else {
        meta_key.assign_ptr(path_buf, static_cast<int32_t>(pos));
      }
      break;
    }
    case ObSSMetaReadParamType::LS_KEY: {
      blocksstable::MacroBlockId macro_id;
      if (ObAtomicProtocolTypeEnum::LS_META == delete_param.protocol_type_) {
        ObSSMetaLSKey ls_key;
        ls_key.ls_id_ = delete_param.ls_key_.ls_id_;
        ls_key.meta_scn_ = delete_param.ls_key_.meta_scn_;
        macro_id = ls_key.convert_to_macro_id(true/*is_in_table*/);
      } else if (ObAtomicProtocolTypeEnum::CHECKPOINT == delete_param.protocol_type_) {
        ObSSMetaLSCkptKey ckpt_key;
        ckpt_key.ls_id_ = delete_param.ls_key_.ls_id_;
        ckpt_key.meta_scn_ = delete_param.ls_key_.meta_scn_;
        macro_id = ckpt_key.convert_to_macro_id(true/*is_in_table*/);
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported protocol type for ls key delete", KR(ret), K(delete_param));
      }
      int64_t pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
              macro_id, path_buf, sizeof(path_buf), pos))) {
        LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id), K(delete_param));
      } else {
        meta_key.assign_ptr(path_buf, static_cast<int32_t>(pos));
      }
      break;
    }
    case ObSSMetaReadParamType::TABLET_PREFIX:
    case ObSSMetaReadParamType::LS_PREFIX: {
      if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(delete_param, key_buf))) {
        LOG_WARN("fail to gen meta key prefix", KR(ret), K(delete_param));
      } else {
        meta_key = key_buf.string();
      }
      break;
    }
    case ObSSMetaReadParamType::TENANT_PREFIX: {
      // empty meta_key means delete all rows of this meta_type under the tenant
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported read_param_type for delete", KR(ret), K(delete_param));
      break;
    }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (delete_param.read_param_type_ == ObSSMetaReadParamType::TENANT_PREFIX) {
    int64_t affected_rows = 0;
    if (OB_FAIL(op.delete_rows_by_tenant(MTL_ID(), meta_type, affected_rows))) {
      LOG_WARN("fail to tenant prefix delete", KR(ret), K(delete_param));
    } else {
      delete_param.affected_rows_ = affected_rows;
    }
  } else {
    const ObLSID &ls_id = is_ls_type ? delete_param.ls_key_.ls_id_
                                     : delete_param.tablet_key_.ls_id_;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.delete_row(MTL_ID(), ls_id, meta_type, meta_key,
                              is_prefix, affected_rows))) {
      LOG_WARN("fail to delete meta row", KR(ret), K(delete_param), K(meta_key), K(is_prefix));
    } else {
      delete_param.affected_rows_ = affected_rows;
    }
  }
  LOG_INFO("delete_meta_info", KR(ret), K(is_prefix), K(meta_key), K(delete_param));
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
