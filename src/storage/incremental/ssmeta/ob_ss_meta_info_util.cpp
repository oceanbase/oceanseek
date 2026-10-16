/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ss_meta_info_util.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSSMetaInfoUtil::build_ls_ckpt_key(
    const ObLSID &ls_id,
    char *buf,
    const int64_t buf_len,
    int64_t &key_len)
{
  int ret = OB_SUCCESS;
  key_len = 0;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", KR(ret), K(ls_id));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt key buffer", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(
      buf, buf_len, key_len, SS_LS_CKPT_KEY_FMT, ls_id.id()))) {
    LOG_WARN("build ls ckpt key failed", KR(ret), K(ls_id), K(buf_len));
  }
  return ret;
}

const char *ObSSMetaInfoUtil::find_marker(
    const ObString &meta_key,
    const char *marker)
{
  const char *result = nullptr;
  const char *p = meta_key.ptr();
  const char *end = p + meta_key.length();
  const int64_t marker_len = strlen(marker);
  bool found = false;
  for (; !found && p + marker_len <= end; ++p) {
    if (0 == MEMCMP(p, marker, marker_len)) {
      result = p;
      found = true;
    }
  }
  return result;
}

int ObSSMetaInfoUtil::extract_prefix(
    const ObString &meta_key,
    ObString &prefix,
    const char *marker)
{
  int ret = OB_SUCCESS;
  prefix.reset();
  if (OB_ISNULL(meta_key.ptr()) || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_key is invalid", K(ret), K(meta_key));
  } else if (OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("marker is null", KR(ret));
  } else {
    const char *pos = find_marker(meta_key, marker);
    if (OB_NOT_NULL(pos)) {
      prefix.assign_ptr(meta_key.ptr(),
                        static_cast<ObString::obstr_size_t>(pos - meta_key.ptr()));
    } else {
      prefix = meta_key;
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::extract_scn_value(
    const ObString &meta_key,
    uint64_t &scn_val,
    const char *marker)
{
  int ret = OB_SUCCESS;
  scn_val = 0;
  if (OB_ISNULL(meta_key.ptr()) || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_key is invalid", K(ret), K(meta_key));
  } else if (OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("marker is null", K(ret));
  } else {
    const char *pos = find_marker(meta_key, marker);
    if (OB_ISNULL(pos)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("marker not found in meta_key", K(ret), K(meta_key));
    } else {
      const char *scn_start = pos + strlen(marker);
      const char *end = meta_key.ptr() + meta_key.length();
      if (scn_start + SSMETA_SCN_DIGITS > end) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("meta_key too short for scn digits", K(ret), K(meta_key),
                 "remaining_len", end - scn_start);
      } else {
        uint64_t val = 0;
        bool parse_ok = true;
        for (int64_t i = 0; parse_ok && i < SSMETA_SCN_DIGITS; ++i) {
          if (scn_start[i] < '0' || scn_start[i] > '9') {
            parse_ok = false;
          } else {
            val = val * 10 + (scn_start[i] - '0');
          }
        }
        if (!parse_ok) {
          ret = OB_INVALID_DATA;
          LOG_WARN("scn digits contain non-numeric character", K(ret), K(meta_key));
        } else {
          scn_val = val;
        }
      }
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::parse_tablet_meta_key(
    const ObString &meta_key,
    ObTabletID &tablet_id,
    SCN &reorgan_scn,
    SCN &meta_scn)
{
  ObLSID ls_id;
  return parse_tablet_meta_key(meta_key, ls_id, tablet_id, reorgan_scn, meta_scn);
}

int ObSSMetaInfoUtil::parse_tablet_meta_key(
    const ObString &meta_key,
    ObLSID &ls_id,
    ObTabletID &tablet_id,
    SCN &reorgan_scn,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  tablet_id.reset();
  reorgan_scn.reset();
  meta_scn.reset();

  static const int64_t MAX_META_KEY_LEN = 512;
  char path_buf[MAX_META_KEY_LEN] = {0};
  blocksstable::MacroBlockId macro_id;

  if (OB_ISNULL(meta_key.ptr()) || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_key is invalid", K(ret), K(meta_key));
  } else if (OB_UNLIKELY(meta_key.length() >= MAX_META_KEY_LEN)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("meta_key too long", K(ret), K(meta_key), K(meta_key.length()));
  } else {
    MEMCPY(path_buf, meta_key.ptr(), meta_key.length());
    path_buf[meta_key.length()] = '\0';
    if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
            blocksstable::ObStorageObjectType::SHARED_TABLET_META,
            path_buf, macro_id))) {
      LOG_WARN("fail to parse meta_key to macro_id", K(ret), K(meta_key));
    } else {
      tablet_id = ObTabletID(static_cast<uint64_t>(macro_id.second_id()));
      meta_scn.convert_for_logservice(static_cast<uint64_t>(macro_id.meta_scn()));
      if (macro_id.meta_is_inner_tablet()) {
        ls_id = ObLSID(macro_id.ls_id());
        reorgan_scn = SCN::min_scn();
      } else {
        reorgan_scn.convert_for_logservice(static_cast<uint64_t>(macro_id.reorganization_scn()));
      }
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::parse_ls_meta_key(
    const ObString &meta_key,
    ObLSID &ls_id,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  meta_scn.reset();

  static const int64_t MAX_META_KEY_LEN = 512;
  char path_buf[MAX_META_KEY_LEN] = {0};
  blocksstable::MacroBlockId macro_id;

  if (OB_ISNULL(meta_key.ptr()) || meta_key.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_key is invalid", K(ret), K(meta_key));
  } else if (OB_UNLIKELY(meta_key.length() >= MAX_META_KEY_LEN)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("meta_key too long", K(ret), K(meta_key), K(meta_key.length()));
  } else {
    MEMCPY(path_buf, meta_key.ptr(), meta_key.length());
    path_buf[meta_key.length()] = '\0';
    if (OB_FAIL(blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
            blocksstable::ObStorageObjectType::SHARED_LS_META,
            path_buf, macro_id))) {
      LOG_WARN("fail to parse ls meta_key to macro_id", K(ret), K(meta_key));
    } else {
      ls_id = ObLSID(static_cast<int64_t>(macro_id.second_id()));
      meta_scn.convert_for_logservice(static_cast<uint64_t>(macro_id.meta_scn()));
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    ObSqlString &prefix)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (tablet_id.is_ls_inner_tablet()) {
    // inner_tablet 不依赖 reorgan_scn，但必须有合法 ls_id
    if (OB_UNLIKELY(!ls_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ls_id for inner tablet", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(prefix.assign_fmt(SSMETA_INNER_TABLET_PREFIX_FMT,
                   ls_id.id(),
                   ObFileHelper::get_ls_inner_tablet_name(tablet_id.id())))) {
      LOG_WARN("fail to build inner tablet prefix", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (reorgan_scn.is_valid()) {
    if (OB_FAIL(prefix.assign_fmt(SSMETA_TABLET_PREFIX_FMT,
                   tablet_id.id(),
                   reorgan_scn.get_val_for_inner_table_field()))) {
      LOG_WARN("fail to build tablet prefix", KR(ret), K(tablet_id), K(reorgan_scn));
    }
  } else {
    // reorgan_scn 非法，仅生成到 tablet_id 层的前缀
    if (OB_FAIL(prefix.assign_fmt(SSMETA_TABLET_ID_PREFIX_FMT, tablet_id.id()))) {
      LOG_WARN("fail to build tablet id prefix", KR(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::gen_meta_key_prefix(
    const ObSSMetaReadParamV2 &read_param,
    ObSqlString &prefix)
{
  int ret = OB_SUCCESS;
  const bool is_ls_type = (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META
                           || read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);
  if (OB_UNLIKELY(!read_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param));
  } else if (is_ls_type) { // true, 表示要获取ls_meta/ls_ckpt；否则表示要获取tablet_meta
    if (OB_FAIL(prefix.assign_fmt(SSMETA_LS_PREFIX_FMT, read_param.ls_key_.ls_id_.id()))) {
      LOG_WARN("fail to build ls prefix", KR(ret), K(read_param));
    }
  } else if (read_param.read_param_type_ == ObSSMetaReadParamType::LS_PREFIX) {
    // tablet_meta_key的format： "tablet/<tablet_id>/<reorgan_scn>/meta_scn"，不带 ls 维度。
    // LS_PREFIX是指以tenant_id、ls_id为前缀（对应TABLET_PREFIX是以tenant_id、tablet_id、reorgan_scn为前缀）
    // 因此这里不需要prefix，底层sql会直接以tenant_id和ls_id这两列进行过滤。
    prefix.reset();
  } else if (OB_FAIL(gen_tablet_meta_key_prefix(read_param.tablet_key_.ls_id_,
                                                read_param.tablet_key_.tablet_id_,
                                                read_param.tablet_key_.reorganization_scn_,
                                                prefix))) {
    LOG_WARN("fail to build tablet prefix", KR(ret), K(read_param));
  }
  return ret;
}

int ObSSMetaInfoUtil::gen_tablet_addr(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganization_scn,
    const uint64_t op_id,
    const int64_t tablet_buffer_size,
    ObMetaDiskAddr &tablet_addr)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  MacroBlockId block_id;
  const ObStorageObjectType object_type = ObStorageObjectType::SHARED_TABLET_META;
  if (tablet_buffer_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_buffer_size));
  } else {
    opt.set_ss_tablet_meta_opt(object_type,
                               ls_id.id(),
                               tablet_id.id(),
                               op_id,
                               tablet_id.is_ls_inner_tablet(),
                               reorganization_scn.get_val_for_inner_table_field(),
                               true /*is_in_table*/);
    if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, block_id))) {
      LOG_WARN("failed to generate obj id", K(ret), K(opt), K(block_id));
    } else if (OB_FAIL(tablet_addr.set_block_addr(block_id,
                                                  0,
                                                  tablet_buffer_size,
                                                  ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
      LOG_WARN("set block addr failed", K(ret), K(block_id));
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::build_tablet_handle(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganization_scn,
    const ObString &meta_value,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  ObSSUpdateTabletLog update_log;
  return build_tablet_handle(ls_id, tablet_id, reorganization_scn, meta_value,
      allocator, tablet_handle, update_log);
}

int ObSSMetaInfoUtil::build_tablet_handle(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorganization_scn,
    const ObString &meta_value,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr tablet_addr;
  int64_t pos = 0;
  if (OB_FAIL(gen_tablet_addr(ls_id, tablet_id, reorganization_scn,
                              0/*op_id*/, meta_value.length(), tablet_addr))) {
    LOG_WARN("fail to gen tablet_addr", KR(ret), K(ls_id), K(tablet_id), K(reorganization_scn), K(meta_value));
  } else if (OB_FAIL(ObSSTabletCreateHelper::get_ss_meta_tablet_handle_by_buf(tablet_addr,
             meta_value.ptr(), meta_value.length(), pos, tablet_handle, update_log, allocator))) {
    LOG_WARN("fail to build tablet_handle from meta_value", KR(ret), K(ls_id), K(tablet_id), K(tablet_addr),
      K(meta_value));
  }
  return ret;
}

int ObSSMetaInfoUtil::get_ssmeta_trans_timeout_us(const uint64_t tenant_id, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  timeout_us = SSMETA_TABLE_SQL_TIMEOUT_US;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      timeout_us = tenant_config->_shared_meta_trans_timeout;
    }
  }
  return ret;
}

int ObSSMetaInfoUtil::get_ls_ckpt_timeout_us(const uint64_t tenant_id, int64_t &timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_LIKELY(tenant_config.is_valid())) {
      timeout_us = tenant_config->_shared_sys_meta_ckpt_interval;
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
