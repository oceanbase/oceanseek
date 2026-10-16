/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/ob_ss_local_cache_stat.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "lib/stat/ob_diagnostic_info_guard.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

void ObSSLocalCacheStat::update_local_cache_hit_stat(const bool need_read_macro_cache, const bool is_macro_cache_hit,
  const MacroBlockId &macro_id, const int64_t delta_cnt, const int64_t delta_bytes)
{
  const bool is_hit = is_macro_cache_hit;
  if (is_hit) {
    local_cache_stat_.hit_stat_.update_cache_hit(delta_cnt, delta_bytes);
  } else {
    local_cache_stat_.hit_stat_.update_cache_miss(delta_cnt, delta_bytes);
  }
  if (need_read_macro_cache) {
    const bool is_tmp_file = SSObjUtil::is_tmp_file(macro_id);
    if (is_tmp_file) {
      macro_cache_stat_.update_tmpfile_hit_stat(is_macro_cache_hit, delta_cnt, delta_bytes);
    } else {
      ObSSMacroBlockType block_type = ObSSMacroBlockType::MAX_TYPE;
      SSObjUtil::get_ss_macro_block_type(macro_id, block_type);
      if (OB_LIKELY(is_valid_ss_macro_block_type(block_type))) {
        macro_cache_stat_.update_macro_block_hit_stat(block_type, is_macro_cache_hit, delta_cnt, delta_bytes);
      }
    }
  }
}

void ObSSLocalCacheDiagnoseInfo::reset()
{
  MEMSET(mod_name_, '\0', OB_SS_DIAGNOSE_MOD_NAME_LENGTH);
  MEMSET(sub_mod_name_, '\0', OB_SS_DIAGNOSE_MOD_NAME_LENGTH);
  MEMSET(diagnose_info_, '\0', OB_DIAGNOSE_INFO_LENGTH);
  MEMSET(extra_info_, '\0', OB_DIAGNOSE_INFO_LENGTH);
  status_ = ObSSLocalCacheModStatus::INVALID_STATUS;
  modify_time_ = 0;
}

int ObSSLocalCacheDiagnoseInfo::init(
    const char *mod_name, 
    const char *sub_mod_name, 
    const ObSSLocalCacheModStatus &status,
    const char *diagnose_info,
    const char *extra_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mod_name) || OB_ISNULL(sub_mod_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mod_name), KP(sub_mod_name));
  } else if (OB_FAIL(databuff_printf(mod_name_, OB_SS_DIAGNOSE_MOD_NAME_LENGTH, "%s", mod_name))) {
    LOG_WARN("fail to print mod_name", KR(ret), K(mod_name));
  } else if (OB_FAIL(databuff_printf(sub_mod_name_, OB_SS_DIAGNOSE_MOD_NAME_LENGTH, "%s", sub_mod_name))) {
    LOG_WARN("fail to print sub_mod_name", KR(ret), K(sub_mod_name));
  } else if (OB_FAIL(update(status, diagnose_info, extra_info))) {
    LOG_WARN("fail to update diagnose info", KR(ret), KPC(this));
  }
  return ret;
}

int ObSSLocalCacheDiagnoseInfo::update(
    const ObSSLocalCacheModStatus &status,
    const char *diagnose_info,
    const char *extra_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSLocalCacheModStatus::INVALID_STATUS == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status));
  } else {
    status_ = status;
    modify_time_ = ObTimeUtility::current_time_us();
    if (nullptr != diagnose_info) {
      if (OB_TMP_FAIL(databuff_printf(diagnose_info_, OB_DIAGNOSE_INFO_LENGTH, "%s", diagnose_info))) {
        LOG_WARN("fail to print diagnose_info", KR(tmp_ret), K(diagnose_info));
        tmp_ret = OB_SUCCESS;
      }
    }
    if (nullptr != extra_info) {
      if (OB_TMP_FAIL(databuff_printf(extra_info_, OB_DIAGNOSE_INFO_LENGTH, "%s", extra_info))) {
        LOG_WARN("fail to print extra_info", KR(tmp_ret), K(extra_info));
        tmp_ret = OB_SUCCESS;
      }
    }

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("diagnose info should be valid", KR(ret), K(status), KPC(this));
    }
  }
  return ret;
}

bool ObSSLocalCacheDiagnoseInfo::is_valid() const
{
  return (ObSSLocalCacheModStatus::INVALID_STATUS != status_ && modify_time_ > 0 &&
          strlen(mod_name_) > 0 && strlen(sub_mod_name_) > 0);
}

bool ObSSLocalCacheDiagnoseInfo::match(const char *mod_name, const char *sub_mod_name) const
{
  return ((nullptr != mod_name) && (nullptr != sub_mod_name) && (0 == strcmp(mod_name, mod_name_)) &&
          (0 == strcmp(sub_mod_name, sub_mod_name_)));
}

ObSSLocalCacheDiagnoseStat::ObSSLocalCacheDiagnoseStat()
  : lock_(ObLatchIds::OB_SS_MEM_BLOCK_MANAGER_LOCK), diagnose_info_arr_()
{}

ObSSLocalCacheDiagnoseStat::~ObSSLocalCacheDiagnoseStat()
{
  reset();
}

int ObSSLocalCacheDiagnoseStat::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    diagnose_info_arr_.set_attr(ObMemAttr(tenant_id, "LCacheDiagList"));
  }
  return ret;
}

void ObSSLocalCacheDiagnoseStat::reset()
{
  SpinWLockGuard guard(lock_);
  diagnose_info_arr_.reset();
}

void ObSSLocalCacheDiagnoseStat::add_local_cache_diagnose_info(
    const char *mod_name, 
    const char *sub_mod_name,
    const ObSSLocalCacheModStatus &status, 
    const char *diagnose_info_str, 
    const char *extra_info_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mod_name) || OB_ISNULL(sub_mod_name) || OB_UNLIKELY(ObSSLocalCacheModStatus::INVALID_STATUS == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mod_name), KP(sub_mod_name), K(status));
  } else {
    SpinWLockGuard guard(lock_);
    int64_t diagnose_info_idx = -1;
    if (OB_FAIL(inner_get_diagnose_info_idx(mod_name, sub_mod_name, diagnose_info_idx))) {
      LOG_WARN("fail to get diagnose info idx", KR(ret), K(mod_name), K(sub_mod_name));
    } else if (-1 != diagnose_info_idx) {
      ObSSLocalCacheDiagnoseInfo &diagnose_info = diagnose_info_arr_.at(diagnose_info_idx);
      if (OB_FAIL(diagnose_info.update(status, diagnose_info_str, extra_info_str))) {
        LOG_WARN("fail to update diagnose info", KR(ret), K(status), K(diagnose_info_idx), 
          K(diagnose_info_arr_.count()), K(diagnose_info));
      }
    } else {
      ObSSLocalCacheDiagnoseInfo diagnose_info;
      if (OB_FAIL(diagnose_info.init(mod_name, sub_mod_name, status, diagnose_info_str, extra_info_str))) {
        LOG_WARN("fail to init diagnose info", KR(ret), K(mod_name), K(sub_mod_name), K(status));
      } else if (OB_FAIL(diagnose_info_arr_.push_back(diagnose_info))) {
        LOG_WARN("fail to push back", KR(ret), K(diagnose_info_arr_.count()), K(diagnose_info));
      }
    }
  }
}

int ObSSLocalCacheDiagnoseStat::inner_get_diagnose_info_idx(
    const char *mod_name, 
    const char *sub_mod_name, 
    int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  const int64_t info_cnt = diagnose_info_arr_.count();
  if (OB_ISNULL(mod_name) || OB_ISNULL(sub_mod_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(mod_name), KP(sub_mod_name));
  } else if (info_cnt > 0) {
    for (int64_t i = 0; (i < info_cnt) && (-1 == idx); ++i) {
      if (diagnose_info_arr_.at(i).match(mod_name, sub_mod_name)) {
        idx = i;
      }
    }
  }
  return ret;
}

int ObSSLocalCacheDiagnoseStat::get_diagnose_info_list(common::ObIArray<ObSSLocalCacheDiagnoseInfo> &info_list)
{
  int ret = OB_SUCCESS;
  info_list.reset();
  SpinRLockGuard guard(lock_);
  const int64_t info_cnt = diagnose_info_arr_.count();
  if (info_cnt > 0) {
    if (OB_FAIL(info_list.reserve(info_cnt))) {
      LOG_WARN("fail to reserve", KR(ret), K(info_cnt));
    } else if (OB_FAIL(info_list.assign(diagnose_info_arr_))) {
      LOG_WARN("fail to assign", KR(ret), K(info_cnt));
    }
  }
  return ret;
}

void inc_local_cache_prewarm_stat(
    const ObSSLocalCachePrewarmType prewarm_type,
    const int64_t delta_cnt,
    const int64_t delta_bytes)
{
  ObStatEventIds::ObStatEventIdEnum cnt_id = ObStatEventIds::STAT_EVENT_ADD_END;
  ObStatEventIds::ObStatEventIdEnum bytes_id = ObStatEventIds::STAT_EVENT_ADD_END;
  switch (prewarm_type) {
    case ObSSLocalCachePrewarmType::MAJOR_COMPACTION: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_MAJOR_COMPACTION_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_MAJOR_COMPACTION_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::MIGRATION: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_HA_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_HA_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::DDL: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_DDL_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_DDL_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::STORAGE_CACHE_POLICY: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_SCP_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_SCP_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::MINOR_COMPACTION: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_MINOR_COMPACTION_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_MINOR_COMPACTION_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::PREREAD_TASK: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_PREREAD_TASK_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_PREREAD_TASK_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::SPLIT_PART: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_SPLIT_PART_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_SPLIT_PART_PREWARM_BYTES;
      break;
    }
    case ObSSLocalCachePrewarmType::TRY_WRITE_LCACHE: {
      cnt_id = ObStatEventIds::LOCAL_CACHE_TRY_WRITE_LCACHE_PREWARM;
      bytes_id = ObStatEventIds::LOCAL_CACHE_TRY_WRITE_LCACHE_PREWARM_BYTES;
      break;
    }
    default: {
      break;
    }
  }
  if (cnt_id < ObStatEventIds::STAT_EVENT_ADD_END && delta_cnt > 0 && delta_bytes > 0
      && oceanbase::lib::is_diagnose_info_enabled()) {
    oceanbase::common::ObLocalDiagnosticInfo::add_stat(cnt_id, delta_cnt);
    oceanbase::common::ObLocalDiagnosticInfo::add_stat(bytes_id, delta_bytes);
  }
}

} // namespace storage
} // namespace oceanbase
 
 