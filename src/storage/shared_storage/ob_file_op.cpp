/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_file_op.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/**
 * --------------------------------ObDirCalcSizeOp------------------------------------
 */

ObDirCalcSizeOp::ObDirCalcSizeOp(volatile bool &is_stop, const char *dir)
  : ObScanDirOp(dir),
    is_stop_(is_stop),
    total_file_size_(0),
    hot_tablet_total_file_size_(0),
    del_tmp_seq_file_size_(0),
    tmp_seq_file_size_(0),
    tmp_seq_file_num_(0),
    start_calc_size_time_s_(0),
    macro_cache_type_(ObSSMacroCacheType::MAX_TYPE),
    is_for_del_tmp_seq_files_(false),
    tmp_seq_file_del_secy_time_s_(DEFAULT_TMP_SEQ_FILE_DELETE_SECURITY_TIME_S),
    not_in_meta_map_file_cnt_(0),
    largest_dir_size_(0),
    total_dir_num_(0)
{
  largest_dir_path_[0] = '\0';
  for (int64_t i = 0; i < MAX_DIR_RANGE; i++) {
    dir_nums_[i] = 0;
    dir_sizes_[i] = 0;
  }
}

int64_t ObDirCalcSizeOp::get_dir_size_bucket_index(const int64_t size)
{
  // Buckets: [0,1k),[1k,2k),[2k,4k),[4k,8k),[8k,16k),[16k,32k),[32k,64k),[64k,128k),[128k,256k),[256k,+)
  // if size = 2^n, return n - 9, because 1k=2^10
  
  if (size <= 0) {
    return 0;
  }
  // floor(log2(size))
  int64_t log2_size = 63 - __builtin_clzll(size);
  
  // [0, 1K)    -> index 0 (log2_size < 10)
  // [1K, 2K)   -> index 1 (log2_size = 10)
  // [2K, 4K)   -> index 2 (log2_size = 11)
  // [4K, 8K)   -> index 3 (log2_size = 12)
  // [8K, 16K)  -> index 4 (log2_size = 13)
  // [16K, 32K) -> index 5 (log2_size = 14)
  // [32K, 64K) -> index 6 (log2_size = 15)
  // [64K, 128K)-> index 7 (log2_size = 16)
  // [128K,256K)-> index 8 (log2_size = 17)
  // [256K, +∞) -> index 9 (log2_size >= 18)
  
  if (log2_size < 10) {
    return 0;
  } else if (log2_size >= 18) {
    return MAX_DIR_RANGE - 1;  // [256K, +∞)
  } else {
    return log2_size - 9;
  }
}

int ObDirCalcSizeOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  const char *dir = get_dir();
  if (OB_ISNULL(entry) || OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret), KP(entry), KP(dir));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("dir calculate size is stop", KR(ret), K_(is_stop));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else {
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir, entry->d_name);
    if (p_ret <= 0 || p_ret >= sizeof(full_path)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", KR(ret), K(dir), K(entry->d_name));
    } else {
      ObIODFileStat statbuf;
      // ::rename operation does not modify statbuf.mtime_s_
      // therefore, logical delete of tmp_file does not affect the calculation here.
      if (OB_FAIL(ObIODeviceLocalFileOp::stat(full_path, statbuf))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to stat file", K(full_path), K(statbuf));
        }
      } else if (!S_ISREG(statbuf.mode_) && !S_ISDIR(statbuf.mode_)) {
        // do nothing, only print WARNING log, cannot report error code
        LOG_WARN("unexpcted file in current dir", K(dir), K(full_path), K(statbuf.mode_));
      } else if ((NULL != STRSTR(entry->d_name, DEFAULT_TMP_STR)) && S_ISREG(statbuf.mode_)) {
        if (OB_FAIL(process_tmp_seq_file(statbuf, full_path))) {
          LOG_WARN("fail to process tmp seq file", KR(ret), K(statbuf), K(full_path));
        }
      } else if (is_for_del_tmp_seq_files_) {
        // do nothing. just del tmp seq files, no need to do things about calibrate
      } else if (S_ISDIR(statbuf.mode_)) {
        // For directories, use create time (birth time) to determine if it existed before
        // start_calc_size_time. This avoids false negatives caused by mtime updates triggered
        // by file writes into the directory during the calibration scan period.
        int64_t dir_create_time_s = INT64_MAX;
        if (OB_FAIL(ObIODeviceLocalFileOp::get_dir_create_time(full_path, dir_create_time_s))) {
          LOG_WARN("fail to get dir create time", KR(ret), K(full_path));
        } else if (dir_create_time_s < start_calc_size_time_s_) {
          total_file_size_ += statbuf.size_;
          total_dir_num_++;
          if (statbuf.size_ > largest_dir_size_) {
            largest_dir_size_ = statbuf.size_;
            STRNCPY(largest_dir_path_, full_path, MAX_PATH_SIZE);
          }
          int64_t bucket_idx = get_dir_size_bucket_index(statbuf.size_);
          dir_nums_[bucket_idx]++;
          dir_sizes_[bucket_idx] += statbuf.size_;
        }
      } else if (S_ISREG(statbuf.mode_)) {
        if (statbuf.mtime_s_ < start_calc_size_time_s_) {
          // only statistics the regular file before start_calc_size_time
          if (0 == statbuf.size_) {
            LOG_INFO("find empty file", K(full_path));
          } else if (ObSSMacroCacheType::TMP_FILE == macro_cache_type_) { // 1. TMP_FILE
            if (OB_FAIL(process_tmp_file(statbuf, full_path))) {
              LOG_WARN("fail to process tmp file", KR(ret), K(statbuf), K(dir), K(entry->d_name));
            }
          } else if (ObSSMacroCacheType::MACRO_BLOCK == macro_cache_type_) { // 2. MACRO_BLOCK and HOT_TABLET_MACRO_BLOCK
            if (OB_FAIL(process_macro_block(statbuf, full_path))) {
              LOG_WARN("fail to process macro block", KR(ret), K(statbuf), K(full_path));
            }
          } else if (ObSSMacroCacheType::META_FILE == macro_cache_type_) { // 3. META_FILE
            // META_FILE has not been added into macro cache since it is not evicted from macro cache. therefore, only
            // accumulate total file size here. need to calibrate macro cache, when add META_FILE
            // into macro cache later.
            if (OB_FAIL(process_meta_file(statbuf, full_path))) {
              LOG_WARN("fail to process meta file", KR(ret), K(statbuf), K(full_path));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected macro cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(macro_cache_type_));
          }
        }
      }
    }
  }
  return ret;
}

int ObDirCalcSizeOp::set_start_calc_size_time(const int64_t start_calc_size_time_s)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_calc_size_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(start_calc_size_time_s));
  } else {
    start_calc_size_time_s_ = start_calc_size_time_s;
  }
  return ret;
}

int ObDirCalcSizeOp::set_tmp_seq_file_del_secy_time_s(const int64_t del_secy_time_s)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(del_secy_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(del_secy_time_s));
  } else {
    tmp_seq_file_del_secy_time_s_ = del_secy_time_s;
  }
  return ret;
}

int ObDirCalcSizeOp::process_tmp_seq_file(const ObIODFileStat &statbuf, const char *full_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(full_path) || OB_UNLIKELY(0 == STRLEN(full_path))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(full_path));
  } else if (ObTimeUtility::current_time_s() > (statbuf.mtime_s_ + tmp_seq_file_del_secy_time_s_)) {
    // .tmp.seq file has reached security deleting time(default 24h), need gc
    if (OB_FAIL(ObIODeviceLocalFileOp::unlink(full_path))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to del tmp seq file", KR(ret), K(full_path));
      }
    } else {
      del_tmp_seq_file_size_ += statbuf.size_;
      tmp_seq_file_num_ += 1;
      FLOG_INFO("succ to del tmp seq file", KR(ret), K(full_path));
    }
  } else if (statbuf.mtime_s_ < start_calc_size_time_s_) {
    int64_t reserved_size = 0;
    ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
    bool found = false;
    if (OB_FAIL(get_tmp_seq_file_reserved_size_(full_path, reserved_size, cache_type, found))) {
      LOG_WARN("fail to get tmp seq file reservation", KR(ret), K(full_path));
    } else {
      const int64_t file_size = ((found) ? reserved_size : statbuf.size_);
      if (found && ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_type) {
        hot_tablet_total_file_size_ += file_size;
      } else {
        total_file_size_ += file_size;
      }
      tmp_seq_file_size_ += statbuf.size_;
      tmp_seq_file_num_ += 1;
      if (!found) {
        LOG_TRACE("tmp seq reservation not found, use file size", K(full_path), K(statbuf));
      }
    }
  }
  return ret;
}

// Expected full_path: <cache_dir>/<object_name>.T<type>.tmp.<write_seq>
// Count only objects with disk accounting. Overwrite objects can also have atomic-write
// entries, but have no reservation to restore. A failed write is inserted into the clean
// map before being removed from the normal map; check both and count a matching seq once.
// A matching write with a cache type outside the scan range returns an error.
int ObDirCalcSizeOp::get_tmp_seq_file_reserved_size_(const char *full_path,
    int64_t &reserved_size, ObSSMacroCacheType &cache_type, bool &found) const
{
  int ret = OB_SUCCESS;
  reserved_size = 0;
  cache_type = ObSSMacroCacheType::MAX_TYPE;
  found = false;

  // file_name: <object_name>.T<type>.tmp.<write_seq>
  // tmp_suffix: .tmp.<write_seq>
  const char *file_name = nullptr;
  const char *tmp_suffix = nullptr;
  if (OB_ISNULL(full_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmp seq file path", KR(ret), KP(full_path));
  } else {
    file_name = STRRCHR(full_path, '/');
    file_name = (OB_ISNULL(file_name) ? full_path : file_name + 1);
    tmp_suffix = STRSTR(file_name, DEFAULT_TMP_STR);
  }

  // Path without ".tmp.<write_seq>", retaining ".T<type>" for macro-id parsing.
  char path[common::MAX_PATH_SIZE] = { 0 };
  uint64_t seq = 0;
  ObTenantFileManager *file_manager = nullptr;
  MacroBlockId macro_id;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tmp_suffix)
      || OB_UNLIKELY(1 != sscanf(tmp_suffix, ".tmp.%lu", &seq))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmp seq file path", KR(ret), K(full_path));
  } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret), K(full_path));
  } else if (OB_FAIL(databuff_printf(path, sizeof(path), "%.*s",
      static_cast<int>(tmp_suffix - full_path), full_path))) {
    LOG_WARN("fail to format tmp seq file path", KR(ret), K(full_path));
  } else if (OB_FAIL(ObFileHelper::convert_path_to_macro_id(path, macro_id))) {
    LOG_WARN("fail to convert tmp seq file path", KR(ret), K(full_path), K(path));
  } else if (SSObjUtil::is_need_stat(macro_id)) {
    for (int64_t map_idx = 0; OB_SUCC(ret) && !found && map_idx < 2; ++map_idx) {
      ObAtomicWriteFileInfo file_info;
      const bool is_clean = (1 == map_idx);
      if (OB_FAIL(file_manager->get_from_atomic_write_file_map(is_clean, macro_id, file_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get atomic write file", KR(ret), K(macro_id), K(seq), K(is_clean));
        }
      } else if (file_info.seq_ == seq) {
        // Scan macro_cache_type_ -> accepted file_info.cache_type_:
        // TMP_FILE -> TMP_FILE
        // META_FILE -> META_FILE
        // MACRO_BLOCK -> MACRO_BLOCK (AUTO) or HOT_TABLET_MACRO_BLOCK
        const bool matches_scan_type = (file_info.cache_type_ == macro_cache_type_
            || (ObSSMacroCacheType::MACRO_BLOCK == macro_cache_type_
                && ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == file_info.cache_type_));
        if (OB_UNLIKELY(!matches_scan_type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to validate atomic write cache type for calibration scan",
              KR(ret), K(full_path), K(seq), K(macro_cache_type_), K(file_info), K(is_clean));
        } else if (OB_UNLIKELY(file_info.size_ > INT64_MAX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid atomic write file size", KR(ret), K(full_path), K(file_info));
        } else {
          reserved_size = file_info.size_;
          cache_type = file_info.cache_type_;
          found = true;
        }
      }
    }
  }
  return ret;
}

int ObDirCalcSizeOp::process_tmp_file(const ObIODFileStat &statbuf, const char *path)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  bool is_macro_cache_meta_exist = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is null", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (NULL != STRSTR(path, DEFAULT_DELETED_STR)) { // .deleted file
    // do nothing. it will be deleted after calibration
  } else if (OB_FAIL(get_macro_id_and_cache_info(path, macro_id, cache_type, is_macro_cache_meta_exist))) {
    LOG_WARN("fail to get macro id and cache info", KR(ret), K(path));
  } else if (is_macro_cache_meta_exist) {
    if (OB_FAIL(macro_cache_mgr->update_is_marked(macro_id, true/*is_marked*/))) {
      LOG_WARN("fail to update is marked", KR(ret), K(macro_id));
    }
  } else { // !is_macro_cache_meta_exist
    ObSSMacroCacheInfo cache_info(ObTabletID::INVALID_TABLET_ID/*effective_tablet_id*/,
                                  statbuf.size_, cache_type, false/*is_write_cache*/);
    if (OB_FAIL(macro_cache_mgr->put(macro_id, cache_info))) {
      LOG_WARN("fail to put macro cache", KR(ret), K(macro_id), K(cache_info));
    } else {
      not_in_meta_map_file_cnt_++;
    }
  }
  if (OB_SUCC(ret)) {
    total_file_size_ += statbuf.size_;
  }
  if (OB_UNEXPECTED_MACRO_CACHE_FILE == ret) {
    ret = OB_SUCCESS; // ignore this error, just means some file(s) are not expected
  }
  return ret;
}

int ObDirCalcSizeOp::process_meta_file(const ObIODFileStat &statbuf, const char *path)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  bool is_macro_cache_meta_exist = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is null", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(get_macro_id_and_cache_info(path, macro_id, cache_type, is_macro_cache_meta_exist))) {
    LOG_WARN("fail to get macro id and cache info", KR(ret), K(path));
  } else {
    total_file_size_ += statbuf.size_;
  }
  if (OB_UNEXPECTED_MACRO_CACHE_FILE == ret) {
    ret = OB_SUCCESS; // ignore this error, just means some file(s) are not expected
  }
  return ret;
}

int ObDirCalcSizeOp::process_macro_block(const ObIODFileStat &statbuf, const char *path)
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
  bool is_macro_cache_meta_exist = false;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is null", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(get_macro_id_and_cache_info(path, macro_id, cache_type, is_macro_cache_meta_exist))) {
    LOG_WARN("fail to get macro id and cache info", KR(ret), K(path));
  } else if (is_macro_cache_meta_exist) {
    if (OB_FAIL(macro_cache_mgr->update_is_marked(macro_id, true/*is_marked*/))) {
      LOG_WARN("fail to update is marked", KR(ret), K(macro_id));
    }
  } else { // !is_macro_cache_meta_exist
    // EXTERNAL_TABLE_FILE has no effective_tablet_id
    uint64_t tmp_tablet_id = ObTabletID::INVALID_TABLET_ID;
    if (SSObjUtil::has_effective_tablet_id(macro_id)) {
      if (OB_FAIL(SSObjUtil::get_effective_tablet_id(macro_id, tmp_tablet_id))) {
        LOG_WARN("fail to get effective tablet id", KR(ret), K(macro_id));
      }
    }
    if (OB_SUCC(ret)) {
      const uint64_t effective_tablet_id = tmp_tablet_id;
      const bool is_write_cache = STI(macro_id.storage_object_type()).has_write_back_strategy();
      ObSSMacroCacheInfo cache_info(effective_tablet_id, statbuf.size_, cache_type, is_write_cache);
      if (OB_FAIL(macro_cache_mgr->put(macro_id, cache_info))) {
        LOG_WARN("fail to put macro cache", KR(ret), K(macro_id), K(cache_info));
      } else {
        not_in_meta_map_file_cnt_++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_type) {
      hot_tablet_total_file_size_ += statbuf.size_;
    } else {
      total_file_size_ += statbuf.size_;
    }
  }
  if (OB_UNEXPECTED_MACRO_CACHE_FILE == ret) {
    ret = OB_SUCCESS; // ignore this error, just means some file(s) are not expected
  }
  return ret;
}

int ObDirCalcSizeOp::get_macro_id_and_cache_info(
    const char *path,
    MacroBlockId &macro_id,
    ObSSMacroCacheType &cache_type,
    bool &is_macro_cache_meta_exist)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMetaHandle macro_cache_meta_handle;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path is null", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret));
  } else if (OB_FAIL(ObFileHelper::convert_path_to_macro_id(path, macro_id))) {
    LOG_WARN("fail to convert path to macro id", KR(ret), K(path));
  } else if (OB_FAIL(macro_cache_mgr->get_macro_cache_type(macro_id, cache_type))) {
    LOG_WARN("fail to get macro cache type", KR(ret), K(macro_id));
  } else if (OB_FAIL(macro_cache_mgr->get_macro_cache_meta(macro_id, is_macro_cache_meta_exist,
                                                           macro_cache_meta_handle))) {
    LOG_WARN("fail to get macro cache meta", KR(ret), K(macro_id));
  }
  return ret;
}


/**
 * --------------------------------ObMajorDataDeleteOp------------------------------------
 */

ObMajorDataDeleteOp::ObMajorDataDeleteOp(const int64_t time_stamp)
  : time_stamp_(time_stamp),
    total_file_count_(0)
{
}

int ObMajorDataDeleteOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else {
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    ObIODFileStat statbuf;
    const char *dir = get_dir();
    if (OB_ISNULL(dir)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dir is null", KR(ret));
    } else if (OB_FAIL(databuff_printf(full_path, sizeof(full_path), "%s/%s", dir, entry->d_name))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(dir), K(entry->d_name));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(full_path, statbuf))) {
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to stat file", K(full_path), K(statbuf));
      }
    // if the file write time before observer restart time, need delete
    } else if (statbuf.mtime_s_ < time_stamp_) {
      ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
      if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant disk space manager is null", KR(ret), KP(disk_space_mgr));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(full_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del file", KR(ret), K(full_path));
        }
      // TODO(binifei.bnf): if this delete op is still need, then need to parse MacroBlockId and check if belong to hot tablet
      } else if (OB_FAIL(disk_space_mgr->free_file_size(statbuf.size_, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE))) {
        LOG_WARN("fail to free major macro size", KR(ret), K(statbuf.size_));
      } else {
        total_file_count_++;
      }
    }
  }
  return ret;
}

int ObMajorDataDeleteOp::set_time_stamp(const int64_t time_stamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(time_stamp <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(time_stamp));
  } else {
    time_stamp_ = time_stamp;
  }
  return ret;
}

/**
 * --------------------------------ObSharedTenantDirListOp------------------------------------
 */

ObSharedTenantDirListOp::ObSharedTenantDirListOp(const uint64_t tenant_id) : file_list_()
{
  // tenant id is used to set ObMemAttr
  file_list_.set_attr(ObMemAttr(tenant_id, "ListSharedTen"));
}

int ObSharedTenantDirListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (1 != sscanf(entry->d_name, "tenant_%lu", &tenant_id)) {
    // do nothing, maybe other files
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), K(tenant_id));
  } else if (OB_FAIL(file_list_.push_back(tenant_id))) {
    LOG_WARN("failed to push back", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSharedTenantDirListOp::get_file_list(common::ObIArray<uint64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObSharedClusterDirListOp------------------------------------
 */

ObSharedClusterDirListOp::ObSharedClusterDirListOp() : cluster_ids_()
{
  cluster_ids_.set_attr(ObMemAttr(OB_SYS_TENANT_ID, "ListClusterIDS"));
}

int ObSharedClusterDirListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  uint64_t cluster_id = 0;
  char format[1024] = {0};
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s_%%lu", CLUSTER_DIR_STR))) { // output "cluster_%lu"
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (1 != sscanf(entry->d_name, format, &cluster_id)) {
    // do nothing, maybe other files
  } else if (OB_FAIL(cluster_ids_.push_back(cluster_id))) {
    LOG_WARN("failed to push back", KR(ret), K(cluster_id));
  }
  return ret;
}

int ObSharedClusterDirListOp::get_cluster_ids(common::ObIArray<uint64_t> &cluster_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cluster_ids.assign(cluster_ids_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObServerDirListOp------------------------------------
 */
int ObServerDirListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char format[1024] = {0};
  uint64_t server_id = 0;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s_%%lu", SERVER_DIR_STR))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (1 != sscanf(entry->d_name, format, &server_id)) {
    // do nothing, maybe other files
  } else if (OB_FAIL(server_ids_.push_back(server_id))) {
    LOG_WARN("fail to push back", KR(ret), K(server_id));
  }
  return ret;
}

int ObServerDirListOp::get_server_ids(ObIArray<uint64_t> &server_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_ids.assign(server_ids_))) {
    LOG_WARN("fail to assign server ids", KR(ret), K_(server_ids));
  }
  return ret;
}


/**
 * --------------------------------ObSingleNumFileListOp------------------------------------
 */

ObSingleNumFileListOp::ObSingleNumFileListOp() : file_list_()
{
  file_list_.set_attr(ObMemAttr(MTL_ID(), "ListSinFile"));
}

int ObSingleNumFileListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t file_id = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (false == ObString(entry->d_name).is_numeric()) {
    // do nothing, only print log
    LOG_WARN("unexpected path", K(entry->d_name));
  } else if (OB_FAIL(ObFileHelper::parse_file_name(entry->d_name, file_id))) {
    LOG_WARN("fail to parse file name", KR(ret));
  } else if (OB_FAIL(file_list_.push_back(file_id))) {
    LOG_WARN("failed to push back", KR(ret), K(file_id));
  }
  return ret;
}

int ObSingleNumFileListOp::get_file_list(common::ObIArray<int64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObTabletMetaListOp------------------------------------
 */
ObTabletMetaListOp::ObTabletMetaListOp() : file_list_()
{
  file_list_.set_attr(ObMemAttr(MTL_ID(), "ListTabMeta"));
}

int ObTabletMetaListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int64_t ver_id = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (NULL != STRSTR(entry->d_name, DEFAULT_TMP_STR)) {
    LOG_INFO("skip tmp seq file", K(entry->d_name));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld.T%hhu", VER_KEY_STR, ObStorageObjectType::PRIVATE_TABLET_META))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (OB_UNLIKELY(1 != sscanf(entry->d_name, format, &ver_id))) {
    LOG_WARN("unexpected path", K(entry->d_name));
  } else if (OB_FAIL(file_list_.push_back(ver_id))) {
    LOG_WARN("failed to push back", KR(ret), K(ver_id));
  }
  return ret;
}

int ObTabletMetaListOp::get_file_list(common::ObIArray<int64_t> &files) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(files.assign(file_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObCkptFileListOp------------------------------------
 */
ObCkptFileListOp::ObCkptFileListOp() : object_ids_()
{
  object_ids_.set_attr(ObMemAttr(MTL_ID(), "ListCkptFile"));
}

int ObCkptFileListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int64_t object_id = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%%ld.T%hhu", ObStorageObjectType::PRIVATE_CKPT_FILE))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (OB_UNLIKELY(1 != sscanf(entry->d_name, format, &object_id))) {
    // skip unexpected file
    LOG_WARN("unexpected path", K(entry->d_name));
  } else if (OB_FAIL(object_ids_.push_back(object_id))) {
    LOG_WARN("failed to push back", KR(ret), K(object_id));
  }
  return ret;
}

int ObCkptFileListOp::get_object_id_list(common::ObIArray<int64_t> &object_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(object_ids.assign(object_ids_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObTmpFileSegListOp------------------------------------
 */
ObTmpFileSegListOp::ObTmpFileSegListOp() : seg_ids_()
{
  seg_ids_.set_attr(ObMemAttr(MTL_ID(), "ListSegId"));
}

int ObTmpFileSegListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  char format[512] = {0};
  int num = 0;
  int64_t seg_id = 0;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld.T%hhu", SEG_KEY_STR, ObStorageObjectType::TMP_FILE))) {
    LOG_WARN("fail to databuff printf", KR(ret));
  } else if (OB_UNLIKELY(1 != sscanf(entry->d_name, format, &seg_id))) {
    // do nothing, only print log
    LOG_WARN("unexpected path", K(entry->d_name));
  } else if (OB_FAIL(seg_ids_.push_back(seg_id))) {
    LOG_WARN("failed to push back", KR(ret), K(seg_id));
  }
  return ret;
}

/**
 * --------------------------------ObPrivateMacroListOp------------------------------------
 */

ObPrivateMacroListOp::ObPrivateMacroListOp(const ObMacroType macro_type)
  : macro_type_(macro_type), file_list_()
{
  file_list_.set_attr(ObMemAttr(MTL_ID(), "ListPriMacro"));
}

int ObPrivateMacroListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t server_id = -1;
  int64_t op_id = -1;
  int64_t seq_id = -1;
  bool is_match = false;
  char format[512] = {0};
  int num = 0;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (NULL != STRSTR(entry->d_name, DEFAULT_TMP_STR)) {
    LOG_INFO("skip tmp seq file", K(entry->d_name));
  } else if (ObMacroType::DATA_MACRO == macro_type_) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld%s%%ld%s%%ld.T%hhu",
                                SVR_KEY_STR, OP_KEY_STR, SEQ_KEY_STR,
                                ObStorageObjectType::PRIVATE_DATA_MACRO))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_UNLIKELY(3 != sscanf(entry->d_name, format, &server_id, &op_id, &seq_id))) {
      // do nothing, only print log
      LOG_WARN("unexpected path", K(entry->d_name));
    } else {
      is_match = true;
    }
  } else if (ObMacroType::META_MACRO == macro_type_) {
    if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld%s%%ld%s%%ld.T%hhu",
                                SVR_KEY_STR, OP_KEY_STR, SEQ_KEY_STR,
                                ObStorageObjectType::PRIVATE_META_MACRO))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (OB_UNLIKELY(3 != sscanf(entry->d_name, format, &server_id, &op_id, &seq_id))) {
      // do nothing, only print log
      LOG_WARN("unexpected path", K(entry->d_name));
    } else {
      is_match = true;
    }
  }

  if (OB_SUCC(ret) && is_match) {
    if (OB_UNLIKELY(server_id < 0 || static_cast<uint64_t>(server_id) != GCONF.observer_id
                    || op_id < 0 || seq_id < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", KR(ret), K(server_id), K(op_id), K(seq_id));
    } else {
      std::pair<int64_t, int64_t> file_id;
      file_id.first = op_id;
      file_id.second = seq_id;
      if (OB_FAIL(file_list_.push_back(file_id))) {
        LOG_WARN("failed to push back", KR(ret), K(server_id), K(op_id), K(seq_id));
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObRMLogicalDeletedFileOp------------------------------------
 */

int ObRMLogicalDeletedFileOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *dir = get_dir();
  if (OB_ISNULL(entry) || OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret), KP(entry), KP(dir));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("del deleted file is stop", KR(ret), K_(is_stop));
  } else if (NULL != STRSTR(entry->d_name, DEFAULT_DELETED_STR)) { // only delete .deleted file
    MacroBlockId file_id;
    if (OB_FAIL(ObFileHelper::tmpfile_path_to_macro_id(dir, entry->d_name, file_id))) {
      LOG_WARN("fail to convert tmpfile path to macro id", KR(ret), K(dir), K(entry->d_name));
    } else if (OB_FAIL(ObSSObjectAccessUtil::delete_local_file(file_id, 0/*ls_epoch_id*/,
                       true/*is_print_log*/, false/*is_del_seg_meta*/, true/*is_logical_delete*/))) {  // is_logical_delete is true
      LOG_WARN("fail to delete local file", KR(ret), K(file_id));
    } else {
      total_file_cnt_++;
    }
  }
  return ret;
}

/**
 * --------------------------------ObDelTmpFileDirOp------------------------------------
 */
int ObDelTmpFileDirOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *dir = get_dir();
  int64_t tmp_file_id = -1;
  bool can_del = false;
  if (OB_ISNULL(entry) || OB_ISNULL(dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret), KP(entry), KP(dir));
  } else if (OB_UNLIKELY(is_stop_)) {
    ret = OB_CANCELED;
    LOG_WARN("del dir is stop", KR(ret), K_(is_stop));
  } else if (false == ObString(entry->d_name).is_numeric()) {
    // do nothing, unexpected dir path, maybe is user created dir
  } else {
    // Construct full directory path: dir + entry->d_name
    char dir_path[common::MAX_PATH_SIZE] = {0};
    if (OB_FAIL(databuff_printf(dir_path, sizeof(dir_path), "%s/%s", dir, entry->d_name))) {
      LOG_WARN("fail to construct dir path", KR(ret), K(dir), K(entry->d_name));
    } else if (OB_FAIL(check_dir_if_can_del(dir_path, can_del))) {
      LOG_WARN("fail to check if dir can del", KR(ret), K(dir_path), K(entry->d_name));
    } else if (can_del) {
      if (OB_FAIL(OB_DIR_MGR.delete_dir(dir_path, ObStorageObjectType::TMP_FILE))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS; // ignore, directory already deleted
        } else {
          LOG_WARN("fail to delete dir", KR(ret), K(dir_path), K(entry->d_name));
        }
      } else {
        FLOG_INFO("succ to delete empty dir", K(dir_path), K(entry->d_name));
      }
    }
  }
  return ret;
}

// Unified function to check if a directory can be deleted
// Check logic: stat mtime + check if directory is empty
int ObDelTmpFileDirOp::check_dir_if_can_del(const char *dir_path, bool &can_del)
{
  int ret = OB_SUCCESS;
  can_del = false;
  bool is_empty = false;
  ObIODFileStat statbuf;
  
  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dir path", KR(ret), KP(dir_path));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(dir_path, statbuf))) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS; // ignore ret. may be gc concurrently
    } else {
      LOG_WARN("fail to stat", KR(ret), K(dir_path), K(statbuf));
    }
  } else if (OB_FAIL(is_dir_empty(dir_path, is_empty))) {
    LOG_WARN("fail to check if dir is empty", KR(ret), K(dir_path));
  } else if (is_empty && ((ObTimeUtility::current_time_s() - statbuf.mtime_s_) > TMP_FILE_DIR_DELETE_SAFE_TIME_S)) {
    can_del = true;
  }
  return ret;
}

/**
 * --------------------------------ObSSGetSlogIdRangeFunctor------------------------------------
 */
int ObSSGetSlogIdRangeFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    char format[512] = {0};
    int num = 0;
    const char *entry_name = entry->d_name;
    int64_t object_id = 0;
    if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld.T%hhu",
                SEQ_KEY_STR, (uint8_t)blocksstable::ObStorageObjectType::PRIVATE_SLOG_FILE))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(entry_name, format, &object_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      // do nothing, skip invalid file
    } else {
      if ((OB_INVALID_FILE_ID == min_file_id_) || (object_id < min_file_id_)) {
        min_file_id_ = object_id;
      }
      if ((OB_INVALID_FILE_ID == max_file_id_) || (object_id > max_file_id_)) {
        max_file_id_ = object_id;
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObSSGetSlogSizeFunctor------------------------------------
 */
int ObSSGetSlogSizeFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    char format[512] = {0};
    int num = 0;
    const char *entry_name = entry->d_name;
    int64_t object_id = 0;
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
    if (p_ret < 0 || p_ret >= sizeof(full_path)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", K(ret), K_(dir), K(entry->d_name));
    } else if (OB_FAIL(databuff_printf(format, sizeof(format), "%s%%ld.T%hhu",
               SEQ_KEY_STR, (uint8_t)blocksstable::ObStorageObjectType::PRIVATE_SLOG_FILE))) {
      LOG_WARN("fail to databuff printf", KR(ret));
    } else if (FALSE_IT(num = sscanf(entry_name, format, &object_id))) {
    } else if (OB_UNLIKELY(1 != num)) {
      // do nothing, skip invalid file
    } else {
      ObIODFileStat statbuf;
      if (OB_FAIL(LOCAL_DEVICE_INSTANCE.stat(full_path, statbuf))
          && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to stat file", K(full_path), K(statbuf));
      } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // file could be renamed (tmp file) or recycled concurrently, ignore
        ret = OB_SUCCESS;
      } else if (S_ISREG(statbuf.mode_)) {
        total_size_ += statbuf.size_;
      } else if (S_ISDIR(statbuf.mode_)) {
        ObGetFileSizeFunctor functor(full_path);
        if (OB_FAIL(LOCAL_DEVICE_INSTANCE.scan_dir(full_path, functor))) {
          LOG_WARN("fail to scan dir", K(ret), K(full_path), K(entry->d_name));
        } else {
          total_size_ += functor.get_total_size();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected file type", K(full_path), K(statbuf));
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObMacroIdListOp------------------------------------
 */
ObMacroIdListOp::ObMacroIdListOp(const char *dir) : dir_(dir), macro_id_list_()
{
  macro_id_list_.set_attr(ObMemAttr(MTL_ID(), "ListMacroId"));
}

int ObMacroIdListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    char format[512] = {0};
    int num = 0;
    const char *entry_name = entry->d_name;
    int64_t object_type = 0;
    blocksstable::MacroBlockId macro_id;
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    const char *sub_path = nullptr;
    if (OB_FAIL(databuff_printf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name))) {
      LOG_WARN("fail to databuff printf", KR(ret), K(dir_), K(entry->d_name));
    } else if (OB_ISNULL(sub_path = ObString(full_path).reverse_find('.', 1))) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(full_path));
    } else if (FALSE_IT(num = sscanf(sub_path, ".T%hhu", &object_type))) {
    } else if (OB_UNLIKELY(1 != num)) {
      ret = OB_UNEXPECTED_MACRO_CACHE_FILE;
      LOG_ERROR("unexpected file in macro cache path", KR(ret), K(sub_path), K(full_path));
    } else if (SSObjUtil::is_macro(static_cast<ObStorageObjectType>(object_type)) ||
              (SSObjUtil::is_tablet_meta(static_cast<ObStorageObjectType>(object_type)) && SSObjUtil::is_shared(static_cast<ObStorageObjectType>(object_type)))) {
      if (OB_FAIL(SSObjUtil::remote_path_to_macro_id(static_cast<ObStorageObjectType>(object_type), full_path, macro_id))) {
        LOG_WARN("failed to convert remote path to macro id", KR(ret), K(full_path), K(object_type));
      } else if (OB_FAIL(macro_id_list_.push_back(macro_id))) {
        LOG_WARN("failed to push back", KR(ret), K(macro_id));
      }
    }
  }
  return ret;
}

int ObMacroIdListOp::get_macro_id_list(common::ObIArray<blocksstable::MacroBlockId> &macro_id_list) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_id_list.assign(macro_id_list_))) {
    LOG_WARN("failed to assign", KR(ret));
  }
  return ret;
}

/**
 * --------------------------------ObDeleteOldVersionTmpFileOp------------------------------------
 */
int ObDeleteOldVersionTmpFileOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid entry", KR(ret));
  } else if (OB_ISNULL(scatter_dir_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scatter_dir is null", KR(ret), KP_(scatter_dir));
  } else if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else {
    char entry_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
    ObIODFileStat st;
    if (OB_FAIL(databuff_printf(entry_path, sizeof(entry_path), "%s/%s", scatter_dir_, entry->d_name))) {
      LOG_WARN("fail to databuff printf", KR(ret), K_(scatter_dir), K(entry->d_name));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::stat(entry_path, st))) {
      LOG_WARN("fail to stat entry", KR(ret), K(entry_path));
    } else if (S_ISREG(st.mode_)) {
      // This is a regular file under scatter directory, must be old version tmp file (e.g., tmp_data/100/seg1.T8)
      // Since the 'delete_local_file' interface internally uses the 'to_path()' function that now points to the new path, 
      // it fails to remove the temporary files from the old path, hence this interface should not be used here.
      if (OB_FAIL(ObIODeviceLocalFileOp::unlink(entry_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS; // ignore, file already deleted
        } else {
          LOG_WARN("fail to unlink old version tmp file", KR(ret), K(entry_path));
        }
      } else {
        delete_success_count_++;
        if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
          FLOG_INFO("successfully deleted old version tmp file in scatter directory", K(entry_path), K_(delete_success_count));
        }
      }
      
      // free size of deleted file, no matter OB_SUCC or OB_FAIL
      int tmp_ret = OB_SUCCESS;
      if (OB_LIKELY(st.size_ > 0)) {
        if (OB_TMP_FAIL(disk_space_mgr->free_file_size(st.size_, ObSSMacroCacheType::TMP_FILE, ObDiskSpaceType::FILE))) {
          LOG_WARN("fail to free file size", KR(tmp_ret), K(st.size_), K(entry_path));
        }
      }
    } else if (S_ISDIR(st.mode_)) {
      // Directories under scatter_dir are new version tmp_file directories, skip them
      LOG_DEBUG("skip new version tmp_file directory", K(entry_path), K(entry->d_name));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
