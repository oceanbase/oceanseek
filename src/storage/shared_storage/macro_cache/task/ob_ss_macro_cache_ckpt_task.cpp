/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_ckpt_task.h"
#include "lib/thread/thread_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "common/ob_file_common_header.h"
#include "share/io/ob_io_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "share/backup/ob_backup_io_adapter.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

static constexpr char MACRO_CACHE_CKPT_META_TAG[] = "MC_CKPT_META";
static constexpr char MACRO_CACHE_CKPT_DATA_TAG[] = "MC_CKPT_DATA";

/******************************** ObSSMacroCacheCkptMeta **********************************/
OB_SERIALIZE_MEMBER(ObSSMacroCacheCkptMeta, ckpt_time_us_, data_seq_arr_);

ObSSMacroCacheCkptMeta::ObSSMacroCacheCkptMeta(const uint64_t tenant_id)
    : ckpt_time_us_(0), data_seq_arr_()
{
  data_seq_arr_.set_attr(ObMemAttr(tenant_id, MACRO_CACHE_CKPT_META_TAG));
}

ObSSMacroCacheCkptMeta::~ObSSMacroCacheCkptMeta()
{
  reset();
}

void ObSSMacroCacheCkptMeta::reset()
{
  ckpt_time_us_ = 0;
  data_seq_arr_.reset();
}

int ObSSMacroCacheCkptMeta::record_ckpt_data_seq(const uint64_t seq_id)
{
  int ret = OB_SUCCESS;
  const int64_t seq_num = data_seq_arr_.count();
  if (OB_UNLIKELY(seq_id == UINT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(seq_id));
  } else if (OB_UNLIKELY(seq_num > 0 && data_seq_arr_.at(seq_num - 1) >= seq_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ckpt data block seq id should follow incremental ordering",
        KR(ret), K(seq_id), K(seq_num), K(data_seq_arr_.at(seq_num - 1)));
  } else if (OB_FAIL(data_seq_arr_.push_back(seq_id))) {
    LOG_WARN("fail to push seq id", KR(ret), K(seq_id), K(seq_num));
  }
  return ret;
}

const ObSSMacroCacheCkptMeta::CkptDataBlockSeqArr &ObSSMacroCacheCkptMeta::get_ckpt_data_seq_arr() const
{
  return data_seq_arr_;
}

void ObSSMacroCacheCkptMeta::record_ckpt_time()
{
  ckpt_time_us_ = ObTimeUtility::fast_current_time();
}

int64_t ObSSMacroCacheCkptMeta::get_ckpt_time() const
{
  return ckpt_time_us_;
}

/******************************** ObMacroCacheGetCkptVersionOp **********************************/
ObMCParseCkptVersionsOp::ObMCParseCkptVersionsOp(const uint64_t tenant_id)
    : max_ckpt_version_(UINT64_MAX), meta_file_size_(-1), version_id_arr_()
{
  version_id_arr_.set_attr(ObMemAttr(tenant_id, MACRO_CACHE_CKPT_META_TAG));
}

int ObMCParseCkptVersionsOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  const char *meta_file_name = nullptr;
  uint64_t ckpt_version = UINT64_MAX;
  char format[64] = { 0 };
  int64_t scanf_num = -1;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("entry is NULL", KR(ret), KP(entry));
  } else if (OB_ISNULL(meta_file_name = entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("entry->d_name is NULL", KR(ret), KP(entry));
  } else if (OB_FAIL(get_path_format(format, sizeof(format)))) {
    LOG_WARN("fail to construct ckpt meta version id format", KR(ret), K(meta_file_name));
  } else if (FALSE_IT(scanf_num = sscanf(meta_file_name, format, &ckpt_version))) {
  } else if (OB_UNLIKELY(1 != scanf_num)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("fail to parse ckpt version", KR(ret),
        K(scanf_num), KP(entry), K(meta_file_name), K(format));
  } else if (OB_UNLIKELY(ckpt_version == UINT64_MAX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ckpt version", KR(ret), KP(entry), K(meta_file_name), K(ckpt_version));
  } else if (OB_FAIL(version_id_arr_.push_back(ckpt_version))) {
    LOG_WARN("fail to record ckpt version", KR(ret),
        K(meta_file_name), K(ckpt_version), K(version_id_arr_.count()));
  } else {
    if (max_ckpt_version_ == UINT64_MAX || max_ckpt_version_ < ckpt_version) {
      max_ckpt_version_ = ckpt_version;
      meta_file_size_ = get_size();
    }
  }
  return ret;
}

bool ObMCParseCkptVersionsOp::is_max_ckpt_version_valid() const
{
  return max_ckpt_version_ != UINT64_MAX && meta_file_size_ > 0;
}

bool ObMCParseCkptVersionsOp::is_empty() const
{
  return version_id_arr_.empty();
}

ObMCGetCkptMetaVersionsOp::ObMCGetCkptMetaVersionsOp(const uint64_t tenant_id)
    : ObMCParseCkptVersionsOp(tenant_id)
{
}

int ObMCGetCkptMetaVersionsOp::get_path_format(char *format, const int64_t format_buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(format) || OB_UNLIKELY(format_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(format), K(format_buf_size));
  } else if (OB_FAIL(databuff_printf(format, format_buf_size, "%s%%lu.T%hhu",
      VER_KEY_STR, static_cast<uint8_t>(ObStorageObjectType::MACRO_CACHE_CKPT_META)))) {
    LOG_WARN("fail to construct ckpt meta version id format", KR(ret), K(format_buf_size));
  }
  return ret;
}

ObMCGetCkptDataVersionsOp::ObMCGetCkptDataVersionsOp(const uint64_t tenant_id)
    : ObMCParseCkptVersionsOp(tenant_id)
{
}

int ObMCGetCkptDataVersionsOp::get_path_format(char *format, const int64_t format_buf_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(format) || OB_UNLIKELY(format_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(format), K(format_buf_size));
  } else if (OB_FAIL(databuff_printf(format, format_buf_size, "%%lu"))) {
    LOG_WARN("fail to construct ckpt meta version id format", KR(ret), K(format_buf_size));
  }
  return ret;
}

/******************************** ObSSMacroCacheCkptTask **********************************/

ObSSMacroCacheCkptTask::ObSSMacroCacheCkptTask(volatile bool &is_stopped)
    : ObSSMacroCacheTaskBase(is_stopped),
      parallelism_(0),
      ckpt_data_blk_size_(0),
      macro_cache_mgr_(nullptr),
      next_version_(UINT64_MAX),
      ckpt_item_cnt_(0)
{
}

ObSSMacroCacheCkptTask::~ObSSMacroCacheCkptTask()
{
  destroy();
}

int64_t ObSSMacroCacheCkptTask::get_data_blk_size() const
{
  return ckpt_data_blk_size_;
}

int ObSSMacroCacheCkptTask::init(ObSSMacroCacheMgr *macro_cache_mgr, const int64_t parallelism)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init macro cache ckpt task twice", KR(ret));
  } else if (OB_ISNULL(macro_cache_mgr)
      || OB_UNLIKELY(parallelism < MIN_PARALLELISM || parallelism > MAX_PARALLELISM)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(macro_cache_mgr), K(parallelism));
  } else {
    parallelism_ = parallelism;
    ckpt_data_blk_size_ = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    macro_cache_mgr_ = macro_cache_mgr;
    is_inited_ = true;
  }
  LOG_INFO("ObSSMacroCacheCkptTask init", KR(ret),
      KPC(macro_cache_mgr), K(parallelism), K(ckpt_data_blk_size_));
  return ret;
}

int ObSSMacroCacheCkptTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro cache ckpt task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tg_id));
  } else if (OB_FAIL(schedule_task_(*this, tg_id, CKPT_SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule macro cache ckpt task", KR(ret), K(tg_id));
  } else {
    LOG_INFO("ObSSMacroCacheCkptTask: succ to start", K(tg_id));
  }
  return ret;
}

void ObSSMacroCacheCkptTask::destroy()
{
}

void ObSSMacroCacheCkptTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSCPTraceIdGuard trace_id_guard;
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  CONSUMER_GROUP_FUNC_GUARD(ObFunctionType::PRIO_SS_LOCAL_CACHE_LOW);
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else if (!macro_cache_mgr_->is_ckpt_replayed()) {
    LOG_INFO("macro cache ckpt has not been replayed, cannot do macro cache ckpt task now");
  } else if (FALSE_IT(++run_cnt_)) {
  } else if (OB_UNLIKELY(UINT64_MAX == next_version_)) {
    FLOG_INFO("next ckpt version is invalid, try restore next ckpt version");
    if (OB_FAIL(try_restore_next_ckpt_version_())) {
      LOG_WARN("fail to try restore next ckpt version", KR(ret));
    }
  } else {
    if (OB_FAIL(write_ckpt_())) {
      LOG_WARN("failed to write macro cache ckpt",
          KR(ret), K(parallelism_), K(next_version_));
    } else if (next_version_ > 1) {
      if (OB_FAIL(clean_up_old_ckpts_(next_version_ - 2))) {
        LOG_WARN("failed to clean_up_old_ckpts",
            KR(ret), K(parallelism_), K(next_version_));
      }
    }

    ret = OB_SUCCESS;
  }
  const int64_t ckpt_used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObSSMacroCacheCkptTask: finish doing macro cache ckpt tasks", KR(ret), K(parallelism_),
      KPC(macro_cache_mgr_), K(next_version_), K(ckpt_used_time_us), K(start_time_us), K_(run_cnt));
}

int ObSSMacroCacheCkptTask::write_ckpt_()
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheCkptMeta ckpt_meta(MTL_ID());
  const int64_t start_time_us = ObTimeUtility::fast_current_time();
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(macro_cache_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr should not be null", KR(ret));
  } else {
    ckpt_item_cnt_ = 0; // reset ckpt item cnt
    ObSSMacroCache *macro_cache = nullptr;
    ObSSMacroCacheType cache_type = ObSSMacroCacheType::MAX_TYPE;
    for (uint8_t i = 0; OB_SUCC(ret) && i < SS_MACRO_CACHE_MAX_TYPE_VAL; i++) {
      macro_cache = nullptr;
      cache_type = static_cast<ObSSMacroCacheType>(i);
      
      if (OB_FAIL(macro_cache_mgr_->get_macro_cache(cache_type, macro_cache))) {
        LOG_WARN("fail to get macro cache", KR(ret), K(i), K(cache_type));
      } else if (OB_ISNULL(macro_cache)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro cache is NULL", KR(ret), K(i), K(cache_type));
      } else if (OB_FAIL(write_ckpt_data_(cache_type, *macro_cache, ckpt_meta))) {
        LOG_WARN("fail to write macro cache ckpt", KR(ret), K(i), K(cache_type), K(ckpt_meta));
      }
    }
    ckpt_meta.record_ckpt_time();

    if (FAILEDx(write_ckpt_meta_(ckpt_meta))) {
      LOG_WARN("fail to write ckpt meta", KR(ret), K(next_version_), K(ckpt_meta));
    }
  }

  const int64_t ckpt_used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
  LOG_INFO("ObSSMacroCacheCkptTask: finish to write macro cache ckpt", KR(ret), K_(parallelism),
      KPC_(macro_cache_mgr), K_(next_version), K(ckpt_used_time_us), K(start_time_us),
      K_(ckpt_item_cnt), K(ckpt_meta));
  return ret;
}

int ObSSMacroCacheCkptTask::write_ckpt_data_(
    const ObSSMacroCacheType &cache_type,
    ObSSMacroCache &macro_cache,
    ObSSMacroCacheCkptMeta &ckpt_meta)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta marker;
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_FAIL(macro_cache.clean_marker())) {
    LOG_WARN("fail to clean marker in macro cache", KR(ret), K(ckpt_meta), K(cache_type));
  } else if (OB_FAIL(ObSSMacroCacheMeta::construct_ckpt_marker(marker))) {
    LOG_WARN("fail to construct_ckpt_marker", KR(ret), K(cache_type), K(ckpt_meta));
  } else {
    marker.set_cache_type(cache_type);
    ObStorageObjectHandle write_handles[MAX_PARALLELISM];
    char *write_bufs[MAX_PARALLELISM];
    const int64_t buf_size = ckpt_data_blk_size_;
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), MACRO_CACHE_CKPT_DATA_TAG));
    for (int64_t i = 0; OB_SUCC(ret) && i < parallelism_; i++) {
      if (OB_ISNULL(write_bufs[i] = static_cast<char *>(allocator.alloc(buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc write buf", KR(ret), K(buf_size), K(ckpt_meta));
      }
    }

    uint64_t seq_id = UINT64_MAX;
    bool is_finished = false;
    int64_t item_cnt = 0;
    int64_t data_block_num = 0;
    while (OB_SUCC(ret) && !is_finished) {
      data_block_num = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < parallelism_; i++) {
        write_handles[i].reset();
      }

      for (int64_t i = 0; OB_SUCC(ret) && !is_finished && i < parallelism_; i++) {
        seq_id = UINT64_MAX;
        item_cnt = 0;
        if (OB_FAIL(macro_cache.incremental_serialize(
            write_bufs[i], buf_size, marker, is_finished, item_cnt))) {
          LOG_WARN("fail to serialize macro cahce",
              KR(ret), K(i), KP(write_bufs[i]), K(buf_size), K(marker));
        } else if (0 == item_cnt) {
          // empty result, no need upload
          if (OB_UNLIKELY(!is_finished)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("empty result but not finished",
                KR(ret), K(i), K(marker), K(item_cnt), K(is_finished), K(ckpt_meta),
                KP(macro_cache_mgr_), KP(&macro_cache));
          }
        } else {
          ckpt_item_cnt_ += item_cnt;
          if (OB_FAIL(TENANT_SEQ_GENERATOR.get_write_seq(seq_id))) {
            LOG_WARN("fail to generate seq id",
                KR(ret), K(i), KP(write_bufs[i]), K(buf_size), K(marker));
          } else if (OB_FAIL(ckpt_meta.record_ckpt_data_seq(seq_id))) {
            LOG_WARN("fail to record seq id", KR(ret),
                K(i), K(seq_id), K(marker), K(ckpt_meta));
          } else if (OB_FAIL(async_write_ckpt_data_block_(
              write_bufs[i], buf_size, seq_id, write_handles[i]))) {
            LOG_WARN("fail to async write ckpt data", KR(ret), K(i),
                KP(write_bufs[i]), K(buf_size), K(seq_id), K(marker));
          } else {
            data_block_num++;
          }
        }
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < data_block_num; i++) {
        if (OB_FAIL(write_handles[i].wait())) {
          LOG_WARN("fail to wait ckpt data block write",
              KR(ret), K(i), K(data_block_num), K(is_finished), K(ckpt_meta));
        }
      }
    } // end while
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(macro_cache.clean_marker())) {
    ret = COVER_SUCC(tmp_ret);
    LOG_WARN("fail to clean marker in macro cache", KR(ret), KR(tmp_ret), K(ckpt_meta), K(marker));
  }
  return ret;
}

int ObSSMacroCacheCkptTask::async_write_ckpt_data_block_(
    const char *buf, const int64_t buf_size,
    const uint64_t seq_id,
    ObStorageObjectHandle &write_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || seq_id == UINT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_size), K(seq_id));
  } else {
    ObStorageObjectOpt opt;
    opt.set_ss_macro_cache_ckpt_opt(false/*is_meta*/, next_version_, seq_id);
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = buf_size;
    write_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(opt, write_info, write_handle))) {
      LOG_WARN("fail to write macro cache ckpt data", KR(ret), K(opt), K(write_info));
    }
  }
  return ret;
}

int ObSSMacroCacheCkptTask::write_ckpt_meta_(const ObSSMacroCacheCkptMeta &ckpt_meta)
{
  int ret = OB_SUCCESS;
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = ckpt_meta.get_serialize_size();
  const int64_t buf_size = header_length + payload_length;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), MACRO_CACHE_CKPT_META_TAG));
  char *buf = nullptr;
  int64_t pos = 0;
  
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(buf_size), K(ckpt_meta));
  } else if (OB_FAIL(ObFileCommonHeaderUtil::serialize<ObSSMacroCacheCkptMeta>(
      ckpt_meta,
      ObSSMacroCacheCkptMeta::OB_SS_MACRO_CACHE_CKPT_META_VERSION,
      buf, buf_size, pos))) {
    LOG_WARN("fail to serialize macro cache ckpt meta", KR(ret), K(ckpt_meta), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(pos != buf_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pos", KR(ret), K(pos), K(buf_size), K(ckpt_meta));
  } else {
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt opt;
    opt.set_ss_macro_cache_ckpt_opt(
        true/*is_meta*/, next_version_, UINT64_MAX - 1 /*seq_id*/);
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = buf_size;
    write_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.write_object(opt, write_info, object_handle))) {
      LOG_WARN("fail to write macro cache ckpt meta",
          KR(ret), K(opt), K(write_info), K(ckpt_meta));
    }
    LOG_INFO("ObSSMacroCacheCkptTask: writing macro cache ckpt meta",
        KR(ret), K(next_version_), KPC(macro_cache_mgr_), K(write_info), K(opt));
    // always increment next_version_ regardless of ckpt meta write success/failure
    // to avoid overwrite issues caused by request timeout
    next_version_++;
  }
  return ret;
}

int ObSSMacroCacheCkptTask::clean_up_old_ckpts_(const uint64_t min_verison_to_keep)
{
  int ret = OB_SUCCESS;
  ObMCGetCkptDataVersionsOp op(MTL_ID());
  char ckpt_data_dir[MAX_PATH_SIZE] = { 0 };
  char ckpt_data_version_dir[MAX_PATH_SIZE] = { 0 };
  ObTenantFileManager *tfm = nullptr;
  if (OB_FAIL(check_init_and_stop_())) {
    // skip log
  } else if (OB_UNLIKELY(min_verison_to_keep == UINT64_MAX
      || min_verison_to_keep >= next_version_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("min_verison_to_keep is invalid", KR(ret), K(min_verison_to_keep), K(next_version_));
  } else if (OB_ISNULL(tfm = MTL(ObTenantFileManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantFileManager is NULL", KR(ret), K(min_verison_to_keep), K(next_version_));
  } else if (OB_FAIL(OB_DIR_MGR.get_macro_cache_ckpt_data_dir(
      ckpt_data_dir, sizeof(ckpt_data_dir), MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get ckpt meta file dir", KR(ret), K(min_verison_to_keep), K(next_version_));
  } else {
    ObBackupDest storage_dest;
    if (OB_FAIL(tfm->get_storage_dest(storage_dest))) {
      LOG_WARN("fail to get storage info", KR(ret),
          K(storage_dest), K(ckpt_data_dir), K(min_verison_to_keep), K(next_version_));
    } else if (OB_FAIL(ObBackupIoAdapter::list_directories(
        ckpt_data_dir, storage_dest.get_storage_info(), op))) {
      LOG_WARN("fail to get storage info", KR(ret),
          K(storage_dest), K(ckpt_data_dir), K(min_verison_to_keep), K(next_version_));
    }

    uint64_t version_id = UINT64_MAX;
    ObStorageObjectOpt opt;
    MacroBlockId macro_id;
    int64_t start_time_us = 0;
    int64_t clean_used_time_us = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < op.version_id_arr_.count(); i++) {
      start_time_us = ObTimeUtility::fast_current_time();
      version_id = op.version_id_arr_.at(i);
      macro_id.reset();
      opt.set_ss_macro_cache_ckpt_opt(
          true/*is_meta*/, version_id, UINT64_MAX - 1 /*seq_id*/);
      
      // first delete ckpt meta file, then cleanup ckpt data files
      if (version_id >= min_verison_to_keep) {
        LOG_INFO("ObSSMacroCacheCkptTask: skip clean up cur version ckpt", KR(ret), K(opt), K(i),
            K(ckpt_data_dir), K(storage_dest), K(min_verison_to_keep), K(next_version_));
      } else if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, macro_id))) {
        LOG_WARN("fail to get object id", KR(ret), K(opt), K(i),
            K(ckpt_data_dir), K(storage_dest), K(min_verison_to_keep), K(next_version_));
      } else if (OB_FAIL(tfm->delete_remote_file(macro_id))) {
        LOG_WARN("fail to clean ckpt meta", KR(ret), K(macro_id), K(i), K(version_id),
            K(ckpt_data_dir), K(storage_dest), K(min_verison_to_keep), K(next_version_));
      } else if (OB_FAIL(databuff_printf(
          ckpt_data_version_dir, sizeof(ckpt_data_version_dir),
          "%s/%lu", ckpt_data_dir, version_id))) {
        LOG_WARN("fail to construct ckpt data version dir", KR(ret), K(i), K(version_id),
            K(ckpt_data_dir), K(storage_dest), K(min_verison_to_keep), K(next_version_));
      } else if (OB_FAIL(tfm->delete_remote_dir(ckpt_data_version_dir))) {
        LOG_WARN("fail to clean ckpt data version dir", KR(ret), K(i), K(version_id),
            K(ckpt_data_version_dir), K(storage_dest), K(min_verison_to_keep), K(next_version_));
      }

      clean_used_time_us = ObTimeUtility::fast_current_time() - start_time_us;
      LOG_INFO("ObSSMacroCacheCkptTask: finish clean up macro cache ckpt", KR(ret), K(opt), K(i),
          K(start_time_us), K(clean_used_time_us), K(storage_dest), K(ckpt_data_dir),
          K(min_verison_to_keep), K(next_version_), K(op));
    }
  }

  return ret;
}

int ObSSMacroCacheCkptTask::read_ckpt_meta(
    const int64_t version_id,
    const int64_t meta_file_size,
    ObSSMacroCacheCkptMeta &ckpt_meta)
{
  int ret = OB_SUCCESS;
  ckpt_meta.reset();
  MacroBlockId macro_id;
  ObStorageObjectOpt opt;
  opt.set_ss_macro_cache_ckpt_opt(
      true/*is_meta*/, version_id, UINT64_MAX - 1 /*seq_id*/);
  ObStorageObjectHandle object_handle;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), MACRO_CACHE_CKPT_META_TAG));
  char *read_buf = nullptr;
  
  if (OB_UNLIKELY(version_id == UINT64_MAX || meta_file_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(version_id), K(meta_file_size));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, object_handle))) {
    LOG_WARN("fail to alloc object", KR(ret), K(opt), K(version_id));
  } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(meta_file_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc read buf", KR(ret), K(opt), K(version_id), K(meta_file_size));
  } else {
    MEMSET(read_buf, 0, meta_file_size);

    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = object_handle.get_macro_id();
    read_info.size_ = meta_file_size;
    read_info.offset_ = 0;
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = read_buf;
    read_info.mtl_tenant_id_ = MTL_ID();

    int64_t pos = 0;
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.read_object(read_info, object_handle))) {
      LOG_WARN("fail to read macro cache ckpt meta", KR(ret), K(opt), K(read_info));
    } else if (OB_FAIL(ObFileCommonHeaderUtil::deserialize<ObSSMacroCacheCkptMeta>(
        read_buf, object_handle.get_data_size(), pos, ckpt_meta))) {
      LOG_WARN("fail to deserialize macro cache ckpt meta", KR(ret), K(opt), K(read_info), K(pos));
    }
  }
  return ret;
}

int ObSSMacroCacheCkptTask::async_read_ckpt_data(
    char *buf, const int64_t buf_size,
    const int64_t version_id,
    const int64_t seq_id,
    blocksstable::ObStorageObjectHandle &read_handle)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_ss_macro_cache_ckpt_opt(false/*is_meta*/, version_id, seq_id);
  
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)
      || OB_UNLIKELY(version_id == UINT64_MAX || seq_id == UINT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(buf_size), K(version_id), K(seq_id));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(opt, read_handle))) {
    LOG_WARN("fail to alloc object", KR(ret), K(opt), KP(buf), K(buf_size));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = read_handle.get_macro_id();
    read_info.size_ = buf_size;
    read_info.offset_ = 0;
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(MTL_ID());
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();

    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_read_object(read_info, read_handle))) {
      LOG_WARN("fail to read macro cache ckpt meta", KR(ret), K(opt), K(read_info));
    }
  }

  return ret;
}

int ObSSMacroCacheCkptTask::try_restore_next_ckpt_version_()
{
  int ret = OB_SUCCESS;
  char ckpt_meta_dir[MAX_PATH_SIZE] = { 0 };
  ObMCGetCkptMetaVersionsOp op(MTL_ID());
  ObTenantFileManager *tfm = nullptr;
  if (OB_FAIL(OB_DIR_MGR.get_macro_cache_ckpt_meta_dir(ckpt_meta_dir, sizeof(ckpt_meta_dir),
                                                       MTL_ID(), MTL_EPOCH_ID()))) {
    LOG_WARN("fail to get ckpt meta file dir", KR(ret));
  } else if (OB_ISNULL(tfm = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant file manager is NULL", KR(ret));
  } else if (OB_FAIL(tfm->list_remote_files(ckpt_meta_dir, op))) {
    LOG_WARN("fail to list ckpt metas", KR(ret), K(ckpt_meta_dir));
  } else if (op.is_empty()) {
    next_version_ = 0;
  } else if (OB_UNLIKELY(!op.is_max_ckpt_version_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ckpt version invalid", KR(ret), K(ckpt_meta_dir), K(op));
  } else {
    next_version_ = op.max_ckpt_version_ + 1;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase