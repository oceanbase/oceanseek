/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_lob_aux_storager.h"
#include "lib/allocator/page_arena.h"               // ObArenaAllocator
#include "ob_log_store_service.h"                   // IObStoreService
#include "ob_log_utils.h"                           // get_timestamp
#include "logservice/libobcdc/src/ob_log_instance.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
int ObLobAuxKey::compare(const ObLobAuxKey &other) const
{
  int cmp_ret = 0;

  if (commit_version_ > other.commit_version_) {
    cmp_ret = 1;
  } else if (commit_version_ < other.commit_version_) {
    cmp_ret = -1;
  } else if (tenant_id_ > other.tenant_id_) {
    cmp_ret = 1;
  } else if (tenant_id_ < other.tenant_id_) {
    cmp_ret = -1;
  } else if (trans_id_ > other.trans_id_) {
    cmp_ret = 1;
  } else if (trans_id_ < other.trans_id_) {
    cmp_ret = -1;
  } else if (lob_aux_table_id_ > other.lob_aux_table_id_) {
    cmp_ret = 1;
  } else if (lob_aux_table_id_ < other.lob_aux_table_id_) {
    cmp_ret = -1;
  } else if (lob_id_ > other.lob_id_) {
    cmp_ret = 1;
  } else if (lob_id_ < other.lob_id_) {
    cmp_ret = -1;
  } else if (lob_operation_seq_no_ > other.lob_operation_seq_no_) {
    cmp_ret = 1;
  } else if (lob_operation_seq_no_ < other.lob_operation_seq_no_) {
    cmp_ret = -1;
  } else {
    // bool operator>(const ObLobAuxKey &other) const { return (1 == compare(other)); }
    // bool operator<(const ObLobAuxKey &other) const { return (-1 == compare(other)); }
    // operator> 和 operator<，依赖 compare() 的返回值为 -1、1。
    // ObString::compare() 只保证返回值的正负，不保证一定是 -1 或 1。例如两个字节相减后可能返回 -2、2，
    // 或其他非零值。这里需要归一化为 -1、1。
    const int seq_id_cmp = seq_id_.compare(other.seq_id_);
    if (seq_id_cmp > 0) {
      cmp_ret = 1;
    } else if (seq_id_cmp < 0) {
      cmp_ret = -1;
    }
  }

  return cmp_ret;
}

int ObLobAuxKey::get_key(std::string &key) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("lob store key is not valid", KR(ret), KPC(this));
  } else {
    key.append(std::to_string(commit_version_));
    // to isolate tenant resources, each tenant data store in separate column famliy
    // so no need to add tenant_id_ in key
    // key.append(std::to_string(tenant_id_));
    key.append("_");
    key.append(std::to_string(trans_id_.get_id()));
    key.append("_");
    key.append(std::to_string(lob_aux_table_id_));
    key.append("_");
    key.append(std::to_string(lob_id_.get_column_idx()));
    key.append("-");
    key.append(std::to_string(lob_id_.get_tablet_id()));
    key.append("_");
    key.append(std::to_string(lob_id_.get_auto_inc_id()));
    key.append("_");
    key.append(std::to_string(lob_operation_seq_no_.cast_to_int()));
    key.append("_");
    key.append(std::to_string(seq_id_.length()));
    key.append("_");
    key.append(storage::ObLobSeqId(seq_id_).get_hex_str());
  }
  return ret;
}

void ObCDCLobAuxDataCleanTask::reset()
{
  is_task_push_ = false;
  commit_version_ = OB_INVALID_VERSION;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

double ObCDCLobAuxDataStatInfo::calc_rps(
    const int64_t delta_time, const int64_t req_cnt, const int64_t last_req_cnt)
{
  double rps = 0.0;
  const int64_t delta_req_count = req_cnt - last_req_cnt;
  if (delta_time > 0) {
    rps = (double)(delta_req_count) * 1000000.0 / (double)delta_time;
  }
  return rps;
}

double ObCDCLobAuxDataStatInfo::calc_rate(
    const int64_t delta_time, const int64_t total_size, const int64_t last_total_size)
{
  double rate = 0.0;
  const int64_t delta_data_size = total_size - last_total_size;
  double delta_data_size_formatted = (double)delta_data_size / (double)_M_;
  if (delta_time > 0) {
    rate = (double)(delta_data_size_formatted) * 1000000.0 / (double)delta_time;
  }
  return rate;
}

ObCDCLobAuxStorager::ObCDCLobAuxStorager() :
    is_inited_(false),
    lob_aux_map_(),
    lob_aux_allocator_(),
    store_service_(nullptr),
    enable_memory_(true),
    clean_task_interval_us_(0)
{

}

ObCDCLobAuxStorager::~ObCDCLobAuxStorager()
{
  destroy();
}

int ObCDCLobAuxStorager::init(IObStoreService *store_service)
{
  int ret = OB_SUCCESS;
  const int64_t memory_limit = TCONF.lob_data_storage_memory_limit.get();
  enable_memory_ = TCONF.enable_lob_data_storage_memory;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObCDCLobAuxStorager has been initialized", KR(ret));
  } else if (enable_memory_) {
    if (memory_limit <= 0) {
      LOG_ERROR("invalid arguments", KR(ret), K(memory_limit));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(lob_aux_map_.init("LobAuxMap"))) {
      LOG_ERROR("lob_aux_map_ init failed", KR(ret));
    } else if (OB_FAIL(lob_aux_allocator_.init(
        LOB_AUX_ALLOCATOR_PAGE_SIZE,
        "LobAuxData",
        OB_SERVER_TENANT_ID,
        memory_limit))) {
      LOG_ERROR("init lob_aux_allocator_ fail", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    store_service_ = store_service;
    is_inited_ = true;
    clean_task_interval_us_ = TCONF.lob_data_storage_clean_interval_sec * _SEC_;

    LOG_INFO("ObCDCLobAuxStorager init succ", K(enable_memory_), K(memory_limit), K(clean_task_interval_us_));
  }

  return ret;
}

void ObCDCLobAuxStorager::destroy()
{
  if (is_inited_) {
    clean_task_interval_us_ = 0;
    store_service_ = nullptr;
    if (enable_memory_) {
      lob_aux_allocator_.destroy();
      lob_aux_map_.destroy();
      enable_memory_ = false;
    }
    is_inited_ = false;
  }
}

void ObCDCLobAuxStorager::configure(const ObLogConfig &cfg)
{
  const int64_t memory_limit = cfg.lob_data_storage_memory_limit.get();
  const int64_t clean_task_interval_us = cfg.lob_data_storage_clean_interval_sec * _SEC_;
  if (is_inited_ && enable_memory_) {
    lob_aux_allocator_.set_total_limit(memory_limit);
  }
  ATOMIC_STORE(&clean_task_interval_us_, clean_task_interval_us);
  LOG_INFO("[LOB_STORAGER] [CONFIG]", K(memory_limit), K(clean_task_interval_us));
}

int ObCDCLobAuxStorager::put(
    const ObLobAuxKey &key,
    const char *key_type,
    const char *lob_data,
    const int64_t lob_data_len)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxStorager has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!key.is_valid()) || OB_ISNULL(key_type)
      || OB_ISNULL(lob_data) || OB_UNLIKELY(lob_data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(key), K(lob_data), K(lob_data_len));
  } else if (!enable_memory_) {
    if (OB_FAIL(disk_put_(key, key_type, lob_data, lob_data_len))) {
      LOG_ERROR("disk_put_ failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    }
  } else if (OB_FAIL(memory_put_(key, key_type, lob_data, lob_data_len))) {
    if (OB_ALLOCATE_MEMORY_FAILED != ret) {
      LOG_ERROR("memory_put_ failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    } else if (OB_FAIL(disk_put_(key, key_type, lob_data, lob_data_len))) {
      LOG_ERROR("disk_put_ failed", KR(ret), K(key), K(lob_data), K(lob_data_len));
    }
  }
  if (OB_SUCC(ret)) {
    ATOMIC_INC(&stat_info_.put_req_cnt_);
    ATOMIC_AAF(&stat_info_.put_data_total_size_, lob_data_len);
  }
  return ret;
}

// ObLobAuxKey::seq_id_ 是不持有内存的 ObString，直接将入参 key 插入 HashMap 只会浅拷贝
// seq_id_ 的地址；调用方内存失效后，缓存中的 Key 会引用无效内存。因此这里将 LOB 数据和
// seq_id 一起复制到 Storager 管理的内存块中，再用 stored_key 复制 key 的其他字段，并将
// stored_key.seq_id_ 重定向到该内存块中的 seq_id，保证缓存 Key 在记录被删除前始终有效。
int ObCDCLobAuxStorager::memory_put_(
    const ObLobAuxKey &key,
    const char *key_type,
    const char *lob_data,
    const int64_t lob_data_len)
{
  int ret = OB_SUCCESS;
  const int64_t alloc_size = lob_data_len + key.seq_id_.length();
  char *alloc_buf = static_cast<char *>(lob_aux_allocator_.alloc(alloc_size));
  if (OB_ISNULL(alloc_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_DEBUG("lob aux allocator alloc failed, fallback to disk", KR(ret), K(key), K(alloc_size),
        "memory_hold", SIZE_TO_STR(lob_aux_allocator_.allocated()));
  } else {
    MEMCPY(alloc_buf, lob_data, lob_data_len);
    ObLobAuxKey stored_key = key;
    if (!key.seq_id_.empty()) {
      MEMCPY(alloc_buf + lob_data_len, key.seq_id_.ptr(), key.seq_id_.length());
      stored_key.seq_id_.assign_ptr(alloc_buf + lob_data_len, key.seq_id_.length());
    }
    ObLobAuxValue value(alloc_buf, alloc_buf/*lob_data*/, lob_data_len);
    if (OB_FAIL(lob_aux_map_.insert(stored_key, value))) {
      LOG_ERROR("[OBCDC][LOB_AUX][PUT][MEM] lob_aux_map_ insert failed", KR(ret), KCSTRING(key_type),
          K(key), K(lob_data), K(lob_data_len));
      lob_aux_allocator_.free(alloc_buf);
    } else {
      ATOMIC_INC(&stat_info_.mem_put_req_cnt_);
      ATOMIC_AAF(&stat_info_.mem_put_data_total_size_, lob_data_len);
      LOG_DEBUG("[OBCDC][LOB_AUX][PUT][MEM] lob_aux_map_ insert succ", KCSTRING(key_type), K(key), K(value));
    }
  }
  return ret;
}

int ObCDCLobAuxStorager::disk_put_(
    const ObLobAuxKey &key,
    const char *key_type,
    const char *lob_data,
    const int64_t lob_data_len)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  void *cf_handle = nullptr;
  if (OB_FAIL(get_cf_handle(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), KCSTRING(key_type), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), KCSTRING(key_type), K(key));
  } else if (OB_FAIL(store_service_->put(cf_handle, key_str, ObSlice(lob_data, lob_data_len)))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("[OBCDC][LOB_AUX][PUT][DISK] store_service_ put fail", KR(ret), KCSTRING(key_type),
          K(key), KCSTRING(key_str.c_str()), K(lob_data_len));
    }
  } else {
    LOG_DEBUG("[OBCDC][LOB_AUX][PUT][DISK] store_service_ insert succ", KCSTRING(key_type), K(key),
        KCSTRING(key_str.c_str()), K(lob_data_len));
  }
  return ret;
}

int ObCDCLobAuxStorager::get(
    ObIAllocator &allocator,
    const ObLobAuxKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxStorager has not been initialized", KR(ret));
  } else if (!enable_memory_) {
    if (OB_FAIL(disk_get_(allocator, key, lob_data, lob_data_len))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("disk_get_ failed", KR(ret), K(key));
      }
    }
  } else if (OB_FAIL(memory_get_(key, lob_data, lob_data_len))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("memory_get_ failed", KR(ret), K(key));
    } else if (OB_FAIL(disk_get_(allocator, key, lob_data, lob_data_len))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("disk_get_ failed", KR(ret), K(key));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ATOMIC_INC(&stat_info_.get_req_cnt_);
    ATOMIC_AAF(&stat_info_.get_data_total_size_, lob_data_len);
  }
  return ret;
}

int ObCDCLobAuxStorager::memory_get_(
    const ObLobAuxKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  lob_data = nullptr;
  lob_data_len = 0;
  ObLobAuxValue value;
  if (OB_FAIL(lob_aux_map_.get(key, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("[OBCDC][LOB_AUX][GET][MEM] lob_aux_map_ get failed", KR(ret), K(key));
    } else if (REACH_TIME_INTERVAL(10 * _SEC_)) {
      LOG_WARN("[OBCDC][LOB_AUX][GET][MEM] lob_aux_map_ get not exist, need retry", KR(ret), K(key));
    }
  } else {
    lob_data = value.lob_data_;
    lob_data_len = value.lob_data_len_;

    ATOMIC_INC(&stat_info_.mem_get_req_cnt_);
    ATOMIC_AAF(&stat_info_.mem_get_data_total_size_, lob_data_len);
  }
  return ret;
}

int ObCDCLobAuxStorager::disk_get_(
    ObIAllocator &allocator,
    const ObLobAuxKey &key,
    const char *&lob_data,
    int64_t &lob_data_len)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  std::string value;
  char *lob_data_ptr = nullptr;
  void *cf_handle = nullptr;
  if (OB_FAIL(get_cf_handle(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->get(cf_handle, key_str, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("[OBCDC][LOB_AUX][GET][DISK] get failed", KR(ret), K(key));
    } else if (REACH_TIME_INTERVAL(10 * _SEC_)) {
      LOG_WARN("[OBCDC][LOB_AUX][GET][DISK] get not exist, need retry", KR(ret), K(key));
    }
  } else if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("data is emptry", KR(ret), K(key));
  } else if (OB_ISNULL(lob_data_ptr = static_cast<char*>(allocator.alloc(value.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc fail", KR(ret), "size", value.length(), K(key));
  } else {
    MEMCPY(lob_data_ptr, value.c_str(), value.length());
    lob_data = lob_data_ptr;
    lob_data_len = value.length();
  }
  return ret;
}

int ObCDCLobAuxStorager::del(
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObCDCLobAuxStorager del", K(lob_data_out_row_ctx_list));
  const IStmtTask *stmt_task = lob_data_out_row_ctx_list.get_stmt_task();
  int64_t commit_version = OB_INVALID_VERSION;
  const uint64_t tenant_id = lob_data_out_row_ctx_list.get_tenant_id();
  const transaction::ObTransID &trans_id = lob_data_out_row_ctx_list.get_trans_id();
  ObLobDataGetCtxList &lob_data_get_ctx_list = lob_data_out_row_ctx_list.get_lob_data_get_ctx_list();
  ObLobDataGetCtx *cur_lob_data_get_ctx = lob_data_get_ctx_list.head_;

  if (OB_ISNULL(stmt_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("stmt_task is nullptr", KR(ret), K(lob_data_out_row_ctx_list));
  } else {
    commit_version = stmt_task->get_host().get_trans_commit_version();
  }

  while (OB_SUCC(ret) && !stop_flag && OB_NOT_NULL(cur_lob_data_get_ctx)) {
    const bool has_new_image_desc = cur_lob_data_get_ctx->has_image_desc(true/*is_new_col*/);
    const bool has_old_image_desc = cur_lob_data_get_ctx->has_image_desc(false/*is_new_col*/);
    if (OB_UNLIKELY(!has_new_image_desc && !has_old_image_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob data get ctx has no image desc", KR(ret), KPC(cur_lob_data_get_ctx));
    } else if (has_new_image_desc
        && OB_FAIL(del_lob_image_(commit_version, tenant_id, trans_id,
            lob_data_out_row_ctx_list, *cur_lob_data_get_ctx, true, stop_flag))) {
      LOG_ERROR("delete new lob image cache failed", KR(ret), KPC(cur_lob_data_get_ctx));
    } else if (has_old_image_desc
        && OB_FAIL(del_lob_image_(commit_version, tenant_id, trans_id,
            lob_data_out_row_ctx_list, *cur_lob_data_get_ctx, false, stop_flag))) {
      LOG_ERROR("delete old lob image cache failed", KR(ret), KPC(cur_lob_data_get_ctx));
    }
    if (OB_SUCC(ret)) {
      cur_lob_data_get_ctx = cur_lob_data_get_ctx->get_next();
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObCDCLobAuxStorager::del_lob_image_(
    const int64_t commit_version,
    const uint64_t tenant_id,
    const transaction::ObTransID &trans_id,
    ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
    ObLobDataGetCtx &lob_data_get_ctx,
    const bool is_new_col,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const ObLobImageAssembleDesc &desc = lob_data_get_ctx.get_image_desc(is_new_col);
  const ObLobColCtx &col_ctx = lob_data_get_ctx.get_lob_col_ctx(is_new_col);
  char summary_seq_buf[storage::ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString summary_seq_id(sizeof(summary_seq_buf), summary_seq_buf);
  if (OB_UNLIKELY(!desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(storage::ObLobSeqId::get_summary_seq_id(summary_seq_id))) {
    LOG_ERROR("generate summary seq id failed", KR(ret), K(desc));
  } else {
    ObArenaAllocator seq_id_allocator(ObMemAttr(tenant_id, "CDCLobSeqTmp"));
    storage::ObLobSeqIdGenerator generator(summary_seq_id, &seq_id_allocator);
    for (uint64_t idx = 0; OB_SUCC(ret) && (idx < col_ctx.chunk_count_) && !stop_flag; ++idx) {
      ObString piece_seq_id;
      if (OB_FAIL(generator.get_next_seq_id(piece_seq_id))) {
        LOG_ERROR("generate piece seq id for cleanup failed", KR(ret), K(idx), K(col_ctx));
      } else {
        const ObLobAuxKey piece_key(commit_version, tenant_id, trans_id,
            lob_data_out_row_ctx_list.get_aux_lob_piece_table_id(), desc.lob_id_,
            desc.lob_operation_seq_no_, piece_seq_id);
        if (OB_FAIL(del(piece_key))) {
          LOG_ERROR("delete lob piece cache failed", KR(ret), K(piece_key));
        }
      }
    }
    if (OB_SUCC(ret) && !stop_flag) {
      const ObLobAuxKey summary_key(commit_version, tenant_id, trans_id,
          lob_data_out_row_ctx_list.get_aux_lob_meta_table_id(), desc.lob_id_,
          desc.lob_operation_seq_no_, summary_seq_id);
      if (OB_FAIL(del(summary_key))) {
        LOG_ERROR("delete lob summary cache failed", KR(ret), K(summary_key));
      }
    }
  }
  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }
  return ret;
}

int ObCDCLobAuxStorager::del(const ObLobAuxKey &key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObCDCLobAuxStorager has not been initialized", KR(ret));
  } else if (!enable_memory_) {
    if (OB_FAIL(disk_del_(key))) {
      LOG_ERROR("disk_del_ fail", KR(ret), K(key));
    }
  } else if (OB_FAIL(memory_del_(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("memory_get_ fail", KR(ret), K(key));
    } else if (OB_FAIL(disk_del_(key))) {
      LOG_ERROR("disk_del_ fail", KR(ret), K(key));
    }
  }
  return ret;
}

int ObCDCLobAuxStorager::memory_del_(const ObLobAuxKey &key)
{
  int ret = OB_SUCCESS;
  ObLobAuxValue value;
  if (OB_FAIL(lob_aux_map_.erase(key, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("lob_aux_map_ erase failed", KR(ret), K(key));
    }
  } else {
    if (OB_ISNULL(value.alloc_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc_buf is nullptr", KR(ret), K(key));
    } else {
      lob_aux_allocator_.free(value.alloc_buf_);
    }
  }
  return ret;
}

int ObCDCLobAuxStorager::disk_del_(const ObLobAuxKey &key)
{
  int ret = OB_SUCCESS;
  std::string key_str;
  void *cf_handle = nullptr;
  if (OB_FAIL(get_cf_handle(key.tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), K(key));
  } else if (OB_FAIL(key.get_key(key_str))) {
    LOG_ERROR("get key_str fail", KR(ret), K(key));
  } else if (OB_FAIL(store_service_->del(cf_handle, key_str))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("del failed", KR(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    LOG_DEBUG("store_service_ del succ", K(key), KCSTRING(key_str.c_str()));
  }
  return ret;
}

int ObCDCLobAuxStorager::clean_unused_data(ObCDCLobAuxDataCleanTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_clean_unused_data_(task))) {
    LOG_WARN("do_clean_unused_data_ fail", KR(ret));
  } else {
    ATOMIC_STORE(&task->is_task_push_, false);
  }
  return ret;
}

int ObCDCLobAuxStorager::do_clean_unused_data_(ObCDCLobAuxDataCleanTask *task)
{
  int ret = OB_SUCCESS;
  void *cf_handle = nullptr;
  if (enable_memory_) {
    if (OB_FAIL(memory_del_(task->tenant_id_, task->commit_version_))) {
      LOG_ERROR("memory_del_ failed", KR(ret), KPC(task));
    }
  }
  if (FAILEDx(get_cf_handle(task->tenant_id_, cf_handle))) {
    LOG_ERROR("get_cf_handle fail", KR(ret), KPC(task));
  } else if (OB_FAIL(disk_del_(cf_handle, task->commit_version_))) {
    LOG_ERROR("disk_del_ failed", KR(ret), KPC(task));
  }
  return ret;
}

int ObCDCLobAuxStorager::memory_del_(const uint64_t tenant_id, const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  ObLobAuxDataPurger lob_aux_data_purger(*this, tenant_id, commit_version);
  if (OB_FAIL(lob_aux_map_.remove_if(lob_aux_data_purger))) {
    LOG_ERROR("[OBCDC][LOB_AUX][CLEAN_TASK][MEM] lob_aux_map_ remove_if failed", KR(ret),
        K(tenant_id), K(commit_version));
  } else {
    LOG_INFO("[OBCDC][LOB_AUX][CLEAN_TASK][MEM] ObCDCLobAuxStorager del", K(tenant_id),
        K(commit_version), "purge_count",
        lob_aux_data_purger.purge_count_, "map_count", lob_aux_map_.count(),
        "memory_hold", SIZE_TO_STR(lob_aux_allocator_.allocated()));
  }
  return ret;
}

int ObCDCLobAuxStorager::disk_del_(void* cf_family, const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  int64_t estimate_live_data_size = 0;
  int64_t estimate_num_keys = 0;
  std::string begin_key;
  std::string end_key;
  // some trans may have same commit_version,
  // to avoid accidentally deleting uncommitted transactions,
  // we set end_key to "commit_version - 1".
  end_key.append(std::to_string(commit_version - 1));
  end_key.append("_}");
  if (OB_FAIL(store_service_->del_range(cf_family, begin_key, end_key))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("[OBCDC][LOB_AUX][CLEAN_TASK][DISK] store_service_ del fail", KR(ret), "begin_key", begin_key.c_str(),
          "end_key", end_key.c_str(), K(commit_version));
    }
  } else if (OB_FAIL(store_service_->compact_range(cf_family, begin_key, end_key))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service_ compact_range fail", KR(ret), "begin_key", begin_key.c_str(),
          "end_key", end_key.c_str(), K(commit_version));
    }
  } else if (OB_FAIL(store_service_->get_mem_usage(cf_family, estimate_live_data_size, estimate_num_keys))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("store_service_ get_mem_usage fail", KR(ret), "begin_key", begin_key.c_str(),
          "end_key", end_key.c_str(), K(commit_version));
    }
  } else {
    LOG_INFO("[OBCDC][LOB_AUX][CLEAN_TASK][DISK] store_service_ del_range succ", K(commit_version),
        "end_key", end_key.c_str(), "estimate_live_data_size", SIZE_TO_STR(estimate_live_data_size),
        K(estimate_num_keys));
  }
  return ret;
}

int ObCDCLobAuxStorager::get_cf_handle(const uint64_t tenant_id, void *&cf_handle)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = nullptr;
  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else {
    tenant = guard.get_tenant();
    cf_handle = tenant->get_lob_storage_cf_handle();
  }
  return ret;
}

int ObCDCLobAuxStorager::get_clean_task(const uint64_t tenant_id, ObCDCLobAuxDataCleanTask *&task)
{
  int ret = OB_SUCCESS;
  ObLogTenantGuard guard;
  ObLogTenant *tenant = nullptr;
  if (OB_FAIL(TCTX.get_tenant_guard(tenant_id, guard))) {
    LOG_ERROR("get_tenant_guard fail", KR(ret), K(tenant_id));
  } else {
    tenant = guard.get_tenant();
    task = &tenant->get_lob_storage_clean_task();
  }
  return ret;
}

void ObCDCLobAuxStorager::print_stat_info()
{
  int64_t current_timestamp = get_timestamp();
  int64_t local_last_stat_time = stat_info_.last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;
  // Update last statistic value
  stat_info_.last_stat_time_ = current_timestamp;

  // get req rps
  int64_t get_req_cnt = ATOMIC_LOAD(&stat_info_.get_req_cnt_);
  int64_t last_get_req_cnt = ATOMIC_LOAD(&stat_info_.last_get_req_cnt_);
  double get_req_rps = stat_info_.calc_rps(delta_time, get_req_cnt, last_get_req_cnt);
  stat_info_.last_get_req_cnt_ = get_req_cnt;

  // mem get req rps
  int64_t mem_get_req_cnt = ATOMIC_LOAD(&stat_info_.mem_get_req_cnt_);
  int64_t last_mem_get_req_cnt = ATOMIC_LOAD(&stat_info_.last_mem_get_req_cnt_);
  double mem_get_req_rps = stat_info_.calc_rps(delta_time, mem_get_req_cnt, last_mem_get_req_cnt);
  double mem_get_req_rps_percentage = (get_req_rps != 0 ? mem_get_req_rps/get_req_rps : 0);
  stat_info_.last_mem_get_req_cnt_ = mem_get_req_cnt;

  // get rate
  int64_t get_data_total_size = ATOMIC_LOAD(&stat_info_.get_data_total_size_);
  int64_t last_get_data_total_size = ATOMIC_LOAD(&stat_info_.last_get_data_total_size_);
  double get_rate = stat_info_.calc_rate(delta_time, get_data_total_size, last_get_data_total_size);
  stat_info_.last_get_data_total_size_ = get_data_total_size;

  // mem get rate
  int64_t mem_get_data_total_size = ATOMIC_LOAD(&stat_info_.mem_get_data_total_size_);
  int64_t last_mem_get_data_total_size = ATOMIC_LOAD(&stat_info_.last_mem_get_data_total_size_);
  double mem_get_rate = stat_info_.calc_rate(delta_time, mem_get_data_total_size, last_mem_get_data_total_size);
  double mem_get_rate_percentage = (get_rate != 0 ? mem_get_rate/get_rate : 0);
  stat_info_.last_mem_get_data_total_size_ = mem_get_data_total_size;

  // put req rps
  int64_t put_req_cnt = ATOMIC_LOAD(&stat_info_.put_req_cnt_);
  int64_t last_put_req_cnt = ATOMIC_LOAD(&stat_info_.last_put_req_cnt_);
  double put_req_rps = stat_info_.calc_rps(delta_time, put_req_cnt, last_put_req_cnt);
  stat_info_.last_put_req_cnt_ = put_req_cnt;

  // mem put req rps
  int64_t mem_put_req_cnt = ATOMIC_LOAD(&stat_info_.mem_put_req_cnt_);
  int64_t last_mem_put_req_cnt = ATOMIC_LOAD(&stat_info_.last_mem_put_req_cnt_);
  double mem_put_req_rps = stat_info_.calc_rps(delta_time, mem_put_req_cnt, last_mem_put_req_cnt);
  double mem_put_req_rps_percentage = (put_req_rps != 0 ? mem_put_req_rps/put_req_rps : 0);
  stat_info_.last_mem_put_req_cnt_ = mem_put_req_cnt;

  // put rate
  int64_t put_data_total_size = ATOMIC_LOAD(&stat_info_.put_data_total_size_);
  int64_t last_put_data_total_size = ATOMIC_LOAD(&stat_info_.last_put_data_total_size_);
  double put_rate = stat_info_.calc_rate(delta_time, put_data_total_size, last_put_data_total_size);
  stat_info_.last_put_data_total_size_ = put_data_total_size;

  // mem put rate
  int64_t mem_put_data_total_size = ATOMIC_LOAD(&stat_info_.mem_put_data_total_size_);
  int64_t last_mem_put_data_total_size = ATOMIC_LOAD(&stat_info_.last_mem_put_data_total_size_);
  double mem_put_rate = stat_info_.calc_rate(delta_time, mem_put_data_total_size, last_mem_put_data_total_size);
  double mem_put_rate_percentage = (put_rate != 0 ? mem_put_rate/put_rate : 0);
  stat_info_.last_mem_put_data_total_size_ = mem_put_data_total_size;


  _LOG_INFO("[LOB_STORAGER] [STAT] "
      "GET_RPS=%.3lf MEM_GET_RPS=%.3lf MEM_GET_RPS_PERCENTAGE=%.3lf "
      "GET_RATE=%.5lfM/s MEM_GET_RATE=%.5lfM/s MEM_GET_RATE_PERCENTAGE=%.3lf "
      "PUT_RPS=%.3lf MEM_PUT_RPS=%.3lf MEM_PUT_RPS_PERCENTAGE=%.3lf "
      "PUT_RATE=%.5lfM/s MEM_PUT_RATE=%.5lfM/s MEM_PUT_RATE_PERCENTAGE=%.3lf "
      "MAP_COUNT=%lu MEMORY_HOLD=%s ",
      get_req_rps, mem_get_req_rps, mem_get_req_rps_percentage,
      get_rate, mem_get_rate, mem_get_rate_percentage,
      put_req_rps, mem_put_req_rps, mem_put_req_rps_percentage,
      put_rate, mem_put_rate, mem_put_rate_percentage,
      lob_aux_map_.count(), SIZE_TO_STR(lob_aux_allocator_.allocated()));
}

bool ObCDCLobAuxStorager::ObLobAuxDataPurger::operator()(
    const ObLobAuxKey &lob_aux_key,
    ObLobAuxValue &lob_aux_value)
{
  int ret = OB_SUCCESS;
  const bool need_purge = (tenant_id_ == lob_aux_key.tenant_id_)
    && (commit_version_ > lob_aux_key.commit_version_);

  if (need_purge) {
    if (OB_ISNULL(lob_aux_value.alloc_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc_buf is nullptr", KR(ret), K(lob_aux_key), K(lob_aux_value));
    } else {
      ++purge_count_;
      host_.lob_aux_allocator_.free(lob_aux_value.alloc_buf_);
      LOG_DEBUG("purge succ", K(lob_aux_key), K(purge_count_),
          "lob_data_len", lob_aux_value.lob_data_len_,
          "memory_hold", SIZE_TO_STR(host_.lob_aux_allocator_.allocated()));
    }
  }

  return need_purge;
}

bool ObCDCLobAuxStorager::ObLobAuxDataPrinter::operator()(
    const ObLobAuxKey &lob_aux_key,
    ObLobAuxValue &lob_aux_value)
{
  LOG_INFO("ObLobAuxKey for_each", K(lob_aux_key));
  return true;
}

} // namespace libobcdc
} // namespace oceanbase
