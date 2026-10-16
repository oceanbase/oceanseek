/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_RPC_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_RPC_STRUCT_H_

#include <stdint.h>
#include "common/ob_version_def.h"
#include "lib/ob_define.h"
#include "lib/container/ob_array_serialization.h"
#include "share/ob_ls_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"

namespace oceanbase
{
namespace storage
{
struct LocalCacheRetentionRate;

const int64_t OB_HA_FETCH_LOCAL_CACHE_BLOCK_RPC_BUF_SIZE = (OB_DEFAULT_MACRO_BLOCK_SIZE * 2); // 4MB

/******************************** for get ls info ************************************/
struct ObGetHALocalCacheLSInfoArg final
{
public:
  ObGetHALocalCacheLSInfoArg() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_() {}
  ~ObGetHALocalCacheLSInfoArg() { reset(); }
  bool is_valid() const { return ((OB_INVALID_TENANT_ID != tenant_id_) && ls_id_.is_valid()); }
  void reset() { tenant_id_ = OB_INVALID_TENANT_ID; ls_id_.reset();}
  TO_STRING_KV(K_(tenant_id), K_(ls_id));

public:
  static const int64_t OB_GET_HA_LOCAL_CACHE_LS_INFO_ARG_VERSION = 1;
  OB_UNIS_VERSION(OB_GET_HA_LOCAL_CACHE_LS_INFO_ARG_VERSION);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};


struct ObSSLocalCacheLSInfo final
{
public:
  ObSSLocalCacheLSInfo() : macro_cache_ls_info_() {}
  ~ObSSLocalCacheLSInfo() { reset(); }
  bool is_valid() const { return macro_cache_ls_info_.is_valid(); }
  void reset() { macro_cache_ls_info_.reset(); }
  int64_t get_valid_size() const { return macro_cache_ls_info_.get_valid_size(); }
  TO_STRING_KV(K_(macro_cache_ls_info));

public:
  static const int64_t OB_SS_LOCAL_CACHE_LS_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_LOCAL_CACHE_LS_INFO_VERSION);

public:
  ObSSMacroCacheLSInfo macro_cache_ls_info_;
};


struct ObGetHALocalCacheLSInfoRes final
{
public:
  ObGetHALocalCacheLSInfoRes() : local_cache_ls_info_() {}
  ~ObGetHALocalCacheLSInfoRes() { reset(); }
  void reset() { local_cache_ls_info_.reset(); }
  TO_STRING_KV(K_(local_cache_ls_info));

public:
  static const int64_t OB_GET_HA_LOCAL_CACHE_LS_INFO_RES_VERSION = 1;
  OB_UNIS_VERSION(OB_GET_HA_LOCAL_CACHE_LS_INFO_RES_VERSION);

public:
  ObSSLocalCacheLSInfo local_cache_ls_info_;
};


/******************************** for get cache job info ************************************/
enum class ObHACacheJobType : uint8_t //FARM COMPAT WHITELIST
{
  MACRO_CACHE_JOB_TYPE = 0,
  JOB_TYPE_MAX = 1
};


struct ObHALocalCacheTaskCount final
{
public:
  ObHALocalCacheTaskCount() : macro_cache_task_count_(0) {}
  ~ObHALocalCacheTaskCount() { reset(); }
  void reset() { macro_cache_task_count_ = 0; }
  TO_STRING_KV(K_(macro_cache_task_count));

public:
  static const int64_t OB_HA_LOCAL_CACHE_TASK_COUNT_VERSION = 1;
  OB_UNIS_VERSION(OB_HA_LOCAL_CACHE_TASK_COUNT_VERSION);

public:
  int64_t macro_cache_task_count_;
};


struct ObHACacheJobInfo final
{
public:
  ObHACacheJobInfo() : job_type_(ObHACacheJobType::JOB_TYPE_MAX), start_idx_(0), end_idx_(0) {}
  ~ObHACacheJobInfo() { reset(); }
  bool is_valid() const;
  void reset() { job_type_ = ObHACacheJobType::JOB_TYPE_MAX; start_idx_ = 0; end_idx_ = 0; }
  TO_STRING_KV(K_(job_type), K_(start_idx), K_(end_idx));

public:
  static const int64_t OB_HA_CACHE_JOB_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HA_CACHE_JOB_INFO_VERSION);

public:
  ObHACacheJobType job_type_;
  int64_t start_idx_;
  int64_t end_idx_;
};


struct ObGetHACacheJobInfoArg final
{
public:
  ObGetHACacheJobInfoArg()
    : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), task_count_()
  {}
  ~ObGetHACacheJobInfoArg() { reset(); }
  bool is_valid() const;
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_id_.reset();
    task_count_.reset();
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(task_count));

public:
  static const int64_t OB_GET_HA_CACHE_JOB_INFO_ARG_VERSION = 1;
  OB_UNIS_VERSION(OB_GET_HA_CACHE_JOB_INFO_ARG_VERSION);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObHALocalCacheTaskCount task_count_;
};


struct ObGetHACacheJobInfoRes final
{
public:
  ObGetHACacheJobInfoRes();
  ~ObGetHACacheJobInfoRes() { reset(); }
  void reset() { cache_job_infos_.reset(); }
  TO_STRING_KV(K_(cache_job_infos));

public:
  static const int64_t OB_GET_HA_CACHE_JOB_INFO_RES_VERSION = 1;
  OB_UNIS_VERSION(OB_GET_HA_CACHE_JOB_INFO_RES_VERSION);

public:
  common::ObSArray<ObHACacheJobInfo> cache_job_infos_;
};


/******************************** for get macro key ************************************/
struct ObGetLocalCacheKeyArg final
{
public:
  ObGetLocalCacheKeyArg() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), job_info_() {}
  ~ObGetLocalCacheKeyArg() { reset(); }
  bool is_valid() const { return ((OB_INVALID_TENANT_ID != tenant_id_) && ls_id_.is_valid() && job_info_.is_valid()); }
  void reset() { tenant_id_ = OB_INVALID_TENANT_ID; ls_id_.reset(); job_info_.reset(); }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(job_info));

public:
  static const int64_t OB_GET_LOCAL_CACHE_KEY_VERSION = 1;
  OB_UNIS_VERSION(OB_GET_LOCAL_CACHE_KEY_VERSION);

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObHACacheJobInfo job_info_;
};


struct ObCopyLocalCacheKeyRpcHeader final
{
public:
  enum ConnectStatus
  {
    RECONNECT = 0,
    ENDCONNECT = 1,
    MAX_STATUS
  };
  ObCopyLocalCacheKeyRpcHeader()
    : object_count_(0), end_idx_(0), connect_status_(ConnectStatus::MAX_STATUS)
  {}
  ~ObCopyLocalCacheKeyRpcHeader() { reset(); }
  void reset() { object_count_ = 0; end_idx_ = 0; connect_status_ = ConnectStatus::MAX_STATUS; }
  TO_STRING_KV(K_(object_count), K_(end_idx), K_(connect_status));

public:
  static const int64_t OB_COPY_LOCAL_CACHE_KEY_RPC_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_COPY_LOCAL_CACHE_KEY_RPC_HEADER_VERSION);

public:
  int32_t object_count_;
  int64_t end_idx_;
  ConnectStatus connect_status_;
};


struct ObHALocalCacheKeys final
{
public:
  ObHALocalCacheKeys();
  ~ObHALocalCacheKeys() { reset(); }
  bool is_valid() const;
  int assign(const ObHALocalCacheKeys &other);
  void reset() { job_type_ = ObHACacheJobType::JOB_TYPE_MAX; macro_meta_array_.reset(); }
  int64_t count();
  int filter_metas_by_retention_rate(const LocalCacheRetentionRate &cache_retention_rate);
  TO_STRING_KV(K_(job_type), "macro_meta_array_cnt", macro_meta_array_.count(),
               K_(macro_meta_array));

private:
  int filter_macro_cache_metas(const LocalCacheRetentionRate &cache_retention_rate);
  int filter_macro_cache_metas_v441(const LocalCacheRetentionRate &cache_retention_rate);
  int filter_macro_cache_metas_v451(const LocalCacheRetentionRate &cache_retention_rate);

public:
  static const int64_t OB_HA_LOCAL_CACHE_KEYS_VERSION = 1;
  OB_UNIS_VERSION(OB_HA_LOCAL_CACHE_KEYS_VERSION);

public:
  ObHACacheJobType job_type_;
  common::ObSArray<ObSSMacroPrewarmMeta> macro_meta_array_;
};


struct ObGetLocalCacheKeyRes final
{
public:
  ObGetLocalCacheKeyRes() : header_(), local_cache_keys_() {}
  ~ObGetLocalCacheKeyRes() { reset(); }
  void reset() { header_.reset(); local_cache_keys_.reset(); }
  TO_STRING_KV(K_(header), K_(local_cache_keys));

public:
  static const int64_t OB_GET_LOCAL_CACHE_KEY_RES_VERSION = 1;
  OB_UNIS_VERSION(OB_GET_LOCAL_CACHE_KEY_RES_VERSION);

public:
  ObCopyLocalCacheKeyRpcHeader header_;
  ObHALocalCacheKeys local_cache_keys_;
};


/******************************** for get macro data ************************************/
struct ObHAFetchLocalCacheBlockArg final
{
public:
  ObHAFetchLocalCacheBlockArg();
  ~ObHAFetchLocalCacheBlockArg() { reset(); }
  bool is_valid() const;
  void reset();
  int assign(const ObHAFetchLocalCacheBlockArg &other);
  TO_STRING_KV(K_(tenant_id), K_(local_cache_keys));

public:
  static const int64_t OB_HA_FETCH_LOCAL_CACHE_BLOCK_ARG_VERSION = 1;
  OB_UNIS_VERSION(OB_HA_FETCH_LOCAL_CACHE_BLOCK_ARG_VERSION);

public:
  uint64_t tenant_id_;
  ObHALocalCacheKeys local_cache_keys_;
};


struct ObHAFetchLocalCacheBlockHeader final
{
public:
  ObHAFetchLocalCacheBlockHeader() : job_type_(ObHACacheJobType::JOB_TYPE_MAX), body_size_(0) {}
  ~ObHAFetchLocalCacheBlockHeader() { reset(); }
  void reset() { job_type_ = ObHACacheJobType::JOB_TYPE_MAX; body_size_ = 0; }
  bool is_valid() const { return (ObHACacheJobType::JOB_TYPE_MAX != job_type_) && (body_size_ > 0); }
  TO_STRING_KV(K_(job_type), K_(body_size));

public:
  static const int64_t OB_HA_FETCH_LOCAL_CACHE_BLOCK_HEADER_VERSION = 1;
  OB_UNIS_VERSION(OB_HA_FETCH_LOCAL_CACHE_BLOCK_HEADER_VERSION);

public:
  ObHACacheJobType job_type_;
  int64_t body_size_;
};


struct ObHAFetchLocalCacheBlockMeta final
{
public:
  ObHAFetchLocalCacheBlockMeta();
  ~ObHAFetchLocalCacheBlockMeta() { reset(); }
  void reset() { job_type_ = ObHACacheJobType::JOB_TYPE_MAX; }
  bool is_valid() const;
  TO_STRING_KV(K_(job_type), K_(macro_prewarm_meta));

public:
  static const int64_t OB_HA_FETCH_LOCAL_CACHE_BLOCK_META_VERSION = 1;
  OB_UNIS_VERSION(OB_HA_FETCH_LOCAL_CACHE_BLOCK_META_VERSION);

public:
  ObHACacheJobType job_type_;
  ObSSMacroPrewarmMeta macro_prewarm_meta_;
};

struct LocalCacheRetentionRate final
{
public:
  LocalCacheRetentionRate() : version_(CLUSTER_VERSION_4_5_1_0), lru_rate_(0), fifo_rate_(0), hot_rate_(0) {}
  ~LocalCacheRetentionRate() { reset(); }
  void reset() { version_ = CLUSTER_VERSION_4_5_1_0; lru_rate_ = 0; fifo_rate_ = 0; hot_rate_ = 0; }
  bool is_valid() const;
  int calc_macro_cache_retention_rate(const ObSSLocalCacheLSInfo &cache_info,
                                      const int64_t macro_cache_free_size,
                                      const int64_t cur_fifo_size,
                                      const int64_t hot_tablet_macro_cache_free_size,
                                      const uint64_t min_cluster_version);
  int calc_macro_cache_retention_rate_v441(const ObSSLocalCacheLSInfo &cache_info,
                                           const int64_t macro_cache_free_size,
                                           const int64_t cur_fifo_size);
  int calc_macro_cache_retention_rate_v451(const ObSSLocalCacheLSInfo &cache_info,
                                           const int64_t macro_cache_free_size,
                                           const int64_t hot_tablet_macro_cache_free_size);
  TO_STRING_KV(K_(version), K(lru_rate_), K(fifo_rate_), K_(hot_rate));

public:
  int64_t version_;
  int64_t lru_rate_;  // auto lru rate
  int64_t fifo_rate_; // auto fifo rate
  int64_t hot_rate_;  // hot lru/fifo rate
};

struct LocalCacheRetentionRateGroup final
{
public:
  LocalCacheRetentionRateGroup() : macro_cache_retention_rate_() {}
  ~LocalCacheRetentionRateGroup() { reset(); }
  void reset() { macro_cache_retention_rate_.reset(); }
  TO_STRING_KV(K(macro_cache_retention_rate_));

public:
  LocalCacheRetentionRate macro_cache_retention_rate_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_RPC_STRUCT_H_ */
