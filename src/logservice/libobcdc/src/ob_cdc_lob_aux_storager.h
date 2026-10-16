/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_STORAGER_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_STORAGER_H_

#include "common/object/ob_object.h"        // ObLobId
#include "storage/lob/common/ob_lob_seq.h" // ObLobSeqId
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/allocator/ob_concurrent_fifo_allocator.h"    // ObConcurrentFIFOAllocator
#include "storage/tx/ob_tx_log.h"    // ObTransID
#include "ob_log_utils.h"            // _G_, _M_
#include "ob_cdc_lob_ctx.h"
#include "logservice/libobcdc/src/ob_log_resource_recycle_task.h"

namespace oceanbase
{
namespace libobcdc
{
struct ObLobAuxKey final
{
  ObLobAuxKey(
      const int64_t commit_version,
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t lob_aux_table_id,
      const common::ObLobId &lob_id,
      const transaction::ObTxSEQ &seq_no,
      const common::ObString &seq_id) :
    commit_version_(commit_version),
    tenant_id_(tenant_id),
    trans_id_(trans_id),
    lob_aux_table_id_(lob_aux_table_id),
    lob_id_(lob_id),
    lob_operation_seq_no_(seq_no),
    seq_id_(seq_id)
  {
  }

  ~ObLobAuxKey() {}

  bool is_valid() const
  {
    return (OB_INVALID_VERSION != commit_version_)
      && (common::OB_INVALID_TENANT_ID != tenant_id_)
      && trans_id_.is_valid()
      && (common::OB_INVALID_ID != lob_aux_table_id_)
      && lob_id_.is_valid()
      && lob_operation_seq_no_.is_valid()
      && (seq_id_.empty() || storage::ObLobSeqId(seq_id_).is_valid());
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&commit_version_, sizeof(commit_version_), hash_val);
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    uint64_t trans_id_hash = trans_id_.hash();
    hash_val = common::murmurhash(&trans_id_hash, sizeof(trans_id_hash), hash_val);
    hash_val = common::murmurhash(&lob_aux_table_id_, sizeof(lob_aux_table_id_), hash_val);
    uint64_t lob_id_hash = lob_id_.hash();
    hash_val = common::murmurhash(&lob_id_hash, sizeof(lob_id_hash), hash_val);
    uint64_t seq_no_hash = lob_operation_seq_no_.hash();
    hash_val = common::murmurhash(&seq_no_hash, sizeof(seq_no_hash), hash_val);
    hash_val = common::murmurhash(seq_id_.ptr(), seq_id_.length(), hash_val);

    return hash_val;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  int compare(const ObLobAuxKey &other) const;
  bool operator>(const ObLobAuxKey &other) const { return (1 == compare(other)); }
  bool operator<(const ObLobAuxKey &other) const { return (-1 == compare(other)); }
  bool operator==(const ObLobAuxKey &other) const
  {
    return (commit_version_ == other.commit_version_)
      && (tenant_id_ == other.tenant_id_)
      && (trans_id_ == other.trans_id_)
      && (lob_aux_table_id_ == other.lob_aux_table_id_)
      && (lob_id_ == other.lob_id_)
      && (lob_operation_seq_no_ == other.lob_operation_seq_no_)
      && (seq_id_ == other.seq_id_);
  }

  int get_key(std::string &key) const;

  TO_STRING_KV(K_(commit_version), K_(tenant_id), K_(trans_id), K_(lob_aux_table_id),
      K_(lob_id), K_(lob_operation_seq_no), "seq_id", storage::ObLobSeqId(seq_id_));

  int64_t commit_version_;
  uint64_t tenant_id_;
  transaction::ObTransID trans_id_;
  uint64_t lob_aux_table_id_;
  common::ObLobId lob_id_;
  transaction::ObTxSEQ lob_operation_seq_no_;
  common::ObString seq_id_;
};

// ObLobAuxValue 记录一条 LOB 辅助数据在内存缓存中的分配信息和数据视图，内存布局是 [lob_data][seq_id]。
// alloc_buf_：allocator 返回的完整内存块首地址，用于删除缓存记录时释放内存。
// lob_data_：完整内存块中的 LOB 数据起始地址，是供读取方使用的数据视图。
// lob_data_len_：lob_data_ 指向的 LOB 数据长度。
// 该结构体只记录上述信息，不负责释放 alloc_buf_，内存由 ObCDCLobAuxStorager 统一管理。
struct ObLobAuxValue final
{
  ObLobAuxValue() :
      alloc_buf_(nullptr),
      lob_data_(nullptr),
      lob_data_len_(0)
  {
  }

  ObLobAuxValue(char *alloc_buf, char *lob_data, const int64_t lob_data_len) :
    alloc_buf_(alloc_buf),
    lob_data_(lob_data),
    lob_data_len_(lob_data_len)
  {
  }

  ~ObLobAuxValue() {}

  TO_STRING_KV(KP_(alloc_buf), KP_(lob_data), K_(lob_data_len));

  char *alloc_buf_;
  char *lob_data_;
  int64_t lob_data_len_;
};

class ObCDCLobAuxDataCleanTask final : public ObLogResourceRecycleTask
{
public:
  ObCDCLobAuxDataCleanTask():
    ObLogResourceRecycleTask(ObLogResourceRecycleTask::LOB_DATA_CLEAN_TASK),
    tenant_id_(OB_INVALID_TENANT_ID),
    commit_version_(OB_INVALID_VERSION),
    is_task_push_(false)
  {}
  ~ObCDCLobAuxDataCleanTask() { reset(); }

  void reset();
  TO_STRING_KV(K_(tenant_id), K_(commit_version), K_(is_task_push));
public:
  uint64_t tenant_id_;
  int64_t commit_version_;
  bool is_task_push_;
};

struct ObCDCLobAuxDataStatInfo final
{
  ObCDCLobAuxDataStatInfo() { reset(); }
  ~ObCDCLobAuxDataStatInfo() { reset(); }
  void reset()
  {
    last_stat_time_ = 0;
    get_req_cnt_ = 0;
    last_get_req_cnt_ = 0;
    mem_get_req_cnt_ = 0;
    last_mem_get_req_cnt_ = 0;
    put_req_cnt_ = 0;
    last_put_req_cnt_ = 0;
    mem_put_req_cnt_ = 0;
    last_mem_put_req_cnt_ = 0;
    get_data_total_size_ = 0;
    mem_get_data_total_size_ = 0;
    put_data_total_size_ = 0;
    mem_put_data_total_size_ = 0;
  }

  double calc_rps(const int64_t delta_time, const int64_t req_cnt, const int64_t last_req_cnt);
  double calc_rate(const int64_t delta_time, const int64_t total_size, const int64_t last_total_size);

  int64_t last_stat_time_;

  int64_t get_req_cnt_;
  int64_t last_get_req_cnt_;
  int64_t mem_get_req_cnt_;
  int64_t last_mem_get_req_cnt_;

  int64_t put_req_cnt_;
  int64_t last_put_req_cnt_;
  int64_t mem_put_req_cnt_;
  int64_t last_mem_put_req_cnt_;

  int64_t get_data_total_size_;
  int64_t last_get_data_total_size_;
  int64_t mem_get_data_total_size_;
  int64_t last_mem_get_data_total_size_;
  int64_t put_data_total_size_;
  int64_t last_put_data_total_size_;
  int64_t mem_put_data_total_size_;
  int64_t last_mem_put_data_total_size_;
};

class IObStoreService;
class ObLogConfig;
class ObCDCLobAuxStorager final
{
public:
  ObCDCLobAuxStorager();
  ~ObCDCLobAuxStorager();
  int init(IObStoreService *store_service);
  void destroy();

  int put(
      const ObLobAuxKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);

  int get(
      ObIAllocator &allocator,
      const ObLobAuxKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);

  int del(
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  int del(const ObLobAuxKey &key);
  int get_clean_task(const uint64_t tenant_id, ObCDCLobAuxDataCleanTask *&task);
  int clean_unused_data(ObCDCLobAuxDataCleanTask *task);
  int get_cf_handle(const uint64_t tenant_id, void *&cf_handle);

  int64_t get_clean_task_interval_us() const { return clean_task_interval_us_; }
  void configure(const ObLogConfig &cfg);
  void print_stat_info();

private:
  int del_lob_image_(
      const int64_t commit_version,
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      ObLobDataGetCtx &lob_data_get_ctx,
      const bool is_new_col,
      volatile bool &stop_flag);

  int memory_put_(
      const ObLobAuxKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);
  int memory_get_(
      const ObLobAuxKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);
  int memory_del_(const ObLobAuxKey &key);
  int memory_del_(const uint64_t tenant_id, const int64_t commit_version);

  int disk_put_(
      const ObLobAuxKey &key,
      const char *key_type,
      const char *lob_data,
      const int64_t lob_data_len);
  int disk_get_(
      ObIAllocator &allocator,
      const ObLobAuxKey &key,
      const char *&lob_data,
      int64_t &lob_data_len);
  int disk_del_(const ObLobAuxKey &key);
  int disk_del_(void* cf_family, const int64_t commit_version);

  int do_clean_unused_data_(ObCDCLobAuxDataCleanTask *task);

  struct ObLobAuxDataPurger final
  {
    ObLobAuxDataPurger(
        ObCDCLobAuxStorager &host,
        const uint64_t tenant_id,
        const int64_t commit_version) :
      host_(host),
      tenant_id_(tenant_id),
      commit_version_(commit_version),
      purge_count_(0)
    {}

    ~ObLobAuxDataPurger() {}

    bool operator()(const ObLobAuxKey &lob_aux_key, ObLobAuxValue &lob_aux_value);

    ObCDCLobAuxStorager &host_;
    uint64_t tenant_id_;
    int64_t commit_version_;
    int64_t purge_count_;
  };

  // For debug
  struct ObLobAuxDataPrinter final
  {
    bool operator()(const ObLobAuxKey &lob_aux_key, ObLobAuxValue &lob_aux_value);
  };

private:
  typedef common::ObLinearHashMap<ObLobAuxKey, ObLobAuxValue> LobAuxMap;
  typedef common::ObConcurrentFIFOAllocator LobAuxAllocator;
  static const int64_t LOB_AUX_ALLOCATOR_PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

  bool is_inited_;
  LobAuxMap lob_aux_map_;
  LobAuxAllocator lob_aux_allocator_;
  IObStoreService *store_service_;
  bool enable_memory_;
  int64_t clean_task_interval_us_;

  ObCDCLobAuxDataStatInfo stat_info_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCLobAuxStorager);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
