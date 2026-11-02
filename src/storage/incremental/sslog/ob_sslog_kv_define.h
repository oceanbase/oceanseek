/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_KV_DEFINE
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_KV_DEFINE

#include "src/storage/incremental/sslog/ob_sslog_define.h"
#include "src/storage/tx/ob_committer_define.h"
#include "src/storage/tx/ob_trans_define.h"
#include "lib/string/ob_string_buffer.h"
#include <cstdint>

namespace oceanbase
{
namespace sslog
{
/***********************
 * SSLog KV Row Format
 ***********************/

class ObSSLogKVRowPhysicalBuf
{
public:
  // ObSSLogKVRowPhysicalBuf() : buf_(nullptr), key_data_pos_(0), val_data_pos_(0), buf_len_(0) {}

  ObSSLogKVRowPhysicalBuf() : allocator_("SSLogKVTempBuf"), key_buf_(&allocator_), val_buf_(&allocator_) {}

  const static int64_t MAX_KEY_BUF_LEN = 512 * 1024L;
  const static int64_t MAX_VAL_BUF_LEN = 2 * 1024 * 1024L;

  // int init();
  int init(const int64_t key_size = MAX_KEY_BUF_LEN, const int64_t val_size = MAX_VAL_BUF_LEN);
  bool is_inited() const { return key_buf_.capacity() > 0 || val_buf_.capacity() > 0; }

  int64_t get_key_buf_pos() const { return key_buf_.length(); }
  int64_t get_val_buf_pos() const { return val_buf_.length(); }
  void set_key_buf_pos(int64_t pos) { key_buf_.set_length(pos); }
  void set_val_buf_pos(int64_t pos) { val_buf_.set_length(pos); }
  char *get_key_buf() { return key_buf_.ptr(); }
  char *get_val_buf() { return val_buf_.ptr(); }

  const char *get_key_buf() const { return key_buf_.ptr(); }
  const char *get_val_buf() const { return val_buf_.ptr(); }

  void reset();
  void reuse();

  ~ObSSLogKVRowPhysicalBuf() { reset(); }

  TO_STRING_KV(K(key_buf_), K(val_buf_));

private:
  
  ModulePageAllocator allocator_;
  ObStringBuffer key_buf_;
  ObStringBuffer val_buf_;
};

class ObSSLogKVTempCache
{
public:
  ObSSLogKVTempCache(const uint64_t tenant_id)
      : allocator_("KVTempCache", tenant_id), key_buf_cache_(),        cur_key_buf_(&allocator_), cur_val_buf_(&allocator_)
  {
    key_buf_cache_.set_attr(allocator_.get_attr());
    val_buf_cache_.set_attr(allocator_.get_attr());
  }

  void reset();

  ~ObSSLogKVTempCache() { reset(); }

  common::ObStringBuffer &get_cur_key_buf() { return cur_key_buf_; }
  common::ObStringBuffer &get_cur_val_buf() { return cur_val_buf_; }

  int store_into_cache();

  void reuse_cur_buf(){
    cur_val_buf_.reuse();
    cur_key_buf_.reuse();
  }

  int key_at(int i, ObString &key_buf) { return key_buf_cache_.at(i, key_buf); }

  int val_at(int i, ObString &val_buf) { return val_buf_cache_.at(i, val_buf); }

  int64_t count() { return key_buf_cache_.count(); }

  TO_STRING_KV(K(key_buf_cache_.count()),
               K(val_buf_cache_.count()),
               K(cur_key_buf_),
               K(cur_val_buf_));

private: 
  ModulePageAllocator allocator_;
    ObSEArray<ObString, 3> key_buf_cache_;
  ObSEArray<ObString, 3> val_buf_cache_;
  common::ObStringBuffer cur_key_buf_;
  common::ObStringBuffer cur_val_buf_;
};

class ObSSLogKVRowUserKey
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  ObSSLogKVRowUserKey()
      : cluster_id_(0), region_id_(0), meta_type_(ObSSLogMetaType::SSLOG_INVALID_META), meta_key_()
  {}

  bool is_valid() const
  {
    return cluster_id_ > 0 && is_valid_sslog_meta_type(meta_type_)
           && !meta_key_.empty();
  }

  int get_practical_end_pos(int64_t &end_pos) const;
  int get_common_key_end_pos(int64_t & end_pos) const;

  bool equal_to(const ObSSLogKVRowUserKey &other) const
  {
    return cluster_id_ == other.cluster_id_ && region_id_ == other.region_id_
           && meta_type_ == other.meta_type_ && meta_key_.compare_equal(other.meta_key_);
  }

  void reset()
  {
    cluster_id_ = 0;
    region_id_ = 0;
    meta_type_ = ObSSLogMetaType::SSLOG_INVALID_META;
    meta_key_.reset();
  }

  TO_STRING_KV(K(cluster_id_), K(region_id_), K(meta_type_), K(meta_key_));

public:
  uint64_t cluster_id_;
  uint64_t region_id_;
  ObSSLogMetaType meta_type_;
  ObString meta_key_;
};

class ObSSLogKVRowInnerKey
{
  OB_UNIS_VERSION(1);

public:
  enum class KeyType : int64_t
  {
    UNKNOWN = 0,
    DATA_ROW = 1,
    TX_CTX_ROW = 2,
  };

  ObSSLogKVRowInnerKey() : key_version_(share::SCN::invalid_scn()), type_(KeyType::UNKNOWN) {}

  bool is_valid(bool is_next_key = false) const
  {
    return (is_next_key || key_version_.is_valid()) && type_ != KeyType::UNKNOWN;
  }

  void set_as_new_row()
  {
    key_version_ = share::SCN::min_scn();
    type_ = KeyType::DATA_ROW;
  }

  void set_as_first_version() { key_version_ = share::SCN::min_scn(); }
  void set_as_last_version() { key_version_.set_invalid(); }
  bool is_first_version() const { return share::SCN::min_scn() == key_version_; }
  bool is_last_version() const { return !key_version_.is_valid(); }

  bool operator==(const ObSSLogKVRowInnerKey &inner_key) const
  {
    return key_version_ == inner_key.key_version_ && type_ == inner_key.type_;
  }

  void reset()
  {
    key_version_.set_invalid();
    type_ = KeyType::UNKNOWN;
  }

  TO_STRING_KV(K(key_version_), K(type_));

public:
  share::SCN key_version_;
  KeyType type_;
};

class ObSSLogKVRowPhysicalKey
{
public:
  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  ObSSLogKVRowPhysicalKey() : user_key_(), inner_key_() {}

  bool is_valid() const { return user_key_.is_valid() && inner_key_.is_valid(); }

  bool is_first_version() { return inner_key_.is_first_version(); }

  int get_user_key_end_pos(int64_t &end_pos) const;
  int get_common_key_end_pos(int64_t & end_pos) const;

  void reset()
  {
    user_key_.reset();
    inner_key_.reset();
  }

  TO_STRING_KV(K(user_key_), K(inner_key_));

public:
  ObSSLogKVRowUserKey user_key_;
  ObSSLogKVRowInnerKey inner_key_;
};

class ObSSLogKVRowUserValue
{
  OB_UNIS_VERSION(1);

public:
  ObSSLogKVRowUserValue() : meta_val_(), extra_info_() {}  
  bool is_valid() const { return !meta_val_.empty() && !extra_info_.empty(); }

  void reset()
  {
    meta_val_.reset();
    extra_info_.reset();
  }

  TO_STRING_KV(K(meta_val_), K(extra_info_));

public:
  ObString meta_val_;
  ObString extra_info_;
};

class ObSSLogKVRowInnerValue
{
  OB_UNIS_VERSION(1);

public:
  union ValFlag
  {
    uint64_t flag_val_;
    struct FLagBit
    {
      unsigned int is_deleted_ : 1;

      TO_STRING_KV(K(is_deleted_))
      void reset() { is_deleted_ = 0; }
      FLagBit() { reset(); }
    } flag_bit_;

    ValFlag()
    {
      flag_val_ = 0;
      flag_bit_.reset();
    }
  };

  ObSSLogKVRowInnerValue()
      : row_version_(share::SCN::invalid_scn()), tx_version_(share::SCN::invalid_scn()),
        tx_state_(transaction::ObTxState::UNKNOWN), tx_id_(), flag_()
  {}

  void set_row_auto_commit(const share::SCN row_version)
  {
    row_version_ = row_version;
    tx_version_ = row_version_;
    tx_state_ = transaction::ObTxState::COMMIT;
    tx_id_ = transaction::ObTransID(0);
  }

  void set_deleted() { flag_.flag_bit_.is_deleted_ = true; }
  bool is_deleted() const { return flag_.flag_bit_.is_deleted_; }

  bool is_valid() const
  {
    return row_version_.is_valid_and_not_min() && tx_state_ != transaction::ObTxState::UNKNOWN;
  }

  void reset()
  {
    row_version_.set_invalid();
    tx_version_.set_invalid();
    tx_state_ = transaction::ObTxState::UNKNOWN;
    tx_id_.reset();
    flag_.flag_bit_.reset();
  }

  TO_STRING_KV(K(row_version_), K(tx_version_), K(tx_state_), K(tx_id_), K(flag_.flag_bit_));

public:
  share::SCN row_version_;
  share::SCN tx_version_;
  transaction::ObTxState tx_state_;
  transaction::ObTransID tx_id_;
  ValFlag flag_;
};

class ObSSLogKVRowPhysicalValue
{
  OB_UNIS_VERSION(1);

public:
  ObSSLogKVRowPhysicalValue() : user_val_(), inner_val_(), next_key_() {}

  bool is_valid() const
  {
    return user_val_.is_valid() && inner_val_.is_valid() && next_key_.is_valid(true);
  }
  void reset()
  {
    user_val_.reset();
    inner_val_.reset();
    next_key_.reset();
  }

  TO_STRING_KV(K(user_val_), K(inner_val_), K(next_key_));

public:
  ObSSLogKVRowUserValue user_val_;
  ObSSLogKVRowInnerValue inner_val_;
  ObSSLogKVRowInnerKey next_key_;
};

/***********************
 * SSLogTable KV Adpater
 ***********************/
class ObPalfKVAdpaterInterface
{
public:
  virtual void reset() = 0;
  virtual int init(const uint64_t cluster_id, const uint64_t tenant_id) = 0;
  virtual int get(const common::ObString &key, common::ObString &value) = 0;
  virtual int put(const common::ObString &key, const common::ObString &value) = 0;
  virtual int set(const common::ObString &key, const common::ObString &value) = 0;
  virtual int cas(const common::ObString &key,
                  const common::ObString &old_value,
                  const common::ObString &new_value,
                  bool &expected) = 0;
  virtual int watch_cas(const common::ObString &key,
                        const common::ObString &watch_key,
                        const common::ObString &old_value,
                        const common::ObString &new_value,
                        bool &expected) = 0;
  virtual int delete_kv(const common::ObString &key) = 0;
  virtual int
  list(const common::ObString &prefix, const int32_t limit, ObSSLogKVTempCache &kv_pairs) = 0;

  virtual int get_gts(share::SCN &) = 0;
  virtual int get_limit_id(share::SCN &) = 0;

  virtual void inc_unused_kv_cnt() = 0;
  virtual int64_t get_unused_kv_cnt() = 0;
  virtual void clear_unused_kv_cnt() = 0;
  virtual void set_max_gc_version(const share::SCN &gc_version) = 0;
  virtual share::SCN get_max_gc_version() const = 0;
  virtual void set_min_gc_succ_version(const share::SCN &gc_succ_version) = 0;
  virtual share::SCN get_min_gc_succ_version() const = 0;
};

class ObPrefixFirstVersionBaseIterator
{
public:
  ObPrefixFirstVersionBaseIterator(const uint64_t tenant_id)
      : allocator_("FirstVerIter", tenant_id), common_prefix_()
  {}
  int get_next(ObString &user_key_buf, ObString &key_buf, ObString &val_buf);

  ~ObPrefixFirstVersionBaseIterator()
  {
    reset();
  }

  virtual void reset()
  {
    if (common_prefix_.ptr() != nullptr) {
      allocator_.free(common_prefix_.ptr());
    }
    common_prefix_.reset();
    allocator_.reset();
  }

private:
  int construct_common_prefix_();

  // unordered
  // OB_ITER_END
  virtual int inner_get_next_kv_(ObString &key_buf, ObString &val_buf) = 0;

protected:
  ModulePageAllocator allocator_;
  ObString common_prefix_;
};

class ObSSLogKVVersionKeyPair : public common::ObDLinkBase<ObSSLogKVVersionKeyPair>
{
public:
  enum class VersionSource
  {
    UNKNOWN = 0,
    UNUSED = 1,
    MVCC = 2,
    OLD = 3,
  };

  static const char *get_source_str(const VersionSource &src)
  {
    const char *tmp_res = "INVALID";
    switch (src) {
    case VersionSource::UNKNOWN: {
      tmp_res = "UNKNOWN";
      break;
    }
    case VersionSource::UNUSED: {
      tmp_res = "UNUSED";
      break;
    }
    case VersionSource::MVCC: {
      tmp_res = "MVCC";
      break;
    }
    case VersionSource::OLD: {
      tmp_res = "OLD";
      break;
    }
    default: {
      break;
    }
    }

    return tmp_res;
  }

public:
  ObSSLogKVVersionKeyPair()
      : cur_key_(), commit_version_(), next_key_(), src_(VersionSource::UNKNOWN)
  {}

  TO_STRING_KV(K(cur_key_), K(commit_version_), K(next_key_), "src_", get_source_str(src_));

public:
  ObSSLogKVRowInnerKey cur_key_;
  share::SCN commit_version_;
  ObSSLogKVRowInnerKey next_key_;
  VersionSource src_;
};

typedef common::ObDList<ObSSLogKVVersionKeyPair> SSLogKVVersionPairList;

class ObUserKeyAllVersionBaseIterator
{
public:
  // static const int64_t VERSION_KV_BUF_CAPACITY = 32;
public:
  ObUserKeyAllVersionBaseIterator(const uint64_t tenant_id)
      : delay_unused_kv_gc_time_(5 * 60 * 1000 * 1000L),allocator_("KvVersionIter", tenant_id), user_key_buf_(), construct_finished_(false),
        version_kv_list_(), unused_kv_list_(), min_version_scn_(share::SCN::max_scn()),
        min_need_gc_scn_(share::SCN::max_scn()), deleted_kv_cnt_(0), interface_(nullptr),
        physical_key_buffer_(&allocator_)
  {}

  ~ObUserKeyAllVersionBaseIterator() { reset(); }

  virtual void reset()
  {
    reset_version_kv_();
    user_key_buf_.reset();
    deleted_kv_cnt_ = 0;
    interface_ = nullptr;
    physical_key_buffer_.reset();
    min_version_scn_.set_max();
    min_need_gc_scn_.set_max();
    allocator_.reset();
  }

  void set_base_param(const ObString &user_key_buf, ObPalfKVAdpaterInterface *interface)
  {
    user_key_buf_ = user_key_buf;
    interface_ = interface;
  }

  int gc_unused_version();
  int gc_old_version(const share::SCN &max_gc_scn);
  int construct_version_list();

  int64_t get_version_kv_size() { return version_kv_list_.get_size(); }
  int64_t get_unused_kv_size() { return unused_kv_list_.get_size(); }
  share::SCN get_min_version_scn() { return min_version_scn_; }
  share::SCN get_min_need_gc_scn() {
    return min_need_gc_scn_;
  }
  uint64_t get_deleted_kv_cnt() { return deleted_kv_cnt_; }
  TO_STRING_KV(K(user_key_buf_),
               K(construct_finished_),
               K(version_kv_list_.get_size()),
               K(unused_kv_list_.get_size()),
               K(min_version_scn_),
               K(delay_unused_kv_gc_time_));

private:
  void reset_version_kv_();
  // unordered
  virtual int inner_get_next_kv_(ObString &key_buf, ObString &val_buf) = 0;

  uint64_t delay_unused_kv_gc_time_;
protected:
  ModulePageAllocator allocator_;
  ObString user_key_buf_;

  bool construct_finished_;

  SSLogKVVersionPairList version_kv_list_;
  SSLogKVVersionPairList unused_kv_list_;
  share::SCN min_version_scn_;
  share::SCN min_need_gc_scn_;
  uint64_t deleted_kv_cnt_;

  ObPalfKVAdpaterInterface *interface_;
  ObStringBuffer physical_key_buffer_;
};

class ObPhysicalRowInnerTool
{
public:
  static int generate_base_new_version(const ObSSLogKVRowPhysicalKey &old_key,
                                       const ObSSLogKVRowPhysicalValue &old_value,
                                       ObSSLogKVRowPhysicalKey &new_key,
                                       ObSSLogKVRowPhysicalValue &new_value);
  static int insert_new_version(ObSSLogKVRowPhysicalKey &new_key,
                                ObSSLogKVRowPhysicalValue &new_value,
                                ObSSLogKVRowPhysicalBuf &new_buf,
                                ObPalfKVAdpaterInterface *interface);
  static int link_next_version(const ObSSLogKVRowPhysicalKey &old_key,
                               const ObSSLogKVRowPhysicalValue &old_value,
                               const ObSSLogKVRowPhysicalBuf &old_buf,
                               const ObSSLogKVRowPhysicalBuf &new_version_buf,
                               ObSSLogKVRowPhysicalBuf &old_version_buf,
                               ObPalfKVAdpaterInterface *interface);
  static int gc_data_version_kv(ObPalfKVAdpaterInterface *interface,
                                ObPrefixFirstVersionBaseIterator *first_version_iter,
                                ObUserKeyAllVersionBaseIterator *all_version_iter,
                                const share::SCN &max_gc_scn);
};

/***********************
 * Physical Row Functor
 ***********************/

class ObPhysicalRowUpdateFunctor
{
public:
  ObPhysicalRowUpdateFunctor(ObPalfKVAdpaterInterface *interface,
                             const ObSSLogWriteParam &param,
                             const ObString &meta_value,
                             const ObString &extra_info)
      : interface_(interface), param_(param), meta_value_(meta_value), extra_info_(extra_info)
  {}
  TO_STRING_KV("FuncName", "Physical KV Update", K(param_), K(meta_value_), K(extra_info_));

  int operator()(const ObSSLogKVRowPhysicalKey &old_key,
                 const ObSSLogKVRowPhysicalValue &old_value,
                 const ObSSLogKVRowPhysicalBuf &old_buf);

private:
  ObPalfKVAdpaterInterface *interface_;
  const ObSSLogWriteParam &param_;
  const ObString &meta_value_;
  const ObString &extra_info_;
};

class ObPhysicalRowDeleteFunctor
{
public:
  TO_STRING_KV("FuncName", "Physical KV Delete");

  ObPhysicalRowDeleteFunctor(ObPalfKVAdpaterInterface *interface)
      : interface_(interface)
  {}

  int operator()(const ObSSLogKVRowPhysicalKey &old_key,
                 const ObSSLogKVRowPhysicalValue &old_value,
                 const ObSSLogKVRowPhysicalBuf &old_buf);

private:
  ObPalfKVAdpaterInterface *interface_;
};

class ObSSLogKVResultRow
{
public:
  struct VersionVal
  {
  public:
    ObSSLogKVRowUserValue user_val_;
    share::SCN val_version_;
    ObStringBuffer val_buf_;
    share::SCN key_suffix_;
    share::SCN next_key_suffix_;

    TO_STRING_KV(K(user_val_),
                 K(val_version_),
                 K(val_buf_.length()),
                 K(key_suffix_),
                 K(next_key_suffix_));

    void reset()
    {
      user_val_.reset();
      val_version_.reset();
      val_buf_.reset();
      key_suffix_.reset();
      next_key_suffix_.reset();
    }

    VersionVal() { reset(); }

    int init(const ObSSLogKVRowUserValue &user_val,
             const share::SCN val_version,
             const share::SCN key_suffix,
             const share::SCN next_key_suffix,
             common::ObIAllocator *allocator);

    void get_row_scn(share::SCN &row_scn) const { row_scn = val_version_; }
    void get_meta_value(ObString &meta_val) const { meta_val = user_val_.meta_val_; }
    void get_extra_info(ObString &extra_info) const { extra_info = user_val_.extra_info_; }
  };

  ObSSLogKVResultRow() { reset(); }

  void reset()
  {
    reset_val_();
    user_key_.reset();
    key_buf_.reset();
    allocator_ = nullptr;
  }

  ~ObSSLogKVResultRow() { reset(); }

  int init(const ObSSLogKVRowUserKey user_key, ObIAllocator *allocator);

  int copy_and_insert(const ObSSLogKVRowPhysicalKey &phy_key,
                      const ObSSLogKVRowPhysicalValue &phy_val);

  // Deep copy a version value from another row
  int deep_copy_version(const VersionVal *src_version, ObIAllocator *allocator);

  // void print_version_val(const share::SCN start_snapshot, const share::SCN end_snapshot);

  int64_t get_version_count() const { return val_.count(); }
  const VersionVal *at(int64_t index) const { return val_[index]; }
  const ObSSLogKVRowUserKey *get_user_key() const { return &user_key_; }

  bool is_same_key(const ObSSLogKVRowUserKey &other) const { return user_key_.equal_to(other); }

  void get_meta_type(ObSSLogMetaType &meta_type) const { meta_type = user_key_.meta_type_; }
  void get_meta_key(ObString &meta_key) const { meta_key = user_key_.meta_key_; }

  void print_version_val() const;

private:
  void reset_val_()
  {
    for (int i = 0; i < val_.count(); i++) {
      if (OB_NOT_NULL(val_[i])) {
        val_[i]->~VersionVal();
        allocator_->free(val_[i]);
        val_[i] = nullptr;
      }
    }
    val_.reset();
  }

private:
  ObSSLogKVRowUserKey user_key_;
  ObSEArray<VersionVal *, 2> val_;
  ObStringBuffer key_buf_;
  common::ObIAllocator *allocator_;

public:
  TO_STRING_KV(K(user_key_), K(val_.count()), K(key_buf_.length()));

public:
  struct ObSSLogRowValueComp
  {
    bool operator()(ObSSLogKVResultRow *&a, ObSSLogKVResultRow *&b) const
    {
      bool is_less = true;

      if (OB_ISNULL(a)) {
        is_less = true;
      } else if (OB_ISNULL(b)) {
        is_less = false;
      } else {
        is_less = a->user_key_.meta_key_ < b->user_key_.meta_key_;
      }

      return is_less;
    }
  };
};

typedef ObSEArray<ObSSLogKVResultRow*, 1> ObSSLogKVResultRowArray;

/***********************
 * SSLogTable Logical Row
 ***********************/
class ObSSLogTableLogicalRow
{
public:
  ObSSLogTableLogicalRow()
      : is_prefix_key_(false), physical_buf_(), physical_key_(), physical_val_(),
        interface_(nullptr)
  {}

  // seri and deseri key
  int init(ObPalfKVAdpaterInterface *interface,
           const ObSSLogMetaType &type,
           const ObString &key,
           bool is_prefix_read);
  int init_for_read(ObPalfKVAdpaterInterface *interface,
                    const ObSSLogMetaType &type,
                    const ObString &key);
  int init(ObPalfKVAdpaterInterface *interface,
           bool is_prefix_read,
           const int64_t key_size,
           const int64_t val_size);

  bool is_inited() const;

  void reuse()
  {
    is_prefix_key_ = false;
    physical_buf_.reuse();
    physical_key_.reset();
    physical_val_.reset();
  }
  void reset()
  {
    is_prefix_key_ = false;
    physical_buf_.reset();
    physical_key_.reset();
    physical_val_.reset();
  }

  int insert_new_row(const ObString meta_value, const ObString &extra_info);

  int update_row(const ObSSLogWriteParam &param,
                 const ObString &meta_value,
                 const ObString &extra_info);

  int delete_row();

  int read_row(const share::SCN start_snapshot,
               const share::SCN end_snapshot,
               ObSSLogKVResultRowArray &result_row_arr,
               common::ObIAllocator *allocator,
               bool is_prefix_read,
               bool include_oldest_version);

  const ObSSLogKVRowUserKey &get_user_key() const { return physical_key_.user_key_; }

  void get_row_scn(share::SCN &row_scn) { row_scn = physical_val_.inner_val_.tx_version_; }
  void get_meta_type(ObSSLogMetaType &meta_type) { meta_type = physical_key_.user_key_.meta_type_; }
  void get_meta_key(ObString &meta_key) { meta_key = physical_key_.user_key_.meta_key_; }
  void get_meta_value(ObString &meta_val) { meta_val = physical_val_.user_val_.meta_val_; }
  void get_extra_info(ObString &extra_info) { extra_info = physical_val_.user_val_.extra_info_; }
  bool is_deleted() { return physical_val_.inner_val_.is_deleted(); }

  TO_STRING_KV(K(is_prefix_key_), K(physical_buf_), K(physical_key_), K(physical_val_));

public:
  struct ObPhyVersionNode : public ObDLinkBase<ObPhyVersionNode>
  {
  public:
    ObSSLogKVRowPhysicalKey phy_key_;
    ObSSLogKVRowPhysicalValue phy_val_;
    ObPhyVersionNode *next_;

    ObPhyVersionNode(const ObSSLogKVRowPhysicalKey &key, const ObSSLogKVRowPhysicalValue &val)
        : phy_key_(key), phy_val_(val), next_(nullptr)
    {}

    ObPhyVersionNode() : phy_key_(), phy_val_(), next_(nullptr) {}

    void add_next_node(ObPhyVersionNode *node)
    {
      node->next_ = next_;
      next_ = node;
    }

    void remove_next_ndoe(ObPhyVersionNode *&node)
    {
      node = next_;
      next_ = node->next_;
    }

    TO_STRING_KV(K(phy_key_), K(phy_val_));
  };

  typedef ObSEArray<ObPhyVersionNode *, 1> ObPhyVersionArray;

private:
  template <typename Functor>
  int iter_row_version_(Functor &fn);

  int construct_version_by_list_(const share::SCN start_snapshot,
                                 const share::SCN end_snapshot,
                                 ObSSLogKVResultRowArray &result_array,
                                 common::ObIAllocator *allocator,
                                 const bool is_prefix_read,
                                 const bool include_oldest_version);

  static int find_result_row_(const ObSSLogKVRowUserKey &user_key,
                              const ObSSLogKVRowPhysicalKey &phy_key,
                              const int64_t max_resut_row_index,
                              common::ObIAllocator *allocator,
                              ObPhyVersionArray &phy_version_array,
                              ObSSLogKVResultRowArray &result_array,
                              int64_t &result_row_index);
  static int link_next_version_(ObPhyVersionArray &phy_version_array,
                                const int64_t result_row_index,
                                ObPhyVersionNode *node);

  static int filter_result_val_(const ObPhyVersionArray &phy_version_array,
                                ObSSLogKVResultRowArray &result_array,
                                const share::SCN &start_snapshot,
                                const share::SCN &end_snapshot,
                                const bool is_prefix_read,
                                const bool include_oldest_version);

  static int assign_result_arr_(const ObPhyVersionArray &phy_version_array,
                               ObSSLogKVResultRowArray &result_array);

  int read_version_(ObSSLogKVResultRow &result_row);

private:
  bool is_prefix_key_;
  ObSSLogKVRowPhysicalBuf physical_buf_;
  ObSSLogKVRowPhysicalKey physical_key_;
  ObSSLogKVRowPhysicalValue physical_val_;
  ObPalfKVAdpaterInterface *interface_;
};

template <typename Functor>
int ObSSLogTableLogicalRow::iter_row_version_(Functor &fn)
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the logical row is not inited", K(ret));
  } else if (!physical_key_.user_key_.is_valid() || nullptr == interface_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid physical_key_", K(ret), K(physical_key_.user_key_), KP(interface_));
  } else {
    physical_key_.inner_key_.set_as_first_version();
    physical_key_.inner_key_.type_ = ObSSLogKVRowInnerKey::KeyType::DATA_ROW;
    do {
      ObString tmp_key, tmp_val;
      int64_t tmp_pos = 0;
      if (OB_FAIL(physical_key_.serialize(physical_buf_.get_key_buf(),
                                          ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN, tmp_pos))) {
        STORAGE_LOG(WARN, "serialize physical key failed", K(ret), K(tmp_pos), KPC(this));
      } else {
        physical_buf_.set_key_buf_pos(tmp_pos);
        physical_key_.reset();
        tmp_pos = 0;
        if (OB_FAIL(physical_key_.deserialize(physical_buf_.get_key_buf(),
                                              physical_buf_.get_key_buf_pos(), tmp_pos))) {
          STORAGE_LOG(WARN, "deserialize physical_key_ failed", K(ret), KPC(this));
        }
      }

      if (OB_SUCC(ret)) {
        ObString key, val;
        key.assign_ptr(physical_buf_.get_key_buf(), physical_buf_.get_key_buf_pos());
        val.assign_ptr(physical_buf_.get_val_buf(), ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN);
        tmp_pos = 0;
        if (OB_FAIL(interface_->get(key, val))) {
          STORAGE_LOG(WARN, "get target value failed", K(ret), KPC(this));
        } else if (OB_FAIL(physical_val_.deserialize(physical_buf_.get_val_buf(),
                                                     ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN,
                                                     tmp_pos))) {
          STORAGE_LOG(WARN, "deserialize physical_val_ failed", K(ret), KPC(this));
        } else if (OB_FALSE_IT(physical_buf_.set_val_buf_pos(tmp_pos))) {
        } else if (OB_FAIL(fn(physical_key_, physical_val_, physical_buf_))) {
          STORAGE_LOG(WARN, "Physical KV functor execute failed", K(ret), K(fn), KPC(this));
        }
      }

      physical_key_.inner_key_ = physical_val_.next_key_;
    } while (OB_SUCC(ret) && physical_key_.inner_key_.is_valid());
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "[SSLogKV] iter row version successfully, print the last version", K(ret),
                K(fn), KPC(this));
  }

  return ret;
}

/***********************
 * SSLogTable Result Row
 ***********************/
class ObSSLogKVResultSet
{
public:
  ObSSLogKVResultSet() { reset(); }
  ~ObSSLogKVResultSet() { reset(); }

  int init(ObPalfKVAdpaterInterface *interface,
           const ObSSLogMetaType &type,
           const ObString &meta_key,
           const share::SCN start_snapshot,
           const share::SCN end_snapshot,
           ObIAllocator *allocator,
           bool read_prefix,
           bool include_oldest_version = false);

  bool is_inited()
  {
    // For batch read, logic_row_ may not be initialized
    return (logic_row_.is_inited() || (start_snapshot_.is_valid() || end_snapshot_.is_valid()))
           && OB_NOT_NULL(allocator_);
  }
  share::SCN get_start_snapshot() const { return start_snapshot_; }
  share::SCN get_end_snapshot() const { return end_snapshot_; }

  TO_STRING_KV("user_key_",
               logic_row_.get_user_key(),
               K(start_snapshot_),
               K(end_snapshot_),
               K(result_rows_.count()),
               K(is_prefix_read_),
               K(include_oldest_version_before_start_),
               K(row_handle_index_),
               K(version_handle_index_),
               K(logic_row_),
               KP(allocator_));

  static bool snapshot_range_is_valid(const share::SCN start_snapshot,
                                      const share::SCN end_snapshot)
  {
    return start_snapshot.is_valid_and_not_min() || end_snapshot.is_valid_and_not_min();
  }

  static bool contain_user_key(const ObSSLogKVRowUserKey &user_key,
                               const ObSSLogKVResultRowArray &result_rows,
                               int64_t &index);

  void reset_index()
  {
    row_handle_index_ = 0;
    version_handle_index_ = INT64_MAX;
  }

  void reset()
  {
    start_snapshot_.reset();
    end_snapshot_.reset();
    for (int i = 0; i < result_rows_.count(); i++) {
      if (OB_NOT_NULL(result_rows_[i])) {
        result_rows_[i]->~ObSSLogKVResultRow();
        allocator_->free(result_rows_[i]);
        result_rows_[i] = nullptr;
      }
    }
    result_rows_.reset();
    is_prefix_read_ = false;
    include_oldest_version_before_start_ = false;
    reset_index();
    logic_row_.reset();
    allocator_ = nullptr;
  }

  bool is_empty() const { return result_rows_.count() <= 0; }

  int get_next(const ObSSLogKVRowUserKey *&user_key,
               const ObSSLogKVResultRow::VersionVal *&version_val);

  // Batch read support: initialize for batch read without logic_row
  int init_for_batch_read(const share::SCN start_snapshot,
                          const share::SCN end_snapshot,
                          ObIAllocator *allocator);

  // Batch read support: merge results from another result array
  int merge_result_rows(const ObSSLogKVResultRowArray &result_rows);

private:
  int get_next_version_(const ObSSLogKVRowUserKey *&user_key,
                        const ObSSLogKVResultRow::VersionVal *&version_val);

  int get_next_row_latest_version_(const ObSSLogKVRowUserKey *&user_key,
                                   const ObSSLogKVResultRow::VersionVal *&latest_version_val);

  void print_version_list_() const;
private:
  share::SCN start_snapshot_;
  share::SCN end_snapshot_;
  ObSSLogKVResultRowArray result_rows_;
  bool is_prefix_read_;
  bool include_oldest_version_before_start_;

  int64_t row_handle_index_;
  int64_t version_handle_index_;

  ObSSLogTableLogicalRow logic_row_;
  ObIAllocator *allocator_;
};

} // namespace sslog

} // namespace oceanbase

#endif
