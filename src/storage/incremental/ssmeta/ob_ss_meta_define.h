/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_DEFINE_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_DEFINE_H_

#include "share/ob_define.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_define.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"

namespace oceanbase
{
namespace storage
{
const static int64_t SSMETA_ROW_BATCH_CNT = 8;
static constexpr char SSMETA_SCN_MARKER[] = "/scn";
// meta_key 中版本号的位数（scn 后 20 位零填充）
static constexpr int64_t SSMETA_SCN_DIGITS = 20;
static constexpr int64_t SSMETA_PATH_MAX_LEN = 1024;
static constexpr int64_t SSMETA_TABLE_SQL_TIMEOUT_US = 30 * 1000 * 1000L;

// tablet/ls meta_key 的 logical prefix 格式串，统一定义，避免散落各处导致漂移。
// 普通 tablet: tablet/{tablet_id}/{reorgan_scn}/meta
// 普通 tablet(reorgan_scn 非法，仅到 tablet_id 层): tablet/{tablet_id}/
// inner_tablet: ls/{ls_id}/{tablet_name}/meta
// ls meta:      ls/{ls_id}/meta
// __all_shared_meta 中meta_key不再以 '/' 开头，与__all_shared_tiered_meta对齐
static constexpr char SSMETA_TABLET_PREFIX_FMT[] = "tablet/%lu/%lu/meta";
static constexpr char SSMETA_TABLET_ID_PREFIX_FMT[] = "tablet/%lu/";
static constexpr char SSMETA_INNER_TABLET_PREFIX_FMT[] = "ls/%ld/%s/meta";
static constexpr char SSMETA_LS_PREFIX_FMT[] = "ls/%ld/meta";
static constexpr char SS_LS_CKPT_KEY_FMT[] = "LS_CKPT_%ld";
static constexpr int64_t SS_LS_CKPT_KEY_BUF_LEN = 64;

// 检查版本是否推到462及以上，是否为sys租户
// 检查通过则允许ssmeta数据双写、双删、GC等操作
int check_ss_ls_ckpt_data_enabled(const uint64_t tenant_id, bool &is_enabled);

// 检查ls_ckpt data是否允许执行，以及检查logservice是否打开
// 检查通过，则其他模块可认为ls_ckpt这个功能是available的
int check_ss_ls_ckpt_enabled(const uint64_t tenant_id, bool &is_enabled);

// 对check_ss_ls_ckpt_data_enabled的封装，增加errsim逻辑。
// 删除、GC分类和存在性探测必须传播版本查询错误，避免在门禁状态未知时按单写布局处理。
int check_double_route_ss_meta(const uint64_t tenant_id, bool &need_double_route);
  
#define OB_SS_META_TYPE_DEF(X)                  \
  X(LS_CKPT,                 0)                 \
  X(LS_META,                 1)                 \
  X(TABLET_META,             2)                 \
  X(META_CTRL,               3)                 \
  X(MAX_TYPE,                4)

#define GENERATE_SS_META_TYPE_ENUM_(name, value) name = value,
enum class ObSSMetaTypeEnum : int64_t
{
  OB_SS_META_TYPE_DEF(GENERATE_SS_META_TYPE_ENUM_)
};
#undef GENERATE_SS_META_TYPE_ENUM_

#define GENERATE_SS_META_TYPE_TO_STRING_(name, value) #name,
constexpr const char *OB_SS_META_TYPE_NAMES_[] = {
  OB_SS_META_TYPE_DEF(GENERATE_SS_META_TYPE_TO_STRING_)
};
#undef GENERATE_SS_META_TYPE_TO_STRING_

inline const char *ss_meta_to_string(ObSSMetaTypeEnum meta_type) {
  const char *type_str = "UNKNOWN";
  const int64_t idx = static_cast<int64_t>(meta_type);
  if (idx >= 0 && idx < static_cast<int64_t>(sizeof(OB_SS_META_TYPE_NAMES_) / sizeof(const char *))) {
    type_str = OB_SS_META_TYPE_NAMES_[idx];
  }
  return type_str;
}

struct ObSSMetaBaseType
{
public:
  int64_t type_val_;

  ObSSMetaBaseType() : type_val_(-1) {}
  virtual ~ObSSMetaBaseType() {};
  virtual void reset() { type_val_ = -1; }
  virtual bool is_valid() const { return type_val_ >= 0; }
  int64_t get_type_val() const { return type_val_; }

  TO_STRING_KV(K_(type_val));
};

struct ObSSMetaVersionFilter final
{
public:
  ObSSMetaVersionFilter()
    : is_dedup_by_prefix_(false), is_desc_order_(false), version_start_(), version_end_(),
      limit_(-1) {}

  void set_dedup_by_prefix() { is_dedup_by_prefix_ = true; }
  void set_desc_order() { is_desc_order_ = true; }
  void set_limit(const int64_t limit) { limit_ = limit; }
  int set_version_range(const share::SCN &start, const share::SCN &end)
  {
    int ret = common::OB_SUCCESS;
    if (!start.is_valid() || !end.is_valid()
        || start.get_val_for_inner_table_field() > end.get_val_for_inner_table_field()) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      version_start_ = start;
      version_end_ = end;
    }
    return ret;
  }
  void set_upper_bound(const share::SCN &upper_bound)
  { version_start_ = share::SCN::min_scn(); version_end_ = upper_bound; }
  void reset()
  { is_dedup_by_prefix_ = false; is_desc_order_ = false; version_start_.reset(); version_end_.reset();
    limit_ = -1; }

  bool is_desc_order() const { return is_desc_order_; }
  bool is_dedup_by_prefix() const { return is_dedup_by_prefix_; }
  int64_t get_limit() const { return limit_; }
  bool has_limit() const { return limit_ > 0; }
  bool has_version_range() const
  { return version_start_.is_valid() && version_end_.is_valid(); }
  bool cover_all_version() const
  { return version_start_ == SCN::min_scn() && version_end_ == SCN::max_scn(); }

  const share::SCN &get_version_start() const { return version_start_; }
  const share::SCN &get_version_end() const   { return version_end_; }

  bool is_valid() const
  {
    return !has_version_range()
           || version_start_.get_val_for_inner_table_field()
              <= version_end_.get_val_for_inner_table_field();
  }

  TO_STRING_KV(K_(is_dedup_by_prefix), K_(is_desc_order), K_(version_start), K_(version_end),
               K_(limit));

private:
  bool         is_dedup_by_prefix_;
  bool         is_desc_order_;
  share::SCN   version_start_;
  share::SCN   version_end_;
  int64_t      limit_;
};

struct ObSSMetaType : public ObSSMetaBaseType
{
public:
  ObSSMetaType();
  virtual ~ObSSMetaType() {};
  virtual bool is_valid() const override;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_str(const ObSSMetaTypeEnum &type);
  static ObSSMetaTypeEnum get_type(const char *type_str);
};

struct ObSSMetaLSKey final
{
public:
  share::ObLSID ls_id_;
  share::SCN meta_scn_;

  ObSSMetaLSKey();
  ~ObSSMetaLSKey() { reset(); }

  void reset();
  bool is_valid() const { return ls_id_.is_valid() && meta_scn_.is_valid(); }
  blocksstable::MacroBlockId convert_to_macro_id(const bool is_in_table);

  TO_STRING_KV(K_(ls_id), K_(meta_scn));
};

struct ObSSMetaLSCkptKey final
{
public:
  share::ObLSID ls_id_;
  share::SCN meta_scn_;

  ObSSMetaLSCkptKey();
  ~ObSSMetaLSCkptKey() { reset(); }

  void reset();
  bool is_valid() const { return ls_id_.is_valid() && meta_scn_.is_valid(); }
  blocksstable::MacroBlockId convert_to_macro_id(const bool is_in_table);

  TO_STRING_KV(K_(ls_id), K_(meta_scn));
};

struct ObSSMetaLSCkptVal final
{
  OB_UNIS_VERSION_V(1);
public:
  int64_t op_id_;

  ObSSMetaLSCkptVal() : op_id_(-1) {}
  ~ObSSMetaLSCkptVal() { reset(); }

  void reset() { op_id_ = -1; }

  TO_STRING_KV(K_(op_id));
};

struct ObSSMetaTabletKey : public ObSSTabletParam
{
public:
  share::SCN meta_scn_;

  ObSSMetaTabletKey();
  ObSSMetaTabletKey(const ObSSMetaTabletKey &rhs);
  ~ObSSMetaTabletKey() { reset(); }

  void reset();
  bool is_valid() const
  {
    return ObSSTabletParam::is_valid() && meta_scn_.is_valid();
  }
  bool operator==(const ObSSMetaTabletKey &rhs) const;
  // 普通 tablet MacroBlockId 不编码 LS，使用 default_ls_id；inner tablet 使用自身编码的 LS。
  int init_from_macro_id(const blocksstable::MacroBlockId &macro_id,
                         const share::ObLSID &default_ls_id);
  blocksstable::MacroBlockId convert_to_macro_id(const bool is_in_table);

  INHERIT_TO_STRING_KV("ObSSTabletParam", ObSSTabletParam, K_(meta_scn));
};

bool is_valid_ls_ckpt_macro_id(const blocksstable::MacroBlockId &macro_id);
int validate_ls_ckpt_object_id(const share::ObLSID &ls_id,
                               const blocksstable::MacroBlockId &macro_id);
bool is_valid_ss_tablet_id(const blocksstable::MacroBlockId &macro_id);

#define OB_SS_TIERED_META_TYPE_DEF(X)           \
  X(TABLE_STORE,             0)                 \
  X(STORAGE_SCHEMA,          1)                 \
  X(SSTABLE_META,            2)                 \
  X(CO_SSTABLE,              3)                 \
  X(MAX_TYPE,                4)

#define GENERATE_SS_TIERED_META_TYPE_ENUM_(name, value) name = value,
enum class ObSSTieredMetaTypeEnum : int64_t
{
  OB_SS_TIERED_META_TYPE_DEF(GENERATE_SS_TIERED_META_TYPE_ENUM_)
};
#undef GENERATE_SS_TIERED_META_TYPE_ENUM_

#define GENERATE_SS_TIERED_META_TYPE_TO_STRING_(name, value) #name,
constexpr const char *OB_SS_TIERED_META_TYPE_NAMES_[] = {
  OB_SS_TIERED_META_TYPE_DEF(GENERATE_SS_TIERED_META_TYPE_TO_STRING_)
};
#undef GENERATE_SS_TIERED_META_TYPE_TO_STRING_

inline const char *ss_tiered_meta_to_string(ObSSTieredMetaTypeEnum meta_type) {
  const char *type_str = "UNKNOWN";
  const int64_t idx = static_cast<int64_t>(meta_type);
  if (idx >= 0 && idx < static_cast<int64_t>(sizeof(OB_SS_TIERED_META_TYPE_NAMES_) / sizeof(const char *))) {
    type_str = OB_SS_TIERED_META_TYPE_NAMES_[idx];
  }
  return type_str;
}

struct ObSSTieredMetaType : public ObSSMetaBaseType
{
public:
  ObSSTieredMetaType();
  virtual ~ObSSTieredMetaType() {};
  virtual bool is_valid() const override;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_str(const ObSSTieredMetaTypeEnum &type);
  static ObSSTieredMetaTypeEnum get_type(const char *type_str);
};

struct ObSSMetaTieredMetaKey
{
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t op_id_;
  int64_t data_seq_;
  share::SCN reorgan_scn_;

  ObSSMetaTieredMetaKey();
  ~ObSSMetaTieredMetaKey() { reset(); }

  void reset();
  blocksstable::MacroBlockId convert_to_macro_id(const bool is_in_table);

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(op_id), K_(data_seq), K_(reorgan_scn));
};

enum class ObSSMetaAtomicOpType
{
  INVALID_OP = 0,
  LS_OP = 1,           // ls_ckpt、ls_meta op
  TABLET_OP = 2,       // tablet op
  SSTABLE_OP = 3,      // sstable op
  MAX_OP
};

struct ObSSMetaOpBuffer;

struct ObSSMetaAtomicOpInitParam
{
public:
  ObSSMetaAtomicOpInitParam()
    : op_id_(-1),
      meta_scn_(),
      ls_id_(),
      tablet_id_(),
      reorgan_scn_(),
      protocol_type_(),
      meta_value_(nullptr)
  {}
  int64_t op_id_;
  share::SCN meta_scn_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorgan_scn_;
  ObAtomicProtocolType protocol_type_;
  const ObSSMetaOpBuffer *meta_value_;

  TO_STRING_KV(K_(op_id), K_(meta_scn), K_(ls_id), K_(tablet_id), K_(reorgan_scn),
    K_(protocol_type), KP_(meta_value));
};

struct ObSSMetaOpBuffer
{
public:
  char *buf_; // user use buffer
  char *ori_buf_; // original buffer
  int64_t size_;
  int64_t data_len_;
  int64_t op_id_;

public:
  ObSSMetaOpBuffer()
    : buf_(NULL), ori_buf_(NULL), size_(0), data_len_(0), op_id_(-1) {}
  ObSSMetaOpBuffer(char *buf, int64_t size)
    : buf_(buf), ori_buf_(buf), size_(size), data_len_(0), op_id_(-1) {}
  // owns ori_buf_; only ownership-transfer via assign() is allowed, copies are forbidden
  ObSSMetaOpBuffer(const ObSSMetaOpBuffer &) = delete;
  ObSSMetaOpBuffer &operator=(const ObSSMetaOpBuffer &) = delete;

  bool is_valid() const { return buf_ != NULL && size_ > 0; }
  bool has_data() const { return is_valid() && data_len_ > 0; }
  void reset();
  void assign(const char *buf, int64_t size);
  void assign(ObSSMetaOpBuffer &rhs); // shallow copy, just like 'move'
  void set_op_id(const int64_t op_id) { op_id_ = op_id; }

  TO_STRING_KV(KP_(buf), KP_(ori_buf), K_(size), K_(data_len), K_(op_id));
};

class ObSSMetaOpBufferGuard
{
public:
  ObSSMetaOpBufferGuard() : buffer_() {}
  ~ObSSMetaOpBufferGuard() { reset(); }
  void reset()
  {
    share::mtl_free(buffer_.ori_buf_);
    buffer_.reset();
  }
  void set_buffer(ObSSMetaOpBuffer &buffer) { buffer_.assign(buffer); }
  ObSSMetaOpBuffer &get_buffer() { return buffer_; }
  bool is_valid() const { return buffer_.is_valid(); }
private:
  ObSSMetaOpBuffer buffer_;
};

int get_meta_op_buf_size(const ObAtomicProtocolType &protocol_type, int64_t &buf_size);
int alloc_meta_op_buffer(const ObAtomicProtocolType &protocol_type, ObSSMetaOpBufferGuard &buffer_guard);

// serialize meta info to content buf
// default implementation is call meta_info serialize function
// specialization this template function to implement your own serialization
template<typename MetaInfo>
inline int common_deserialize_meta_info(const ObSSMetaOpBuffer &content_buffer,
                                        MetaInfo &meta_info)
{
  #define PRINT_WRAPPER K(ret), K(content_buffer), K(meta_info)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t data_size = content_buffer.data_len_ > 0 ? content_buffer.data_len_ : content_buffer.size_;
  if (OB_FAIL(meta_info.deserialize(content_buffer.buf_, data_size, pos))) {
    STORAGE_LOG(WARN, "fail to deserialize meta info", PRINT_WRAPPER);
  }
  STORAGE_LOG(INFO, "deserialize meta info end", PRINT_WRAPPER);
  return ret;
  #undef PRINT_WRAPPER
}

class ObSSMetaHeadValueInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSMetaHeadValueInfo()
    : mark_delete_(false),
      update_reason_(ObMetaUpdateReason::INVALID_META_UPDATE_REASON),
      table_store_meta_info_(),
      deleted_scn_()
  {}
  ObSSMetaHeadValueInfo(const ObSSMetaHeadValueInfo &other)
    : mark_delete_(other.mark_delete_),
      update_reason_(other.update_reason_),
      table_store_meta_info_(other.table_store_meta_info_),
      deleted_scn_(other.deleted_scn_)
  {}

  ~ObSSMetaHeadValueInfo() {}
  int set(const ObMetaUpdateReason &update_reason, const bool mark_delete = false);
  int set(const ObMetaUpdateReason &update_reason,
          const ObSSTabletTableStoreMetaInfo &table_store_meta_info,
          const bool mark_delete = false);
  void get_update_reason(ObMetaUpdateReason &update_reason) const
  {
    update_reason = update_reason_;
  }
  void get_table_store_meta(ObSSTabletTableStoreMetaInfo &table_store_meta_info) const
  {
    table_store_meta_info = table_store_meta_info_;
  }
  bool is_mark_deleted() const { return mark_delete_; }
  void set_deleted_scn(const share::SCN &deleted_scn) { deleted_scn_ = deleted_scn; }
  const share::SCN &get_deleted_scn() const { return deleted_scn_; }
  bool is_valid() const { return is_valid_update_reason(update_reason_); }
  bool operator==(const ObSSMetaHeadValueInfo &rhs) const
  {
    return (mark_delete_ == rhs.mark_delete_ && update_reason_ == rhs.update_reason_
            && table_store_meta_info_ == rhs.table_store_meta_info_
            && deleted_scn_ == rhs.deleted_scn_);
  }
  TO_STRING_KV(K_(mark_delete), K_(update_reason), K_(table_store_meta_info), K_(deleted_scn));
public:
  bool mark_delete_;
  ObMetaUpdateReason update_reason_;
  // 部分场景下避免反序列化整个tablet_meta
  ObSSTabletTableStoreMetaInfo table_store_meta_info_;
  // only valid when mark_delete_ is true; appended for UNIS compat
  share::SCN deleted_scn_;
};

class ObSSMetaHeadValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSMetaHeadValue() : head_info_() {}
  virtual ~ObSSMetaHeadValue() {}

  // buf is produced by default_ss_meta_value or tablet_meta_value, and we just want
  // to deserialize head_value from it.
  int deserialize_value_info(const char *buf, const int64_t data_len, int64_t &pos);
  int set_meta_head_info(const ObSSMetaHeadValueInfo &head_info);
  const ObSSMetaHeadValueInfo &get_meta_head_info() const { return head_info_; }
  static int check_mark_deleted(const common::ObString &meta_value, bool &is_deleted);
  // parse only the head; deleted_scn may be invalid for rows written by older versions
  static int get_mark_delete_info(const common::ObString &meta_value,
                                  bool &is_deleted,
                                  share::SCN &deleted_scn);
  TO_STRING_KV(K_(head_info));
public:
  ObSSMetaHeadValueInfo head_info_;
};

template<typename MetaValueType>
class ObDefaultSSMetaValue : public ObSSMetaHeadValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObDefaultSSMetaValue() : ObSSMetaHeadValue(), meta_value_() {}
  ~ObDefaultSSMetaValue() {}
  INHERIT_TO_STRING_KV("ObSSMetaHeadValue", ObSSMetaHeadValue, K_(meta_value));
public:
  MetaValueType meta_value_;
};

template<typename MetaValueType>
int ObDefaultSSMetaValue<MetaValueType>::serialize(SERIAL_PARAMS) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = common::serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "serialize fail", K(ret));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      CHECK_SERIALIZE_SIZE(ObDefaultSSMetaValue<MetaValueType>, serial_size);
      ret = common::serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes,
                                                          size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

template<typename MetaValueType>
int ObDefaultSSMetaValue<MetaValueType>::serialize_(SERIAL_PARAMS) const
{
  int ret = ObSSMetaHeadValue::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, meta_value_);
  }
  return ret;
}

template<typename MetaValueType>
int ObDefaultSSMetaValue<MetaValueType>::deserialize(DESERIAL_PARAMS)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  CHECK_VERSION_LENGTH(ObDefaultSSMetaValue<MetaValueType>, version, len);
  if (OB_SUCC(ret)) {
    int64_t pos_orig = pos;
    pos = 0;
    if (OB_FAIL(deserialize_(buf + pos_orig, len, pos))) {
      STORAGE_LOG(WARN, "deserialize_ fail", "slen", len, K(pos), K(ret));
    }
    pos = pos_orig + len;
  }
  return ret;
}

template<typename MetaValueType>
int ObDefaultSSMetaValue<MetaValueType>::deserialize_(DESERIAL_PARAMS)
{
  int ret = ObSSMetaHeadValue::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, meta_value_);
  }
  return ret;
}

template<typename MetaValueType>
int64_t ObDefaultSSMetaValue<MetaValueType>::get_serialize_size() const
{
  int64_t len = get_serialize_size_();
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  len += common::serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
  return len;
}

template<typename MetaValueType>
int64_t ObDefaultSSMetaValue<MetaValueType>::get_serialize_size_() const
{
  int64_t len = ObSSMetaHeadValue::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, meta_value_);
  return len;
}

struct ObSSMetaKeyParam
{
  OB_UNIS_VERSION(1);
public:
  ObSSMetaKeyParam() 
    : is_in_table_(true), protocol_type_(ObAtomicProtocolTypeEnum::INVALID_TYPE), 
      tablet_key_() {}
  ObSSMetaKeyParam(const ObSSMetaKeyParam &other)
    : is_in_table_(true), protocol_type_(ObAtomicProtocolTypeEnum::INVALID_TYPE),
      tablet_key_()
  {
    assign(other);
  }
  virtual ~ObSSMetaKeyParam() { reset(); }
  ObSSMetaKeyParam &operator=(const ObSSMetaKeyParam &other)
  {
    if (this != &other) {
      assign(other);
    }
    return *this;
  }
  void assign(const ObSSMetaKeyParam &other)
  {
    is_in_table_ = other.is_in_table_;
    protocol_type_ = other.protocol_type_;
    // tablet_key_ is the larger member of the union and fully covers ls_key_ storage;
    // copying it propagates the active key regardless of which member is in use.
    tablet_key_ = other.tablet_key_;
  }
  // 
  void set_tablet_level_param(const ObAtomicProtocolTypeEnum protocol_type, 
                              const share::ObLSID ls_id, 
                              const common::ObTabletID tablet_id,
                              const share::SCN &meta_scn, 
                              const share::SCN &reorgan_scn,
                              const bool is_in_table = true);
  void set_ls_level_param(const ObAtomicProtocolTypeEnum protocol_type,
                          const share::ObLSID ls_id,
                          const share::SCN &meta_scn,
                          const bool is_in_table = true);
  void set_tenant_level_param(const ObAtomicProtocolTypeEnum protocol_type,
                              const bool is_in_table = true);
  bool is_valid() const;
  void reset()
  {
    is_in_table_ = true;
    protocol_type_ = ObAtomicProtocolTypeEnum::INVALID_TYPE;
    tablet_key_.reset();
  }
public:
  bool is_in_table_; // If true, means stored in inner_table
  ObAtomicProtocolTypeEnum protocol_type_;
  union {
    ObSSMetaLSKey ls_key_;
    ObSSMetaTabletKey tablet_key_;
  };
  TO_STRING_KV(K_(is_in_table), K_(protocol_type),
               K(tablet_key_.ls_id_),
               K(tablet_key_.tablet_id_),
               K(tablet_key_.meta_scn_),
               K(tablet_key_.reorganization_scn_));
};

struct ObSSMetaReadParamV2 : public ObSSMetaKeyParam
{
  OB_UNIS_VERSION(1);
public:
  ObSSMetaReadParamV2() : ObSSMetaKeyParam(), read_param_type_(ObSSMetaReadParamType::INVALID_KEY),
      read_result_type_(ObSSMetaReadResultType::READ_WHOLE_ROW), read_mark_delete_(false) {}
  ObSSMetaReadParamV2(const ObSSMetaReadParamV2 &other) { *this = other; }
  virtual ~ObSSMetaReadParamV2() { reset(); }
  void reset()
  {
    ObSSMetaKeyParam::reset();
    read_param_type_ = ObSSMetaReadParamType::INVALID_KEY;
    read_result_type_ = ObSSMetaReadResultType::READ_WHOLE_ROW;
    read_mark_delete_ = false;
  }
  ObSSMetaReadParamV2 &operator=(const ObSSMetaReadParamV2 &other)
  {
    if (this != &other) {
      ObSSMetaKeyParam::operator=(other);
      read_param_type_ = other.read_param_type_;
      read_result_type_ = other.read_result_type_;
      read_mark_delete_ = other.read_mark_delete_;
    }
    return *this;
  }
  void set_read_mark_delete(const bool read_mark_delete) { read_mark_delete_ = read_mark_delete; }
  void set_tablet_level_param(const ObSSMetaReadParamType read_param_type,
                              const ObSSMetaReadResultType read_result_type,
                              const ObAtomicProtocolTypeEnum protocol_type,
                              const share::ObLSID ls_id,
                              const common::ObTabletID tablet_id,
                              const share::SCN &meta_scn,
                              const share::SCN &reorgan_scn,
                              const bool is_in_table);
  void set_ls_level_param(const ObSSMetaReadParamType read_param_type,
                          const ObSSMetaReadResultType read_result_type,
                          const ObAtomicProtocolTypeEnum protocol_type,
                          const share::ObLSID ls_id,
                          const share::SCN &meta_scn,
                          const bool is_in_table);
  void set_tenant_level_param(const ObSSMetaReadParamType read_param_type,
                              const ObSSMetaReadResultType read_result_type,
                              const ObAtomicProtocolTypeEnum protocol_type,
                              const bool is_in_table);
  bool is_valid() const;
  bool read_prefix() const;
  bool read_key() const;
  ObSSMetaReadParamType read_param_type_;
  ObSSMetaReadResultType read_result_type_;
  bool read_mark_delete_;
  INHERIT_TO_STRING_KV("ObSSMetaKeyParam", ObSSMetaKeyParam,
                       K_(read_param_type), K_(read_result_type),
                       K_(read_mark_delete));
};

struct ObSSMetaRow
{
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSMetaBaseType meta_type_;
  common::ObString meta_key_;
  common::ObString meta_value_;

  ObSSMetaRow()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      ls_id_(),
      meta_type_(),
      meta_key_(),
      meta_value_()
  {}
  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    ls_id_.reset();
    meta_type_.reset();
    meta_key_.reset();
    meta_value_.reset();
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(meta_type), K_(meta_key), K_(meta_value));
};

struct ObSSMetaDeleteParamV2 : public ObSSMetaReadParamV2
{
  OB_UNIS_VERSION(1);
public:
  ObSSMetaDeleteParamV2() : ObSSMetaReadParamV2(), affected_rows_(0) {}
  virtual ~ObSSMetaDeleteParamV2() {}
  void reset()
  {
    ObSSMetaReadParamV2::reset();
    affected_rows_ = 0;
  }

  int64_t affected_rows_;
  INHERIT_TO_STRING_KV("ObSSMetaReadParamV2",  ObSSMetaReadParamV2, K_(affected_rows));
};

struct ObSSMetaTabletRow
{
public:
  int64_t type_;
  ObSSMetaTabletKey key_;
  ObString value_;

  ObSSMetaTabletRow()
    : type_(-1), key_(), value_() {}
  ObSSMetaTabletRow(const int64_t type,
                    const ObSSMetaTabletKey &key,
                    const ObString &value)
    : type_(type), key_(key), value_(value) {}
  ~ObSSMetaTabletRow() {}
  bool operator==(const ObSSMetaTabletRow &rhs) const
  {
    return (type_ == rhs.type_ && key_ == rhs.key_ && value_ == rhs.value_);
  }

  TO_STRING_KV(K_(type), K_(key), K_(value));
};

struct ObSSMetaTabletTinyInfo final
{
public:
  ObSSMetaTabletTinyInfo();
  ~ObSSMetaTabletTinyInfo() { reset(); }
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(is_mark_delete), K_(deleted_scn), K_(max_replayed_scn), K_(min_major_snapshot),
               K_(max_major_snapshot));

public:
  bool is_mark_delete_;
  share::SCN deleted_scn_;
  share::SCN max_replayed_scn_;
  int64_t min_major_snapshot_;
  int64_t max_major_snapshot_;
};


} // end namespace storage
} // end namespace oceanbase
#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_DEFINE_H_ */
