/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_META_COMMON_
#define OCEANBASE_STORAGE_OB_SHARED_META_COMMON_

#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/incremental/sslog/ob_sslog_define.h"
#include "storage/ob_i_table.h"
#include "storage/incremental/share/ob_shared_tablet_extra_info.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
};
namespace common
{
class ObArenaAllocator;
class ObTabletID;
};
namespace storage
{
using namespace sslog;

class ObTablet;
class ObSSLSMeta;
class ObTabletTableStore;


enum class ObMetaUpdateReason : int64_t  // FARM COMPAT WHITELIST
{
  INVALID_META_UPDATE_REASON             = 0,
  DEFAULT_META_UPDATE_REASON             = 1,

  MIN_LS_META_UPDATE_REASON              = 2,
  CREATE_LS                              = 3,
  UPDATE_LS_ID_META                      = 4,
  UPDATE_LS_GC_STATE                     = 5,
  UPDATE_LS_SS_CHECKPOINT_SCN            = 6,
  RESTORE_UPDATE_LS_META                 = 7,
  CREATE_LS_CKPT                         = 8,  
  MAX_LS_META_UPDATE_REASON                 ,
  // should less than 1000

  MIN_TABLET_META_UPDATE_REASON          = 1001,
  CREATE_TABLET                          = 1002,
  TABLET_UPLOAD_DATA_MINI_SSTABLE        = 1003,
  TABLET_UPLOAD_DDL_DUMP_SSTABLE         = 1005,
  TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE  = 1006,
  TABLET_COMPACT_ADD_DATA_MINOR_SSTABLE  = 1008,
  TABLET_DDL_ADD_DATA_MAJOR_SSTABLE      = 1009,
  TABLET_STORAGE_HA_ADD_SSTABLES         = 1010,
  TABLET_TRANSFER_ADD_SSTABLES           = 1011, // deprecated
  TABLET_SPLIT_ADD_MINOR_SSTABLE         = 1012,
  TABLET_SPLIT_ADD_MAJOR_SSTABLE         = 1014,
  TABLET_META_UPDATE                     = 1015,
  TABLET_BACKFILL_ADD_SSTABLE            = 1016,
  TABLET_BACKFILL_REPLACE_ADD_SSTABLES   = 1017,
  TABLET_GC_SSTABLE                      = 1018,
  TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE   = 1019,
  TABLET_ADD_DATA_INC_MAJOR_SSTABLE      = 1020,
  TABLET_UPLOAD_INC_MAJOR_DDL_DUMP_SSTABLE
                                         = 1021,
  TABLET_UPLOAD_COMPACT_ADD_DATA_MINOR_SSTABLE = 1022,
  TABLET_BACKFILL_UPLOAD_SSTABLE         = 1023,
  TABLET_UPLOAD_REPLACE_MINOR_SSTABLE    = 1024,
  TABLET_TRANSFER_REPLACE_MINOR_SSTABLE  = 1025,
  TABLET_FORK_REPLACE_SSTABLES           = 1026,
  TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE = 1027,
  MAX_TABLET_META_UPDATE_REASON,
};
const char *get_meta_update_reason_name(const ObMetaUpdateReason reason);
static inline bool is_valid_update_reason(const ObMetaUpdateReason reason)
{
  return (reason == ObMetaUpdateReason::DEFAULT_META_UPDATE_REASON
          || (reason > ObMetaUpdateReason::MIN_LS_META_UPDATE_REASON
              && reason < ObMetaUpdateReason::MAX_LS_META_UPDATE_REASON)
          || (reason > ObMetaUpdateReason::MIN_TABLET_META_UPDATE_REASON
              && reason < ObMetaUpdateReason::MAX_TABLET_META_UPDATE_REASON));
}
static inline bool is_transfer_backfill_update_reason(const ObMetaUpdateReason reason)
{
  // 仅 Transfer 回填类 reason 可在 TRANSFER_OUT/TRANSFER_OUT_DELETED 下生成新版本；
  // 漏配会阻断回填与 replace，使 min_recycle_scn 无法刷新。通用 direct load/DDL reason
  // 不应借道放行。
  return reason == ObMetaUpdateReason::TABLET_BACKFILL_ADD_SSTABLE
      || reason == ObMetaUpdateReason::TABLET_BACKFILL_UPLOAD_SSTABLE
      || reason == ObMetaUpdateReason::TABLET_TRANSFER_REPLACE_MINOR_SSTABLE
      || reason == ObMetaUpdateReason::TABLET_BACKFILL_UPDATE_DATA_INC_MAJOR_SSTABLE;
}

class ObSSTabletTableStoreMetaInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSTabletTableStoreMetaInfo()
    : is_inited_(false),
      major_tables_cnt_(-1),
      minor_tables_cnt_(-1),
      ddl_tables_cnt_(-1),
      meta_major_tables_cnt_(-1),
      minor_trans_state_determined_(true),
      last_major_snapshot_version_(-1),
      tx_recycle_scn_(),
      uncommitted_inc_major_cnt_(-1)
  {}
  ~ObSSTabletTableStoreMetaInfo() {}
  bool is_inited() const { return is_inited_; }
  // for empty shell tablet
  void set_none()
  {
    major_tables_cnt_ = 0;
    minor_tables_cnt_ = 0;
    ddl_tables_cnt_ = 0;
    meta_major_tables_cnt_ = 0;
    minor_trans_state_determined_ = true;
    last_major_snapshot_version_ = 0;
    tx_recycle_scn_ = share::SCN::min_scn();
    is_inited_ = true;
  }

  int set(const int64_t major_tables_cnt,
          const int64_t minor_tables_cnt,
          const int64_t ddl_tables_cnt,
          const int64_t meta_major_tables_cnt,
          const bool minor_trans_state_determined,
          const int64_t last_major_snapshot_version,
          const share::SCN tx_recycle_scn,
          const int64_t uncommitted_inc_major_cnt)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
    } else {
      major_tables_cnt_ = major_tables_cnt;
      minor_tables_cnt_ = minor_tables_cnt;
      ddl_tables_cnt_ = ddl_tables_cnt;
      meta_major_tables_cnt_ = meta_major_tables_cnt;
      minor_trans_state_determined_ = minor_trans_state_determined;
      last_major_snapshot_version_ = last_major_snapshot_version;
      tx_recycle_scn_ = tx_recycle_scn;
      uncommitted_inc_major_cnt_ = uncommitted_inc_major_cnt;
      is_inited_ = true;
    }
    return ret;
  }
  int init_by_table_store(
    const ObTabletTableStore &table_store,
    const bool is_ls_tx_data_tablet);
  bool operator==(const ObSSTabletTableStoreMetaInfo &other) const
  {
    return major_tables_cnt_ == other.major_tables_cnt_
        && minor_tables_cnt_ == other.minor_tables_cnt_
        && ddl_tables_cnt_ == other.ddl_tables_cnt_
        && meta_major_tables_cnt_ == other.meta_major_tables_cnt_
        && minor_trans_state_determined_ == other.minor_trans_state_determined_
        && last_major_snapshot_version_ == other.last_major_snapshot_version_
        && tx_recycle_scn_ == other.tx_recycle_scn_
        && uncommitted_inc_major_cnt_ == other.uncommitted_inc_major_cnt_;
  }
  TO_STRING_KV(K_(major_tables_cnt), K_(minor_tables_cnt),
               K_(ddl_tables_cnt), K_(meta_major_tables_cnt),
               K_(minor_trans_state_determined), K_(last_major_snapshot_version), K_(tx_recycle_scn),
               K_(uncommitted_inc_major_cnt));
public:
  bool is_inited_;
  int64_t major_tables_cnt_;
  int64_t minor_tables_cnt_;
  int64_t ddl_tables_cnt_;
  int64_t meta_major_tables_cnt_;
  bool minor_trans_state_determined_;
  int64_t last_major_snapshot_version_;
  share::SCN tx_recycle_scn_;
  int64_t uncommitted_inc_major_cnt_;
};

class ObSSMetaUpdateMetaInfo
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSMetaUpdateMetaInfo()
    : update_reason_(ObMetaUpdateReason::INVALID_META_UPDATE_REASON),
      acquire_scn_(),
      sstable_op_id_(-1),
      table_store_meta_info_()
  {}
  ObSSMetaUpdateMetaInfo(const ObSSMetaUpdateMetaInfo &other)
    : update_reason_(other.update_reason_),
      acquire_scn_(other.acquire_scn_),
      sstable_op_id_(other.sstable_op_id_),
      table_store_meta_info_(other.table_store_meta_info_)
  {}
  virtual ~ObSSMetaUpdateMetaInfo() {}
  int set(const ObMetaUpdateReason reason,
          const share::SCN acquire_scn);
  int set(const ObMetaUpdateReason reason,
          const share::SCN acquire_scn,
          const int64_t sstable_op_id);
  int set(const ObMetaUpdateReason reason,
          const share::SCN acquire_scn,
          const int64_t sstable_op_id,
          const ObSSTabletTableStoreMetaInfo &table_store_meta_info);
  void get(ObMetaUpdateReason &reason,
           share::SCN &acquire_scn) const
  {
    reason = update_reason_;
    acquire_scn = acquire_scn_;
  }
  void get(ObMetaUpdateReason &reason,
           share::SCN &acquire_scn,
           int64_t &sstable_op_id) const
  {
    reason = update_reason_;
    acquire_scn = acquire_scn_;
    sstable_op_id = sstable_op_id_;
  }
  void get(ObMetaUpdateReason &reason,
           share::SCN &acquire_scn,
           int64_t &sstable_op_id,
           ObSSTabletTableStoreMetaInfo &table_store_meta_info) const
  {
    reason = update_reason_;
    acquire_scn = acquire_scn_;
    sstable_op_id = sstable_op_id_;
    table_store_meta_info = table_store_meta_info_;
  }
  bool is_valid() const
  { return (is_valid_update_reason(update_reason_)
            && acquire_scn_.is_valid()); }
  bool operator==(const ObSSMetaUpdateMetaInfo &rhs) const
  {
    return (update_reason_ == rhs.update_reason_
            && acquire_scn_ == rhs.acquire_scn_
            && sstable_op_id_ == rhs.sstable_op_id_
            && table_store_meta_info_ == rhs.table_store_meta_info_);
  }
  TO_STRING_KV(K_(update_reason), K_(acquire_scn), K_(sstable_op_id),
               K_(table_store_meta_info));
public:
  ObMetaUpdateReason update_reason_;
  share::SCN acquire_scn_;
  int64_t sstable_op_id_; // sstable operation id, used for sstable gc.
  ObSSTabletTableStoreMetaInfo table_store_meta_info_;
};



class ObSSMetaIterator
{
public:
  enum IterType
  {
    Invalid = 0,
    ObSSTabletIterator = 1,
    ObSSTabletIDIterator = 2,
    ObSSLSMetaIterator = 3,
    ObSSLSIDIterator = 4,
    ObRawMetaRowIterator = 5,
    // DB-based wrapper iterators (ss_meta inner table)
    ObSSMetaLSIDIter = 6,
    ObSSMetaLSSCNIterator = 7,
    ObSSMetaTabletKeyIterator = 8,
    ObSSMetaTabletSCNIterator = 9,
    ObSSMetaRawRowIter = 10,
    ObSSMetaTabletIter = 11,
    ObSSMetaTabletIDIter = 12,
    ObSSMetaTabletMacroIDIter = 13,
    ObSSMetaTabletTinyInfoIter = 14,
    ObSSMetaLSCkptKeyIter = 15, //FARM COMPAT WHITELIST
  };
public:
  ObSSMetaIterator() : type_(Invalid) {}
  explicit ObSSMetaIterator(const IterType type) : type_(type) {}
  virtual ~ObSSMetaIterator() {}
  IterType get_type() const { return type_; }
  VIRTUAL_TO_STRING_KV(K_(type));
public:
  IterType type_;
};

enum class ObSSMetaReadParamType : int64_t
{
  INVALID_KEY = 0,
  // read tablet level row with whole meta key, including tenant_id, ls_id, tablet_id, reorganization_scn
  TABLET_KEY = 1,
  // read ls level row with whole meta key, including tenant_id, ls_id
  LS_KEY = 2,
  // prefix read tablet level row with ls id, prefix with tenant_id and ls_id
  LS_PREFIX = 3,
  // prefix read ls level row with tenant id as prefix, prefix with tenant_id
  TENANT_PREFIX = 4,
  // prefix read tablet level row prefix with tenant_id, ls_id, tablet_id
  TABLET_PREFIX = 5,
};

enum class ObSSMetaReadResultType : int64_t
{
  INVALID_READ = 0,
  READ_WHOLE_ROW = 1, // read the whole row
  READ_ONLY_KEY = 2, // just read meta key
  DML_READ = 2, // read during dm stmt 
};

struct ObSSTabletParam
{
public:
  ObSSTabletParam() : ls_id_(), tablet_id_(), reorganization_scn_(share::SCN::min_scn()) {}
  ObSSTabletParam(const ObSSTabletParam &rhs)
    : ls_id_(rhs.ls_id_), tablet_id_(rhs.tablet_id_), 
      reorganization_scn_(rhs.reorganization_scn_) {}
  void reset()
  {
    ls_id_ = share::ObLSID();
    tablet_id_ = common::ObTabletID();
    reorganization_scn_ = share::SCN::min_scn();
  }
  bool is_valid() const
  {
    return ls_id_.is_valid()
        && tablet_id_.is_valid()
        && reorganization_scn_.is_valid();
  }
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorganization_scn_;

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(reorganization_scn));
};




class ObSSTabletCreateHelper final
{
  public:
  // 从 __all_shared_meta 内部表的 meta_value 反序列化 tablet_handle
  // meta_value 格式为 ObSSTabletMetaValue（基于 ObSSMetaHeadValue）
  static int get_ss_meta_tablet_handle_by_buf(const char *buf,
                                              const int64_t data_len,
                                              int64_t &pos,
                                              ObTabletHandle &tablet_hdl,
                                              ObArenaAllocator &allocator);
  static int get_ss_meta_tablet_handle_by_buf(const ObMetaDiskAddr &tablet_addr,
                                              const char *buf,
                                              const int64_t data_len,
                                              int64_t &pos,
                                              ObTabletHandle &tablet_hdl,
                                              ObArenaAllocator &allocator);
  static int get_ss_meta_tablet_handle_by_buf(const ObMetaDiskAddr &tablet_addr,
                                              const char *buf,
                                              const int64_t data_len,
                                              int64_t &pos,
                                              ObTabletHandle &tablet_hdl,
                                              ObSSUpdateTabletLog &update_log,
                                              ObArenaAllocator &allocator);
  // 从 __all_shared_meta 内部表的 meta_value（ObSSTabletMetaValue 格式）中
  // 取出 tablet 原始序列化 buffer 及 update_log，不构造 ObTabletHandle
  static int get_ss_meta_raw_tablet_buf_by_buf(const ObMetaDiskAddr &tablet_addr,
                                               const char *buf,
                                               const int64_t data_len,
                                               int64_t &pos,
                                               char *&tablet_raw_buf,
                                               int64_t &tablet_raw_buf_len,
                                               ObSSUpdateTabletLog &update_log,
                                               ObArenaAllocator &allocator);

  private:
  static int bind_base_pointer_for_tablet_(const ObTabletHandle &tablet_handle,
                                           ObIAllocator &allocator);
};


//
// Below for new ssmeta
//
class ObSSRawMetaRow
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSRawMetaRow()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      ls_id_(),
      meta_type_(-1),
      part_(0),
      meta_key_(),
      meta_value_()
  {}
  ObSSRawMetaRow(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t meta_type,
                 const ObString &meta_key, const ObString &meta_value)
    : tenant_id_(tenant_id), ls_id_(ls_id), meta_type_(meta_type), part_(0),
      meta_key_(meta_key), meta_value_(meta_value) {}
  virtual ~ObSSRawMetaRow() {}
  int deep_copy(ObIAllocator &allocator, const ObSSRawMetaRow &other);
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(meta_type), K_(part), K_(meta_key), K_(meta_value));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t meta_type_;
  // 当meta_value过大的时候会进行拆行，对于shared_meta表，不支持拆行，默认是0
  // 对于tiered表，如果不拆行，默认只有一行，part列为0，如果拆行，那么part列从0开始递增
  int64_t part_;
  ObString meta_key_;
  ObString meta_value_;
};

}
}
#endif // OCEANBASE_STORAGE_OB_SHARED_META_COMMON_
