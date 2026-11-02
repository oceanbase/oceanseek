/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/incremental/ssmeta/ob_ss_tablet_meta_info_iterator.h"

#include "share/ob_rpc_struct.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_store.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/index_block/ob_sstable_meta_info.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/ob_i_table.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

const int64_t ObSSTabletMetaInfoIterator::META_VALUE_BUF_LEN;
const int64_t ObSSTabletMetaInfoIterator::FULL_STR_ALLOC_MAX_LEN;

// ---- file-local helpers (moved verbatim from __all_virtual_shared_tablet_meta_info) ----
// 反序列化 tablet meta_value 得到 ObTablet（与 GC iterator 同路径），据此定位两个内嵌的
// singleton sub-meta（storage schema / table store）的 addr。
int ObSSTabletMetaInfoIterator::build_tablet_handle_and_sub_meta_addrs_(
    const common::ObString &tablet_meta_value,
    ObTabletHandle &tablet_handle,
    ObMetaDiskAddr &schema_addr,
    ObMetaDiskAddr &table_store_addr,
    ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  schema_addr.reset();
  table_store_addr.reset();
  if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(ls_id_, tablet_id_, transfer_scn_,
                                                    tablet_meta_value, allocator_,
                                                    tablet_handle, update_log))) {
    STORAGE_LOG(WARN, "build tablet handle failed", KR(ret), K_(ls_id), K_(tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet obj is null", KR(ret));
  } else {
    const ObTablet &tablet = *tablet_handle.get_obj();
    schema_addr = tablet.get_storage_schema_addr();
    table_store_addr = tablet.get_table_store_addr();
    if (!schema_addr.is_block()) {
      STORAGE_LOG(WARN, "schema addr is not block, skip read sub meta", K(schema_addr));
      schema_addr.reset();
    }
    if (!table_store_addr.is_block()) {
      STORAGE_LOG(WARN, "table store addr is not block, skip read sub meta", K(table_store_addr));
      table_store_addr.reset();
    }
  }
  return ret;
}

// ---- curated sub-meta summary formatters (replace verbose to_string) ----
// `arr` 中各 sstable 的最大 upper_trans_version（空则 0），用于概览 table store 的 minor/inc 落盘进度。
static int64_t max_upper_trans_version_(const ObSSTableArray &arr)
{
  int64_t max_upper_trans_version = 0;
  for (int64_t i = 0; i < arr.count(); ++i) {
    if (OB_NOT_NULL(arr.at(i))) {
      const int64_t upper_trans_version = arr.at(i)->get_upper_trans_version();
      if (upper_trans_version > max_upper_trans_version) {
        max_upper_trans_version = upper_trans_version;
      }
    }
  }
  return max_upper_trans_version;
}

static int64_t format_storage_schema_summary_(const ObStorageSchema &s,
                                              char *buf, const int64_t cap)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || cap <= 0) {
    // leave pos = 0
  } else {
    if (OB_FAIL(databuff_printf(buf, cap, pos, "{"))) {
      STORAGE_LOG(WARN, "databuff_printf open brace failed", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, cap, pos,
        "storage_schema_version=%ld,schema_version=%ld,column_cnt=%ld,"
        "store_column_cnt=%ld,rowkey_cnt=%ld,compat_mode=%d,table_type=%d,index_type=%d,"
        "row_store_type=%d,compressor_type=%d,column_group_cnt=%ld,has_all_column_group=%ld,"
        "is_cs_replica_compat=%d",
        s.storage_schema_version_, s.get_schema_version(), s.get_column_count(),
        s.store_column_cnt_, s.get_rowkey_column_num(),
        static_cast<int>(s.get_compat_mode()), static_cast<int>(s.get_table_type()),
        static_cast<int>(s.get_index_type()), static_cast<int>(s.get_row_store_type()),
        static_cast<int>(s.get_compressor_type()), s.get_column_group_count(),
        s.has_all_column_group(), s.is_cs_replica_compat() ? 1 : 0))) {
      STORAGE_LOG(WARN, "databuff_printf storage schema body failed", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, cap, pos, "}"))) {
      STORAGE_LOG(WARN, "databuff_printf close brace failed", KR(ret));
    }
    if (pos >= cap) {
      pos = cap - 1;
    }
    if (pos < 0) {
      STORAGE_LOG(WARN, "format buffer pos negative, reset to 0", K(pos), K(cap));
      pos = 0;
    }
    buf[pos] = '\0';
  }
  return pos;
}

static int64_t format_table_store_summary_(const ObTabletTableStore &ts,
                                           char *buf, const int64_t cap)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || cap <= 0) {
    // leave pos = 0
  } else {
    const ObSSTableArray &major = ts.get_major_sstables();
    const ObSSTableArray &minor = ts.get_minor_sstables();
    const ObSSTableArray &ddl = ts.get_ddl_sstables();
    const ObSSTableArray &inc_major = ts.get_inc_major_sstables();
    const ObSSTableArray &meta_major = ts.get_meta_major_sstables();
    if (OB_FAIL(databuff_printf(buf, cap, pos, "{"))) {
      STORAGE_LOG(WARN, "databuff_printf open brace failed", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, cap, pos,
        "major_cnt=%ld,minor_cnt=%ld,ddl_cnt=%ld,inc_major_cnt=%ld,"
        "meta_major_cnt=%ld,",
        major.count(), minor.count(), ddl.count(), inc_major.count(),
        meta_major.count()))) {
      STORAGE_LOG(WARN, "databuff_printf table store counts failed", KR(ret));
    }
    int64_t last_major_ss = 0;
    int64_t last_major_row = 0;
    if (!major.empty() && OB_NOT_NULL(major.at(0))) {
      blocksstable::ObSSTable *major_sst = major.at(0);
      last_major_ss = major_sst->get_snapshot_version();
      blocksstable::ObSSTableMetaHandle meta_hdl;
      if (OB_SUCCESS == major_sst->get_meta(meta_hdl)) {
        last_major_row = meta_hdl.get_sstable_meta().get_basic_meta().row_count_;
      }
    }
    int64_t max_upper_trans = max_upper_trans_version_(major);
    max_upper_trans = MAX(max_upper_trans, max_upper_trans_version_(minor));
    max_upper_trans = MAX(max_upper_trans, max_upper_trans_version_(inc_major));
    max_upper_trans = MAX(max_upper_trans, max_upper_trans_version_(meta_major));
    int64_t inc_row_cnt = 0;
    blocksstable::ObSSTableMetaHandle meta_hdl;
    for (int64_t i = 0; i < minor.count(); ++i) {
      if (OB_NOT_NULL(minor.at(i))) {
        if (OB_SUCCESS == minor.at(i)->get_meta(meta_hdl)) {
          inc_row_cnt += meta_hdl.get_sstable_meta().get_basic_meta().row_count_;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, cap, pos,
          "last_major_snapshot_version=%ld,last_major_row_cnt=%ld,max_upper_trans_version=%ld,"
          "unmerged_inc_major_cnt=%ld,inc_row_cnt=%ld",
          last_major_ss, last_major_row, max_upper_trans, inc_major.count(), inc_row_cnt))) {
        STORAGE_LOG(WARN, "databuff_printf table store body failed", KR(ret));
      } else if (OB_FAIL(databuff_printf(buf, cap, pos, "}"))) {
        STORAGE_LOG(WARN, "databuff_printf close brace failed", KR(ret));
      }
    }
    if (pos >= cap) {
      pos = cap - 1;
    }
    if (pos < 0) {
      STORAGE_LOG(WARN, "format buffer pos negative, reset to 0", K(pos), K(cap));
      pos = 0;
    }
    buf[pos] = '\0';
  }
  return pos;
}

template <typename T>
inline void append_obj_to_string_(char *buf, const int64_t cap, int64_t &pos, const T &obj)
{
  int64_t n = obj.to_string(buf + pos, cap - pos);
  if (n < 0) {
    n = 0;
  } else if (n > cap - pos) {
    n = cap - pos;
  }
  pos += n;
}

static int64_t format_sstable_summary_(const blocksstable::ObSSTable &st,
                                       char *buf, const int64_t cap)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || cap <= 0) {
    // leave pos = 0
  } else {
    const ObITable::TableKey &key = st.get_key();
    blocksstable::ObSSTableMetaHandle meta_hdl;
    bool have_meta = (OB_SUCCESS == st.get_meta(meta_hdl));
    if (OB_FAIL(databuff_printf(buf, cap, pos, "{"))) {
      STORAGE_LOG(WARN, "databuff_printf open brace failed", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, cap, pos,
        "tablet_id=%lu,table_type=%d,column_group_idx=%ld,start_scn=%lu,end_scn=%lu,",
        static_cast<uint64_t>(key.get_tablet_id().id()), static_cast<int>(key.table_type_),
        static_cast<int64_t>(key.column_group_idx_),
        st.get_start_scn().get_val_for_inner_table_field(),
        st.get_end_scn().get_val_for_inner_table_field()))) {
      STORAGE_LOG(WARN, "databuff_printf sstable key failed", KR(ret));
    }
    if (OB_SUCC(ret) && have_meta) {
      const blocksstable::ObSSTableBasicMeta &bm = meta_hdl.get_sstable_meta().get_basic_meta();
      if (OB_FAIL(databuff_printf(buf, cap, pos,
          "row_count=%ld,occupy_size=%ld,original_size=%ld,data_checksum=%ld,column_cnt=%ld,"
          "rowkey_cnt=%ld,data_macro_blk=%ld,data_micro_blk=%ld,index_macro_blk=%ld,"
          "upper_trans_version=%ld,max_merged_trans_version=%ld,create_snapshot_version=%ld,"
          "schema_version=%ld,contain_uncommitted_row=%d,filled_tx_scn=%lu,status=%d,",
          bm.row_count_, bm.occupy_size_, bm.original_size_, bm.data_checksum_, bm.column_cnt_,
          bm.rowkey_column_count_, bm.data_macro_block_count_, bm.data_micro_block_count_,
          bm.index_macro_block_count_, bm.upper_trans_version_, bm.max_merged_trans_version_,
          bm.create_snapshot_version_, bm.schema_version_, bm.contain_uncommitted_row_ ? 1 : 0,
          bm.filled_tx_scn_.get_val_for_inner_table_field(), static_cast<int>(bm.status_)))) {
        STORAGE_LOG(WARN, "databuff_printf sstable basic meta failed", KR(ret));
      } else if (OB_FAIL(databuff_printf(buf, cap, pos, "shared_flag="))) {
        STORAGE_LOG(WARN, "databuff_printf shared_flag failed", KR(ret));
      } else {
        append_obj_to_string_(buf, cap, pos, bm.table_shared_flag_);
        if (OB_FAIL(databuff_printf(buf, cap, pos, ",backup_flag="))) {
          STORAGE_LOG(WARN, "databuff_printf backup_flag failed", KR(ret));
        } else {
          append_obj_to_string_(buf, cap, pos, bm.table_backup_flag_);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, cap, pos, "}"))) {
        STORAGE_LOG(WARN, "databuff_printf close brace failed", KR(ret));
      }
    }
    if (pos >= cap) {
      pos = cap - 1;
    }
    if (pos < 0) {
      STORAGE_LOG(WARN, "format buffer pos negative, reset to 0", K(pos), K(cap));
      pos = 0;
    }
    buf[pos] = '\0';
  }
  return pos;
}

static int64_t format_co_sstable_summary_(const ObCOSSTableV2 &co,
                                          char *buf, const int64_t cap)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || cap <= 0) {
    // leave pos = 0
  } else {
    const ObITable::TableKey &key = co.get_key();
    const ObCOSSTableMeta &cs = co.get_cs_meta();
    const char *base_type = co.is_all_cg_base() ? "ALL_CG"
                          : (co.is_rowkey_cg_base() ? "ROWKEY_CG" : "OTHER");
    if (OB_FAIL(databuff_printf(buf, cap, pos, "{"))) {
      STORAGE_LOG(WARN, "databuff_printf open brace failed", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, cap, pos,
        "tablet_id=%lu,table_type=%d,column_group_idx=%ld,start_scn=%lu,end_scn=%lu,"
        "base_type=%s,is_cgs_empty_co=%d,column_group_cnt=%u,full_column_cnt=%ld,"
        "data_macro_blk=%lu,data_micro_blk=%lu,occupy_size=%lu,original_size=%lu,"
        "data_checksum=%ld",
        static_cast<uint64_t>(key.get_tablet_id().id()), static_cast<int>(key.table_type_),
        static_cast<int64_t>(key.column_group_idx_),
        co.get_start_scn().get_val_for_inner_table_field(),
        co.get_end_scn().get_val_for_inner_table_field(),
        base_type, co.is_cgs_empty_co_table() ? 1 : 0, cs.get_column_group_count(),
        static_cast<int64_t>(cs.full_column_cnt_), cs.data_macro_block_cnt_, cs.data_micro_block_cnt_,
        cs.occupy_size_, cs.original_size_, cs.data_checksum_))) {
      STORAGE_LOG(WARN, "databuff_printf co sstable body failed", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, cap, pos, "}"))) {
      STORAGE_LOG(WARN, "databuff_printf close brace failed", KR(ret));
    }
    if (pos >= cap) {
      pos = cap - 1;
    }
    if (pos < 0) {
      STORAGE_LOG(WARN, "format buffer pos negative, reset to 0", K(pos), K(cap));
      pos = 0;
    }
    buf[pos] = '\0';
  }
  return pos;
}

static int64_t format_tablet_meta_summary_(const ObTablet &tablet,
                                           const ObSSMetaHeadValueInfo &head_info,
                                           uint64_t data_version,
                                           char *buf, const int64_t cap)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || cap <= 0) {
    // leave pos = 0
  } else {
    const ObTabletMeta &meta = tablet.get_tablet_meta();
    if (OB_FAIL(databuff_print_multi_objs(buf, cap, pos,
        "{ls_id=", static_cast<int64_t>(tablet.get_ls_id().id()),
        ",tablet_id=", static_cast<uint64_t>(tablet.get_tablet_id().id()),
        ",data_tablet_id=", static_cast<uint64_t>(tablet.get_data_tablet_id().id()),
        ",is_empty_shell=", meta.is_empty_shell_ ? 1 : 0,
        ",snapshot_version=", meta.snapshot_version_,
        ",multi_version_start=", meta.multi_version_start_,
        ",clog_checkpoint_scn=", static_cast<int64_t>(meta.clog_checkpoint_scn_.get_val_for_inner_table_field()),
        ",ddl_checkpoint_scn=", static_cast<int64_t>(meta.ddl_checkpoint_scn_.get_val_for_inner_table_field()),
        ",mds_start_scn=", static_cast<int64_t>(meta.mds_start_scn_.get_val_for_inner_table_field()),
        ",ha_status=", meta.ha_status_,
        ",transfer_info=", meta.transfer_info_,
        ",ddl_replay_status=", static_cast<int>(meta.ddl_replay_status_),
        ",table_store_flag=", meta.table_store_flag_,
        ",space_usage=", meta.space_usage_,
        ",min_ss_tablet_scn=", static_cast<int64_t>(meta.min_ss_tablet_scn_.get_val_for_inner_table_field()),
        ",data_version=", data_version,
        ",head_info=", head_info,
        ",is_mark_deleted=", head_info.is_mark_deleted() ? 1 : 0,
        "}"))) {
      STORAGE_LOG(WARN, "format tablet meta summary failed", KR(ret));
    }
    if (pos >= cap) {
      pos = cap - 1;
    }
    if (pos < 0) {
      STORAGE_LOG(WARN, "format buffer pos negative, reset to 0", K(pos), K(cap));
      pos = 0;
    }
    buf[pos] = '\0';
  }
  return pos;
}

ObSSTabletMetaInfoIterator::ObSSTabletMetaInfoIterator()
    : is_inited_(false), collected_(false),
      tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), tablet_id_(),
      transfer_scn_(), meta_scn_(), mode_(obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE),
      sql_client_(nullptr),
      allocator_(), str_alloc_(), full_scratch_(), rows_(), cur_idx_(0),
      str_alloc_limit_reached_(false), meta_value_buf_(nullptr)
{
}

ObSSTabletMetaInfoIterator::~ObSSTabletMetaInfoIterator()
{
  reset();
}

void ObSSTabletMetaInfoIterator::reset()
{
  is_inited_ = false;
  collected_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  tablet_id_.reset();
  transfer_scn_.reset();
  meta_scn_.reset();
  mode_ = obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE;
  sql_client_ = nullptr;
  allocator_.reset();
  str_alloc_.reset();
  full_scratch_.reset();
  rows_.reset();
  cur_idx_ = 0;
  str_alloc_limit_reached_ = false;
  // meta_value_buf_ 由 allocator_ 分配，上面的 allocator_.reset() 已释放它。
  meta_value_buf_ = nullptr;
}

int ObSSTabletMetaInfoIterator::init(common::ObISQLClient &sql_client,
                                     const uint64_t tenant_id,
                                     const share::ObLSID &ls_id,
                                     const common::ObTabletID &tablet_id,
                                     const share::SCN &transfer_scn,
                                     const share::SCN &meta_scn,
                                     const obrpc::ObDumpSSTabletMetaInfoMode mode)
{
  int ret = OB_SUCCESS;
  const int64_t mode_val = static_cast<int64_t>(mode);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || !ls_id.is_valid_with_tenant(tenant_id)
             || !tablet_id.is_valid()
             || mode_val < static_cast<int64_t>(obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE)
             || mode_val > static_cast<int64_t>(obrpc::ObDumpSSTabletMetaInfoMode::FULL)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arg", KR(ret), K(tenant_id), K(ls_id), K(tablet_id), K(mode));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    transfer_scn_ = transfer_scn;
    meta_scn_ = meta_scn;
    mode_ = mode;
    sql_client_ = &sql_client;
    // init() 在 MTL_SWITCH(tenant) 下执行，成员 arena 按该租户记账。
    allocator_.set_attr(common::ObMemAttr(tenant_id_, "SubMetaIterA"));
    str_alloc_.set_attr(common::ObMemAttr(tenant_id_, "SubMetaIterS"));
    full_scratch_.set_attr(common::ObMemAttr(tenant_id_, "SubMetaIterF"));
    // meta_value_buf_ 16K，放栈的话内存太大，从 arena 分配。
    meta_value_buf_ = static_cast<char *>(allocator_.alloc(META_VALUE_BUF_LEN));
    if (OB_ISNULL(meta_value_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc meta_value buf failed", KR(ret), K(META_VALUE_BUF_LEN));
    } else if (OB_FAIL(generate_rows_())) {
      // generate_rows_ 失败：reset 释放已分配资源，保持 is_inited_=false，
      // 避免出现 is_inited_=true 但 rows_ 仅部分填充的中间态。
      STORAGE_LOG(WARN, "generate sub meta rows failed",
                 KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
      reset();
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::get_next(ObSSTabletMetaInfoRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret));
  } else if (cur_idx_ >= rows_.count()) {
    ret = OB_ITER_END;
  } else {
    row = rows_.at(cur_idx_);
    ++cur_idx_;
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::grow_buf_cap_(common::ObArenaAllocator &alloc,
                                              char *&buf, int64_t &cap, int64_t &len,
                                              const bool preserve_prefix)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || cap <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (cap >= FULL_MAX_BUF_LEN) {
    buf[cap - 1] = '\0';
    len = cap - 1;
    ret = OB_SIZE_OVERFLOW;
  } else {
    int64_t new_cap = cap * 2;
    if (new_cap > FULL_MAX_BUF_LEN) {
      new_cap = FULL_MAX_BUF_LEN;
    }
    if (preserve_prefix) {
      char *new_buf = static_cast<char *>(alloc.alloc(new_cap));
      if (OB_ISNULL(new_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(new_buf, buf, len);
        new_buf[len] = '\0';
        buf = new_buf;
        cap = new_cap;
      }
    } else {
      alloc.reuse();
      buf = static_cast<char *>(alloc.alloc(new_cap));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        cap = new_cap;
      }
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::append_cstr_grow_(common::ObArenaAllocator &alloc,
                                                  const char *s,
                                                  char *&buf, int64_t &cap, int64_t &len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || cap <= 0 || len < 0 || OB_ISNULL(s)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t slen = static_cast<int64_t>(strlen(s));
    while (OB_SUCC(ret)) {
      const int64_t avail = cap - len;
      if (slen < avail) {
        MEMCPY(buf + len, s, slen);
        len += slen;
        buf[len] = '\0';
        break;
      }
      if (OB_FAIL(grow_buf_cap_(alloc, buf, cap, len, true))) {
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          break;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf) && cap > 0) {
      buf[0] = '\0';
    }
    len = 0;
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::add_row_(const int64_t level,
                                         const common::ObString &type,
                                         const common::ObString &meta_value)
{
  int ret = OB_SUCCESS;
  const int64_t mv_len = meta_value.length();
  const int64_t need = type.length() + mv_len;
  if (str_alloc_limit_reached_) {
    ret = OB_SIZE_OVERFLOW;
  } else if (obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE != mode_
             && str_alloc_.used() + need > FULL_STR_ALLOC_MAX_LEN) {
    str_alloc_limit_reached_ = true;
    STORAGE_LOG(WARN, "str_alloc total reaches limit, stop appending rows",
                K(str_alloc_.used()), K(need), K(FULL_STR_ALLOC_MAX_LEN),
                K(level), K(type), K_(mode));
    ret = OB_SIZE_OVERFLOW;
  } else {
    ObSSTabletMetaInfoRow row;
    row.level_ = level;
    if (type.length() > 0) {
      char *type_buf = static_cast<char *>(str_alloc_.alloc(type.length()));
      if (OB_ISNULL(type_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc type buf failed", KR(ret), K(type.length()));
      } else {
        MEMCPY(type_buf, type.ptr(), type.length());
        row.type_.assign_ptr(type_buf, type.length());
      }
    }
    if (OB_SUCC(ret) && mv_len > 0) {
      char *mv_buf = static_cast<char *>(str_alloc_.alloc(mv_len));
      if (OB_ISNULL(mv_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "alloc meta_value buf failed", KR(ret), K(mv_len));
      } else {
        MEMCPY(mv_buf, meta_value.ptr(), mv_len);
        row.meta_value_.assign_ptr(mv_buf, mv_len);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rows_.push_back(row))) {
        STORAGE_LOG(WARN, "push row failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::read_tiered_meta_value_(
    ObTieredMetadataStore &tiered_store,
    const ObMetaDiskAddr &addr,
    common::ObArenaAllocator &allocator,
    common::ObString &sub_meta_value)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId macro_id;
  int64_t offset = 0;
  int64_t buf_size = 0;
  int64_t actual_size = 0;
  char *buf = nullptr;
  sub_meta_value.reset();

  if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "tiered meta addr is not block", KR(ret), K(addr));
  } else if (OB_FAIL(addr.get_block_addr(macro_id, offset, buf_size))) {
    STORAGE_LOG(WARN, "get tiered meta block addr failed", KR(ret), K(addr));
  } else if (OB_UNLIKELY(0 != offset || buf_size <= 0)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid tiered meta block range", KR(ret), K(addr), K(offset), K(buf_size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc tiered meta read buffer failed", KR(ret), K(addr), K(buf_size));
  } else if (OB_FAIL(tiered_store.read_meta(macro_id, buf, buf_size, actual_size))) {
    STORAGE_LOG(WARN, "read tiered meta value failed", KR(ret), K(addr), K(buf_size));
  } else if (OB_UNLIKELY(actual_size != buf_size)) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "tiered meta value size does not match addr", KR(ret), K(addr), K(actual_size), K(buf_size));
  } else {
    sub_meta_value.assign_ptr(buf, actual_size);
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::read_sub_meta_blob_(const SubMetaEmitCtx &ctx,
                                                    const ObMetaDiskAddr &addr,
                                                    common::ObString &blob)
{
  int ret = OB_SUCCESS;
  blob.reset();
  ctx.tiered_alloc->reuse();
  ret = read_tiered_meta_value_(*ctx.tiered_store, addr, *ctx.tiered_alloc, blob);
  if (OB_FAIL(ret)) {
    // sub-meta 块已不存在时跳过继续收集。blob 保持空，调用方直接略过。
    if (OB_OBJECT_NOT_EXIST == ret) {
      STORAGE_LOG(WARN, "skip sub-meta, no value emitted", KR(ret), K(addr));
      ret = OB_SUCCESS;
    } else {
      // addr 长度不足说明引用数据已损坏，转换为数据错误并向上传递。
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_INVALID_DATA;
      }
      STORAGE_LOG(WARN, "read sub-meta tiered value failed", KR(ret), K(addr));
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::append_block_id_array_(const char *name, int type,
    const blocksstable::ObSSTableMacroInfo &mi,
    common::ObArenaAllocator &alloc,
    char *&buf, int64_t &cap, int64_t &len)
{
  int ret = OB_SUCCESS;
  int64_t cnt = -1;
  int tmp_ret = OB_SUCCESS;
  if (blocksstable::ObMacroIdIterator::DATA_BLOCK == type) {
    tmp_ret = mi.get_data_block_count(cnt);
  } else if (blocksstable::ObMacroIdIterator::OTHER_BLOCK == type) {
    tmp_ret = mi.get_other_block_count(cnt);
  } else if (blocksstable::ObMacroIdIterator::LINKED_BLOCK == type) {
    tmp_ret = mi.get_linked_block_count(cnt);
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid block id array type", KR(ret), K(type));
  }
  if (OB_FAIL(ret)) {
  } else {
    if (OB_SUCCESS != tmp_ret) {
      cnt = -1;
      STORAGE_LOG(WARN, "get block count failed, use -1", KR(tmp_ret), K(name), K(type));
    }
    char cnt_buf[32];
    const int cnt_len = snprintf(cnt_buf, sizeof(cnt_buf), "%ld", static_cast<long>(cnt));
    if (OB_FAIL(append_cstr_grow_(alloc, name, buf, cap, len))) {
      STORAGE_LOG(WARN, "grow array name failed", KR(ret), K(name));
    } else if (OB_FAIL(append_cstr_grow_(alloc, ":{count:", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow count prefix failed", KR(ret), K(name));
    } else if (cnt_len > 0 && OB_FAIL(append_cstr_grow_(alloc, cnt_buf, buf, cap, len))) {
      STORAGE_LOG(WARN, "grow count failed", KR(ret), K(name));
    } else if (OB_FAIL(append_cstr_grow_(alloc, ",ids:[", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow ids open failed", KR(ret), K(name));
    } else {
      blocksstable::ObMacroIdIterator iter;
      tmp_ret = OB_SUCCESS;
      if (blocksstable::ObMacroIdIterator::DATA_BLOCK == type) {
        tmp_ret = mi.get_data_block_iter(iter);
      } else if (blocksstable::ObMacroIdIterator::OTHER_BLOCK == type) {
        tmp_ret = mi.get_other_block_iter(iter);
      } else {
        tmp_ret = mi.get_linked_block_iter(iter);
      }
      if (OB_SUCCESS != tmp_ret) {
        STORAGE_LOG(WARN, "get block iter failed", KR(tmp_ret), K(name), K(type));
      } else {
        bool first = true;
        while (OB_SUCC(ret)) {
          blocksstable::MacroBlockId id;
          const int next_ret = iter.get_next_macro_id(id);
          if (OB_ITER_END == next_ret) {
            break;
          } else if (OB_SUCCESS != next_ret) {
            STORAGE_LOG(WARN, "get_next_macro_id failed", KR(next_ret), K(name));
            break;
          }
          if (!first) {
            if (OB_FAIL(append_cstr_grow_(alloc, ",", buf, cap, len))) {
              STORAGE_LOG(WARN, "grow id comma failed", KR(ret), K(name));
              break;
            }
          }
          first = false;
          if (OB_FAIL(append_obj_to_string_grow_(alloc, id, buf, cap, len))) {
            STORAGE_LOG(WARN, "grow macro id failed", KR(ret), K(name));
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_cstr_grow_(alloc, "]}", buf, cap, len))) {
        STORAGE_LOG(WARN, "grow array close failed", KR(ret), K(name));
      }
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::append_macro_block_ids_(const blocksstable::ObSSTable &sst,
                                                        common::ObArenaAllocator &alloc,
                                                        char *&buf, int64_t &cap, int64_t &len)
{
  int ret = OB_SUCCESS;
  blocksstable::ObSSTableMetaHandle meta_hdl;
  if (OB_FAIL(sst.get_meta(meta_hdl))) {
    // get_meta 失败：忽略错误码，行仍产出。
    STORAGE_LOG(WARN, "get sstable meta for macro block ids failed", KR(ret));
    ret = OB_SUCCESS;
    if (OB_FAIL(append_cstr_grow_(alloc, "{get_meta_failed}", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow get_meta_failed failed", KR(ret));
    }
  } else {
    const blocksstable::ObSSTableMacroInfo &mi = meta_hdl.get_sstable_meta().get_macro_info();
    if (OB_FAIL(append_cstr_grow_(alloc, "{", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow open brace failed", KR(ret));
    } else if (OB_FAIL(append_block_id_array_("data_block_ids",
        blocksstable::ObMacroIdIterator::DATA_BLOCK, mi, alloc, buf, cap, len))) {
      STORAGE_LOG(WARN, "append data_block_ids failed", KR(ret));
    } else if (OB_FAIL(append_cstr_grow_(alloc, ",", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow separator failed", KR(ret));
    } else if (OB_FAIL(append_block_id_array_("other_block_ids",
        blocksstable::ObMacroIdIterator::OTHER_BLOCK, mi, alloc, buf, cap, len))) {
      STORAGE_LOG(WARN, "append other_block_ids failed", KR(ret));
    } else if (OB_FAIL(append_cstr_grow_(alloc, ",", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow separator failed", KR(ret));
    } else if (OB_FAIL(append_block_id_array_("linked_block_ids",
        blocksstable::ObMacroIdIterator::LINKED_BLOCK, mi, alloc, buf, cap, len))) {
      STORAGE_LOG(WARN, "append linked_block_ids failed", KR(ret));
    } else if (OB_FAIL(append_cstr_grow_(alloc, "}", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow close brace failed", KR(ret));
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::emit_sstable_rows_from_table_store_(
    const ObTabletTableStore &ts)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator sstable_iter;
  if (OB_FAIL(ts.get_all_sstable(sstable_iter, false /*need_unpack*/))) {
    STORAGE_LOG(WARN, "get_all_sstable from table store failed", KR(ret));
  } else {
    ObITable *table = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
        STORAGE_LOG(WARN, "sstable iter get_next failed", KR(ret));
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null table from sstable iter", KR(ret), KP(table));
      } else {
        ret = emit_one_sstable_row_(*table);
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          break;
        } else if (OB_FAIL(ret)) {
          STORAGE_LOG(WARN, "emit one sstable row failed", KR(ret), KP(table));
        }
      }
    }
  }
  return ret;
}

// 对 table store 里的单个 sstable 产出一行 "sstable"/"cg sstable"，直接用已反序列化的对象格式化
// get_meta 失败只是该行摘要退化（无 basic_meta），行照常产出。
// co 与非 co 决定对象 + 摘要格式化器 + level/type。
// mode==FULL 时行 = {<sst.to_string()>, macro_block_ids:{...}}，
// 追加段由 append_macro_block_ids_ 生成；DETAIL 经 to_string；SIMPLE 调对应 curated 摘要。
int ObSSTabletMetaInfoIterator::emit_one_sstable_row_(const ObITable &table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table.is_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected non-sstable table", KR(ret), "table_type", table.get_key().table_type_);
  } else {
    const blocksstable::ObSSTable &sst =
        static_cast<const blocksstable::ObSSTable &>(table);
    const bool is_co = sst.is_co_sstable();
    const ObMetaDiskAddr &sst_addr = sst.get_addr();
    bool is_in_table_sub_meta = false;
    if (sst_addr.is_block()) {
      blocksstable::MacroBlockId macro_id;
      if (OB_SUCCESS == sst_addr.get_macro_block_id(macro_id)) {
        is_in_table_sub_meta = macro_id.is_shared_tablet_sub_meta_in_table();
      }
    }
    STORAGE_LOG(DEBUG, "dump_ss_tablet_meta_info emit sstable",
                K(is_co), "table_type", sst.get_key().table_type_,
                "addr", sst_addr, "is_block", sst_addr.is_block(),
                "is_memory", sst_addr.is_memory(),
                "is_in_table_sub_meta", is_in_table_sub_meta);
    const int64_t level = is_co ? 4 : 3;
    const common::ObString type_str = is_co ? ObString::make_string("cg sstable")
                                            : ObString::make_string("sstable");
    if (obrpc::ObDumpSSTabletMetaInfoMode::FULL == mode_) {
      // FULL：行 = {<sst.to_string()>, macro_block_ids:{...}}
      full_scratch_.reuse();
      char *buf = meta_value_buf_;
      int64_t cap = META_VALUE_BUF_LEN;
      int64_t len = 0;
      if (OB_FAIL(append_cstr_grow_(full_scratch_, "{", buf, cap, len))) {
        STORAGE_LOG(WARN, "grow open brace failed", KR(ret));
      } else if (is_co) {
        const ObCOSSTableV2 &co = static_cast<const ObCOSSTableV2 &>(table);
        if (OB_FAIL(append_obj_to_string_grow_(full_scratch_, co, buf, cap, len))) {
          STORAGE_LOG(WARN, "grow cg sstable to_string failed", KR(ret));
        }
      } else if (OB_FAIL(append_obj_to_string_grow_(full_scratch_, sst, buf, cap, len))) {
        STORAGE_LOG(WARN, "grow sstable to_string failed", KR(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append_cstr_grow_(full_scratch_, ",macro_block_ids:", buf, cap, len))) {
          STORAGE_LOG(WARN, "grow macro prefix failed", KR(ret));
        } else if (OB_FAIL(append_macro_block_ids_(sst, full_scratch_, buf, cap, len))) {
          STORAGE_LOG(WARN, "append macro block ids failed", KR(ret));
        } else if (OB_FAIL(append_cstr_grow_(full_scratch_, "}", buf, cap, len))) {
          STORAGE_LOG(WARN, "grow close brace failed", KR(ret));
        } else if (OB_FAIL(add_row_(level, type_str, ObString(len, buf)))) {
          STORAGE_LOG(WARN, "add row failed", KR(ret), K(level), K(type_str));
        }
      }
    } else if (is_co) {
      const ObCOSSTableV2 &co = static_cast<const ObCOSSTableV2 &>(table);
      if (OB_FAIL(format_and_add_row_(level, type_str, co,
                                       format_co_sstable_summary_))) {
        STORAGE_LOG(WARN, "format and add cg sstable row failed", KR(ret));
      }
    } else {
      if (OB_FAIL(format_and_add_row_(level, type_str, sst,
                                       format_sstable_summary_))) {
        STORAGE_LOG(WARN, "format and add sstable row failed", KR(ret));
      }
    }
  }
  return ret;
}

// 从 tablet meta_value 反推 data_version（取自 ObTabletBlockHeader），仅用于 SIMPLE 摘要行。
// 任何一步失败都忽略错误码，避免影响后续继续执行，在本函数里面打印WARN日志即可
void ObSSTabletMetaInfoIterator::get_tablet_data_version_(
    const common::ObString &tablet_meta_value,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  char *tablet_buf = nullptr;
  int64_t tablet_buf_len = 0;
  if (OB_FAIL(ObSSTabletMetaValue::get_tablet_raw_buffer(tablet_meta_value.ptr(),
                                                         tablet_meta_value.length(),
                                                         allocator_,
                                                         tablet_buf, tablet_buf_len))) {
    STORAGE_LOG(WARN, "get tablet raw buffer for data_version failed, use 0",
                KR(ret), K(tablet_meta_value.length()));
  } else {
    ObTabletBlockHeader header;
    int64_t hpos = 0;
    if (OB_FAIL(header.deserialize(tablet_buf, tablet_buf_len, hpos))) {
      STORAGE_LOG(WARN, "deserialize tablet block header for data_version failed, use 0",
                  KR(ret), K(tablet_buf_len));
    } else {
      data_version = header.data_version_;
    }
  }
}

// 把 tablet-meta 行格式化并产出（add_row_）。SIMPLE 调 format_tablet_meta_summary_（单层 {}，已内嵌
// head_info 与 data_version）；DETAIL/FULL 拼成 {<tablet>,<head_info>[,<update_log>]}，外层一个 {}，内含
// 各对象自身 {...} 的 to_string()，逗号分隔。分隔符用单个 ','（不是 "},{"），因为各对象 to_string() 自带括号。
// DETAIL/FULL 经 full_scratch_ 扩容，buffer 可能在 full_scratch_ 内（非 meta_value_buf_），故 add_row_
// 在本函数内用扩容后的指针完成，深拷贝进 str_alloc_。
int ObSSTabletMetaInfoIterator::emit_tablet_meta_row_(
    const ObTablet &tablet,
    const ObSSMetaHeadValueInfo &head_info,
    const uint64_t data_version,
    const ObSSUpdateTabletLog &update_log)
{
  int ret = OB_SUCCESS;
  char *buf = meta_value_buf_;
  int64_t cap = META_VALUE_BUF_LEN;
  int64_t len = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "meta value buf is null", KR(ret));
  } else if (obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE != mode_) {
    full_scratch_.reuse();
    if (OB_FAIL(append_cstr_grow_(full_scratch_, "{", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow open brace failed", KR(ret));
    } else if (OB_FAIL(append_obj_to_string_grow_(full_scratch_, tablet, buf, cap, len))) {
      STORAGE_LOG(WARN, "grow tablet to_string failed", KR(ret));
    } else if (OB_FAIL(append_cstr_grow_(full_scratch_, ",", buf, cap, len))) {
      STORAGE_LOG(WARN, "grow separator failed", KR(ret));
    } else if (OB_FAIL(append_obj_to_string_grow_(full_scratch_, head_info, buf, cap, len))) {
      STORAGE_LOG(WARN, "grow head_info to_string failed", KR(ret));
    } else if (update_log.is_valid()) {
      if (OB_FAIL(append_cstr_grow_(full_scratch_, ",", buf, cap, len))) {
        STORAGE_LOG(WARN, "grow separator failed", KR(ret));
      } else if (OB_FAIL(append_obj_to_string_grow_(full_scratch_, update_log, buf, cap, len))) {
        STORAGE_LOG(WARN, "grow update_log to_string failed", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_cstr_grow_(full_scratch_, "}", buf, cap, len))) {
        STORAGE_LOG(WARN, "grow close brace failed", KR(ret));
      } else if (OB_FAIL(add_row_(1, ObString::make_string("tablet meta"), ObString(len, buf)))) {
        STORAGE_LOG(WARN, "add tablet meta row failed", KR(ret));
      }
    }
  } else {
    len = format_tablet_meta_summary_(tablet, head_info, data_version, buf, cap);
    if (len < 0) {
      len = 0;
    } else if (len >= cap) {
      len = cap - 1;
    }
    buf[len] = '\0';
    if (OB_FAIL(add_row_(1, ObString::make_string("tablet meta"), ObString(len, buf)))) {
      STORAGE_LOG(WARN, "add tablet meta row failed", KR(ret));
    }
  }
  return ret;
}

// 读+反序列化 table-store sub-meta blob，产 "table store" 摘要行，再从同一 ObTabletTableStore 经
// emit_sstable_rows_from_table_store_ 逐个产出 sstable/cg sstable 行。sstable meta 内联在此 blob，
// 无需二次 tiered 读、不用 macro-id/addr 过滤。须在任何 sub_alloc->reuse() 释放本 table store 之前
// 产出 sstable 行（即本函数内一次性完成）。
int ObSSTabletMetaInfoIterator::emit_table_store_sub_meta_(
    const SubMetaEmitCtx &ctx,
    const ObMetaDiskAddr &table_store_addr,
    const ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.sub_alloc)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sub alloc is null", KR(ret));
  } else {
    ctx.sub_alloc->reuse();
    ObString ts_blob;
    if (OB_FAIL(read_sub_meta_blob_(ctx, table_store_addr, ts_blob))) {
      STORAGE_LOG(WARN, "read table store sub-meta failed", KR(ret), K(table_store_addr));
    } else if (!ts_blob.empty()) {
      ObTabletTableStore table_store;
      int64_t ts_pos = 0;
      if (OB_FAIL(table_store.deserialize(*ctx.sub_alloc, tablet, ts_blob.ptr(),
                                          ts_blob.length(), ts_pos))) {
        STORAGE_LOG(WARN, "deserialize table_store failed", KR(ret), K(table_store_addr));
      } else if (OB_FAIL(format_and_add_row_(2, ObString::make_string("table store"),
                                              table_store, format_table_store_summary_))) {
        STORAGE_LOG(WARN, "format and add table store row failed", KR(ret), K(table_store_addr));
      } else if (OB_FAIL(emit_sstable_rows_from_table_store_(table_store))) {
        STORAGE_LOG(WARN, "emit sstable rows from table store failed", KR(ret), K(table_store_addr));
      }
    }
  }
  return ret;
}

// 读+反序列化+格式化 storage schema sub-meta，产 "storage schema" 摘要行。与 emit_table_store_sub_meta_
// 同形（read->deserialize->format->add_row）。
int ObSSTabletMetaInfoIterator::emit_schema_sub_meta_(
    const SubMetaEmitCtx &ctx,
    const ObMetaDiskAddr &schema_addr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.sub_alloc)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sub alloc is null", KR(ret));
  } else {
    ctx.sub_alloc->reuse();
    ObString schema_blob;
    if (OB_FAIL(read_sub_meta_blob_(ctx, schema_addr, schema_blob))) {
      STORAGE_LOG(WARN, "read storage schema sub-meta failed", KR(ret), K(schema_addr));
    } else if (!schema_blob.empty()) {
      ObStorageSchema schema;
      int64_t schema_pos = 0;
      if (OB_FAIL(schema.deserialize(*ctx.sub_alloc, schema_blob.ptr(),
                                     schema_blob.length(), schema_pos))) {
        STORAGE_LOG(WARN, "deserialize storage schema failed", KR(ret), K(schema_addr));
      } else if (OB_FAIL(format_and_add_row_(2, ObString::make_string("storage schema"),
                                              schema, format_storage_schema_summary_))) {
        STORAGE_LOG(WARN, "format and add storage schema row failed", KR(ret), K(schema_addr));
      }
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::collect_and_fill_sub_meta_(
    const common::ObString &tablet_meta_value,
    const ObSSMetaHeadValueInfo &head_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_meta_value.empty())) {
    // 无 tablet meta value，无 sub-meta 可产出
  } else {
    // 从 tablet meta_value 反推 data_version（仅用于 SIMPLE 摘要），失败忽略错误码
    // get_tablet_data_version_函数已经打印WARN日志了
    uint64_t data_version = 0;
    get_tablet_data_version_(tablet_meta_value, data_version);
    ObTabletHandle tablet_handle;
    ObMetaDiskAddr schema_addr;
    ObMetaDiskAddr table_store_addr;
    ObSSUpdateTabletLog update_log;
    if (OB_FAIL(build_tablet_handle_and_sub_meta_addrs_(tablet_meta_value,
                                         tablet_handle,
                                         schema_addr, table_store_addr, update_log))) {
      STORAGE_LOG(WARN, "build tablet handle and sub meta addrs failed",
                  KR(ret), K_(ls_id), K_(tablet_id));
    } else {
      const ObTablet *tablet = tablet_handle.get_obj();
      // build_tablet_handle_and_sub_meta_addrs_ 已保证 tablet 非空（null 时返回 OB_ERR_UNEXPECTED）。
      if (OB_ISNULL(tablet)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet obj is null after build", KR(ret), K_(ls_id), K_(tablet_id));
      } else {
        if (OB_FAIL(emit_tablet_meta_row_(*tablet, head_info, data_version, update_log))) {
          STORAGE_LOG(WARN, "emit tablet meta row failed", KR(ret), K_(ls_id), K_(tablet_id));
        }
      }
      if (OB_SUCC(ret)) {
        ObTieredMetadataStore tiered_store;
        if (OB_FAIL(tiered_store.init(tenant_id_, sql_client_))) {
          STORAGE_LOG(WARN, "fail to init tiered metadata store", KR(ret), K_(tenant_id));
        } else {
          ObArenaAllocator tiered_alloc("SubMetaTiered",
                                        OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(),
                                        ObCtxIds::DEFAULT_CTX_ID);
          ObArenaAllocator sub_alloc("SubMetaDec",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(),
                                     ObCtxIds::DEFAULT_CTX_ID);
          SubMetaEmitCtx ctx;
          ctx.tiered_store = &tiered_store;
          ctx.tiered_alloc = &tiered_alloc;
          ctx.sub_alloc = &sub_alloc;
          if (table_store_addr.is_valid()) {
            // table store 在此一次性读+反序列化，同一份 ObTabletTableStore 既产出 "table store" 摘要行，
            // 又经 emit_sstable_rows_from_table_store_ 直接从中已反序列化的 sstable 对象逐个产出行
            // （sstable meta 内联在此 blob，无需二次 tiered 读）。须在 schema 分支 reuse 释放本 blob 前。
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(emit_table_store_sub_meta_(ctx, table_store_addr, *tablet))) {
              STORAGE_LOG(WARN, "emit table store sub meta failed, continue to schema",
                          KR(tmp_ret), K(table_store_addr));
            }
          }
          if (schema_addr.is_valid()) {
            // 内联读+反序列化+格式化 storage schema sub-meta。与 table store 分支独立：
            // table store 失败不阻断 schema 导出。
            int tmp_ret = OB_SUCCESS;
            // schema sub-meta 读取失败只记日志并吞掉错误，不影响其他已收集的结果。
            if (OB_TMP_FAIL(emit_schema_sub_meta_(ctx, schema_addr))) {
              STORAGE_LOG(WARN, "emit schema sub meta failed", KR(tmp_ret), K(schema_addr));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSSTabletMetaInfoIterator::generate_rows_()
{
  int ret = OB_SUCCESS;
  if (collected_) {
    // 幂等：上次调用已收集过 rows
  } else {
    collected_ = true;

    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                      ObSSMetaReadResultType::READ_WHOLE_ROW,
                                      ObAtomicProtocolTypeEnum::TABLET_META,
                                      ls_id_, tablet_id_,
                                      meta_scn_ /*meta_scn*/,
                                      transfer_scn_ /*reorgan_scn*/,
                                      true /*is_in_table*/);
    read_param.set_read_mark_delete(true);

    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaVersionFilter version_filter;
    ObSqlString prefix;
    ObSSMetaRawRowIter iter;
    ObSSRawMetaRow row;
    if (OB_FAIL(version_filter.set_version_range(meta_scn_, meta_scn_))) {
      STORAGE_LOG(WARN, "set version range failed", KR(ret), K_(meta_scn));
    } else if (OB_UNLIKELY(!read_param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "read param is invalid", KR(ret), K(read_param));
    } else if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
      STORAGE_LOG(WARN, "gen meta key prefix failed", KR(ret), K(read_param));
    } else if (OB_FAIL(iter.init(*sql_client_, MTL_ID(), ls_id_, meta_type,
                                  prefix.string(), version_filter))) {
      STORAGE_LOG(WARN, "init raw row iter failed", KR(ret), K(read_param), K(prefix));
    } else if (OB_FAIL(iter.get_next(allocator_, row))) {
      if (OB_ITER_END == ret) {
        // 指定版本的 tablet meta 在 __all_shared_meta 中不存在。不算错：
        // 迭代器保持空，get_next 返回 OB_ITER_END。
        ret = OB_SUCCESS;
      } else {
        STORAGE_LOG(WARN, "get next raw row from __all_shared_meta failed", KR(ret));
      }
    } else {
      ObSSMetaHeadValue head_value;
      int64_t pos = 0;
      if (OB_FAIL(head_value.deserialize_value_info(row.meta_value_.ptr(),
                                                    row.meta_value_.length(), pos))) {
        const int tmp_ret = ret;
        pos = 0;
        ret = OB_SUCCESS;
        if (OB_FAIL(head_value.deserialize(row.meta_value_.ptr(),
                                           row.meta_value_.length(), pos))) {
          STORAGE_LOG(WARN, "deserialize tablet meta head value failed",
                     KR(ret), K(tmp_ret), K(row.meta_value_.length()));
        }
      }
      if (OB_SUCC(ret)) {
        const ObSSMetaHeadValueInfo &head_info = head_value.get_meta_head_info();
        if (OB_FAIL(collect_and_fill_sub_meta_(row.meta_value_, head_info))) {
          STORAGE_LOG(WARN, "collect and fill sub meta failed",
                     KR(ret), K_(tenant_id), K_(ls_id), K_(tablet_id));
        }
      }
    }

    if (OB_TENANT_NOT_IN_SERVER == ret) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

} // storage
} // oceanbase
