/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#define USING_LOG_PREFIX STORAGE

#include "object/ob_object.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/access/ob_table_read_info.h"
#include "ob_lob_context.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/lob/common/ob_lob_location.h"
#include "storage/tx_storage/ob_access_service.h"
#include "observer/ob_server.h"
#include "storage/ob_dml_running_ctx.h"

namespace oceanbase
{
namespace storage
{

//==================== ObLobContextBase ====================
ObLobContextBase::ObLobContextBase()
{
  reset();
}

int ObLobContextBase::assign_common(const ObLobContextBase &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    allocator_ = other.allocator_;
    sql_mode_ = other.sql_mode_;
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = other.lob_piece_tablet_id_;
    coll_type_ = other.coll_type_;
    src_loc_ = other.src_loc_;
    byte_size_ = other.byte_size_;
    timeout_ = other.timeout_;
    fb_snapshot_ = other.fb_snapshot_;
    offset_ = other.offset_;
    len_ = other.len_;
    read_latest_ = other.read_latest_;
    need_read_latest_ = other.need_read_latest_;
    store_column_idx_ = other.store_column_idx_;
    addr_ = other.addr_;
    src_loc_wrapper_.reset();
    if (OB_FAIL(snapshot_.assign(other.snapshot_))) {
      LOG_WARN("assign snapshot fail", K(ret), K(other));
    } else if (other.src_loc_wrapper_.is_valid()
               && OB_FAIL(init_src_loc_wrapper_())) {
      LOG_WARN("init src loc wrapper fail", K(ret), K(other), KPC(this));
    }
  }
  return ret;
}

void ObLobContextBase::reset()
{
  allocator_ = nullptr,
  snapshot_.reset();
  sql_mode_ = SMO_DEFAULT;
  tenant_id_ = MTL_ID();
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
  lob_meta_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID,
  lob_piece_tablet_id_ = common::ObTabletID::INVALID_TABLET_ID,
  coll_type_ = CS_TYPE_INVALID,
  src_loc_.reset();
  byte_size_ = UINT64_MAX,
  timeout_ = 0,
  fb_snapshot_ = share::SCN::invalid_scn(),
  offset_ = 0,
  len_ = 0,
  read_latest_ = false,
  need_read_latest_ = false,
  src_loc_wrapper_.reset();
  store_column_idx_ = 0,
  addr_.reset();
}

int ObLobContextBase::init_src_loc_wrapper_()
{
  int ret = OB_SUCCESS;
  ObString disk_loc_buff;
  src_loc_wrapper_.reset();
  if (0 == src_loc_.size_) {
    // empty lob on insert has no disk locator yet
  } else if (!src_loc_.is_valid()) {
    LOG_WARN("invalid src loc", KR(ret));
  } else if (src_loc_.is_external_temp_lob()) {
    // TODO by renhe: remove this silent non-op, expose it outside
    // Temporary External has no Disk locator; first insert gets references from the Mem descriptor.
  } else if (OB_FAIL(src_loc_.get_disk_locator(disk_loc_buff))) {
    LOG_WARN("failed to get disk locator", K(ret), K(src_loc_));
  } else if (OB_FAIL(src_loc_wrapper_.init(disk_loc_buff.ptr(),
                                           disk_loc_buff.length()))) {
    LOG_WARN("failed to init src loc wrapper", K(ret), K(src_loc_), K(disk_loc_buff));
  }
  return ret;
}

//==================== ObLobReadContext ====================

ObLobReadContext::ObLobReadContext()
  : iter_allocator_("LobIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
  reset();
}

void ObLobReadContext::reset()
{
  scan_backward_ = false;
  from_rpc_ = false;
  inrow_read_nocopy_ = false;
  enable_remote_retry_ = false;
  is_full_table_scan_ = false;
  access_ctx_ = nullptr;
  seq_id_start_.reset();
  seq_id_end_.reset();
  ObLobContextBase::reset();
}

ObLobReadContext::~ObLobReadContext()
{
}

int ObLobReadContext::build_for_dml(
    const ObDMLRunningCtx &run_ctx,
    const int16_t col_idx,
    const blocksstable::ObStorageDatum &datum,
    const bool allow_none_read_snapshot)
{
  int ret = OB_SUCCESS;
  const ObColDesc *column = nullptr;
  const ObTableDMLParam *table_param = run_ctx.effective_table_param_;
  transaction::ObTxDesc *tx_desc = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_;
  ObLobLocatorV2 disk_locator;
  ObLobLocatorV2 mem_locator;
  ObMemLobLocatorBuilder locator_builder;
  transaction::ObTxReadSnapshot read_snapshot;
  int32_t col_offset = OB_INVALID_INDEX;
  int32_t store_column_idx = OB_INVALID_INDEX;
  ObCollationType coll_type = CS_TYPE_INVALID;
  const uint64_t table_id = run_ctx.relative_table_.get_table_id();
  const ObTabletID tablet_id = run_ctx.relative_table_.get_tablet_id();
  const ObLSID ls_id = run_ctx.store_ctx_.ls_id_;

  if (OB_ISNULL(run_ctx.col_descs_)
      || OB_ISNULL(table_param)
      || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dml runtime context for lob read", KR(ret),
        KP(run_ctx.col_descs_), KP(table_param), KP(tx_desc), K(col_idx));
  } else if (OB_UNLIKELY((col_idx < 0)
      || (col_idx >= run_ctx.col_descs_->count())
      || datum.is_null()
      || datum.is_nop_value()
      || (OB_INVALID_ID == table_id)
      || !tablet_id.is_valid()
      || !ls_id.is_valid()
      || (run_ctx.dml_param_.timeout_ <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for building dml lob read context", KR(ret),
        K(col_idx), K(datum), K(table_id), K(tablet_id), K(ls_id),
        "timeout", run_ctx.dml_param_.timeout_);
  } else if (FALSE_IT(column = &run_ctx.col_descs_->at(col_idx))) {
  } else if (OB_UNLIKELY(!column->col_type_.is_lob_storage())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column is not lob storage", KR(ret), K(col_idx), KPC(column));
  } else if (FALSE_IT(datum.get_lob_locatorv2(disk_locator))) {
  } else if (OB_UNLIKELY(!disk_locator.is_valid()
      || !disk_locator.is_outline_lob_disk_locator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("datum is not a valid outline disk lob locator", KR(ret),
        K(col_idx), K(datum), K(disk_locator));
  } else {
    const share::schema::ObTableSchemaParam &data_table =
        table_param->get_data_table();
    const ObTableReadInfo &read_info = data_table.get_read_info();
    coll_type = ObLobCharsetUtil::get_collation_type(
        column->col_type_.get_type(), column->col_type_.get_collation_type());
    if (OB_UNLIKELY(!ObCharset::is_valid_collation(coll_type))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob collation type", KR(ret), K(col_idx), K(coll_type),
          KPC(column));
    } else if (OB_FAIL(data_table.get_col_map().get(column->col_id_, col_offset))) {
      LOG_WARN("get lob column offset failed", KR(ret), K(column->col_id_));
    } else if (OB_UNLIKELY((col_offset < 0)
        || (col_offset >= read_info.get_columns_index().count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob column offset", KR(ret), K(col_offset),
          K(column->col_id_), "column_count", read_info.get_columns_index().count());
    } else if (FALSE_IT(store_column_idx =
        read_info.get_columns_index().at(col_offset))) {
    } else if (OB_UNLIKELY((OB_INVALID_INDEX == store_column_idx)
        || (store_column_idx < 0)
        || (store_column_idx > UINT16_MAX))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob storage column index", KR(ret), K(col_offset),
          K(store_column_idx), K(column->col_id_));
    } else if (OB_FAIL(read_snapshot.assign(run_ctx.dml_param_.snapshot_))) {
      LOG_WARN("assign dml lob read snapshot failed", KR(ret),
          K(run_ctx.dml_param_.snapshot_));
    } else if (OB_UNLIKELY(read_snapshot.is_none_read() && !allow_none_read_snapshot)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("none read snapshot is not allowed for dml lob read", KR(ret),
          K(allow_none_read_snapshot), K(read_snapshot), K(col_idx), K(table_id), K(ls_id));
    } else {
      // 普通 INSERT 不需要先扫描旧行来产生待写入数据，因此符合条件时可以用 none_read，省掉提前获取全局快照的开销。
      // 进入存储层写入时才获取目标 LS 的本地快照，并存入 store_ctx_.mvcc_acc_ctx_.snapshot_。
      // 因此，这里遇到 none_read 时，将 LS 的本地快照设置到 read_snapshot 中。
      if (read_snapshot.is_none_read()) {
        read_snapshot.init_ls_read(run_ctx.store_ctx_.ls_id_,
                                   run_ctx.store_ctx_.mvcc_acc_ctx_.snapshot_);
      }
      if (OB_FAIL(locator_builder.init(
          run_ctx.dml_param_.lob_allocator_, datum.get_string(), false/*need_data*/))) {
        LOG_WARN("init dml lob locator builder failed", KR(ret), K(col_idx), K(datum));
      } else if (OB_FAIL(locator_builder.set_extern_info(
          table_id, store_column_idx, coll_type))) {
        LOG_WARN("set dml lob locator extern info failed", KR(ret),
            K(table_id), K(store_column_idx), K(coll_type));
      } else if (OB_FAIL(locator_builder.set_location_info(
          tablet_id.id(), ls_id.id(), MYADDR))) {
        LOG_WARN("set dml lob locator location failed", KR(ret),
            K(tablet_id), K(ls_id), "self_addr", MYADDR);
      } else if (OB_FAIL(locator_builder.set_retry_info(
          true/*select_leader*/, true/*read_latest*/,
          run_ctx.dml_param_.timeout_))) {
        LOG_WARN("set dml lob locator retry info failed", KR(ret),
            "timeout", run_ctx.dml_param_.timeout_);
      } else if (OB_FAIL(locator_builder.set_read_snapshot(
          read_snapshot, ls_id.id(), share::SCN::invalid_scn(),
          tx_desc->get_seq_base(), true/*read_latest*/))) {
        LOG_WARN("set dml lob locator read snapshot failed", KR(ret),
            K(read_snapshot), K(ls_id), "tx_seq_base", tx_desc->get_seq_base());
      } else if (OB_FAIL(locator_builder.build(mem_locator))) {
        LOG_WARN("build dml mem lob locator failed", KR(ret), K(locator_builder));
      } else if (OB_FAIL(build(
          mem_locator, run_ctx.dml_param_.lob_allocator_,
          0/*offset*/, UINT64_MAX/*len*/, tx_desc))) {
        LOG_WARN("build dml lob read context failed", KR(ret), K(mem_locator));
      } else {
        sql_mode_ = run_ctx.dml_param_.sql_mode_;
        tenant_id_ = MTL_ID();
        byte_size_ = UINT64_MAX;
        need_read_latest_ = true;
      }
    }
  }
  return ret;
}

int ObLobReadContext::build(
    common::ObLobLocatorV2 &lob_locator,
    common::ObIAllocator &allocator,
    uint64_t offset,
    uint64_t len,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  src_loc_ = lob_locator;
  byte_size_ = UINT64_MAX;
  allocator_ = &allocator;
  offset_ = offset;
  len_ = len;
  if (lob_locator.is_persist_lob() && !lob_locator.has_inrow_data()) {
    ObMemLobCommon *mem_loc = nullptr;
    ObMemLobLocationInfo *location_info = nullptr;
    ObMemLobExternHeader *extern_header = nullptr;
    ObMemLobRetryInfo *retry_info = nullptr;
    bool is_remote = false;
    if (OB_FAIL(lob_locator.get_mem_locator(mem_loc))) {
      LOG_WARN("failed to get mem locator", K(ret), K(lob_locator));
    } else if (OB_FAIL(lob_locator.get_extern_header(extern_header))) {
      LOG_WARN("failed to get extern header", K(ret), K(lob_locator));
    } else if (OB_FAIL(lob_locator.get_location_info(location_info))) {
      LOG_WARN("failed to get location info", K(ret), K(lob_locator));
    } else if (OB_FAIL(lob_locator.get_retry_info(retry_info))) {
      LOG_WARN("failed to get retry info", K(ret), K(lob_locator));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(set_tx_read_snapshot_(lob_locator, tx_desc))) {
        LOG_WARN("set tx read snapshot fail", K(ret), KPC(this), K(lob_locator));
      } else {
        coll_type_ = extern_header->cs_type_;
        ls_id_ = share::ObLSID(location_info->ls_id_);
        tablet_id_ = ObTabletID(location_info->tablet_id_);
        store_column_idx_ = extern_header->column_idx_;
        timeout_ = retry_info->timeout_;
        if (OB_FAIL(ObLobLocationUtil::is_remote(*this, is_remote, addr_))) {
          LOG_WARN("get lob addr fail", K(ret), KPC(this), K(lob_locator));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !lob_locator.has_inrow_data()
      && OB_FAIL(init_src_loc_wrapper_())) {
    LOG_WARN("failed to init src loc wrapper", K(ret), K(lob_locator));
  }
  return ret;
}

int ObLobReadContext::assign(const ObLobReadContext &read_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign_common(read_ctx))) {
    LOG_WARN("assign common lob context fail", K(ret), K(read_ctx));
  } else if (this != &read_ctx) {
    scan_backward_ = read_ctx.scan_backward_;
    from_rpc_ = read_ctx.from_rpc_;
    inrow_read_nocopy_ = read_ctx.inrow_read_nocopy_;
    enable_remote_retry_ = read_ctx.enable_remote_retry_;
    is_full_table_scan_ = read_ctx.is_full_table_scan_;
    access_ctx_ = read_ctx.access_ctx_;
    seq_id_start_ = read_ctx.seq_id_start_;
    seq_id_end_ = read_ctx.seq_id_end_;
  }
  return ret;
}

bool ObLobContextBase::is_across_tenant() const
{
  return MTL_ID() != tenant_id_;
}


bool ObLobReadContext::is_remote() const
{
  return (!from_rpc_ || enable_remote_retry_)
      && addr_.is_valid()
      && (GCONF.self_addr_ != addr_ || is_across_tenant());
}

bool ObLobReadContext::enable_block_cache() const
{
  bool res = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    res = byte_size_ <= tenant_config->lob_enable_block_cache_threshold;
  }
  return res;
}

int ObLobReadContext::get_tx_read_snapshot_(
    ObLobLocatorV2 &locator,
    transaction::ObTxReadSnapshot &read_snapshot)
{
  int ret = OB_SUCCESS;
  ObMemLobExternHeader *extern_header = nullptr;
  if (!locator.is_persist_lob() || locator.has_inrow_data()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not outrow persist lob", K(ret), K(locator));
  } else if (OB_FAIL(locator.get_extern_header(extern_header))) {
    LOG_WARN("failed to get extern header", K(ret), K(locator));
  } else if (extern_header->has_read_snapshot_) {
    ObString read_snapshot_data;
    int64_t read_snapshot_data_pos = 0;
    if (OB_FAIL(locator.get_read_snapshot_data(read_snapshot_data))) {
      LOG_WARN("failed to get read_snapshot_data", K(ret), K(locator));
    } else if (OB_FAIL(read_snapshot.deserialize_for_lob(fb_snapshot_,
        read_snapshot_data.ptr(), read_snapshot_data.length(), read_snapshot_data_pos))) {
      LOG_WARN("failed to deserialize read_snapshot_data", K(ret), K(locator));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx info not found", K(ret), K(locator));
  }

  if (OB_SUCC(ret) && extern_header->has_retry_info_) {
    ObMemLobRetryInfo *retry_info = nullptr;
    if (OB_FAIL(locator.get_retry_info(retry_info))) {
      LOG_WARN("failed to get retry info", K(ret), K(locator));
    } else if (retry_info->read_latest_) {
      read_latest_ = true;
    }
  }
  return ret;
}

int ObLobReadContext::set_tx_read_snapshot_(
    ObLobLocatorV2 &locator,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  transaction::ObTxReadSnapshot read_snapshot;
  if (OB_FAIL(get_tx_read_snapshot_(locator, read_snapshot))) {
    LOG_WARN("get tx read snapshot fail", K(ret), KPC(this), K(locator));
  } else if (OB_ISNULL(tx_desc)
      || tx_desc->get_tx_id() == read_snapshot.tx_id()
      || read_snapshot.is_not_in_tx_snapshot()) {
    if (OB_FAIL(snapshot_.assign(read_snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret), K(read_snapshot));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("it is not support for reading lob after transaction commit",
             K(ret), K(locator), K(read_snapshot));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "it is not support for reading lob after transaction commit, please re-select lob locator");
  }
  return ret;
}

//==================== ObLobWriteContext ====================
ObLobWriteContext::ObLobWriteContext()
  : ObLobContextBase(),
    own_allocator_(),
    dml_op_(ObLobDmlOp::INVALID),
    tx_desc_(nullptr),
    dml_base_meta_param_(nullptr),
    dml_base_piece_param_(nullptr),
    dest_loc_(),
    parent_seq_no_(),
    insert_lob_seq_no_(),
    update_lob_seq_no_(),
    delete_lob_seq_no_(),
    is_total_quantity_log_(true),
    is_mlog_(false),
    try_flush_redo_(false),
    is_index_table_(false),
    table_def_type_(share::schema::TABLE_DEF_TYPE_MAX),
    is_table_without_pk_(false),
    inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
    intable_threshold_(OB_DEFAULT_LOB_INTABLE_THRESHOLD),
    data_row_(nullptr),
    outtable_lob_control_keys_(nullptr),
    allow_outtable_lob_(false)
{
  ObMemAttr attr(MTL_ID(), "LobOwn");
  own_allocator_.set_attr(attr);
}

ObLobWriteContext::~ObLobWriteContext()
{
  if (OB_NOT_NULL(dml_base_meta_param_)) {
    if (OB_NOT_NULL(dml_base_meta_param_->store_ctx_guard_)) {
      dml_base_meta_param_->store_ctx_guard_->~ObStoreCtxGuard();
    }
    dml_base_meta_param_->~ObDMLBaseParam();
  }
  if (OB_NOT_NULL(dml_base_piece_param_)) {
    if (OB_NOT_NULL(dml_base_piece_param_->store_ctx_guard_)) {
      dml_base_piece_param_->store_ctx_guard_->~ObStoreCtxGuard();
    }
    dml_base_piece_param_->~ObDMLBaseParam();
  }
}

int ObLobWriteContext::assign(const ObLobWriteContext &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign_common(other))) {
    LOG_WARN("assign common lob context fail", K(ret), K(other));
  } else if (this != &other) {
    // own_allocator_ is not copied
    dml_op_ = other.dml_op_;
    tx_desc_ = other.tx_desc_;
    dml_base_meta_param_ = nullptr;
    dml_base_piece_param_ = nullptr;
    dest_loc_ = other.dest_loc_;
    parent_seq_no_ = other.parent_seq_no_;
    insert_lob_seq_no_ = other.insert_lob_seq_no_;
    update_lob_seq_no_ = other.update_lob_seq_no_;
    delete_lob_seq_no_ = other.delete_lob_seq_no_;
    is_total_quantity_log_ = other.is_total_quantity_log_;
    is_mlog_ = other.is_mlog_;
    try_flush_redo_ = other.try_flush_redo_;
    is_index_table_ = other.is_index_table_;
    table_def_type_ = other.table_def_type_;
    is_table_without_pk_ = other.is_table_without_pk_;
    inrow_threshold_ = other.inrow_threshold_;
    intable_threshold_ = other.intable_threshold_;
    data_row_ = other.data_row_;
    outtable_lob_control_keys_ = other.outtable_lob_control_keys_;
    allow_outtable_lob_ = other.allow_outtable_lob_;
  }
  return ret;
}

bool ObLobWriteContext::is_valid()
{
  return nullptr != allocator_;
}

int ObLobWriteContext::assign_source(const ObLobReadContext &source)
{
  int ret = OB_SUCCESS;
  allocator_ = source.allocator_;
  ls_id_ = source.ls_id_;
  tablet_id_ = source.tablet_id_;
  lob_meta_tablet_id_ = source.lob_meta_tablet_id_;
  lob_piece_tablet_id_ = source.lob_piece_tablet_id_;
  coll_type_ = source.coll_type_;
  src_loc_ = source.src_loc_;
  byte_size_ = source.byte_size_;
  timeout_ = source.timeout_;
  fb_snapshot_ = source.fb_snapshot_;
  offset_ = source.offset_;
  len_ = source.len_;
  read_latest_ = source.read_latest_;
  store_column_idx_ = source.store_column_idx_;
  addr_ = source.addr_;
  if (OB_FAIL(snapshot_.assign(source.snapshot_))) {
    LOG_WARN("assign snapshot fail", K(ret), K(source));
  } else if (source.src_loc_wrapper_.is_valid()
             && OB_FAIL(init_src_loc_wrapper_())) {
    LOG_WARN("init src loc wrapper fail", K(ret), K(source), KPC(this));
  }
  return ret;
}

int64_t ObLobWriteContext::get_chunk_size(const bool is_outtable) const
{
  return is_outtable ? ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE : ObLobMetaUtil::LOB_PIECE_DATA_SIZE;
}

int64_t ObLobWriteContext::get_inrow_threshold() const
{
  int64_t res = inrow_threshold_;
  if (res < OB_MIN_LOB_INROW_THRESHOLD || res > OB_MAX_LOB_INROW_THRESHOLD) {
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid inrow threshold, use default inrow threshold", K(res));
    res = OB_DEFAULT_LOB_INROW_THRESHOLD;
  }
  return res;
}

int64_t ObLobWriteContext::get_intable_threshold() const
{
  int64_t res = intable_threshold_;
  if (res < inrow_threshold_ || res < OB_MIN_LOB_INTABLE_THRESHOLD || res > OB_MAX_LOB_INTABLE_THRESHOLD) {
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED,
                "invalid intable threshold, use default intable threshold", K(res), K(inrow_threshold_));
    res = OB_DEFAULT_LOB_INTABLE_THRESHOLD;
  }
  return res;
}

bool ObLobWriteContext::can_write_outtable() const
{
  // Unknown definition types must not enable OutTable writes.
  return allow_outtable_lob_ && common::is_user_tenant(tenant_id_)
         && !tablet_id_.is_inner_tablet()
         && share::schema::TABLE_DEF_TYPE_USER == table_def_type_;
}

int ObLobWriteContext::is_timeout() const
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();
  if (cur_time > timeout_) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout", K(ret), K(cur_time), K(timeout_));
  }
  return ret;
}

int ObLobWriteContext::build(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const ObLobLocatorV2 &src_loc)
{
  int ret = OB_SUCCESS;
  const ObColDesc &column = run_ctx.col_descs_->at(col_idx);
  src_loc_ = src_loc;
  tx_desc_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_;
  parent_seq_no_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_;
  is_mlog_ = run_ctx.effective_table_param_->get_data_table().is_mlog_table();

  sql_mode_ = run_ctx.dml_param_.sql_mode_;
  is_total_quantity_log_ = run_ctx.dml_param_.is_total_quantity_log_;
  ls_id_ = run_ctx.store_ctx_.ls_id_;
  tablet_id_ = run_ctx.relative_table_.get_tablet_id();
  coll_type_ = ObLobCharsetUtil::get_collation_type(column.col_type_.get_type(), column.col_type_.get_collation_type());
  allocator_ = &run_ctx.dml_param_.lob_allocator_;
  timeout_ = run_ctx.dml_param_.timeout_;
  offset_ = 0;
  data_row_ = &data_row;
  is_index_table_ = run_ctx.relative_table_.is_index_table();
  outtable_lob_control_keys_ = run_ctx.dml_param_.outtable_lob_control_keys_;
  allow_outtable_lob_ = run_ctx.dml_param_.allow_outtable_lob_;

  // get inrow threshold and schema chunk size
  const ObTableDMLParam *table_param = nullptr;
  const ObColumnParam *column_param = nullptr;
  int32_t col_offset = OB_INVALID_INDEX;
  // External requires EXTBLOB; locator pass-through conversions must check the target column.
  // This write guard relies on External sources arriving as flagged Mem locators;
  // it cannot reject bare External Disk locators.
  if (OB_UNLIKELY(!src_loc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to check source lob locator", KR(ret), K(tablet_id_), K(column.col_id_),
             "locator_size", src_loc_.size_);
  } else if (OB_UNLIKELY(src_loc_.is_mem_external_lob() && !column.col_type_.is_extblob())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "assigning an external LOB to a non-EXTBLOB column");
    LOG_WARN("external lob requires an EXTBLOB column", KR(ret),
             K(tablet_id_), K(column.col_id_), K(column.col_type_));
  } else if (OB_ISNULL(table_param = run_ctx.effective_table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_param is null", KR(ret), KPC(this));
  } else if (OB_ISNULL(column_param = table_param->get_data_table().get_column(column.col_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", KR(ret));
  } else {
    inrow_threshold_ = table_param->get_data_table().get_lob_inrow_threshold();
    intable_threshold_ = table_param->get_data_table().get_lob_intable_threshold();
    is_table_without_pk_ = table_param->get_data_table().is_table_without_pk();

    const share::schema::ObTableSchemaParam &data_table = table_param->get_data_table();
    table_def_type_ = data_table.get_def_type();
    const storage::ObTableReadInfo &read_info = data_table.get_read_info();
    if (OB_FAIL(data_table.get_col_map().get(column.col_id_, col_offset))) {
      LOG_WARN("get param idx by column_id fail", KR(ret), K(column.col_id_));
    } else if (col_offset < 0 || col_offset >= read_info.get_columns_index().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param idx", KR(ret), K(col_offset), K(column.col_id_));
    } else if (FALSE_IT(store_column_idx_ = read_info.get_columns_index().at(col_offset))) {
    } else if (OB_UNLIKELY(store_column_idx_ == OB_INVALID_INDEX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid store_column_idx", KR(ret), K(col_offset), K(store_column_idx_));
    }
  }

  // get snapshot
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(snapshot_.assign(run_ctx.dml_param_.snapshot_))) {
    LOG_WARN("assign snapshot fail", K(ret), K(run_ctx.dml_param_.snapshot_));
  } else if (snapshot_.is_none_read()) {
    // NOTE:
    // lob_insert need table_scan, the snapshot already generated in
    // run_ctx.store_ctx, use it as an LS ReadSnapshot
    snapshot_.init_ls_read(run_ctx.store_ctx_.ls_id_,
                                      run_ctx.store_ctx_.mvcc_acc_ctx_.snapshot_);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_src_loc_wrapper_())) {
    LOG_WARN("failed init src_loc_wrapper", KR(ret), K(src_loc_));
  }
  return ret;
}

int ObLobWriteContext::build_for_dml(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;

  datum.get_lob_locatorv2(src_loc_);

  if (OB_UNLIKELY(src_loc_.is_append_delta_lob())) {
    // LOB_WRITEAPPEND 的 delta locator 只允许走 ObLobAppendContext 路径，
    // insert/update/delete 路径不应接受到 append delta lob。
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_WRITEAPPEND in this DML path is");
    STORAGE_LOG(WARN, "append delta lob is not supported in this dml path",
                KR(ret), K(data_row), K(col_idx), K(datum));
  } else if (OB_UNLIKELY(!src_loc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "input is not a inrow disk locator", KR(ret), K(data_row), K(col_idx), K(datum));
  } else if (OB_FAIL(build(run_ctx, data_row, col_idx, src_loc_))) {
    LOG_WARN("failed build lob param", KR(ret), K(data_row), K(col_idx), K(datum));
  }
  return ret;
}

bool ObLobWriteContext::is_remote() const
{
  return addr_.is_valid() && (GCONF.self_addr_ != addr_ || is_across_tenant());
}
int ObLobWriteContext::update_seq_no(const LobAuxTarget target)
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam *dml = nullptr;
  if (OB_FAIL(get_aux_dml_base_param(target, dml))) {
    LOG_WARN("get_aux_dml_base_param fail", K(ret), KPC(this));
  } else if (OB_ISNULL(dml)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml base param is null", K(ret), KPC(this));
  } else {
    dml->spec_seq_no_.reset();
    // 普通行外 LOB 更新会被拆分为删除和插入。LOB_WRITEAPPEND 还会通过更新操作
    // 填充尾部数据分片及元信息，或更新摘要。追加写使用新 Locator 携带的同一个
    // seq_no 初始化插入和更新两类写入序号。
    // 查询不需要 seq_no，因此无需处理。
    if (dml_op_ == ObLobDmlOp::INSERT) {
      dml->spec_seq_no_ = insert_lob_seq_no_;
    } else if (dml_op_ == ObLobDmlOp::UPDATE) {
      dml->spec_seq_no_ = update_lob_seq_no_;
    } else if (dml_op_ == ObLobDmlOp::DELETE) {
      dml->spec_seq_no_ = delete_lob_seq_no_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid dml_op_", KR(ret), K(dml_op_), K(lbt()));
    }
  }
  return ret;
}

int ObLobWriteContext::get_aux_dml_base_param(
    const LobAuxTarget target,
    ObDMLBaseParam *&base_param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    base_param = dml_base_meta_param_;
  } else if (LobAuxTarget::PIECE == target) {
    base_param = dml_base_piece_param_;
  }
  return ret;
}

int ObLobWriteContext::set_aux_dml_base_param(
    const LobAuxTarget target,
    ObDMLBaseParam *value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    dml_base_meta_param_ = value;
  } else if (LobAuxTarget::PIECE == target) {
    dml_base_piece_param_ = value;
  }
  return ret;
}

int ObLobWriteContext::get_trans_ctx(transaction::ObPartTransCtx *&tx_ctx) const
{
  int ret = OB_SUCCESS;
  ObDMLBaseParam *meta_dml = nullptr;
  tx_ctx = nullptr;
  if (OB_FAIL(get_aux_dml_base_param(LobAuxTarget::META, meta_dml))) {
    LOG_WARN("failed to get lob meta dml param", KR(ret), KPC(this));
  } else if (OB_ISNULL(meta_dml) || OB_ISNULL(meta_dml->store_ctx_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob meta store context is null", KR(ret), KP(meta_dml), KPC(this));
  } else if (OB_ISNULL(tx_ctx =
      meta_dml->store_ctx_guard_->get_store_ctx().mvcc_acc_ctx_.tx_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transaction context is null", KR(ret), KPC(meta_dml), KPC(this));
  }
  return ret;
}

int ObLobWriteContext::get_aux_tablet_id(
    const LobAuxTarget target,
    common::ObTabletID &tablet_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    tablet_id = lob_meta_tablet_id_;
  } else if (LobAuxTarget::PIECE == target) {
    tablet_id = lob_piece_tablet_id_;
  }
  return ret;
}

int ObLobWriteContext::get_aux_column_cnt(
    const LobAuxTarget target,
    uint64_t &column_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    column_cnt = ObLobMetaUtil::LOB_META_COLUMN_CNT;
  } else if (LobAuxTarget::PIECE == target) {
    column_cnt = ObLobPieceUtil::LOB_PIECE_COLUMN_CNT;
  }
  return ret;
}

int ObLobWriteContext::get_aux_rowkey_col_cnt(
    const LobAuxTarget target,
    uint64_t &rowkey_col_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_lob_aux_target(target))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux target", KR(ret), "target", get_lob_aux_target_str(target));
  } else if (LobAuxTarget::META == target) {
    rowkey_col_cnt = ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT;
  } else if (LobAuxTarget::PIECE == target) {
    rowkey_col_cnt = ObLobPieceUtil::LOB_PIECE_SCHEMA_ROWKEY_COL_CNT;
  }
  return ret;
}

//==================== ObLobInsertContext ====================
int ObLobInsertContext::build(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const ObLobLocatorV2 &src_loc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobWriteContext::build(run_ctx, data_row, col_idx, src_loc))) {
    LOG_WARN("failed build access param for dml", KR(ret), K(data_row), K(col_idx), K(src_loc));
  } else {
    dml_op_ = ObLobDmlOp::INSERT;
  }
  return ret;
}

int ObLobInsertContext::build_for_dml(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobWriteContext::build_for_dml(run_ctx, data_row, col_idx, datum))) {
    LOG_WARN("failed build access param for dml", KR(ret), K(data_row), K(col_idx), K(datum));
  } else {
    dml_op_ = ObLobDmlOp::INSERT;
  }
  return ret;
}

//==================== ObLobDeleteContext ====================
int ObLobDeleteContext::build_for_dml(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobWriteContext::build_for_dml(run_ctx, data_row, col_idx, datum))) {
    LOG_WARN("failed build access param for dml", KR(ret), K(data_row), K(col_idx), K(datum));
  } else if (src_loc_wrapper_.is_inrow()) {
    // do nothing
  } else {
    dml_op_ = ObLobDmlOp::DELETE;
    // 同一条 DML 可能先写入再删除 LOB meta，需要读取事务内最新数据。
    need_read_latest_ = true;
    if (run_ctx.is_delete_insert_table_) {
      delete_lob_seq_no_ = parent_seq_no_ - 1;
    } else {
      delete_lob_seq_no_ = parent_seq_no_;
    }
  }
  return ret;
}

int ObLobDeleteContext::build(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const ObLobLocatorV2 &src_loc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobWriteContext::build(run_ctx, data_row, col_idx, src_loc))) {
    LOG_WARN("failed build access param for dml", KR(ret), K(data_row), K(col_idx), K(src_loc));
  } else if (src_loc_wrapper_.is_inrow()) {
    // do nothing
  } else {
    dml_op_ = ObLobDmlOp::DELETE;
    // 同一条 DML 可能先写入再删除 LOB meta，需要读取事务内最新数据。
    need_read_latest_ = true;
    if (run_ctx.is_delete_insert_table_) {
      delete_lob_seq_no_ = parent_seq_no_ - 1;
    } else {
      delete_lob_seq_no_ = parent_seq_no_;
    }
  }
  return ret;
}

int ObLobWriteContext::build_read_context(ObLobReadContext &read_ctx) const
{
  int ret = OB_SUCCESS;
  read_ctx.allocator_ = allocator_;
  read_ctx.sql_mode_ = sql_mode_;
  read_ctx.tenant_id_ = tenant_id_;
  read_ctx.ls_id_ = ls_id_;
  read_ctx.tablet_id_ = tablet_id_;
  read_ctx.lob_meta_tablet_id_ = lob_meta_tablet_id_;
  read_ctx.lob_piece_tablet_id_ = lob_piece_tablet_id_;
  read_ctx.coll_type_ = coll_type_;
  read_ctx.src_loc_ = src_loc_;
  read_ctx.src_loc_wrapper_ = src_loc_wrapper_;
  read_ctx.byte_size_ = byte_size_;
  read_ctx.timeout_ = timeout_;
  read_ctx.fb_snapshot_ = fb_snapshot_;
  read_ctx.offset_ = offset_;
  read_ctx.len_ = len_;
  read_ctx.read_latest_ = read_latest_;
  read_ctx.scan_backward_ = false;
  read_ctx.from_rpc_ = false;
  read_ctx.inrow_read_nocopy_ = false;
  read_ctx.need_read_latest_ = need_read_latest_;
  read_ctx.enable_remote_retry_ = false;
  read_ctx.is_full_table_scan_ = false;
  read_ctx.store_column_idx_ = store_column_idx_;
  read_ctx.access_ctx_ = nullptr;
  read_ctx.addr_ = addr_;
  read_ctx.seq_id_start_.reset();
  read_ctx.seq_id_end_.reset();
  if (OB_FAIL(read_ctx.snapshot_.assign(snapshot_))) {
    LOG_WARN("assign snapshot fail", K(ret), KPC(this));
  }
  return ret;
}

//==================== ObLobUpdateContext ====================
int ObLobUpdateContext::build_for_dml(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const blocksstable::ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobWriteContext::build_for_dml(run_ctx, data_row, col_idx, datum))) {
    LOG_WARN("failed build access param for dml", KR(ret), K(data_row), K(col_idx), K(datum));
  } else {
    dml_op_ = ObLobDmlOp::UPDATE;
  }
  return ret;
}

//==================== ObLobAppendContext ====================
int ObLobAppendContext::build_for_dml(
    const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const blocksstable::ObStorageDatum &old_datum,
    const blocksstable::ObStorageDatum &new_datum)
{
  int ret = OB_SUCCESS;
  common::ObLobLocatorV2 old_loc;
  common::ObLobLocatorV2 delta_loc;
  old_datum.get_lob_locatorv2(old_loc);
  new_datum.get_lob_locatorv2(delta_loc);
  if (OB_FAIL(build(run_ctx, data_row, col_idx, old_loc, delta_loc))) {
    LOG_WARN("failed to build append context for dml", KR(ret), K(data_row),
             K(col_idx), K(old_datum), K(new_datum));
  }
  return ret;
}

int ObLobAppendContext::build(const ObDMLRunningCtx &run_ctx,
    const blocksstable::ObDatumRow &data_row,
    const int16_t col_idx,
    const common::ObLobLocatorV2 &old_loc,
    const common::ObLobLocatorV2 &delta_loc)
{
  int ret = OB_SUCCESS;
  const ObMemLobExternHeader *extern_header = nullptr;
  ObString append_data;
  old_loc_ = old_loc;
  delta_loc_ = delta_loc;
  if (OB_UNLIKELY(!old_loc_.is_valid() || !delta_loc_.is_append_delta_lob())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob append locator", KR(ret), K(old_loc_), K(delta_loc_));
  } else if (OB_FAIL(delta_loc_.get_append_delta(extern_header, append_data))) {
    LOG_WARN("failed to parse append delta", KR(ret), K(delta_loc_));
  } else if (OB_ISNULL(extern_header)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("append delta extern header is null", KR(ret), K(delta_loc_));
  } else if (OB_FAIL(ObLobWriteContext::build(run_ctx, data_row, col_idx, old_loc_))) {
    LOG_WARN("failed to build access param for append", KR(ret), K(old_loc_));
  } else if (FALSE_IT(is_main_table_rowkey_col_ = run_ctx.is_main_table_rowkey_col(col_idx)
      || (!run_ctx.relative_table_.is_index_table()
          && col_idx < run_ctx.relative_table_.get_rowkey_column_num()))) {
  } else if (OB_UNLIKELY(extern_header->cs_type_ != coll_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("append delta collation does not match target column", KR(ret),
             "delta_collation", extern_header->cs_type_,
             "target_collation", coll_type_);
  } else if (!src_loc_wrapper_.is_inrow()
             && OB_FAIL(src_loc_wrapper_.get_lob_id(lob_id_))) {
    LOG_WARN("failed to get old lob id for append", KR(ret), K(src_loc_wrapper_));
  } else {
    dml_op_ = ObLobDmlOp::UPDATE;
    // 为安全起见，读取当前事务中最新的 LOB 辅助表版本。
    need_read_latest_ = true;
  }
  return ret;
}

} // storage
} // oceanbase
