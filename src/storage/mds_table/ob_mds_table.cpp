/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX MDS

#include "ob_mds_table.h"
#include "ob_mds_value_type_name.h"

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ls/ob_ls.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/ob_i_store.h"
#include "storage/ob_storage_schema.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_global_iterator_pool.h"
#include "storage/access/ob_single_merge.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "common/rowkey/ob_store_rowkey.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/ob_i_tablet_memtable.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/ob_relative_table.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/memtable/ob_concurrent_control.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_force_print_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "lib/ob_lib_config.h"

namespace oceanbase
{
using namespace share;
using namespace common;

namespace storage
{

namespace
{

template <typename T>
class ObMdsArenaObj
{
public:
  explicit ObMdsArenaObj(common::ObIAllocator &allocator)
      : allocator_(allocator),
        obj_(nullptr)
  {}
  ~ObMdsArenaObj()
  {
    reset();
  }
  int init()
  {
    int ret = OB_SUCCESS;
    void *buf = allocator_.alloc(sizeof(T));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      obj_ = new (buf) T();
    }
    return ret;
  }
  void reset()
  {
    if (OB_NOT_NULL(obj_)) {
      obj_->~T();
      allocator_.free(obj_);
      obj_ = nullptr;
    }
  }
  T *ptr() const { return obj_; }
  T &get() { return *obj_; }
  T *operator->() { return obj_; }
  T &operator*() { return *obj_; }

private:
  common::ObIAllocator &allocator_;
  T *obj_;
  DISALLOW_COPY_AND_ASSIGN(ObMdsArenaObj);
};

} // namespace

thread_local bool ObMdsTable::thread_tablet_status_read_empty_diag_ = false;

void ObMdsTable::set_tablet_status_read_empty_diag_()
{
  thread_tablet_status_read_empty_diag_ = true;
}

void ObMdsTable::clear_tablet_status_read_empty_diag_()
{
  thread_tablet_status_read_empty_diag_ = false;
}

const char *get_mds_value_type_name(const uint16_t mds_type)
{
  switch (mds_type) {
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
    case mds_type_id_detail::TypeIdOf<KEY_TYPE, VALUE_TYPE>::value:                             \
      return #VALUE_TYPE;
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
    default:
      return "UNKNOWN_MDS_VALUE_TYPE";
  }
}

ObMdsTable::ObMdsTable()
  : is_inited_(false),
    checkpoint_registered_(false),
    ls_id_(),
    ls_(nullptr),
    state_(static_cast<uint8_t>(MdsTableState::INIT))
{}

ObMdsTable::~ObMdsTable()
{
  destroy();
}

int ObMdsTable::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mds table already inited", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls));
  } else if (OB_ISNULL(ls->get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls tx service is null", K(ret), KP(ls));
  } else {
    ls_ = ls;
    ls_id_ = ls->get_ls_id();
    is_inited_ = true;
  }
  LOG_INFO("init mds table finish", K(ret), K_(ls_id));
  return ret;
}

int ObMdsTable::start()
{
  int ret = OB_SUCCESS;
  LOG_INFO("mds table start finish", K(ret), KPC(this));
  return ret;
}

void ObMdsTable::stop()
{
  LOG_INFO("mds table stop finish", KPC(this));
}

void ObMdsTable::destroy()
{
  int ret = OB_SUCCESS;
  if (!checkpoint_registered_) {
  } else if (OB_ISNULL(ls_) || OB_ISNULL(ls_->get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls or tx service is null when unregister mds checkpoint",
              K(ret), K_(ls_id), KP(ls_));
  } else if (OB_FAIL(ls_->get_tx_svr()->unregister_common_checkpoint(checkpoint::MDS_TABLE_TYPE, this))) {
    // Destroy proceeds regardless; if the registry somehow still referenced
    // this object it would dangle, so surface the invariant violation loudly.
    LOG_ERROR("failed to unregister mds table common checkpoint", K(ret), K_(ls_id));
  }
  LOG_INFO("destroy mds table finish", K_(ls_id));
  checkpoint_registered_ = false;
  ls_id_.reset();
  ls_ = nullptr;
  is_inited_ = false;
  ATOMIC_STORE(&state_, static_cast<uint8_t>(MdsTableState::INIT));
}

int ObMdsTable::create_tablet(const share::SCN &create_scn)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = nullptr;
  ObArenaAllocator arena_allocator;
  ObCreateTabletSchema create_tablet_schema;
  const lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_UNLIKELY(!create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid create_scn", K(ret), K(create_scn));
  } else if (OB_ISNULL(table_schema = ObMdsTabletSchemaHelper::get_instance().get_table_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds tablet table schema is null", K(ret));
  } else if (OB_FAIL(create_tablet_schema.init(arena_allocator,
                                               *table_schema,
                                               compat_mode,
                                               false/*skip_column_info*/,
                                               DATA_CURRENT_VERSION))) {
    LOG_WARN("failed to init create tablet schema", K(ret), KPC(table_schema));
  } else if (OB_FAIL(ls_->create_ls_inner_tablet(ls_id_,
                                                 LS_MDS_TABLET,
                                                 ObLS::LS_INNER_TABLET_FROZEN_SCN,
                                                 create_tablet_schema,
                                                 create_scn))) {
    LOG_WARN("failed to create mds tablet", K(ret), K_(ls_id), K(compat_mode), K(create_scn));
  }
  LOG_INFO("create mds tablet finish", K(ret), K_(ls_id), K(create_scn));
  return ret;
}

int ObMdsTable::remove_tablet_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_->remove_ls_inner_tablet(ls_id_, tablet_id))) {
    LOG_WARN("failed to remove mds tablet", K(ret), K_(ls_id), K(tablet_id));
  }
  return ret;
}

int ObMdsTable::remove_tablet()
{
  int ret = OB_SUCCESS;
  bool exist = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_FAIL(remove_tablet_(LS_MDS_TABLET))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to remove mds tablet", K(ret));
    }
  }
  LOG_INFO("remove mds tablet finish", K(ret), K_(ls_id), K(exist));
  return ret;
}

static int __get_mds_tablet_handle__(ObLS &ls, ObTabletHandle &mds_tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls.get_tablet_svr()->get_tablet(LS_MDS_TABLET, mds_tablet_handle))) {
    LOG_WARN("failed to get mds tablet", K(ret));
  } else if (OB_ISNULL(mds_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("mds tablet is null", K(ret));
  }
  return ret;
}

int ObMdsTable::offline()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is null", K(ret), K_(ls_id));
  } else if (checkpoint_registered_ && OB_ISNULL(ls_->get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls tx service is null", K(ret), K_(ls_id));
  } else if (checkpoint_registered_
             && OB_FAIL(ls_->get_tx_svr()->unregister_common_checkpoint(
                    checkpoint::MDS_TABLE_TYPE, this))) {
    LOG_WARN("failed to unregister mds table common checkpoint", K(ret), K_(ls_id));
  } else if (FALSE_IT(checkpoint_registered_ = false)) {
  } else if (FALSE_IT(ATOMIC_STORE(&state_, static_cast<uint8_t>(MdsTableState::OFFLINE)))) {
    // Unregister finished while still ONLINE, then publish OFFLINE so an
    // in-flight checkpoint cannot observe max_scn on a registered object.
  } else if (OB_FAIL(__get_mds_tablet_handle__(*ls_, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("mds tablet not exist, skip release memtables", K_(ls_id));
    } else {
      LOG_WARN("failed to get mds tablet for offline", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->release_memtables())) {
    LOG_WARN("failed to release mds tablet memtables", K(ret), KPC(ls_));
  }
  LOG_INFO("offline mds table finish", K(ret), K_(ls_id));
  return ret;
}

int ObMdsTable::online()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  const MdsTableState previous_state = static_cast<MdsTableState>(ATOMIC_LOAD(&state_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is null", K(ret), K_(ls_id));
  } else if (OB_FAIL(__get_mds_tablet_handle__(*ls_, tablet_handle))) {
    LOG_WARN("failed to get mds tablet", K(ret));
  } else if (OB_ISNULL(ls_->get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls tx service is null", K(ret), K_(ls_id));
  } else if (FALSE_IT(ATOMIC_STORE(&state_, static_cast<uint8_t>(MdsTableState::ONLINE)))) {
    // Publish ONLINE before registration so a checkpoint reader can never see
    // a registered MDS table return max_scn. Roll back if registration fails.
  } else if (!checkpoint_registered_
             && OB_FAIL(ls_->get_tx_svr()->register_common_checkpoint(checkpoint::MDS_TABLE_TYPE, this))) {
    ATOMIC_STORE(&state_, static_cast<uint8_t>(previous_state));
    LOG_WARN("failed to register mds table common checkpoint", K(ret), K_(ls_id));
  } else {
    checkpoint_registered_ = true;
  }
  LOG_INFO("online mds table finish", K(ret), K_(ls_id));
  return ret;
}

int ObMdsTable::init_tablet_for_compat()
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const share::SCN create_scn(SCN::base_scn());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_FAIL(ls_->get_tablet_svr()->is_tablet_exist(LS_MDS_TABLET, is_exist))) {
    LOG_WARN("failed to check tablet exist", K(ret));
  } else if (is_exist) {
    LOG_INFO("mds tablet already exists, skip create", K_(ls_id));
  } else if (OB_FAIL(create_tablet(create_scn))) {
    LOG_WARN("failed to create mds tablet", K(ret), KPC(ls_));
  }
  return ret;
}

int ObMdsTable::table_scan_by_mds_type_(
    common::ObIAllocator &allocator,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const uint8_t mds_unit_id,
    const share::SCN &read_snapshot,
    ObMdsTableScanIterator &iter)
{
  int ret = OB_SUCCESS;
  ObTabletHandle ls_mds_handle;
  ObLSTabletService *ls_tablet_svr = nullptr;
  common::ObNewRange key_range;
  ObTableScanParam *scan_param = nullptr;
  ObStoreCtx *store_ctx = nullptr;
  void *key_range_buf = nullptr;
  void *scan_param_buf = nullptr;
  void *store_ctx_buf = nullptr;
  share::SCN effective_read_snapshot;
  bool is_follower = false;
  // scan_param and store_ctx are intentionally allocated from the CALLER's
  // arena (not freed here on success): iter holds pointers/refs into them and
  // outlives this function. They are only torn down on the error path.
  const int64_t abs_timeout =
      THIS_WORKER.is_timeout_ts_valid()
          ? THIS_WORKER.get_timeout_ts()
          : (ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US + ObClockGenerator::getClock());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_UNLIKELY(!read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid read snapshot", K(ret), K(read_snapshot));
  } else if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is null", K(ret), KP(ls_));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_MDS_TABLET, ls_mds_handle))) {
    LOG_WARN("failed to get ls mds tablet", K(ret), K_(ls_id));
  } else if (OB_FAIL(prepare_mds_read_snapshot_(read_snapshot, ls_mds_handle, effective_read_snapshot, is_follower))) {
    LOG_WARN("prepare mds scan snapshot failed", K(ret), K_(ls_id), K(read_snapshot), K(is_follower));
  } else if (OB_FAIL(build_mds_scan_key_range_(allocator, tablet_id, transfer_scn, mds_unit_id,
                                               key_range_buf, key_range))) {
    LOG_WARN("failed to build mds scan key range", K(ret), K(tablet_id), K(transfer_scn), K(mds_unit_id));
  } else if (OB_ISNULL(scan_param_buf = allocator.alloc(sizeof(ObTableScanParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc scan param", K(ret));
  } else if (FALSE_IT(scan_param = new (scan_param_buf) ObTableScanParam())) {
  } else if (OB_ISNULL(store_ctx_buf = allocator.alloc(sizeof(ObStoreCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc store ctx", K(ret));
  } else if (FALSE_IT(store_ctx = new (store_ctx_buf) ObStoreCtx())) {
  } else if (OB_FAIL(store_ctx->init_for_read(
                 ls_id_,
                 LS_MDS_TABLET,
                 abs_timeout,
                 -1, /*tx lock timeout*/
                 effective_read_snapshot))) {
    LOG_WARN("failed to init store ctx for read", K(ret), K_(ls_id), K(effective_read_snapshot), K(abs_timeout));
  } else if (OB_FAIL(ObMdsScanParamHelper::build_mds_table_scan_param(
                 allocator,
                 ls_id_,
                 key_range,
                 abs_timeout,
                 ObVersionRange(
                     0/*base_version*/,
                     effective_read_snapshot.get_val_for_tx()/*snapshot_version*/),
                 false/*read_uncommitted*/,
                 is_follower,
                 *scan_param))) {
    LOG_WARN("failed to build ls mds table scan param",
        K(ret), K_(ls_id), K(effective_read_snapshot), K(key_range), K(mds_unit_id));
  } else if (OB_FAIL(iter.init(*scan_param, ls_mds_handle, *store_ctx))) {
    LOG_WARN("fail to init mds table scan iterator", K(ret), KPC(scan_param), K(key_range), K(mds_unit_id));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(store_ctx)) {
      store_ctx->~ObStoreCtx();
      allocator.free(store_ctx);
    }
    if (OB_NOT_NULL(scan_param)) {
      scan_param->~ObTableScanParam();
      allocator.free(scan_param);
    }
    if (OB_NOT_NULL(key_range_buf)) {
      allocator.free(key_range_buf);
    }
  }

  return ret;
}

int ObMdsTable::build_mds_scan_key_range_(
    common::ObIAllocator &allocator,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const uint8_t mds_unit_id,
    void *&key_range_buf,
    common::ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  const int64_t rk_cnt = ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM;
  key_range_buf = allocator.alloc(sizeof(ObObj) * rk_cnt * 2);
  if (OB_ISNULL(key_range_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for mds scan key range", K(ret), K(rk_cnt));
  } else {
    ObObj *start_obj = new (key_range_buf) ObObj[rk_cnt]();
    ObObj *end_obj = new (static_cast<char *>(key_range_buf) + sizeof(ObObj) * rk_cnt) ObObj[rk_cnt]();
    const int64_t transfer_scn_val = transfer_scn.get_val_for_tx(true /*ignore_invalid_scn*/);
    const int64_t unit_id_val = static_cast<int32_t>(mds_unit_id);
    start_obj[0].set_int(tablet_id.id());
    end_obj[0].set_int(tablet_id.id());
    start_obj[1].set_int(transfer_scn_val);
    end_obj[1].set_int(transfer_scn_val);
    start_obj[2].set_int(unit_id_val);
    end_obj[2].set_int(unit_id_val);
    start_obj[3].set_min_value();
    end_obj[3].set_max_value();
    key_range.table_id_ = ObMdsTabletSchemaDef::MDS_TABLE_ID;
    key_range.start_key_.assign(start_obj, rk_cnt);
    key_range.end_key_.assign(end_obj, rk_cnt);
    key_range.border_flag_.unset_inclusive_start();
    key_range.border_flag_.unset_inclusive_end();
    key_range.flag_ = 0;
  }
  return ret;
}

int ObMdsTable::prepare_mds_read_snapshot_(
    const share::SCN &read_snapshot,
    const ObTabletHandle &ls_mds_handle,
    share::SCN &effective_read_snapshot,
    bool &is_follower)
{
  int ret = OB_SUCCESS;
  effective_read_snapshot = read_snapshot;
  is_follower = false;
  common::ObRole ls_role = common::ObRole::INVALID_ROLE;
  int64_t proposal_id = 0;
  if (OB_FAIL(ls_->get_log_handler()->get_role(ls_role, proposal_id))) {
    LOG_WARN("get ls role failed", K(ret), K_(ls_id));
  } else if (common::ObRole::FOLLOWER == ls_role) {
    is_follower = true;
    share::SCN max_decided_scn;
    if (OB_FAIL(ls_->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("get_max_decided_scn failed", K(ret), K_(ls_id));
      if (OB_STATE_NOT_MATCH == ret) {
        if (ls_->is_offline()) {
          ret = OB_LS_OFFLINE;
          LOG_WARN("ls is offline", K(ret), K_(ls_id), KPC(ls_));
        }
      }
    } else if (!max_decided_scn.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max_decided_scn is invalid", K(ret), K_(ls_id));
    } else if (read_snapshot > max_decided_scn) {
      // On follower, read_snapshot may exceed max_decided_scn after restart or
      // during recovery replay and would otherwise return OB_SNAPSHOT_DISCARDED.
      // Clamp to max(max_decided_scn, multi_version_start) so recovery can still
      // read MDS (e.g. TabletStatus). Other MDS types may be restricted later.
      // TODO: restrict recovery reads to required MDS types only.
      int64_t multi_version_start = ls_mds_handle.get_obj()->get_multi_version_start();
      share::SCN multi_version_start_scn;
      if (OB_FAIL(multi_version_start_scn.convert_for_tx(multi_version_start))) {
        LOG_ERROR("convert for tx failed", K(ret), K(multi_version_start));
      } else {
        effective_read_snapshot = share::SCN::max(max_decided_scn, multi_version_start_scn);
      }
    }
  }
  return ret;
}

int ObMdsTable::fill_mds_row_read_info_(
    const blocksstable::ObDatumRow &src_row,
    ObMdsRowReadInfo &row_info)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = src_row.get_column_count();
  const int64_t trans_version_col_idx =
      static_cast<int64_t>(ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX::TRANS_VERSION);
  if (OB_UNLIKELY(!ObMdsTabletSchemaDef::is_known_mds_row_layout(column_count))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported mds row layout", KR(ret), K(column_count), K(src_row));
  } else {
    const int64_t writer_tx_id_col_idx =
        ObMdsTabletSchemaDef::get_phy_col_idx(
            column_count,
            ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::WRITER_TRANS_ID);
    row_info.reset();
    row_info.trans_version_ = src_row.storage_datums_[trans_version_col_idx].get_int();
    row_info.row_trans_id_ = src_row.get_trans_id();
    row_info.writer_tx_id_ = src_row.storage_datums_[writer_tx_id_col_idx].get_int();
    row_info.is_uncommitted_ = (row_info.trans_version_ == INT64_MAX);
  }
  return ret;
}

// Bundles the per-call resources needed to drive ObSingleMerge for a point read
// on LS_MDS_TABLET. Caller holds an instance so that single_merge stays alive
// until read_and_verify_single_row_ has consumed it.
//
// ObSingleMerge prefers the tenant-level ObGlobalIteratorPool (T_SINGLE_GET),
// falling back to allocator_ when the pool is unavailable. Pool-owned iters
// must be released via cached_iter_node_ and must not be destroyed here.
struct ObMdsTable::ObMdsSingleMergeResources
{
  explicit ObMdsSingleMergeResources(common::ObIAllocator &alloc)
      : allocator_(alloc),
        ls_mds_handle(),
        scan_param(alloc),
        store_ctx(alloc),
        access_param(alloc),
        access_ctx(alloc),
        get_table_param(alloc),
        single_merge_(nullptr),
        cached_iter_node_(nullptr),
        rowkey_obj(),
        rowkey(),
        datum_rowkey()
  {}
  ~ObMdsSingleMergeResources()
  {
    // Reclaim/release while access_ctx is still alive: CachedIteratorNode::reclaim
    // may touch access_ctx through ObSingleMerge::reclaim().
    if (nullptr != single_merge_) {
      if (nullptr == cached_iter_node_) {
        single_merge_->~ObSingleMerge();
        allocator_.free(single_merge_);
      }
      single_merge_ = nullptr;
    }
    if (nullptr != cached_iter_node_) {
      MTL(ObGlobalIteratorPool*)->release(cached_iter_node_);
    }
  }

  int alloc_access_objs()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(scan_param.init())) {
      LOG_WARN("failed to alloc mds get scan param", KR(ret));
    } else if (OB_FAIL(store_ctx.init())) {
      LOG_WARN("failed to alloc mds get store ctx", KR(ret));
    } else if (OB_FAIL(access_param.init())) {
      LOG_WARN("failed to alloc mds get access param", KR(ret));
    } else if (OB_FAIL(access_ctx.init())) {
      LOG_WARN("failed to alloc mds get access ctx", KR(ret));
    } else if (OB_FAIL(get_table_param.init())) {
      LOG_WARN("failed to alloc mds get table param", KR(ret));
    }
    return ret;
  }

  void build_rowkey(const ObMdsReadArg &read_arg)
  {
    rowkey_obj[0].set_int(read_arg.tablet_id_.id());
    rowkey_obj[1].set_int(read_arg.transfer_scn_.get_val_for_tx(true/*ignore_invalid_scn*/));
    rowkey_obj[2].set_int(static_cast<int32_t>(read_arg.mds_unit_id_));
    rowkey_obj[3].set_binary(read_arg.udf_key_);
    rowkey.assign(rowkey_obj, ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM);
  }

  // LS_MDS_TABLET is an inner tablet; normal table-scan paths skip the pool for
  // inner tablets, but MDS schema is fixed and small so reuse is intentional.
  // Soft-miss (success + null node) falls back to arena allocation.
  int prepare_cached_iter_node()
  {
    int ret = OB_SUCCESS;
    ObGlobalIteratorPool *iter_pool = MTL(ObGlobalIteratorPool*);
    if (OB_NOT_NULL(iter_pool)
        && iter_pool->can_use_iter_pool(
            1 /*table_cnt: MDS tablet table count is typically tiny*/,
            ObMdsTabletSchemaDef::MDS_SSTABLE_COLUMN_CNT,
            T_SINGLE_GET)) {
      if (OB_FAIL(iter_pool->get(T_SINGLE_GET, cached_iter_node_))) {
        LOG_WARN("failed to get cached iter node for mds get", KR(ret));
      } else if (nullptr != cached_iter_node_) {
        access_param->set_use_global_iter_pool();
        access_param->iter_param_.set_use_stmt_iter_pool();
        LOG_TRACE("mds point-get use global iter pool", KP(cached_iter_node_));
      }
    }
    return ret;
  }

  int bind_tablet_for_get()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(get_table_param->tablet_iter_.set_tablet_handle(ls_mds_handle))) {
      LOG_WARN("failed to set tablet handle for mds get", KR(ret), K(ls_mds_handle));
    } else {
      get_table_param->frozen_version_ = scan_param->frozen_version_;
      get_table_param->sample_info_ = scan_param->sample_info_;
    }
    return ret;
  }

  int init_and_open_single_merge()
  {
    int ret = OB_SUCCESS;
    ObQueryRowIterator *cached_iter =
        nullptr == cached_iter_node_ ? nullptr : cached_iter_node_->get_iter();
    if (OB_NOT_NULL(cached_iter)) {
      single_merge_ = static_cast<ObSingleMerge *>(cached_iter);
    }
    if (nullptr == single_merge_) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = access_ctx->get_long_life_allocator()->alloc(sizeof(ObSingleMerge)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc ObSingleMerge for mds get", KR(ret));
      } else {
        single_merge_ = new (buf) ObSingleMerge();
        if (OB_FAIL(single_merge_->init(*access_param, *access_ctx, *get_table_param))) {
          LOG_WARN("failed to init single merge for mds get", KR(ret));
          single_merge_->~ObSingleMerge();
          access_ctx->get_long_life_allocator()->free(single_merge_);
          single_merge_ = nullptr;
        } else if (nullptr != cached_iter_node_) {
          cached_iter_node_->set_iter(single_merge_);
        }
      }
    } else if (OB_FAIL(single_merge_->switch_table(*access_param, *access_ctx, *get_table_param))) {
      LOG_WARN("failed to switch table for mds single merge", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(single_merge_->open(datum_rowkey))) {
      LOG_WARN("failed to open single merge for mds get", KR(ret), K(datum_rowkey));
    }
    return ret;
  }

  common::ObIAllocator &allocator_;
  ObTabletHandle ls_mds_handle;
  ObMdsArenaObj<ObTableScanParam> scan_param;
  ObMdsArenaObj<ObStoreCtx> store_ctx;
  ObMdsArenaObj<ObTableAccessParam> access_param;
  ObMdsArenaObj<ObTableAccessContext> access_ctx;
  ObMdsArenaObj<ObGetTableParam> get_table_param;
  ObSingleMerge *single_merge_;
  CachedIteratorNode *cached_iter_node_;
  // rowkey_obj / rowkey / datum_rowkey must outlive single_merge_->open(): the
  // merge holds a pointer to datum_rowkey, and datum_rowkey shallow-copies
  // ObObj string data from rowkey_obj. Living on this struct keeps them
  // valid for the caller's stack frame (read_mds_row_) — moving any of
  // them into a helper-local would dangle.
  ObObj rowkey_obj[ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM];
  ObRowkey rowkey;
  blocksstable::ObDatumRowkey datum_rowkey;
  DISALLOW_COPY_AND_ASSIGN(ObMdsSingleMergeResources);
};

struct ObMdsTable::ObMdsSingleReadDiagnoseInfo
{
  bool is_follower = false;
  share::SCN effective_read_snapshot;
  bool has_result_row = false;
  int64_t result_trans_version = INT64_MAX;
  int64_t writer_tx_id = 0;
  bool result_uncommitted = false;
  TO_STRING_KV(K(is_follower),
               K(effective_read_snapshot),
               K(has_result_row),
               K(result_trans_version),
               K(writer_tx_id),
               K(result_uncommitted));
};

int ObMdsTable::read_mds_row_(
    const ObMdsReadArg &read_arg,
    ObMdsReadResult &read_result)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_allocator(ObMemAttr(MTL_ID(), "MdsGetRow", ObCtxIds::DEFAULT_CTX_ID));
  ObMdsSingleReadDiagnoseInfo diagnose_info;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_UNLIKELY(!read_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mds read arg", K(ret), K(read_arg));
  } else if (OB_UNLIKELY(!read_result.is_valid(read_arg))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mds read result", K(ret), K(read_arg), K(read_result));
  } else {
    // Internal LS_MDS_TABLET point-get must not pollute the current user SQL audit.
    lib::ObDisableDiagnoseGuard disable_diagnose_guard;
    ObMdsSingleMergeResources resources(arena_allocator);
    ACTIVE_GLOBAL_ITERATOR_GUARD(ret, resources.cached_iter_node_);
    if (OB_FAIL(setup_single_merge_for_read_(
            read_arg,
            arena_allocator,
            resources,
            diagnose_info))) {
      LOG_WARN("failed to set up single merge for mds read", KR(ret), K(read_arg));
    } else if (OB_FAIL(read_and_verify_single_row_(
                   read_arg,
                   read_result,
                   resources,
                   diagnose_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read and verify single mds row", KR(ret), K(read_arg));
      }
    }
  }

  const char *mds_value_type_name = get_mds_value_type_name(read_arg.mds_unit_id_);
  LOG_TRACE("completed mds get",
            K(ret),
            K_(ls_id),
            K(read_arg),
            K(mds_value_type_name),
            K(diagnose_info));
  return ret;
}

// Write mds node to LS_MDS_TABLET's memtable.
// Row layout: (tablet_id, transfer_scn, mds_type, mds_key, writer_trans_id,
// commit_version, commit_scn, nop /*value*/)
// The fixed metadata and live V object live in the MDS MVCC node payload; the
// value datum is nop here and materialized from the payload at read/dump time.
int ObMdsTable::write_mds_node_(const common::ObTabletID &tablet_id,
                                const share::SCN &transfer_scn,
                                const uint8_t mds_type_id,
                                const common::ObString &mds_key,
                                const void *mds_value,
                                const ObMdsValueMeta *meta,
                                ObStoreCtx &store_ctx,
                                mds::MdsCtx &mds_ctx,
                                const bool check_dup)
{
  int ret = OB_SUCCESS;
  ObMdsTabletSchemaHelper &schema_helper = ObMdsTabletSchemaHelper::get_instance();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_UNLIKELY(!store_ctx.is_valid() || !store_ctx.is_write())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid store ctx, mds write requires valid write store ctx", K(ret), K(store_ctx));
  } else {
    store_ctx.ls_id_ = ls_id_;
    store_ctx.ls_ = ls_;
    store_ctx.tablet_id_ = LS_MDS_TABLET;
    store_ctx.table_version_ = schema_helper.get_table_schema()->get_schema_version();
    store_ctx.mvcc_acc_ctx_.set_mds_write_context(mds_type_id, mds_value, meta);
    blocksstable::ObDatumRow new_row;
    ObRelativeTable local_relative_table;
    // Caller may install relative_table on mds_ctx (ObMdsRelativeTableGuard) to
    // reuse initialized read tables across batch insert_row calls.
    ObRelativeTable *relative_table = mds_ctx.get_relative_table();
    if (OB_ISNULL(relative_table)) {
      relative_table = &local_relative_table;
    }

    if (!relative_table->is_valid()) {
      ObTabletHandle mds_tablet_handle;
      const share::schema::ObTableSchemaParam *schema_param = schema_helper.get_schema_param();
      if (OB_ISNULL(schema_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema param is null", K(ret));
      } else if (OB_FAIL(relative_table->init(schema_param, LS_MDS_TABLET))) {
        LOG_WARN("failed to init relative table for mds write", K(ret));
      } else if (OB_FAIL(get_mds_tablet_handle_(mds_tablet_handle))) {
        LOG_WARN("failed to get mds tablet", K(ret));
      } else if (FALSE_IT(relative_table->tablet_iter_.set_tablet_handle(mds_tablet_handle))) {
      } else if (OB_FAIL(relative_table->tablet_iter_.refresh_read_tables_from_tablet(
          store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),/*snapshot version*/
          relative_table->allow_not_ready(),
          false /*major_sstable_only*/,
          true /*need_split_src_table*/,
          false /*need_split_dst_table*/))) {
        LOG_WARN("failed to refresh read tables from mds tablet", K(ret));
      }
      if (OB_FAIL(ret)) {
        relative_table->destroy();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(new_row.init(ObMdsTabletSchemaDef::MDS_ROW_COLUMN_CNT))) {
      LOG_WARN("failed to init mds datum row", K(ret));
    } else {
      typedef blocksstable::ObDmlFlag ObDmlFlag;
      const ObDmlFlag dml_flag = check_dup ? ObDmlFlag::DF_INSERT : ObDmlFlag::DF_PUT;
      new_row.row_flag_.set_flag(dml_flag);
      new_row.storage_datums_[0].set_int(tablet_id.id());
      new_row.storage_datums_[1].set_int(transfer_scn.get_val_for_tx(true /*ignore_invalid_scn*/));
      new_row.storage_datums_[2].set_int(mds_type_id);
      new_row.storage_datums_[3].set_string(mds_key);
      new_row.storage_datums_[4].set_nop();
      new_row.storage_datums_[5].set_nop();
      new_row.storage_datums_[6].set_nop();
      new_row.storage_datums_[7].set_nop();
    }
    const common::ObIArray<share::schema::ObColDesc> &col_descs = schema_helper.get_column_descs();
    // insert_row may refresh relative_table and replace tablet_iter_'s handle; keep a
    // local handle so the tablet is not released before the write completes.
    ObTabletHandle mds_tablet_handle;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(mds_tablet_handle.assign(relative_table->tablet_iter_.get_tablet_handle()))) {
      LOG_WARN("failed to assign mds tablet handle", K(ret), K_(ls_id), KPC(relative_table));
    } else if (OB_UNLIKELY(!mds_tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mds tablet handle is invalid", K(ret), K_(ls_id), KPC(relative_table));
    } else if (OB_FAIL(mds_tablet_handle.get_obj()->insert_row(*relative_table, store_ctx,
                                                               check_dup, col_descs,
                                                               new_row, nullptr /*encrypt_meta_arr*/))) {
      LOG_WARN("failed to write mds node to mds tablet", K(ret), K(tablet_id), K_(ls_id));
    } else {
      mds_ctx.register_mds_memtable_write();
    }
    store_ctx.mvcc_acc_ctx_.clear_mds_write_context();
  }
  const char *mds_value_type_name = get_mds_value_type_name(mds_type_id);
  LOG_INFO("completed mds write", K(ret),
           K(tablet_id), K(ls_id_),
           "tx_id", store_ctx.mvcc_acc_ctx_.tx_id_,
           "seq_no", store_ctx.mvcc_acc_ctx_.tx_scn_,
           K(transfer_scn),
           K(mds_type_id),
           K(mds_value_type_name),
           K(mds_key),
           K(check_dup));
  return ret;
}

bool ObMdsTable::is_mds_replay_ready_() const
{
  return nullptr != ls_ && nullptr != ls_->get_tablet_svr();
}

int ObMdsTable::get_mds_tablet_handle_(ObTabletHandle &mds_tablet_handle) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(__get_mds_tablet_handle__(*ls_, mds_tablet_handle))) {
    LOG_WARN("failed to get mds tablet", K(ret));
  }
  return ret;
}

int ObMdsTable::get_mds_checkpoint_scn(share::SCN &mds_ckpt_scn) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle mds_tablet_handle;
  if (OB_FAIL(get_mds_tablet_handle_(mds_tablet_handle))) {
    LOG_WARN("failed to get mds tablet", K(ret));
  } else {
    mds_ckpt_scn = mds_tablet_handle.get_obj()->get_clog_checkpoint_scn();
  }
  return ret;
}

share::SCN ObMdsTable::get_rec_scn()
{
  share::SCN rec_scn = share::SCN::max_scn();
  int ret = OB_SUCCESS;
  ObTabletHandle mds_tablet_handle;
  bool frozen_memtable = false;
  const MdsTableState current_state = static_cast<MdsTableState>(ATOMIC_LOAD(&state_));
  if (MdsTableState::ONLINE != current_state) {
    // LS_MDS_TABLET legitimately absent in INIT (create_ls_inner_tablet not
    // done yet) or OFFLINE (memtables released, tablet may be gone).
    // max_scn means this checkpoint has no log to retain.
  } else if (!is_inited_ || OB_ISNULL(ls_)) {
    // Fail closed: without the LS there is no known-safe rec_scn. base_scn()
    // is smaller than any durable clog checkpoint, so the checkpoint
    // executor refuses to advance the clog checkpoint until this heals.
    rec_scn = share::SCN::base_scn();
    LOG_ERROR("online ObMdsTable not inited or ls is null in get_rec_scn", K_(ls_id));
  } else if (OB_FAIL(get_mds_tablet_handle_(mds_tablet_handle))) {
    // Fail closed on lookup errors: report the current clog checkpoint so
    // the executor holds the checkpoint where it is instead of recycling
    // MDS logs whose durable state cannot be determined. (SCN::min_scn()
    // must not be used for this: ObCheckpointExecutor::get_min_rec_scn
    // filters it out, which would silently fail open.)
    rec_scn = ls_->get_clog_checkpoint_scn();
    LOG_ERROR("failed to get LS_MDS_TABLET handle for get_rec_scn, hold clog checkpoint",
              K(ret), K_(ls_id), K(rec_scn));
  } else if (OB_FAIL(mds_tablet_handle.get_obj()->get_rec_log_scn(rec_scn, frozen_memtable))) {
    // rec_scn may have been partially updated by the failed call.
    rec_scn = ls_->get_clog_checkpoint_scn();
    LOG_ERROR("failed to get rec log scn on LS_MDS_TABLET, hold clog checkpoint",
              K(ret), K_(ls_id), K(rec_scn));
  }
  return rec_scn;
}

int ObMdsTable::freeze_ls_mds_tablet_(const int64_t trace_id, bool is_sync)
{
  int ret = OB_SUCCESS;
  ObSEArray<common::ObTabletID, 1> tablet_ids;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is null", K(ret), K_(ls_id));
  } else if (OB_FAIL(tablet_ids.push_back(LS_MDS_TABLET))) {
    LOG_WARN("failed to push back ls mds tablet id", K(ret));
  } else if (OB_FAIL(ls_->tablet_freeze(trace_id,
                                        tablet_ids,
                                        is_sync /*is_sync*/,
                                        0 /*input_abs_timeout_ts*/,
                                        false /*need_rewrite_meta*/,
                                        ObFreezeSourceFlag::MDS_FLUSH))) {
    LOG_WARN("failed to freeze LS_MDS_TABLET", K(ret), K_(ls_id));
  }
  return ret;
}

int ObMdsTable::flush(share::SCN recycle_scn, const int64_t trace_id, bool need_freeze)
{
  int ret = OB_SUCCESS;
  ObTabletHandle mds_tablet_handle;
  const MdsTableState current_state = static_cast<MdsTableState>(ATOMIC_LOAD(&state_));
  const bool is_online = (MdsTableState::ONLINE == current_state);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (!is_online) {
    // An inactive MDS table does not participate in checkpoint flush. Keep
    // this guard even though online/offline register the common checkpoint,
    // because direct callers and lifecycle transitions may race with flush.
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls is null", K(ret), K_(ls_id));
  } else if (OB_FAIL(get_mds_tablet_handle_(mds_tablet_handle))) {
    LOG_WARN("failed to get mds tablet", K(ret));
  } else if (ls_->flush_is_disabled()) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10LL * 1000LL * 1000LL)) {
      LOG_INFO("mds checkpoint flush skipped, flush disabled", K(ret), K_(ls_id));
    }
  }

  if (OB_SUCC(ret) && is_online && need_freeze) {
    ObTableHandleV2 active_memtable;
    const bool reach_log_interval = REACH_TIME_INTERVAL(10_s);
    ret = mds_tablet_handle.get_obj()->get_active_memtable(active_memtable);
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (reach_log_interval) {
        LOG_INFO("no active memtable, skip freeze", K_(ls_id));
      }
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get active memtable", K(ret));
    } else if (active_memtable.get_table()->get_rec_scn() > recycle_scn) {
      if (reach_log_interval) {
        LOG_INFO("active memtable rec_scn > recycle_scn, skip freeze",
                 K(recycle_scn),
                 K(active_memtable.get_table()->get_rec_scn()),
                 K_(ls_id));
      }
    } else if (OB_FAIL(freeze_ls_mds_tablet_(trace_id, false))) {
      LOG_WARN("failed to freeze ls mds tablet", K(ret), K_(ls_id), K(trace_id));
    } else if (reach_log_interval) {
      LOG_INFO("mds checkpoint freeze kicked off", K(ret), K_(ls_id), K(trace_id));
    }
  }

  if (OB_SUCC(ret) && is_online
      && OB_FAIL(flush_frozen_memtables_(mds_tablet_handle, trace_id))) {
    LOG_WARN("failed to flush frozen mds memtables", K(ret), K_(ls_id), K(trace_id));
  }
  return ret;
}

int ObMdsTable::replay_mds_node_(const common::ObTabletID &tablet_id,
                                 const share::SCN &transfer_scn,
                                 const uint8_t mds_type_id,
                                 const common::ObString &mds_key,
                                 const void *mds_value,
                                 const ObMdsValueMeta *meta,
                                 mds::MdsCtx &mds_ctx,
                                 const share::SCN &replay_scn)
{
  int ret = OB_SUCCESS;
  share::SCN mds_clog_ckpt_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mds table not inited", K(ret));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(get_mds_checkpoint_scn(mds_clog_ckpt_scn))) {
    LOG_WARN("failed to get mds tablet clog_checkpoint_scn", K(ret));
  } else if (replay_scn <= mds_clog_ckpt_scn) {
    // Logs at or before LS_MDS_TABLET clog_checkpoint are already durable; skip memtable replay.
    ret = OB_NO_NEED_UPDATE;
    LOG_DEBUG("skip ls mds replay, replay_scn not beyond mds tablet clog_checkpoint_scn",
              K(ret),
              K(replay_scn),
              K(mds_clog_ckpt_scn),
              K(tablet_id),
              K_(ls_id));
  } else {
    ObTabletHandle mds_tablet_handle;
    common::ObObj rk_objs[ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM];
    common::ObStoreRowkey store_rowkey;
    rk_objs[0].set_int(static_cast<int64_t>(tablet_id.id()));
    rk_objs[1].set_int(transfer_scn.get_val_for_tx(true /*ignore_invalid_scn*/));
    rk_objs[2].set_int(static_cast<int64_t>(mds_type_id));
    rk_objs[3].set_binary(mds_key);
    if (OB_FAIL(store_rowkey.assign(rk_objs, ObMdsTabletSchemaDef::ROWKEY_COLUMN_NUM))) {
      LOG_WARN("failed to assign ls mds aggregate store rowkey", K(ret));
    } else if (OB_FAIL(get_mds_tablet_handle_(mds_tablet_handle))) {
      LOG_WARN("failed to get mds tablet", K(ret));
    } else {
      ObTablet *mds_tablet = mds_tablet_handle.get_obj();
      ObStoreCtx store_ctx;
      store_ctx.mvcc_acc_ctx_.init_replay_light(
        *mds_ctx.get_mem_ctx(),
        mds_ctx.get_mem_ctx()->get_tx_id()
      );
      store_ctx.tablet_id_ = LS_MDS_TABLET;
      store_ctx.ls_id_ = ls_id_;
      store_ctx.ls_ = ls_;
      store_ctx.table_version_ = ObMdsTabletSchemaHelper::get_instance().get_table_schema()->get_schema_version();
      ObStorageTableGuard guard(mds_tablet, store_ctx, true /*need_control_mem*/, true /*for_replay*/, replay_scn);
      if (OB_FAIL(guard.refresh_and_protect_memtable_for_replay())) {
        LOG_WARN("failed to refresh memtable for ls mds replay", K(ret), K_(ls_id));
      } else {
        ObIMemtable *imem = nullptr;
        memtable::ObMemtable *write_memtable = nullptr;
        if (OB_FAIL(guard.get_memtable_for_replay(imem))) {
          LOG_WARN("failed to get memtable for ls mds replay", K(ret), K_(ls_id));
        } else if (OB_ISNULL(imem)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable is null after ls mds replay refresh", K(ret), K_(ls_id));
        } else if (OB_ISNULL(write_memtable = static_cast<memtable::ObMemtable *>(imem))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable is not ObMemtable for ls mds replay", K(ret), K_(ls_id));
        } else if (OB_FAIL(write_memtable->set_max_end_scn(replay_scn))) {
          LOG_WARN("set_max_end_scn failed before ls mds replay", K(ret), K_(ls_id), K(replay_scn));
        } else if (OB_FAIL(write_memtable->replay_mds_row(store_ctx,
                                                          store_rowkey,
                                                          mds_ctx.get_seq_no(),
                                                          replay_scn,
                                                          mds_type_id,
                                                          mds_value,
                                                          meta))) {
          LOG_WARN("replay_mds_row failed", K(ret), K(tablet_id), K_(ls_id));
        } else if (OB_FAIL(write_memtable->set_rec_scn(replay_scn))) {
          LOG_WARN("set_rec_scn failed after ls mds replay", K(ret), K_(ls_id), K(replay_scn));
        }
      }
    }
  }
  return ret;
}

// Stage 1 of read_mds_row_: fetch LS_MDS_TABLET, init the arena-backed access
// objects, prefer ObGlobalIteratorPool for ObSingleMerge, build the row-key,
// and open the SingleMerge. On success, resources.single_merge_ is opened and
// ready for get_next_row.
int ObMdsTable::setup_single_merge_for_read_(
    const ObMdsReadArg &read_arg,
    common::ObIAllocator &allocator,
    ObMdsSingleMergeResources &resources,
    ObMdsSingleReadDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObMdsTabletSchemaHelper &schema = ObMdsTabletSchemaHelper::get_instance();
  const share::schema::ObTableParam *query_table_param = schema.get_query_table_param();
  const ObRowkeyReadInfo *rowkey_read_info = schema.get_rowkey_read_info();
  share::SCN effective_read_snapshot;
  bool is_follower = false;
  const int64_t now = ObTimeUtility::current_time();
  const int64_t abs_timeout = now > INT64_MAX - read_arg.timeout_us_
      ? INT64_MAX : now + read_arg.timeout_us_;
  if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet service is null", K(ret), KP(ls_));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_MDS_TABLET, resources.ls_mds_handle))) {
    LOG_WARN("failed to get ls mds tablet", K(ret), K_(ls_id));
  } else if (OB_FAIL(prepare_mds_read_snapshot_(read_arg.read_snapshot_, resources.ls_mds_handle, effective_read_snapshot, is_follower))) {
    LOG_WARN("prepare mds read snapshot failed", K(ret), K_(ls_id), K(is_follower));
  } else if (OB_FAIL(resources.alloc_access_objs())) {
    LOG_WARN("failed to alloc mds get access objects", KR(ret));
  } else {
    const ObVersionRange read_version_range(0/*base_version*/, effective_read_snapshot.get_val_for_tx());
    resources.build_rowkey(read_arg);
    if (OB_FAIL(ObMdsScanParamHelper::init_mds_get_scan_param(
            allocator,
            ls_id_,
            read_version_range,
            read_arg.read_uncommitted_,
            is_follower,
            abs_timeout,
            *query_table_param,
            *resources.scan_param))) {
      LOG_WARN("failed to init mds get scan param", KR(ret), K(read_arg));
    } else if (OB_FAIL(resources.store_ctx->init_for_read(
                   ls_id_,
                   LS_MDS_TABLET,
                   abs_timeout,
                   -1, /*tx lock timeout*/
                   effective_read_snapshot))) {
      LOG_WARN("failed to init store ctx for read", K(ret), K_(ls_id), K(effective_read_snapshot), K(abs_timeout));
    } else if (OB_FAIL(resources.access_param->init(*resources.scan_param, nullptr/*tablet_handle*/, rowkey_read_info))) {
      LOG_WARN("failed to init access param for mds get", KR(ret), KPC(resources.scan_param.ptr()));
    } else if (OB_FAIL(resources.prepare_cached_iter_node())) {
      LOG_WARN("failed to prepare cached iter node for mds get", KR(ret));
    } else if (OB_FAIL(resources.access_ctx->init(
                   *resources.scan_param,
                   *resources.store_ctx,
                   read_version_range,
                   resources.cached_iter_node_))) {
      LOG_WARN("failed to init access ctx for mds get", KR(ret), KPC(resources.scan_param.ptr()), KPC(resources.store_ctx.ptr()));
    } else if (OB_FAIL(resources.bind_tablet_for_get())) {
      LOG_WARN("failed to bind tablet for mds get", KR(ret), K(resources.ls_mds_handle));
    } else if (OB_FAIL(resources.datum_rowkey.from_rowkey(resources.rowkey, allocator))) {
      LOG_WARN("failed to convert mds rowkey to datum rowkey", KR(ret), K(resources.rowkey));
    } else if (OB_FAIL(resources.init_and_open_single_merge())) {
      LOG_WARN("failed to init and open single merge for mds get", KR(ret), K(resources.datum_rowkey));
    }
  }
  diagnose_info.is_follower = is_follower;
  diagnose_info.effective_read_snapshot = effective_read_snapshot;
  return ret;
}

// Stage 2 of read_mds_row_: consume exactly one row from the opened
// SingleMerge, dispatch into read_result (row_info / typed KV), capture
// logging values, and verify the SSTable scan terminates at ITER_END.
// Propagates OB_ITER_END unchanged so callers can translate it to
// OB_EMPTY_RESULT.
ERRSIM_POINT_DEF(EN_DEBUG_SYNC_MDS_TABLET_STATUS_READ_EMPTY);
int ObMdsTable::read_and_verify_single_row_(
    const ObMdsReadArg &read_arg,
    ObMdsReadResult &read_result,
    ObMdsSingleMergeResources &resources,
    ObMdsSingleReadDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *src_row = nullptr;
  if (OB_ISNULL(resources.single_merge_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("single merge is null", KR(ret), K(read_arg));
  } else if (OB_FAIL(resources.single_merge_->get_next_row(src_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next row from mds", K(ret));
    }
  } else if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null", KR(ret), K(read_arg));
  } else if (read_arg.need_row_info_
             && OB_FAIL(fill_mds_row_read_info_(*src_row, *read_result.row_info_))) {
    LOG_WARN("failed to fill mds row read info", KR(ret), K(read_arg));
  } else if (read_arg.need_kv_
             && OB_FAIL(read_result.kv_convert_func_(
                 read_arg.allocator_, *src_row, read_result.key_, read_result.value_, *read_result.meta_))) {
    LOG_WARN("failed to convert mds row to kv result", KR(ret), K(read_arg));
  } else {
    // Capture logging values from src_row BEFORE the verify call below.
    // Subsequent calls to single_merge_->get_next_row may reuse/reset the
    // underlying row buffer, so dereferencing src_row afterwards is unsafe.
    const int64_t trans_version_col_idx =
        static_cast<int64_t>(ObMdsTabletSchemaDef::MDS_TABLET_SSTABLE_COL_IDX::TRANS_VERSION);
    diagnose_info.result_trans_version = src_row->storage_datums_[trans_version_col_idx].get_int();
    diagnose_info.result_uncommitted = (diagnose_info.result_trans_version == INT64_MAX);
    const int64_t writer_tx_id_col_idx =
        ObMdsTabletSchemaDef::get_phy_col_idx(
            src_row->get_column_count(),
            ObMdsTabletSchemaDef::MDS_TABLET_SCHEMA_COL_IDX::WRITER_TRANS_ID);
    diagnose_info.writer_tx_id = src_row->storage_datums_[writer_tx_id_col_idx].get_int();
    // Verify single-row uniqueness: the rowkey is fully qualified so the
    // SSTable scan must terminate after exactly one row.
    blocksstable::ObDatumRow *next_row = nullptr;
    const int tmp_ret = resources.single_merge_->get_next_row(next_row);
    if (OB_ITER_END != tmp_ret) {
      ret = (OB_SUCCESS == tmp_ret) ? OB_ERR_UNEXPECTED : tmp_ret;
      LOG_WARN("single row mds should reach ITER_END", KR(ret), K(tmp_ret), K(resources.datum_rowkey), KP(next_row));
    } else {
      diagnose_info.has_result_row = true;
    }
  }

  if (OB_ITER_END == ret
      && thread_tablet_status_read_empty_diag_
      && read_arg.mds_unit_id_ == static_cast<uint8_t>(
          mds_type_id_detail::TypeIdOf<mds::DummyKey, ObTabletCreateDeleteMdsUserData>::value)) {
    FLOG_INFO("mds tablet status read empty",
              K(ret),
              K_(ls_id),
              K(read_arg),
              K(diagnose_info));
    while (EN_DEBUG_SYNC_MDS_TABLET_STATUS_READ_EMPTY) {
      ob_usleep(10_s);
      LOG_ERROR("wait for debug attach, EN_DEBUG_SYNC_MDS_TABLET_STATUS_READ_EMPTY is set");
    }
  }

  return ret;
}

int ObMdsTable::flush_frozen_memtables_(ObTabletHandle &mds_tablet_handle, const int64_t trace_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableHandleV2, 2> memtable_handles;
  if (OB_ISNULL(mds_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds tablet is null", K(ret), K_(ls_id));
  } else if (OB_FAIL(mds_tablet_handle.get_obj()->get_all_memtables_from_memtable_mgr(memtable_handles))) {
    LOG_WARN("failed to get all mds memtables", K(ret), K_(ls_id));
  } else if (memtable_handles.empty()) {
    // Common when idle: active memtable is created lazily on next write.
    LOG_DEBUG("mds memtable handles is empty, skip flush once", K_(ls_id));
  } else {
    // After freeze, a new active memtable is created lazily on next write, so
    // there can be a single frozen memtable with no active. Mini merge dag is
    // unique per (ls_id, tablet_id); only schedule when oldest is frozen and
    // its right boundary has been resolved. Async freeze may mark frozen
    // earlier; scheduling before READY_FOR_FLUSH only creates an
    // empty/NO_NEED_MERGE dag.
    ObITabletMemtable *memtable = nullptr;
    if (OB_FAIL(memtable_handles.at(0).get_tablet_memtable(memtable))) {
      LOG_WARN("failed to get oldest mds memtable", K(ret), K_(ls_id));
    } else if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("oldest mds memtable is null", K(ret), K_(ls_id));
    } else if (memtable->is_frozen_memtable() && memtable->can_be_minor_merged()) {
      memtable->set_trace_id(trace_id);
      if (OB_FAIL(memtable->flush(ls_id_))) {
        if (OB_NO_NEED_UPDATE == ret || OB_EAGAIN == ret || OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("mds memtable flush failed", K(ret), KPC(memtable));
        }
      } else {
        LOG_INFO("schedule mds memtable flush dag successfully", K_(ls_id), "table_key", memtable->get_key());
      }
    } else if (memtable->is_frozen_memtable()
               && 0 != memtable->get_frozen_time()
               && ObTimeUtility::current_time() - memtable->get_frozen_time() > 30_s
               && REACH_TIME_INTERVAL(10_s)) {
      LOG_WARN("oldest mds memtable frozen but not ready for minor merge for long time",
               K_(ls_id), K(trace_id), KPC(memtable));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
