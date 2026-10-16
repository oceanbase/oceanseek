/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_table_access_context.h"
#include "share/ob_fork_table_util.h"
#include "storage/access/ob_mds_filter_mgr.h"
#include "storage/access/ob_index_skip_scanner.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
int ObTableAccessContext::init_column_scale_info(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected tenant mode", K(ret));
  } else if (OB_ISNULL(scan_param.table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected table parameter to init column scale info", K(ret), KPC(scan_param.table_param_));
  } else {
    const ObIArray<share::schema::ObColumnParam *> *out_col_param = scan_param.table_param_->get_read_info().get_columns();
    const ObIArray<int32_t> *out_col_project = &scan_param.table_param_->get_output_projector();
    for (int64_t i = 0; OB_SUCC(ret) && i < out_col_project->count(); ++i) {
      share::schema::ObColumnParam *col_param = NULL;
      int32_t idx = out_col_project->at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= out_col_param->count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid project idx", K(ret), K(idx), K(out_col_param->count()));
      } else if (OB_ISNULL(col_param = out_col_param->at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column param is null", K(ret), K(idx));
      }
    }
    STORAGE_LOG(DEBUG, "check need fill scale", K(ret), KPC(out_col_project), KPC(out_col_param));
  }
  return ret;
}

ObTableAccessContext::ObTableAccessContext()
  : is_inited_(false),
    is_fork_ctx_(false),
    use_fuse_row_cache_(false),
    need_scn_(false),
    need_release_mview_scan_info_(true),
    need_release_mds_filter_(true),
    timeout_(0),
    query_flag_(),
    sql_mode_(0),
    micro_block_handle_mgr_(),
    store_ctx_(NULL),
    limit_param_(NULL),
    stmt_allocator_(NULL),
    allocator_(NULL),
    scan_mem_(nullptr),
    table_scan_stat_(NULL),
    table_store_stat_(),
    out_cnt_(0),
    trans_version_range_(),
    range_array_pos_(nullptr),
    merge_scn_(),
    lob_allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    lob_locator_helper_(nullptr),
    cached_iter_node_(nullptr),
    stmt_iter_pool_(nullptr),
    cg_iter_pool_(nullptr),
    cg_param_pool_(nullptr),
    block_row_store_(nullptr),
    sample_filter_(nullptr),
    trans_state_mgr_(nullptr),
    mview_scan_info_(nullptr),
    mds_filter_mgr_(nullptr),
    scan_resume_point_(nullptr),
    row_scan_cnt_(nullptr),
    skip_scan_factory_(nullptr),
    is_inc_major_query_(false),
    fork_read_snapshot_version_(-1)
{
  merge_scn_.set_max();
}

ObTableAccessContext::~ObTableAccessContext()
{
  reset_lob_locator_helper();
  cached_iter_node_ = nullptr;
  if (is_fork_ctx_) {
    stmt_iter_pool_ = nullptr;
    cg_iter_pool_ = nullptr;
    skip_scan_factory_ = nullptr;
    sample_filter_ = nullptr;
  } else {
    if (nullptr != stmt_iter_pool_) {
      stmt_iter_pool_->~ObStoreRowIterPool<ObStoreRowIterator>();
      if (OB_NOT_NULL(stmt_allocator_)) {
        stmt_allocator_->free(stmt_iter_pool_);
      }
      stmt_iter_pool_ = nullptr;
    }
    if (nullptr != cg_iter_pool_) {
      cg_iter_pool_->~ObStoreRowIterPool<ObICGIterator>();
      if (OB_NOT_NULL(stmt_allocator_)) {
        stmt_allocator_->free(cg_iter_pool_);
      }
      cg_iter_pool_ = nullptr;
    }
    if (OB_UNLIKELY(nullptr != skip_scan_factory_)) {
      skip_scan_factory_->~ObIndexSkipScanFactory();
      if (OB_NOT_NULL(stmt_allocator_)) {
        stmt_allocator_->free(skip_scan_factory_);
      }
      skip_scan_factory_ = nullptr;
    }
    if (OB_UNLIKELY(nullptr != sample_filter_)) {
      ObRowSampleFilterFactory::destroy_sample_filter(sample_filter_);
    }
  }
  if (OB_UNLIKELY(need_release_mview_scan_info_ && nullptr != mview_scan_info_)) {
    release_mview_scan_info(stmt_allocator_, mview_scan_info_);
    need_release_mview_scan_info_ = false;
  } else {
    mview_scan_info_ = nullptr;
  }
  if (OB_UNLIKELY(need_release_mds_filter_ && nullptr != mds_filter_mgr_)) {
    ObMDSFilterMgrFactory::destroy_mds_filter_mgr(mds_filter_mgr_);
  } else {
    mds_filter_mgr_ = nullptr;
  }
}

int ObTableAccessContext::build_lob_locator_helper(ObTableScanParam &scan_param,
                                                   const ObStoreCtx &ctx,
                                                   const ObVersionRange &trans_version_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  // reset lob allocator for mem used;
  reset_lob_locator_helper();
  // locator is used for all types of lobs
  if (OB_UNLIKELY(nullptr == scan_param.table_param_ || nullptr == stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build lob locator helper", K(ret), K(scan_param), KP_(stmt_allocator));
  } else if (!scan_param.table_param_->use_lob_locator()) {
    lob_locator_helper_ = nullptr;
  } else if (!scan_param.table_param_->enable_lob_locator_v2() && !lib::is_oracle_mode()) {
    // if lob locator v2 is enabled, locator will be used for all types of lobs, including mysql mode
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected tenant mode", K(ret), K(lib::is_oracle_mode()));
  } else if (OB_ISNULL(buf = lob_allocator_.alloc(sizeof(ObLobLocatorHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for ObLobLocatorHelper", K(ret));
  } else if (FALSE_IT(lob_locator_helper_ = new (buf) ObLobLocatorHelper())) {
  } else if (OB_FAIL(lob_locator_helper_->init(scan_param,
                                               ctx,
                                               scan_param.ls_id_,
                                               trans_version_range.snapshot_version_))) {
    STORAGE_LOG(WARN, "Failed to init lob locator helper",
      K(ret), KPC(scan_param.table_param_), K(scan_param.ls_id_), K(trans_version_range));
    reset_lob_locator_helper();
  } else {
    STORAGE_LOG(DEBUG, "succ to init lob locator helper", KPC(lob_locator_helper_));
  }

  return ret;
}

int ObTableAccessContext::build_lob_locator_helper(const ObStoreCtx &ctx,
                                                   const ObVersionRange &trans_version_range)
{
  // lob locator for internal routine, no rowid
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  // reset lob allocator for mem used;
  reset_lob_locator_helper();
  // locator is used for all types of lobs
  if (OB_ISNULL(buf = lob_allocator_.alloc(sizeof(ObLobLocatorHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for ObLobLocatorHelper", K(ret));
  } else if (FALSE_IT(lob_locator_helper_ = new (buf) ObLobLocatorHelper())) {
  } else if (OB_FAIL(lob_locator_helper_->init(tablet_id_.id(),
                                               tablet_id_.id(),
                                               ctx,
                                               ls_id_,
                                               trans_version_range.snapshot_version_))) {
    STORAGE_LOG(WARN, "Failed to init lob locator helper limit", K(ret), K(ls_id_), K(trans_version_range));
    reset_lob_locator_helper();
  } else {
    STORAGE_LOG(DEBUG, "succ to init lob locator helper", KPC(lob_locator_helper_));
  }

  return ret;
}

int ObTableAccessContext::init(ObTableScanParam &scan_param,
                               ObStoreCtx &ctx,
                               const ObVersionRange &trans_version_range,
                               CachedIteratorNode *cached_iter_node)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_ && stmt_allocator_ != scan_param.allocator_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "Unexpected access context reuse scenario",
        K(ret), KP(stmt_allocator_), KP(allocator_), K(scan_param));
  } else if (OB_FAIL(init_scan_allocator(scan_param))) {
    LOG_WARN("Failed to init scan allocator", K(ret));
  } else {
    stmt_allocator_ = scan_param.allocator_;
    cached_iter_node_ = cached_iter_node;
    ls_id_ = scan_param.ls_id_;
    tablet_id_ = scan_param.tablet_id_;
    query_flag_ = scan_param.scan_flag_;
    sql_mode_ = scan_param.sql_mode_;
    timeout_ = scan_param.timeout_;
    store_ctx_ = &ctx;
    table_scan_stat_ = &scan_param.main_table_scan_stat_;
    limit_param_ = scan_param.limit_param_.is_valid() ? &scan_param.limit_param_ : NULL;
    table_scan_stat_->reset();
    table_store_stat_.in_bf_cache_threshold_ = scan_param.in_bf_cache_threshold_;
    table_store_stat_.in_fuse_row_cache_threshold_ = scan_param.in_fuse_row_cache_threshold_;
    table_store_stat_.in_row_cache_threshold_ = scan_param.in_row_cache_threshold_;
    trans_version_range_ = trans_version_range;
    need_scn_ = scan_param.need_scn_ || 
                (nullptr != scan_param.table_param_ && OB_INVALID_INDEX != scan_param.table_param_->get_read_info().get_trans_col_index());
    range_array_pos_ = &scan_param.range_array_pos_;
    use_fuse_row_cache_ = false;
    scan_resume_point_ = scan_param.scan_resume_point_;
    row_scan_cnt_ = scan_param.row_scan_cnt_;
    is_inc_major_query_ = false;
    fork_read_snapshot_version_ = 0;
    if(OB_FAIL(build_lob_locator_helper(scan_param, ctx, trans_version_range))) {
      STORAGE_LOG(WARN, "Failed to build lob locator helper", K(ret));
      // new static engine do not need fill scale
    } else if (lib::is_oracle_mode() && OB_ISNULL(scan_param.output_exprs_)
        && OB_FAIL(init_column_scale_info(scan_param))) {
      LOG_WARN("init column scale info failed", K(ret), K(scan_param));
    } else if (micro_block_handle_mgr_.is_valid()) {
      // Note: update effective_tablet_id for reused micro_block_handle_mgr_
      micro_block_handle_mgr_.set_effective_tablet_id(tablet_id_);
    } else if (!micro_block_handle_mgr_.is_valid()
               && OB_FAIL(micro_block_handle_mgr_.init(
                  static_cast<sql::ObStoragePushdownFlag>(scan_param.pd_storage_flag_).is_enable_prefetch_limiting(),
                  tablet_id_,
                  table_store_stat_,
                  table_scan_stat_,
                  query_flag_))) {
      LOG_WARN("Fail to init micro block handle mgr", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (scan_param.sample_info_.is_row_sample() 
        && OB_FAIL(ObRowSampleFilterFactory::build_sample_filter(
          scan_param.sample_info_,
          sample_filter_,
          scan_param.op_,
          query_flag_.is_reverse_scan(),
          scan_param.allocator_))) {
      LOG_WARN("Failed to build sample filter", K(ret), K(scan_param));
    } else if (scan_param.use_index_skip_scan() && OB_FAIL(alloc_skip_scan_factory())) {
      LOG_WARN("Failed to alloc skip scan factory", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableAccessContext::init(const common::ObQueryFlag &query_flag,
                               ObStoreCtx &ctx,
                               ObIAllocator &allocator,
                               ObIAllocator &stmt_allocator,
                               const ObVersionRange &trans_version_range,
                               memtable::ObMvccMdsFilter *mvcc_mds_filter,
                               const bool for_exist)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    const bool enable_limit = false;
    query_flag_ = query_flag;
    use_fuse_row_cache_ = false;
    store_ctx_ = &ctx;
    timeout_ = ctx.timeout_;
    allocator_ = &allocator;
    stmt_allocator_ = &stmt_allocator;
    trans_version_range_ = trans_version_range;
    ls_id_ = ctx.ls_id_;
    tablet_id_ = ctx.tablet_id_;
    is_inc_major_query_ = false;
    fork_read_snapshot_version_ = 0;
    // handle lob types without ObTableScanParam:
    // 1. use lob locator instead of full lob data
    // 2. without rowkey, since need not send result to dbmslob/client
    // 3. tablet id/ table id here maybe invalid, call update_lob_locator_ctx to fix
    // 4. only init lob locator helper when nessary?
    // exist do not need lob locator
    if (!for_exist && OB_FAIL(build_lob_locator_helper(ctx, trans_version_range))) {
      STORAGE_LOG(WARN, "Failed to build lob locator helper", K(ret));
    } else if (micro_block_handle_mgr_.is_valid()) {
      // Note: update effective_tablet_id for reused micro_block_handle_mgr_
      micro_block_handle_mgr_.set_effective_tablet_id(tablet_id_);
    } else if (!micro_block_handle_mgr_.is_valid()
               && OB_FAIL(micro_block_handle_mgr_.init(enable_limit, tablet_id_, table_store_stat_,
                  table_scan_stat_,query_flag_))) {
      LOG_WARN("Fail to init micro block handle mgr", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(mvcc_mds_filter) && mvcc_mds_filter->is_valid()) {
      need_release_mds_filter_ = false;
      mds_filter_mgr_ = mvcc_mds_filter->mds_filter_mgr_;
      if (OB_FAIL(ctx.init_mds_filter(*mvcc_mds_filter))) {
        LOG_WARN("failed to init mds filter on StoreCtx", KR(ret), KPC(mvcc_mds_filter));
      }
    } else {
      ctx.clear_mds_filter();
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableAccessContext::init(const common::ObQueryFlag &query_flag,
                               ObStoreCtx &ctx,
                               common::ObIAllocator &allocator,
                               const common::ObVersionRange &trans_version_range,
                               memtable::ObMvccMdsFilter *mvcc_mds_filter,
                               CachedIteratorNode *cached_iter_node)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    const bool enable_limit = false;
    query_flag_ = query_flag;
    use_fuse_row_cache_ = false;
    store_ctx_ = &ctx;
    timeout_ = ctx.timeout_;
    allocator_ = &allocator;
    stmt_allocator_ = &allocator;
    trans_version_range_ = trans_version_range;
    ls_id_ = ctx.ls_id_;
    tablet_id_ = ctx.tablet_id_;
    lob_locator_helper_ = nullptr;
    cached_iter_node_ = cached_iter_node;
    is_inc_major_query_ = false;
    fork_read_snapshot_version_ = 0;
    if (micro_block_handle_mgr_.is_valid()) {
      // Note: update effective_tablet_id for reused micro_block_handle_mgr_
      micro_block_handle_mgr_.set_effective_tablet_id(tablet_id_);
    } else if (!micro_block_handle_mgr_.is_valid()
        && OB_FAIL(micro_block_handle_mgr_.init(enable_limit, tablet_id_, table_store_stat_, table_scan_stat_, query_flag_))) {
      LOG_WARN("Fail to init micro block handle mgr", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(mvcc_mds_filter) && mvcc_mds_filter->is_valid()) {
      need_release_mds_filter_ = false;
      mds_filter_mgr_ = mvcc_mds_filter->mds_filter_mgr_;
      if (OB_FAIL(ctx.init_mds_filter(*mvcc_mds_filter))) {
        LOG_WARN("failed to init mds filter on StoreCtx", KR(ret), KPC(mvcc_mds_filter));
      }
    } else {
      ctx.clear_mds_filter();
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableAccessContext::init_for_mview(common::ObIAllocator *allocator, const ObTableAccessContext &access_ctx, ObStoreCtx &store_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_ && stmt_allocator_ != access_ctx.stmt_allocator_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    stmt_allocator_ = access_ctx.stmt_allocator_;
    allocator_ = allocator;
    cached_iter_node_ = nullptr;
    ls_id_ = access_ctx.ls_id_;
    tablet_id_ = access_ctx.tablet_id_;
    query_flag_ = access_ctx.query_flag_;
    sql_mode_ = access_ctx.sql_mode_;
    timeout_ = access_ctx.timeout_;
    store_ctx_ = &store_ctx;
    table_scan_stat_ = nullptr;
    limit_param_ = nullptr;
    trans_version_range_.base_version_ = 0;
    trans_version_range_.snapshot_version_ = store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    need_scn_ = false;
    range_array_pos_ = nullptr;
    use_fuse_row_cache_ = true;
    lob_locator_helper_ = nullptr;
    is_inc_major_query_ = false;
    fork_read_snapshot_version_ = access_ctx.fork_read_snapshot_version_;
    if (micro_block_handle_mgr_.is_valid()) {
      // Note: update effective_tablet_id for reused micro_block_handle_mgr_
      micro_block_handle_mgr_.set_effective_tablet_id(tablet_id_);
    } else if (!micro_block_handle_mgr_.is_valid() &&
        OB_FAIL(micro_block_handle_mgr_.init(
                false,
                tablet_id_,
                table_store_stat_,
                table_scan_stat_,
                query_flag_))) {
      LOG_WARN("Failed to init micro block handle mgr", K(ret));
    }
    if (OB_SUCC(ret)) {
      mview_scan_info_ = access_ctx.mview_scan_info_;
      need_release_mview_scan_info_ = false;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableAccessContext::enter_fork_snapshot(
    const share::SCN &fork_snapshot_scn,
    const common::ObTabletID &src_tablet_id,
    share::SCN &saved_snapshot_version,
    bool &saved_fork_source_check,
    common::ObTabletID &saved_tablet_id,
    common::ObTabletID &saved_effective_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_ctx_) || OB_UNLIKELY(!src_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork source read context", K(ret), KP_(store_ctx), K(src_tablet_id));
  } else if (OB_FAIL(store_ctx_->enter_fork_snapshot(fork_snapshot_scn,
                                                    saved_snapshot_version,
                                                    saved_fork_source_check))) {
    LOG_WARN("failed to enter fork snapshot", K(ret), K(fork_snapshot_scn));
  } else {
    saved_tablet_id = tablet_id_;
    saved_effective_tablet_id = micro_block_handle_mgr_.get_effective_tablet_id();
    tablet_id_ = src_tablet_id;
    micro_block_handle_mgr_.set_effective_tablet_id(src_tablet_id);
  }
  return ret;
}

void ObTableAccessContext::exit_fork_snapshot(
    const share::SCN &saved_snapshot_version,
    const bool saved_fork_source_check,
    const common::ObTabletID &saved_tablet_id,
    const common::ObTabletID &saved_effective_tablet_id)
{
  tablet_id_ = saved_tablet_id;
  micro_block_handle_mgr_.set_effective_tablet_id(saved_effective_tablet_id);
  store_ctx_->exit_fork_snapshot(saved_snapshot_version, saved_fork_source_check);
}

int ObTableAccessContext::init_for_fork(ObTableAccessContext &other,
                                        ObStoreCtx *store_ctx,
                                        const share::ObForkTabletInfo &fork_info)
{
  int ret = OB_SUCCESS;
  const common::ObTabletID &src_tablet_id = fork_info.get_src_tablet_id();
  const int64_t fork_snapshot_version = fork_info.get_fork_snapshot_version();
  if (OB_UNLIKELY(is_inited_
                  && (!is_fork_ctx_ || stmt_allocator_ != other.stmt_allocator_))) {
    ret = OB_ERR_SYS;
    LOG_WARN("[FORKTABLE] unexpected fork access context reuse", K(ret), K(other));
  } else if (OB_UNLIKELY(!other.is_valid() || nullptr == store_ctx
                         || !src_tablet_id.is_valid() || fork_snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid argument to init fork access context", K(ret), K(other),
        KP(store_ctx), K(src_tablet_id), K(fork_snapshot_version));
  } else {
    is_fork_ctx_ = true;
    use_fuse_row_cache_ = false;  // fork overlay 读禁用 fuse row cache
    need_scn_ = other.need_scn_;
    need_release_mview_scan_info_ = false;
    need_release_mds_filter_ = false;
    timeout_ = other.timeout_;
    ls_id_ = other.ls_id_;
    tablet_id_ = src_tablet_id;
    query_flag_.assign_for_fork(other.query_flag_);
    query_flag_.set_not_use_row_cache();
    sql_mode_ = other.sql_mode_;
    store_ctx_ = store_ctx;
    limit_param_ = other.limit_param_;
    stmt_allocator_ = other.stmt_allocator_;
    allocator_ = other.allocator_;
    table_scan_stat_ = other.table_scan_stat_;
    table_store_stat_.reset();
    table_store_stat_.in_bf_cache_threshold_ = other.table_store_stat_.in_bf_cache_threshold_;
    table_store_stat_.in_row_cache_threshold_ = other.table_store_stat_.in_row_cache_threshold_;
    table_store_stat_.in_fuse_row_cache_threshold_ =
        other.table_store_stat_.in_fuse_row_cache_threshold_;
    out_cnt_ = other.out_cnt_;
    trans_version_range_ = other.trans_version_range_;
    trans_version_range_.snapshot_version_ = fork_snapshot_version;
    if (trans_version_range_.multi_version_start_ > fork_snapshot_version) {
      trans_version_range_.multi_version_start_ = fork_snapshot_version;
    }
    range_array_pos_ = other.range_array_pos_;
    merge_scn_ = other.merge_scn_;
    lob_locator_helper_ = other.lob_locator_helper_;
    cached_iter_node_ = other.cached_iter_node_;
    stmt_iter_pool_ = other.stmt_iter_pool_;
    cg_iter_pool_ = other.cg_iter_pool_;
    cg_param_pool_ = other.cg_param_pool_;
    block_row_store_ = other.block_row_store_;
    sample_filter_ = other.sample_filter_;
    trans_state_mgr_ = other.trans_state_mgr_;
    mview_scan_info_ = other.mview_scan_info_;
    mds_filter_mgr_ = nullptr;
    scan_resume_point_ = other.scan_resume_point_;
    row_scan_cnt_ = other.row_scan_cnt_;
    skip_scan_factory_ = other.skip_scan_factory_;
    is_inc_major_query_ = other.is_inc_major_query_;
    fork_read_snapshot_version_ = fork_snapshot_version;
    if (micro_block_handle_mgr_.is_valid()) {
      // 异步 I/O 可能跨越 Tablet 重扫，复用句柄管理器以覆盖其完整生命周期。
      micro_block_handle_mgr_.set_effective_tablet_id(tablet_id_);
    } else if (OB_FAIL(micro_block_handle_mgr_.init(false/*enable_limit*/,
                                                   tablet_id_,
                                                   table_store_stat_,
                                                   table_scan_stat_,
                                                   query_flag_))) {
      LOG_WARN("[FORKTABLE] failed to init micro block handle mgr", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObTableAccessContext::inc_micro_access_cnt()
{
  ++table_store_stat_.micro_access_cnt_;
}

int ObTableAccessContext::init_scan_allocator(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!(scan_param.sample_info_.is_block_sample() ||
                  scan_param.sample_info_.is_ddl_block_sample()))) {
    allocator_ = scan_param.scan_allocator_;
  } else {
    if (scan_mem_ == nullptr) {
      lib::ContextParam param;
      param.set_mem_attr(scan_param.tenant_id_,
                        ObModIds::OB_TABLE_SCAN_ITER,
                        ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(scan_mem_, param))) {
        LOG_WARN("fail to create entity", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      allocator_ = &scan_mem_->get_arena_allocator();
    }
  }
  return ret;
}

int ObTableAccessContext::init_mview_scan_info(const int64_t multi_version_start, const sql::ObExprPtrIArray *op_filters, sql::ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (nullptr != mview_scan_info_) {
  } else if (OB_FAIL(build_mview_scan_info_if_need(query_flag_, op_filters, eval_ctx, stmt_allocator_, mview_scan_info_))) {
    LOG_WARN("Failed to build mview scan info", K(ret));
  }
  if (OB_FAIL(ret) || nullptr == mview_scan_info_) {
  } else if (OB_FAIL(mview_scan_info_->check_and_update_version_range(multi_version_start, trans_version_range_))) {
    LOG_WARN("Failed to check and update version range", K(ret), K(multi_version_start), K(trans_version_range_), KPC_(mview_scan_info));
  }
  return ret;
}

void ObTableAccessContext::reset()
{
  reset_lob_locator_helper();
  if (OB_UNLIKELY(need_release_mview_scan_info_ && nullptr != mview_scan_info_)) {
    release_mview_scan_info(stmt_allocator_, mview_scan_info_);
    need_release_mview_scan_info_ = false;
  } else {
    mview_scan_info_ = nullptr;
  }
  cached_iter_node_ = nullptr;
  if (is_fork_ctx_) {
    stmt_iter_pool_ = nullptr;
    cg_iter_pool_ = nullptr;
    skip_scan_factory_ = nullptr;
  } else {
    if (nullptr != stmt_iter_pool_) {
      stmt_iter_pool_->~ObStoreRowIterPool<ObStoreRowIterator>();
      if (OB_NOT_NULL(stmt_allocator_)) {
        stmt_allocator_->free(stmt_iter_pool_);
      }
      stmt_iter_pool_ = nullptr;
    }
    if (nullptr != cg_iter_pool_) {
      cg_iter_pool_->~ObStoreRowIterPool<ObICGIterator>();
      if (OB_NOT_NULL(stmt_allocator_)) {
        stmt_allocator_->free(cg_iter_pool_);
      }
      cg_iter_pool_ = nullptr;
    }
    if (OB_UNLIKELY(nullptr != skip_scan_factory_)) {
      skip_scan_factory_->~ObIndexSkipScanFactory();
      if (OB_NOT_NULL(stmt_allocator_)) {
        stmt_allocator_->free(skip_scan_factory_);
      }
      skip_scan_factory_ = nullptr;
    }
  }
  is_inited_ = false;
  timeout_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  query_flag_.reset();
  sql_mode_ = 0;
  if (NULL != store_ctx_) {
    store_ctx_->clear_mds_filter();
    store_ctx_ = NULL;
  }
  micro_block_handle_mgr_.reset();
  limit_param_ = NULL;
  stmt_allocator_ = NULL;
  if (NULL != scan_mem_) {
    DESTROY_CONTEXT(scan_mem_);
    scan_mem_ = NULL;
  }
  allocator_ = NULL;
  table_scan_stat_ = NULL;
  table_store_stat_.reset();
  out_cnt_ = 0;
  trans_version_range_.reset();
  use_fuse_row_cache_ = false;
  range_array_pos_ = nullptr;
  cg_param_pool_ = nullptr;
  block_row_store_ = nullptr;
  if (OB_UNLIKELY(need_release_mds_filter_ && nullptr != mds_filter_mgr_)) {
    ObMDSFilterMgrFactory::destroy_mds_filter_mgr(mds_filter_mgr_);
  } else {
    mds_filter_mgr_ = nullptr;
  }
  scan_resume_point_ = nullptr;
  if (is_fork_ctx_) {
    sample_filter_ = nullptr;
  } else if (OB_UNLIKELY(nullptr != sample_filter_)) {
    ObRowSampleFilterFactory::destroy_sample_filter(sample_filter_);
  }
  row_scan_cnt_ = nullptr;
  is_inc_major_query_ = false;
  fork_read_snapshot_version_ = -1;
  is_fork_ctx_ = false;
  need_release_mview_scan_info_ = true;
  need_release_mds_filter_ = true;
}

int ObTableAccessContext::rescan_reuse(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_scan_allocator(scan_param))) {
    LOG_WARN("Failed to init scan allocator", K(ret));
  } else {
    out_cnt_ = 0;
    if (nullptr != table_scan_stat_) {
      table_scan_stat_->reset();
    }
    if (nullptr != sample_filter_) {
      sample_filter_->reuse();
    }
  }
  return ret;
}

void ObTableAccessContext::reuse()
{
  timeout_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  query_flag_.reset();
  sql_mode_ = 0;
  if (NULL != store_ctx_) {
    store_ctx_->clear_mds_filter();
    store_ctx_ = NULL;
  }
  limit_param_ = NULL;
  reset_lob_locator_helper();
  if (NULL != scan_mem_) {
    scan_mem_->reuse_arena();
  }
  table_scan_stat_ = NULL;
  out_cnt_ = 0;
  trans_version_range_.reset();
  use_fuse_row_cache_ = false;
  range_array_pos_ = nullptr;
  cg_param_pool_ = nullptr;
  block_row_store_ = nullptr;
  if (nullptr != sample_filter_) {
    sample_filter_->reuse();
  }
  reuse_skip_scan_factory();
  scan_resume_point_ = nullptr;
  row_scan_cnt_ = nullptr;
  is_inc_major_query_ = false;
  fork_read_snapshot_version_ = -1;
}

int ObTableAccessContext::alloc_iter_pool(const bool use_column_store)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObTableAccessContext not inited", K(ret), KPC(this));
  } else if (nullptr == stmt_iter_pool_ && nullptr == cached_iter_node_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = stmt_allocator_->alloc(sizeof(ObStoreRowIterPool<ObStoreRowIterator>)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc row iter pool", K(ret));
    } else {
      stmt_iter_pool_ = new(buf) ObStoreRowIterPool<ObStoreRowIterator>(*stmt_allocator_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (use_column_store && nullptr == cg_iter_pool_) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = stmt_allocator_->alloc(sizeof(ObStoreRowIterPool<ObICGIterator>)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to alloc row iter pool", K(ret));
    } else {
      cg_iter_pool_ = new(buf) ObStoreRowIterPool<ObICGIterator>(*stmt_allocator_);
    }
  }
  return ret;
}

int ObTableAccessContext::check_filtered_by_base_version(ObDatumRow &row)
{
  int ret = OB_SUCCESS;

  bool filtered = false;

  if (nullptr != mds_filter_mgr_) {
    if (row.row_flag_.is_not_exist()) {
      // skip check, already not exist
    } else if (OB_FAIL(mds_filter_mgr_->filter(
            row, filtered, false /*check_filter*/, true /*check_version*/))) {
      LOG_WARN("failed to filter by base version", K(ret));
    } else if (filtered) {
      row.row_flag_.reset();
      row.row_flag_.set_flag(DF_NOT_EXIST);
    }
  }
  return ret;
}

int ObTableAccessContext::combine_to_filter_tree(sql::ObPushdownFilterExecutor *&root_filter, sql::ObPushdownFilterExecutor *sql_pushdown_filter)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (nullptr == mds_filter_mgr_) {
    root_filter = sql_pushdown_filter;
  } else if (OB_FAIL(mds_filter_mgr_->combine_to_filter_tree_with_check(root_filter, sql_pushdown_filter))) {
    // the function will check double combine to filter tree
    // if double combine isn't the same, it will return error
    LOG_WARN("failed to combine to filter tree", K(ret));
  }

  return ret;
}

int ObTableAccessContext::alloc_skip_scan_factory()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (nullptr != skip_scan_factory_) {
    skip_scan_factory_->reuse();
  } else if (OB_ISNULL(stmt_allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt allocator is null", K(ret));
  } else if (OB_ISNULL(buf = stmt_allocator_->alloc(sizeof(ObIndexSkipScanFactory)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alloc memory for ObIndexSkipScanFactory", K(ret));
  } else {
    skip_scan_factory_ = new (buf) ObIndexSkipScanFactory();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
