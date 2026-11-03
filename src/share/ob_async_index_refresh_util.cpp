/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_async_index_refresh_util.h"
#include "share/ob_debug_sync.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_ddl_common.h"
#include "share/ob_share_util.h"
#include "common/ob_timeout_ctx.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/location_cache/ob_location_service.h"
#include "share/location_cache/ob_location_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/tx/ob_ts_mgr.h"

namespace oceanbase
{
namespace share
{

int ObAsyncIndexRefreshProgress::PartitionTarget::assign(const PartitionTarget &other)
{
  if (memory_tenant_id_ != other.memory_tenant_id_) {
    indexes_.reset();
  }
  tenant_id_ = other.tenant_id_;
  memory_tenant_id_ = other.memory_tenant_id_;
  data_table_id_ = other.data_table_id_;
  tablet_id_ = other.tablet_id_;
  target_scn_ = other.target_scn_;
  refresh_type_ = other.refresh_type_;
  int ret = OB_SUCCESS;
  indexes_.set_attr(ObMemAttr(memory_tenant_id_, "AsyncIdxPrg"));
  if (OB_FAIL(indexes_.assign(other.indexes_))) {
    LOG_WARN("failed to copy async index refresh partition target", K(ret), K(other));
  }
  return ret;
}

int ObAsyncIndexRefreshProgress::PartitionTarget::init(
    const obrpc::ObAsyncIndexRefreshArg &arg, const uint64_t memory_tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id_ = arg.tenant_id_;
  memory_tenant_id_ = memory_tenant_id;
  data_table_id_ = arg.data_table_id_;
  tablet_id_ = arg.tablet_id_.id();
  target_scn_ = arg.target_refresh_scn_;
  refresh_type_ = arg.refresh_type_;
  indexes_.reset();
  indexes_.set_attr(ObMemAttr(memory_tenant_id_, "AsyncIdxPrg"));
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_table_ids_.count(); ++i) {
    const IndexTarget index = {arg.index_table_ids_.at(i), arg.index_tablet_ids_.at(i).id(), arg.is_delete_flags_.at(i)};
    if (OB_FAIL(indexes_.push_back(index))) {
      LOG_WARN("failed to cache async index refresh index target", K(ret), K(i));
    }
  }
  return ret;
}

bool ObAsyncIndexRefreshProgress::PartitionTarget::matches(const obrpc::ObAsyncIndexRefreshArg &arg) const
{
  bool same = tenant_id_ == arg.tenant_id_ && data_table_id_ == arg.data_table_id_
      && tablet_id_ == arg.tablet_id_.id() && target_scn_ == arg.target_refresh_scn_
      && refresh_type_ == arg.refresh_type_ && indexes_.count() == arg.index_table_ids_.count();
  for (int64_t i = 0; same && i < indexes_.count(); ++i) {
    const IndexTarget &index = indexes_.at(i);
    same = index.table_id_ == arg.index_table_ids_.at(i)
        && index.tablet_id_ == arg.index_tablet_ids_.at(i).id()
        && index.is_delete_ == arg.is_delete_flags_.at(i);
  }
  return same;
}

int ObAsyncIndexRefreshProgress::prepare(
    const ObIArray<obrpc::ObAsyncIndexRefreshArg> &targets, const uint64_t memory_tenant_id)
{
  int ret = OB_SUCCESS;
  bool same = memory_tenant_id_ == memory_tenant_id && targets.count() == targets_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < targets.count(); ++i) {
    if (!targets.at(i).is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid async index refresh target", K(ret), K(i), K(targets.at(i)));
    } else if (targets.at(i).tenant_id_ != targets.at(0).tenant_id_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("async index refresh targets belong to different tenants", K(ret), K(i),
          K(targets.at(i)), K(targets.at(0)));
    } else if (same) {
      same = targets_.at(i).matches(targets.at(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (targets.empty()) {
    reset();
  } else if (!same) {
    reset();
    memory_tenant_id_ = memory_tenant_id;
    const ObMemAttr attr(memory_tenant_id_, "AsyncIdxPrg");
    targets_.set_attr(attr);
    succeeded_.set_attr(attr);
    if (OB_FAIL(targets_.prepare_allocate(targets.count()))) {
      LOG_WARN("failed to allocate async index refresh targets", K(ret));
    } else if (OB_FAIL(succeeded_.reserve(targets.count()))) {
      LOG_WARN("failed to reserve async index refresh success bitmap", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < targets.count(); ++i) {
        if (OB_FAIL(targets_.at(i).init(targets.at(i), memory_tenant_id_))) {
          LOG_WARN("failed to cache async index refresh target", K(ret), K(i));
        }
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

bool ObAsyncIndexRefreshUtil::is_refresh_retry_error(const int ret, const int8_t refresh_type)
{
  bool retry = false;
  if (storage::ASYNC_REFRESH_TYPE_MANUAL == refresh_type) {
    retry = OB_OLD_SCHEMA_VERSION == ret || OB_SCHEMA_EAGAIN == ret
        || OB_DDL_SCHEMA_VERSION_NOT_MATCH == ret || OB_NEED_RETRY == ret
        || OB_INVALID_QUERY_TIMESTAMP == ret || OB_TASK_EXPIRED == ret
        || is_master_changed_error(ret) || is_partition_change_error(ret)
        || is_ddl_stmt_packet_retry_err(ret);
  } else if (storage::ASYNC_REFRESH_TYPE_CREATE_INDEX == refresh_type
      || storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL == refresh_type) {
    retry = ObIDDLTask::in_ddl_retry_white_list(ret);
  }
  return retry;
}

// Preserve a fatal error even if other partitions subsequently succeed/retry.
static int record_refresh_error(const int tmp_ret, const int8_t refresh_type, const int current_ret)
{
  int ret = current_ret;
  if (OB_SUCCESS != tmp_ret && (OB_SUCC(ret)
      || (ObAsyncIndexRefreshUtil::is_refresh_retry_error(ret, refresh_type)
          && !ObAsyncIndexRefreshUtil::is_refresh_retry_error(tmp_ret, refresh_type)))) {
    ret = tmp_ret;
  }
  return ret;
}

static int renew_refresh_location(ObLocationService &location_service,
                                  const obrpc::ObAsyncIndexRefreshArg &arg, const int code)
{
  int ret = OB_SUCCESS;
  if (is_location_service_renew_error(code)
      || OB_NOT_MASTER == code || OB_STATE_NOT_MATCH == code
      || OB_LS_NOT_EXIST == code || OB_TABLET_NOT_EXIST == code
      || OB_LS_OFFLINE == code || OB_TENANT_NOT_IN_SERVER == code
      || OB_TIMEOUT == code || OB_CONNECT_ERROR == code
      || OB_RPC_CONNECT_ERROR == code || OB_RPC_SEND_ERROR == code
      || OB_RPC_POST_ERROR == code || OB_PACKET_STATUS_UNKNOWN == code) {
    if (OB_FAIL(location_service.renew_tablet_location(
        arg.tenant_id_, arg.tablet_id_, code, true /*is_nonblock*/))) {
      LOG_WARN("failed to renew async index refresh location", K(ret), K(arg), K(code));
    }
  }
  if (storage::ASYNC_REFRESH_TYPE_MANUAL == arg.refresh_type_ && is_master_changed_error(code)) {
    ObAddr new_leader;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(location_service.get_leader(
        GCONF.cluster_id, arg.tenant_id_, arg.ls_id_, true /*force_renew*/, new_leader))) {
      LOG_WARN("failed to renew leader after manual refresh error", K(tmp_ret), K(code), K(arg));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

ObAsyncIndexRefreshUtil::RefreshDispatcher::RefreshDispatcher(
    const uint64_t tenant_id, const uint64_t data_table_id,
    const int64_t target_scn, const int8_t refresh_type,
    ObLocationService &location_service, obrpc::ObSrvRpcProxy &rpc_proxy,
    ObAsyncIndexRefreshProgress &progress)
  : tenant_id_(tenant_id), memory_tenant_id_(MTL_CTX() ? MTL_ID() : ob_thread_tenant_id()),
    data_table_id_(data_table_id), target_scn_(target_scn),
    refresh_type_(refresh_type), refresh_rpc_timeout_(GCONF._ob_ddl_timeout),
    location_service_(location_service), progress_(progress), rpc_proxy_(rpc_proxy),
    allocator_(ObMemAttr(memory_tenant_id_, "AsyncIdxRpc")), pending_count_(0),
    round_start_ts_(ObTimeUtility::current_time()), rpc_count_(0), skipped_count_(0)
{
  // Cancellation detection belongs to this call, not the shared RPC proxy.
  rpc_proxy_.detect_session_killed(true);
  callbacks_.set_attr(ObMemAttr(memory_tenant_id_, "AsyncIdxRpc"));
}

rpc::frame::ObReqTransport::AsyncCB *ObAsyncIndexRefreshUtil::RefreshDispatcher::Callback::clone(
    const rpc::frame::SPAlloc &) const
{
  return const_cast<Callback *>(this);
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::Callback::process()
{
  return owner_.complete(*this, rcode_.rcode_);
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::Callback::on_error(int err)
{
  // Match synchronous PNIO: only confirmed unsent errors permit routing retries.
  const int code = PNIO_TIMEOUT == err || PNIO_DISCONNECT == err || PNIO_PKT_TERMINATE == err
      ? OB_TIMEOUT : OB_RPC_SEND_ERROR;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(owner_.complete(*this, code))) {
    LOG_WARN("failed to record refresh transport error", K(tmp_ret), K(err));
  }
  // The transport calls on_timeout() again on failure; completion must occur once.
  return ret;
}

void ObAsyncIndexRefreshUtil::RefreshDispatcher::Callback::on_timeout()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(owner_.complete(*this, OB_TIMEOUT))) {
    LOG_WARN("failed to record refresh RPC timeout", K(ret));
  }
}

void ObAsyncIndexRefreshUtil::RefreshDispatcher::Callback::on_invalid()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(owner_.complete(*this, OB_RPC_PACKET_INVALID))) {
    LOG_WARN("failed to record invalid refresh RPC response", K(ret));
  }
}

ObAsyncIndexRefreshUtil::RefreshDispatcher::~RefreshDispatcher()
{
  // Keep callbacks alive even if a future error path exits before collection.
  if (pending_count_ > 0) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(drain_callbacks())) {
      LOG_WARN("failed while draining refresh callbacks during destruction", K(ret));
    }
  }
  reuse_callbacks();
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::post_rpc(
    const ObAddr &server, const int64_t timeout, const obrpc::ObAsyncIndexRefreshArg &arg, Callback &cb)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rpc_proxy_.to(server).by(OB_SYS_TENANT_ID).timeout(timeout).async_index_refresh(arg, &cb))) {
    LOG_WARN("failed to post refresh RPC", K(ret), K(server), K(arg));
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::post_partition(
    const ObAddr &server, const int64_t timeout, const int64_t target_index)
{
  int ret = OB_SUCCESS;
  const auto &arg = args_.at(target_index);
  void *buf = nullptr;
  if (!server.is_valid() || timeout <= 0 || !arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh RPC argument", K(ret), K(server), K(timeout), K(arg));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(Callback)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate refresh RPC callback", K(ret));
  } else {
    Callback *cb = new (buf) Callback(*this, target_index);
    if (OB_FAIL(callbacks_.push_back(cb))) {
      LOG_WARN("failed to append refresh RPC callback", K(ret));
      cb->~Callback();
    } else {
      {
        ObThreadCondGuard guard(cond_);
        ++pending_count_;
      }
      if (OB_FAIL(post_rpc(server, timeout, arg, *cb))) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(complete(*cb, ret))) {
          LOG_WARN("failed to record refresh posting failure", K(tmp_ret), K(ret));
        }
      } else {
        cb->posted_ = true; // Only the sending/waiting thread accesses posted_.
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::complete(Callback &cb, const int code)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  if (cb.done_ || pending_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("duplicate refresh callback completion", K(ret), K(cb.done_), K_(pending_count));
  } else {
    cb.set_ret_code(code);
    cb.done_ = true;
    --pending_count_;
    if (0 == pending_count_ && OB_FAIL(cond_.broadcast())) {
      LOG_WARN("failed to signal refresh RPC completion", K(ret));
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::terminate_rpc(const uint64_t gtid, const uint32_t pkt_id)
{
  int ret = OB_SUCCESS;
  const int err = pn_terminate_pkt(gtid, pkt_id);
  if (0 != err) {
    ret = obrpc::tranlate_to_ob_error(err);
    LOG_WARN("failed to terminate refresh RPC", K(ret), K(err), K(gtid), K(pkt_id));
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::drain_callbacks()
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  bool terminated = false;
  while (pending_count_ > 0) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(cond_.wait(1000)) && OB_TIMEOUT != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to wait for refresh RPCs, keep draining", K(ret));
    }
    if (!terminated && obrpc::is_interrupt_error(THIS_WORKER.check_status())) {
      terminated = true;
      for (int64_t i = 0; i < callbacks_.count(); ++i) {
        Callback &cb = *callbacks_.at(i);
        if (cb.posted_ && !cb.done_) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(terminate_rpc(cb.gtid_, cb.pkt_id_))) {
            terminated = false;
            LOG_WARN("failed to cancel refresh RPC, keep draining", K(tmp_ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

void ObAsyncIndexRefreshUtil::RefreshDispatcher::reuse_callbacks()
{
  // The dispatcher drains every round before reuse or destruction.
  for (int64_t i = 0; i < callbacks_.count(); ++i) {
    callbacks_.at(i)->~Callback();
  }
  callbacks_.reuse();
  allocator_.reuse();
}


int ObAsyncIndexRefreshUtil::RefreshDispatcher::init(
    const ObIArray<ObTabletID> &data_tablet_ids, const ObIArray<ObLSID> &ls_ids,
    const ObIArray<uint64_t> &valid_index_table_ids,
    const ObIArray<ObSEArray<ObTabletID, 16>> &index_tablet_arrays,
    const ObIArray<bool> &is_delete_flags)
{
  int ret = OB_SUCCESS;
  // DDL runs on the coordinator, which need not host the business tenant.
  const ObMemAttr attr(memory_tenant_id_, "AsyncIdxPrg");
  args_.set_attr(attr);
  retry_states_.set_attr(attr);
  // Validate the complete target set before posting any RPC.
  if (OB_FAIL(cond_.init(ObWaitEventIds::ASYNC_RPC_PROXY_COND_WAIT))) {
    LOG_WARN("failed to initialize refresh RPC condition", K(ret));
  } else if (OB_FAIL(args_.prepare_allocate(data_tablet_ids.count()))) {
    LOG_WARN("failed to allocate async index refresh targets", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < args_.count(); ++i) {
    auto &arg = args_.at(i);
    arg.index_table_ids_.set_attr(attr);
    arg.index_tablet_ids_.set_attr(attr);
    arg.is_delete_flags_.set_attr(attr);
    arg.tenant_id_ = tenant_id_;
    arg.ls_id_ = ls_ids.at(i);
    arg.tablet_id_ = data_tablet_ids.at(i);
    arg.target_refresh_scn_ = target_scn_;
    arg.data_table_id_ = data_table_id_;
    arg.refresh_type_ = refresh_type_;
    if (OB_FAIL(arg.index_table_ids_.assign(valid_index_table_ids))) {
      LOG_WARN("failed to assign refresh index table ids", K(ret), K(i));
    } else if (OB_FAIL(arg.is_delete_flags_.assign(is_delete_flags))) {
      LOG_WARN("failed to assign refresh delete flags", K(ret), K(i));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_arrays.count(); ++j) {
        if (OB_FAIL(arg.index_tablet_ids_.push_back(index_tablet_arrays.at(j).at(i)))) {
          LOG_WARN("failed to append refresh index tablet", K(ret), K(i), K(j));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(progress_.prepare(args_, memory_tenant_id_))) {
    LOG_WARN("failed to prepare async index refresh progress", K(ret));
  } else if (OB_FAIL(retry_states_.prepare_allocate(args_.count()))) {
    LOG_WARN("failed to allocate refresh retry states", K(ret));
  } else if (OB_FAIL(callbacks_.reserve(args_.count()))) {
    LOG_WARN("failed to reserve async index refresh callbacks", K(ret));
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::send_partition(const int64_t i)
{
  int ret = OB_SUCCESS;
  auto &state = retry_states_.at(i);
  const auto &arg = args_.at(i);
  ObAddr leader_addr;
  common::ObTimeoutCtx timeout_ctx;
  if (0 == state.deadline_) {
    state.deadline_ = ObTimeUtility::current_time() + refresh_rpc_timeout_;
    const int64_t worker_deadline = THIS_WORKER.get_timeout_ts();
    if (worker_deadline > 0) {
      state.deadline_ = MIN(state.deadline_, worker_deadline);
    }
  }
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, refresh_rpc_timeout_))) {
    LOG_WARN("failed to initialize refresh RPC timeout", K(ret), K(arg));
  } else if (OB_FAIL(timeout_ctx.set_abs_timeout(
      MIN(timeout_ctx.get_abs_timeout(), state.deadline_)))) {
    LOG_WARN("failed to set refresh RPC deadline", K(ret), K(arg), K(state.deadline_));
  } else {
    state.deadline_ = MIN(state.deadline_, timeout_ctx.get_abs_timeout());
    if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("refresh RPC deadline expired before leader lookup", K(ret), K(arg), K(state.deadline_));
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("refresh RPC interrupted before leader lookup", K(ret), K(arg));
    } else if (OB_FAIL(location_service_.get_leader(
        GCONF.cluster_id, tenant_id_, arg.ls_id_, state.force_renew_, leader_addr))) {
      LOG_WARN("failed to get refresh RPC leader", K(ret), K(arg), K(state.force_renew_));
    }
  }
  if (OB_SUCC(ret)) {
    if (storage::ASYNC_REFRESH_TYPE_MANUAL == refresh_type_) {
      DEBUG_SYNC(BEFORE_MANUAL_ASYNC_INDEX_REFRESH_RPC);
    }
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("refresh RPC interrupted before dispatch", K(ret), K(arg));
    } else if (timeout_ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("refresh RPC deadline expired before dispatch", K(ret), K(arg), K(state.deadline_));
    } else {
      ++rpc_count_;
      if (OB_FAIL(post_partition(leader_addr, timeout_ctx.get_timeout(), i))) {
        // The caller retains posting failures even when no callback was allocated.
        LOG_WARN("failed to post async index refresh RPC", K(ret), K(arg), K(leader_addr));
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::collect_results()
{
  int ret = OB_SUCCESS;
  // Drain unconditionally before reading callbacks or returning to the caller.
  if (OB_FAIL(drain_callbacks())) {
    LOG_WARN("failed to drain async index refresh callbacks", K(ret));
  }
  for (int64_t i = 0; i < callbacks_.count(); ++i) {
    const Callback &cb = *callbacks_.at(i);
    auto &state = retry_states_.at(cb.target_index_);
    // Allocation failures without callbacks remain in their partition states.
    // Preserve a posting error instead of overwriting it with a response.
    if (OB_SUCCESS == state.result_) {
      state.result_ = cb.get_ret_code();
      if (OB_SUCCESS == state.result_) {
        state.result_ = cb.get_result().ret_code_;
      }
    }
    if (OB_SUCCESS == state.result_) {
      const int mark_ret = progress_.mark_success(cb.target_index_);
      if (OB_FAIL(record_refresh_error(mark_ret, refresh_type_, ret)) && OB_SUCCESS != mark_ret) {
        LOG_WARN("failed to mark async index refresh success", K(ret), K(mark_ret), K(cb.target_index_));
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::prepare_retry(
    const int64_t attempt, const int current_ret, bool &has_pending, int64_t &sleep_us)
{
  int ret = current_ret;
  has_pending = false;
  sleep_us = RETRY_INTERVAL_US;
  for (int64_t i = 0; i < retry_states_.count(); ++i) {
    auto &state = retry_states_.at(i);
    if (progress_.is_success(i)) {
      state.pending_ = false;
    } else if (state.pending_) {
      const int code = state.result_;
      const bool renew_leader = OB_NOT_MASTER == code || OB_NEED_RETRY == code
          || is_server_down_error(code) || OB_TENANT_NOT_IN_SERVER == code
          || OB_LOCATION_NOT_EXIST == code || OB_LS_LOCATION_NOT_EXIST == code
          || OB_LS_LOCATION_LEADER_NOT_EXIST == code;
      const int64_t remain_us = state.deadline_ - ObTimeUtility::current_time();
      // This is the baseline's bounded same-request retry policy, not the
      // outer manual/DDL retry policy. Mapping/schema changes go outward.
      if (OB_SUCC(ret) && (OB_EAGAIN == code || renew_leader)
          && remain_us > 0 && attempt < MAX_RPC_RETRY_COUNT) {
        state.force_renew_ = renew_leader;
        has_pending = true;
        sleep_us = MIN(sleep_us, remain_us);
      } else {
        state.pending_ = false;
        if ((OB_EAGAIN == code || renew_leader) && remain_us <= 0) {
          state.result_ = OB_TIMEOUT;
        }
        if (OB_SUCCESS != state.result_) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(renew_refresh_location(location_service_, args_.at(i), state.result_))) {
            LOG_WARN("failed to renew refresh location, preserve refresh error", K(tmp_ret), K(state.result_));
          }
          LOG_WARN("async index partition refresh failed", K(state.result_), K(args_.at(i)), K(attempt));
          if (!ObAsyncIndexRefreshUtil::is_refresh_retry_error(state.result_, refresh_type_)
              && OB_FAIL(record_refresh_error(state.result_, refresh_type_, ret))) {
            LOG_WARN("stop refresh retries after terminal partition error", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::RefreshDispatcher::execute()
{
  int ret = OB_SUCCESS;
  bool interrupted = false;
  int interrupt_ret = OB_SUCCESS;
  bool has_pending = true;
  for (int64_t attempt = 0; OB_SUCC(ret) && has_pending
      && attempt <= MAX_RPC_RETRY_COUNT; ++attempt) {
    reuse_callbacks(); // The previous round has drained every callback.
    for (int64_t i = 0; !interrupted && i < args_.count(); ++i) {
      const int status_ret = THIS_WORKER.check_status();
      auto &state = retry_states_.at(i);
      if (obrpc::is_interrupt_error(status_ret)) {
        interrupted = true;
        interrupt_ret = status_ret;
      } else if (progress_.is_success(i)) {
        if (0 == attempt) {
          ++skipped_count_;
        }
        state.pending_ = false;
      } else if (state.pending_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(send_partition(i))) {
          LOG_WARN("failed to send async index refresh partition", K(tmp_ret), K(i));
        }
        state.result_ = tmp_ret;
        if (obrpc::is_interrupt_error(tmp_ret)) {
          interrupted = true;
          interrupt_ret = tmp_ret;
        }
      }
    }
    // Collect unconditionally, including when a partition failed to post.
    if (OB_FAIL(collect_results())) {
      LOG_WARN("failed to collect async index refresh round", K(ret), K(attempt));
    }
    // Cancellation takes precedence over errors from cancelled RPCs.
    const int status_ret = interrupted ? interrupt_ret : THIS_WORKER.check_status();
    if (OB_SUCCESS != status_ret) {
      interrupted = true;
      if (OB_FAIL(status_ret)) {
        LOG_WARN("async index refresh interrupted", K(ret), K(attempt));
      }
    }
    has_pending = false;
    int64_t sleep_us = RETRY_INTERVAL_US;
    if (!interrupted && OB_FAIL(prepare_retry(attempt, ret, has_pending, sleep_us))) {
      LOG_WARN("async index refresh round cannot retry", K(ret), K(attempt));
    }
    if (OB_SUCC(ret) && has_pending) {
      ob_usleep(sleep_us);
    }
  }
  // Only unresolved errors survive: a successful retry must clear its earlier
  // failure, while completed partitions remain reusable by the outer caller.
  for (int64_t i = 0; !interrupted && i < retry_states_.count(); ++i) {
    if (!progress_.is_success(i)
        && OB_FAIL(record_refresh_error(retry_states_.at(i).result_, refresh_type_, ret))) {
      LOG_WARN("async index refresh target remains incomplete", K(ret), K(args_.at(i)));
    }
  }
  int64_t success_count = 0;
  for (int64_t i = 0; i < args_.count(); ++i) {
    success_count += progress_.is_success(i) ? 1 : 0;
  }
  if (OB_SUCC(ret) || REACH_TIME_INTERVAL(1000L * 1000L)) {
    LOG_INFO("[ASYNC_INDEX] async index refresh round completed", K(ret), K_(tenant_id),
        K_(data_table_id), K_(target_scn), K_(refresh_type), K_(rpc_count), K_(skipped_count),
        "new_success_count", success_count - skipped_count_,
        "remaining_count", args_.count() - success_count,
        "elapsed_us", ObTimeUtility::current_time() - round_start_ts_);
  }
  return ret;
}

int ObAsyncIndexDDLRefreshTarget::refresh(
    ObIAsyncIndexDDLPublishOp &publish_op, const SCN &specified_scn)
{
  int ret = OB_SUCCESS;
  bool need_publish = true;
  if (specified_scn.is_valid() && !specified_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid specified DDL refresh target", K(ret), K(specified_scn));
  } else if (specified_scn.is_valid() && specified_scn != publish_scn_) {
    reset();
  }
  if (OB_SUCC(ret) && !is_prepared()) {
    SCN candidate = specified_scn;
    if (OB_FAIL(publish_op.prepare(need_publish))) {
      if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_WARN("[ASYNC_INDEX] failed to prepare DDL publish phase", K(ret), K(publish_op), K_(publish_scn));
      }
    } else if (!need_publish) {
      // 没有待刷新的索引，不获取或缓存发布 GTS。
      LOG_INFO("[ASYNC_INDEX] skip DDL publish without targets", K(publish_op));
    } else if (!candidate.is_valid() && OB_FAIL(publish_op.get_gts(candidate))) {
      LOG_WARN("[ASYNC_INDEX] failed to acquire DDL publish target", K(ret), K(candidate), K(publish_op));
    } else if (!candidate.is_valid_and_not_min()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ASYNC_INDEX] invalid DDL publish target", K(ret), K(candidate), K(publish_op));
    } else {
      // Cache before the first partition RPC, including failed publish attempts.
      publish_scn_ = candidate;
      LOG_INFO("[ASYNC_INDEX] cached DDL publish target", K_(publish_scn), K(publish_op));
    }
  }
  if (OB_SUCC(ret) && need_publish) {
    publish_op.set_progress(&progress_);
    if (OB_FAIL(publish_op.publish(publish_scn_))) {
      if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
        LOG_WARN("[ASYNC_INDEX] failed to publish DDL target, retain cached target",
            K(ret), K_(publish_scn), K(publish_op));
      }
    }
    publish_op.set_progress(nullptr);
  }
  return ret;
}

ObAsyncIndexDDLPublishOp::ObAsyncIndexDDLPublishOp(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    common::ObIArray<uint64_t> &index_table_ids,
    const int8_t refresh_type)
  : tenant_id_(tenant_id),
    data_table_id_(data_table_id),
    index_table_ids_(index_table_ids),
    refresh_type_(refresh_type),
    progress_(nullptr)
{
}

int ObAsyncIndexDDLPublishOp::prepare(bool &need_publish)
{
  int ret = OB_SUCCESS;
  need_publish = true;
  if (OB_FAIL(collect_publish_targets())) {
    LOG_WARN("[ASYNC_INDEX] failed to collect targets before DDL publish GTS", K(ret), K(*this));
  } else {
    // 即使只有一个异步索引，也需推进 published SCN，避免查询快照早于新建同步索引的可读版本。
    need_publish = !index_table_ids_.empty();
  }
  return ret;
}

int ObAsyncIndexDDLPublishOp::collect_publish_targets()
{
  int ret = OB_SUCCESS;
  if (storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL == refresh_type_) {
    ret = ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
        tenant_id_, data_table_id_, index_table_ids_,
        true /*retry_on_missing_search_schema*/, true /*allow_unavailable*/);
    if (OB_FAIL(ret)) {
      LOG_WARN("[ASYNC_INDEX] fail to collect publish targets", K(ret), K(*this));
    }
  }
  return ret;
}

int ObAsyncIndexDDLPublishOp::get_gts(SCN &scn)
{
  int ret = OB_SUCCESS;
  // Require a GTS issued after target preparation, not an older cached GTS
  // which may precede the new tablets' create_commit_version.
  const transaction::MonotonicTs stc = transaction::MonotonicTs::current_time();
  transaction::MonotonicTs receive_ts;
  if (OB_FAIL(OB_TS_MGR.get_gts_sync(tenant_id_, stc,
      ObDDLUtil::get_default_ddl_rpc_timeout(), scn, receive_ts))) {
    LOG_WARN("[ASYNC_INDEX] fail to get publish gts", K(ret), K(*this));
  }
  return ret;
}

int ObAsyncIndexDDLPublishOp::publish(const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collect_publish_targets())) {
    LOG_WARN("[ASYNC_INDEX] failed to recollect targets for DDL publish", K(ret), K(scn), K(*this));
  } else if (index_table_ids_.empty() && storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL == refresh_type_) {
    // All async indexes may have been dropped since target preparation.
    LOG_INFO("[ASYNC_INDEX] skip DDL publish after all indexes were dropped", K(scn), K(*this));
  } else if (OB_FAIL(ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
      tenant_id_, data_table_id_, scn.get_val_for_tx(), index_table_ids_, refresh_type_, progress_))) {
    if (OB_EAGAIN != ret || REACH_TIME_INTERVAL(1000L * 1000L)) {
      LOG_WARN("[ASYNC_INDEX] failed to refresh DDL publish snapshot", K(ret), K(scn), K(*this));
    }
  }
  return ret;
}

int ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    const int64_t target_scn,
    const common::ObIArray<uint64_t> &async_index_table_ids,
    const int8_t refresh_type,
    ObAsyncIndexRefreshProgress *progress)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> data_tablet_ids;
  ObArray<ObLSID> ls_ids;
  share::ObLocationService *location_service = nullptr;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy = nullptr;
  const bool is_manual = storage::ASYNC_REFRESH_TYPE_MANUAL == refresh_type;
  const bool is_supported_refresh = is_manual
      || storage::ASYNC_REFRESH_TYPE_CREATE_INDEX == refresh_type
      || storage::ASYNC_REFRESH_TYPE_OFFLINE_DDL == refresh_type;

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == data_table_id
      || target_scn <= 0 || async_index_table_ids.empty() || !is_supported_refresh)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(data_table_id),
        K(target_scn), K(refresh_type), "index_count", async_index_table_ids.count());
  } else if (!is_async_index_refresh_enabled(tenant_id)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[ASYNC_INDEX] refresh not allowed, async index refresh is disabled", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", K(ret));
  } else if (OB_ISNULL(srv_rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("srv rpc proxy is null", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, data_table_id, data_tablet_ids))) {
    LOG_WARN("fail to get data table tablets", K(ret), K(tenant_id), K(data_table_id));
  } else if (data_tablet_ids.empty()) {
    LOG_INFO("[ASYNC_INDEX] no data tablets found, skip refresh", K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(
      *GCTX.sql_proxy_, tenant_id, data_tablet_ids, ls_ids))) {
    LOG_WARN("fail to batch get ls for data tablets", K(ret), K(tenant_id));
    if (is_manual && OB_ITEM_NOT_MATCH == ret) {
      // DDL can remove tablets between schema enumeration and the mapping lookup.
      ret = OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST;
    }
  } else if (data_tablet_ids.count() != ls_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data tablet count not equal ls count", K(ret),
        K(data_tablet_ids.count()), K(ls_ids.count()));
  }

  // For each index table, get its tablet array and is_delete flag
  ObSEArray<uint64_t, 4> valid_index_table_ids;
  ObSEArray<ObSEArray<ObTabletID, 16>, 4> index_tablet_arrays;
  ObSEArray<bool, 4> is_delete_flags;

  if (OB_FAIL(ret) || data_tablet_ids.empty()) {
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < async_index_table_ids.count(); ++j) {
      const uint64_t index_table_id = async_index_table_ids.at(j);
      ObSEArray<ObTabletID, 16> index_tablets;
      const share::schema::ObTableSchema *idx_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, idx_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_table_id));
      } else if (OB_ISNULL(idx_schema)) {
        LOG_INFO("[ASYNC_INDEX] index dropped, skip", K(index_table_id));
        continue;
      } else if (idx_schema->is_in_deleting()) {
        LOG_INFO("[ASYNC_INDEX] index is being deleted, skip refresh", K(index_table_id));
        continue;
      } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, index_table_id, index_tablets))) {
        LOG_WARN("fail to get index table tablets", K(ret), K(index_table_id));
      } else if (index_tablets.count() != data_tablet_ids.count()) {
        // The two tablet arrays may come from opposite sides of a partition DDL.
        ret = is_manual ? OB_SCHEMA_EAGAIN : OB_ERR_UNEXPECTED;
        LOG_WARN("index tablet count mismatch data tablet count", K(ret),
            K(index_table_id), K(index_tablets.count()), K(data_tablet_ids.count()));
      } else if (OB_FAIL(valid_index_table_ids.push_back(index_table_id))) {
        LOG_WARN("fail to push back valid index table id", K(ret));
      } else if (OB_FAIL(index_tablet_arrays.push_back(index_tablets))) {
        LOG_WARN("fail to push back index tablets", K(ret));
      } else if (OB_FAIL(is_delete_flags.push_back(
          share::schema::is_async_delete_index(idx_schema->get_index_type())))) {
        LOG_WARN("fail to push back is_delete flag", K(ret));
      }
    }
  }

  // Send one RPC per data tablet, containing all index entries for that tablet
  if (OB_FAIL(ret) || data_tablet_ids.empty() || valid_index_table_ids.empty()) {
  } else {
    ObAsyncIndexRefreshProgress local_progress;
    if (nullptr == progress) {
      progress = &local_progress;
    }
    RefreshDispatcher dispatcher(tenant_id, data_table_id, target_scn, refresh_type,
        *location_service, *srv_rpc_proxy, *progress);
    if (OB_FAIL(dispatcher.init(data_tablet_ids, ls_ids, valid_index_table_ids,
        index_tablet_arrays, is_delete_flags))) {
      LOG_WARN("failed to initialize async index refresh dispatcher", K(ret), K(tenant_id), K(data_table_id));
    } else if (OB_FAIL(dispatcher.execute())) {
      LOG_WARN("failed to execute async index refresh dispatcher", K(ret), K(tenant_id), K(data_table_id));
    }
  }
  return ret;
}

bool ObAsyncIndexRefreshUtil::is_async_index_enabled(const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    enabled = cfg->_enable_async_index;
  }
  // Invalid guard: fail closed.
  return enabled;
}

bool ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    enabled = cfg->_enable_async_index_refresh;
  }
  // Invalid guard: fail closed.
  return enabled;
}

bool ObAsyncIndexRefreshUtil::is_async_index_adaptive_parallelism_enabled(
    const uint64_t tenant_id)
{
  bool enabled = false;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    enabled = cfg->_enable_async_index_adaptive_parallelism;
  }
  // Invalid guard: fail closed.
  return enabled;
}

int64_t ObAsyncIndexRefreshUtil::get_async_index_refresh_dop_limit(
    const uint64_t tenant_id)
{
  int64_t dop_limit = 1;
  omt::ObTenantConfigGuard cfg(TENANT_CONF(tenant_id));
  if (cfg.is_valid()) {
    dop_limit = cfg->_async_index_refresh_dop_limit;
    if (0 == dop_limit) {
      dop_limit = OB_MAX(
          static_cast<int64_t>(
              MTL_CPU_COUNT() * storage::DEFAULT_ASYNC_INDEX_REFRESH_DOP_PERCENT / 100),
          1L);
    }
  }
  return dop_limit;
}

int ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    common::ObIArray<uint64_t> &out_index_table_ids,
    const bool retry_on_missing_search_schema,
    const bool allow_unavailable)
{
  int ret = OB_SUCCESS;
  out_index_table_ids.reuse();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(data_table_id));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *data_table_schema = nullptr;
    if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
        tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get data table schema", K(ret), K(tenant_id), K(data_table_id));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("data table schema not exist", K(ret), K(data_table_id));
    } else {
      const common::ObIArray<share::schema::ObAuxTableMetaInfo> &simple_index_infos =
          data_table_schema->get_simple_index_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const share::schema::ObAuxTableMetaInfo &info = simple_index_infos.at(i);
        const bool is_async_search = share::schema::is_async_search_def_index(info.index_type_);
        uint64_t index_table_id = info.table_id_;
        const share::schema::ObTableSchema *index_schema = nullptr;
        if (!share::schema::is_async_index(info.index_type_)) {
        } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
          LOG_WARN("fail to get index schema", K(ret), K(index_table_id));
        } else if (OB_ISNULL(index_schema)) {
          if (retry_on_missing_search_schema && is_async_search) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("async search def schema does not exist", K(ret), K(tenant_id), K(info));
          } else {
            LOG_INFO("index schema does not exist, skip", K(tenant_id), K(data_table_id), K(i), K(info));
          }
        } else if (share::schema::INDEX_STATUS_AVAILABLE != index_schema->get_index_status()
                   && !(allow_unavailable && share::schema::INDEX_STATUS_UNAVAILABLE == index_schema->get_index_status())) {
          LOG_INFO("index schema is not available, skip", K(tenant_id), K(data_table_id), K(i), K(info));
        } else if (is_async_search
                   && OB_FAIL(index_schema->get_search_data_index_tid(index_table_id))) {
          LOG_WARN("fail to get async search data table id", K(ret), K(info));
        } else if (is_async_search
                   && OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
          LOG_WARN("fail to get async search data schema", K(ret), K(index_table_id));
        } else if (OB_ISNULL(index_schema)) {
          if (retry_on_missing_search_schema) {
            ret = OB_SCHEMA_EAGAIN;
            LOG_WARN("async search data schema does not exist", K(ret), K(tenant_id), K(index_table_id));
          } else {
            LOG_INFO("async search data schema does not exist, skip", K(tenant_id), K(data_table_id), K(index_table_id));
          }
        } else if (share::schema::INDEX_STATUS_AVAILABLE != index_schema->get_index_status()
                   && !(allow_unavailable && share::schema::INDEX_STATUS_UNAVAILABLE == index_schema->get_index_status())) {
          LOG_INFO("async search data schema is not available, skip",
              K(tenant_id), K(data_table_id), K(index_table_id));
        } else if (OB_FAIL(out_index_table_ids.push_back(index_table_id))) {
          LOG_WARN("fail to push back index table id", K(ret), K(index_table_id));
        }
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
