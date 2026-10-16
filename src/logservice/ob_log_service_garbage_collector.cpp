/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_log_service_garbage_collector.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "logservice/ob_log_service.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
namespace logservice
{

SharedGarbageCollector::SharedGarbageCollector():
  is_inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  palf_env_(nullptr),
  sql_proxy_(nullptr),
  tenant_drop_time_() {}

SharedGarbageCollector::~SharedGarbageCollector()
{
  stop();
  wait();
  destroy();
}

int SharedGarbageCollector::init(
    libpalf::LibPalfEnv *palf_env,
    common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "shared garbage collector has been initialized",
        KP(sql_proxy), K(tenant_id_), KP(sql_proxy_));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "sql_proxy is null", KP(sql_proxy));
  } else if (OB_FAIL(tenant_drop_time_.create(1024, ObMemAttr(MTL_ID(), "TenantDropMap")))) {
    CLOG_LOG(WARN, "failed to init server_start_time_for_tenant_drop_");
  } else {
    tenant_id_ = MTL_ID();
    palf_env_ = palf_env;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }

  return ret;
}

int SharedGarbageCollector::start()
{
  int ret = OB_SUCCESS;

  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer*);
  if (OB_ISNULL(timer)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get null sharedTimer, unexpected", KP(timer), K(tenant_id_));
  } else if (! is_sys_tenant(tenant_id_)) {
    CLOG_LOG(INFO, "not sys tenant, no need to start shared GC", K(tenant_id_));
  } else if (OB_FAIL(TG_SCHEDULE(timer->get_tg_id(), *this, SCHEDULE_INTERVAL, true))) {
    CLOG_LOG(WARN, "failed to schedule shared GC timertask", KP(this));
  } else {
    CLOG_LOG(INFO, "succeed to schedule shared GC timer task", K(tenant_id_));
  }

  return ret;
}

void SharedGarbageCollector::stop()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer*);
  if (OB_ISNULL(timer)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get null sharedTimer, unexpected", KP(timer), K(tenant_id_));
  } else if (! is_sys_tenant(tenant_id_)) {
    CLOG_LOG(INFO, "not sys tenant, no need to stop shared GC", K(tenant_id_));
  } else {
    TG_CANCEL_TASK(timer->get_tg_id(), *this);
    CLOG_LOG(INFO, "stop shard GC timer task", K(tenant_id_));
  }
}

void SharedGarbageCollector::wait()
{
  int ret = OB_SUCCESS;
  omt::ObSharedTimer *timer = MTL(omt::ObSharedTimer*);
  if (OB_ISNULL(timer)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "get null sharedTimer, unexpected", KP(timer), K(tenant_id_));
  } else if (! is_sys_tenant(tenant_id_)) {
    CLOG_LOG(INFO, "not sys tenant, no need to stop shared GC", K(tenant_id_));
  } else {
    TG_WAIT_TASK(timer->get_tg_id(), *this);
    CLOG_LOG(INFO, "wait shard GC timer task", K(tenant_id_));
  }
}

void SharedGarbageCollector::destroy()
{
  sql_proxy_ = nullptr;
  palf_env_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_drop_time_.destroy();
  is_inited_ = false;
}

void SharedGarbageCollector::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id_) && is_sys_ls_leader_()) {
    ObArray<uint64_t> tenant_in_logservice, tenant_in_cluster;
    if (OB_FAIL(palf_env_->list_dir_in_logservice(tenant_in_logservice))) {
      CLOG_LOG(WARN, "failed to get tenant in logservice");
    } else if (OB_FAIL(get_tenant_in_cluster_(tenant_in_cluster))) {
      CLOG_LOG(WARN, "failed to get tenant in cluster");
    } else {
      gc_tenant_(tenant_in_logservice, tenant_in_cluster);
      gc_ls_(tenant_in_cluster);
    }
  }
}

void SharedGarbageCollector::gc_tenant_(ObArray<uint64_t> &tenant_in_logservice, ObArray<uint64_t> &tenant_in_cluster)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> tenant_set_cluster;
  ObArray<ObServerInfoInTable> server_infos;
  if (OB_FAIL(tenant_set_cluster.create(tenant_in_cluster.count()))) {
    CLOG_LOG(WARN, "failed to create tenant_set_cluster", "size", tenant_in_cluster.count());
  } else if (OB_FAIL(ObServerTableOperator::get(*sql_proxy_, server_infos))) {
    CLOG_LOG(WARN, "failed to get server_infos");
  }
  CLOG_LOG(INFO, "start to gc_tenant", K(tenant_in_logservice), K(tenant_in_cluster));
  ARRAY_FOREACH(tenant_in_cluster, idx) {
    const int64_t tenant_id = tenant_in_cluster.at(idx);
     if (OB_FAIL(tenant_set_cluster.set_refactored(tenant_id))) {
      CLOG_LOG(WARN, "failed to set tenant_id in tenant_set_cluster",
          K(tenant_id), K(tenant_set_cluster));
    }
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_NORET(tenant_in_logservice, idx) {
      const int64_t tenant_id = tenant_in_logservice.at(idx);
      if (OB_ISNULL(tenant_set_cluster.get(tenant_id))) {
        CLOG_LOG(INFO, "need to gc_tenant", K(tenant_id));
        if (need_delay_tenant_(tenant_id, server_infos)) {
          CLOG_LOG(INFO, "need to delay gc_tenant", K(tenant_id));
        } else if (OB_FAIL(palf_env_->remove_dir(tenant_id))) {
          CLOG_LOG(WARN, "failed to gc tenant in logservice, retry next round", K(tenant_id));
        }
      }
    }
  }
}

void SharedGarbageCollector::gc_ls_(ObArray<uint64_t> &tenant_in_cluster)
{
  int ret = OB_SUCCESS;

  ARRAY_FOREACH_NORET(tenant_in_cluster, idx) {
    const uint64_t tenant_id = tenant_in_cluster.at(idx);
    ObLSStatusInfoArray status_array;
    ObArray<ObLSID> ls_ids;
    ObLSStatusOperator ls_op;
    if (OB_FAIL(palf_env_->list_ls_in_logservice(tenant_id, ls_ids))) {
      CLOG_LOG(WARN, "failed to get ls in logservice", K(tenant_id), KP(palf_env_));
    } else if (OB_FAIL(ls_op.get_all_ls_status_by_order(tenant_id, status_array, *sql_proxy_))) {
      CLOG_LOG(WARN, "failed to get ls_status in cluster", K(tenant_id), KP(sql_proxy_));
    } else {
      hash::ObHashSet<ObLSID> ls_set_cluster;
      if (OB_FAIL(ls_set_cluster.create(status_array.count()))) {
        CLOG_LOG(WARN, "failed to create ls_set_cluster", "size", status_array.count());
      }
      ARRAY_FOREACH(status_array, idx) {
        const ObLSID &ls_id = status_array.at(idx).get_ls_id();
        if (OB_FAIL(ls_set_cluster.set_refactored(ls_id))) {
          CLOG_LOG(WARN, "failed to set ls_id to ls_set_cluster", K(ls_id), K(ls_set_cluster));
        }
      }
      CLOG_LOG(INFO, "start to gc ls in tenant", K(tenant_id), K(ls_ids), K(ls_set_cluster));
      if (OB_SUCC(ret)) {
        ARRAY_FOREACH_NORET(ls_ids, idx) {
          const ObLSID &ls_id = ls_ids.at(idx);
          if (OB_ISNULL(ls_set_cluster.get(ls_id))) {
            CLOG_LOG(INFO, "need to gc ls", K(ls_id), K(tenant_id));
            if (need_delay_(tenant_id, ls_id)) {
              CLOG_LOG(INFO, "it's not time to gc ls in logservice", K(ls_id), K(tenant_id), K(*time));
            } else if (OB_FAIL(palf_env_->remove_stream(tenant_id, ls_id.id()))) {
              CLOG_LOG(WARN, "failed to gc ls in logservice, retry next round", K(ls_id));
            }
          }
        }
      }
    }
  }
}

int SharedGarbageCollector::get_tenant_in_cluster_(ObArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;

  ObSqlString tenant_query;
  if (OB_FAIL(tenant_query.append_fmt("SELECT tenant_id from %s", OB_ALL_TENANT_TNAME))) {
    CLOG_LOG(WARN, "failed to append to tenant_query", K(tenant_query));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_proxy_->read(res, OB_SYS_TENANT_ID, tenant_query.ptr()))) {
        CLOG_LOG(WARN, "failed to execute query", K(tenant_query));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "get null result from mysqlresult", KP(result));
      } else {
        int64_t tenant_id = OB_INVALID_TENANT_ID;

        while(OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              CLOG_LOG(WARN, "failed to iterate result");
            }
          } else if (OB_FAIL(result->get_int("tenant_id", tenant_id))) {
            CLOG_LOG(WARN, "failed to get tenant id", K(tenant_id));
          } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
            CLOG_LOG(WARN, "failed to push back to tenant_ids", K(tenant_id), K(tenant_ids));
          }
        }
      }
    }
  }

  return ret;
}

bool SharedGarbageCollector::is_sys_ls_leader_()
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  ObRole role = ObRole::INVALID_ROLE;
  int64_t proposal_id = palf::INVALID_PROPOSAL_ID;

  bool is_leader = false;

  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "logservice is null, unexpected", K(tenant_id_));
  } else if (OB_FAIL(log_service->get_palf_role(SYS_LS, role, proposal_id))) {
    CLOG_LOG(WARN, "failed to get sys_ls role");
  } else {
    is_leader = is_strong_leader(role);
  }

  return is_leader;
}

bool SharedGarbageCollector::need_delay_tenant_(const uint64_t tenant_id, const ObIArray<ObServerInfoInTable> &server_infos)
{
  int ret = OB_SUCCESS;
  bool bret = true;

  int64_t start_time = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(tenant_drop_time_.get_refactored(tenant_id, start_time))) {
    if (OB_HASH_NOT_EXIST != ret) {
      CLOG_LOG(WARN, "failed to get start_time", K(tenant_id));
    } else if (OB_FAIL(tenant_drop_time_.set_refactored(tenant_id, ObTimeUtility::current_time()))) {
      CLOG_LOG(WARN, "failed to insert start_time", K(tenant_id));
    }
  } else {
    const int64_t now = ObTimeUtility::current_time();
    int64_t max_gc_start_time = start_time;
    bool all_server_in_service = true;
    ARRAY_FOREACH(server_infos, idx) {
      const ObServerInfoInTable &server_info = server_infos.at(idx);
      if (server_info.in_service()) {
        max_gc_start_time = max(max_gc_start_time, server_info.get_start_service_time());
      } else {
        all_server_in_service = false;
        CLOG_LOG(INFO, "server not in service, cannot gc tenant", K(tenant_id), K(server_info));
      }
    }
    if (all_server_in_service && (now > max_gc_start_time + DELAY_GC_INTERVAL)) {
      bret = false;
      CLOG_LOG(INFO, "all server in service and DELAY_GC_INTERVAL passed, can gc tenant",
        K(max_gc_start_time), K(all_server_in_service), K(tenant_id));
    } else {
      CLOG_LOG(INFO, "cannot gc tenant", K(all_server_in_service), K(max_gc_start_time), K(tenant_id));
    }
  }

  return bret;
}

bool SharedGarbageCollector::need_delay_(const uint64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  bool need_delay = true;
  ObLSTableOperator *lst_operator = GCTX.lst_operator_;
  const int64_t cluster_id = GCONF.cluster_id;
  ObLSInfo ls_info;

  if (OB_ISNULL(lst_operator)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "lst_operator is null, unexpected", KP(lst_operator));
  } else if (OB_FAIL(lst_operator->get(cluster_id, tenant_id, ls_id, ObLSTable::Mode::DEFAULT_MODE, ls_info))) {
    CLOG_LOG(WARN, "failed to get ls_info");
  } else if (0 == ls_info.replica_count()) {
    need_delay = false;
    CLOG_LOG(INFO, "no record in ls_meta, no need to delay", K(ls_info), K(need_delay));
  } else {
    CLOG_LOG(INFO, "there are records in ls_meta, need delay", K(ls_info), K(need_delay));
  }

  return need_delay;
}

}
}
