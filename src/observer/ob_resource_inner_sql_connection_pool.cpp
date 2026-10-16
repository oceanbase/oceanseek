/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "ob_resource_inner_sql_connection_pool.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;

namespace observer
{

ObResourceInnerSQLConnectionPool::ObResourceInnerSQLConnectionPool()
  : is_inited_(false), max_conn_id_(OB_INVALID_ID), inner_sql_conn_pool_(), lock_(), id_conn_map_(),
    recycle_task_(*this), recycle_timer_()
{
}

ObResourceInnerSQLConnectionPool::~ObResourceInnerSQLConnectionPool()
{
  recycle_timer_.destroy();
}

int ObResourceInnerSQLConnectionPool::init(ObMultiVersionSchemaService *schema_service,
                                           ObSql *ob_sql,
                                           ObVTIterCreator *vt_iter_creator,
                                           common::ObServerConfig *config,
                                           const bool is_ddl)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::INNER_CONN_POOL_LOCK);

  ObMemAttr attr(OB_SERVER_TENANT_ID, "IdConnMap");
  SET_USE_500(attr);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObResourceInnerSQLConnectionPool has already been inited", K(ret));
  } else if (OB_FAIL(id_conn_map_.create(ObInnerSQLConnectionPool::WARNNING_CONNECTION_CNT,
                                         attr, attr))) {
    LOG_WARN("fail to create id_conn_map_", K(ret));
  } else if (OB_FAIL(inner_sql_conn_pool_.init(schema_service,
                                               ob_sql,
                                               vt_iter_creator,
                                               config,
                                               false,
                                               true /* is_resource_conn_pool */))) {
    LOG_ERROR("init inner sql connection pool failed", K(ret));
  } else if (OB_FAIL(recycle_timer_.init("ResInnerSqlGC"))) {
    LOG_WARN("init resource inner sql recycle timer failed", K(ret));
  } else if (OB_FAIL(recycle_timer_.schedule(recycle_task_, RECYCLE_INTERVAL_US, true))) {
    LOG_WARN("schedule resource inner sql recycle task failed", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObResourceInnerSQLConnectionPool::fetch_max_conn_id(uint64_t &max_conn_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObResourceInnerSQLConnectionPool has not been inited", K(ret));
  } else if (OB_INVALID_ID == max_conn_id_) {
    max_conn_id = (max_conn_id_ = 0);
  } else {
    max_conn_id = ++max_conn_id_;
  }

  return ret;
}

int ObResourceInnerSQLConnectionPool::acquire(
    const uint64_t conn_id, const bool is_oracle_mode, const bool kill_using_conn,
    common::sqlclient::ObISQLConnection *&conn, sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *inner_conn = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObResourceInnerSQLConnectionPool has not been inited", K(ret));
  } else if (OB_INVALID_ID == conn_id) {
    if (NULL != session_info) {
      session_info->set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    }
    if (OB_FAIL(inner_sql_conn_pool_.acquire(session_info, conn, is_oracle_mode))) {
      LOG_WARN("failed to acquire inner connection", K(ret));
    } else if (FALSE_IT(inner_conn = static_cast<ObInnerSQLConnection *>(conn))) {
    } else if (OB_ISNULL(inner_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conn is null", K(ret));
    } else {
      ObLatchWGuard guard(lock_, ObLatchIds::INNER_CONN_POOL_LOCK);
      uint64_t conn_id = OB_INVALID_ID;
      if (OB_FAIL(fetch_max_conn_id(conn_id))) {
        LOG_WARN("failed to fetch max_conn_id", K(ret));
      } else {
        inner_conn->set_is_idle(false);
        inner_conn->set_resource_svr(MYADDR);
        inner_conn->set_resource_conn_id(conn_id);
        inner_conn->set_last_query_timestamp(ObTimeUtility::current_time());
        if (OB_FAIL(id_conn_map_.set_refactored(conn_id, conn,
                                                0 /* do not overwrite existing object */))) {
          LOG_WARN("set the pair of conn_id and inner_conn to the id_conn_map_ failed", K(ret));
        }
      }
    }
  } else { // get conn with conn_id from id_conn_map_ directly
    ObLatchWGuard guard(lock_, ObLatchIds::INNER_CONN_POOL_LOCK);
    if (OB_FAIL(id_conn_map_.get_refactored(conn_id, conn))) {
      // Failed to get the connection by conn_id. the connection may not have received 
      // any requests from the source observer for more than ten minutes, and was then 
      // released by the connection leak check mechanism. Please check first.
      ret = OB_SESSION_NOT_FOUND;
      LOG_WARN("failed to get the connection by conn_id. the connection may not have received "
               "any requests from the source observer for more than ten minutes, and was then "
               "released by the connection leak check mechanism.", K(ret));
    } else if (FALSE_IT(inner_conn = static_cast<ObInnerSQLConnection *>(conn))) {
    } else if (OB_ISNULL(inner_conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conn is null", K(ret), K(conn_id));
    } else if (!inner_conn->is_idle()) {
      if (kill_using_conn) {
        /* related issue : 
         * Why we need to set need_trans_rollback flag to inner_conn ?
         * Consider this situation:
         * Local obs inner sql rpc is timeout or gets some errors, and then will transmit a rollback inner sql to remote obs.
         * But the same conn of remote obs may be still executing the last inner sql rpc.
         * This rollback can not be executed before the last inner sql finished, because the two inner sqls can not be concurrent in the same inner conn.
         * This rollback can not be executed after the last inner sql finished in the rollback inner conn, because the inner conn may be released in the last inner sql and the inner conn may also be reused by other inner sql after released.
         * This rollback can not be omitted, because that may cause the trans suspended.
         * So we use this flag to rollback and release the inner conn if the same inner conn has received a rollback operation from other inner sql.
        */
        inner_conn->set_force_no_reuse(true);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("using the same inner_conn concurrently is unexpected", K(ret));
      }
      conn = NULL;
    } else {
      inner_conn->set_is_idle(false);
    }
  }

  return ret;
}

void ObResourceInnerSQLConnectionPool::stop()
{
  inner_sql_conn_pool_.stop();
  recycle_timer_.stop();
}

int ObResourceInnerSQLConnectionPool::wait()
{
  const int ret = inner_sql_conn_pool_.wait();
  if (recycle_timer_.inited()) {
    recycle_timer_.wait();
  }
  return ret;
}

void ObResourceInnerSQLConnectionPool::RecycleTask::runTimerTask()
{
  pool_.recycle_expired_connections();
}

bool ObResourceInnerSQLConnectionPool::is_expired(const ObInnerSQLConnection &conn, const int64_t now)
{
  return conn.is_idle() && conn.get_last_query_timestamp() > 0
      && now - conn.get_last_query_timestamp() >= IDLE_TIMEOUT_US;
}

void ObResourceInnerSQLConnectionPool::recycle_expired_connections()
{
  const int64_t now = ObTimeUtility::current_time();
  ObLatchWGuard guard(lock_, ObLatchIds::INNER_CONN_POOL_LOCK);
  if (is_inited_) {
    // The used list is ordered by creation, not by last activity. Inspect every connection.
    for (IdConnMap::iterator it = id_conn_map_.begin(); it != id_conn_map_.end();) {
      const uint64_t conn_id = it->first;
      ObInnerSQLConnection *conn = static_cast<ObInnerSQLConnection *>(it->second);
      // inner_release() erases the current map entry, so advance before releasing it.
      ++it;
      if (OB_NOT_NULL(conn)) {
        int ret = OB_SUCCESS;
        const uint64_t resource_conn_id = conn->get_resource_conn_id();
        const int64_t last_query_timestamp = conn->get_last_query_timestamp();
        if (0 == last_query_timestamp || conn_id != resource_conn_id) {
          LOG_ERROR("invalid resource inner sql connection state during recycling",
                    "map_conn_id", conn_id, K(resource_conn_id), K(last_query_timestamp),
                    "is_idle", conn->is_idle(), K(now), KP(conn));
        }
        if (is_expired(*conn, now)) {
          LOG_TRACE("recycle resource inner sql connection", K(conn_id));
          if (OB_FAIL(inner_release(conn))) {
            LOG_WARN("release expired resource inner sql connection failed", K(ret), K(conn_id));
          }
        }
      }
    }
  }
}

int ObResourceInnerSQLConnectionPool::inner_release(common::sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn is null", K(ret));
  } else if (OB_FAIL(id_conn_map_.erase_refactored(
                     static_cast<ObInnerSQLConnection *>(conn)->get_resource_conn_id()))) {
    LOG_WARN("fail to erase from id_conn_map_", K(ret));
  } else if (OB_FAIL(inner_sql_conn_pool_.release(conn, true))) {
    LOG_WARN("inner_release connection failed", K(ret));
  }

  return ret;
}

int ObResourceInnerSQLConnectionPool::release(
    const bool reuse_conn,
    common::sqlclient::ObISQLConnection *&conn)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard guard(lock_, ObLatchIds::INNER_CONN_POOL_LOCK);

  if (OB_ISNULL(conn)) {
    if (reuse_conn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("conn is null", K(ret));
    }
  } else {
    ObInnerSQLConnection *inner_conn = static_cast<ObInnerSQLConnection *>(conn);
    if (!reuse_conn || inner_conn->is_force_no_reuse()) {
      if (OB_FAIL(inner_release(conn))) {
        LOG_WARN("release connection failed", K(ret));
      }
      conn = NULL;
    } else {
      inner_conn->set_is_idle(true);
      inner_conn->set_last_query_timestamp(ObTimeUtility::current_time());
    }
  }

  return ret;
}

} // end namespace observer
} // end namespace oceanbase
