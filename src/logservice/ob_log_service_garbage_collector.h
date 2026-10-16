/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_LOG_SERVICE_GARBAGE_COLLECTOR_H_
#define OCEANBASE_SHARED_LOG_SERVICE_GARBAGE_COLLECTOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "logservice/libpalf/libpalf_env.h"
#include "share/ob_server_table_operator.h"
#include <cstdint>
namespace oceanbase
{
namespace logservice
{

class SharedGarbageCollector: ObTimerTask
{
  const int64_t SCHEDULE_INTERVAL = 60 * 1000 * 1000L;
  const int64_t DELAY_GC_INTERVAL = 300 * 1000 * 1000L;
public:
  SharedGarbageCollector();
  ~SharedGarbageCollector();

public:
  virtual void runTimerTask() override;

public:
  int init(libpalf::LibPalfEnv *palf_env, common::ObMySQLProxy *sql_proxy);
  int start();
  void stop();
  void wait();
  void destroy();

private:
  void gc_tenant_(ObArray<uint64_t> &tenant_in_logservice, ObArray<uint64_t> &tenant_in_cluster);
  void gc_ls_(ObArray<uint64_t> &tenant_in_cluster);
  int get_tenant_in_cluster_(ObArray<uint64_t> &tenant_ids);
  bool is_sys_ls_leader_();
  bool need_delay_(const uint64_t tenant_id, const ObLSID &ls_id);
  bool need_delay_tenant_(const uint64_t tenant_id, const ObIArray<ObServerInfoInTable> &server_infos);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  libpalf::LibPalfEnv *palf_env_;
  common::ObMySQLProxy *sql_proxy_;
  hash::ObHashMap<uint64_t, int64_t> tenant_drop_time_;
};

}
}

#endif