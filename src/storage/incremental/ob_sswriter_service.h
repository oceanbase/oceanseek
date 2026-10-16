/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_SERVICE_
#define OCEANBASE_STORAGE_SSWRITER_SERVICE_

#include "lib/thread/thread_mgr_interface.h"
#include "storage/incremental/ob_inc_ss_define.h"
#include "storage/incremental/ob_sswriter_rpc.h"
#include "storage/incremental/ob_sswriter_mgr.h"
#include "storage/incremental/ob_sswriter_lease_mgr.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/incremental/ob_sswriter_stat.h"
#include "storage/incremental/ob_sswriter_location.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_task.h"

namespace oceanbase
{
namespace storage
{
class ObSSWriterLeaseService;

class ObISSWriterCb : public common::ObDLinkBase<ObISSWriterCb>
{
private:
  share::ObLSID ls_id_;
public:
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  virtual int on_sswriter_up(const share::ObLSID &ls_id, const int64_t epoch, const bool is_default_group) = 0;
};

class ObSSWriterStatistics
{
public:
  ObSSWriterStatistics()
    : last_print_stat_ts_(0),
      active_sswriter_ctx_count_(0),
      check_lease_count_(0),
      check_lease_with_groupid_count_(0),
      grant_sswriter_request_count_(0),
      lease_request_count_(0),
      sswriter_loop_count_(0),
      sswriter_loop_used_time_us_(0),
      get_ctx_not_exist_count_(0),
      lease_async_request_count_(0),
      active_location_ctx_count_(0) {}
  ~ObSSWriterStatistics() { destroy(); }
  void destroy() { reset(); }
  void reset();
  void inc_active_sswriter_ctx_count();
  void inc_check_lease_count();
  void inc_check_lease_with_groupid_count();
  void inc_grant_sswriter_request_count();
  void inc_lease_request_count();
  void inc_sswriter_loop_count();
  void add_sswriter_loop_used_time(const int64_t used_time_us);
  void inc_get_ctx_not_exist_count();
  void inc_lease_async_request_count();
  void inc_active_location_ctx_count();
public:
  void try_print();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterStatistics);
private:
  int64_t last_print_stat_ts_;
  int64_t active_sswriter_ctx_count_;
  int64_t check_lease_count_;
  int64_t check_lease_with_groupid_count_;
  int64_t grant_sswriter_request_count_;
  int64_t lease_request_count_;
  int64_t sswriter_loop_count_;
  int64_t sswriter_loop_used_time_us_;
  int64_t get_ctx_not_exist_count_;
  int64_t lease_async_request_count_;
  int64_t active_location_ctx_count_;
};

class ObSSWriterWorker : public lib::TGRunnable
{
public:
  ObSSWriterWorker()
    : is_inited_(false),
      is_running_(false),
      lease_service_lock_(common::ObLatchIds::OB_SS_WRITER_WORKER_LOCK),
      tg_id_(-1),
      ctx_mgr_(NULL),
      lease_service_list_(),
      lease_service_count_(0),
      location_mgr_(NULL),
      ls_ckpt_executor_(NULL),
      next_ls_ckpt_time_us_(0) {}
  ~ObSSWriterWorker() { destroy(); }
  int init(
      ObLsSSWriterCtxMgr *ctx_mgr,
      ObSSWriterLocationMgr *location_mgr);
  void destroy();
  int start();
  void stop();
  void wait();
  void run1();
  void set_ls_ckpt_executor(ObSSLsCkptExecutor *executor) { ls_ckpt_executor_ = executor; }
public:
  int register_ls_lease_service(ObSSWriterLeaseService *lease_service);
  int unregister_ls_lease_service(ObSSWriterLeaseService *lease_service);
private:
  void test_();
  void try_do_ls_checkpoint_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterWorker);
private:
  bool is_inited_;
  bool is_running_;
  ObSpinLock lease_service_lock_;
  int tg_id_;
  ObLsSSWriterCtxMgr *ctx_mgr_;
  ObDList<ObSSWriterLeaseService> lease_service_list_;
  int64_t lease_service_count_;
  ObSSWriterLocationMgr *location_mgr_;
  ObSSLsCkptExecutor *ls_ckpt_executor_;
  int64_t next_ls_ckpt_time_us_;
};

class ObSSWriterService
{
public:
  ObSSWriterService()
    : is_inited_(false),
      is_running_(false),
      cbs_lock_(common::ObLatchIds::OB_SS_WRITER_CBS_MGR_LOCK),
      sswriter_rpc_(),
      statistics_(),
      ctx_mgr_(),
      cbs_(),
      location_mgr_(),
      ls_ckpt_executor_() {}
  ~ObSSWriterService() { destroy(); }
  static int mtl_init(ObSSWriterService* &sswriter_service);
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
public:
  // if local is sswriter of group with specified key,
  // return true of is_sswriter and the corresponding epoch
  int check_lease(const ObSSWriterKey &key, bool &is_sswriter, int64_t &epoch);
  // Atomic protocols use the PALF proposal_id on primary tenants and the SSWriter lease epoch on standby tenants.
  int check_lease_for_atomic_protocol(const ObSSWriterKey &key, bool &is_sswriter, int64_t &epoch);
  // Use PALF leadership of the management LS for writer qualification, with proposal_id as the epoch.
  // Set is_sswriter to false and epoch to 0 if this server is not the leader or does not host the LS.
  int check_lease_by_palf_role(const ObSSWriterKey &key, bool &is_sswriter, int64_t &epoch);
  // register cb
  int register_callback(const share::ObLSID &ls_id, ObISSWriterCb *cb);
  // unregister cb
  int unregister_callback(const share::ObLSID &ls_id, ObISSWriterCb *cb);
  int handle_sswriter_grant_req(const ObSSWriterGrantRequest &req);
  int handle_sswriter_lease_req(const ObSSWriterLeaseRequest &req);
  int handle_sswriter_lease_async_req(const ObSSWriterLeaseRequest &req);
  int handle_sswriter_lease_async_resp(const ObSSWriterLeaseResponse &resp);
  int handle_sswriter_location_req(
      const ObSSWriterLocationRequest &req,
      const bool is_local_request = false);
  int handle_sswriter_location_resp(const ObSSWriterLocationResponse &resp);
  int grant_sswriter(const ObSSWriterGrantArg &arg);
  int request_sswriter_lease(const ObSSWriterLeaseRequestArg &arg);
  int request_sswriter_lease_async(const ObSSWriterLeaseRequestArg &arg);
  int respond_sswriter_lease_async(const ObSSWriterLeaseRequest &req);
  int register_ls_lease_service(ObSSWriterLeaseService *lease_service);
  int unregister_ls_lease_service(ObSSWriterLeaseService *lease_service);
  int iterate_sswriter_group_stat(ObSSWriterGroupStatIterator &stat_iter);
  int iterate_sswriter_mgr_stat(ObSSWriterMgrStatIterator &stat_iter);
  // get address of specifed sswriter in local region
  int get_sswriter_addr(
      const ObSSWriterKey &key,
      ObSSWriterAddr &sswriter_addr,
      const int64_t timeout_us = 1000000/*one second*/);
  int request_sswriter_location(
      const ObSSWriterLocationRequestArg &arg);
  int respond_sswriter_location(
      const ObSSWriterLocationRequest &req,
      const ObSSWriterLocationResponseArg &arg);
public:
  ObSSWriterRpc* get_sswriter_rpc() { return &sswriter_rpc_; }
  ObSSWriterStatistics &get_statistics() { return statistics_; }
  void try_print_statistics();
  int do_ls_checkpoint(const ObSSLsCkptMode mode);
private:
  void stop_ls_ckpt_dag_();
  int request_sswriter_lease_(const ObSSWriterLeaseRequestArg &arg);
  int update_sswriter_location_(
      const ObSSWriterKey &key,
      const common::ObAddr &addr,
      const int64_t epoch,
      const int64_t remaining_lease_us);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSWriterService);
private:
  bool is_inited_;
  bool is_running_;
  ObSpinLock cbs_lock_;
  ObSSWriterRpc sswriter_rpc_;
  ObSSWriterStatistics statistics_;
  ObSSWriterWorker worker_;
  ObLsSSWriterCtxMgr ctx_mgr_;
  ObDList<ObISSWriterCb> cbs_;
  ObSSWriterLocationMgr location_mgr_;
  ObSSLsCkptExecutor ls_ckpt_executor_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_SERVICE_
