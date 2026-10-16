/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_LEASE_MGR_
#define OCEANBASE_STORAGE_SSWRITER_LEASE_MGR_

#include "lib/thread/thread_mgr_interface.h"
#include "storage/incremental/ob_inc_ss_define.h"
#include "storage/incremental/ob_sswriter_stat.h"
#include "share/location_cache/ob_location_service.h"

namespace oceanbase
{
namespace storage
{
class ObSSWriterEpochGenerator
{
public:
  ObSSWriterEpochGenerator() : epoch_value_(-1) {};
  ~ObSSWriterEpochGenerator() { reset(); }
  void reset() { ATOMIC_STORE(&epoch_value_, -1); }
  int64_t get_next_value();
  int64_t get_current_value() const;
  void set(const int64_t value) { ATOMIC_STORE(&epoch_value_, value); }
  bool is_valid() const { return 0 < ATOMIC_LOAD(&epoch_value_); }
public:
  TO_STRING_KV(K_(epoch_value));
private:
  int64_t epoch_value_;
};

enum class ObSSWriterServiceTenantType : int8_t {
  INVALID = -1,
  PRIMARY = 0,
  RESTORE = 1, // standby, restore and clone
};

struct ObSSWriterGroupKey
{
  ObSSWriterGroupKey()
    : id_(-1),
      ls_id_(),
      sswriter_type_(ObSSWriterType::INVALID) {}
  ~ObSSWriterGroupKey() { reset(); }
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(id), K_(ls_id), K_(sswriter_type));
  int64_t id_;
  share::ObLSID ls_id_;
  ObSSWriterType sswriter_type_;
  // TODO, add region id
};

class ObSSWriterRegionLease;

class ObSSWriterGroupLease
{
  friend class ObSSWriterRegionLease;
public:
  ObSSWriterGroupLease()
    : is_inited_(false),
      lock_(common::ObLatchIds::OB_SS_WRITER_LEASE_MGR_LOCK),
      group_key_(),
      sswriter_region_lease_(NULL),
      state_(ObSSWriterGroupState::INVALID),
      last_update_state_ts_(-1),
      lease_expire_ts_(-1),
      epoch_(-1),
      server_(),
      epoch_generator_(NULL) {}
  ~ObSSWriterGroupLease() { destroy(); }
  int init(
      const int64_t id,
      const share::ObLSID ls_id,
      const ObSSWriterType type,
      ObSSWriterRegionLease *sswriter_region_lease,
      ObSSWriterEpochGenerator *epoch_generator);
  void reset();
  void reset_lease();
  void destroy();
public:
  int set_server(const common::ObAddr &server);
  int64_t get_id() const { return group_key_.id_; }
  int check_status();
  int update_lease(const int64_t epoch, const int64_t lease_us);
  const common::ObAddr &get_target_addr() const { return server_; }
  int64_t get_epoch() const { return epoch_; }
  int64_t get_lease_expire_ts() const { return lease_expire_ts_; }
public:
  TO_STRING_KV(K_(is_inited), K_(group_key), K_(state), K_(last_update_state_ts),
               K_(lease_expire_ts), K_(epoch), K_(server));
private:
  void set_state_(const ObSSWriterGroupState state);
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  // no change after init
  ObSSWriterGroupKey group_key_;
  ObSSWriterRegionLease *sswriter_region_lease_;
  // dynamic
  ObSSWriterGroupState state_;
  int64_t last_update_state_ts_;
  int64_t lease_expire_ts_;
  int64_t epoch_;
  common::ObAddr server_;
  ObSSWriterEpochGenerator *epoch_generator_;
};

// for ls leader to manage sswriter lease
class ObSSWriterRegionLease
{
public:
  ObSSWriterRegionLease()
    : is_inited_(false),
      ls_id_(),
      region_(),
      default_group_lease_(),
      epoch_generator_(NULL) {}
  ~ObSSWriterRegionLease() { destroy(); }
  int init(
      const common::ObRegion &region,
      const share::ObLSID ls_id,
      ObSSWriterEpochGenerator *epoch_generator);
  void reset_lease();
  void destroy();
  bool is_inited() const { return is_inited_; }
public:
  const common::ObRegion &get_region() const { return region_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  int check_status();
  int handle_lease_request(const ObSSWriterLeaseRequest &req);
  int get_sswriter_addr(
      common::ObRegion &target_region,
      const ObSSWriterKey &key,
      ObSSWriterAddr &sswriter_addr,
      int64_t &remaining_lease_us);
  int iterate_sswriter_mgr_stat(ObSSWriterMgrStatIterator &stat_iter);
  TO_STRING_KV(K_(is_inited), K_(region), K_(ls_id),
      K_(default_group_lease));
private:
  int is_inited_;
  share::ObLSID ls_id_;
  common::ObRegion region_;
  // default group lease
  ObSSWriterGroupLease default_group_lease_;
  // ObSSWriterGroupLease compaction_default_group_lease_;
  ObSSWriterEpochGenerator *epoch_generator_;
};

class ObSSWriterLSHandler;

// for ls leader
// to manage lease of groups in this ls
class ObSSWriterLeaseService : public common::ObDLinkBase<ObSSWriterLeaseService>
{
public:
  ObSSWriterLeaseService()
    : is_inited_(false),
      ls_id_(),
      is_running_(false),
      ls_handler_(NULL),
      start_ts_(-1),
      epoch_base_(-1),
      sswriter_region_lease_(),
      epoch_generator_() {}
  void reset_lease();
  int init(ObSSWriterLSHandler *ls_handler);
  int start();
  void stop();
  void wait();
  void destroy();
  int do_work();
  // void run1();
public:
  int handle_lease_request(const ObSSWriterLeaseRequest &req);
  int get_sswriter_addr(
      common::ObRegion &target_region,
      const ObSSWriterKey &key,
      ObSSWriterAddr &sswriter_addr,
      int64_t &remaining_lease_us);
  int iterate_sswriter_mgr_stat(ObSSWriterMgrStatIterator &stat_iter);
public:
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(is_running), K_(start_ts), K_(epoch_base));
private:
  int prepare_epoch_generator_();
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  bool is_running_;
  // int tg_id_;
  ObSSWriterLSHandler *ls_handler_;
  int64_t start_ts_;
  int64_t epoch_base_;
  // TODO, instead of map
  ObSSWriterRegionLease sswriter_region_lease_;
  ObSSWriterEpochGenerator epoch_generator_;
};

class ObSSWriterLSHandler : public logservice::ObIReplaySubHandler,
                            public logservice::ObIRoleChangeSubHandler,
                            public logservice::ObICheckpointSubHandler
{
public:
  ObSSWriterLSHandler()
    : is_inited_(false),
      lock_(common::ObLatchIds::OB_SS_WRITER_LEASE_MGR_LOCK),
      ls_id_(),
      ls_(NULL),
      lease_service_(),
      leader_start_ts_(-1),
      service_tenant_type_(ObSSWriterServiceTenantType::INVALID) {}
  ~ObSSWriterLSHandler() { destroy(); }
  int init(ObLS *ls, ObSSWriterServiceTenantType service_tenant_type);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  ObSSWriterLeaseService &get_sswriter_lease_service() { return lease_service_; }
  int handle_lease_request(const ObSSWriterLeaseRequest &req);
  int get_sswriter_addr(
      common::ObRegion &target_region,
      const ObSSWriterKey &key,
      ObSSWriterAddr &sswriter_addr,
      int64_t &remaining_lease_us);
  ObSSWriterServiceTenantType get_service_tenant_type() const { return service_tenant_type_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  int iterate_sswriter_mgr_stat(ObSSWriterMgrStatIterator &stat_iter);
public:
  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;
  // for replay
  virtual int replay(
      const void *buffer,
      const int64_t nbytes,
      const palf::LSN &lsn,
      const share::SCN &scn) override;
  // for checkpoint
  virtual share::SCN get_rec_scn() override;
  virtual int flush(share::SCN &scn) override;
public:
  // for offline and online
  int offline();
  int online();
public:
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(leader_start_ts), K_(service_tenant_type));
private:
  bool is_inited_;
  // Serialize switch / offline / online
  common::ObSpinLock lock_;
  share::ObLSID ls_id_;
  ObLS *ls_;
  ObSSWriterLeaseService lease_service_;
  // after switch to leader, start_ts is set to a valid value
  // after switch to follower, start_ts is set to -1
  int64_t leader_start_ts_;
  ObSSWriterServiceTenantType service_tenant_type_;
};

// for primary tenant
class ObPrimarySSWriterLSHandler : public ObSSWriterLSHandler
{
public:
  ObPrimarySSWriterLSHandler() : ObSSWriterLSHandler() {}
  ~ObPrimarySSWriterLSHandler() { ObSSWriterLSHandler::destroy(); }
  int init(ObLS *ls);
};

// for standby (restore or clone) tenant
class ObRestoreSSWriterLSHandler : public ObSSWriterLSHandler
{
public:
  ObRestoreSSWriterLSHandler() : ObSSWriterLSHandler() {}
  ~ObRestoreSSWriterLSHandler() { ObSSWriterLSHandler::destroy(); }
  int init(ObLS *ls);
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_LEASE_MGR_
