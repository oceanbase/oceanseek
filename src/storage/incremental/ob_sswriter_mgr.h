/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_MGR_
#define OCEANBASE_STORAGE_SSWRITER_MGR_

#include "lib/thread/thread_mgr_interface.h"
#include "storage/incremental/ob_inc_ss_define.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/incremental/ob_sswriter_stat.h"

namespace oceanbase
{
namespace storage
{
struct ObLsSSWriterCtxCreateArg
{
  ObLsSSWriterCtxCreateArg()
    : grant_server_() {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(grant_server));
  common::ObAddr grant_server_;
};

class ObIterateSSWriterGroupStatFunctor;

struct ObSSWriterGroup
{
  ObSSWriterGroup()
    : group_id_(-1),
      lease_expire_ts_(-1),
      epoch_(-1),
      create_ts_(-1),
      last_request_lease_ts_(-1) {}
  ~ObSSWriterGroup() { reset(); }
  bool is_valid() const { return 0 < group_id_; }
  bool is_in_grant() const { return 0 < group_id_ && -1 == lease_expire_ts_; }
  bool need_refresh(const int64_t current_ts) const;
  bool is_lease_expired(const int64_t current_ts) const;
  void reset();
  TO_STRING_KV(K_(group_id), K_(lease_expire_ts), K_(epoch), K_(create_ts),
      K_(last_request_lease_ts));
  int64_t group_id_;
  int64_t lease_expire_ts_;
  int64_t epoch_;
  int64_t create_ts_;
  int64_t last_request_lease_ts_;
};

typedef common::LinkHashValue<ObLsSSWriterKey> LsSSWriterHashValue;
typedef common::LinkHashNode<ObLsSSWriterKey> LsSSWriterCtxHashNode;

class ObLsSSWriterCtxMgr;

// used to manage sswriter lease in sswriter
// NOTE that one ctx is responsible for all sswriters in this region in this server
// accorting to sswriter type and ls id
class ObLsSSWriterCtx : public LsSSWriterHashValue
{
  friend class ObIterateSSWriterGroupStatFunctor;
public:
  ObLsSSWriterCtx()
    : is_inited_(false),
      is_exiting_(false),
      is_waiting_for_group_(true),
      lock_(common::ObLatchIds::SSWRITER_CTX_LOCK),
      key_(),
      ctx_mgr_(NULL),
      grant_server_(),
      default_group_() {}
  ~ObLsSSWriterCtx() { destroy(); }
  void reset();
  void destroy();
  int init(
      const ObLsSSWriterKey &key,
      ObLsSSWriterCtxMgr *ctx_mgr,
      const common::ObAddr &grant_server);
  bool is_inited() const { return is_inited_; }
  int kill();
public:
  int add_group(
      const int64_t group_id,
      const int64_t epoch,
      const common::ObAddr &grant_server);
  int remove_group(const int64_t group_id);
  int update_lease(
      const int64_t group_id,
      const int64_t lease_expire_ts,
      const int64_t epoch);
  int try_renew_lease_for_default_group();
  bool is_lease_valid(const common::ObTabletID &tablet_id);
  bool is_lease_valid(const int64_t group_id);
  int check_status_and_try_exit();
  int check_lease(
      const common::ObTabletID &tablet_id,
      bool &is_sswriter,
      int64_t &epoch);
  bool is_lease_valid_with_epoch(
      const int64_t epoch) const;
  TO_STRING_KV(KP(this), K_(is_inited), K_(is_exiting), K_(is_waiting_for_group),
      K_(key), K_(grant_server), K_(default_group), "uref", get_uref());
private:
  int set_exiting_();
  bool is_lease_valid_(const int64_t group_id) const;
  int add_group_(
      const int64_t group_id,
      const int64_t epoch,
      const common::ObAddr &grant_server);
  void remove_group_(const int64_t group_id);
  int update_lease_(
      const int64_t group_id,
      const int64_t lease_expire_ts,
      const int64_t epoch);
private:
  bool is_inited_;
  bool is_exiting_;
  bool is_waiting_for_group_;
  ObSpinLock lock_;
  // no change after init
  ObLsSSWriterKey key_;
  ObLsSSWriterCtxMgr *ctx_mgr_;
  common::ObAddr grant_server_;
  // for default group
  ObSSWriterGroup default_group_;
};

class LsSSWriterCtxAlloc
{
public:
  LsSSWriterCtxAlloc()
    : active_value_count_(0) {}
  ~LsSSWriterCtxAlloc() { active_value_count_ = 0; }
  int64_t get_active_value_count() const { return ATOMIC_LOAD(&active_value_count_); }
public:
  ObLsSSWriterCtx* alloc_value()
  {
    ObMemAttr memattr(MTL_ID(), "LsSSWriterCtx");
    ObLsSSWriterCtx *ctx = OB_NEW(ObLsSSWriterCtx, memattr);
    if (NULL != ctx) {
      ATOMIC_INC(&active_value_count_);
    }
    return ctx;
  }
  void free_value(ObLsSSWriterCtx *ctx)
  {
    if (NULL != ctx) {
      ObMemAttr memattr(MTL_ID(), "LsSSWriterCtx");
      OB_DELETE(ObLsSSWriterCtx, memattr, ctx);
      ATOMIC_DEC(&active_value_count_);
    }
  }
  LsSSWriterCtxHashNode *alloc_node(ObLsSSWriterCtx *ctx)
  {
    // the input parameter ctx is unused
    UNUSED(ctx);
    ObMemAttr memattr(MTL_ID(), "LsSSWCtxNode");
    return OB_NEW(LsSSWriterCtxHashNode, memattr);
  }
  void free_node(LsSSWriterCtxHashNode *node)
  {
    if (NULL != node) {
      ObMemAttr memattr(MTL_ID(), "LsSSWCtxNode");
      OB_DELETE(LsSSWriterCtxHashNode, memattr, node);
      node = NULL;
    }
  }
private:
  int64_t active_value_count_;
};

typedef common::ObLinkHashMap<ObLsSSWriterKey, ObLsSSWriterCtx,
        LsSSWriterCtxAlloc, common::RefHandle> ObLsSSWriterCtxMap;

class ObLsSSWriterCtxMgr
{
public:
  ObLsSSWriterCtxMgr()
    : is_inited_(false),
      ctx_map_(),
      last_print_ts_(0) {}
  ~ObLsSSWriterCtxMgr() { destroy(); }
  int init();
  void destroy();
public:
  int acquire_ls_sswriter_ctx(
      const ObLsSSWriterCtxCreateArg &create_arg,
      const ObLsSSWriterKey &key,
      ObLsSSWriterCtx *&ctx);
  int get_ls_sswriter_ctx(
      const ObLsSSWriterKey &key,
      ObLsSSWriterCtx *&ctx);
  int revert_ls_sswriter_ctx(ObLsSSWriterCtx *ctx);
  int erase_ls_sswriter_ctx(const ObLsSSWriterKey &key);
  void try_print_statistics();
  int try_renew_lease();
  int iterate_sswriter_group_stat(ObSSWriterGroupStatIterator &stat_iter);
  TO_STRING_KV(K(is_inited_));
private:
  int get_ls_sswriter_ctx_(const ObLsSSWriterKey &key, ObLsSSWriterCtx *&ctx);
  int release_all_ctx_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObLsSSWriterCtxMgr);
private:
  bool is_inited_;
  ObLsSSWriterCtxMap ctx_map_;
  int64_t last_print_ts_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_MGR_
