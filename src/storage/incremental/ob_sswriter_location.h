/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_LOCATION_
#define OCEANBASE_STORAGE_SSWRITER_LOCATION_

#include "lib/hash/ob_link_hashmap.h"
#include "lib/allocator/ob_sql_mem_leak_checker.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ob_inc_ss_define.h"

namespace oceanbase
{
namespace storage
{
typedef common::LinkHashValue<ObSSWriterKey> SSWriterLocationHashValue;
typedef common::LinkHashNode<ObSSWriterKey> SSWriterLocationHashNode;

class ObSSWriterLocationMgr;

class ObSSWriterLocationCtx : public SSWriterLocationHashValue
{
public:
  ObSSWriterLocationCtx()
    : is_inited_(false),
      is_exiting_(false),
      lock_(common::ObLatchIds::OB_SS_WRITER_LOCATION_MGR_LOCK),
      sswriter_key_(),
      addr_(),
      epoch_(-1),
      lease_expire_ts_(-1),
      last_get_addr_ts_(-1),
      last_update_ts_(-1),
      last_request_ts_(-1),
      last_send_request_ts_(-1),
      location_mgr_(NULL) {}
  ~ObSSWriterLocationCtx() { destroy(); }
  int init(
      const ObSSWriterKey &sswriter_key,
      ObSSWriterLocationMgr *location_mgr);
  void reset();
  void destroy() { reset(); }
public:
  int kill();
  int try_renew_location();
  int check_status_and_try_exit();
  int get_addr(ObSSWriterAddr &sswriter_addr);
  int update_addr(
      const common::ObAddr &addr,
      const int64_t epoch,
      const int64_t lease_expire_ts);
public:
  TO_STRING_KV(K_(is_inited), K_(is_exiting), K_(sswriter_key),
      K_(addr), K_(epoch), K_(lease_expire_ts),
      K_(last_get_addr_ts), K_(last_update_ts), K_(last_request_ts),
      K_(last_send_request_ts));
  static const int64_t GC_TIME_INTERVAL = 10 * 1000 * 1000; // 10 seconds
private:
  int set_exiting_();
private:
  bool is_inited_;
  bool is_exiting_;
  ObSpinLock lock_;
  ObSSWriterKey sswriter_key_;
  common::ObAddr addr_;
  int64_t epoch_;
  int64_t lease_expire_ts_;
  int64_t last_get_addr_ts_;
  int64_t last_update_ts_;
  int64_t last_request_ts_; // send request ts
  int64_t last_send_request_ts_;
  ObSSWriterLocationMgr *location_mgr_;
};

class SSWriterLocationAlloc
{
public:
  SSWriterLocationAlloc()
    : active_value_count_(0) {}
  ~SSWriterLocationAlloc() { active_value_count_ = 0; }
  int64_t get_active_value_count() const { return ATOMIC_LOAD(&active_value_count_); }
public:
  ObSSWriterLocationCtx* alloc_value()
  {
    DISABLE_SQL_MEMLEAK_GUARD;
    ObSSWriterLocationCtx *ctx = MTL_NEW(ObSSWriterLocationCtx, "SSWriterLoc");
    if (NULL != ctx) {
      ATOMIC_INC(&active_value_count_);
    }
    return ctx;
  }
  void free_value(ObSSWriterLocationCtx *ctx)
  {
    if (NULL != ctx) {
      MTL_DELETE(ObSSWriterLocationCtx, "SSWriterLoc", ctx);
      ATOMIC_DEC(&active_value_count_);
    }
  }
  SSWriterLocationHashNode *alloc_node(ObSSWriterLocationCtx *ctx)
  {
    DISABLE_SQL_MEMLEAK_GUARD;
    // the input parameter ctx is unused
    UNUSED(ctx);
    return MTL_NEW(SSWriterLocationHashNode, "SSWLocNode");
  }
  void free_node(SSWriterLocationHashNode *node)
  {
    if (NULL != node) {
      MTL_DELETE(SSWriterLocationHashNode, "SSWLocNode", node);
      node = NULL;
    }
  }
private:
  int64_t active_value_count_;
};

typedef common::ObLinkHashMap<ObSSWriterKey, ObSSWriterLocationCtx,
        SSWriterLocationAlloc, common::RefHandle> ObSSWriterLocationMap;

class ObSSWriterLocationMgr
{
public:
  ObSSWriterLocationMgr()
    : is_inited_(false),
      ctx_map_(),
      last_print_ts_(-1) {}
  ~ObSSWriterLocationMgr() { destroy(); }
  int init();
  void destroy();
public:
  void try_print_statistics();
  int try_renew_location();
  int acquire_sswriter_location_ctx(
      const ObSSWriterKey &key,
      ObSSWriterLocationCtx *&ctx);
  int get_sswriter_location_ctx(
      const ObSSWriterKey &sswriter_key,
      ObSSWriterLocationCtx *&sswriter_location_ctx);
  int revert_sswriter_location_ctx(
      ObSSWriterLocationCtx *ctx);
  int erase_sswriter_location_ctx(const ObSSWriterKey &key);
public:
  TO_STRING_KV(K_(is_inited), K_(last_print_ts));
private:
  int release_all_ctx_();
  int get_sswriter_location_ctx_(
      const ObSSWriterKey &sswriter_key,
      ObSSWriterLocationCtx *&sswriter_location_ctx);
private:
  bool is_inited_;
  ObSSWriterLocationMap ctx_map_;
  int64_t last_print_ts_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_LOCATION_
