/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_META_RPC_COMMON
#define OCEANBASE_STORAGE_OB_SHARED_META_RPC_COMMON
#include "share/ob_ls_id.h"
#include "lib/string/ob_string.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/scn.h"
#include "logservice/ob_garbage_collector.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "rpc/obrpc/ob_rpc_opts.h"
#include "storage/incremental/share/ob_shared_meta_common.h"

namespace oceanbase
{
namespace storage
{
class ObSharedRpcTransmitArg
{
  OB_UNIS_VERSION(1);
public:
  enum RPCType
  {
    RPC_TYPE_INVALID = 0,
    RPC_TYPE_SHARED_META = 1,
    RPC_TYPE_MAX = 100
  };
  ObSharedRpcTransmitArg()
    : rpc_type_(RPCType::RPC_TYPE_INVALID),
    tenant_id_(OB_INVALID_ID),
    rpc_content_(NULL) {}
  ObSharedRpcTransmitArg(const RPCType rpc_type,
                         const uint64_t tenant_id,
                         const common::ObString rpc_content)
   : rpc_type_(rpc_type),
    tenant_id_(tenant_id),
    rpc_content_(rpc_content) {}

  RPCType get_rpc_type() const { return rpc_type_; }
  void set_rpc_type(const RPCType rpc_type)
  { rpc_type_ = rpc_type; }

  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  const common::ObString &get_rpc_content() const { return rpc_content_; }
  void set_rpc_content(const common::ObString &rpc_content)
  { rpc_content_ = rpc_content; }

  TO_STRING_KV(K_(rpc_type), K_(tenant_id), K_(rpc_content));
private:
  RPCType rpc_type_;
  uint64_t tenant_id_;
  common::ObString rpc_content_;
};

class ObSharedRpcTransmitResult
{
  OB_UNIS_VERSION(1);
public:
  ObSharedRpcTransmitResult()
    : res_code_(),
    res_content_(NULL) {}
  ~ObSharedRpcTransmitResult() { reset(); }
  void reset()
  {
    res_code_.reset();
    res_content_.reset();
  }
  void set_err_code(const int ret) { res_code_.rcode_ = ret; }
  TO_STRING_KV(K_(res_code), K_(res_content));
private:
  obrpc::ObRpcResultCode res_code_;
  common::ObString res_content_;
};

struct ObSharedMetaUpdateArg
{
  OB_UNIS_VERSION_V(1);
public:
  enum : int32_t {
    INVALID_TYPE                 = 0,
    UPDATE_LS_ID_META            = 1,
    UPDATE_LS_GC_STATE           = 2,
    UPDATE_LS_WITH_FUNC          = 3,
    REMOVE_LS                    = 4,
    UPDATE_TABLET_TABLE_STORE    = 5,
    UPDATE_TABLET_META           = 6,
    UPDATE_TABLET_TO_EMPTY_SHELL = 7,
    REMOVE_TABLET                = 8,
  };
  ObSharedMetaUpdateArg()
    : type_(INVALID_TYPE),
      update_reason_(ObMetaUpdateReason::INVALID_META_UPDATE_REASON),
      abs_timeout_ts_(0),
      check_lease_(true) {}
  virtual ~ObSharedMetaUpdateArg() {}
  int64_t get_timeout_remain() const;
  VIRTUAL_TO_STRING_KV(K_(type), K_(update_reason), K_(abs_timeout_ts), K_(check_lease));
public:
  int32_t type_;
  ObMetaUpdateReason update_reason_;
  int64_t abs_timeout_ts_;
  bool check_lease_;
};

struct ObUpdateSharedLSMetaArg : public ObSharedMetaUpdateArg
{
  static constexpr int64_t MAX_ARG_SIZE = 64; // union(32) + functor_buf_(ObString, 16) + padding
  OB_UNIS_VERSION_V(1);
public:
  ObUpdateSharedLSMetaArg()
    : ObSharedMetaUpdateArg()
  {
    init_and_check();
  }
  ObUpdateSharedLSMetaArg(
      const share::ObLSID &ls_id,
      const logservice::LSGCState gc_state,
      const share::SCN &offline_scn,
      const int64_t abs_timeout_ts)
    : ObSharedMetaUpdateArg()
  {
    init_and_check();
    type_ = UPDATE_LS_GC_STATE;
    update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;
    abs_timeout_ts_ = abs_timeout_ts;
    gc_arg_.ls_id_ = ls_id;
    gc_arg_.gc_state_ = gc_state;
    gc_arg_.offline_scn_ = offline_scn;
  }
  ObUpdateSharedLSMetaArg(
      const share::ObLSID &ls_id,
      const ObMetaUpdateReason update_reason,
      const int64_t func_code,
      const int64_t abs_timeout_ts)
    : ObSharedMetaUpdateArg()
  {
    init_and_check();
    type_ = UPDATE_LS_WITH_FUNC;
    update_reason_ = update_reason;
    abs_timeout_ts_ = abs_timeout_ts;
    update_fn_arg_.ls_id_ = ls_id;
    update_fn_arg_.func_code_ = func_code;
  }
  ObUpdateSharedLSMetaArg(
      const share::ObLSID &ls_id,
      const ObMetaUpdateReason update_reason,
      const int64_t func_code,
      const common::ObString &functor_buf,
      const int64_t abs_timeout_ts)
    : ObSharedMetaUpdateArg(),
      functor_buf_(functor_buf)
  {
    init_and_check();
    type_ = UPDATE_LS_WITH_FUNC;
    update_reason_ = update_reason;
    abs_timeout_ts_ = abs_timeout_ts;
    update_fn_arg_.ls_id_ = ls_id;
    update_fn_arg_.func_code_ = func_code;
  }
  ObUpdateSharedLSMetaArg(
      const share::ObLSID &ls_id,
      const int64_t service_type,
      const int64_t limited_id,
      const share::SCN &latest_scn,
      const int64_t abs_timeout_ts)
    : ObSharedMetaUpdateArg()
  {
    init_and_check();
    type_ = UPDATE_LS_ID_META;
    update_reason_ = ObMetaUpdateReason::UPDATE_LS_ID_META;
    abs_timeout_ts_ = abs_timeout_ts;
    id_meta_arg_.ls_id_ = ls_id;
    id_meta_arg_.service_type_ = service_type;
    id_meta_arg_.limited_id_ = limited_id;
    id_meta_arg_.latest_scn_ = latest_scn;
  }
  virtual ~ObUpdateSharedLSMetaArg() {}
  void init_and_check()
  {
    static_assert(sizeof(ObUpdateSharedLSMetaArg) - sizeof(ObSharedMetaUpdateArg) <= MAX_ARG_SIZE, "argument size should not greater than MAX_ARG_SIZE");
    serialize_arg_.first_ = 0;
    serialize_arg_.second_ = 0;
    serialize_arg_.third_ = 0;
    serialize_arg_.forth_ = 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  union {
    struct {
      share::ObLSID ls_id_;
      int64_t service_type_;
      int64_t limited_id_;
      share::SCN latest_scn_;
    } id_meta_arg_;
    struct {
      share::ObLSID ls_id_;
      logservice::LSGCState gc_state_;
      share::SCN offline_scn_;
    } gc_arg_;
    struct {
      share::ObLSID ls_id_;
      int64_t func_code_;
    } update_fn_arg_;
    struct {
      int64_t first_;
      int64_t second_;
      int64_t third_;
      int64_t forth_;
    } serialize_arg_; // smaller than MAX_ARG_SIZE
  };
  common::ObString functor_buf_;
};

} // namespace storage

namespace obrpc
{
class ObSharedRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObSharedRpcProxy);
  virtual ~ObSharedRpcProxy() {}
  RPC_S(PR5 meta_rpc_sync_transmit, obrpc::OB_META_RPC_SYNC_TRANSMIT, (ObSharedRpcTransmitArg), ObSharedRpcTransmitResult);
};

class ObSharedRpcP : public obrpc::ObRpcProcessor< ObSharedRpcProxy::ObRpc<obrpc::OB_META_RPC_SYNC_TRANSMIT> >
{
public:
  ObSharedRpcP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObSharedRpcP() {}
public:
  virtual int process();
private:
  int process_meta_rpc_(const storage::ObSharedRpcTransmitArg &arg,
                        storage::ObSharedRpcTransmitResult &res);
  const observer::ObGlobalContext &gctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedRpcP);
};
} // namespace obrpc
} // namespace oceanbase
#endif
