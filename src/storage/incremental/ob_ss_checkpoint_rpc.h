/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SS_CHECKPOINT_RPC_H_
#define OB_SS_CHECKPOINT_RPC_H_

#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace storage {
// CN means Computing-Node
class ObSSRpcCollectCNClogCkptArg {
  OB_UNIS_VERSION(1);

public:
  ObSSRpcCollectCNClogCkptArg() {}
  ObSSRpcCollectCNClogCkptArg(const uint64_t tenant_id, const share::ObLSID ls_id)
      : tenant_id_(tenant_id), ls_id_(ls_id) {}
  int assign(const ObSSRpcCollectCNClogCkptArg &other);
  void reset() {
    tenant_id_ = 0;
    ls_id_.reset();
  }

  bool is_valid() const { return true; }

  TO_STRING_KV(K(tenant_id_), K(ls_id_));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

class ObSSRpcCollectCNClogCkptResult {
  OB_UNIS_VERSION(1);

public:
  ObSSRpcCollectCNClogCkptResult() : ret_code_(), clog_checkpoint_scn_() {}
  void reset()
  {
    ret_code_ = 0;
    clog_checkpoint_scn_.set_min();
  }
  TO_STRING_KV(K(ret_code_), K(clog_checkpoint_scn_));

  obrpc::Int64 ret_code_;
  share::SCN clog_checkpoint_scn_;
};
}  // namespace storage

namespace obrpc {
class ObSSCkptRpcProxy : public obrpc::ObRpcProxy {
public:
  DEFINE_TO(ObSSCkptRpcProxy);
  virtual ~ObSSCkptRpcProxy() {}
  RPC_S(PR5 collect_cn_clog_ckpt,
        obrpc::OB_COLLECT_CN_CLOG_CKPT,
        (ObSSRpcCollectCNClogCkptArg),
        ObSSRpcCollectCNClogCkptResult);
};

class ObSSCkptRpcP : public obrpc::ObRpcProcessor<ObSSCkptRpcProxy::ObRpc<obrpc::OB_COLLECT_CN_CLOG_CKPT>> {
public:
  ObSSCkptRpcP() {}
  virtual ~ObSSCkptRpcP() {}

public:
  virtual int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSCkptRpcP);
};
}  // namespace obrpc

} // namespace oceanbase

#endif