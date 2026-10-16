/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/incremental/ob_shared_meta_rpc_common.h"
#include "storage/incremental/ob_shared_meta_service.h"

using namespace oceanbase::storage;
using namespace oceanbase::obrpc;
namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObSharedRpcTransmitArg,
                    rpc_type_,
                    tenant_id_,
                    rpc_content_);

OB_SERIALIZE_MEMBER(ObSharedRpcTransmitResult,
                    res_code_,
                    res_content_);

OB_SERIALIZE_MEMBER(ObSharedMetaUpdateArg,
                    type_,
                    update_reason_,
                    check_lease_);

OB_SERIALIZE_MEMBER_INHERIT(ObUpdateSharedLSMetaArg, ObSharedMetaUpdateArg,
                            serialize_arg_.first_,
                            serialize_arg_.second_,
                            serialize_arg_.third_,
                            serialize_arg_.forth_,
                            functor_buf_);

int64_t ObSharedMetaUpdateArg::get_timeout_remain() const
{
  return abs_timeout_ts_ - ObTimeUtility::current_time();
}

int64_t ObUpdateSharedLSMetaArg::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type));
  J_KV(K_(update_reason));
  J_COMMA();
  switch (type_) {
  case UPDATE_LS_ID_META: {
    J_KV(K_(id_meta_arg_.ls_id), K_(id_meta_arg_.service_type), K_(id_meta_arg_.limited_id), K_(id_meta_arg_.latest_scn));
    break;
  }
  case UPDATE_LS_GC_STATE: {
    J_KV(K_(gc_arg_.ls_id), K_(gc_arg_.gc_state), K_(gc_arg_.offline_scn));
    break;
  }
  case UPDATE_LS_WITH_FUNC: {
    J_KV(K_(update_fn_arg_.ls_id), K_(update_fn_arg_.func_code), K_(functor_buf));
    break;
  }
  default: {
    J_KV(K_(serialize_arg_.first), K_(serialize_arg_.second), K_(serialize_arg_.third), K_(serialize_arg_.forth));
  }
  }
  J_OBJ_END();
  return pos;
}

} // namespace storage

namespace obrpc
{
int ObSharedRpcP::process()
{
  int ret = OB_SUCCESS;
  const ObSharedRpcTransmitArg &transmit_arg = arg_;
  ObSharedRpcTransmitResult &transmit_result = result_;
  switch (transmit_arg.get_rpc_type()) {
  case ObSharedRpcTransmitArg::RPC_TYPE_SHARED_META: {
    if (OB_FAIL(process_meta_rpc_(transmit_arg, transmit_result))) {
      LOG_WARN("process_meta_rpc failed", K(ret), K(transmit_arg));
    }
    break;
  }
  default: {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Unknown rpc type", K(ret), K(transmit_arg.get_rpc_type()));
    break;
  }
  }
  if (OB_FAIL(ret)) {
    transmit_result.set_err_code(ret);
  }

  return ret;
}

int ObSharedRpcP::process_meta_rpc_(const ObSharedRpcTransmitArg &arg,
                                    ObSharedRpcTransmitResult &res)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService*);
  if (OB_ISNULL(meta_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta service v2 should not be null", K(ret), KP(meta_svr));
  } else if (OB_FAIL(meta_svr->process_meta_rpc(arg, res))) {
    LOG_WARN("process meta rpc failed", K(ret), K(arg));
  }
  return ret;
}

}
} // namespace oceanbase
