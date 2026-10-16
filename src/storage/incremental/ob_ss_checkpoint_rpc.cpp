/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_ss_checkpoint_rpc.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::storage;
using namespace oceanbase::obrpc;

namespace oceanbase {
namespace storage {

OB_SERIALIZE_MEMBER(ObSSRpcCollectCNClogCkptArg, tenant_id_, ls_id_);

OB_SERIALIZE_MEMBER(ObSSRpcCollectCNClogCkptResult, ret_code_, clog_checkpoint_scn_);

}  // namespace storage

namespace obrpc {

int ObSSCkptRpcP::process()
{
  int ret = OB_SUCCESS;
  ObSSRpcCollectCNClogCkptArg &rpc_arg = arg_;
  ObSSRpcCollectCNClogCkptResult &rpc_result = result_;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(arg_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    result_.ret_code_ = ret;
  } else {
    result_.ret_code_ = OB_SUCCESS;
    result_.clog_checkpoint_scn_ = ls_handle.get_ls()->get_clog_checkpoint_scn();
    FLOG_INFO("finish handle collect ckpt rpc", K(arg_.ls_id_), K(result_));
  }
  return ret;
}

}  // namespace obrpc

}  // namespace oceanbase
