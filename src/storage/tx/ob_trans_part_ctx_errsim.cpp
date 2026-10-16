/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX TRANS

#include "storage/tx/ob_trans_part_ctx_errsim.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{

ERRSIM_POINT_DEF(ERRSIM_TX_SUBMIT_LOG_EAGAIN_LOG_TYPE);
ERRSIM_POINT_DEF(ERRSIM_TX_SUBMIT_LOG_EAGAIN_TRANS_ID);

int trans_ctx_submit_log_errsim(const ObTxCbArgArray &cb_arg_array,
                                const ObTransID &trans_id,
                                const share::ObLSID &ls_id)
{
  const int log_type_cfg = abs(ERRSIM_TX_SUBMIT_LOG_EAGAIN_LOG_TYPE);
  const int trans_id_cfg = abs(ERRSIM_TX_SUBMIT_LOG_EAGAIN_TRANS_ID);
  if (0 == log_type_cfg && 0 == trans_id_cfg) {
    return OB_SUCCESS;
  }
  if (0 != trans_id_cfg
      && trans_id.get_id() != static_cast<uint64_t>(trans_id_cfg)) {
    return OB_SUCCESS;
  }
  if (0 != log_type_cfg) {
    const ObTxLogType target_log_type = static_cast<ObTxLogType>(log_type_cfg);
    bool hit = false;
    for (int64_t i = 0; i < cb_arg_array.count(); ++i) {
      if (cb_arg_array.at(i).get_log_type() == target_log_type) {
        hit = true;
        break;
      }
    }
    if (!hit) {
      return OB_SUCCESS;
    }
  }
  TRANS_LOG(INFO, "[ERRSIM] submit log eagain", K(log_type_cfg), K(trans_id_cfg),
            K(trans_id), K(ls_id), K(cb_arg_array));
  return OB_EAGAIN;
}

} // namespace transaction
} // namespace oceanbase
