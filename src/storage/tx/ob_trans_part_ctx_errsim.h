/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_TX_OB_TRANS_PART_CTX_ERRSIM
#define OCEANBASE_STORAGE_TX_OB_TRANS_PART_CTX_ERRSIM

#include "lib/ob_errno.h"
#include "share/ob_ls_id.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_log.h"

namespace oceanbase
{
namespace transaction
{

// Inject OB_EAGAIN in submit_log_block_out_ before palf append.
// set_tp error_code is stored negated; abs() is applied when matching.
// ERRSIM_TX_SUBMIT_LOG_EAGAIN_LOG_TYPE: 0 = all log types; else exact ObTxLogType match.
// ERRSIM_TX_SUBMIT_LOG_EAGAIN_TRANS_ID: 0 = all transactions; else exact trans_id match.
// Both 0 means errsim disabled.
//
// Usage (error_code in set_tp is positive; common ObTxLogType values):
//   TX_COMMIT_INFO_LOG = 0x40 (64), TX_PREPARE_LOG = 0x80 (128), TX_COMMIT_LOG = 0x100 (256)
//
//   -- PREPARE log only, all transactions (LOG_TYPE tp only; TRANS_ID unset / 0)
//   alter system set_tp tp_name = 'ERRSIM_TX_SUBMIT_LOG_EAGAIN_LOG_TYPE',
//     error_code = 128, frequency = 1000000;
//
//   -- all log types, trans_id = 12345 only (TRANS_ID tp only; LOG_TYPE unset / 0)
//   alter system set_tp tp_name = 'ERRSIM_TX_SUBMIT_LOG_EAGAIN_TRANS_ID',
//     error_code = 12345, frequency = 1000000;
//
//   -- PREPARE log + trans_id = 12345
//   alter system set_tp tp_name = 'ERRSIM_TX_SUBMIT_LOG_EAGAIN_LOG_TYPE',
//     error_code = 128, frequency = 1000000;
//   alter system set_tp tp_name = 'ERRSIM_TX_SUBMIT_LOG_EAGAIN_TRANS_ID',
//     error_code = 12345, frequency = 1000000;
//
//   -- disable
//   alter system set_tp tp_name = 'ERRSIM_TX_SUBMIT_LOG_EAGAIN_LOG_TYPE', frequency = 0;
//   alter system set_tp tp_name = 'ERRSIM_TX_SUBMIT_LOG_EAGAIN_TRANS_ID', frequency = 0;
int trans_ctx_submit_log_errsim(const ObTxCbArgArray &cb_arg_array,
                                const ObTransID &trans_id,
                                const share::ObLSID &ls_id);

} // namespace transaction
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_TX_OB_TRANS_PART_CTX_ERRSIM */
