/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_NEW_ATOMIC_PROTOCOL_CTRL_DEFINE_
#define OCEANBASE_NEW_ATOMIC_PROTOCOL_CTRL_DEFINE_

#include <string.h>
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_lock_atomic_protocol.h"

namespace oceanbase
{
namespace storage
{
class ObAtomicProtocolCtrl final
{
public:
  ObAtomicProtocolCtrl();
  ~ObAtomicProtocolCtrl();
  int init(
      const ObSSMetaCtrlKeyInfo &meta_key_info);
  int cancel_atomic_protocol();
private:
  int start_trans_(
      const int64_t stmt_timeout,
      ObTimeoutCtx &timeout_ctx,
      ObMySQLTransaction &trans);
  int commit_trans_(
      const int32_t &result,
      ObMySQLTransaction &trans);
  int lock_lock_row_(
      ObMySQLTransaction &trans,
      ObSSMetaCtrlLockRowValue &lock_row_value);
  int prepare_lock_row_(
      ObLockAtomicProtocol &lock_row);
  int select_ctrl_row_(
      common::ObISQLClient &trans,
      ObSSMetaCtrlValue &ctrl_value);
  int update_ctrl_row_(
      const ObSSMetaCtrlValue &ctrl_value,
      common::ObISQLClient &trans);
  int get_row_meta_key_(
      const ObSSMetaCtrlKey &ctrl_meta_key,
      common::ObIAllocator &allocator,
      ObString &meta_key);
  int get_row_meta_value_(
      const ObSSMetaCtrlValue &row_value,
      common::ObIAllocator &allocator,
      ObString &meta_value);
  int get_row_ctrl_value_(
      const ObString &meta_value,
      ObSSMetaCtrlValue &ctrl_value);
private:
  bool is_inited_;
  ObSSMetaCtrlKeyInfo meta_key_info_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t sql_timeout_us_;
  DISALLOW_COPY_AND_ASSIGN(ObAtomicProtocolCtrl);
};

} //storage
} //oceanbase

#endif
