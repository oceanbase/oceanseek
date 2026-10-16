/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOCK_ATOMIC_PROTOCOL_
#define OCEANBASE_LOCK_ATOMIC_PROTOCOL_

#include <string.h>
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "ob_atomic_protocol_define.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"


namespace oceanbase
{
namespace storage
{

class ObLockAtomicProtocol final
{
public:
  ObLockAtomicProtocol();
  ~ObLockAtomicProtocol();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &meta_key);
  int check_and_create_lock_row(
      common::ObISQLClient &trans);
  int lock_lock_row(
      common::ObISQLClient &trans,
      ObSSMetaCtrlLockRowValue &row_value);
  // Read the lock row value WITHOUT taking a FOR UPDATE lock. Unlike
  // lock_lock_row(), this only does a plain SELECT, so it can run on a bare
  // sql_proxy outside any transaction.
  int read_lock_row(
      common::ObISQLClient &sql_client,
      ObSSMetaCtrlLockRowValue &row_value);
  int update_lock_row(
      const ObSSMetaCtrlLockRowValue &row_value,
      common::ObISQLClient &trans);
  int persist_kept_meta_scn(
      const share::SCN &kept_meta_scn,
      common::ObISQLClient &trans);
private:
  int get_row_meta_key_(
      common::ObIAllocator &allocator,
      ObString &meta_value);
  int get_row_meta_value_(
      const ObSSMetaCtrlLockRowValue &row_value,
      common::ObIAllocator &allocator,
      ObString &meta_value);
  // 只执行精确 UPDATE，不重复读取；调用方负责在需要时先完成校验和加锁。
  int write_lock_row_value_(
      const ObSSMetaCtrlLockRowValue &row_value,
      common::ObISQLClient &trans);
  int check_can_update_(
      const ObSSMetaCtrlLockRowValue &src_value,
      const ObSSMetaCtrlLockRowValue &dest_value);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObSSMetaCtrlKey meta_ctrl_key_;
  int64_t sql_timeout_us_;
  DISALLOW_COPY_AND_ASSIGN(ObLockAtomicProtocol);
};



} //storage
} //oceanbase



#endif
