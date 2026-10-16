/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ATOMIC_PROTOCOL_
#define OCEANBASE_ATOMIC_PROTOCOL_

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
#include "ob_lock_atomic_protocol.h"


namespace oceanbase
{
namespace storage
{

// User-side hook executed inside ObAtomicProtocol::finish_()'s transaction.
// Subclass and override `process()` to inject custom DML in the same trans;
// returning a non-OB_SUCCESS rolls the whole finish trans back.
// The default no-op implementation is intentionally provided so callers that
// do not need extra DML can simply pass a default-constructed instance.
class ObIAtomicProtocolOP
{
public:
  ObIAtomicProtocolOP() {}
  virtual ~ObIAtomicProtocolOP() {}
  virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) = 0;
};

// No-op ObIAtomicProtocolOP for protocols that pass an op to finish_()/abort_()
// but need no extra in-trans DML of their own.
class ObEmptyAtomicProtocolOP : public ObIAtomicProtocolOP
{
public:
  ObEmptyAtomicProtocolOP() {}
  virtual ~ObEmptyAtomicProtocolOP() {}
  int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override
  {
    UNUSED(trans);
    UNUSED(meta_scn);
    return OB_SUCCESS;
  }
  DISALLOW_COPY_AND_ASSIGN(ObEmptyAtomicProtocolOP);
};

class ObAtomicProtocol
{
public:
  ObAtomicProtocol();
  virtual ~ObAtomicProtocol();
  virtual int create(
      const bool need_check_sswriter) = 0;
  virtual int finish(ObIAtomicProtocolOP &op) = 0;
  virtual int abort() = 0;
  virtual int get_op_id(int64_t &op_id);
  virtual int get_protocol_type(ObAtomicProtocolType &type);
  virtual int get_meta_scn(share::SCN &meta_scn);
  bool is_status_start() const { return STATUS::START == status_;}
  bool is_status_complete() const { return STATUS::COMPLETE == status_; }
  bool is_status_abandon() const { return STATUS::ABANDON == status_; }

  VIRTUAL_TO_STRING_KV(K_(status), K_(lease_epoch), K_(type), K_(control_scn), K_(meta_scn),
      KP_(sql_proxy), K_(op_id), K_(is_deleted), K_(deleted_scn), K_(sql_timeout_us));
protected:
  virtual int prepare_ss_meta_ctrl_key_(
      ObSSMetaCtrlKey &ctrl_key)= 0 ;
  virtual int prepare_atomic_ctrl_info_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObSSMetaCtrlValue &ctrl_value) = 0;
  virtual int update_lock_row_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObMySQLTransaction &trans) = 0;
  virtual int prepare_lock_row_(
      ObLockAtomicProtocol &lock_row) = 0;
  // create_() 拿到本实体 lock row 行锁后、写入 ctrl row 前调用的前置检查，
  // 运行在 create 事务内（sql_client 即该事务），与同一实体的其他
  // create/finish/abort/gc 持锁串行。子类用它实现幂等 create：发现已有
  // 可复用的 ctrl row 时置 need_create=false 并从该行加载协议状态，
  // create_() 随即跳过写入直接成功返回。默认不做检查、总是创建。
  virtual int check_before_create_under_lock_(
      common::ObISQLClient &sql_client,
      bool &need_create);
  // Lock the parent lock row(s) this entity depends on, BEFORE its own lock row
  // is locked, so the global lock order is always tablet -> sstable (matching
  // ObTabletAtomicProtocol::delete_key_infos_) and no deadlock can arise.
  // Default is a no-op; only sstable-level protocols (which depend on the owning
  // tablet's TABLET_LOCK_ROW) override it.
  virtual int lock_parent_lock_row_(ObMySQLTransaction &trans);
  virtual int get_control_scn_(share::SCN &control_scn);
protected:
  int check_can_do_create_();
  int check_can_do_finish_();
  int check_can_do_abort_();
  void set_status_init_() { status_ = STATUS::INIT; }
  void set_status_start_() { status_ = STATUS::START; }
  void set_status_abandon_() { status_ = STATUS::ABANDON; }
  void set_status_complete_() { status_ = STATUS::COMPLETE; }
  int on_start_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool need_check_sswriter,
      int64_t &current_lease_epoch);
  int on_finish_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  int check_lease_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      bool &is_sswriter,
      int64_t &current_epoch);
  int get_sswriter_type(ObSSWriterType &sswriter_type) const;

  int select_ctrl_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &ctrl_key,
      common::ObISQLClient &trans,
      ObSSMetaCtrlValue &ctrl_value);
  int insert_ctrl_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &ctrl_key,
      const ObSSMetaCtrlValue &ctrl_value,
      common::ObISQLClient &trans);
  int delete_ctrl_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &ctrl_key,
      common::ObISQLClient &trans);
  int update_ctrl_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &ctrl_key,
      const ObSSMetaCtrlValue &ctrl_value,
      common::ObISQLClient &trans);

  int get_row_meta_key_(
      const ObSSMetaCtrlKey &ctrl_meta_key,
      common::ObIAllocator &allocator,
      ObString &meta_value);
  // Serialize only the key prefix (id_1/id_2) for LIKE-prefix range scans.
  // control_scn is intentionally not part of the prefix, so the key here may have
  // an unset control_scn and is_valid() is deliberately not enforced.
  int get_row_meta_key_prefix_(
      const ObSSMetaCtrlKey &ctrl_meta_key,
      common::ObIAllocator &allocator,
      ObString &meta_key_prefix);
  int get_row_meta_value_(
      const ObSSMetaCtrlValue &row_value,
      common::ObIAllocator &allocator,
      ObString &meta_value);
  int get_row_ctrl_key_(
      const ObString &meta_value,
      ObSSMetaCtrlKey &ctrl_key);
  int get_row_ctrl_value_(
      const ObString &meta_value,
      ObSSMetaCtrlValue &ctrl_value);

protected:
  int start_trans_(
      const int64_t stmt_timeout,
      ObTimeoutCtx &timeout_ctx,
      ObMySQLTransaction &trans);
  int commit_trans_(
      const int32_t &result,
      ObMySQLTransaction &trans);
  int get_control_scn_(
      const uint64_t tenant_id,
      const ObSSMetaCtrlLockRowValue &lock_row_value);
  // Allocates control_scn_ at create(): max(GTS, lock_row.meta_scn + 1).
  // Does not read max_reserved_scn; GC safety is enforced on business meta_scn at finish.
  int create_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool need_check_sswrite);
  int abort_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);
  int abort_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      ObIAtomicProtocolOP &op);
  int finish_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const bool need_delete_row,
      const bool need_check_op_id,
      ObIAtomicProtocolOP &op);
  int lock_lock_row_(
      ObMySQLTransaction &trans,
      ObSSMetaCtrlLockRowValue &lock_row_value);
  int check_and_create_lock_row_();
  int update_lock_row_at_finish_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObMySQLTransaction &trans);
  int prepare_lock_row_key_(
      const ObSSMetaCtrlKey &src,
      ObSSMetaCtrlKey &dest);

  int get_max_reserved_scn_(
      const uint64_t tenant_id,
      const bool need_lock,
      ObMySQLTransaction &trans,
      share::SCN &max_reserved_scn);
  // Allocates meta_scn_ at finish(): max(GTS, lock_row.meta_scn + 1, max_reserved_scn + 1).
  int get_meta_scn_(
      const uint64_t tenant_id,
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObMySQLTransaction &trans);

private:
  // A zero epoch skips writer qualification checks; otherwise FINISH must match the epoch recorded at START.
  bool need_check_sswriter_() const { return lease_epoch_ > 0; }

protected:
  enum class STATUS : int
  {
    INVALID = -1,
    INIT = 0,
    START = 1,
    COMPLETE = 2,
    ABANDON = 3,
    MAX,
  };
protected:
  STATUS status_;
  int64_t lease_epoch_;
  ObAtomicProtocolType type_;
  share::SCN control_scn_;
  share::SCN meta_scn_;
  common::ObMySQLProxy *sql_proxy_;
  int64_t op_id_;
  bool is_deleted_;
  share::SCN deleted_scn_;
  int64_t last_major_snapshot_;
  int64_t sql_timeout_us_;
};


} //storage
} //oceanbase

#endif
