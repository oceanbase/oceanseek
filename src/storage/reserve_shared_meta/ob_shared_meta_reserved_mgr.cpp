/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_shared_meta_reserved_mgr.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_define.h"
#include "share/ob_share_util.h"
#include "share/schema/ob_schema_struct.h"
#include "observer/ob_server_struct.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/tablelock/ob_lock_utils.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

ObSharedMetaReservedMgr::ObSharedMetaReservedMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    sql_proxy_(nullptr)
{
}

int ObSharedMetaReservedMgr::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("shared meta reserved mgr init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(tenant_id)
                         && !is_meta_tenant(tenant_id)
                         && !is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init get invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

void ObSharedMetaReservedMgr::reset()
{
  sql_proxy_ = nullptr;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

int ObSharedMetaReservedMgr::create_meta_row(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSharedMetaReservedOperator trans_op;
  ObSharedMetaReservedInfo max_reserved_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_FAIL(prepare_max_reserved_info_(share::SCN::min_scn(), max_reserved_info))) {
    LOG_WARN("failed to prepare max reserved info", KR(ret));
  } else if (OB_FAIL(trans_op.init(trans, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init trans operator", KR(ret));
  } else if (OB_FAIL(trans_op.insert_row(max_reserved_info, affected_rows))) {
    LOG_WARN("failed to insert max reserved row", KR(ret), K(max_reserved_info));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows on insert", KR(ret), K(max_reserved_info), K(affected_rows));
  }
  return ret;
}

int ObSharedMetaReservedMgr::prepare_max_reserved_info_(
    const share::SCN &meta_scn,
    ObSharedMetaReservedInfo &info) const
{
  int ret = OB_SUCCESS;
  info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare max reserved info get invalid argument", KR(ret), K(meta_scn));
  } else {
    const ObSharedMetaReservedType max_reserved_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
    info = ObSharedMetaReservedInfo(tenant_id_, max_reserved_type, meta_scn);
  }
  return ret;
}

int ObSharedMetaReservedMgr::get_max_reserved_info_(ObSharedMetaReservedInfo &info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(get_max_reserved_info_(*sql_proxy_, info))) {
    LOG_WARN("failed to get max reserved info", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedMgr::get_max_reserved_info_(
    common::ObISQLClient &sql_client,
    ObSharedMetaReservedInfo &info) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSharedMetaReservedInfo, 1> infos;
  ObSharedMetaReservedOperator op;
  const ObSharedMetaReservedType max_reserved_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  info.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_FAIL(op.init(sql_client, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init shared meta reserved operator", KR(ret));
  } else if (OB_FAIL(op.scan_rows(tenant_id_, max_reserved_type, 0 /*limit_count*/, infos))) {
    LOG_WARN("failed to scan max reserved info", KR(ret), K_(tenant_id));
  } else if (0 == infos.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("max reserved info not exist", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(1 != infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max reserved info count", KR(ret), K_(tenant_id), K(infos));
  } else if (OB_FAIL(info.assign(infos.at(0)))) {
    LOG_WARN("failed to assign max reserved info", KR(ret), K(infos.at(0)));
  }
  return ret;
}

int ObSharedMetaReservedMgr::update_max_reserved_info_(
    common::ObMySQLTransaction &trans,
    const ObSharedMetaReservedInfo &info,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSharedMetaReservedInfo new_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!info.is_valid()
                         || info.tenant_id_ != tenant_id_
                         || !info.type_.is_max_reserved()
                         || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update max reserved info get invalid argument", KR(ret), K(info), K(meta_scn));
  } else if (meta_scn <= info.meta_scn_) {
    // MAX_RESERVED is a monotonically non-decreasing high water mark: only
    // advance it when the new scn is strictly larger than the current one.
    LOG_INFO("skip updating max reserved info", K(info), K(meta_scn));
  } else if (OB_FAIL(prepare_max_reserved_info_(meta_scn, new_info))) {
    LOG_WARN("failed to prepare new max reserved info", KR(ret), K(meta_scn));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else {
    // The EXCLUSIVE table lock is already held by the caller (create_reserved_scn
    // takes it before entering create_reserved_scn_ -> update_max_reserved_info_),
    // so we do not re-lock here.
    ObSharedMetaReservedOperator trans_op;
    if (OB_FAIL(trans_op.init(trans, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init trans operator", KR(ret));
    } else if (OB_FAIL(trans_op.delete_row(info, affected_rows))) {
      LOG_WARN("failed to delete old max reserved info", KR(ret), K(info));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows on delete", KR(ret), K(info), K(affected_rows));
    } else if (FALSE_IT(affected_rows = 0)) {
    } else if (OB_FAIL(trans_op.insert_row(new_info, affected_rows))) {
      LOG_WARN("failed to insert new max reserved info", KR(ret), K(new_info));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows on insert", KR(ret), K(new_info), K(affected_rows));
    }
  }
  return ret;
}

int ObSharedMetaReservedMgr::create_reserved_scn(
    const ObSharedMetaReservedType &type,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_scn_keyed() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scn-keyed create get invalid argument", KR(ret), K(type), K(meta_scn));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(start_trans_(trans))) {
      LOG_WARN("failed to start trans", KR(ret));
    } else {
      if (OB_FAIL(create_reserved_scn(trans, type, meta_scn))) {
        LOG_WARN("failed to create reserved scn", KR(ret), K(type), K(meta_scn));
      }
      if (OB_TMP_FAIL(commit_trans_(trans, ret))) {
        LOG_WARN("failed to commit trans", KR(tmp_ret), KR(ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObSharedMetaReservedMgr::create_reserved_scn(
    common::ObMySQLTransaction &trans,
    const ObSharedMetaReservedType &type,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  ObSharedGCControlInfo gc_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_scn_keyed() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scn-keyed create get invalid argument", KR(ret), K(type), K(meta_scn));
  } else if (OB_FAIL(lock_table_in_exclusive_(trans))) {
    LOG_WARN("failed to lock table in exclusive mode", KR(ret));
  } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(trans, tenant_id_, gc_info))) {
    LOG_WARN("failed to select tenant id for update", KR(ret));
  } else if (gc_info.gc_meta_scn_ >= meta_scn) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("gc meta scn is greater than meta scn, can't create reserved scn", KR(ret),
        K(gc_info.gc_meta_scn_), K(meta_scn));
  } else if (OB_FAIL(create_reserved_scn_(trans, type, meta_scn))) {
    LOG_WARN("failed to create reserved scn", KR(ret), K(type), K(meta_scn));
  }
  return ret;
}

int ObSharedMetaReservedMgr::create_reserved_scn(
    const ObSharedMetaReservedType &type,
    const ObString &owner,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SCN tmp_meta_scn;
  ObSharedMetaReservedInfo owner_info;
  meta_scn.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_owner_keyed() || owner.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("owner-keyed create get invalid argument", KR(ret), K(type), K(owner.length()));
  } else if (OB_FAIL(owner_info.set_owner(owner))) {
    LOG_WARN("invalid owner", KR(ret), K(type), K(owner.length()));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(start_trans_(trans))) {
      LOG_WARN("failed to start trans", KR(ret));
    } else {
      if (OB_FAIL(create_reserved_scn(trans, type, owner_info.get_owner(), tmp_meta_scn))) {
        LOG_WARN("failed to create owner-keyed reserved scn",
            KR(ret), K(type), K(owner.length()));
      }
      if (OB_TMP_FAIL(commit_trans_(trans, ret))) {
        LOG_WARN("failed to commit trans", KR(tmp_ret), KR(ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    meta_scn = tmp_meta_scn;
  }
  return ret;
}

int ObSharedMetaReservedMgr::create_reserved_scn(
    common::ObMySQLTransaction &trans,
    const ObSharedMetaReservedType &type,
    const ObString &owner,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  SCN gts;
  ObSharedGCControlInfo gc_info;
  ObSharedMetaReservedInfo max_reserved_info;
  ObSharedMetaReservedInfo owner_info;
  ObSharedMetaReservedOperator trans_op;

  meta_scn.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_owner_keyed() || owner.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("owner-keyed create get invalid argument", KR(ret), K(type), K(owner.length()));
  } else if (OB_FAIL(owner_info.set_owner(owner))) {
    LOG_WARN("invalid owner", KR(ret), K(type), K(owner.length()));
  } else if (OB_FAIL(lock_table_in_exclusive_(trans))) {
    LOG_WARN("failed to lock table in exclusive mode", KR(ret));
  } else if (OB_FAIL(trans_op.init(trans, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init trans operator", KR(ret));
  } else if (OB_FAIL(ObSharedGCControlOperator::select_tenant_id_for_update(
      trans, tenant_id_, gc_info))) {
    LOG_WARN("failed to select tenant id for update", KR(ret));
  } else if (OB_FAIL(ObAtomicProtocolUtil::get_gts_sync(tenant_id_, gts))) {
    LOG_WARN("failed to get gts for owner-keyed reservation", KR(ret), K(type));
  } else if (OB_UNLIKELY(gts < gc_info.gc_meta_scn_)) {
    // GTS是在持有EXCLUSIVE表锁和gc控制行锁之后获取的, 而gc_meta_scn只会在
    // 同样的锁内推进, 且不会超过其所在事务的GTS, 因此锁内新取的GTS不可能
    // 小于已提交的gc_meta_scn
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gts fetched under lock is less than gc meta scn",
        KR(ret), K(gc_info.gc_meta_scn_), K(gts));
  } else if (OB_FAIL(get_max_reserved_info_(trans, max_reserved_info))) {
    LOG_WARN("failed to get max reserved info", KR(ret), K_(tenant_id));
  } else {
    owner_info.tenant_id_ = tenant_id_;
    owner_info.type_ = type;
    owner_info.meta_scn_ = gts;
    if (OB_FAIL(update_max_reserved_info_(trans, max_reserved_info, gts))) {
      LOG_WARN("failed to update max reserved info",
          KR(ret), K(max_reserved_info), K(gts));
    } else if (OB_FAIL(trans_op.insert_row(owner_info, affected_rows))) {
      LOG_WARN("failed to insert owner-keyed reserved row", KR(ret), K(owner_info));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows on insert",
          KR(ret), K(owner_info), K(affected_rows));
    }
  }
  if (OB_SUCC(ret)) {
    meta_scn = gts;
  }
  return ret;
}

int ObSharedMetaReservedMgr::create_reserved_scn_(
    common::ObMySQLTransaction &trans,
    const ObSharedMetaReservedType &type,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const ObSharedMetaReservedInfo info(tenant_id_, type, meta_scn);
  ObSharedMetaReservedOperator trans_op;
  ObSharedMetaReservedInfo max_reserved_info;
  if (OB_FAIL(get_max_reserved_info_(trans, max_reserved_info))) {
    LOG_WARN("failed to get max reserved info", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(update_max_reserved_info_(trans, max_reserved_info, meta_scn))) {
    LOG_WARN("failed to update max reserved info", KR(ret), K(max_reserved_info), K(meta_scn));
  } else if (OB_FAIL(trans_op.init(trans, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init trans operator", KR(ret));
  } else if (OB_FAIL(trans_op.insert_row(info, affected_rows))) {
    LOG_WARN("failed to insert reserved row", KR(ret), K(info));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows on insert", KR(ret), K(info), K(affected_rows));
  }
  return ret;
}

int ObSharedMetaReservedMgr::get_real_tenant_id_(uint64_t &real_tenant_id) const
{
  int ret = OB_SUCCESS;
  real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_FAIL(ObSharedMetaReservedUtil::get_real_tenant_id(tenant_id_, real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedMgr::start_trans_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id_(real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(trans.start(sql_proxy_, real_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(real_tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedMgr::commit_trans_(
    common::ObMySQLTransaction &trans,
    int &result)
{
  int ret = OB_SUCCESS;
  const bool want_commit = (OB_SUCCESS == result);
  if (OB_FAIL(trans.end(want_commit))) {
    LOG_WARN("failed to end trans", KR(ret), K(want_commit), K(result));
    result = (OB_SUCCESS == result) ? ret : result;
  }
  return ret;
}

int ObSharedMetaReservedMgr::lock_table_in_exclusive_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  // The writer takes the EXCLUSIVE lock with HIGH1 priority so that, when it
  // contends with concurrent readers (lock_table_in_share_, NORMAL priority),
  // the writer is granted first and the high water mark keeps advancing.
  // force_use_priority is true so the priority is honored regardless of the
  // tenant config `enable_lock_priority`.
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id_(real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(transaction::tablelock::ObInnerTableLockUtil::lock_inner_table_in_trans(
                 trans,
                 real_tenant_id,
                 share::OB_ALL_SHARED_META_RESERVED_SCN_TID,
                 transaction::tablelock::EXCLUSIVE,
                 false/*is_from_sql*/,
                 transaction::tablelock::ObTableLockPriority::HIGH1,
                 true/*force_use_priority*/))) {
    LOG_WARN("failed to lock shared meta reserved table", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedMgr::lock_table_in_share_(common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  // Readers take the SHARE lock without forcing the priority queue. The SHARE
  // lock does not need priority scheduling, while writers still use HIGH1 with
  // force_use_priority in lock_table_in_exclusive_.
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_FAIL(get_real_tenant_id_(real_tenant_id))) {
    LOG_WARN("failed to get real tenant id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(transaction::tablelock::ObInnerTableLockUtil::lock_inner_table_in_trans(
                 trans,
                 real_tenant_id,
                 share::OB_ALL_SHARED_META_RESERVED_SCN_TID,
                 transaction::tablelock::SHARE,
                 false/*is_from_sql*/,
                 transaction::tablelock::ObTableLockPriority::NORMAL,
                 false/*force_use_priority*/))) {
    LOG_WARN("failed to lock shared meta reserved table", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSharedMetaReservedMgr::remove_reserved_scn(
    const ObSharedMetaReservedType &type,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_scn_keyed() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scn-keyed remove get invalid argument", KR(ret), K(type), K(meta_scn));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(start_trans_(trans))) {
      LOG_WARN("failed to start remove reserved scn trans", KR(ret), K(type), K(meta_scn));
    } else {
      if (OB_FAIL(remove_reserved_scn(trans, type, meta_scn))) {
        LOG_WARN("failed to remove reserved scn", KR(ret), K(type), K(meta_scn));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(commit_trans_(trans, ret))) {
        LOG_WARN("failed to commit remove reserved scn trans", KR(tmp_ret), KR(ret),
            K(type), K(meta_scn));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObSharedMetaReservedMgr::remove_reserved_scn(
    common::ObMySQLTransaction &trans,
    const ObSharedMetaReservedType &type,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_scn_keyed() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scn-keyed remove get invalid argument", KR(ret), K(type), K(meta_scn));
  } else if (OB_FAIL(lock_table_in_exclusive_(trans))) {
    LOG_WARN("failed to lock shared meta reserved table before remove", KR(ret),
        K(type), K(meta_scn));
  } else if (OB_FAIL(remove_reserved_scn_(trans, type, meta_scn))) {
    LOG_WARN("failed to remove reserved scn", KR(ret), K(type), K(meta_scn));
  }
  return ret;
}

int ObSharedMetaReservedMgr::remove_reserved_scn(
    const ObSharedMetaReservedType &type,
    const ObString &owner)
{
  int ret = OB_SUCCESS;
  ObSharedMetaReservedInfo owner_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_owner_keyed() || owner.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("owner-keyed remove get invalid argument", KR(ret), K(type), K(owner.length()));
  } else if (OB_FAIL(owner_info.set_owner(owner))) {
    LOG_WARN("invalid owner", KR(ret), K(type), K(owner.length()));
  } else {
    ObMySQLTransaction trans;
    if (OB_FAIL(start_trans_(trans))) {
      LOG_WARN("failed to start remove owner-keyed reserved scn trans", KR(ret),
          K(type), K(owner.length()));
    } else {
      if (OB_FAIL(remove_reserved_scn(trans, type, owner_info.get_owner()))) {
        LOG_WARN("failed to remove owner-keyed reserved scn",
            KR(ret), K(type), K(owner.length()));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(commit_trans_(trans, ret))) {
        LOG_WARN("failed to commit remove owner-keyed reserved scn trans", KR(tmp_ret),
            KR(ret), K(type), K(owner.length()));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObSharedMetaReservedMgr::remove_reserved_scn(
    common::ObMySQLTransaction &trans,
    const ObSharedMetaReservedType &type,
    const ObString &owner)
{
  int ret = OB_SUCCESS;
  ObSharedMetaReservedInfo owner_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (OB_UNLIKELY(!type.is_valid() || !type.is_owner_keyed() || owner.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("owner-keyed remove get invalid argument", KR(ret), K(type), K(owner.length()));
  } else if (OB_FAIL(owner_info.set_owner(owner))) {
    LOG_WARN("invalid owner", KR(ret), K(type), K(owner.length()));
  } else if (OB_FAIL(lock_table_in_exclusive_(trans))) {
    LOG_WARN("failed to lock shared meta reserved table before remove", KR(ret),
        K(type), K(owner.length()));
  } else if (OB_FAIL(remove_reserved_scn_(trans, type, owner_info.get_owner()))) {
    LOG_WARN("failed to remove owner-keyed reserved scn",
        KR(ret), K(type), K(owner.length()));
  }
  return ret;
}

int ObSharedMetaReservedMgr::remove_reserved_scn_(
    common::ObISQLClient &sql_client,
    const ObSharedMetaReservedType &type,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const ObSharedMetaReservedInfo info(tenant_id_, type, meta_scn);
  ObSharedMetaReservedOperator trans_op;
  if (OB_FAIL(trans_op.init(sql_client, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init trans operator", KR(ret));
  } else if (OB_FAIL(trans_op.delete_row(info, affected_rows))) {
    LOG_WARN("failed to delete reserved row", KR(ret), K(info));
  } else if (OB_UNLIKELY(1 != affected_rows && 0 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows on delete", KR(ret), K(info), K(affected_rows));
  }
  return ret;
}

int ObSharedMetaReservedMgr::remove_reserved_scn_(
    common::ObISQLClient &sql_client,
    const ObSharedMetaReservedType &type,
    const ObString &owner)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSharedMetaReservedOperator trans_op;
  if (OB_FAIL(trans_op.init(sql_client, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init trans operator", KR(ret));
  } else if (OB_FAIL(trans_op.delete_row(tenant_id_, type, owner, affected_rows))) {
    LOG_WARN("failed to delete owner-keyed reserved row",
        KR(ret), K_(tenant_id), K(type), K(owner.length()));
  } else if (OB_UNLIKELY(1 != affected_rows && 0 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows on owner delete",
        KR(ret), K_(tenant_id), K(type), K(owner.length()), K(affected_rows));
  }
  return ret;
}

int ObSharedMetaReservedMgr::get_max_reserved_scn(
    const bool need_lock,
    common::ObMySQLTransaction &trans,
    share::SCN &max_meta_scn)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSharedMetaReservedInfo, 1> infos;
  ObSharedMetaReservedOperator trans_op;
  const ObSharedMetaReservedType max_reserved_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  max_meta_scn = share::SCN::min_scn();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta reserved mgr not init", KR(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans is not started", KR(ret));
  } else if (need_lock && OB_FAIL(lock_table_in_share_(trans))) {
    LOG_WARN("failed to lock table in share mode", KR(ret));
  } else if (OB_FAIL(trans_op.init(trans, SHARED_META_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init trans operator", KR(ret));
  } else if (OB_FAIL(trans_op.scan_rows(tenant_id_, max_reserved_type, 0 /*limit_count*/, infos))) {
    LOG_WARN("failed to scan max reserved info", KR(ret), K_(tenant_id));
  } else if (0 == infos.count()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("max reserved info not exist", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(1 != infos.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected max reserved info count", KR(ret), K_(tenant_id), K(infos));
  } else {
    max_meta_scn = infos.at(0).meta_scn_;
  }
  return ret;
}
