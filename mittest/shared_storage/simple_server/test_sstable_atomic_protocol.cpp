// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/atomic_protocol/ob_lock_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;
using namespace storage;

class TestSSTableAtomicProtocol : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSTableAtomicProtocol() : unittest::ObSimpleClusterTestBase("test_sstable_atomic_protocol") {}

  // SSTable atomic protocol must be parameterised with an SSTable-typed
  // atomic-protocol type. MINI_SSTABLE is one of the simplest "tablet level"
  // sstable types registered in compile_utility/ob_atomic_protocol_register.h.
  static ObAtomicProtocolType make_mini_sstable_type()
  {
    ObAtomicProtocolType type;
    type.type_ = ObAtomicProtocolTypeEnum::MINI_SSTABLE;
    return type;
  }

  static ObAtomicProtocolType make_major_sstable_type()
  {
    ObAtomicProtocolType type;
    type.type_ = ObAtomicProtocolTypeEnum::MAJOR_SSTABLE;
    return type;
  }

  static ObAtomicProtocolType make_minor_sstable_type()
  {
    ObAtomicProtocolType type;
    type.type_ = ObAtomicProtocolTypeEnum::MINOR_SSTABLE;
    return type;
  }

  static ObAtomicProtocolType make_inc_major_compaction_sstable_type()
  {
    ObAtomicProtocolType type;
    type.type_ = ObAtomicProtocolTypeEnum::INC_MAJOR_COMPACTION_SSTABLE;
    return type;
  }

  // A non-default-constructed object info, used to verify that the value
  // is faithfully stored on the protocol object.
  static ObSSMetaCtrlObjectInfo make_object_info()
  {
    ObSSMetaCtrlObjectInfo info;
    EXPECT_EQ(OB_SUCCESS, info.init_by_none_major(
        share::SCN::base_scn(),
        1 /*parallel_cnt*/));
    return info;
  }

  static share::SCN make_tx_scn(const int64_t val)
  {
    share::SCN scn;
    EXPECT_EQ(OB_SUCCESS, scn.convert_for_tx(val));
    return scn;
  }

  static ObSSMetaCtrlObjectInfo make_major_object_info(const int64_t snapshot_version)
  {
    ObSSMetaCtrlObjectInfo info;
    EXPECT_EQ(OB_SUCCESS, info.init_by_major(
        make_tx_scn(snapshot_version), 10 /*seq_step*/, 1 /*parallel_cnt*/, 1 /*cg_cnt*/));
    return info;
  }

  static ObSSMetaCtrlObjectInfo make_inc_major_object_info(const int64_t snapshot_version)
  {
    ObSSMetaCtrlObjectInfo info;
    EXPECT_EQ(OB_SUCCESS, info.init_by_inc_major(
        make_tx_scn(snapshot_version), 10 /*seq_step*/, 1 /*parallel_cnt*/, 1 /*cg_cnt*/));
    return info;
  }

  static int build_meta_key_str(const ObSSMetaCtrlKey &ctrl_key,
                                common::ObIAllocator &allocator,
                                common::ObString &meta_key)
  {
    int ret = OB_SUCCESS;
    int64_t size = 0;
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_FAIL(ctrl_key.get_meta_key_serialize_size(size))) {
      LOG_WARN("failed to get meta key serialize size", K(ret), K(ctrl_key));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc meta key buf", K(ret), K(size));
    } else if (OB_FAIL(ctrl_key.serialize_meta_key(buf, size, pos))) {
      LOG_WARN("failed to serialize meta key", K(ret), K(ctrl_key));
    } else {
      meta_key.assign(buf, pos);
    }
    return ret;
  }

  static int build_meta_value_str(const ObSSMetaCtrlValue &ctrl_value,
                                  common::ObIAllocator &allocator,
                                  common::ObString &meta_value)
  {
    int ret = OB_SUCCESS;
    const int64_t size = ctrl_value.get_serialize_size();
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc meta value buf", K(ret), K(size));
    } else if (OB_FAIL(ctrl_value.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize meta value", K(ret), K(ctrl_value));
    } else {
      meta_value.assign(buf, size);
    }
    return ret;
  }

  int insert_sstable_ctrl_row(const ObLSID &ls,
                              const ObTabletID &tablet_id,
                              const share::SCN &reorg_scn,
                              const share::SCN &meta_scn,
                              ObAtomicProtocolType protocol_type,
                              const ObSSMetaCtrlObjectInfo &object_info,
                              const ObSSMetaCtrlStatus::STATUS status,
                              const int64_t op_id,
                              const int64_t lease_epoch)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlType meta_type;
    ObSSMetaCtrlKey ctrl_key;
    ObSSMetaCtrlValue ctrl_value;
    ObArenaAllocator allocator("LdMajorRow");
    ObString meta_key;
    ObString meta_value;
    ObSSMetaCtrlTableOperator table_operator;
    int64_t affected_rows = 0;

    if (OB_FAIL(protocol_type.get_ss_meta_ctrl_type(meta_type))) {
      LOG_WARN("failed to get ss meta ctrl type", K(ret), K(protocol_type));
    } else {
      ctrl_key.assign_type(meta_type);
      ctrl_key.sstable_key_.tablet_id_ = tablet_id;
      ctrl_key.sstable_key_.reorg_scn_ = reorg_scn;
      ctrl_key.sstable_key_.control_scn_ = meta_scn;
      ctrl_key.sstable_key_.ls_id_ = ls;
      ctrl_value.ctrl_info_.op_id_ = op_id;
      ctrl_value.ctrl_info_.lease_epoch_ = lease_epoch;
      if (ObSSMetaCtrlStatus::STATUS::START == status) {
        if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
          LOG_WARN("failed to set status start", K(ret), K(status), K(ctrl_value));
        }
      } else if (ObSSMetaCtrlStatus::STATUS::COMPLETE == status) {
        if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
          LOG_WARN("failed to set status start", K(ret), K(status), K(ctrl_value));
        } else if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_complete())) {
          LOG_WARN("failed to set status complete", K(ret), K(status), K(ctrl_value));
        }
      } else if (ObSSMetaCtrlStatus::STATUS::ABANDON == status) {
        if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
          LOG_WARN("failed to set status start", K(ret), K(status), K(ctrl_value));
        } else if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_abandon())) {
          LOG_WARN("failed to set status abandon", K(ret), K(status), K(ctrl_value));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unexpected test status", K(ret), K(status));
      }
      ctrl_value.object_info_ = object_info;
    }

    if (OB_FAIL(ret)) {
    } else if (!ctrl_key.is_valid() || !ctrl_value.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("test ctrl row is invalid", K(ret), K(ctrl_key), K(ctrl_value));
    } else if (OB_FAIL(build_meta_key_str(ctrl_key, allocator, meta_key))) {
      LOG_WARN("failed to build meta key str", K(ret), K(ctrl_key));
    } else if (OB_FAIL(build_meta_value_str(ctrl_value, allocator, meta_value))) {
      LOG_WARN("failed to build meta value str", K(ret), K(ctrl_value));
    } else if (OB_FAIL(table_operator.init(
        get_curr_simple_server().get_observer().get_mysql_proxy(),
        10L * 1000L * 1000L /*sql_timeout_us*/))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(table_operator.insert_row(kTenantId, ls, meta_type.type_, meta_key, meta_value, affected_rows))) {
      LOG_WARN("failed to insert test ctrl row", K(ret), K(ctrl_key), K(ctrl_value));
    }
    return ret;
  }

  // SSTable create()/finish() now lock the owning tablet's TABLET_LOCK_ROW first
  // (ObSSTableAtomicProtocol::lock_parent_lock_row_) and require that row to
  // already exist. Production guarantees it; in tests we must create it up-front.
  int ensure_tablet_lock_row(const ObLSID &ls,
                             const ObTabletID &tablet_id,
                             const share::SCN &reorg_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_lock_key;
    const ObSSMetaCtrlKey tablet_meta_key(
        blocksstable::ObStorageObjectType::SHARED_TABLET_META,
        tablet_id,
        reorg_scn,
        share::SCN::min_scn(),
        ls);
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("failed to get tablet lock row key", K(ret), K(tablet_meta_key));
    } else {
      ObLockAtomicProtocol lock_row;
      if (OB_FAIL(lock_row.init(kTenantId, ls, tablet_lock_key))) {
        LOG_WARN("failed to init tablet lock row", K(ret), K(ls), K(tablet_lock_key));
      } else if (OB_FAIL(lock_row.check_and_create_lock_row(
          get_curr_simple_server().get_observer().get_mysql_proxy()))) {
        LOG_WARN("failed to check and create tablet lock row", K(ret), K(ls), K(tablet_lock_key));
      }
    }
    return ret;
  }

  int read_major_lock_row(const ObLSID &ls,
                          const ObTabletID &tablet_id,
                          const share::SCN &reorg_scn,
                          ObSSMetaCtrlLockRowValue &lock_row_value)
  {
    int ret = OB_SUCCESS;
    ObSSTableAtomicProtocol protocol;
    ObLockAtomicProtocol lock_row;
    lock_row_value.reset();
    if (OB_FAIL(protocol.init_for_load(make_major_sstable_type(),
                                       kTenantId,
                                       ls,
                                       tablet_id,
                                       reorg_scn))) {
      LOG_WARN("failed to init major sstable protocol for load", K(ret), K(ls), K(tablet_id), K(reorg_scn));
    } else if (OB_FAIL(protocol.prepare_lock_row_(lock_row))) {
      LOG_WARN("failed to prepare major lock row", K(ret), K(ls), K(tablet_id), K(reorg_scn));
    } else if (OB_FAIL(lock_row.read_lock_row(
        get_curr_simple_server().get_observer().get_mysql_proxy(), lock_row_value))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read major lock row", K(ret), K(ls), K(tablet_id), K(reorg_scn));
      }
    }
    return ret;
  }

  // Mark the owning tablet's TABLET_LOCK_ROW as deleted, so that a subsequent
  // sstable create/finish on the same tablet is rejected with OB_STATE_NOT_MATCH
  // by lock_parent_lock_row_. The lock row must already exist.
  int mark_tablet_lock_row_deleted(const ObLSID &ls,
                                   const ObTabletID &tablet_id,
                                   const share::SCN &reorg_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_lock_key;
    const ObSSMetaCtrlKey tablet_meta_key(
        blocksstable::ObStorageObjectType::SHARED_TABLET_META,
        tablet_id,
        reorg_scn,
        share::SCN::min_scn(),
        ls);
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("failed to get tablet lock row key", K(ret), K(tablet_meta_key));
    } else {
      common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
      ObLockAtomicProtocol lock_row;
      ObMySQLTransaction trans;
      ObSSMetaCtrlLockRowValue lock_value;
      if (OB_FAIL(lock_row.init(kTenantId, ls, tablet_lock_key))) {
        LOG_WARN("failed to init tablet lock row", K(ret), K(ls), K(tablet_lock_key));
      } else if (OB_FAIL(lock_row.check_and_create_lock_row(sql_proxy))) {
        LOG_WARN("failed to check and create tablet lock row", K(ret), K(ls), K(tablet_lock_key));
      } else if (OB_FAIL(trans.start(&sql_proxy, kTenantId))) {
        LOG_WARN("failed to start trans", K(ret));
      } else {
        if (OB_FAIL(lock_row.lock_lock_row(trans, lock_value))) {
          LOG_WARN("failed to lock tablet lock row", K(ret), K(ls), K(tablet_lock_key));
        } else {
          lock_value.is_mark_deleted_ = true;
          lock_value.deleted_scn_ = share::SCN::min_scn();
          lock_value.last_major_snapshot_ = 0;
          if (OB_FAIL(lock_row.update_lock_row(lock_value, trans))) {
            LOG_WARN("failed to update tablet lock row", K(ret), K(lock_value));
          }
        }
        const bool commit = OB_SUCC(ret);
        int tmp_ret = trans.end(commit);
        if (OB_SUCC(ret) && OB_TMP_FAIL(tmp_ret)) {
          ret = tmp_ret;
        }
      }
    }
    return ret;
  }

  // common test parameters
  static constexpr uint64_t kTenantId = 1;
};

// ==================== init ====================

TEST_F(TestSSTableAtomicProtocol, init_invalid_argument)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  // invalid type (default ctor type_ == INVALID_TYPE)
  {
    ObSSTableAtomicProtocol p;
    ObAtomicProtocolType bad_type;
    int ret = p.init(bad_type, kTenantId, ls, tablet_id, reorg_scn, object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid tenant id
  {
    ObSSTableAtomicProtocol p;
    int ret = p.init(make_mini_sstable_type(), OB_INVALID_ID, ls, tablet_id, reorg_scn, object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid ls id
  {
    ObSSTableAtomicProtocol p;
    ObLSID invalid_ls;
    int ret = p.init(make_mini_sstable_type(), kTenantId, invalid_ls, tablet_id, reorg_scn, object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid tablet id
  {
    ObSSTableAtomicProtocol p;
    ObTabletID invalid_tablet;
    int ret = p.init(make_mini_sstable_type(), kTenantId, ls, invalid_tablet, reorg_scn, object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid reorg scn
  {
    ObSSTableAtomicProtocol p;
    share::SCN invalid_scn;
    int ret = p.init(make_mini_sstable_type(), kTenantId, ls, tablet_id, invalid_scn, object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // 原子协议类型必须与控制行的对象范围布局匹配。
  {
    ObSSTableAtomicProtocol p;
    int ret = p.init(
        make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  {
    ObSSTableAtomicProtocol p;
    const ObSSMetaCtrlObjectInfo major_object_info =
        make_major_object_info(100 /*snapshot_version*/);
    int ret = p.init(
        make_mini_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, major_object_info);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

TEST_F(TestSSTableAtomicProtocol, inner_tablet_key_uses_ls_id)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls(7);
  const ObTabletID tablet_id(ObTabletID::LS_TX_DATA_TABLET_ID);
  const share::SCN reorg_scn = make_tx_scn(42);
  ObSSTableAtomicProtocol protocol;

  ASSERT_EQ(OB_SUCCESS,
      protocol.init_for_load(
          make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn));

  ObSSMetaCtrlKey ctrl_key;
  ASSERT_EQ(OB_SUCCESS, protocol.prepare_ss_meta_ctrl_key_(ctrl_key));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_MAJOR_SSTABLE_CTRL,
      ctrl_key.type_);
  ASSERT_EQ(tablet_id, ctrl_key.sstable_key_.tablet_id_);
  ASSERT_EQ(reorg_scn, ctrl_key.sstable_key_.reorg_scn_);
  ASSERT_EQ(ls, ctrl_key.sstable_key_.ls_id_);

  ObArenaAllocator allocator("InnerSSTable");
  ObString key_prefix;
  ASSERT_EQ(OB_SUCCESS, protocol.build_query_prefix_(allocator, key_prefix));
  ASSERT_EQ(ObString::make_string("ls/7/TX_DATA/"), key_prefix);
}

TEST_F(TestSSTableAtomicProtocol, init_twice)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(make_mini_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_INIT_TWICE, p.init(make_mini_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info));
}

TEST_F(TestSSTableAtomicProtocol, not_init)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSTableAtomicProtocol p;
  ObEmptyAtomicProtocolOP op;
  ASSERT_EQ(OB_NOT_INIT, p.create(false /*need_check_sswriter*/));
  ASSERT_EQ(OB_NOT_INIT, p.finish(op));
  ASSERT_EQ(OB_NOT_INIT, p.abort());
}

// ==================== state-machine: create / finish / abort ====================

TEST_F(TestSSTableAtomicProtocol, create)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObSSTableAtomicProtocol sstable_atomic_protocol1;
  ret = sstable_atomic_protocol1.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTableAtomicProtocol sstable_atomic_protocol2;
  ret = sstable_atomic_protocol2.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol2.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create on the same instance again => state machine rejects it
  ret = sstable_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestSSTableAtomicProtocol, finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol sstable_atomic_protocol1;
  ret = sstable_atomic_protocol1.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol1.finish(op);
  ASSERT_EQ(OB_SUCCESS, ret);
  // finish twice
  ret = sstable_atomic_protocol1.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestSSTableAtomicProtocol, abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObSSTableAtomicProtocol sstable_atomic_protocol1;
  ret = sstable_atomic_protocol1.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol1.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
  // abort twice => state is ABANDON, not in {INIT, START, COMPLETE} => OB_STATE_NOT_MATCH
  ret = sstable_atomic_protocol1.abort();
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);

  // INIT -> abort directly is also allowed (status INIT belongs to {INIT, START, COMPLETE})
  ObSSTableAtomicProtocol sstable_atomic_protocol2;
  ret = sstable_atomic_protocol2.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable_atomic_protocol2.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// finish then abort: status is COMPLETE which IS allowed by check_can_do_abort_
// (it accepts {INIT, START, COMPLETE}). SSTable finish() updates the ctrl row
// status to COMPLETE rather than deleting it (need_delete_row=false), so
// abort_() then reads it back and flips status to ABANDON. Result: abort succeeds.
TEST_F(TestSSTableAtomicProtocol, abort_after_finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(2);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.finish(op));
  ret = p.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// abort then finish: status is ABORT, not START => OB_STATE_NOT_MATCH
TEST_F(TestSSTableAtomicProtocol, finish_after_abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(3);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.abort());
  ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// finish without preceding create: status is INIT, not START => OB_STATE_NOT_MATCH
TEST_F(TestSSTableAtomicProtocol, finish_without_create)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(4);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info));
  int ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// Two independent tablets running create -> finish in sequence on the same LS
// should both succeed (lock row is per-tablet).
TEST_F(TestSSTableAtomicProtocol, multi_tablet_create_finish)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, ObTabletID(11), reorg_scn));
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, ObTabletID(12), reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p_a;
  ObSSTableAtomicProtocol p_b;
  ASSERT_EQ(OB_SUCCESS, p_a.init(type, kTenantId, ls, ObTabletID(11), reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, p_b.init(type, kTenantId, ls, ObTabletID(12), reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, p_a.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_b.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_a.finish(op));
  ASSERT_EQ(OB_SUCCESS, p_b.finish(op));
}

// Different SSTable families (MINI_SSTABLE vs MAJOR_SSTABLE) over the same
// (ls, tablet, reorg_scn) should not collide. MINI uses the unified
// MINOR_SSTABLE ctrl type and MINOR_LOCK_ROW, while MAJOR maps to its own
// MAJOR_SSTABLE ctrl type and MAJOR_LOCK_ROW. The sequence
// MINI.create -> MAJOR.create -> MINI.finish -> MAJOR.finish must succeed.
TEST_F(TestSSTableAtomicProtocol, different_types_coexist)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(20);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();
  const ObSSMetaCtrlObjectInfo major_object_info =
      make_major_object_info(100 /*snapshot_version*/);

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p_mini;
  ObSSTableAtomicProtocol p_major;
  ASSERT_EQ(OB_SUCCESS,
      p_mini.init(make_mini_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS,
      p_major.init(
          make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, major_object_info));
  ASSERT_EQ(OB_SUCCESS, p_mini.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_major.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_mini.finish(op));
  ASSERT_EQ(OB_SUCCESS, p_major.finish(op));
}

// Two SSTable sub-types unified as MINOR_SSTABLE over the same
// (ls, tablet, reorg_scn) must still coexist. They serialize on the same
// MINOR_LOCK_ROW. Because each
// create()/finish() commits its own transaction, the row lock is released
// between calls and the sequence MINI.create -> MINOR.create ->
// MINI.finish -> MINOR.finish must succeed; control_scn distinguishes the
// resulting ctrl rows.
TEST_F(TestSSTableAtomicProtocol, same_lock_row_subtypes_coexist)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(21);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p_mini;
  ObSSTableAtomicProtocol p_minor;
  ASSERT_EQ(OB_SUCCESS,
      p_mini.init(make_mini_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS,
      p_minor.init(make_minor_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, p_mini.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_minor.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_mini.finish(op));
  ASSERT_EQ(OB_SUCCESS, p_minor.finish(op));
}

// 验证 inc-major compaction 使用独立的控制行、锁行和 op_id 序列。
TEST_F(TestSSTableAtomicProtocol, inc_major_compaction_uses_independent_op_id_sequence)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls(1);
  const ObTabletID tablet_id(23);
  const share::SCN reorg_scn = share::SCN::min_scn();
  ObAtomicProtocolType inc_major_type = make_inc_major_compaction_sstable_type();
  ObSSMetaCtrlType inc_major_ctrl_type;
  ObSSMetaCtrlType inc_major_lock_type;

  ASSERT_EQ(OB_SUCCESS, inc_major_type.get_ss_meta_ctrl_type(inc_major_ctrl_type));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_SSTABLE_CTRL,
      inc_major_ctrl_type.type_);
  ASSERT_EQ(OB_SUCCESS, inc_major_ctrl_type.get_lock_type(inc_major_lock_type));
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_INC_MAJOR_LOCK, inc_major_lock_type.type_);

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObSSTableAtomicProtocol first_protocol;
  ObSSTableAtomicProtocol second_protocol;
  ASSERT_EQ(OB_SUCCESS,
      first_protocol.init(
          make_minor_sstable_type(), kTenantId, ls, tablet_id, reorg_scn,
          make_object_info()));
  ASSERT_EQ(OB_SUCCESS, first_protocol.create(false /*不检查 ss writer*/));
  ASSERT_EQ(OB_SUCCESS,
      second_protocol.init(
          inc_major_type, kTenantId, ls, tablet_id, reorg_scn,
          make_inc_major_object_info(200 /*snapshot_version*/)));
  ASSERT_EQ(OB_SUCCESS, second_protocol.create(false /*不检查 ss writer*/));

  int64_t first_op_id = -1;
  int64_t second_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, first_protocol.get_op_id(first_op_id));
  ASSERT_EQ(OB_SUCCESS, second_protocol.get_op_id(second_op_id));
  ASSERT_EQ(first_op_id, second_op_id);

  ASSERT_EQ(OB_SUCCESS, first_protocol.abort());
  ASSERT_EQ(OB_SUCCESS, second_protocol.abort());
}

// 验证同一 tablet 的两代 Inc-Major 使用不同 op_id，各代 sequence 可独立从 0 开始。
TEST_F(TestSSTableAtomicProtocol, inc_major_compaction_generations_use_new_op_id)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls(1);
  const ObTabletID tablet_id(24);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const ObAtomicProtocolType type = make_inc_major_compaction_sstable_type();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObSSTableAtomicProtocol first_protocol;
  ASSERT_EQ(OB_SUCCESS,
      first_protocol.init(
          type, kTenantId, ls, tablet_id, reorg_scn,
          make_inc_major_object_info(200 /*snapshot_version*/)));
  ASSERT_EQ(OB_SUCCESS, first_protocol.create(false /*不检查 ss writer*/));
  int64_t first_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, first_protocol.get_op_id(first_op_id));
  ASSERT_EQ(OB_SUCCESS, first_protocol.abort());

  ObSSTableAtomicProtocol second_protocol;
  ASSERT_EQ(OB_SUCCESS,
      second_protocol.init(
          type, kTenantId, ls, tablet_id, reorg_scn,
          make_inc_major_object_info(201 /*snapshot_version*/)));
  ASSERT_EQ(OB_SUCCESS, second_protocol.create(false /*不检查 ss writer*/));
  int64_t second_op_id = -1;
  ASSERT_EQ(OB_SUCCESS, second_protocol.get_op_id(second_op_id));
  ASSERT_GT(second_op_id, first_op_id);

  const int64_t first_sequence = 0;
  const int64_t second_sequence = 0;
  ASSERT_EQ(first_sequence, second_sequence);
  ASSERT_EQ(OB_SUCCESS, second_protocol.abort());
}

// MINOR_SSTABLE/MINOR_LOCK_ROW and MAJOR_SSTABLE/MAJOR_LOCK_ROW over the same
// (ls, tablet, reorg_scn) must not collide: they map to disjoint lock rows
// and ctrl types.
TEST_F(TestSSTableAtomicProtocol, minor_and_major_coexist)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(22);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();
  const ObSSMetaCtrlObjectInfo major_object_info =
      make_major_object_info(100 /*snapshot_version*/);

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p_minor;
  ObSSTableAtomicProtocol p_major;
  ASSERT_EQ(OB_SUCCESS,
      p_minor.init(make_minor_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS,
      p_major.init(
          make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn, major_object_info));
  ASSERT_EQ(OB_SUCCESS, p_minor.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_major.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_minor.finish(op));
  ASSERT_EQ(OB_SUCCESS, p_major.finish(op));
}

// When the owning tablet has been mark-deleted, sstable create() must be
// rejected: lock_parent_lock_row_ locks the tablet lock row, sees
// is_mark_deleted_, and returns OB_STATE_NOT_MATCH.
TEST_F(TestSSTableAtomicProtocol, create_rejected_when_tablet_mark_deleted)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(30);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, mark_tablet_lock_row_deleted(ls, tablet_id, reorg_scn));

  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_STATE_NOT_MATCH, p.create(need_check_sswrite));
}

// When the owning tablet is mark-deleted after the sstable was created,
// finish() must also be rejected by lock_parent_lock_row_.
TEST_F(TestSSTableAtomicProtocol, finish_rejected_when_tablet_mark_deleted)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObAtomicProtocolType type = make_mini_sstable_type();
  const ObTabletID tablet_id = ObTabletID(31);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;
  const ObSSMetaCtrlObjectInfo object_info = make_object_info();

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObEmptyAtomicProtocolOP op;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(type, kTenantId, ls, tablet_id, reorg_scn, object_info));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));

  // Tablet gets deleted between create and finish.
  ASSERT_EQ(OB_SUCCESS, mark_tablet_lock_row_deleted(ls, tablet_id, reorg_scn));

  ASSERT_EQ(OB_STATE_NOT_MATCH, p.finish(op));
}

// ==================== load_if_exist ====================

TEST_F(TestSSTableAtomicProtocol, load_if_exist_invalid_argument)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  bool need_create = false;
  ObSSTableAtomicProtocol not_inited;
  ASSERT_EQ(OB_NOT_INIT, not_inited.load_if_exist(100 /*snapshot_version*/, need_create));

  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init_for_load(make_major_sstable_type(),
                                        kTenantId,
                                        ObLSID(60),
                                        ObTabletID(600),
                                        share::SCN::min_scn()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, p.load_if_exist(-1 /*snapshot_version*/, need_create));

  ObSSTableAtomicProtocol mini;
  ASSERT_EQ(OB_SUCCESS, mini.init(make_mini_sstable_type(),
                                  kTenantId,
                                  ObLSID(60),
                                  ObTabletID(601),
                                  share::SCN::min_scn(),
                                  make_object_info()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mini.load_if_exist(100 /*snapshot_version*/, need_create));
}

TEST_F(TestSSTableAtomicProtocol, load_if_exist_no_matching_row_need_create)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(61);
  const ObTabletID tablet_id = ObTabletID(610);
  const share::SCN reorg_scn = share::SCN::min_scn();
  bool need_create = false;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init_for_load(make_major_sstable_type(),
                                        kTenantId,
                                        ls,
                                        tablet_id,
                                        reorg_scn));
  ASSERT_EQ(OB_SUCCESS, p.load_if_exist(100 /*snapshot_version*/, need_create));
  ASSERT_TRUE(need_create);
  ASSERT_EQ(ObAtomicProtocol::STATUS::INIT, p.status_);

  ObSSMetaCtrlLockRowValue lock_row_value;
  ASSERT_EQ(OB_ITER_END, read_major_lock_row(ls, tablet_id, reorg_scn, lock_row_value));
}

TEST_F(TestSSTableAtomicProtocol, load_if_exist_loads_latest_meta_scn_for_same_snapshot)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(62);
  const ObTabletID tablet_id = ObTabletID(620);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const share::SCN old_meta_scn = make_tx_scn(1000);
  const share::SCN latest_meta_scn = make_tx_scn(2000);
  const int64_t snapshot_version = 500;

  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, old_meta_scn,
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::START,
                                                101 /*op_id*/,
                                                201 /*lease_epoch*/));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, latest_meta_scn,
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                102 /*op_id*/,
                                                202 /*lease_epoch*/));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, make_tx_scn(5000),
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version - 1),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                105 /*op_id*/,
                                                205 /*lease_epoch*/));
  // Distractor rows must not be selected even when they are newer or have the
  // same snapshot; load_if_exist filters by tablet/reorg/type/snapshot.
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, ObTabletID(621), reorg_scn, make_tx_scn(3000),
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                103 /*op_id*/,
                                                203 /*lease_epoch*/));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, make_tx_scn(4000),
                                                make_minor_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                104 /*op_id*/,
                                                204 /*lease_epoch*/));

  bool need_create = true;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init_for_load(make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, p.load_if_exist(snapshot_version, need_create));
  ASSERT_FALSE(need_create);
  ASSERT_TRUE(p.is_status_complete());
  share::SCN loaded_control_scn;
  ASSERT_EQ(OB_SUCCESS, p.get_control_scn_(loaded_control_scn));
  ASSERT_EQ(latest_meta_scn, loaded_control_scn);
  ASSERT_EQ(102, p.op_id_);
  ASSERT_EQ(202, p.lease_epoch_);
  ASSERT_TRUE(p.object_info_.is_major());
  ASSERT_EQ(snapshot_version, p.object_info_.snapshot_version_.get_val_for_tx());

  ObSSMetaCtrlLockRowValue lock_row_value;
  ASSERT_EQ(OB_ITER_END, read_major_lock_row(ls, tablet_id, reorg_scn, lock_row_value));
}

TEST_F(TestSSTableAtomicProtocol, load_if_exist_ignores_abandoned_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(63);
  const ObTabletID tablet_id = ObTabletID(630);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const share::SCN meta_scn = make_tx_scn(3000);
  const int64_t snapshot_version = 600;

  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, meta_scn,
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::ABANDON,
                                                301 /*op_id*/,
                                                401 /*lease_epoch*/));

  bool need_create = false;
  ObSSTableAtomicProtocol protocol;
  ASSERT_EQ(OB_SUCCESS, protocol.init_for_load(make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, protocol.load_if_exist(snapshot_version, need_create));
  ASSERT_TRUE(need_create);
  ASSERT_EQ(ObAtomicProtocol::STATUS::INIT, protocol.status_);
  share::SCN loaded_control_scn;
  ASSERT_EQ(OB_STATE_NOT_MATCH, protocol.get_control_scn_(loaded_control_scn));
}

TEST_F(TestSSTableAtomicProtocol, load_if_exist_newer_snapshot_expires_current_request)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(64);
  const ObTabletID tablet_id = ObTabletID(640);
  const share::SCN reorg_scn = share::SCN::min_scn();

  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, make_tx_scn(4000),
                                                make_major_sstable_type(),
                                                make_major_object_info(900 /*snapshot_version*/),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                501 /*op_id*/,
                                                601 /*lease_epoch*/));

  bool need_create = true;
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init_for_load(make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, p.load_if_exist(800 /*snapshot_version*/, need_create));
  ASSERT_FALSE(need_create);
  ASSERT_EQ(ObAtomicProtocol::STATUS::INIT, p.status_);
}

TEST_F(TestSSTableAtomicProtocol, create_major_rechecks_existing_row_under_lock)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(65);
  const ObTabletID tablet_id = ObTabletID(650);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const share::SCN existing_meta_scn = make_tx_scn(5000);
  const int64_t snapshot_version = 1000;
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, existing_meta_scn,
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                701 /*op_id*/,
                                                801 /*lease_epoch*/));

  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(make_major_sstable_type(),
                               kTenantId,
                               ls,
                               tablet_id,
                               reorg_scn,
                               make_major_object_info(snapshot_version)));
  ASSERT_EQ(OB_SUCCESS, p.create(false /*need_check_sswriter*/));
  ASSERT_TRUE(p.is_status_complete());
  // meta_scn_ 只在 finish() 分配，adopt 已有行时恢复的是 key 里的 control_scn
  share::SCN adopted_control_scn;
  ASSERT_EQ(OB_SUCCESS, p.get_control_scn_(adopted_control_scn));
  ASSERT_EQ(existing_meta_scn, adopted_control_scn);
  ASSERT_EQ(701, p.op_id_);
  ASSERT_EQ(801, p.lease_epoch_);
  ASSERT_TRUE(p.object_info_.is_major());
}

TEST_F(TestSSTableAtomicProtocol, create_major_expired_by_newer_snapshot_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(66);
  const ObTabletID tablet_id = ObTabletID(660);
  const share::SCN reorg_scn = share::SCN::min_scn();
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, make_tx_scn(6000),
                                                make_major_sstable_type(),
                                                make_major_object_info(2000 /*snapshot_version*/),
                                                ObSSMetaCtrlStatus::STATUS::COMPLETE,
                                                901 /*op_id*/,
                                                1001 /*lease_epoch*/));

  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(make_major_sstable_type(),
                               kTenantId,
                               ls,
                               tablet_id,
                               reorg_scn,
                               make_major_object_info(1000 /*snapshot_version*/)));
  ASSERT_EQ(OB_TASK_EXPIRED, p.create(false /*need_check_sswriter*/));
  ASSERT_EQ(ObAtomicProtocol::STATUS::INIT, p.status_);
}

TEST_F(TestSSTableAtomicProtocol, create_major_adopts_existing_start_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(67);
  const ObTabletID tablet_id = ObTabletID(670);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const share::SCN existing_meta_scn = make_tx_scn(7000);
  const int64_t snapshot_version = 1000;
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, existing_meta_scn,
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::START,
                                                702 /*op_id*/,
                                                802 /*lease_epoch*/));

  // object info 与已有行一致：adopt 路径不应打 WARN，也不应改写该行。
  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(make_major_sstable_type(),
                               kTenantId,
                               ls,
                               tablet_id,
                               reorg_scn,
                               make_major_object_info(snapshot_version)));
  ASSERT_EQ(OB_SUCCESS, p.create(false /*need_check_sswriter*/));
  ASSERT_TRUE(p.is_status_start());
  // meta_scn_ 只在 finish() 分配，adopt 已有行时恢复的是 key 里的 control_scn
  share::SCN adopted_control_scn;
  ASSERT_EQ(OB_SUCCESS, p.get_control_scn_(adopted_control_scn));
  ASSERT_EQ(existing_meta_scn, adopted_control_scn);
  ASSERT_EQ(702, p.op_id_);
  ASSERT_EQ(802, p.lease_epoch_);
}

TEST_F(TestSSTableAtomicProtocol, create_major_recreates_after_existing_abandon_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  const ObLSID ls = ObLSID(68);
  const ObTabletID tablet_id = ObTabletID(680);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const share::SCN abandoned_control_scn = make_tx_scn(8000);
  const int64_t snapshot_version = 1000;
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, insert_sstable_ctrl_row(ls, tablet_id, reorg_scn, abandoned_control_scn,
                                                make_major_sstable_type(),
                                                make_major_object_info(snapshot_version),
                                                ObSSMetaCtrlStatus::STATUS::ABANDON,
                                                703 /*op_id*/,
                                                803 /*lease_epoch*/));

  ObSSTableAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(make_major_sstable_type(),
                               kTenantId,
                               ls,
                               tablet_id,
                               reorg_scn,
                               make_major_object_info(snapshot_version)));
  ASSERT_EQ(OB_SUCCESS, p.create(false /*need_check_sswriter*/));
  ASSERT_TRUE(p.is_status_start());
  share::SCN recreated_control_scn;
  ASSERT_EQ(OB_SUCCESS, p.get_control_scn_(recreated_control_scn));
  ASSERT_NE(abandoned_control_scn, recreated_control_scn);

  // The abandoned row remains a GC candidate, while load_if_exist must find
  // the newly-created START row for the same snapshot.
  bool need_create = true;
  ObSSTableAtomicProtocol load_recreated;
  ASSERT_EQ(OB_SUCCESS, load_recreated.init_for_load(
      make_major_sstable_type(), kTenantId, ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, load_recreated.load_if_exist(snapshot_version, need_create));
  ASSERT_FALSE(need_create);
  ASSERT_TRUE(load_recreated.is_status_start());
  share::SCN loaded_control_scn;
  ASSERT_EQ(OB_SUCCESS, load_recreated.get_control_scn_(loaded_control_scn));
  ASSERT_EQ(recreated_control_scn, loaded_control_scn);
}


} // share
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
