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
#include "storage/incremental/atomic_protocol/ob_tablet_atomic_protocol.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/atomic_protocol/ob_lock_atomic_protocol.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_cluster_version.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "lib/guard/ob_scope_guard.h"
#include "logservice/ob_log_handler.h"
#include "logservice/palf/palf_handle_impl.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

struct PalfRoleResult
{
  oceanbase::common::ObRole role_ = oceanbase::common::LEADER;
  int64_t proposal_id_ = 41;
  int ret_ = oceanbase::common::OB_SUCCESS;
  bool is_pending_state_ = false;
  const oceanbase::ipalf::IPalfHandle *queried_handle_ = nullptr;
  int64_t call_count_ = 0;
};

// Mock only this thread's PALF state; keep the real writer qualification checks and atomic protocol.
static thread_local PalfRoleResult *mock_palf_role = nullptr;

namespace oceanbase
{
const char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace palf
{
int PalfHandle::get_role(common::ObRole &role,
                        int64_t &proposal_id,
                        bool &is_pending_state) const
{
  int ret = common::OB_SUCCESS;
  if (nullptr != mock_palf_role) {
    mock_palf_role->queried_handle_ = this;
    ++mock_palf_role->call_count_;
    role = mock_palf_role->role_;
    proposal_id = mock_palf_role->proposal_id_;
    is_pending_state = mock_palf_role->is_pending_state_;
    ret = mock_palf_role->ret_;
  } else if (nullptr == palf_handle_impl_) {
    ret = common::OB_NOT_INIT;
  } else {
    ret = palf_handle_impl_->get_role(role, proposal_id, is_pending_state);
  }
  return ret;
}
}
namespace logservice
{
int ObLogHandler::get_role(common::ObRole &role, int64_t &proposal_id) const
{
  // Internal SQL runs in this thread, so regular role checks must read the real PALF state.
  PalfRoleResult *saved_mock = mock_palf_role;
  mock_palf_role = nullptr;
  const int ret = ObLogHandlerBase::get_role(role, proposal_id);
  mock_palf_role = saved_mock;
  return ret;
}
}
}

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

// Test-side concrete ObIAtomicProtocolOP. The base class declares
// process() as pure virtual, so test cases that only need a no-op hook
// must provide their own subclass.
class TestNoopProtocolOP final : public ObIAtomicProtocolOP
{
public:
  TestNoopProtocolOP() {}
  virtual ~TestNoopProtocolOP() {}
  virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override
  {
    UNUSED(trans);
    UNUSED(meta_scn);
    return OB_SUCCESS;
  }
};

class TestFailProtocolOP final : public ObIAtomicProtocolOP
{
public:
  explicit TestFailProtocolOP(const int ret) : ret_(ret) {}
  virtual ~TestFailProtocolOP() {}
  virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override
  {
    UNUSED(trans);
    UNUSED(meta_scn);
    return ret_;
  }
private:
  int ret_;
};

class TestTabletAtomicProtocol : public unittest::ObSimpleClusterTestBase
{
public:
  TestTabletAtomicProtocol() : unittest::ObSimpleClusterTestBase("test_tablet_atomic_protocol") {}

  // Build a default valid ObTabletAtomicProtocolParam. Tests tweak individual
  // fields on the returned param to exercise is_valid() boundaries.
  static void build_param_with_tenant(
      ObTabletAtomicProtocolParam &param,
      const uint64_t tenant_id,
      const ObLSID &ls,
      const ObTabletID &tablet_id,
      const share::SCN &reorg_scn)
  {
    param.reset();
    param.tenant_id_ = tenant_id;
    param.ls_id_ = ls;
    param.tablet_id_ = tablet_id;
    param.reorg_scn_ = reorg_scn;
    param.is_deleted_ = false;
    param.deleted_scn_ = share::SCN::min_scn();
    param.last_major_snapshot_ = 0;
  }

  static void build_param(
      ObTabletAtomicProtocolParam &param,
      const ObLSID &ls,
      const ObTabletID &tablet_id,
      const share::SCN &reorg_scn)
  {
    build_param_with_tenant(param, kTenantId, ls, tablet_id, reorg_scn);
  }

  static ObSSMetaCtrlObjectInfo make_object_info()
  {
    ObSSMetaCtrlObjectInfo info;
    EXPECT_EQ(OB_SUCCESS, info.init_by_none_major(
        share::SCN::base_scn(),
        1 /*parallel_cnt*/));
    return info;
  }

  static ObAtomicProtocolType make_mini_sstable_type()
  {
    ObAtomicProtocolType type;
    type.type_ = ObAtomicProtocolTypeEnum::MINI_SSTABLE;
    return type;
  }

  // ObSSTableAtomicProtocol::create() locks the owning tablet's TABLET_LOCK_ROW
  // first and requires that row to already exist. Create it up-front so a tablet
  // test can stage sstable ctrl rows before driving the tablet protocol.
  int ensure_tablet_lock_row(const ObLSID &ls,
                             const ObTabletID &tablet_id,
                             const share::SCN &reorg_scn)
  {
    return ensure_tablet_lock_row_with_tenant(kTenantId, ls, tablet_id, reorg_scn);
  }

  int ensure_tablet_lock_row_with_tenant(const uint64_t tenant_id,
                                         const ObLSID &ls,
                                         const ObTabletID &tablet_id,
                                         const share::SCN &reorg_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_meta_key;
    ObSSMetaCtrlKey tablet_lock_key;
    tablet_meta_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
    tablet_meta_key.tablet_key_.tablet_id_ = tablet_id;
    tablet_meta_key.tablet_key_.reorg_scn_ = reorg_scn;
    tablet_meta_key.tablet_key_.control_scn_ = share::SCN::min_scn();
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("failed to get tablet lock row key", K(ret), K(tablet_meta_key));
    } else {
      ObLockAtomicProtocol lock_row;
      if (OB_FAIL(lock_row.init(tenant_id, ls, tablet_lock_key))) {
        LOG_WARN("failed to init tablet lock row", K(ret), K(ls), K(tablet_lock_key));
      } else if (OB_FAIL(lock_row.check_and_create_lock_row(
          get_curr_simple_server().get_observer().get_mysql_proxy()))) {
        LOG_WARN("failed to check and create tablet lock row", K(ret), K(ls), K(tablet_lock_key));
      }
    }
    return ret;
  }

  // Create a MINI_SSTABLE ctrl row belonging to (ls, tablet_id, reorg_scn) and
  // return its ctrl-key info, so tablet-level tests can populate key_infos_ with
  // a real sstable row and observe how finish()/abort() resolve it. The sstable
  // protocol is left in START state (created but not finished).
  int create_sstable_ctrl_row(const ObLSID &ls,
                              const ObTabletID &tablet_id,
                              const share::SCN &reorg_scn,
                              ObSSMetaCtrlKeyInfo &key_info)
  {
    int ret = OB_SUCCESS;
    ObSSTableAtomicProtocol sstable_protocol;
    if (OB_FAIL(sstable_protocol.init(make_mini_sstable_type(), kTenantId, ls,
                                      tablet_id, reorg_scn, make_object_info()))) {
      LOG_WARN("failed to init sstable protocol", K(ret), K(ls), K(tablet_id), K(reorg_scn));
    } else if (OB_FAIL(sstable_protocol.create(false /*need_check_sswriter*/))) {
      LOG_WARN("failed to create sstable ctrl row", K(ret), K(ls), K(tablet_id), K(reorg_scn));
    } else if (OB_FAIL(sstable_protocol.get_ctrl_key_info(key_info))) {
      LOG_WARN("failed to get sstable ctrl key info", K(ret), K(ls), K(tablet_id), K(reorg_scn));
    }
    return ret;
  }

  // Read back the ctrl-row status for the sstable ctrl row referenced by
  // key_info. Returns OB_ITER_END (via select_ctrl_row_) if the row is absent.
  int get_ctrl_row_status(const ObSSMetaCtrlKeyInfo &key_info,
                          ObSSMetaCtrlStatus::STATUS &status)
  {
    int ret = OB_SUCCESS;
    // ObAtomicProtocol is abstract; use a concrete subclass purely as an accessor
    // for the protected select_ctrl_row_ (exposed here via `#define protected public`).
    ObTabletAtomicProtocol probe;
    ObSSMetaCtrlValue ctrl_value;
    common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    if (OB_FAIL(probe.select_ctrl_row_(kTenantId, key_info.ls_id_, key_info.key_,
                                       sql_proxy, ctrl_value))) {
      LOG_WARN("failed to select ctrl row", K(ret), K(key_info));
    } else {
      status = ctrl_value.ctrl_info_.status_.status_;
    }
    return ret;
  }

  int set_ctrl_row_status(const ObSSMetaCtrlKeyInfo &key_info,
                          const ObSSMetaCtrlStatus::STATUS target_status)
  {
    int ret = OB_SUCCESS;
    ObTabletAtomicProtocol probe;
    ObSSMetaCtrlValue ctrl_value;
    common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    if (OB_FAIL(probe.select_ctrl_row_(kTenantId, key_info.ls_id_, key_info.key_,
                                       sql_proxy, ctrl_value))) {
      LOG_WARN("failed to select ctrl row", K(ret), K(key_info));
    } else if (ObSSMetaCtrlStatus::STATUS::START == target_status) {
      if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
        LOG_WARN("failed to set status start", K(ret), K(key_info), K(ctrl_value));
      }
    } else if (ObSSMetaCtrlStatus::STATUS::COMPLETE == target_status) {
      if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_complete())) {
        LOG_WARN("failed to set status complete", K(ret), K(key_info), K(ctrl_value));
      }
    } else if (ObSSMetaCtrlStatus::STATUS::ABANDON == target_status) {
      if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_abandon())) {
        LOG_WARN("failed to set status abandon", K(ret), K(key_info), K(ctrl_value));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported target status", K(ret), K(target_status), K(key_info));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(probe.update_ctrl_row_(kTenantId, key_info.ls_id_, key_info.key_,
                                         ctrl_value, sql_proxy))) {
        LOG_WARN("failed to update ctrl row status", K(ret), K(key_info), K(ctrl_value));
      }
    }
    return ret;
  }

  int remove_ctrl_row(const ObSSMetaCtrlKeyInfo &key_info)
  {
    int ret = OB_SUCCESS;
    ObTabletAtomicProtocol probe;
    common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    if (OB_FAIL(probe.delete_ctrl_row_(kTenantId, key_info.ls_id_, key_info.key_, sql_proxy))) {
      LOG_WARN("failed to delete ctrl row", K(ret), K(key_info));
    }
    return ret;
  }

  int read_tablet_lock_row(const ObLSID &ls,
                           const ObTabletID &tablet_id,
                           const share::SCN &reorg_scn,
                           ObSSMetaCtrlLockRowValue &lock_row_value)
  {
    return read_tablet_lock_row_with_tenant(kTenantId, ls, tablet_id, reorg_scn, lock_row_value);
  }

  int read_tablet_lock_row_with_tenant(const uint64_t tenant_id,
                                       const ObLSID &ls,
                                       const ObTabletID &tablet_id,
                                       const share::SCN &reorg_scn,
                                       ObSSMetaCtrlLockRowValue &lock_row_value)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_meta_key;
    ObSSMetaCtrlKey tablet_lock_key;
    tablet_meta_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
    tablet_meta_key.tablet_key_.tablet_id_ = tablet_id;
    tablet_meta_key.tablet_key_.reorg_scn_ = reorg_scn;
    tablet_meta_key.tablet_key_.control_scn_ = share::SCN::min_scn();
    ObLockAtomicProtocol lock_row;
    common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("failed to get tablet lock key", K(ret), K(tablet_meta_key));
    } else if (OB_FAIL(lock_row.init(tenant_id, ls, tablet_lock_key))) {
      LOG_WARN("failed to init lock row protocol", K(ret), K(ls), K(tablet_lock_key));
    } else if (OB_FAIL(lock_row.read_lock_row(sql_proxy, lock_row_value))) {
      LOG_WARN("failed to read tablet lock row", K(ret), K(ls), K(tablet_lock_key));
    }
    return ret;
  }

  int update_tablet_lock_row(const ObLSID &ls,
                             const ObTabletID &tablet_id,
                             const share::SCN &reorg_scn,
                             const ObSSMetaCtrlLockRowValue &new_value)
  {
    return update_tablet_lock_row_with_tenant(kTenantId, ls, tablet_id, reorg_scn, new_value);
  }

  int update_tablet_lock_row_with_tenant(const uint64_t tenant_id,
                                         const ObLSID &ls,
                                         const ObTabletID &tablet_id,
                                         const share::SCN &reorg_scn,
                                         const ObSSMetaCtrlLockRowValue &new_value)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_meta_key;
    ObSSMetaCtrlKey tablet_lock_key;
    tablet_meta_key.type_ = blocksstable::ObStorageObjectType::SHARED_TABLET_META;
    tablet_meta_key.tablet_key_.tablet_id_ = tablet_id;
    tablet_meta_key.tablet_key_.reorg_scn_ = reorg_scn;
    tablet_meta_key.tablet_key_.control_scn_ = share::SCN::min_scn();
    ObLockAtomicProtocol lock_row;
    common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    if (OB_FAIL(tablet_meta_key.get_lock_row_key(tablet_lock_key))) {
      LOG_WARN("failed to get tablet lock key", K(ret), K(tablet_meta_key));
    } else if (OB_FAIL(lock_row.init(tenant_id, ls, tablet_lock_key))) {
      LOG_WARN("failed to init lock row protocol", K(ret), K(ls), K(tablet_lock_key));
    } else if (OB_FAIL(lock_row.update_lock_row(new_value, sql_proxy))) {
      LOG_WARN("failed to update tablet lock row", K(ret), K(ls), K(tablet_lock_key), K(new_value));
    }
    return ret;
  }

  int get_tablet_ctrl_row_status(ObTabletAtomicProtocol &protocol,
                                 ObSSMetaCtrlStatus::STATUS &status)
  {
    int ret = OB_SUCCESS;
    ObSSMetaCtrlKey tablet_ctrl_key;
    ObSSMetaCtrlValue ctrl_value;
    common::ObISQLClient &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    if (OB_FAIL(protocol.prepare_ss_meta_ctrl_key_(tablet_ctrl_key))) {
      LOG_WARN("failed to prepare tablet ctrl key", K(ret), K(protocol));
    } else if (OB_FAIL(protocol.select_ctrl_row_(kTenantId, protocol.ls_id_, tablet_ctrl_key,
                                                 sql_proxy, ctrl_value))) {
      LOG_WARN("failed to select tablet ctrl row", K(ret), K(tablet_ctrl_key), K(protocol));
    } else {
      status = ctrl_value.ctrl_info_.status_.status_;
    }
    return ret;
  }

  // common test parameters
  static constexpr uint64_t kTenantId = 1;
};

TEST_F(TestTabletAtomicProtocol, palf_role_does_not_wait_for_log_handler)
{
  class TestLogHandler final : public logservice::ObLogHandlerBase
  {
  public:
    void switch_role(const ObRole &, const int64_t) override {}
  } handler;
  palf::PalfHandle palf_handle;
  PalfRoleResult palf_result;
  auto role_guard = MAKE_SCOPE(palf_result, [](PalfRoleResult &) { mock_palf_role = nullptr; });
  mock_palf_role = &palf_result;
  ObRole role = LEADER;
  int64_t proposal_id = 1;
  ObRole curr_role = FOLLOWER;
  int64_t curr_proposal_id = 0;
  bool is_pending_state = false;
  EXPECT_EQ(OB_NOT_INIT, handler.prepare_switch_role(
      curr_role, curr_proposal_id, role, proposal_id, is_pending_state));
  handler.is_inited_ = true;
  EXPECT_EQ(OB_NOT_RUNNING, handler.prepare_switch_role(
      curr_role, curr_proposal_id, role, proposal_id, is_pending_state));
  EXPECT_EQ(0, palf_result.call_count_);

  handler.is_in_stop_state_ = false;
  handler.palf_handle_ = &palf_handle;
  ASSERT_EQ(FOLLOWER, handler.role_);
  ASSERT_EQ(OB_SUCCESS, handler.prepare_switch_role(
      curr_role, curr_proposal_id, role, proposal_id, is_pending_state));
  EXPECT_EQ(FOLLOWER, curr_role);
  EXPECT_EQ(LEADER, role);
  EXPECT_EQ(41, proposal_id);
  EXPECT_FALSE(is_pending_state);
  EXPECT_EQ(&palf_handle, palf_result.queried_handle_);

  palf_result.is_pending_state_ = true;
  ASSERT_EQ(OB_SUCCESS, handler.prepare_switch_role(
      curr_role, curr_proposal_id, role, proposal_id, is_pending_state));
  EXPECT_TRUE(is_pending_state);

  // After stop() closes the handle, role checks must return without dereferencing the cleared pointer.
  handler.is_in_stop_state_ = true;
  handler.palf_handle_ = nullptr;
  EXPECT_EQ(OB_NOT_RUNNING, handler.prepare_switch_role(
      curr_role, curr_proposal_id, role, proposal_id, is_pending_state));
  EXPECT_EQ(2, palf_result.call_count_);
}

TEST_F(TestTabletAtomicProtocol, writer_qualification_uses_existing_palf_handle)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(kTenantId));
  ASSERT_FALSE(GCONF.enable_logservice);
  ObSSWriterService *service = MTL(ObSSWriterService *);
  ASSERT_NE(nullptr, service);
  const ObLSID ls = SYS_LS;
  const ObTabletID tablet_id(9977001);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls, ls_handle, ObLSGetMod::STORAGE_MOD));
  const ipalf::IPalfHandle *palf_handle = ls_handle.get_ls()->get_log_handler()->palf_handle_;

  PalfRoleResult palf_result;
  auto role_guard = MAKE_SCOPE(palf_result, [](PalfRoleResult &) { mock_palf_role = nullptr; });
  mock_palf_role = &palf_result;
  bool is_writer = false;
  int64_t epoch = 0;
  // All three writer types must reuse the management LS handle instead of opening one for each check.
  for (const ObSSWriterType type : {ObSSWriterType::FREEZE, ObSSWriterType::COMPACTION, ObSSWriterType::META}) {
    const ObSSWriterKey key(type, ls, tablet_id);
    ASSERT_EQ(OB_SUCCESS, service->check_lease_by_palf_role(key, is_writer, epoch));
    EXPECT_TRUE(is_writer);
    EXPECT_EQ(palf_result.proposal_id_, epoch);
    EXPECT_EQ(palf_handle, palf_result.queried_handle_);
  }

  const ObSSWriterKey key(ObSSWriterType::META, ls, tablet_id);
  palf_result.role_ = FOLLOWER;
  ASSERT_EQ(OB_SUCCESS, service->check_lease_by_palf_role(key, is_writer, epoch));
  EXPECT_FALSE(is_writer);
  EXPECT_EQ(0, epoch);

  palf_result.role_ = LEADER;
  palf_result.is_pending_state_ = true;
  ASSERT_EQ(OB_SUCCESS, service->check_lease_by_palf_role(key, is_writer, epoch));
  EXPECT_FALSE(is_writer);
  EXPECT_EQ(0, epoch);
  palf_result.is_pending_state_ = false;

  const int64_t call_count = palf_result.call_count_;
  ASSERT_EQ(OB_SUCCESS, service->check_lease_by_palf_role(
      ObSSWriterKey(ObSSWriterType::META, ObLSID(9977), tablet_id), is_writer, epoch));
  EXPECT_FALSE(is_writer);
  EXPECT_EQ(0, epoch);
  EXPECT_EQ(call_count, palf_result.call_count_);

  palf_result.ret_ = OB_ERR_UNEXPECTED;
  EXPECT_EQ(OB_ERR_UNEXPECTED, service->check_lease_by_palf_role(key, is_writer, epoch));
  EXPECT_FALSE(is_writer);
  EXPECT_EQ(0, epoch);
}

TEST_F(TestTabletAtomicProtocol, finish_rechecks_common_writer_qualification)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(kTenantId));
  ASSERT_FALSE(GCONF.enable_logservice);
  PalfRoleResult palf_result;
  auto role_guard = MAKE_SCOPE(palf_result, [](PalfRoleResult &) { mock_palf_role = nullptr; });
  mock_palf_role = &palf_result;

  class CountingProtocolOP final : public ObIAtomicProtocolOP
  {
  public:
    int process(common::ObMySQLTransaction &, const share::SCN &) override
    {
      ++call_count_;
      return OB_SUCCESS;
    }
    int64_t call_count_ = 0;
  };

  ObTabletAtomicProtocolParam param;
  build_param(param, ObLSID(9978), ObTabletID(9978001), share::SCN::min_scn());
  {
    ObTabletAtomicProtocol protocol;
    ASSERT_EQ(OB_SUCCESS, protocol.init(param));
    // An absent local LS means non-writer at the service entry; the atomic protocol reports OB_NOT_MASTER.
    EXPECT_EQ(OB_NOT_MASTER, protocol.create(true));
    EXPECT_EQ(ObAtomicProtocol::STATUS::INIT, protocol.status_);
  }
  build_param(param, SYS_LS, ObTabletID(9978001), share::SCN::min_scn());
  {
    ObTabletAtomicProtocol protocol;
    ASSERT_EQ(OB_SUCCESS, protocol.init(param));
    auto protocol_guard = MAKE_SCOPE(protocol, [](ObTabletAtomicProtocol &p) {
      if (ObAtomicProtocol::STATUS::START == p.status_) {
        EXPECT_EQ(OB_SUCCESS, p.abort());
      }
    });
    palf_result.role_ = FOLLOWER;
    EXPECT_EQ(OB_NOT_MASTER, protocol.create(true));
    EXPECT_EQ(ObAtomicProtocol::STATUS::INIT, protocol.status_);

    palf_result.role_ = LEADER;
    ++palf_result.proposal_id_;
    ASSERT_EQ(OB_SUCCESS, protocol.create(true));
    EXPECT_EQ(palf_result.proposal_id_, protocol.lease_epoch_);
    CountingProtocolOP op;
    ASSERT_EQ(OB_SUCCESS, protocol.finish(op));
    EXPECT_EQ(1, op.call_count_);
  }
  {
    ObTabletAtomicProtocol protocol;
    ASSERT_EQ(OB_SUCCESS, protocol.init(param));
    auto protocol_guard = MAKE_SCOPE(protocol, [](ObTabletAtomicProtocol &p) {
      if (ObAtomicProtocol::STATUS::START == p.status_) {
        EXPECT_EQ(OB_SUCCESS, p.abort());
      }
    });
    ASSERT_EQ(OB_SUCCESS, protocol.create(true));
    CountingProtocolOP op;
    palf_result.role_ = FOLLOWER;
    ++palf_result.proposal_id_;
    EXPECT_EQ(OB_NOT_MASTER, protocol.finish(op));
    // Re-election on the same node must not allow an operation from the previous term to commit.
    palf_result.role_ = LEADER;
    ++palf_result.proposal_id_;
    EXPECT_EQ(OB_NOT_MASTER, protocol.finish(op));
    EXPECT_EQ(0, op.call_count_);
    ObSSMetaCtrlStatus::STATUS status;
    ASSERT_EQ(OB_SUCCESS, get_tablet_ctrl_row_status(protocol, status));
    EXPECT_EQ(ObSSMetaCtrlStatus::STATUS::START, status);
  }
}

// ==================== init ====================

TEST_F(TestTabletAtomicProtocol, init_invalid_argument)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());

  // invalid tenant id
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.tenant_id_ = OB_INVALID_ID;
    int ret = p.init(param);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid ls id
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.ls_id_.reset();
    int ret = p.init(param);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid tablet id
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.tablet_id_.reset();
    int ret = p.init(param);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // invalid reorg scn
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.reorg_scn_.reset();
    int ret = p.init(param);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // is_deleted = true but deleted_scn is invalid
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.is_deleted_ = true;
    param.deleted_scn_.reset();
    param.last_major_snapshot_ = 0;
    int ret = p.init(param);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // is_deleted = true with valid deleted_scn but last_major_snapshot < 0
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.is_deleted_ = true;
    param.deleted_scn_ = share::SCN::min_scn();
    param.last_major_snapshot_ = -1;
    int ret = p.init(param);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // is_deleted = true with both deleted_scn and last_major_snapshot valid
  {
    ObTabletAtomicProtocol p;
    ObTabletAtomicProtocolParam param;
    build_param(param, ls, tablet_id, reorg_scn);
    param.is_deleted_ = true;
    param.deleted_scn_ = share::SCN::min_scn();
    param.last_major_snapshot_ = 100;
    int ret = p.init(param);
    ASSERT_NE(OB_INVALID_ARGUMENT, ret);
  }
}

TEST_F(TestTabletAtomicProtocol, init_twice)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);

  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_INIT_TWICE, p.init(param));
}

TEST_F(TestTabletAtomicProtocol, not_init)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObTabletAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_NOT_INIT, p.create(false /*need_check_sswriter*/));
  ASSERT_EQ(OB_NOT_INIT, p.finish(op));
  ASSERT_EQ(OB_NOT_INIT, p.abort());
}

// Init succeeds and type_ is hardcoded to TABLET_META.
TEST_F(TestTabletAtomicProtocol, init_ok)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);

  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(ObAtomicProtocolTypeEnum::TABLET_META, p.type_.type_);
}

// ==================== state-machine: create / finish / abort ====================

TEST_F(TestTabletAtomicProtocol, create)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObTabletAtomicProtocolParam param1;
  build_param(param1, ls, tablet_id, reorg_scn);
  ObTabletAtomicProtocol tablet_atomic_protocol1;
  ret = tablet_atomic_protocol1.init(param1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletAtomicProtocolParam param2;
  build_param(param2, ls, tablet_id, reorg_scn);
  ObTabletAtomicProtocol tablet_atomic_protocol2;
  ret = tablet_atomic_protocol2.init(param2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol2.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create on the same instance again => state machine rejects it
  ret = tablet_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestTabletAtomicProtocol, finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);

  TestNoopProtocolOP op;
  ObTabletAtomicProtocol tablet_atomic_protocol1;
  ret = tablet_atomic_protocol1.init(param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol1.finish(op);
  ASSERT_EQ(OB_SUCCESS, ret);
  // finish twice
  ret = tablet_atomic_protocol1.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestTabletAtomicProtocol, abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObTabletAtomicProtocolParam param1;
  build_param(param1, ls, tablet_id, reorg_scn);
  ObTabletAtomicProtocol tablet_atomic_protocol1;
  ret = tablet_atomic_protocol1.init(param1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol1.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
  // abort twice => state is ABANDON, not in {INIT, START, COMPLETE} => OB_STATE_NOT_MATCH
  ret = tablet_atomic_protocol1.abort();
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);

  // INIT -> abort directly is also allowed (status INIT belongs to {INIT, START, COMPLETE})
  ObTabletAtomicProtocolParam param2;
  build_param(param2, ls, tablet_id, reorg_scn);
  ObTabletAtomicProtocol tablet_atomic_protocol2;
  ret = tablet_atomic_protocol2.init(param2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_atomic_protocol2.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// finish then abort: status is COMPLETE which IS allowed by check_can_do_abort_
// (it accepts {INIT, START, COMPLETE}). The ctrl row was already deleted by
// finish(), so select_ctrl_row_ returns OB_ITER_END which abort_() rewrites
// to OB_SUCCESS. Result: abort succeeds.
TEST_F(TestTabletAtomicProtocol, abort_after_finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(2);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);

  TestNoopProtocolOP op;
  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.finish(op));
  ret = p.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// abort then finish: status is ABORT, not START => OB_STATE_NOT_MATCH
TEST_F(TestTabletAtomicProtocol, finish_after_abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(3);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);

  TestNoopProtocolOP op;
  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.abort());
  ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// finish without preceding create: status is INIT, not START => OB_STATE_NOT_MATCH
TEST_F(TestTabletAtomicProtocol, finish_without_create)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(4);
  const share::SCN reorg_scn(share::SCN::min_scn());

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);

  TestNoopProtocolOP op;
  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  int ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// Two independent tablets running create -> finish in sequence on the same LS
// should both succeed (lock row is per-tablet).
TEST_F(TestTabletAtomicProtocol, multi_tablet_create_finish)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObTabletAtomicProtocolParam param_a;
  build_param(param_a, ls, ObTabletID(11), reorg_scn);
  ObTabletAtomicProtocolParam param_b;
  build_param(param_b, ls, ObTabletID(12), reorg_scn);

  TestNoopProtocolOP op;
  ObTabletAtomicProtocol p_a;
  ObTabletAtomicProtocol p_b;
  ASSERT_EQ(OB_SUCCESS, p_a.init(param_a));
  ASSERT_EQ(OB_SUCCESS, p_b.init(param_b));
  ASSERT_EQ(OB_SUCCESS, p_a.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_b.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_a.finish(op));
  ASSERT_EQ(OB_SUCCESS, p_b.finish(op));
}

// ==================== key_infos_ resolution: finish deletes / abort abandons ====================

// abort() must mark every sstable ctrl row tracked by key_infos_ as ABANDON, in
// the same transaction that abandons the tablet ctrl row.
TEST_F(TestTabletAtomicProtocol, abort_abandons_key_infos)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(21);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  // Stage a real sstable ctrl row (START) owned by this tablet.
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));
  {
    ObSSMetaCtrlStatus::STATUS status = ObSSMetaCtrlStatus::STATUS::INVALID;
    ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, status));
    ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, status);
  }

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.abort());

  // The sstable ctrl row is now ABANDON (abort abandons rather than deletes).
  ObSSMetaCtrlStatus::STATUS status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::ABANDON, status);
}

// abort() is idempotent over key_infos_: aborting once leaves the sstable row in
// ABANDON, and abandon_key_infos_ tolerates an already-ABANDON row on re-entry.
TEST_F(TestTabletAtomicProtocol, abort_abandons_key_infos_idempotent)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const ObTabletID tablet_id = ObTabletID(22);
  const share::SCN reorg_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.abort());

  // A second protocol round observes the sstable row already ABANDON and calling
  // abandon_key_infos_ directly on it must stay OB_SUCCESS (idempotent).
  ObTabletAtomicProtocolParam param2;
  build_param(param2, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param2.key_infos_.push_back(key_info));
  ObTabletAtomicProtocol p2;
  ASSERT_EQ(OB_SUCCESS, p2.init(param2));
  ASSERT_EQ(OB_SUCCESS, p2.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p2.abort());

  ObSSMetaCtrlStatus::STATUS status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::ABANDON, status);
}

// finish() allocates meta_scn_ based on max(GTS, lock_row.meta_scn_ + 1, ...).
// In sys tenant tests max_reserved_scn is skipped, so forcing lock_row.meta_scn_
// to near max verifies the lock_row + 1 path dominates and is persisted back.
TEST_F(TestTabletAtomicProtocol, finish_uses_lock_row_meta_plus_one_and_advances_lock_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(31);
  const ObTabletID tablet_id = ObTabletID(310);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));

  ObSSMetaCtrlLockRowValue lock_before;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_before));

  ObSSMetaCtrlLockRowValue lock_pre_finish = lock_before;
  lock_pre_finish.meta_scn_ = share::SCN::scn_dec(share::SCN::max_scn());
  ASSERT_TRUE(lock_pre_finish.meta_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, update_tablet_lock_row(ls, tablet_id, reorg_scn, lock_pre_finish));

  ObSSMetaCtrlLockRowValue lock_check;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_check));
  ASSERT_EQ(lock_pre_finish.meta_scn_, lock_check.meta_scn_);

  TestNoopProtocolOP op;
  ASSERT_EQ(OB_SUCCESS, p.finish(op));
  ASSERT_EQ(share::SCN::max_scn(), p.meta_scn_);

  ObSSMetaCtrlLockRowValue lock_after;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_after));
  ASSERT_EQ(p.meta_scn_, lock_after.meta_scn_);
  ASSERT_TRUE(lock_after.meta_scn_ > lock_pre_finish.meta_scn_);

  ObSSMetaCtrlStatus::STATUS tablet_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_ITER_END, get_tablet_ctrl_row_status(p, tablet_status));
}

// If user op fails, the whole finish transaction must roll back:
// - tablet ctrl row stays START (not deleted)
// - key_infos_ rows are not deleted
// - tablet lock row meta_scn is not advanced
TEST_F(TestTabletAtomicProtocol, finish_user_op_failure_rolls_back_tablet_and_key_infos)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(32);
  const ObTabletID tablet_id = ObTabletID(320);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));
  ASSERT_EQ(OB_SUCCESS, set_ctrl_row_status(key_info, ObSSMetaCtrlStatus::STATUS::COMPLETE));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));

  ObSSMetaCtrlLockRowValue lock_before;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_before));

  ObSSMetaCtrlStatus::STATUS key_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, key_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::COMPLETE, key_status);

  ObSSMetaCtrlStatus::STATUS tablet_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_tablet_ctrl_row_status(p, tablet_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, tablet_status);

  TestFailProtocolOP fail_op(OB_ERR_UNEXPECTED);
  ASSERT_EQ(OB_ERR_UNEXPECTED, p.finish(fail_op));

  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, key_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::COMPLETE, key_status);

  ASSERT_EQ(OB_SUCCESS, get_tablet_ctrl_row_status(p, tablet_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, tablet_status);

  ObSSMetaCtrlLockRowValue lock_after;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_after));
  ASSERT_EQ(lock_before.meta_scn_, lock_after.meta_scn_);
  ASSERT_EQ(lock_before.op_id_, lock_after.op_id_);
}

TEST_F(TestTabletAtomicProtocol, finish_deletes_complete_key_infos_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(33);
  const ObTabletID tablet_id = ObTabletID(330);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));
  ASSERT_EQ(OB_SUCCESS, set_ctrl_row_status(key_info, ObSSMetaCtrlStatus::STATUS::COMPLETE));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.finish(op));

  ObSSMetaCtrlStatus::STATUS key_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_ITER_END, get_ctrl_row_status(key_info, key_status));
}

TEST_F(TestTabletAtomicProtocol, finish_with_start_key_info_returns_need_retry)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(34);
  const ObTabletID tablet_id = ObTabletID(340);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_NEED_RETRY, p.finish(op));

  ObSSMetaCtrlStatus::STATUS key_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, key_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, key_status);

  ObSSMetaCtrlStatus::STATUS tablet_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_tablet_ctrl_row_status(p, tablet_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, tablet_status);
}

TEST_F(TestTabletAtomicProtocol, finish_with_missing_key_info_row_returns_iter_end)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(35);
  const ObTabletID tablet_id = ObTabletID(350);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));
  ASSERT_EQ(OB_SUCCESS, remove_ctrl_row(key_info));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_ITER_END, p.finish(op));

  ObSSMetaCtrlStatus::STATUS tablet_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_tablet_ctrl_row_status(p, tablet_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, tablet_status);
}

TEST_F(TestTabletAtomicProtocol, finish_with_abandon_key_info_returns_need_retry)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(36);
  const ObTabletID tablet_id = ObTabletID(360);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ObSSMetaCtrlKeyInfo key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));
  ASSERT_EQ(OB_SUCCESS, set_ctrl_row_status(key_info, ObSSMetaCtrlStatus::STATUS::ABANDON));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));

  ObTabletAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_NEED_RETRY, p.finish(op));

  ObSSMetaCtrlStatus::STATUS key_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, key_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::ABANDON, key_status);
}

// key_info ownership checks must run regardless of lock_set hit/miss. Even when
// the first key_info has already inserted the lock type into lock_set, a later
// same-type key_info from another tablet/reorg must still be rejected.
TEST_F(TestTabletAtomicProtocol, finish_rejects_mismatched_key_info_after_lock_type_cached)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(37);
  const ObTabletID tablet_id = ObTabletID(370);
  const ObTabletID other_tablet_id = ObTabletID(371);
  const share::SCN reorg_scn = share::SCN::min_scn();
  const bool need_check_sswrite = false;

  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, other_tablet_id, reorg_scn));

  ObSSMetaCtrlKeyInfo key_info;
  ObSSMetaCtrlKeyInfo other_key_info;
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, tablet_id, reorg_scn, key_info));
  ASSERT_EQ(OB_SUCCESS, create_sstable_ctrl_row(ls, other_tablet_id, reorg_scn, other_key_info));
  ASSERT_EQ(OB_SUCCESS, set_ctrl_row_status(key_info, ObSSMetaCtrlStatus::STATUS::COMPLETE));
  ASSERT_EQ(OB_SUCCESS, set_ctrl_row_status(other_key_info, ObSSMetaCtrlStatus::STATUS::COMPLETE));

  ObTabletAtomicProtocolParam param;
  build_param(param, ls, tablet_id, reorg_scn);
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(key_info));
  ASSERT_EQ(OB_SUCCESS, param.key_infos_.push_back(other_key_info));

  ObTabletAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_SUCCESS, p.init(param));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_ERR_UNEXPECTED, p.finish(op));

  ObSSMetaCtrlStatus::STATUS status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(key_info, status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::COMPLETE, status);
  ASSERT_EQ(OB_SUCCESS, get_ctrl_row_status(other_key_info, status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::COMPLETE, status);

  ObSSMetaCtrlStatus::STATUS tablet_status = ObSSMetaCtrlStatus::STATUS::INVALID;
  ASSERT_EQ(OB_SUCCESS, get_tablet_ctrl_row_status(p, tablet_status));
  ASSERT_EQ(ObSSMetaCtrlStatus::STATUS::START, tablet_status);
}

TEST_F(TestTabletAtomicProtocol, kept_meta_scn_data_version_gate)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls(38);
  const ObTabletID tablet_id(380);
  const share::SCN reorg_scn = share::SCN::min_scn();
  share::SCN kept_meta_scn;
  ASSERT_EQ(OB_SUCCESS, kept_meta_scn.convert_for_logservice(1000));
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  const ObSSMetaCtrlKey tablet_meta_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, reorg_scn, share::SCN::min_scn(), ls);
  ObSSMetaCtrlKey lock_row_key;
  ASSERT_EQ(OB_SUCCESS, tablet_meta_key.get_lock_row_key(lock_row_key));
  ObLockAtomicProtocol lock_row;
  ASSERT_EQ(OB_SUCCESS, lock_row.init(kTenantId, ls, lock_row_key));
  common::ObMySQLProxy &sql_proxy =
      get_curr_simple_server().get_observer().get_mysql_proxy();
  ObClusterVersion &cluster_version = ObClusterVersion::get_instance();
  const uint64_t original_cluster_version = cluster_version.get_cluster_version();
  struct ScopedVersionRestore
  {
    explicit ScopedVersionRestore(const uint64_t version) : version_(version) {}
    ~ScopedVersionRestore()
    {
      ObClusterVersion::get_instance().update_data_version(version_);
      ObClusterVersion::get_instance().update_cluster_version(version_);
    }
    const uint64_t version_;
  } version_restore(original_cluster_version);

  // 门禁前调用成功但不得写出旧版本无法保留的有效位点。
  cluster_version.update_data_version(DATA_VERSION_4_6_2_0);
  cluster_version.update_cluster_version(CLUSTER_VERSION_4_6_2_0);
  ASSERT_EQ(OB_SUCCESS, lock_row.persist_kept_meta_scn(kept_meta_scn, sql_proxy));
  ObSSMetaCtrlLockRowValue lock_value;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_value));
  ASSERT_TRUE(lock_value.kept_meta_scn_.is_min());

  cluster_version.update_data_version(DATA_CURRENT_VERSION);
  cluster_version.update_cluster_version(CLUSTER_CURRENT_VERSION);
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, kTenantId));
  ASSERT_EQ(OB_SUCCESS, lock_row.persist_kept_meta_scn(kept_meta_scn, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit*/));
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_value));
  ASSERT_EQ(kept_meta_scn, lock_value.kept_meta_scn_);
}

TEST_F(TestTabletAtomicProtocol, persist_kept_meta_scn_missing_row_returns_iter_end)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls(42);
  const ObTabletID tablet_id(420);
  const share::SCN reorg_scn = share::SCN::min_scn();
  share::SCN kept_meta_scn;
  ASSERT_EQ(OB_SUCCESS, kept_meta_scn.convert_for_logservice(4200));
  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(kTenantId, data_version));
  ASSERT_GE(data_version, DATA_VERSION_4_6_2_1);

  const ObSSMetaCtrlKey tablet_meta_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, reorg_scn, share::SCN::min_scn(), ls);
  ObSSMetaCtrlKey lock_row_key;
  ASSERT_EQ(OB_SUCCESS, tablet_meta_key.get_lock_row_key(lock_row_key));
  ObLockAtomicProtocol lock_row;
  ASSERT_EQ(OB_SUCCESS, lock_row.init(kTenantId, ls, lock_row_key));
  common::ObMySQLProxy &sql_proxy =
      get_curr_simple_server().get_observer().get_mysql_proxy();
  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy, kTenantId));
  ASSERT_EQ(OB_ITER_END, lock_row.persist_kept_meta_scn(kept_meta_scn, trans));
  ASSERT_EQ(OB_SUCCESS, trans.end(false /*rollback*/));

  ObSSMetaCtrlLockRowValue lock_value;
  ASSERT_EQ(OB_ITER_END, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_value));
}

TEST_F(TestTabletAtomicProtocol, update_lock_row_rejects_kept_meta_scn_rollback)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls(39);
  const ObTabletID tablet_id(390);
  const share::SCN reorg_scn = share::SCN::min_scn();
  share::SCN low_kept_meta_scn;
  share::SCN high_kept_meta_scn;
  ASSERT_EQ(OB_SUCCESS, low_kept_meta_scn.convert_for_logservice(1000));
  ASSERT_EQ(OB_SUCCESS, high_kept_meta_scn.convert_for_logservice(2000));
  ASSERT_EQ(OB_SUCCESS, ensure_tablet_lock_row(ls, tablet_id, reorg_scn));

  ObSSMetaCtrlLockRowValue lock_value;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, lock_value));
  ASSERT_TRUE(lock_value.kept_meta_scn_.is_min());
  lock_value.kept_meta_scn_ = high_kept_meta_scn;
  ASSERT_EQ(OB_SUCCESS,
      update_tablet_lock_row(ls, tablet_id, reorg_scn, lock_value));

  ObSSMetaCtrlLockRowValue rollback_value = lock_value;
  rollback_value.kept_meta_scn_ = low_kept_meta_scn;
  ASSERT_EQ(OB_STATE_NOT_MATCH,
      update_tablet_lock_row(ls, tablet_id, reorg_scn, rollback_value));
  rollback_value.kept_meta_scn_.set_min();
  ASSERT_EQ(OB_STATE_NOT_MATCH,
      update_tablet_lock_row(ls, tablet_id, reorg_scn, rollback_value));

  ObSSMetaCtrlLockRowValue stored_value;
  ASSERT_EQ(OB_SUCCESS, read_tablet_lock_row(ls, tablet_id, reorg_scn, stored_value));
  ASSERT_EQ(high_kept_meta_scn, stored_value.kept_meta_scn_);
}

} // share
} // oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  oceanbase::common::databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
