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
#include "storage/incremental/atomic_protocol/ob_checkpoint_atomic_protocol.h"
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

class TestCheckpointAtomicProtocol : public unittest::ObSimpleClusterTestBase
{
public:
  TestCheckpointAtomicProtocol() : unittest::ObSimpleClusterTestBase("test_checkpoint_atomic_protocol") {}

  // Only the sys tenant is accepted by ObCheckpointAtomicProtocol::init
  // (see is_sys_tenant() guard); OB_SYS_TENANT_ID == 1.
  static constexpr uint64_t kTenantId = OB_SYS_TENANT_ID;
  // A non-sys tenant id, used to verify init() rejects non-sys tenants.
  static constexpr uint64_t kNonSysTenantId = 1001;
};

// ==================== init ====================

TEST_F(TestCheckpointAtomicProtocol, init_invalid_argument)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);

  // 1) invalid tenant id
  {
    ObCheckpointAtomicProtocol p;
    int ret = p.init(OB_INVALID_ID, ls);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // 2) non-sys tenant id is rejected (init only accepts sys tenant).
  {
    ObCheckpointAtomicProtocol p;
    int ret = p.init(kNonSysTenantId, ls);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // 3) invalid ls id
  {
    ObCheckpointAtomicProtocol p;
    ObLSID invalid_ls;
    int ret = p.init(kTenantId, invalid_ls);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

TEST_F(TestCheckpointAtomicProtocol, init_twice)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);

  ObCheckpointAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls));
  ASSERT_EQ(OB_INIT_TWICE, p.init(kTenantId, ls));
}

TEST_F(TestCheckpointAtomicProtocol, not_init)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObCheckpointAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_NOT_INIT, p.create(false /*need_check_sswriter*/));
  ASSERT_EQ(OB_NOT_INIT, p.finish(op));
  ASSERT_EQ(OB_NOT_INIT, p.abort());
}

// Init with the canonical (sys tenant, valid ls) pair succeeds and pins
// type_ to CHECKPOINT internally; the state machine is left in INIT.
TEST_F(TestCheckpointAtomicProtocol, init_ok)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);

  ObCheckpointAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls));
  ASSERT_EQ(ObAtomicProtocolTypeEnum::CHECKPOINT, p.type_.type_);
}

// ==================== state-machine: create / finish / abort ====================

TEST_F(TestCheckpointAtomicProtocol, create)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const bool need_check_sswrite = false;

  ObCheckpointAtomicProtocol p1;
  ret = p1.init(kTenantId, ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  // A second independent protocol session on the same (tenant, ls) is allowed
  // because create_() picks up a fresh meta_scn_ and therefore writes a
  // distinct ctrl_row (ctrl_key.ls_key_.control_scn_ differs).
  ObCheckpointAtomicProtocol p2;
  ret = p2.init(kTenantId, ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p2.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create on the same instance again => state machine rejects it
  ret = p1.create(need_check_sswrite);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestCheckpointAtomicProtocol, finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObCheckpointAtomicProtocol p;
  ret = p.init(kTenantId, ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p.finish(op);
  ASSERT_EQ(OB_SUCCESS, ret);
  // finish twice => state is COMPLETE, not START => OB_STATE_NOT_MATCH
  ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestCheckpointAtomicProtocol, abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const bool need_check_sswrite = false;

  ObCheckpointAtomicProtocol p1;
  ret = p1.init(kTenantId, ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p1.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
  // abort twice => state is ABANDON, not in {INIT, START, COMPLETE} => OB_STATE_NOT_MATCH
  ret = p1.abort();
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);

  // INIT -> abort directly is also allowed (status INIT belongs to {INIT, START, COMPLETE}).
  // The base abort_() tolerates "ctrl row not found" (OB_ITER_END is treated
  // as success), so this works without a preceding create().
  ObCheckpointAtomicProtocol p2;
  ret = p2.init(kTenantId, ls);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p2.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// finish then abort: status is COMPLETE which IS allowed by check_can_do_abort_
// (it accepts {INIT, START, COMPLETE}). The ctrl row was already deleted by
// finish(), so select_ctrl_row_ returns OB_ITER_END which abort_() rewrites
// to OB_SUCCESS. Result: abort succeeds.
TEST_F(TestCheckpointAtomicProtocol, abort_after_finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(2);
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObCheckpointAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.finish(op));
  ret = p.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// abort then finish: status is ABANDON, not START => OB_STATE_NOT_MATCH
TEST_F(TestCheckpointAtomicProtocol, finish_after_abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(3);
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObCheckpointAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.abort());
  ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// finish without preceding create: status is INIT, not START => OB_STATE_NOT_MATCH
TEST_F(TestCheckpointAtomicProtocol, finish_without_create)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(4);

  TestNoopProtocolOP op;
  ObCheckpointAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls));
  int ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// Two independent LSs running create/finish in sequence should both succeed
// (lock row is per-(ls, type)).
TEST_F(TestCheckpointAtomicProtocol, multi_ls_create_finish)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObCheckpointAtomicProtocol p_a;
  ObCheckpointAtomicProtocol p_b;
  ASSERT_EQ(OB_SUCCESS, p_a.init(kTenantId, ObLSID(11)));
  ASSERT_EQ(OB_SUCCESS, p_b.init(kTenantId, ObLSID(12)));
  ASSERT_EQ(OB_SUCCESS, p_a.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_b.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p_a.finish(op));
  ASSERT_EQ(OB_SUCCESS, p_b.finish(op));
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
