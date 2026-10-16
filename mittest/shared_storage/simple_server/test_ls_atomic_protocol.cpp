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
#include "storage/incremental/atomic_protocol/ob_ls_atomic_protocol.h"
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

class TestLSAtomicProtocol : public unittest::ObSimpleClusterTestBase
{
public:
  TestLSAtomicProtocol() : unittest::ObSimpleClusterTestBase("test_ls_atomic_protocol") {}

  // common test parameters
  static constexpr uint64_t kTenantId = 1;
};

// ==================== init ====================

TEST_F(TestLSAtomicProtocol, init_invalid_argument)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN deleted_scn(share::SCN::min_scn());

  // 1) invalid tenant id
  {
    ObLSAtomicProtocol p;
    int ret = p.init(OB_INVALID_ID, ls, false /*is_deleted*/, deleted_scn);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // 2) invalid ls id
  {
    ObLSAtomicProtocol p;
    ObLSID invalid_ls;
    int ret = p.init(kTenantId, invalid_ls, false /*is_deleted*/, deleted_scn);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
  // 3) is_deleted = true but deleted_scn is invalid
  {
    ObLSAtomicProtocol p;
    share::SCN invalid_scn;
    int ret = p.init(kTenantId, ls, true /*is_deleted*/, invalid_scn);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }
}

TEST_F(TestLSAtomicProtocol, init_twice)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN deleted_scn(share::SCN::min_scn());

  ObLSAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn));
  ASSERT_EQ(OB_INIT_TWICE, p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn));
}

TEST_F(TestLSAtomicProtocol, not_init)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObLSAtomicProtocol p;
  TestNoopProtocolOP op;
  ASSERT_EQ(OB_NOT_INIT, p.create(false /*need_check_sswriter*/));
  ASSERT_EQ(OB_NOT_INIT, p.finish(op));
  ASSERT_EQ(OB_NOT_INIT, p.abort());
}

// Init succeeds and type_ is hardcoded to LS_META, leaving state machine in INIT.
TEST_F(TestLSAtomicProtocol, init_ok)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN deleted_scn(share::SCN::min_scn());

  ObLSAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn));
  ASSERT_EQ(ObAtomicProtocolTypeEnum::LS_META, p.type_.type_);
}

// ==================== state-machine: create / finish / abort ====================

TEST_F(TestLSAtomicProtocol, create)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN deleted_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObLSAtomicProtocol p1;
  ret = p1.init(kTenantId, ls, false /*is_deleted*/, deleted_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  // A second independent protocol session on the same (tenant, ls) is allowed
  // because each create() picks up a fresh meta_scn_ and therefore writes a
  // distinct ctrl_row (ctrl_key.ls_key_.control_scn_ differs).
  ObLSAtomicProtocol p2;
  ret = p2.init(kTenantId, ls, false /*is_deleted*/, deleted_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p2.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create on the same instance again => state machine rejects it
  ret = p1.create(need_check_sswrite);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestLSAtomicProtocol, finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN deleted_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObLSAtomicProtocol p;
  ret = p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p.finish(op);
  ASSERT_EQ(OB_SUCCESS, ret);
  // finish twice => state machine rejects (status is COMPLETE, not START)
  ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

TEST_F(TestLSAtomicProtocol, abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(1);
  const share::SCN deleted_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  ObLSAtomicProtocol p1;
  ret = p1.init(kTenantId, ls, false /*is_deleted*/, deleted_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p1.create(need_check_sswrite);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p1.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
  // abort twice => state is ABANDON, not in {INIT, START, COMPLETE} => OB_STATE_NOT_MATCH
  ret = p1.abort();
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);

  // INIT -> abort directly is also allowed (status INIT belongs to {INIT, START, COMPLETE}).
  // The abort path internally tolerates "ctrl row not found" (OB_ITER_END
  // is treated as success) so this works without a preceding create().
  ObLSAtomicProtocol p2;
  ret = p2.init(kTenantId, ls, false /*is_deleted*/, deleted_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = p2.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// finish then abort: status is COMPLETE which IS allowed by check_can_do_abort_
// (it accepts {INIT, START, COMPLETE}). The ctrl row was already deleted by
// finish(), so select_ctrl_row_ returns OB_ITER_END which abort_() rewrites
// to OB_SUCCESS. Result: abort succeeds.
TEST_F(TestLSAtomicProtocol, abort_after_finish)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(2);
  const share::SCN deleted_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObLSAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.finish(op));
  ret = p.abort();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// abort then finish: status is ABORT, not START => OB_STATE_NOT_MATCH
TEST_F(TestLSAtomicProtocol, finish_after_abort)
{
  int ret = OB_SUCCESS;
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(3);
  const share::SCN deleted_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObLSAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn));
  ASSERT_EQ(OB_SUCCESS, p.create(need_check_sswrite));
  ASSERT_EQ(OB_SUCCESS, p.abort());
  ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// finish without preceding create: status is INIT, not START => OB_STATE_NOT_MATCH
TEST_F(TestLSAtomicProtocol, finish_without_create)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls = ObLSID(4);
  const share::SCN deleted_scn(share::SCN::min_scn());

  TestNoopProtocolOP op;
  ObLSAtomicProtocol p;
  ASSERT_EQ(OB_SUCCESS, p.init(kTenantId, ls, false /*is_deleted*/, deleted_scn));
  int ret = p.finish(op);
  ASSERT_EQ(OB_STATE_NOT_MATCH, ret);
}

// Two independent LSs must not collide when running create/finish in
// parallel-ish fashion (lock row is per-(ls, type)).
TEST_F(TestLSAtomicProtocol, multi_ls_create_finish)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const share::SCN deleted_scn(share::SCN::min_scn());
  const bool need_check_sswrite = false;

  TestNoopProtocolOP op;
  ObLSAtomicProtocol p_a;
  ObLSAtomicProtocol p_b;
  ASSERT_EQ(OB_SUCCESS, p_a.init(kTenantId, ObLSID(11), false /*is_deleted*/, deleted_scn));
  ASSERT_EQ(OB_SUCCESS, p_b.init(kTenantId, ObLSID(12), false /*is_deleted*/, deleted_scn));
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
