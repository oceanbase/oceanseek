/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_server_struct.h"
#include "src/storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_define.h"
#include "src/storage/ss_garbage_collector/shared_gc_control/ob_shared_gc_control_operator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

// Tenant routing for __all_shared_gc_control (meta_record_in_sys=True):
//   sys tenant  (1)       -> SQL executes in SYS
//   meta tenant (odd id)  -> SQL executes in SYS
//   user tenant (even id) -> SQL executes in gen_meta_tenant_id(user_id)
//
// All test cases use meta-tenant IDs (odd, e.g. 1001, 1003, ...) as the
// business tenant_id rowkey so SQLs are routed to the SYS tenant, which always
// exists in the simple server cluster.
class TestSharedGCControlInfoOperator : public ObSimpleClusterTestBase
{
public:
  TestSharedGCControlInfoOperator()
    : ObSimpleClusterTestBase("test_shared_gc_control_operator") {}

  ObISQLClient &sql_proxy() { return get_curr_simple_server().get_observer().get_mysql_proxy(); }

  static int64_t cur_server_id() { return static_cast<int64_t>(GCTX.get_server_id()); }

  static void fill_info(ObSharedGCControlInfo &info,
                        const uint64_t tenant_id,
                        const int64_t executor_server_id,
                        const ObSharedGCControlStatus status,
                        const int64_t gc_round_id,
                        const int64_t allow_gc_round_id,
                        const SCN gc_meta_scn,
                        const SCN last_succ_gc_meta_scn)
  {
    info.reset();
    info.tenant_id_ = tenant_id;
    info.executor_server_id_ = executor_server_id;
    info.status_ = status;
    info.gc_round_id_ = gc_round_id;
    info.allow_gc_round_id_ = allow_gc_round_id;
    info.gc_meta_scn_ = gc_meta_scn;
    info.last_succ_gc_meta_scn_ = last_succ_gc_meta_scn;
  }
};

// =====================================================================
// CRUD round trip: insert -> read -> update -> read -> delete -> read NX
// =====================================================================
TEST_F(TestSharedGCControlInfoOperator, crud_round_trip)
{
  const uint64_t tenant_id = 1001; // meta tenant -> routed to SYS
  int64_t affected = 0;

  ObSharedGCControlInfo to_insert;
  fill_info(to_insert, tenant_id, cur_server_id(),
            ObSharedGCControlStatus::IDLE, 5 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
            SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(sql_proxy(), to_insert, affected));
  ASSERT_EQ(1, affected);

  ObSharedGCControlInfo read_back;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, read_back));
  ASSERT_EQ(tenant_id, read_back.tenant_id_);
  ASSERT_EQ(cur_server_id(), read_back.executor_server_id_);
  ASSERT_TRUE(ObSharedGCControlStatus::IDLE == read_back.status_);
  ASSERT_EQ(5, read_back.gc_round_id_);
  ASSERT_EQ(0, read_back.allow_gc_round_id_);
  ASSERT_TRUE(read_back.gc_meta_scn_.is_min());
  ASSERT_TRUE(read_back.last_succ_gc_meta_scn_.is_min());

  ObSharedGCControlInfo updated;
  fill_info(updated, tenant_id, cur_server_id(),
            ObSharedGCControlStatus::DOING, 5 /*gc_round_id*/, 100 /*allow_gc_round_id*/,
            SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_row(sql_proxy(), updated, affected));
  ASSERT_EQ(1, affected);

  ObSharedGCControlInfo after_update;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, after_update));
  ASSERT_TRUE(ObSharedGCControlStatus::DOING == after_update.status_);
  ASSERT_EQ(100, after_update.allow_gc_round_id_);
  ASSERT_EQ(cur_server_id(), after_update.executor_server_id_);
  // update_row's SET clause does not touch gc_round_id, so it stays at 5.
  ASSERT_EQ(5, after_update.gc_round_id_);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
  ASSERT_EQ(1, affected);

  ObSharedGCControlInfo gone;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, gone));
}

// read_row of a tenant_id that was never inserted returns OB_ENTRY_NOT_EXIST
// (not OB_SUCCESS with reset info) — callers rely on this distinction.
TEST_F(TestSharedGCControlInfoOperator, read_non_existing)
{
  const uint64_t tenant_id = 1003; // meta tenant -> SYS
  ObSharedGCControlInfo info;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
}

// update_row hard-rejects info.executor_server_id_ != cur_server_id at the
// client side (no SQL is sent). Guards against accidentally overwriting your
// own ownership with a foreign server_id via the full-row path.
TEST_F(TestSharedGCControlInfoOperator, update_row_rejects_foreign_executor_server_id)
{
  const uint64_t tenant_id = 1005; // meta tenant -> SYS
  int64_t affected = 0;

  ObSharedGCControlInfo to_insert;
  fill_info(to_insert, tenant_id, cur_server_id(),
            ObSharedGCControlStatus::IDLE, 0 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
            SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(sql_proxy(), to_insert, affected));

  ObSharedGCControlInfo with_foreign_server;
  fill_info(with_foreign_server, tenant_id, cur_server_id() + 7,
            ObSharedGCControlStatus::DOING, 0 /*gc_round_id*/, 50 /*allow_gc_round_id*/,
            SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_row(sql_proxy(), with_foreign_server, affected));

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
}

// Server-Condition update_status: succeeds when the row's executor_server_id
// matches GCTX.get_server_id(); silently no-ops (affected_rows == 0) when the
// row belongs to another server.
TEST_F(TestSharedGCControlInfoOperator, update_status_server_condition_filter)
{
  const uint64_t tenant_a = 1007;  // meta tenant -> SYS, owned by current server
  const uint64_t tenant_b = 1009;  // meta tenant -> SYS, owned by foreign server
  int64_t affected = 0;

  // Row A: owned by current server
  {
    ObSharedGCControlInfo info;
    fill_info(info, tenant_a, cur_server_id(),
              ObSharedGCControlStatus::IDLE, 0 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
              SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  }
  // Row B: owned by foreign server
  {
    ObSharedGCControlInfo info;
    fill_info(info, tenant_b, cur_server_id() + 99,
              ObSharedGCControlStatus::IDLE, 0 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
              SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  }

  // A: owner matches → status flipped, affected == 1
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_status(
      sql_proxy(), tenant_a, ObSharedGCControlStatus::DOING, affected));
  ASSERT_EQ(1, affected);
  ObSharedGCControlInfo a_after;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_a, a_after));
  ASSERT_TRUE(ObSharedGCControlStatus::DOING == a_after.status_);

  // B: owner mismatches → no rows touched, affected == 0; row unchanged
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_status(
      sql_proxy(), tenant_b, ObSharedGCControlStatus::DOING, affected));
  ASSERT_EQ(0, affected);
  ObSharedGCControlInfo b_after;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_b, b_after));
  ASSERT_TRUE(ObSharedGCControlStatus::IDLE == b_after.status_);
  ASSERT_EQ(cur_server_id() + 99, b_after.executor_server_id_);

  // Cleanup
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_a, affected));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_b, affected));
}

// update_allow_gc_round_id new semantics: under a self-managed select-for-update trans, the
// watermark advances ONLY when (1) the row's executor is the current server, (2) the row's
// gc_round_id equals the passed round_id, and (3) round_id strictly exceeds the current
// allow_gc_round_id. Executor/round mismatches are silent no-ops; matched non-increasing
// round_id is unexpected.
TEST_F(TestSharedGCControlInfoOperator, update_allow_gc_round_id_server_condition_filter)
{
  const uint64_t tenant_id = 1011; // meta tenant -> SYS
  int64_t affected = 0;

  // Seed a row, then make the current server the executor with gc_round_id = 10 (allow = 0).
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::insert_default_row(sql_proxy(), tenant_id, affected));
  ASSERT_EQ(1, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
      sql_proxy(), tenant_id, cur_server_id(), ObSharedGCControlStatus::DOING,
      10 /*gc_round_id*/, affected));
  ASSERT_EQ(1, affected);

  ObSharedGCControlInfo info;

  // case A: executor matches, round_id matches (10), 10 > allow(0) -> advance.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_allow_gc_round_id(
      sql_proxy(), tenant_id, 10 /*round_id*/, affected));
  ASSERT_EQ(1, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(10, info.allow_gc_round_id_);

  // case B: round_id (11) does not match the row's gc_round_id (10) -> no-op.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_allow_gc_round_id(
      sql_proxy(), tenant_id, 11 /*round_id*/, affected));
  ASSERT_EQ(0, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(10, info.allow_gc_round_id_);

  // case C: executor matches and round_id matches, but round_id (10) == current allow (10).
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSharedGCControlOperator::update_allow_gc_round_id(
      sql_proxy(), tenant_id, 10 /*round_id*/, affected));
  ASSERT_EQ(0, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(10, info.allow_gc_round_id_);

  // case D: executor matches and round_id matches, but round_id (9) < current allow (10).
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
      sql_proxy(), tenant_id, cur_server_id(), ObSharedGCControlStatus::DOING,
      9 /*gc_round_id*/, affected));
  ASSERT_EQ(1, affected);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObSharedGCControlOperator::update_allow_gc_round_id(
      sql_proxy(), tenant_id, 9 /*round_id*/, affected));
  ASSERT_EQ(0, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(10, info.allow_gc_round_id_);

  // case E: executor is no longer self -> no-op even if round_id matches the new gc_round_id.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
      sql_proxy(), tenant_id, cur_server_id() + 99, ObSharedGCControlStatus::DOING,
      30 /*gc_round_id*/, affected));
  ASSERT_EQ(1, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_allow_gc_round_id(
      sql_proxy(), tenant_id, 30 /*round_id*/, affected));
  ASSERT_EQ(0, affected);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(10, info.allow_gc_round_id_);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
}

// Takeover path: a row owned by another server is reclaimed by the current
// server inside one transaction. select_tenant_id_for_update locks the row,
// then update_executor_state (no server-condition) flips ownership.
// Verifies allow_gc_round_id_ is preserved across takeover.
TEST_F(TestSharedGCControlInfoOperator, takeover_via_select_for_update_in_trans)
{
  const uint64_t tenant_id = 1015; // meta tenant -> routed to SYS
  const int64_t foreign_server = cur_server_id() + 99;
  const int64_t preserved_allow_round_id = 555;
  const int64_t seed_gc_round_id = 333;
  const int64_t takeover_gc_round_id = 777;
  int64_t affected = 0;

  // Pre-populate a row owned by a "foreign" server with a non-trivial
  // allow_gc_round_id so we can verify takeover does not touch it.
  ObSharedGCControlInfo seed;
  fill_info(seed, tenant_id, foreign_server,
            ObSharedGCControlStatus::IDLE, seed_gc_round_id, preserved_allow_round_id,
            SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(sql_proxy(), seed, affected));
  ASSERT_EQ(1, affected);

  {
    // Meta tenant 1015 routes to SYS for the transaction start.
    ObMySQLTransaction trans;
    ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), OB_SYS_TENANT_ID));

    ObSharedGCControlInfo locked;
    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::select_tenant_id_for_update(
        trans, tenant_id, locked));
    ASSERT_EQ(foreign_server, locked.executor_server_id_);
    ASSERT_TRUE(ObSharedGCControlStatus::IDLE == locked.status_);
    ASSERT_EQ(seed_gc_round_id, locked.gc_round_id_);
    ASSERT_EQ(preserved_allow_round_id, locked.allow_gc_round_id_);

    ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
        trans, tenant_id, cur_server_id(), ObSharedGCControlStatus::DOING,
        takeover_gc_round_id, affected));
    ASSERT_EQ(1, affected);

    ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit*/));
  }

  ObSharedGCControlInfo after;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, after));
  ASSERT_EQ(cur_server_id(), after.executor_server_id_);
  ASSERT_TRUE(ObSharedGCControlStatus::DOING == after.status_);
  // Takeover overwrites gc_round_id with the value fetched at acquire time, but does
  // not touch allow_gc_round_id.
  ASSERT_EQ(takeover_gc_round_id, after.gc_round_id_);
  ASSERT_EQ(preserved_allow_round_id, after.allow_gc_round_id_);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
}

// select_tenant_id_for_update of a missing row returns OB_ENTRY_NOT_EXIST and
// leaves the surrounding transaction usable for commit.
TEST_F(TestSharedGCControlInfoOperator, select_for_update_not_found)
{
  const uint64_t tenant_id = 1017; // meta tenant -> SYS

  ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&sql_proxy(), OB_SYS_TENANT_ID));

  ObSharedGCControlInfo out;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCControlOperator::select_tenant_id_for_update(trans, tenant_id, out));

  ASSERT_EQ(OB_SUCCESS, trans.end(true /*commit empty trans*/));
}

// delete_row: success on existing row reports affected_rows == 1; calling
// delete on a missing row returns OB_SUCCESS with affected_rows == 0 (the
// operator does NOT translate this to OB_ENTRY_NOT_EXIST).
TEST_F(TestSharedGCControlInfoOperator, delete_row)
{
  const uint64_t tenant_id = 1019; // meta tenant -> SYS
  int64_t affected = 0;

  ObSharedGCControlInfo info;
  fill_info(info, tenant_id, cur_server_id(),
            ObSharedGCControlStatus::IDLE, 0 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
            SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  ASSERT_EQ(1, affected);

  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
  ASSERT_EQ(1, affected);

  ObSharedGCControlInfo gone;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, gone));

  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
  ASSERT_EQ(0, affected);
}

// gc_meta_scn and last_succ_gc_meta_scn: default values, update, and monotone read-back.
TEST_F(TestSharedGCControlInfoOperator, gc_meta_scn_columns)
{
  const uint64_t tenant_id = 1021; // meta tenant -> SYS
  int64_t affected = 0;

  // insert_default_row: both scn columns default to min_scn (stored as 0).
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::insert_default_row(sql_proxy(), tenant_id, affected));
  ASSERT_EQ(1, affected);

  ObSharedGCControlInfo after_insert;
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, after_insert));
  ASSERT_TRUE(after_insert.gc_meta_scn_.is_min());
  ASSERT_TRUE(after_insert.last_succ_gc_meta_scn_.is_min());

  // Advance gc_meta_scn; cur_server owns the row (executor_server_id=0 does not
  // match cur_server_id, so we first take over ownership).
  const int64_t round_id = 1;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
      sql_proxy(), tenant_id, cur_server_id(), ObSharedGCControlStatus::DOING,
      round_id /*gc_round_id*/, affected));
  ASSERT_EQ(1, affected);

  SCN scn1;
  ASSERT_EQ(OB_SUCCESS, scn1.convert_from_ts(1000000 /*us*/));
  bool is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn1, is_updated));
  ASSERT_TRUE(is_updated);

  ObSharedGCControlInfo after_gc_scn;
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, after_gc_scn));
  ASSERT_EQ(scn1, after_gc_scn.gc_meta_scn_);
  ASSERT_TRUE(after_gc_scn.last_succ_gc_meta_scn_.is_min());

  // Advance last_succ_gc_meta_scn.
  SCN scn2;
  ASSERT_EQ(OB_SUCCESS, scn2.convert_from_ts(2000000 /*us*/));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn2));

  ObSharedGCControlInfo final_state;
  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, final_state));
  ASSERT_EQ(scn1, final_state.gc_meta_scn_);
  ASSERT_EQ(scn2, final_state.last_succ_gc_meta_scn_);

  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
}

// Monotonic (no-fallback) advance of gc_meta_scn / last_succ_gc_meta_scn:
// a larger scn advances; a smaller/equal scn is a no-op; advancing again works.
// Also: only the current executor server may advance the scn.
TEST_F(TestSharedGCControlInfoOperator, meta_scn_no_fallback)
{
  const uint64_t tenant_id = 1023; // meta tenant -> SYS
  int64_t affected = 0;

  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::insert_default_row(sql_proxy(), tenant_id, affected));
  ASSERT_EQ(1, affected);

  // Make the current server the executor so the server/round_id-condition update takes effect.
  const int64_t round_id = 1;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
      sql_proxy(), tenant_id, cur_server_id(), ObSharedGCControlStatus::DOING,
      round_id /*gc_round_id*/, affected));
  ASSERT_EQ(1, affected);

  SCN scn1, scn2, scn3;
  ASSERT_EQ(OB_SUCCESS, scn1.convert_from_ts(1000000 /*us*/));
  ASSERT_EQ(OB_SUCCESS, scn2.convert_from_ts(2000000 /*us*/));
  ASSERT_EQ(OB_SUCCESS, scn3.convert_from_ts(3000000 /*us*/));

  ObSharedGCControlInfo info;
  bool is_updated = false;

  // gc_meta_scn: advance to scn2.
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn2, is_updated));
  ASSERT_TRUE(is_updated);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn2, info.gc_meta_scn_);

  // gc_meta_scn: a smaller scn is a no-op (no rollback).
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn1, is_updated));
  ASSERT_FALSE(is_updated);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn2, info.gc_meta_scn_);

  // gc_meta_scn: equal scn is a no-op.
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn2, is_updated));
  ASSERT_FALSE(is_updated);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn2, info.gc_meta_scn_);

  // gc_meta_scn: a round_id mismatch (same executor server) is a success no-op.
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id + 1 /*wrong round*/, scn3, is_updated));
  ASSERT_FALSE(is_updated);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn2, info.gc_meta_scn_);

  // gc_meta_scn: a larger scn advances.
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn3, is_updated));
  ASSERT_TRUE(is_updated);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn3, info.gc_meta_scn_);

  // last_succ_gc_meta_scn: same monotonic behavior.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn2));
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn2, info.last_succ_gc_meta_scn_);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn1)); // smaller -> no-op
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn2, info.last_succ_gc_meta_scn_);

  // Non-executor server cannot advance: hand the row to another server, then a larger
  // scn must be a success no-op (executor mismatch filters it out), gc_meta_scn stays at scn3.
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_executor_state(
      sql_proxy(), tenant_id, cur_server_id() + 99, ObSharedGCControlStatus::DOING,
      round_id /*gc_round_id*/, affected));
  ASSERT_EQ(1, affected);
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn3, is_updated)); // scn3 is already current, but ensure no error
  ASSERT_FALSE(is_updated); // no-op, not actually updated
  SCN scn4;
  ASSERT_EQ(OB_SUCCESS, scn4.convert_from_ts(4000000 /*us*/));
  is_updated = false;
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, round_id, scn4, is_updated)); // larger, but we are not executor
  ASSERT_FALSE(is_updated);
  ASSERT_EQ(OB_SUCCESS, ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, info));
  ASSERT_EQ(scn3, info.gc_meta_scn_);

  ASSERT_EQ(OB_SUCCESS,
            ObSharedGCControlOperator::delete_row(sql_proxy(), tenant_id, affected));
}

// Advancing on a non-existing row is a no-op success (row inserted by tenant-create flow).
TEST_F(TestSharedGCControlInfoOperator, meta_scn_no_fallback_row_not_exist)
{
  const uint64_t tenant_id = 1025; // meta tenant -> SYS, no row inserted
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_from_ts(1000000 /*us*/));
  bool is_updated = false;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, 1 /*round_id*/, scn, is_updated));
  ASSERT_FALSE(is_updated);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), tenant_id, 1 /*round_id*/, scn));

  ObSharedGCControlInfo gone;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
            ObSharedGCControlOperator::read_row(sql_proxy(), tenant_id, gone));
}

// invalid argument coverage for the no-fallback interfaces.
TEST_F(TestSharedGCControlInfoOperator, meta_scn_no_fallback_invalid_argument)
{
  const uint64_t tenant_id = 1027;
  SCN valid_scn;
  ASSERT_EQ(OB_SUCCESS, valid_scn.convert_from_ts(1000000 /*us*/));

  bool is_updated = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), OB_INVALID_TENANT_ID, 1 /*round_id*/, valid_scn, is_updated));
  ASSERT_FALSE(is_updated);
  is_updated = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, 1 /*round_id*/, SCN::invalid_scn(), is_updated));
  ASSERT_FALSE(is_updated);
  is_updated = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedGCControlOperator::update_gc_meta_scn(
      sql_proxy(), tenant_id, -1 /*invalid round_id*/, valid_scn, is_updated));
  ASSERT_FALSE(is_updated);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), OB_INVALID_TENANT_ID, 1 /*round_id*/, valid_scn));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), tenant_id, 1 /*round_id*/, SCN::invalid_scn()));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_last_succ_gc_meta_scn(
      sql_proxy(), tenant_id, -1 /*invalid round_id*/, valid_scn));
}

// Negative parameter coverage across every public method.
TEST_F(TestSharedGCControlInfoOperator, invalid_argument)
{
  int64_t affected = 0;
  const uint64_t tenant_id = 1099; // meta tenant -> SYS

  // insert_row: invalid tenant_id
  {
    ObSharedGCControlInfo info;
    fill_info(info, OB_INVALID_TENANT_ID, cur_server_id(),
              ObSharedGCControlStatus::IDLE, 0 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
              SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  }
  // insert_row: invalid status (MAX)
  {
    ObSharedGCControlInfo info;
    fill_info(info, tenant_id, cur_server_id(),
              ObSharedGCControlStatus::MAX, 0 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
              SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  }
  // insert_row: negative gc_round_id
  {
    ObSharedGCControlInfo info;
    fill_info(info, tenant_id, cur_server_id(),
              ObSharedGCControlStatus::IDLE, -1 /*gc_round_id*/, 0 /*allow_gc_round_id*/,
              SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  }
  // insert_row: negative allow_gc_round_id
  {
    ObSharedGCControlInfo info;
    fill_info(info, tenant_id, cur_server_id(),
              ObSharedGCControlStatus::IDLE, 0 /*gc_round_id*/, -1 /*allow_gc_round_id*/,
              SCN::min_scn()/*gc_meta_scn*/, SCN::min_scn()/*last_succ_gc_meta_scn*/);
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCControlOperator::insert_row(sql_proxy(), info, affected));
  }
  // delete_row: invalid tenant_id
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::delete_row(sql_proxy(), OB_INVALID_TENANT_ID, affected));
  // read_row: invalid tenant_id
  {
    ObSharedGCControlInfo info;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCControlOperator::read_row(sql_proxy(), OB_INVALID_TENANT_ID, info));
  }
  // update_allow_gc_round_id: negative round_id
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_allow_gc_round_id(sql_proxy(), tenant_id,
                -1 /*round_id*/, affected));
  // update_allow_gc_round_id: invalid tenant_id
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_allow_gc_round_id(
                sql_proxy(), OB_INVALID_TENANT_ID, 1 /*round_id*/, affected));
  // update_status: invalid status
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_status(
                sql_proxy(), tenant_id, ObSharedGCControlStatus::MAX, affected));
  // update_executor_state: negative executor_server_id
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_executor_state(
                sql_proxy(), tenant_id, -1 /*executor_server_id*/, ObSharedGCControlStatus::IDLE,
                0 /*gc_round_id*/, affected));
  // update_executor_state: negative gc_round_id
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            ObSharedGCControlOperator::update_executor_state(
                sql_proxy(), tenant_id, cur_server_id(), ObSharedGCControlStatus::DOING,
                -1 /*gc_round_id*/, affected));
  // select_tenant_id_for_update: invalid tenant_id
  {
    ObSharedGCControlInfo info;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
              ObSharedGCControlOperator::select_tenant_id_for_update(
                  sql_proxy(), OB_INVALID_TENANT_ID, info));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
