/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include <gtest/gtest.h>
#include <iostream>
#include <atomic>
#include <random>
#include <thread>
#include <vector>
#include "lib/utility/ob_defer.h"
#define private public
#define protected public

#include "simple_server/vector_index/test_vector_index_utils.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"

namespace oceanbase {
namespace unittest {

using namespace oceanbase::common;
using namespace oceanbase::share;

class TestVectorIndexFreeze : public TestVectorIndexBase {
public:
  static constexpr char tenant_name[] = "vdb";

  TestVectorIndexFreeze()
      : TestVectorIndexBase("10G", "test_vector_index_scn_guard")
  {
  }
};

TEST_F(TestVectorIndexFreeze, same_scn_freeze_merge_guard)
{
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, "12G", "16G", false, 10));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, tenant_name));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name));
  auto &sys = get_curr_simple_server().get_sql_proxy();
  auto &sql = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.write("alter system set _persist_vector_index_incremental = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.write("alter system set ob_vector_index_active_segment_max_size = '100GB'", affected_rows));

  auto enable_merge = [&](bool enable) {
    ObSqlString stmt;
    EXPECT_EQ(OB_SUCCESS, stmt.assign_fmt(
        "alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = %d, frequency = %d",
        enable ? 0 : 1, enable ? 0 : 1));
    return sys.write(stmt.ptr(), affected_rows);
  };
  auto set_errsim = [&](const char *name, int64_t tablet_id, bool enable) {
    ObSqlString stmt;
    const int ret = stmt.assign_fmt(
        "alter system set_tp tp_name = %s, error_code = %d, frequency = %d, match = %ld%s",
        name, enable ? -OB_EAGAIN : 0, enable ? 1 : 0, tablet_id, enable ? ", occur = 1" : "");
    return OB_SUCCESS == ret ? sys.write(stmt.ptr(), affected_rows) : ret;
  };
  DEFER(
    for (const char *name : {"ERRSIM_VEC_FREEZE_SCN_EQUAL_MERGE", "ERRSIM_VEC_FREEZE_SCN_BEFORE_MERGE",
                             "ERRSIM_VEC_MERGE_SCN_EQUAL_FREEZE", "ERRSIM_VEC_MERGE_SCN_BEFORE_FREEZE",
                             "ERRSIM_VEC_FREEZE_PERSIST_FAILED"}) {
      EXPECT_EQ(OB_SUCCESS, set_errsim(name, 0, false));
    }
    EXPECT_EQ(OB_SUCCESS, enable_merge(true));
  );
  auto check_injected = [&](const char *name, int64_t tablet_id) {
    ObSqlString stmt;
    int64_t remaining = 1;
    ASSERT_EQ(OB_SUCCESS, stmt.assign_fmt(
        "select coalesce(min(tp_occur), 1) val from oceanbase.__all_virtual_tracepoint_info "
        "where tp_name = '%s' and tp_match = %ld and tp_error_code = %d", name, tablet_id, OB_EAGAIN));
    ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sys, stmt.ptr(), remaining));
    ASSERT_EQ(0, remaining) << name << " must have fired for the target tablet";
  };
  auto get_latest_task = [&](int64_t tablet_id, int64_t type, int64_t &id) {
    ObSqlString stmt;
    int ret = stmt.assign_fmt("select coalesce(max(task_id), 0) val "
        "from oceanbase.__all_vector_index_task_history where tablet_id = %ld and task_type = %ld",
        tablet_id, type);
    return OB_SUCCESS == ret ? SSH::select_int64(sql, stmt.ptr(), id) : ret;
  };
  auto wait_task = [&](int64_t tablet_id, int64_t type, int64_t prev_id, int64_t &id, int64_t &code) {
    ObSqlString stmt;
    ASSERT_EQ(OB_SUCCESS, stmt.assign_fmt("select coalesce(min(task_id), 0) val "
        "from oceanbase.__all_vector_index_task_history "
        "where tablet_id = %ld and task_type = %ld and task_id > %ld", tablet_id, type, prev_id));
    const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
    do {
      ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql, stmt.ptr(), id));
      if (id <= prev_id) {
        ob_usleep(100 * 1000);
      }
    } while (id <= prev_id && ObTimeUtility::current_time() < deadline);
    ASSERT_GT(id, prev_id);
    ASSERT_EQ(OB_SUCCESS, stmt.assign_fmt("select ret_code val "
        "from oceanbase.__all_vector_index_task_history where tablet_id = %ld and task_id = %ld",
        tablet_id, id));
    ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql, stmt.ptr(), code));
    ASSERT_EQ(OB_SUCCESS, stmt.assign_fmt("select count(*) val "
        "from oceanbase.__all_vector_index_task where tablet_id = %ld and task_id = %ld",
        tablet_id, id));
    int64_t pending = 0;
    do {
      ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql, stmt.ptr(), pending));
      if (pending != 0) {
        ob_usleep(100 * 1000);
      }
    } while (pending != 0 && ObTimeUtility::current_time() < deadline);
    ASSERT_EQ(0, pending);
  };
  auto run_task = [&](bool merge, int64_t tablet_id, int64_t &id, int64_t &code) {
    const int64_t type = merge ? OB_VECTOR_ASYNC_INDEX_MERGE : OB_VECTOR_ASYNC_INDEX_FREEZE;
    int64_t prev_id = 0;
    ASSERT_EQ(OB_SUCCESS, get_latest_task(tablet_id, type, prev_id));
    ASSERT_EQ(OB_SUCCESS, sql.write(merge ? "call dbms_vector.compact_index('t1', 'vec_idx1')"
                                        : "call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
    ASSERT_NO_FATAL_FAILURE(wait_task(tablet_id, type, prev_id, id, code));
  };

  auto run_direction = [&](bool merge_first) {
    int ret = OB_SUCCESS;
    ASSERT_EQ(OB_SUCCESS, enable_merge(false));
    ASSERT_EQ(OB_SUCCESS, sql.write("create table t1 (c1 int auto_increment primary key, "
        "embedding vector(18), vector index vec_idx1(embedding) with "
        "(distance=cosine,type=hnsw,lib=vsag))", affected_rows));
    int64_t inc_tablet = 0;
    int64_t ls_id = 0;
    int64_t task_id = 0;
    int64_t ret_code = 0;
    ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql,
        "select t2.tablet_id val from oceanbase.__all_table t1 "
        "join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id "
        "where t1.table_name like '__idx_%_vec_idx1' limit 1", inc_tablet));
    ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql,
        "select ls_id val from oceanbase.__all_tablet_to_ls where tablet_id in "
        "(select tablet_id from oceanbase.__all_table where table_name like '__idx_%_vec_idx1')",
        ls_id));
    ASSERT_GT(inc_tablet, 0);
    ASSERT_GT(ls_id, 0);
    for (int i = 0; i < 2; ++i) {
      ASSERT_NO_FATAL_FAILURE(insert_data(i, kFreezeRowsPerWorker));
      for (int attempt = 0; attempt < 3; ++attempt) {
        ASSERT_EQ(OB_SUCCESS, sql.write(
            "call dbms_vector.refresh_index('vec_idx1', 't1', 'embedding', 1, 'FAST')", affected_rows));
        ASSERT_NO_FATAL_FAILURE(run_task(false, inc_tablet, task_id, ret_code));
        if (OB_EAGAIN != ret_code) {
          break;
        }
      }
      ASSERT_EQ(OB_SUCCESS, ret_code);
    }

    MTL_SWITCH(tenant_id) {
      auto *service = MTL(ObPluginVectorIndexService *);
      const ObTabletID tablet(inc_tablet);
      const ObLSID ls(ls_id);
      auto get_adaptor = [&](ObPluginVectorIndexAdapterGuard &guard) {
        const int64_t deadline = ObTimeUtility::current_time() + 15L * 1000 * 1000;
        do {
          if (OB_SUCCESS == service->get_ls_local_adapter_inst_guard(ls, tablet, guard)
              && nullptr != guard.get_adatper() && guard.get_adatper()->is_ready_complete()) {
            return OB_SUCCESS;
          }
          ob_usleep(100 * 1000);
        } while (ObTimeUtility::current_time() < deadline);
        return OB_TIMEOUT;
      };
      ObPluginVectorIndexAdapterGuard initial_guard;
      ASSERT_EQ(OB_SUCCESS, get_adaptor(initial_guard));
      const int64_t bitmap_tablet = initial_guard.get_adatper()->get_vbitmap_tablet_id().id();
      ASSERT_GT(bitmap_tablet, 0);
      if (merge_first) {
        // The frozen batch must already be visible at the SCN published by merge.
        ASSERT_NO_FATAL_FAILURE(insert_data(2, kFreezeRowsPerWorker));
        ASSERT_EQ(OB_SUCCESS, sql.write(
            "call dbms_vector.refresh_index('vec_idx1', 't1', 'embedding', 1, 'FAST')", affected_rows));
        ASSERT_EQ(OB_SUCCESS, enable_merge(true));
        ASSERT_NO_FATAL_FAILURE(run_task(true, bitmap_tablet, task_id, ret_code));
        ASSERT_EQ(OB_SUCCESS, ret_code);
        ASSERT_EQ(OB_SUCCESS, enable_merge(false));
      }
      ObPluginVectorIndexAdapterGuard before_guard;
      ASSERT_EQ(OB_SUCCESS, get_adaptor(before_guard));
      auto *adaptor = before_guard.get_adatper();
      ObVectorIndexMeta before_meta;
      ASSERT_EQ(OB_SUCCESS, get_snapshot_metadata(adaptor, ls, before_meta));
      ASSERT_TRUE(before_meta.is_valid());
      const auto &segments = merge_first ? before_meta.bases_ : before_meta.incrs_;
      ASSERT_GT(segments.count(), 0);
      ASSERT_EQ(VIAT_HNSW, segments.at(segments.count() - 1).index_type_);
      ASSERT_EQ(VIAT_HNSW, adaptor->get_incr_index_type());
      const uint64_t scn = segments.at(segments.count() - 1).scn_;
      const ObVectorIndexSegment *active_before = nullptr;
      SCN freeze_scn_before;
      {
        TCWLockGuard guard(adaptor->get_incr_data()->mem_data_rwlock_);
        const SCN &registered_scn = merge_first ? adaptor->get_incr_data()->merge_task_scn_
                                                : adaptor->get_incr_data()->freeze_task_scn_;
        ASSERT_TRUE(registered_scn.is_valid());
        ASSERT_EQ(scn, registered_scn.get_val_for_inner_table_field());
        active_before = adaptor->get_incr_data()->segment_handle_.get();
        freeze_scn_before = adaptor->get_incr_data()->freeze_task_scn_;
      }

      if (merge_first) {
        ASSERT_EQ(OB_SUCCESS, set_errsim("ERRSIM_VEC_FREEZE_SCN_EQUAL_MERGE", inc_tablet, true));
        ASSERT_NO_FATAL_FAILURE(run_task(false, inc_tablet, task_id, ret_code));
        const int64_t conflict_task_id = task_id;
        ASSERT_NO_FATAL_FAILURE(check_injected("ERRSIM_VEC_FREEZE_SCN_EQUAL_MERGE", inc_tablet));
        ASSERT_EQ(OB_EAGAIN, ret_code) << "same-SCN freeze must stop before switching";
        {
          TCWLockGuard guard(adaptor->get_incr_data()->mem_data_rwlock_);
          EXPECT_FALSE(adaptor->has_frozen());
          EXPECT_EQ(active_before, adaptor->get_incr_data()->segment_handle_.get());
          EXPECT_EQ(freeze_scn_before, adaptor->get_incr_data()->freeze_task_scn_);
        }
        ObVectorIndexMeta after_conflict;
        ASSERT_EQ(OB_SUCCESS, get_snapshot_metadata(adaptor, ls, after_conflict));
        EXPECT_EQ(before_meta.segment_count(), after_conflict.segment_count());
        ASSERT_GT(after_conflict.bases_.count(), 0);
        EXPECT_EQ(before_meta.bases_.at(0).start_key_, after_conflict.bases_.at(0).start_key_);
        ASSERT_EQ(OB_SUCCESS, set_errsim("ERRSIM_VEC_FREEZE_SCN_EQUAL_MERGE", inc_tablet, false));

        // A stale candidate must stop before switching or updating its registration.
        ASSERT_EQ(OB_SUCCESS, set_errsim("ERRSIM_VEC_FREEZE_SCN_BEFORE_MERGE", inc_tablet, true));
        ASSERT_NO_FATAL_FAILURE(run_task(false, inc_tablet, task_id, ret_code));
        ASSERT_NO_FATAL_FAILURE(check_injected("ERRSIM_VEC_FREEZE_SCN_BEFORE_MERGE", inc_tablet));
        ASSERT_EQ(OB_EAGAIN, ret_code) << "older-SCN freeze must stop before switching";
        {
          TCWLockGuard guard(adaptor->get_incr_data()->mem_data_rwlock_);
          EXPECT_FALSE(adaptor->has_frozen());
          EXPECT_EQ(active_before, adaptor->get_incr_data()->segment_handle_.get());
          EXPECT_EQ(freeze_scn_before, adaptor->get_incr_data()->freeze_task_scn_);
        }
        ObVectorIndexMeta after_stale;
        ASSERT_EQ(OB_SUCCESS, get_snapshot_metadata(adaptor, ls, after_stale));
        EXPECT_EQ(before_meta.segment_count(), after_stale.segment_count());
        EXPECT_EQ(before_meta.header_.scn_, after_stale.header_.scn_);
        ASSERT_EQ(OB_SUCCESS, set_errsim("ERRSIM_VEC_FREEZE_SCN_BEFORE_MERGE", inc_tablet, false));

        // A fresh SCN may freeze; a persistence retry must keep that accepted SCN.
        ASSERT_EQ(OB_SUCCESS, set_errsim("ERRSIM_VEC_FREEZE_PERSIST_FAILED", inc_tablet, true));
        ASSERT_NO_FATAL_FAILURE(run_task(false, inc_tablet, task_id, ret_code));
        ASSERT_NO_FATAL_FAILURE(check_injected("ERRSIM_VEC_FREEZE_PERSIST_FAILED", inc_tablet));
        ASSERT_EQ(OB_EAGAIN, ret_code) << "persist failure must keep the frozen segment for retry";
        const int64_t failed_persist_task_id = task_id;
        uint64_t persist_scn = 0;
        {
          TCWLockGuard guard(adaptor->get_incr_data()->mem_data_rwlock_);
          ASSERT_TRUE(adaptor->has_frozen());
          persist_scn = adaptor->get_frozen_data()->frozen_scn_.get_val_for_inner_table_field();
        }
        ASSERT_GT(persist_scn, scn);
        ASSERT_EQ(OB_SUCCESS, set_errsim("ERRSIM_VEC_FREEZE_PERSIST_FAILED", inc_tablet, false));
        const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
        do {
          const int64_t prev_id = task_id;
          ASSERT_NO_FATAL_FAILURE(wait_task(inc_tablet, OB_VECTOR_ASYNC_INDEX_FREEZE,
                                           prev_id, task_id, ret_code));
        } while (OB_EAGAIN == ret_code && ObTimeUtility::current_time() < deadline);
        ASSERT_GT(task_id, failed_persist_task_id);
        ASSERT_GT(task_id, conflict_task_id);
        ASSERT_EQ(OB_SUCCESS, ret_code);
        ObVectorIndexMeta after_retry;
        ASSERT_EQ(OB_SUCCESS, get_snapshot_metadata(adaptor, ls, after_retry));
        ASSERT_GT(after_retry.incrs_.count(), 0);
        EXPECT_EQ(persist_scn, after_retry.incrs_.at(after_retry.incrs_.count() - 1).scn_);
      } else {
        for (const char *name : {"ERRSIM_VEC_MERGE_SCN_EQUAL_FREEZE", "ERRSIM_VEC_MERGE_SCN_BEFORE_FREEZE"}) {
          ASSERT_EQ(OB_SUCCESS, set_errsim(name, inc_tablet, true));
          ASSERT_EQ(OB_SUCCESS, enable_merge(true));
          ASSERT_NO_FATAL_FAILURE(run_task(true, bitmap_tablet, task_id, ret_code));
          ASSERT_EQ(OB_SUCCESS, enable_merge(false));
          ASSERT_NO_FATAL_FAILURE(check_injected(name, inc_tablet));
          ASSERT_EQ(OB_EAGAIN, ret_code) << "equal or older-SCN merge must finish before building";
          {
            TCWLockGuard guard(adaptor->get_incr_data()->mem_data_rwlock_);
            EXPECT_FALSE(adaptor->get_incr_data()->merge_task_scn_.is_valid());
            EXPECT_EQ(freeze_scn_before, adaptor->get_incr_data()->freeze_task_scn_);
          }
          ObVectorIndexMeta after_conflict;
          ASSERT_EQ(OB_SUCCESS, get_snapshot_metadata(adaptor, ls, after_conflict));
          EXPECT_EQ(before_meta.header_.scn_, after_conflict.header_.scn_);
          EXPECT_EQ(before_meta.segment_count(), after_conflict.segment_count());
          ASSERT_EQ(OB_SUCCESS, set_errsim(name, inc_tablet, false));
        }
        const int64_t conflict_task_id = task_id;

        // Let the scheduler create a new merge task with a fresh SCN.
        ASSERT_EQ(OB_SUCCESS, enable_merge(true));
        const int64_t deadline = ObTimeUtility::current_time() + 60L * 1000 * 1000;
        do {
          const int64_t prev_id = task_id;
          ASSERT_NO_FATAL_FAILURE(wait_task(bitmap_tablet, OB_VECTOR_ASYNC_INDEX_MERGE,
                                           prev_id, task_id, ret_code));
        } while (OB_EAGAIN == ret_code && ObTimeUtility::current_time() < deadline);
        ASSERT_GT(task_id, conflict_task_id);
        ASSERT_EQ(OB_SUCCESS, ret_code);
        ASSERT_EQ(OB_SUCCESS, enable_merge(false));
        uint64_t merge_scn = 0;
        {
          TCWLockGuard guard(adaptor->get_incr_data()->mem_data_rwlock_);
          ASSERT_TRUE(adaptor->get_incr_data()->merge_task_scn_.is_valid());
          merge_scn = adaptor->get_incr_data()->merge_task_scn_.get_val_for_inner_table_field();
        }
        EXPECT_GT(merge_scn, scn);
        ObVectorIndexMeta after_meta;
        ASSERT_EQ(OB_SUCCESS, get_snapshot_metadata(adaptor, ls, after_meta));
        ASSERT_GT(after_meta.bases_.count(), 0);
        EXPECT_EQ(merge_scn, after_meta.bases_.at(0).scn_);
      }
    }
    int64_t row_count = 0;
    ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql, "select count(*) val from t1", row_count));
    EXPECT_EQ((merge_first ? 3 : 2) * kFreezeRowsPerWorker, row_count);
    ASSERT_EQ(OB_SUCCESS, SSH::select_int64(sql,
        "select count(*) val from (select c1 from t1 order by cosine_distance(embedding, "
        "'[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]') approx limit 10) hits", row_count));
    EXPECT_EQ(10, row_count);
    ASSERT_EQ(OB_SUCCESS, sql.write("drop table t1", affected_rows));
  };
  run_direction(true);
  if (!HasFatalFailure()) {
    run_direction(false);
  }
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
