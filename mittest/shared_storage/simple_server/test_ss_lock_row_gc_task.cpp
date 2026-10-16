// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <set>
#include <thread>
#include <unistd.h>
#include <vector>
#define private public
#define protected public

#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_util.h"
#include "storage/ss_garbage_collector/dag/ob_ss_lock_row_gc_dag.h"
#include "storage/ss_garbage_collector/task/ob_ss_lock_row_gc_task.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_operator.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_page_iterator.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
using namespace unittest;
namespace storage
{
using namespace common;
using namespace share;

// Tests cover the LockRow GC Timer/controller lifecycle and DAG execution
// plane. The simple-server harness does not bind ObSSMetaService, so Executor
// and Barrier tests inject only MetaService results while retaining the real
// control-table transaction paths. Storage-backed and context-only cases use
// dedicated synthetic LS IDs to avoid interference with other tests.
class TestSSLockRowGCTask : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t SQL_TIMEOUT_US = 10L * 1000L * 1000L; // 10s
  TestSSLockRowGCTask()
    : unittest::ObSimpleClusterTestBase("test_ss_lock_row_gc_task") {}

  // Insert an LS_LOCK_ROW that marks the LS as visible to ObSSMetaCtrlLSIterator
  // (LSIterator filters meta_type = LS_LOCK_ROW).
  // is_mark_deleted controls whether GC is allowed to delete tablets under it.
  int insert_ls_lock_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const bool is_mark_deleted);

  // Insert a TABLET_LOCK_ROW under (tenant, ls). Pair fields meta_scn /
  // is_mark_deleted are observable via ObSSMetaCtrlLockRowValue.
  int insert_tablet_lock_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t tablet_id_int,
      const share::SCN &reorg_scn,
      const share::SCN &meta_scn,
      const bool is_mark_deleted,
      const blocksstable::ObStorageObjectType lock_type =
          blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK);

  int insert_tablet_meta_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &meta_scn);

  // Read back an ObSSMetaCtrlLockRowValue by (tenant, ls, key) for assertions.
  // Returns OB_ITER_END when the row has been deleted (read_row convention).
  int read_lock_row_value_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &key,
      ObSSMetaCtrlLockRowValue &out_value);

  int delete_lock_row_exact_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObSSMetaCtrlKey &key);

public:
  volatile bool task_stopped_ = false;
};

class ScriptedSSLockRowGCTimerTask : public ObSSLockRowGCTask
{
public:
  explicit ScriptedSSLockRowGCTimerTask(volatile bool &is_stopped)
    : ObSSLockRowGCTask(is_stopped),
      check_ret_(OB_SUCCESS),
      need_schedule_(true),
      check_ts_(100),
      submit_ret_(OB_SUCCESS),
      probe_ret_(OB_SUCCESS),
      probe_exist_(true),
      check_count_(0),
      submit_count_(0),
      probe_count_(0),
      submitted_tenant_id_(OB_INVALID_TENANT_ID),
      submitted_schedule_ts_(-1)
  {
  }

  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override
  {
    ++check_count_;
    need_schedule = need_schedule_;
    check_ts = check_ts_;
    return check_ret_;
  }

  virtual int submit_lock_row_gc_dag_(
      const ObSSLockRowGCDagParam &param) override
  {
    ++submit_count_;
    submitted_tenant_id_ = param.tenant_id_;
    submitted_schedule_ts_ = param.schedule_ts_;
    return submit_ret_;
  }

  virtual int check_lock_row_gc_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    exist = probe_exist_;
    return probe_ret_;
  }

  int check_ret_;
  bool need_schedule_;
  int64_t check_ts_;
  int submit_ret_;
  int probe_ret_;
  bool probe_exist_;
  int64_t check_count_;
  int64_t submit_count_;
  int64_t probe_count_;
  uint64_t submitted_tenant_id_;
  int64_t submitted_schedule_ts_;
};

class FakeClockSSLockRowGCTimerTask : public ObSSLockRowGCTask
{
public:
  explicit FakeClockSSLockRowGCTimerTask(volatile bool &is_stopped)
    : ObSSLockRowGCTask(is_stopped),
      eligibility_ret_(OB_SUCCESS),
      can_schedule_(true),
      check_time_us_(0),
      interval_us_(100)
  {
  }

  virtual int check_can_schedule_lock_row_gc_dag_(
      bool &can_schedule) const override
  {
    can_schedule = can_schedule_;
    return eligibility_ret_;
  }

  virtual int64_t get_gc_dag_schedule_check_time_us_() const override
  {
    return check_time_us_;
  }

  virtual void get_gc_exec_interval_us_(
      int64_t &exec_interval_us) const override
  {
    exec_interval_us = interval_us_;
  }

  int eligibility_ret_;
  bool can_schedule_;
  int64_t check_time_us_;
  int64_t interval_us_;
};

class DynamicIntervalSSLockRowGCTimerTask final
    : public FakeClockSSLockRowGCTimerTask
{
public:
  explicit DynamicIntervalSSLockRowGCTimerTask(
      volatile bool &is_stopped)
    : FakeClockSSLockRowGCTimerTask(is_stopped),
      probe_exist_(true),
      check_count_(0),
      submit_count_(0),
      probe_count_(0),
      submitted_schedule_ts_(-1)
  {
  }

  virtual int check_need_schedule_dag_(
      bool &need_schedule,
      int64_t &check_ts) override
  {
    ++check_count_;
    return ObSSLockRowGCTask::check_need_schedule_dag_(
        need_schedule, check_ts);
  }

  virtual int submit_lock_row_gc_dag_(
      const ObSSLockRowGCDagParam &param) override
  {
    ++submit_count_;
    submitted_schedule_ts_ = param.schedule_ts_;
    return OB_SUCCESS;
  }

  virtual int check_lock_row_gc_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    exist = probe_exist_;
    return OB_SUCCESS;
  }

  bool probe_exist_;
  int64_t check_count_;
  int64_t submit_count_;
  int64_t probe_count_;
  int64_t submitted_schedule_ts_;
};

int TestSSLockRowGCTask::insert_ls_lock_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const bool is_mark_deleted)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSLockGC");

  ObSSMetaCtrlKey ls_key;
  ls_key.type_ = blocksstable::ObStorageObjectType::SHARED_LS_LOCK;
  ls_key.ls_key_.first_id_ = ls_id.id();
  ls_key.ls_key_.second_id_ = 0;
  ls_key.ls_key_.control_scn_.set_min();

  ObSSMetaCtrlLockRowValue ls_value;
  ls_value.is_mark_deleted_ = is_mark_deleted;
  ls_value.op_id_ = 1;
  ls_value.meta_scn_.set_min();
  if (is_mark_deleted) {
    // is_valid() requires deleted_scn_ valid and last_major_snapshot_ >= 0
    // when is_mark_deleted_ is true.
    ls_value.deleted_scn_.set_min();
    ls_value.last_major_snapshot_ = 0;
  } else {
    ls_value.deleted_scn_.reset();
  }

  if (OB_UNLIKELY(!ls_key.is_valid() || !ls_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared ls lock key/value", K(ret), K(ls_key), K(ls_value));
  }

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  ObString meta_key_str;
  int64_t value_size = 0;
  int64_t value_pos = 0;
  char *value_buf = nullptr;
  ObString meta_value_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ls_key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret), K(ls_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ls_key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize ls lock key", K(ret));
  } else {
    meta_key_str.assign(key_buf, key_pos);
    value_size = ls_value.get_serialize_size();
    if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ls_value.serialize(value_buf, value_size, value_pos))) {
      LOG_WARN("failed to serialize ls lock value", K(ret), K(ls_value));
    } else {
      meta_value_str.assign(value_buf, value_pos);
    }
  }

  if (OB_SUCC(ret)) {
    ObSSMetaCtrlTableOperator op;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.insert_row(tenant_id, ls_id, ls_key.type_,
                                     meta_key_str, meta_value_str,
                                     affected_rows))) {
      LOG_WARN("failed to insert ls lock row", K(ret), K(tenant_id), K(ls_id),
          K(ls_key));
    }
  }
  return ret;
}

int TestSSLockRowGCTask::insert_tablet_lock_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t tablet_id_int,
    const share::SCN &reorg_scn,
    const share::SCN &meta_scn,
    const bool is_mark_deleted,
    const blocksstable::ObStorageObjectType lock_type)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSLockGC");

  ObSSMetaCtrlKey lock_key;
  lock_key.type_ = lock_type;
  lock_key.tablet_key_.tablet_id_ = ObTabletID(tablet_id_int);
  lock_key.tablet_key_.reorg_scn_ = reorg_scn;
  lock_key.tablet_key_.control_scn_.set_min();
  if (lock_key.tablet_key_.tablet_id_.is_ls_inner_tablet()) {
    lock_key.tablet_key_.ls_id_ = ls_id;
  }

  ObSSMetaCtrlLockRowValue lock_value;
  lock_value.is_mark_deleted_ = is_mark_deleted;
  lock_value.op_id_ = 1;
  lock_value.meta_scn_ = meta_scn;
  if (is_mark_deleted) {
    lock_value.deleted_scn_ = meta_scn;
    lock_value.last_major_snapshot_ = 0;
  } else {
    lock_value.deleted_scn_.reset();
  }

  if (OB_UNLIKELY(!lock_key.is_valid() || !lock_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared tablet lock key/value", K(ret), K(lock_key), K(lock_value));
  }

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  ObString meta_key_str;
  int64_t value_size = 0;
  int64_t value_pos = 0;
  char *value_buf = nullptr;
  ObString meta_value_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lock_key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret), K(lock_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(lock_key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize tablet lock key", K(ret));
  } else {
    meta_key_str.assign(key_buf, key_pos);
    value_size = lock_value.get_serialize_size();
    if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(lock_value.serialize(value_buf, value_size, value_pos))) {
      LOG_WARN("failed to serialize tablet lock value", K(ret), K(lock_value));
    } else {
      meta_value_str.assign(value_buf, value_pos);
    }
  }

  if (OB_SUCC(ret)) {
    ObSSMetaCtrlTableOperator op;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.insert_row(tenant_id, ls_id, lock_key.type_,
                                     meta_key_str, meta_value_str,
                                     affected_rows))) {
      LOG_WARN("failed to insert tablet lock row", K(ret), K(tenant_id), K(ls_id),
          K(lock_key));
    }
  }
  return ret;
}

int TestSSLockRowGCTask::insert_tablet_meta_row_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSLockGC");
  ObSSMetaCtrlKey meta_key(
      blocksstable::ObStorageObjectType::SHARED_TABLET_META,
      tablet_id, SCN(SCN::min_scn()), meta_scn, ls_id);

  ObSSMetaCtrlValue ctrl_value;
  ctrl_value.ctrl_info_.lease_epoch_ = 1;
  ctrl_value.ctrl_info_.op_id_ = 1;
  if (OB_FAIL(ctrl_value.ctrl_info_.status_.set_status_start())) {
    LOG_WARN("failed to set status start", K(ret));
  }
  ctrl_value.object_info_.set_none();

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  ObString meta_key_str;
  int64_t value_size = 0;
  int64_t value_pos = 0;
  char *value_buf = nullptr;
  ObString meta_value_str;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!meta_key.is_valid() || !ctrl_value.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid prepared tablet meta key/value", K(ret), K(meta_key),
        K(ctrl_value));
  } else if (OB_FAIL(meta_key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get tablet meta key size", K(ret), K(meta_key));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(meta_key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize tablet meta key", K(ret), K(meta_key));
  } else {
    meta_key_str.assign(key_buf, key_pos);
    value_size = ctrl_value.get_serialize_size();
    if (OB_ISNULL(value_buf = static_cast<char *>(allocator.alloc(value_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ctrl_value.serialize(
        value_buf, value_size, value_pos))) {
      LOG_WARN("failed to serialize ctrl value", K(ret), K(ctrl_value));
    } else {
      meta_value_str.assign(value_buf, value_pos);
    }
  }

  if (OB_SUCC(ret)) {
    ObSSMetaCtrlTableOperator op;
    int64_t affected_rows = 0;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.insert_row(tenant_id, ls_id, meta_key.type_,
                                     meta_key_str, meta_value_str,
                                     affected_rows))) {
      LOG_WARN("failed to insert tablet meta row", K(ret), K(tenant_id),
          K(ls_id), K(meta_key));
    }
  }
  return ret;
}

int TestSSLockRowGCTask::read_lock_row_value_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &key,
    ObSSMetaCtrlLockRowValue &out_value)
{
  int ret = OB_SUCCESS;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObArenaAllocator allocator("TestSSLockGC");
  out_value.reset();

  int64_t key_size = 0;
  int64_t key_pos = 0;
  char *key_buf = nullptr;
  if (OB_FAIL(key.get_meta_key_serialize_size(key_size))) {
    LOG_WARN("failed to get meta key size", K(ret));
  } else if (OB_ISNULL(key_buf = static_cast<char *>(allocator.alloc(key_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(key.serialize_meta_key(key_buf, key_size, key_pos))) {
    LOG_WARN("failed to serialize meta key", K(ret));
  } else {
    ObString meta_key_str(key_pos, key_buf);
    ObString meta_value_str;
    ObSSMetaCtrlTableOperator op;
    if (OB_FAIL(op.init(proxy, SQL_TIMEOUT_US))) {
      LOG_WARN("failed to init table operator", K(ret));
    } else if (OB_FAIL(op.read_row(tenant_id, ls_id, key.type_, meta_key_str,
                                   allocator, meta_value_str))) {
      // OB_ITER_END surfaces back to caller when the row is absent.
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to read row", K(ret), K(tenant_id), K(ls_id), K(key));
      }
    } else {
      int64_t des_pos = 0;
      if (OB_FAIL(out_value.deserialize(meta_value_str.ptr(),
                                        meta_value_str.length(), des_pos))) {
        LOG_WARN("failed to deserialize lock row value", K(ret));
      }
    }
  }
  return ret;
}

int TestSSLockRowGCTask::delete_lock_row_exact_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObSSMetaCtrlKey &key)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObArenaAllocator allocator("TestSSLockGC");
  ObString serialized_key;
  auto &proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ObSSMetaCtrlTableOperator table_operator;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid() || !key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exact lock row delete argument", K(ret), K(tenant_id),
        K(ls_id), K(key));
  } else if (OB_FAIL(ObSSGCDagUtil::serialize_meta_key(
      key, allocator, serialized_key))) {
    LOG_WARN("failed to serialize exact lock row key", K(ret), K(key));
  } else if (OB_FAIL(table_operator.init(proxy, SQL_TIMEOUT_US))) {
    LOG_WARN("failed to init table operator", K(ret), K(tenant_id));
  } else if (OB_FAIL(table_operator.delete_row(
      tenant_id, ls_id, key.type_, serialized_key,
      false/*prefix_delete*/, affected_rows))) {
    LOG_WARN("failed to delete exact lock row", K(ret), K(tenant_id),
        K(ls_id), K(key));
  } else if (0 != affected_rows && 1 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows for exact lock row delete", K(ret),
        K(tenant_id), K(ls_id), K(key), K(affected_rows));
  }
  return ret;
}

// Build an SCN from a nanosecond counter for tests; only ordering matters.
static share::SCN make_scn(const int64_t val_ns)
{
  share::SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_gts(val_ns));
  return scn;
}

static int get_lock_row_gc_finish_event(
    const int64_t begin_ts,
    const int64_t schedule_ts,
    int64_t &event_count,
    int64_t &dag_ret,
    int64_t &dag_status,
    int64_t &first_error_code,
    char *extra_info,
    const int64_t extra_info_len)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  event_count = 0;
  dag_ret = OB_ERR_UNEXPECTED;
  dag_status = -1;
  first_error_code = OB_ERR_UNEXPECTED;
  if (OB_ISNULL(extra_info) || extra_info_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(extra_info[0] = '\0')) {
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT COUNT(*) AS event_count FROM oceanbase.%s "
      "WHERE time_to_usec(gmt_create) >= %ld "
      "AND module = 'SHARED_GC' "
      "AND event = 'FINISH_LOCK_ROW_GC_DAG' "
      "AND name1 = 'tenant_id' AND value1 = '%lu' "
      "AND name2 = 'schedule_ts' AND value2 = '%ld'",
      OB_ALL_SERVER_EVENT_HISTORY_TNAME, begin_ts,
      OB_SYS_TENANT_ID, schedule_ts))) {
    LOG_WARN("fail to build lock row gc finish event count sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("fail to query lock row gc finish event count", KR(ret),
            K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to read lock row gc finish event count", KR(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(
            *result, "event_count", event_count, int64_t);
      }
    }
  }

  if (OB_SUCC(ret) && event_count > 0) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt(
        "SELECT CAST(value3 AS SIGNED) AS dag_ret, "
        "CAST(value4 AS SIGNED) AS dag_status, "
        "CAST(value6 AS SIGNED) AS first_error_code, extra_info "
        "FROM oceanbase.%s "
        "WHERE time_to_usec(gmt_create) >= %ld "
        "AND module = 'SHARED_GC' "
        "AND event = 'FINISH_LOCK_ROW_GC_DAG' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'schedule_ts' AND value2 = '%ld' "
        "ORDER BY gmt_create DESC LIMIT 1",
        OB_ALL_SERVER_EVENT_HISTORY_TNAME, begin_ts,
        OB_SYS_TENANT_ID, schedule_ts))) {
      LOG_WARN("fail to build lock row gc finish event sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = nullptr;
        int64_t real_len = 0;
        if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
          LOG_WARN("fail to query lock row gc finish event", KR(ret),
              K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to read lock row gc finish event", KR(ret));
        } else {
          EXTRACT_INT_FIELD_MYSQL(
              *result, "dag_ret", dag_ret, int64_t);
          EXTRACT_INT_FIELD_MYSQL(
              *result, "dag_status", dag_status, int64_t);
          EXTRACT_INT_FIELD_MYSQL(
              *result, "first_error_code", first_error_code, int64_t);
          EXTRACT_STRBUF_FIELD_MYSQL(
              *result, "extra_info", extra_info, extra_info_len, real_len);
        }
      }
    }
  }
  return ret;
}

struct LockRowGCDagTaskSnapshot final
{
  ObSSLockRowGCPrepareTask *prepare_task_ = nullptr;
  ObSSLockRowGCDeleteLSLockTask *barrier_task_ = nullptr;
  std::vector<ObSSLockRowGCScheduleTask *> schedule_tasks_;
  std::vector<ObSSLockRowGCExecutorTask *> executor_tasks_;
  int64_t recognized_task_count_ = 0;
};

static LockRowGCDagTaskSnapshot inspect_lock_row_gc_dag_tasks(
    ObSSLockRowGCDag &dag)
{
  LockRowGCDagTaskSnapshot snapshot;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSLockRowGCPrepareTask *>(task)) {
      snapshot.prepare_task_ = static_cast<ObSSLockRowGCPrepareTask *>(task);
      ++snapshot.recognized_task_count_;
    } else if (nullptr != dynamic_cast<ObSSLockRowGCScheduleTask *>(task)) {
      snapshot.schedule_tasks_.push_back(
          static_cast<ObSSLockRowGCScheduleTask *>(task));
      ++snapshot.recognized_task_count_;
    } else if (nullptr != dynamic_cast<ObSSLockRowGCExecutorTask *>(task)) {
      snapshot.executor_tasks_.push_back(
          static_cast<ObSSLockRowGCExecutorTask *>(task));
      ++snapshot.recognized_task_count_;
    } else if (nullptr
        != dynamic_cast<ObSSLockRowGCDeleteLSLockTask *>(task)) {
      snapshot.barrier_task_ =
          static_cast<ObSSLockRowGCDeleteLSLockTask *>(task);
      ++snapshot.recognized_task_count_;
    }
    task = task->get_next();
  }
  return snapshot;
}

static bool has_lock_row_gc_child(ObITask &parent, const ObITask &child)
{
  bool found = false;
  ObSEArray<ObINodeWithChild *, 8> children;
  if (OB_SUCCESS == parent.copy_child_nodes(children)) {
    for (int64_t i = 0; !found && i < children.count(); ++i) {
      found = children.at(i)
          == static_cast<const ObINodeWithChild *>(&child);
    }
  }
  return found;
}

static int init_lock_row_gc_dag_with_ls(
    ObSSLockRowGCDag &dag,
    const ObLSID &ls_id,
    ObSSLockRowGCDagCtx *&ctx,
    ObSSLockRowGCLSCtx *&ls_ctx)
{
  int ret = OB_SUCCESS;
  ObSSLockRowGCDagParam param;
  ctx = nullptr;
  ls_ctx = nullptr;
  if (OB_FAIL(param.init(OB_SYS_TENANT_ID))) {
  } else if (OB_FAIL(dag.init_by_param(&param))) {
  } else if (OB_FAIL(dag.create_first_task())) {
  } else if (OB_ISNULL(ctx = dag.get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ctx->sql_timeout_us_ = TestSSLockRowGCTask::SQL_TIMEOUT_US;
    if (OB_FAIL(ctx->alloc_ls_ctx(ls_id, ls_ctx))) {
    }
  }
  return ret;
}

static int set_lock_row_gc_ls_lock_value(
    ObSSLockRowGCLSCtx &ls_ctx,
    const bool is_mark_deleted)
{
  int ret = OB_SUCCESS;
  ls_ctx.ls_lock_row_value_.reset();
  ls_ctx.ls_lock_row_value_.is_mark_deleted_ = is_mark_deleted;
  ls_ctx.ls_lock_row_value_.op_id_ = 1;
  ls_ctx.ls_lock_row_value_.meta_scn_.set_min();
  if (is_mark_deleted) {
    ls_ctx.ls_lock_row_value_.deleted_scn_.set_min();
    ls_ctx.ls_lock_row_value_.last_major_snapshot_ = 0;
  } else {
    ls_ctx.ls_lock_row_value_.deleted_scn_.reset();
  }
  if (OB_UNLIKELY(!ls_ctx.ls_lock_row_value_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

static ObSSMetaCtrlKey make_tablet_lock_row_key(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorg_scn)
{
  return ObSSMetaCtrlKey(
      blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
      tablet_id, reorg_scn, SCN(SCN::min_scn()), ls_id);
}

struct LockRowGCCheckResult final
{
  LockRowGCCheckResult(const int ret, const bool can_delete)
    : ret_(ret), can_delete_(can_delete)
  {
  }

  int ret_;
  bool can_delete_;
};

class ScriptedSSLockRowGCExecutorTask final
    : public ObSSLockRowGCExecutorTask
{
public:
  ScriptedSSLockRowGCExecutorTask()
    : call_base_delete_(false),
      check_count_(0),
      delete_count_(0),
      check_results_(),
      delete_results_(),
      deleted_lock_row_counts_(),
      checked_tablet_ids_()
  {
  }

  bool call_base_delete_;
  int64_t check_count_;
  int64_t delete_count_;
  std::vector<LockRowGCCheckResult> check_results_;
  std::vector<int> delete_results_;
  std::vector<int64_t> deleted_lock_row_counts_;
  std::vector<uint64_t> checked_tablet_ids_;

protected:
  virtual int check_can_delete_tablet_lock_row_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      const ObSSGCTabletBaseInfo &tablet_info,
      bool &can_delete) override
  {
    UNUSED(ctx);
    UNUSED(tablet_lock_row_key);
    int ret = OB_SUCCESS;
    can_delete = false;
    checked_tablet_ids_.push_back(tablet_info.tablet_id_.id());
    if (check_count_ >= static_cast<int64_t>(check_results_.size())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const LockRowGCCheckResult &result = check_results_[check_count_];
      ret = result.ret_;
      can_delete = result.can_delete_;
    }
    ++check_count_;
    return ret;
  }

  virtual int delete_tablet_lock_rows_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      int64_t &deleted_lock_row_count) override
  {
    int ret = OB_SUCCESS;
    deleted_lock_row_count = 0;
    const int64_t delete_idx = delete_count_++;
    if (call_base_delete_) {
      ret = ObSSLockRowGCExecutorTask::delete_tablet_lock_rows_(
          ctx, tablet_lock_row_key, deleted_lock_row_count);
    }
    if (OB_SUCC(ret)
        && delete_idx < static_cast<int64_t>(delete_results_.size())) {
      ret = delete_results_.at(delete_idx);
    }
    if (OB_SUCC(ret) && !call_base_delete_
        && delete_idx
            < static_cast<int64_t>(deleted_lock_row_counts_.size())) {
      deleted_lock_row_count = deleted_lock_row_counts_.at(delete_idx);
    } else if (OB_SUCCESS != ret) {
      deleted_lock_row_count = 0;
    }
    return ret;
  }
};

struct LockRowGCMetaCheckResult final
{
  LockRowGCMetaCheckResult(const int ret, const bool ls_meta_exist)
    : ret_(ret), ls_meta_exist_(ls_meta_exist)
  {
  }

  int ret_;
  bool ls_meta_exist_;
};

class ScriptedSSLockRowGCDeleteLSLockTask final
    : public ObSSLockRowGCDeleteLSLockTask
{
public:
  ScriptedSSLockRowGCDeleteLSLockTask()
    : call_base_delete_(false),
      stop_after_delete_count_(0),
      check_count_(0),
      delete_count_(0),
      check_results_(),
      delete_results_(),
      deleted_lock_row_counts_(),
      checked_ls_ids_(),
      delete_attempt_ls_ids_()
  {
  }

  bool call_base_delete_;
  int64_t stop_after_delete_count_;
  int64_t check_count_;
  int64_t delete_count_;
  std::vector<LockRowGCMetaCheckResult> check_results_;
  std::vector<int> delete_results_;
  std::vector<int64_t> deleted_lock_row_counts_;
  std::vector<int64_t> checked_ls_ids_;
  std::vector<int64_t> delete_attempt_ls_ids_;

protected:
  virtual int check_ls_meta_exist_(
      const ObLSID &ls_id,
      bool &ls_meta_exist) override
  {
    int ret = OB_SUCCESS;
    ls_meta_exist = false;
    checked_ls_ids_.push_back(ls_id.id());
    if (check_count_ >= static_cast<int64_t>(check_results_.size())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const LockRowGCMetaCheckResult &result =
          check_results_.at(check_count_);
      ret = result.ret_;
      ls_meta_exist = result.ls_meta_exist_;
    }
    ++check_count_;
    return ret;
  }

  virtual int delete_ls_lock_row_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSLockRowGCLSCtx &ls_ctx,
      int64_t &deleted_lock_row_count) override
  {
    int ret = OB_SUCCESS;
    deleted_lock_row_count = 0;
    const int64_t delete_idx = delete_count_++;
    delete_attempt_ls_ids_.push_back(ls_ctx.ls_id_.id());
    if (call_base_delete_) {
      ret = ObSSLockRowGCDeleteLSLockTask::delete_ls_lock_row_(
          ctx, ls_ctx, deleted_lock_row_count);
    } else if (delete_idx >= static_cast<int64_t>(delete_results_.size())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = delete_results_.at(delete_idx);
    }
    if (OB_SUCC(ret) && !call_base_delete_
        && delete_idx
            < static_cast<int64_t>(deleted_lock_row_counts_.size())) {
      deleted_lock_row_count = deleted_lock_row_counts_.at(delete_idx);
    } else if (OB_SUCCESS != ret) {
      deleted_lock_row_count = 0;
    }
    if (stop_after_delete_count_ > 0
        && stop_after_delete_count_ == delete_count_) {
      ObIDag *dag = get_dag();
      if (OB_ISNULL(dag)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        dag->simply_set_stop();
      }
    }
    return ret;
  }
};

class LockRowGCTestSignal final
{
public:
  LockRowGCTestSignal() : signaled_(false) {}

  void signal()
  {
    {
      std::lock_guard<std::mutex> guard(lock_);
      signaled_ = true;
    }
    cond_.notify_all();
  }

  bool wait(const int64_t timeout_ms = 30 * 1000L)
  {
    std::unique_lock<std::mutex> guard(lock_);
    return cond_.wait_for(guard, std::chrono::milliseconds(timeout_ms),
        [this]() { return signaled_; });
  }

  bool is_signaled() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return signaled_;
  }

private:
  mutable std::mutex lock_;
  std::condition_variable cond_;
  bool signaled_;
};

class LockRowGCConcurrentDeleteBarrier final
{
public:
  LockRowGCConcurrentDeleteBarrier()
    : arrived_count_(0), released_(false), timed_out_(false)
  {
  }

  bool arrive_and_wait()
  {
    std::unique_lock<std::mutex> guard(lock_);
    ++arrived_count_;
    if (2 <= arrived_count_) {
      released_ = true;
      cond_.notify_all();
    } else if (!cond_.wait_for(guard, std::chrono::seconds(30),
        [this]() { return released_; })) {
      timed_out_ = true;
      released_ = true;
      cond_.notify_all();
    }
    return !timed_out_;
  }

  int64_t arrived_count() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return arrived_count_;
  }

  bool timed_out() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return timed_out_;
  }

private:
  mutable std::mutex lock_;
  std::condition_variable cond_;
  int64_t arrived_count_;
  bool released_;
  bool timed_out_;
};

class ConcurrentDeleteSSLockRowGCExecutorTask final
    : public ObSSLockRowGCExecutorTask
{
public:
  ConcurrentDeleteSSLockRowGCExecutorTask()
    : check_barrier_(nullptr), delete_barrier_(nullptr)
  {
  }

  void set_barriers(
      LockRowGCConcurrentDeleteBarrier &check_barrier,
      LockRowGCConcurrentDeleteBarrier &delete_barrier)
  {
    check_barrier_ = &check_barrier;
    delete_barrier_ = &delete_barrier;
  }

protected:
  virtual int check_can_delete_tablet_lock_row_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      const ObSSGCTabletBaseInfo &tablet_info,
      bool &can_delete) override
  {
    UNUSED(ctx);
    UNUSED(tablet_lock_row_key);
    UNUSED(tablet_info);
    int ret = OB_SUCCESS;
    can_delete = false;
    if (OB_ISNULL(check_barrier_)) {
      ret = OB_NOT_INIT;
    } else {
      can_delete = check_barrier_->arrive_and_wait();
      ret = can_delete ? OB_SUCCESS : OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  virtual int delete_tablet_lock_rows_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      int64_t &deleted_lock_row_count) override
  {
    int ret = OB_SUCCESS;
    deleted_lock_row_count = 0;
    if (OB_ISNULL(delete_barrier_)) {
      ret = OB_NOT_INIT;
    } else if (!delete_barrier_->arrive_and_wait()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = ObSSLockRowGCExecutorTask::delete_tablet_lock_rows_(
          ctx, tablet_lock_row_key, deleted_lock_row_count);
    }
    return ret;
  }

private:
  LockRowGCConcurrentDeleteBarrier *check_barrier_;
  LockRowGCConcurrentDeleteBarrier *delete_barrier_;
};

struct LockRowGCConcurrentTestState final
{
  LockRowGCConcurrentTestState()
    : arrived_count_(0),
      completed_count_(0),
      observed_completed_count_(-1),
      observed_aggregate_(true),
      both_entered_(),
      release_executors_(),
      barrier_entered_()
  {
  }

  std::atomic<int64_t> arrived_count_;
  std::atomic<int64_t> completed_count_;
  std::atomic<int64_t> observed_completed_count_;
  std::atomic<bool> observed_aggregate_;
  LockRowGCTestSignal both_entered_;
  LockRowGCTestSignal release_executors_;
  LockRowGCTestSignal barrier_entered_;
};

class ScopedLockRowGCDagAdaptiveLimit final
{
public:
  ScopedLockRowGCDagAdaptiveLimit(
      ObTenantDagScheduler &scheduler,
      const int64_t old_limit)
    : scheduler_(scheduler), old_limit_(old_limit), need_restore_(false)
  {
  }

  ~ScopedLockRowGCDagAdaptiveLimit()
  {
    if (need_restore_) {
      const int ret = scheduler_.set_adaptive_limit(
          ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit_);
      if (OB_SUCCESS != ret) {
        LOG_WARN("fail to restore lock row gc dag adaptive limit", KR(ret),
            K_(old_limit));
      }
      scheduler_.notify();
    }
  }

  int set(const int64_t new_limit)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(need_restore_)) {
      ret = OB_INIT_TWICE;
    } else if (OB_FAIL(scheduler_.set_adaptive_limit(
        ObDagPrio::DAG_PRIO_SS_GC_GEN, new_limit))) {
      LOG_WARN("fail to set lock row gc dag adaptive limit", KR(ret),
          K(new_limit));
    } else {
      need_restore_ = true;
    }
    return ret;
  }

private:
  ObTenantDagScheduler &scheduler_;
  const int64_t old_limit_;
  bool need_restore_;
  DISALLOW_COPY_AND_ASSIGN(ScopedLockRowGCDagAdaptiveLimit);
};

class ScopedUnsubmittedLockRowGCDag final
{
public:
  explicit ScopedUnsubmittedLockRowGCDag(ObTenantDagScheduler &scheduler)
    : scheduler_(scheduler), dag_(nullptr)
  {
  }

  ~ScopedUnsubmittedLockRowGCDag()
  {
    if (nullptr != dag_) {
      scheduler_.free_dag(*dag_);
    }
  }

  void hold(ObSSLockRowGCDag *dag) { dag_ = dag; }
  void release() { dag_ = nullptr; }

private:
  ObTenantDagScheduler &scheduler_;
  ObSSLockRowGCDag *dag_;
  DISALLOW_COPY_AND_ASSIGN(ScopedUnsubmittedLockRowGCDag);
};

class ConcurrentSSLockRowGCExecutorTask final
    : public ObSSLockRowGCExecutorTask
{
public:
  ConcurrentSSLockRowGCExecutorTask(
      const bool can_delete,
      std::atomic<int64_t> &arrived_count,
      LockRowGCTestSignal &both_entered,
      LockRowGCTestSignal &release,
      std::atomic<int64_t> &completed_count)
    : can_delete_(can_delete),
      arrived_count_(arrived_count),
      both_entered_(both_entered),
      release_(release),
      completed_count_(completed_count)
  {
  }

  virtual int process() override
  {
    const int ret = ObSSLockRowGCExecutorTask::process();
    (void)completed_count_.fetch_add(1, std::memory_order_release);
    return ret;
  }

protected:
  virtual int check_can_delete_tablet_lock_row_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      const ObSSGCTabletBaseInfo &tablet_info,
      bool &can_delete) override
  {
    UNUSED(ctx);
    UNUSED(tablet_lock_row_key);
    UNUSED(tablet_info);
    int ret = OB_SUCCESS;
    if (2 == arrived_count_.fetch_add(1, std::memory_order_acq_rel) + 1) {
      both_entered_.signal();
    }
    if (!release_.wait(60 * 1000L)) {
      ret = OB_TIMEOUT;
    } else {
      can_delete = can_delete_;
    }
    return ret;
  }

  virtual int delete_tablet_lock_rows_(
      const ObSSLockRowGCDagCtx &ctx,
      const ObSSMetaCtrlKey &tablet_lock_row_key,
      int64_t &deleted_lock_row_count) override
  {
    UNUSED(ctx);
    UNUSED(tablet_lock_row_key);
    deleted_lock_row_count = 1;
    return OB_SUCCESS;
  }

private:
  const bool can_delete_;
  std::atomic<int64_t> &arrived_count_;
  LockRowGCTestSignal &both_entered_;
  LockRowGCTestSignal &release_;
  std::atomic<int64_t> &completed_count_;
};

class ObservedSSLockRowGCBarrierTask final
    : public ObSSLockRowGCDeleteLSLockTask
{
public:
  ObservedSSLockRowGCBarrierTask(
      ObSSLockRowGCLSCtx &ls_ctx,
      const std::atomic<int64_t> &completed_count,
      std::atomic<int64_t> &observed_completed_count,
      std::atomic<bool> &observed_aggregate,
      LockRowGCTestSignal &entered)
    : ls_ctx_(ls_ctx),
      completed_count_(completed_count),
      observed_completed_count_(observed_completed_count),
      observed_aggregate_(observed_aggregate),
      entered_(entered)
  {
  }

  virtual int process() override
  {
    observed_completed_count_.store(
        completed_count_.load(std::memory_order_acquire),
        std::memory_order_release);
    observed_aggregate_.store(
        ls_ctx_.are_all_tablet_lock_rows_deleted(),
        std::memory_order_release);
    entered_.signal();
    return ObSSLockRowGCDeleteLSLockTask::process();
  }

private:
  ObSSLockRowGCLSCtx &ls_ctx_;
  const std::atomic<int64_t> &completed_count_;
  std::atomic<int64_t> &observed_completed_count_;
  std::atomic<bool> &observed_aggregate_;
  LockRowGCTestSignal &entered_;
};

static int make_lock_row_gc_ls_barrier_eligible(
    ObSSLockRowGCLSCtx &ls_ctx)
{
  int ret = set_lock_row_gc_ls_lock_value(ls_ctx, true);
  if (OB_SUCC(ret)) {
    ls_ctx.scan_finished_ = true;
    ls_ctx.scan_failed_ = false;
  }
  return ret;
}

// ============================================================================
// init / destroy
// ============================================================================

TEST_F(TestSSLockRowGCTask, init_invalid_tenant_id)
{
  ObSSLockRowGCTask task(task_stopped_);
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(OB_INVALID_TENANT_ID));
  ASSERT_FALSE(task.is_inited_);
}

TEST_F(TestSSLockRowGCTask, init_twice)
{
  ObSSLockRowGCTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());
  ASSERT_EQ(OB_INIT_TWICE, task.init(OB_SYS_TENANT_ID));
  ASSERT_TRUE(task.is_inited_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
  task.destroy();
}

// init() initializes the DAG scheduling control plane; destroy() resets it.
TEST_F(TestSSLockRowGCTask, init_and_destroy_reset_control_plane)
{
  ObSSLockRowGCTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  ASSERT_TRUE(task.is_inited_);
  ASSERT_EQ(0, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.commit_gc_dag_schedule_time_(100);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());
  task.destroy();

  ASSERT_FALSE(task.is_inited_);
  ASSERT_EQ(0, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_schedule_check_is_side_effect_free_with_fake_clock)
{
  FakeClockSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);

  bool need_schedule = true;
  int64_t check_ts = -1;
  task.check_time_us_ = 150;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(150, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.check_time_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(200, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.check_time_us_ = 250;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(250, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.can_schedule_ = false;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(0, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.can_schedule_ = true;
  task.eligibility_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(0, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.commit_gc_dag_schedule_time_(250);
  ASSERT_EQ(250, task.last_period_commit_time_us_);
}

TEST_F(TestSSLockRowGCTask,
       lock_row_schedule_check_reloads_dynamic_interval_without_commit)
{
  FakeClockSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  task.check_time_us_ = 250;

  bool need_schedule = true;
  int64_t check_ts = -1;
  task.interval_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(250, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  need_schedule = false;
  check_ts = -1;
  task.interval_us_ = 100;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(250, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  need_schedule = true;
  check_ts = -1;
  task.interval_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(250, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  need_schedule = false;
  check_ts = -1;
  task.check_time_us_ = 300;
  ASSERT_EQ(OB_SUCCESS,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_TRUE(need_schedule);
  ASSERT_EQ(300, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_timer_reloads_interval_after_dag_completion)
{
  DynamicIntervalSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());

  task.check_time_us_ = 200;
  task.probe_exist_ = true;
  task.runTimerTask();
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.interval_us_ = 300;
  task.check_time_us_ = 310;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_time_us_ = 320;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);

  task.interval_us_ = 200;
  task.check_time_us_ = 321;
  task.runTimerTask();
  ASSERT_EQ(2, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(321, task.submitted_schedule_ts_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.check_time_us_ = 330;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(2, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.interval_us_ = 100;
  task.check_time_us_ = 400;
  task.runTimerTask();
  ASSERT_EQ(3, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);

  task.interval_us_ = 300;
  task.check_time_us_ = 421;
  task.runTimerTask();
  ASSERT_EQ(4, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(321, task.last_period_commit_time_us_);

  task.check_time_us_ = 621;
  task.runTimerTask();
  ASSERT_EQ(5, task.check_count_);
  ASSERT_EQ(2, task.submit_count_);
  ASSERT_EQ(2, task.run_cnt_);
  ASSERT_EQ(621, task.submitted_schedule_ts_);
  ASSERT_EQ(621, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLockRowGCTask, lock_row_schedule_check_honors_entry_gate)
{
  FakeClockSSLockRowGCTimerTask task(task_stopped_);
  bool need_schedule = true;
  int64_t check_ts = -1;
  ASSERT_EQ(OB_NOT_INIT,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(0, check_ts);

  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  task_stopped_ = true;
  need_schedule = true;
  check_ts = -1;
  ASSERT_EQ(OB_SERVICE_STOPPED,
      task.check_need_schedule_dag_(need_schedule, check_ts));
  ASSERT_FALSE(need_schedule);
  ASSERT_EQ(0, check_ts);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  task_stopped_ = false;
}

TEST_F(TestSSLockRowGCTask,
       lock_row_try_schedule_commits_only_accepted_submission)
{
  ScriptedSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(10);

  task.need_schedule_ = false;
  ASSERT_EQ(OB_SUCCESS, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);

  task.need_schedule_ = true;
  task.check_ret_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);

  task.check_ret_ = OB_SUCCESS;
  task.check_ts_ = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);

  task.check_ts_ = 30;
  task.submit_ret_ = OB_SIZE_OVERFLOW;
  ASSERT_EQ(OB_SIZE_OVERFLOW, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 40;
  task.submit_ret_ = OB_TASK_EXIST;
  ASSERT_EQ(OB_TASK_EXIST, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(2, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 50;
  task.submit_ret_ = OB_EAGAIN;
  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = false;
  ASSERT_EQ(OB_EAGAIN, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(3, task.submit_count_);
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 60;
  task.probe_ret_ = OB_TIMEOUT;
  task.probe_exist_ = true;
  ASSERT_EQ(OB_TIMEOUT, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(4, task.submit_count_);
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 70;
  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = true;
  ASSERT_EQ(OB_SUCCESS, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(70, task.last_period_commit_time_us_);
  ASSERT_EQ(5, task.submit_count_);
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(5, task.run_cnt_);
  ASSERT_EQ(OB_SYS_TENANT_ID, task.submitted_tenant_id_);
  ASSERT_EQ(70, task.submitted_schedule_ts_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_try_schedule_commits_success_without_probe)
{
  ScriptedSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(10);
  task.check_ts_ = 80;
  task.submit_ret_ = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(80, task.last_period_commit_time_us_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(0, task.probe_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(OB_SYS_TENANT_ID, task.submitted_tenant_id_);
  ASSERT_EQ(80, task.submitted_schedule_ts_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_timer_waits_for_dag_and_separates_finish_tick)
{
  ScriptedSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(100);
  ASSERT_EQ(OB_SUCCESS,
      task.dag_schedule_controller_.switch_to_wait_for_dag_finish());

  task.probe_ret_ = OB_TIMEOUT;
  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(1, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.probe_ret_ = OB_SUCCESS;
  task.probe_exist_ = true;
  task.runTimerTask();
  ASSERT_EQ(2, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task.probe_exist_ = false;
  task.runTimerTask();
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(0, task.check_count_);
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(100, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());

  task.check_ts_ = 320;
  task.need_schedule_ = true;
  task.submit_ret_ = OB_SUCCESS;
  task.runTimerTask();
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(320, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());

  task_stopped_ = true;
  task.runTimerTask();
  ASSERT_EQ(3, task.probe_count_);
  ASSERT_EQ(1, task.check_count_);
  ASSERT_EQ(1, task.submit_count_);
  ASSERT_EQ(1, task.run_cnt_);
  ASSERT_EQ(320, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      task.dag_schedule_controller_.state());
  task_stopped_ = false;
}

TEST_F(TestSSLockRowGCTask,
       lock_row_try_schedule_checks_stop_before_submission)
{
  ScriptedSSLockRowGCTimerTask task(task_stopped_);
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID));
  task.commit_gc_dag_schedule_time_(10);
  task.check_ts_ = 100;
  task_stopped_ = true;

  ASSERT_EQ(OB_SERVICE_STOPPED, task.try_schedule_lock_row_gc_dag_());
  ASSERT_EQ(0, task.submit_count_);
  ASSERT_EQ(0, task.run_cnt_);
  ASSERT_EQ(10, task.last_period_commit_time_us_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_NEXT_SCHEDULE,
      task.dag_schedule_controller_.state());
}

TEST_F(TestSSLockRowGCTask, lock_row_gc_scheduler_registration)
{
  ASSERT_STREQ("SS_LOCK_ROW_GC",
      ObITask::ObITaskTypeStr[ObITask::TASK_TYPE_SS_LOCK_ROW_GC]);
  ASSERT_STREQ("SS_LOCK_ROW_GC",
      ObIDag::get_dag_type_str(ObDagType::DAG_TYPE_SS_LOCK_ROW_GC));
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_LOCK_ROW_GC].init_dag_prio_);
  ASSERT_EQ(ObSysTaskType::SS_LOCK_ROW_GC_TASK,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_LOCK_ROW_GC].sys_task_type_);
  ASSERT_STREQ("SS_LOCK_ROW_GC_TASK",
      sys_task_type_to_str(ObSysTaskType::SS_LOCK_ROW_GC_TASK));
}

TEST_F(TestSSLockRowGCTask, lock_row_gc_context_atomic_contracts)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSLockRowGCDagCtx dag_ctx;
  ASSERT_EQ(OB_INVALID_ARGUMENT, dag_ctx.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag_ctx.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_INIT_TWICE, dag_ctx.init(OB_SYS_TENANT_ID));

  ObSSLockRowGCLSCtx *first_ls_ctx = nullptr;
  ObSSLockRowGCLSCtx *second_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      dag_ctx.alloc_ls_ctx(ObLSID(91), first_ls_ctx));
  ASSERT_EQ(OB_SUCCESS,
      dag_ctx.alloc_ls_ctx(ObLSID(92), second_ls_ctx));
  ASSERT_NE(nullptr, first_ls_ctx);
  ASSERT_NE(nullptr, second_ls_ctx);
  ASSERT_TRUE(first_ls_ctx->is_valid());
  ASSERT_TRUE(second_ls_ctx->is_valid());
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_LOCK,
      first_ls_ctx->ls_lock_row_key_.type_);
  ASSERT_EQ(ObLSID(91), first_ls_ctx->ls_id_);
  ASSERT_EQ(91, first_ls_ctx->ls_lock_row_key_.ls_key_.first_id_);
  ASSERT_EQ(0, first_ls_ctx->ls_lock_row_key_.ls_key_.second_id_);
  ASSERT_TRUE(first_ls_ctx->ls_lock_row_key_.ls_key_.control_scn_.is_min());
  ASSERT_EQ(2, dag_ctx.get_ls_count());
  ASSERT_EQ(0, dag_ctx.get_failed_ls_count());
  ASSERT_EQ(OB_SUCCESS, dag_ctx.get_first_error_code());
  ASSERT_EQ(0, dag_ctx.get_scanned_tablet_count());
  ASSERT_EQ(0, dag_ctx.get_deleted_tablet_lock_row_count());
  ASSERT_EQ(0, dag_ctx.get_deleted_ls_lock_row_count());
  ASSERT_TRUE(first_ls_ctx->are_all_tablet_lock_rows_deleted());

  const int64_t thread_count = 16;
  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start_record(false);
  std::atomic<int64_t> winner_count(0);
  std::vector<std::thread> workers;
  workers.reserve(thread_count);
  for (int64_t i = 0; i < thread_count; ++i) {
    workers.emplace_back([&, i]() {
      ready_count.fetch_add(1, std::memory_order_acq_rel);
      while (!start_record.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      const int error_code = 0 == i % 2 ? OB_TIMEOUT : OB_TRANS_TIMEOUT;
      if (dag_ctx.record_error_code(*first_ls_ctx, error_code)) {
        winner_count.fetch_add(1, std::memory_order_relaxed);
      }
      dag_ctx.add_scanned_tablet_count(1);
      dag_ctx.add_deleted_tablet_lock_row_count(1);
      dag_ctx.add_deleted_ls_lock_row_count(1);
      first_ls_ctx->mark_tablet_lock_rows_not_all_deleted();
    });
  }
  while (thread_count != ready_count.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  start_record.store(true, std::memory_order_release);
  for (std::thread &worker : workers) {
    worker.join();
  }

  ASSERT_EQ(1, winner_count.load(std::memory_order_relaxed));
  ASSERT_TRUE(OB_TIMEOUT == first_ls_ctx->get_first_error_code()
      || OB_TRANS_TIMEOUT == first_ls_ctx->get_first_error_code());
  ASSERT_EQ(first_ls_ctx->get_first_error_code(),
      dag_ctx.get_first_error_code());
  ASSERT_EQ(1, dag_ctx.get_failed_ls_count());
  ASSERT_EQ(thread_count, dag_ctx.get_scanned_tablet_count());
  ASSERT_EQ(thread_count, dag_ctx.get_deleted_tablet_lock_row_count());
  ASSERT_EQ(thread_count, dag_ctx.get_deleted_ls_lock_row_count());
  ASSERT_FALSE(first_ls_ctx->are_all_tablet_lock_rows_deleted());

  const int dag_first_error_code = dag_ctx.get_first_error_code();
  ASSERT_TRUE(dag_ctx.record_error_code(
      *second_ls_ctx, OB_TRANS_STMT_TIMEOUT));
  ASSERT_FALSE(dag_ctx.record_error_code(*second_ls_ctx, OB_TIMEOUT));
  ASSERT_EQ(dag_first_error_code, dag_ctx.get_first_error_code());
  ASSERT_EQ(2, dag_ctx.get_failed_ls_count());

  ObSSLockRowGCLSCtx unowned_ls_ctx;
  unowned_ls_ctx.ls_id_ = ObLSID(93);
  unowned_ls_ctx.ls_lock_row_key_.type_ =
      blocksstable::ObStorageObjectType::SHARED_LS_LOCK;
  unowned_ls_ctx.ls_lock_row_key_.ls_key_.first_id_ = 93;
  unowned_ls_ctx.ls_lock_row_key_.ls_key_.second_id_ = 0;
  unowned_ls_ctx.ls_lock_row_key_.ls_key_.control_scn_.set_min();
  ASSERT_TRUE(unowned_ls_ctx.is_valid());
  ASSERT_FALSE(dag_ctx.record_error_code(unowned_ls_ctx, OB_TIMEOUT));
  ASSERT_EQ(2, dag_ctx.get_failed_ls_count());
  unowned_ls_ctx.mark_tablet_lock_rows_not_all_deleted();
  ASSERT_FALSE(unowned_ls_ctx.are_all_tablet_lock_rows_deleted());
  unowned_ls_ctx.reset();
  ASSERT_FALSE(unowned_ls_ctx.is_valid());
  ASSERT_TRUE(unowned_ls_ctx.are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, unowned_ls_ctx.get_first_error_code());

  dag_ctx.reset();
  ASSERT_FALSE(dag_ctx.is_valid());
  ASSERT_EQ(0, dag_ctx.get_ls_count());
  ASSERT_EQ(0, dag_ctx.get_failed_ls_count());
  ASSERT_EQ(OB_SUCCESS, dag_ctx.get_first_error_code());
  ASSERT_EQ(0, dag_ctx.get_scanned_tablet_count());
  ASSERT_EQ(0, dag_ctx.get_deleted_tablet_lock_row_count());
  ASSERT_EQ(0, dag_ctx.get_deleted_ls_lock_row_count());
}

TEST_F(TestSSLockRowGCTask, lock_row_gc_dag_param_and_identity_contracts)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSLockRowGCDagParam invalid_schedule_param;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      invalid_schedule_param.init(OB_SYS_TENANT_ID, -1));
  ObSSLockRowGCDagParam first_param;
  ASSERT_FALSE(first_param.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      first_param.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, first_param.init(OB_SYS_TENANT_ID, 123));
  ASSERT_TRUE(first_param.is_valid());
  ASSERT_EQ(123, first_param.schedule_ts_);
  ASSERT_EQ(OB_INIT_TWICE, first_param.init(OB_SYS_TENANT_ID));

  ObSSLockRowGCDagParam second_param;
  ASSERT_EQ(OB_SUCCESS, second_param.init(OB_SYS_TENANT_ID, 456));
  ObArenaAllocator first_allocator("LockDagFirst");
  ObArenaAllocator second_allocator("LockDagSecond");
  ObSSLockRowGCDag first_dag;
  ObSSLockRowGCDag second_dag;
  ASSERT_EQ(OB_SUCCESS, first_dag.basic_init(first_allocator));
  ASSERT_EQ(OB_SUCCESS, second_dag.basic_init(second_allocator));
  ASSERT_EQ(OB_SUCCESS, first_dag.init_by_param(&first_param));
  ASSERT_EQ(OB_SUCCESS, second_dag.init_by_param(&second_param));
  ASSERT_EQ(OB_INIT_TWICE, first_dag.init_by_param(&first_param));
  ASSERT_EQ(123, first_dag.get_schedule_ts());
  ASSERT_EQ(456, second_dag.get_schedule_ts());

  ObSSLockRowGCDag fresh_probe;
  ObSSLockRowGCDag other_tenant_probe;
  fresh_probe.set_tenant_id(OB_SYS_TENANT_ID);
  other_tenant_probe.set_tenant_id(1002);
  ASSERT_EQ(nullptr, fresh_probe.get_ctx());
  ASSERT_EQ(ObDagType::DAG_TYPE_SS_LOCK_ROW_GC, first_dag.get_type());
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN, first_dag.get_priority());
  ASSERT_TRUE(first_dag == second_dag);
  ASSERT_EQ(first_dag.hash(), second_dag.hash());
  ASSERT_TRUE(first_dag == fresh_probe);
  ASSERT_TRUE(fresh_probe == first_dag);
  ASSERT_EQ(first_dag.hash(), fresh_probe.hash());
  ASSERT_FALSE(first_dag == other_tenant_probe);

  ObSSLockRowGCDagCtx *first_ctx = first_dag.get_ctx();
  ASSERT_NE(nullptr, first_ctx);
  first_ctx->add_scanned_tablet_count(7);
  first_ctx->add_deleted_tablet_lock_row_count(3);
  first_ctx->add_deleted_ls_lock_row_count(2);

  char dag_key[128] = {0};
  ASSERT_EQ(OB_SUCCESS,
      fresh_probe.fill_dag_key(dag_key, sizeof(dag_key)));
  ASSERT_NE(nullptr, std::strstr(dag_key, "tenant_id=1"));

  compaction::ObInfoParamBuffer info_allocator;
  compaction::ObIBasicInfoParam *info_param = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      first_dag.fill_info_param(info_param, info_allocator));
  ASSERT_NE(nullptr, info_param);
  char comment[common::OB_DAG_WARNING_INFO_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, info_param->fill_comment(comment, sizeof(comment)));
  ASSERT_NE(nullptr, std::strstr(comment, "tenant_id"));
  ASSERT_NE(nullptr, std::strstr(comment, "server_id"));
  ASSERT_NE(nullptr, std::strstr(comment, "ls_count"));
  ASSERT_NE(nullptr, std::strstr(comment, "first_error_code"));
  ASSERT_NE(nullptr, std::strstr(comment, "failed_ls_count"));
  ASSERT_NE(nullptr, std::strstr(comment, "server_addr"));
  ASSERT_NE(nullptr, std::strstr(comment, "row_stat"));
  ASSERT_NE(nullptr, std::strstr(comment, "scanned=7"));
  ASSERT_NE(nullptr,
      std::strstr(comment, "tablet_lock_rows_deleted=3"));
  ASSERT_NE(nullptr, std::strstr(comment, "ls_lock_rows_deleted=2"));

  first_dag.report_start_event();
  first_dag.report_start_event();
  ASSERT_EQ(1, ATOMIC_LOAD(&first_dag.start_event_reported_));
  first_dag.set_dag_ret(OB_TIMEOUT);
  ASSERT_EQ(OB_SUCCESS, first_dag.report_result());
  ASSERT_EQ(OB_TIMEOUT, first_dag.get_dag_ret());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_dag_static_prepare_schedule_barrier_topology)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockDagGraph");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ASSERT_EQ(OB_NOT_INIT, dag.create_first_task());
  ASSERT_EQ(0, dag.get_task_list_count());

  ObSSLockRowGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ASSERT_EQ(3, dag.get_task_list_count());

  ObSSLockRowGCPrepareTask *prepare_task = nullptr;
  ObSSLockRowGCScheduleTask *schedule_task = nullptr;
  ObSSLockRowGCDeleteLSLockTask *delete_task = nullptr;
  int64_t recognized_task_count = 0;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    ASSERT_EQ(&dag, task->get_dag());
    ASSERT_EQ(ObITask::TASK_STATUS_WAITING, task->get_status());
    ASSERT_EQ(ObITask::TASK_TYPE_SS_LOCK_ROW_GC, task->get_type());
    if (nullptr != dynamic_cast<ObSSLockRowGCPrepareTask *>(task)) {
      ASSERT_EQ(nullptr, prepare_task);
      prepare_task = static_cast<ObSSLockRowGCPrepareTask *>(task);
      ++recognized_task_count;
    } else if (nullptr != dynamic_cast<ObSSLockRowGCScheduleTask *>(task)) {
      ASSERT_EQ(nullptr, schedule_task);
      schedule_task = static_cast<ObSSLockRowGCScheduleTask *>(task);
      ++recognized_task_count;
    } else if (nullptr
        != dynamic_cast<ObSSLockRowGCDeleteLSLockTask *>(task)) {
      ASSERT_EQ(nullptr, delete_task);
      delete_task = static_cast<ObSSLockRowGCDeleteLSLockTask *>(task);
      ++recognized_task_count;
    }
    task = task->get_next();
  }
  ASSERT_EQ(3, recognized_task_count);
  ASSERT_EQ(dag.get_task_list_count(), recognized_task_count);
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_NE(nullptr, schedule_task);
  ASSERT_NE(nullptr, delete_task);
  ASSERT_TRUE(prepare_task->is_inited_);
  ASSERT_TRUE(schedule_task->is_inited_);
  ASSERT_TRUE(delete_task->is_inited_);
  ASSERT_EQ(0, schedule_task->ls_idx_);
  ASSERT_TRUE(schedule_task->is_ls_head_);
  ASSERT_EQ(nullptr, schedule_task->ls_ctx_);

  ASSERT_EQ(0, prepare_task->get_indegree());
  ASSERT_EQ(1, schedule_task->get_indegree());
  ASSERT_EQ(1, delete_task->get_indegree());
  ObSEArray<ObINodeWithChild *, 2> children;
  ASSERT_EQ(OB_SUCCESS, prepare_task->copy_child_nodes(children));
  ASSERT_EQ(1, children.count());
  ASSERT_EQ(static_cast<ObINodeWithChild *>(schedule_task), children.at(0));
  children.reset();
  ASSERT_EQ(OB_SUCCESS, schedule_task->copy_child_nodes(children));
  ASSERT_EQ(1, children.count());
  ASSERT_EQ(static_cast<ObINodeWithChild *>(delete_task), children.at(0));
  children.reset();
  ASSERT_EQ(OB_SUCCESS, delete_task->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_empty_and_multiple_ls_heads_preserve_barrier)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator empty_allocator("LockDagEmpty");
  ObSSLockRowGCDag empty_dag;
  ASSERT_EQ(OB_SUCCESS, empty_dag.basic_init(empty_allocator));
  ObSSLockRowGCDagParam empty_param;
  ASSERT_EQ(OB_SUCCESS, empty_param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, empty_dag.init_by_param(&empty_param));
  ASSERT_EQ(OB_SUCCESS, empty_dag.create_first_task());
  ObSSLockRowGCPrepareTask *empty_prepare = nullptr;
  ObSSLockRowGCScheduleTask *empty_schedule = nullptr;
  ObSSLockRowGCDeleteLSLockTask *empty_delete = nullptr;
  ObITask *task = empty_dag.task_list_.get_first();
  const ObITask *empty_list_head = empty_dag.task_list_.get_header();
  while (nullptr != task && empty_list_head != task) {
    if (nullptr != dynamic_cast<ObSSLockRowGCPrepareTask *>(task)) {
      empty_prepare = static_cast<ObSSLockRowGCPrepareTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSLockRowGCScheduleTask *>(task)) {
      empty_schedule = static_cast<ObSSLockRowGCScheduleTask *>(task);
    } else if (nullptr
        != dynamic_cast<ObSSLockRowGCDeleteLSLockTask *>(task)) {
      empty_delete = static_cast<ObSSLockRowGCDeleteLSLockTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, empty_prepare);
  ASSERT_NE(nullptr, empty_schedule);
  ASSERT_NE(nullptr, empty_delete);
  ObArray<ObLSID> empty_ls_ids;
  ASSERT_EQ(OB_SUCCESS,
      empty_prepare->alloc_ls_ctxs_(*empty_dag.get_ctx(), empty_ls_ids));
  ASSERT_EQ(0, empty_dag.get_ctx()->get_ls_count());
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(empty_schedule)->generate_next_task());
  ASSERT_EQ(3, empty_dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, empty_schedule->process());
  ASSERT_EQ(nullptr, empty_schedule->ls_ctx_);
  ASSERT_EQ(OB_SUCCESS, empty_delete->process());

  ObArenaAllocator multi_allocator("LockDagMulti");
  ObSSLockRowGCDag multi_dag;
  ASSERT_EQ(OB_SUCCESS, multi_dag.basic_init(multi_allocator));
  ObSSLockRowGCDagParam multi_param;
  ASSERT_EQ(OB_SUCCESS, multi_param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, multi_dag.init_by_param(&multi_param));
  ASSERT_EQ(OB_SUCCESS, multi_dag.create_first_task());
  ObSSLockRowGCPrepareTask *multi_prepare = nullptr;
  ObSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ObSSLockRowGCScheduleTask *heads[3] = {nullptr, nullptr, nullptr};
  task = multi_dag.task_list_.get_first();
  const ObITask *multi_list_head = multi_dag.task_list_.get_header();
  while (nullptr != task && multi_list_head != task) {
    if (nullptr != dynamic_cast<ObSSLockRowGCPrepareTask *>(task)) {
      multi_prepare = static_cast<ObSSLockRowGCPrepareTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSLockRowGCScheduleTask *>(task)) {
      heads[0] = static_cast<ObSSLockRowGCScheduleTask *>(task);
    } else if (nullptr
        != dynamic_cast<ObSSLockRowGCDeleteLSLockTask *>(task)) {
      barrier = static_cast<ObSSLockRowGCDeleteLSLockTask *>(task);
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, multi_prepare);
  ASSERT_NE(nullptr, heads[0]);
  ASSERT_NE(nullptr, barrier);

  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(94)));
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(95)));
  ASSERT_EQ(OB_SUCCESS, ls_ids.push_back(ObLSID(96)));
  ASSERT_EQ(OB_SUCCESS,
      multi_prepare->alloc_ls_ctxs_(*multi_dag.get_ctx(), ls_ids));
  ASSERT_EQ(3, multi_dag.get_ctx()->get_ls_count());
  for (int64_t i = 0; i < 3; ++i) {
    ObSSLockRowGCLSCtx *ls_ctx = multi_dag.get_ctx()->ls_ctxs_.at(i);
    ASSERT_NE(nullptr, ls_ctx);
    ASSERT_EQ(ls_ids.at(i), ls_ctx->ls_id_);
    ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_LS_LOCK,
        ls_ctx->ls_lock_row_key_.type_);
    ASSERT_EQ(ls_ids.at(i).id(),
        ls_ctx->ls_lock_row_key_.ls_key_.first_id_);
    ASSERT_EQ(0, ls_ctx->ls_lock_row_key_.ls_key_.second_id_);
    ASSERT_TRUE(ls_ctx->ls_lock_row_key_.ls_key_.control_scn_.is_min());
    ASSERT_EQ(0, ls_ctx->ls_lock_row_value_.op_id_);
    ASSERT_FALSE(ls_ctx->iterator_inited_);
    ASSERT_FALSE(ls_ctx->scan_finished_);
    ASSERT_FALSE(ls_ctx->scan_failed_);
    ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
    ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  }

  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, heads[i]);
    const int64_t old_task_count = multi_dag.get_task_list_count();
    ASSERT_EQ(OB_SUCCESS,
        static_cast<ObITask *>(heads[i])->generate_next_task());
    ASSERT_EQ(old_task_count + (i < 2 ? 1 : 0),
        multi_dag.get_task_list_count());
    task = multi_dag.task_list_.get_first();
    while (nullptr != task && multi_list_head != task) {
      if (nullptr != dynamic_cast<ObSSLockRowGCScheduleTask *>(task)) {
        ObSSLockRowGCScheduleTask *head =
            static_cast<ObSSLockRowGCScheduleTask *>(task);
        ASSERT_GE(head->ls_idx_, 0);
        ASSERT_LT(head->ls_idx_, 3);
        heads[head->ls_idx_] = head;
      }
      task = task->get_next();
    }
  }
  ASSERT_EQ(5, multi_dag.get_task_list_count());
  ASSERT_EQ(3, barrier->get_indegree());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, heads[i]);
    ASSERT_EQ(i, heads[i]->ls_idx_);
    ASSERT_TRUE(heads[i]->is_ls_head_);
    ASSERT_EQ(0 == i ? 1 : 0, heads[i]->get_indegree());
    ObSEArray<ObINodeWithChild *, 2> children;
    ASSERT_EQ(OB_SUCCESS, heads[i]->copy_child_nodes(children));
    ASSERT_EQ(1, children.count());
    ASSERT_EQ(static_cast<ObINodeWithChild *>(barrier), children.at(0));
    ASSERT_EQ(OB_SUCCESS, heads[i]->bind_ls_ctx_(*multi_dag.get_ctx()));
    ASSERT_EQ(multi_dag.get_ctx()->ls_ctxs_.at(i), heads[i]->ls_ctx_);
  }
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_ls_head_generate_next_is_idempotent_after_dispatch_retry)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator dag_allocator("LockDagGenRetry");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *first_ls_ctx = nullptr;
  ObSSLockRowGCLSCtx *second_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(131), ctx, first_ls_ctx));
  ASSERT_NE(nullptr, ctx);
  ASSERT_NE(nullptr, first_ls_ctx);
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(132), second_ls_ctx));
  ASSERT_NE(nullptr, second_ls_ctx);

  const LockRowGCDagTaskSnapshot initial =
      inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, initial.schedule_tasks_.size());
  ASSERT_NE(nullptr, initial.barrier_task_);
  ObSSLockRowGCScheduleTask *head = initial.schedule_tasks_.at(0);
  ASSERT_NE(nullptr, head);
  ASSERT_TRUE(head->is_ls_head_);
  ASSERT_EQ(0, head->ls_idx_);
  ASSERT_EQ(ObITask::TASK_STATUS_WAITING, head->get_status());

  const int64_t initial_task_count = dag.get_task_list_count();
  const int64_t initial_barrier_indegree =
      initial.barrier_task_->get_indegree();
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(head)->generate_next_task());
  ASSERT_EQ(initial_task_count + 1, dag.get_task_list_count());

  LockRowGCDagTaskSnapshot after_first_generate =
      inspect_lock_row_gc_dag_tasks(dag);
  ObSSLockRowGCScheduleTask *generated_head = nullptr;
  int64_t generated_head_count = 0;
  for (ObSSLockRowGCScheduleTask *schedule_task :
       after_first_generate.schedule_tasks_) {
    ASSERT_NE(nullptr, schedule_task);
    if (1 == schedule_task->ls_idx_ && schedule_task->is_ls_head_) {
      generated_head = schedule_task;
      ++generated_head_count;
    }
  }
  ASSERT_EQ(1, generated_head_count);
  ASSERT_NE(nullptr, generated_head);
  ASSERT_EQ(initial_barrier_indegree + 1,
      initial.barrier_task_->get_indegree());

  // dispatch 失败后 schedule_one_ 会将原任务恢复为 WAITING，下一次调度时
  // 会再次调用同一个 generate_next_task() 包装函数。
  head->set_status(ObITask::TASK_STATUS_WAITING);
  ASSERT_EQ(OB_SUCCESS,
      static_cast<ObITask *>(head)->generate_next_task());
  ASSERT_EQ(initial_task_count + 1, dag.get_task_list_count());

  const LockRowGCDagTaskSnapshot after_dispatch_retry =
      inspect_lock_row_gc_dag_tasks(dag);
  generated_head_count = 0;
  ObSSLockRowGCScheduleTask *retried_generated_head = nullptr;
  for (ObSSLockRowGCScheduleTask *schedule_task :
       after_dispatch_retry.schedule_tasks_) {
    ASSERT_NE(nullptr, schedule_task);
    if (1 == schedule_task->ls_idx_ && schedule_task->is_ls_head_) {
      retried_generated_head = schedule_task;
      ++generated_head_count;
    }
  }
  ASSERT_EQ(1, generated_head_count);
  ASSERT_EQ(generated_head, retried_generated_head);
  ASSERT_EQ(initial_barrier_indegree + 1,
      initial.barrier_task_->get_indegree());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_prepare_populates_context_without_eager_ls_queries)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID first_expected_ls(97);
  const ObLSID second_expected_ls(98);
  ASSERT_EQ(OB_SUCCESS,
      insert_ls_lock_row_(OB_SYS_TENANT_ID, first_expected_ls, false));
  ASSERT_EQ(OB_SUCCESS,
      insert_ls_lock_row_(OB_SYS_TENANT_ID, second_expected_ls, true));

  ObArenaAllocator dag_allocator("LockDagPrepare");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagParam param;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ObSSLockRowGCPrepareTask *prepare_task = nullptr;
  int64_t schedule_task_count = 0;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    if (nullptr != dynamic_cast<ObSSLockRowGCPrepareTask *>(task)) {
      prepare_task = static_cast<ObSSLockRowGCPrepareTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSLockRowGCScheduleTask *>(task)) {
      ++schedule_task_count;
    }
    task = task->get_next();
  }
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_EQ(1, schedule_task_count);
  ASSERT_EQ(OB_SUCCESS, prepare_task->process());
  ASSERT_EQ(1, ATOMIC_LOAD(&dag.start_event_reported_));

  ObSSLockRowGCDagCtx *ctx = dag.get_ctx();
  ASSERT_NE(nullptr, ctx);
  ASSERT_GT(ctx->sql_timeout_us_, 0);
  ASSERT_GT(ctx->ls_ctxs_.count(), 0);
  int64_t first_expected_count = 0;
  int64_t second_expected_count = 0;
  for (int64_t i = 0; i < ctx->ls_ctxs_.count(); ++i) {
    ObSSLockRowGCLSCtx *ls_ctx = ctx->ls_ctxs_.at(i);
    ASSERT_NE(nullptr, ls_ctx);
    ASSERT_TRUE(ls_ctx->is_valid());
    ASSERT_EQ(0, ls_ctx->ls_lock_row_value_.op_id_);
    ASSERT_FALSE(ls_ctx->ls_lock_row_value_.is_mark_deleted_);
    ASSERT_FALSE(ls_ctx->iterator_inited_);
    ASSERT_FALSE(ls_ctx->scan_finished_);
    ASSERT_FALSE(ls_ctx->scan_failed_);
    ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
    ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
    if (first_expected_ls == ls_ctx->ls_id_) {
      ++first_expected_count;
    } else if (second_expected_ls == ls_ctx->ls_id_) {
      ++second_expected_count;
    }
  }
  ASSERT_EQ(1, first_expected_count);
  ASSERT_EQ(1, second_expected_count);
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, dag.report_result());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_schedule_handles_missing_and_empty_ls_lock_rows)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator missing_allocator("LockDagMissing");
  ObSSLockRowGCDag missing_dag;
  ASSERT_EQ(OB_SUCCESS, missing_dag.basic_init(missing_allocator));
  ObSSLockRowGCDagCtx *missing_ctx = nullptr;
  ObSSLockRowGCLSCtx *missing_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      missing_dag, ObLSID(99), missing_ctx, missing_ls_ctx));
  LockRowGCDagTaskSnapshot missing_tasks =
      inspect_lock_row_gc_dag_tasks(missing_dag);
  ASSERT_EQ(1, missing_tasks.schedule_tasks_.size());
  ASSERT_NE(nullptr, missing_tasks.barrier_task_);
  ASSERT_EQ(OB_SUCCESS, missing_tasks.schedule_tasks_.at(0)->process());
  ASSERT_FALSE(missing_ls_ctx->iterator_inited_);
  ASSERT_TRUE(missing_ls_ctx->scan_finished_);
  ASSERT_FALSE(missing_ls_ctx->scan_failed_);
  ASSERT_EQ(OB_SUCCESS, missing_ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, missing_ctx->get_first_error_code());
  ASSERT_EQ(3, missing_dag.get_task_list_count());
  ASSERT_EQ(1, missing_tasks.barrier_task_->get_indegree());
  ObSSLockRowGCScheduleTask invalid_continuation;
  ASSERT_EQ(OB_SUCCESS,
      invalid_continuation.init(0, false /* is_ls_head */));
  invalid_continuation.ls_ctx_ = missing_ls_ctx;
  bool need_scan = true;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      invalid_continuation.init_ls_scan_(*missing_ctx, need_scan));
  ASSERT_FALSE(need_scan);

  const ObLSID empty_ls_id(100);
  ASSERT_EQ(OB_SUCCESS,
      insert_ls_lock_row_(OB_SYS_TENANT_ID, empty_ls_id, true));
  ObArenaAllocator empty_allocator("LockDagEmpty");
  ObSSLockRowGCDag empty_dag;
  ASSERT_EQ(OB_SUCCESS, empty_dag.basic_init(empty_allocator));
  ObSSLockRowGCDagCtx *empty_ctx = nullptr;
  ObSSLockRowGCLSCtx *empty_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      empty_dag, empty_ls_id, empty_ctx, empty_ls_ctx));
  LockRowGCDagTaskSnapshot empty_tasks =
      inspect_lock_row_gc_dag_tasks(empty_dag);
  ASSERT_EQ(1, empty_tasks.schedule_tasks_.size());
  ASSERT_NE(nullptr, empty_tasks.barrier_task_);
  ObSSMetaCtrlPageScanParam scan_param;
  empty_tasks.schedule_tasks_.at(0)->fill_page_scan_param_(scan_param);
  ASSERT_EQ(ObSSMetaCtrlPageScanMode::SINGLE_LOCK_META_TYPE,
      scan_param.scan_mode_);
  ASSERT_EQ(blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK,
      scan_param.single_meta_type_);
  ASSERT_EQ(ObSSLockRowGCScheduleTask::CTRL_SCAN_PAGE_SIZE,
      scan_param.page_size_);
  ASSERT_EQ(OB_SUCCESS, empty_tasks.schedule_tasks_.at(0)->process());
  ASSERT_TRUE(empty_ls_ctx->ls_lock_row_value_.is_valid());
  ASSERT_TRUE(empty_ls_ctx->ls_lock_row_value_.is_mark_deleted_);
  ASSERT_TRUE(empty_ls_ctx->iterator_inited_);
  ASSERT_TRUE(empty_ls_ctx->scan_finished_);
  ASSERT_FALSE(empty_ls_ctx->scan_failed_);
  ASSERT_TRUE(empty_ls_ctx->page_iter_.is_finished());
  ASSERT_EQ(0, empty_ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(3, empty_dag.get_task_list_count());
  ASSERT_EQ(1, empty_tasks.barrier_task_->get_indegree());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_schedule_generates_single_batch_with_barrier)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(101);
  const SCN meta_scn = make_scn(50);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(OB_SYS_TENANT_ID, ls_id, false));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
        OB_SYS_TENANT_ID, ls_id, 101000 + i, make_scn(100 + i),
        meta_scn, false));
  }

  ObArenaAllocator dag_allocator("LockDagSingle");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  LockRowGCDagTaskSnapshot before = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, before.schedule_tasks_.size());
  ASSERT_NE(nullptr, before.barrier_task_);
  ObSSLockRowGCScheduleTask *head = before.schedule_tasks_.at(0);
  ASSERT_EQ(0, head->batch_idx_);
  ASSERT_EQ(OB_SUCCESS, head->process());

  LockRowGCDagTaskSnapshot after = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(4, dag.get_task_list_count());
  ASSERT_EQ(dag.get_task_list_count(), after.recognized_task_count_);
  ASSERT_EQ(1, after.schedule_tasks_.size());
  ASSERT_EQ(1, after.executor_tasks_.size());
  ASSERT_EQ(before.barrier_task_, after.barrier_task_);
  ObSSLockRowGCExecutorTask *executor = after.executor_tasks_.at(0);
  ASSERT_TRUE(executor->is_inited_);
  ASSERT_EQ(0, executor->batch_idx_);
  ASSERT_EQ(ls_ctx, executor->ls_ctx_);
  ASSERT_EQ(3, executor->tablet_infos_.count());
  ASSERT_TRUE(ls_ctx->iterator_inited_);
  ASSERT_TRUE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_EQ(3, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(3, ctx->get_scanned_tablet_count());
  ASSERT_EQ(2, after.barrier_task_->get_indegree());
  ObSEArray<ObINodeWithChild *, 4> children;
  ASSERT_EQ(OB_SUCCESS, head->copy_child_nodes(children));
  ASSERT_EQ(2, children.count());
  ASSERT_TRUE(has_lock_row_gc_child(*head, *after.barrier_task_));
  ASSERT_TRUE(has_lock_row_gc_child(*head, *executor));
  children.reset();
  ASSERT_EQ(OB_SUCCESS, executor->copy_child_nodes(children));
  ASSERT_EQ(1, children.count());
  ASSERT_TRUE(has_lock_row_gc_child(*executor, *after.barrier_task_));
  ASSERT_EQ(1, executor->get_indegree());
  ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());

  ObSSMetaCtrlKey first_lock_key;
  first_lock_key.type_ =
      blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  first_lock_key.tablet_key_.tablet_id_ = ObTabletID(101000);
  first_lock_key.tablet_key_.reorg_scn_ = make_scn(100);
  first_lock_key.tablet_key_.control_scn_.set_min();
  ObSSMetaCtrlLockRowValue first_lock_value;
  ASSERT_EQ(OB_SUCCESS, read_lock_row_value_(
      OB_SYS_TENANT_ID, ls_id, first_lock_key, first_lock_value));
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_schedule_generates_multiple_batches_with_barrier)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(102);
  const int64_t row_count =
      ObSSLockRowGCScheduleTask::EXECUTOR_BATCH_SIZE + 1;
  const int64_t first_tablet_id = 102000;
  const SCN meta_scn = make_scn(500);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(OB_SYS_TENANT_ID, ls_id, false));
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
        OB_SYS_TENANT_ID, ls_id, first_tablet_id + i,
        make_scn(1000 + i), meta_scn, false));
  }

  ObArenaAllocator dag_allocator("LockDagMulti");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  LockRowGCDagTaskSnapshot initial = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, initial.schedule_tasks_.size());
  ASSERT_NE(nullptr, initial.barrier_task_);
  ObSSLockRowGCScheduleTask *head = initial.schedule_tasks_.at(0);
  ASSERT_EQ(0, head->batch_idx_);
  ASSERT_EQ(OB_SUCCESS, head->process());

  LockRowGCDagTaskSnapshot first_batch = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_EQ(dag.get_task_list_count(), first_batch.recognized_task_count_);
  ASSERT_EQ(2, first_batch.schedule_tasks_.size());
  ASSERT_EQ(1, first_batch.executor_tasks_.size());
  ASSERT_EQ(3, first_batch.barrier_task_->get_indegree());
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_EQ(ObSSLockRowGCScheduleTask::EXECUTOR_BATCH_SIZE,
      first_batch.executor_tasks_.at(0)->tablet_infos_.count());
  ASSERT_EQ(0, first_batch.executor_tasks_.at(0)->batch_idx_);
  ObSSLockRowGCScheduleTask *continuation = nullptr;
  for (ObSSLockRowGCScheduleTask *schedule : first_batch.schedule_tasks_) {
    if (!schedule->is_ls_head_) {
      continuation = schedule;
    }
  }
  ASSERT_NE(nullptr, continuation);
  ASSERT_EQ(1, continuation->batch_idx_);
  ASSERT_EQ(head->ls_idx_, continuation->ls_idx_);
  ASSERT_EQ(nullptr, continuation->ls_ctx_);
  ASSERT_TRUE(has_lock_row_gc_child(
      *head, *first_batch.executor_tasks_.at(0)));
  ASSERT_TRUE(has_lock_row_gc_child(*head, *continuation));
  ASSERT_TRUE(has_lock_row_gc_child(*head, *first_batch.barrier_task_));
  ASSERT_TRUE(has_lock_row_gc_child(
      *first_batch.executor_tasks_.at(0), *first_batch.barrier_task_));
  ASSERT_TRUE(has_lock_row_gc_child(
      *continuation, *first_batch.barrier_task_));
  ASSERT_EQ(OB_SUCCESS, continuation->process());
  ASSERT_EQ(ls_ctx, continuation->ls_ctx_);

  LockRowGCDagTaskSnapshot all_batches = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(6, dag.get_task_list_count());
  ASSERT_EQ(dag.get_task_list_count(), all_batches.recognized_task_count_);
  ASSERT_EQ(2, all_batches.schedule_tasks_.size());
  ASSERT_EQ(2, all_batches.executor_tasks_.size());
  ASSERT_EQ(4, all_batches.barrier_task_->get_indegree());
  ASSERT_TRUE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_EQ(row_count, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(row_count, ctx->get_scanned_tablet_count());
  std::set<uint64_t> tablet_ids;
  std::set<int64_t> batch_indices;
  int64_t executor_input_count = 0;
  for (ObSSLockRowGCExecutorTask *executor : all_batches.executor_tasks_) {
    ASSERT_TRUE(executor->is_inited_);
    ASSERT_EQ(ls_ctx, executor->ls_ctx_);
    ASSERT_TRUE(batch_indices.insert(executor->batch_idx_).second);
    ASSERT_EQ(1, executor->get_indegree());
    ASSERT_TRUE(has_lock_row_gc_child(*executor, *all_batches.barrier_task_));
    executor_input_count += executor->tablet_infos_.count();
    for (int64_t i = 0; i < executor->tablet_infos_.count(); ++i) {
      ASSERT_TRUE(executor->tablet_infos_.at(i).is_valid());
      ASSERT_TRUE(tablet_ids.insert(
          executor->tablet_infos_.at(i).tablet_id_.id()).second);
    }
  }
  ASSERT_EQ(row_count, executor_input_count);
  ASSERT_EQ((std::set<int64_t>{0, 1}), batch_indices);
  ASSERT_EQ(static_cast<size_t>(row_count), tablet_ids.size());
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(1, tablet_ids.count(first_tablet_id + i));
  }
  ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_schedule_exact_batch_stops_after_empty_continuation)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(131);
  const int64_t first_tablet_id = 131000;
  const int64_t row_count =
      ObSSLockRowGCScheduleTask::EXECUTOR_BATCH_SIZE;
  const SCN meta_scn = make_scn(500);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(
      OB_SYS_TENANT_ID, ls_id, false /* is_mark_deleted */));
  for (int64_t i = 0; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
        OB_SYS_TENANT_ID, ls_id, first_tablet_id + i,
        make_scn(1000 + i), meta_scn, false /* is_mark_deleted */));
  }

  ObArenaAllocator dag_allocator("LockGCExact");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  LockRowGCDagTaskSnapshot initial = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, initial.schedule_tasks_.size());
  ASSERT_NE(nullptr, initial.barrier_task_);

  ObSSLockRowGCScheduleTask *head = initial.schedule_tasks_.at(0);
  ASSERT_EQ(OB_SUCCESS, head->process());
  LockRowGCDagTaskSnapshot first_batch =
      inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_EQ(2, first_batch.schedule_tasks_.size());
  ASSERT_EQ(1, first_batch.executor_tasks_.size());
  ASSERT_EQ(ObSSLockRowGCScheduleTask::EXECUTOR_BATCH_SIZE,
      first_batch.executor_tasks_.at(0)->tablet_infos_.count());
  ASSERT_EQ(3, first_batch.barrier_task_->get_indegree());
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_EQ(row_count, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(row_count, ctx->get_scanned_tablet_count());

  ObSSLockRowGCScheduleTask *continuation = nullptr;
  for (ObSSLockRowGCScheduleTask *schedule :
       first_batch.schedule_tasks_) {
    if (!schedule->is_ls_head_) {
      continuation = schedule;
    }
  }
  ASSERT_NE(nullptr, continuation);
  ASSERT_TRUE(has_lock_row_gc_child(
      *continuation, *first_batch.barrier_task_));

  const int64_t old_task_count = dag.get_task_list_count();
  const int64_t old_barrier_indegree =
      first_batch.barrier_task_->get_indegree();
  ASSERT_EQ(OB_SUCCESS, continuation->process());
  LockRowGCDagTaskSnapshot terminal = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(old_task_count, dag.get_task_list_count());
  ASSERT_EQ(2, terminal.schedule_tasks_.size());
  ASSERT_EQ(1, terminal.executor_tasks_.size());
  ASSERT_EQ(first_batch.barrier_task_, terminal.barrier_task_);
  ASSERT_EQ(old_barrier_indegree, terminal.barrier_task_->get_indegree());
  ASSERT_TRUE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_TRUE(ls_ctx->page_iter_.is_finished());
  ASSERT_EQ(row_count, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(row_count, ctx->get_scanned_tablet_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_schedule_absorbs_page_iterator_timeout)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(103);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(OB_SYS_TENANT_ID, ls_id, false));
  ObArenaAllocator dag_allocator("LockDagPgTout");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  LockRowGCDagTaskSnapshot initial = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, initial.schedule_tasks_.size());
  ObSSLockRowGCScheduleTask *schedule = initial.schedule_tasks_.at(0);
  ASSERT_EQ(OB_SUCCESS, schedule->bind_ls_ctx_(*ctx));
  bool need_scan = false;
  ASSERT_EQ(OB_SUCCESS, schedule->init_ls_scan_(*ctx, need_scan));
  ASSERT_TRUE(need_scan);

  ObSSMetaCtrlPageIterator::PageRow page_row;
  page_row.ctrl_key_.type_ =
      blocksstable::ObStorageObjectType::SHARED_TABLET_LOCK;
  page_row.ctrl_key_.tablet_key_.tablet_id_ = ObTabletID(103001);
  page_row.ctrl_key_.tablet_key_.reorg_scn_ = make_scn(100);
  page_row.ctrl_key_.tablet_key_.control_scn_.set_min();
  page_row.lock_row_value_.is_mark_deleted_ = false;
  page_row.lock_row_value_.op_id_ = 1;
  page_row.lock_row_value_.meta_scn_ = make_scn(50);
  page_row.lock_row_value_.deleted_scn_.reset();
  page_row.meta_key_ = ObString::make_string("lock-timeout-cursor");
  ASSERT_TRUE(page_row.ctrl_key_.is_valid());
  ASSERT_TRUE(page_row.lock_row_value_.is_valid());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->page_iter_.page_buffer_.push_back(page_row));
  {
    lib::WorkerTimeoutGuard worker_timeout_guard(
        ObTimeUtility::current_time() - 1);
    ASSERT_EQ(OB_SUCCESS, schedule->process());
  }

  LockRowGCDagTaskSnapshot after = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_EQ(0, after.executor_tasks_.size());
  ASSERT_EQ(1, after.schedule_tasks_.size());
  ASSERT_EQ(1, after.barrier_task_->get_indegree());
  ASSERT_TRUE(ls_ctx->iterator_inited_);
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_TRUE(ls_ctx->scan_failed_);
  ASSERT_FALSE(ls_ctx->page_iter_.is_finished());
  ASSERT_EQ(1, ls_ctx->page_iter_.get_returned_row_count());
  ASSERT_EQ(1, ctx->get_scanned_tablet_count());
  ASSERT_EQ(OB_TIMEOUT, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_TIMEOUT, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
  ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_TRUE(has_lock_row_gc_child(*schedule, *after.barrier_task_));
}

TEST_F(TestSSLockRowGCTask, lock_row_gc_schedule_absorbs_ls_lock_lookup_timeout)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(104);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(OB_SYS_TENANT_ID, ls_id, false));
  ObArenaAllocator dag_allocator("LockDagLkTout");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  LockRowGCDagTaskSnapshot tasks = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, tasks.schedule_tasks_.size());
  {
    lib::WorkerTimeoutGuard worker_timeout_guard(
        ObTimeUtility::current_time() - 1);
    ASSERT_EQ(OB_SUCCESS, tasks.schedule_tasks_.at(0)->process());
  }
  ASSERT_FALSE(ls_ctx->iterator_inited_);
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_TRUE(ls_ctx->scan_failed_);
  ASSERT_EQ(OB_TIMEOUT, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_TIMEOUT, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_EQ(1, tasks.barrier_task_->get_indegree());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_schedule_stops_before_mutating_ls_context)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockDagStopped");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ObLSID(105), ctx, ls_ctx));
  LockRowGCDagTaskSnapshot tasks = inspect_lock_row_gc_dag_tasks(dag);
  ASSERT_EQ(1, tasks.schedule_tasks_.size());
  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED, tasks.schedule_tasks_.at(0)->process());
  ASSERT_FALSE(ls_ctx->iterator_inited_);
  ASSERT_FALSE(ls_ctx->scan_finished_);
  ASSERT_FALSE(ls_ctx->scan_failed_);
  ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_EQ(1, tasks.barrier_task_->get_indegree());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_executor_deletes_eligible_rows_and_preserves_unrelated)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(106);
  const ObTabletID target_tablet_id(1006000);
  const ObTabletID unrelated_tablet_id(1006001);
  const SCN target_reorg_scn = make_scn(100);
  const SCN unrelated_reorg_scn = make_scn(101);
  const SCN meta_scn = make_scn(50);
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, target_tablet_id.id(),
      target_reorg_scn, meta_scn, true));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, target_tablet_id.id(),
      target_reorg_scn, meta_scn, true,
      blocksstable::ObStorageObjectType::SHARED_MINOR_LOCK));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, target_tablet_id.id(),
      target_reorg_scn, meta_scn, true,
      blocksstable::ObStorageObjectType::SHARED_MAJOR_LOCK));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, unrelated_tablet_id.id(),
      unrelated_reorg_scn, meta_scn, true));

  ObArenaAllocator dag_allocator("LockExecDel");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(target_tablet_id, target_reorg_scn)));
  ScriptedSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  executor->call_base_delete_ = true;
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_SUCCESS, true));
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->check_count_);
  ASSERT_EQ(1, executor->delete_count_);
  ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(3, ctx->get_deleted_tablet_lock_row_count());
  ObSSMetaCtrlLockRowValue lock_value;
  ASSERT_EQ(OB_ITER_END, read_lock_row_value_(
      OB_SYS_TENANT_ID, ls_id,
      make_tablet_lock_row_key(
          ls_id, target_tablet_id, target_reorg_scn),
      lock_value));
  ObSSMetaCtrlKey minor_lock_key = make_tablet_lock_row_key(
      ls_id, target_tablet_id, target_reorg_scn);
  minor_lock_key.type_ = blocksstable::ObStorageObjectType::SHARED_MINOR_LOCK;
  ASSERT_EQ(OB_ITER_END, read_lock_row_value_(
      OB_SYS_TENANT_ID, ls_id, minor_lock_key, lock_value));
  ObSSMetaCtrlKey major_lock_key = make_tablet_lock_row_key(
      ls_id, target_tablet_id, target_reorg_scn);
  major_lock_key.type_ = blocksstable::ObStorageObjectType::SHARED_MAJOR_LOCK;
  ASSERT_EQ(OB_ITER_END, read_lock_row_value_(
      OB_SYS_TENANT_ID, ls_id, major_lock_key, lock_value));
  ASSERT_EQ(OB_SUCCESS, read_lock_row_value_(
      OB_SYS_TENANT_ID, ls_id,
      make_tablet_lock_row_key(
          ls_id, unrelated_tablet_id, unrelated_reorg_scn),
      lock_value));
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_two_dag_workers_concurrently_delete_same_row)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(132);
  const ObTabletID tablet_id(10132000);
  const SCN reorg_scn = make_scn(320);
  const SCN meta_scn = make_scn(319);
  const ObSSMetaCtrlKey lock_key =
      make_tablet_lock_row_key(ls_id, tablet_id, reorg_scn);

  ObArenaAllocator observer_a_allocator("LockObsDagA");
  ObArenaAllocator observer_b_allocator("LockObsDagB");
  ObSSLockRowGCDag observer_a_dag;
  ObSSLockRowGCDag observer_b_dag;
  ASSERT_EQ(OB_SUCCESS, observer_a_dag.basic_init(observer_a_allocator));
  ASSERT_EQ(OB_SUCCESS, observer_b_dag.basic_init(observer_b_allocator));
  ObSSLockRowGCDagCtx *observer_a_ctx = nullptr;
  ObSSLockRowGCDagCtx *observer_b_ctx = nullptr;
  ObSSLockRowGCLSCtx *observer_a_ls_ctx = nullptr;
  ObSSLockRowGCLSCtx *observer_b_ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      observer_a_dag, ls_id, observer_a_ctx, observer_a_ls_ctx));
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      observer_b_dag, ls_id, observer_b_ctx, observer_b_ls_ctx));
  ASSERT_NE(nullptr, observer_a_ctx);
  ASSERT_NE(nullptr, observer_b_ctx);
  ASSERT_NE(observer_a_ctx, observer_b_ctx);
  ASSERT_NE(nullptr, observer_a_ls_ctx);
  ASSERT_NE(nullptr, observer_b_ls_ctx);
  ASSERT_NE(observer_a_ls_ctx, observer_b_ls_ctx);
  ASSERT_EQ(OB_SUCCESS,
      set_lock_row_gc_ls_lock_value(*observer_a_ls_ctx, true));
  ASSERT_EQ(OB_SUCCESS,
      set_lock_row_gc_ls_lock_value(*observer_b_ls_ctx, true));

  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(tablet_id, reorg_scn)));
  ConcurrentDeleteSSLockRowGCExecutorTask *observer_a_executor = nullptr;
  ConcurrentDeleteSSLockRowGCExecutorTask *observer_b_executor = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      observer_a_dag.alloc_task(observer_a_executor));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_dag.alloc_task(observer_b_executor));
  ASSERT_NE(nullptr, observer_a_executor);
  ASSERT_NE(nullptr, observer_b_executor);
  LockRowGCConcurrentDeleteBarrier check_barrier;
  LockRowGCConcurrentDeleteBarrier delete_barrier;
  observer_a_executor->set_barriers(check_barrier, delete_barrier);
  observer_b_executor->set_barriers(check_barrier, delete_barrier);
  ASSERT_EQ(OB_SUCCESS,
      observer_a_executor->init(observer_a_ls_ctx, tablet_infos));
  ASSERT_EQ(OB_SUCCESS,
      observer_b_executor->init(observer_b_ls_ctx, tablet_infos));

  ASSERT_EQ(OB_SUCCESS,
      delete_lock_row_exact_(OB_SYS_TENANT_ID, ls_id, lock_key));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, tablet_id.id(), reorg_scn,
      meta_scn, true /* is_mark_deleted */));

  std::atomic<int> observer_a_ret(OB_ERR_UNEXPECTED);
  std::atomic<int> observer_b_ret(OB_ERR_UNEXPECTED);
  std::thread observer_a_worker([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret)) {
      ret = observer_a_executor->process();
    }
    observer_a_ret.store(ret, std::memory_order_release);
  });
  std::thread observer_b_worker([&]() {
    ObTenantSwitchGuard worker_tenant_guard;
    int ret = worker_tenant_guard.switch_to(OB_SYS_TENANT_ID);
    if (OB_SUCC(ret)) {
      ret = observer_b_executor->process();
    }
    observer_b_ret.store(ret, std::memory_order_release);
  });
  observer_a_worker.join();
  observer_b_worker.join();

  ObSSMetaCtrlLockRowValue lock_value;
  const int read_ret = read_lock_row_value_(
      OB_SYS_TENANT_ID, ls_id, lock_key, lock_value);
  const int cleanup_ret =
      delete_lock_row_exact_(OB_SYS_TENANT_ID, ls_id, lock_key);
  EXPECT_EQ(OB_SUCCESS, observer_a_ret.load(std::memory_order_acquire));
  EXPECT_EQ(OB_SUCCESS, observer_b_ret.load(std::memory_order_acquire));
  EXPECT_EQ(2, check_barrier.arrived_count());
  EXPECT_EQ(2, delete_barrier.arrived_count());
  EXPECT_FALSE(check_barrier.timed_out());
  EXPECT_FALSE(delete_barrier.timed_out());
  EXPECT_EQ(OB_SUCCESS, observer_a_ctx->get_first_error_code());
  EXPECT_EQ(OB_SUCCESS, observer_b_ctx->get_first_error_code());
  EXPECT_EQ(0, observer_a_ctx->get_failed_ls_count());
  EXPECT_EQ(0, observer_b_ctx->get_failed_ls_count());
  EXPECT_TRUE(observer_a_ls_ctx->are_all_tablet_lock_rows_deleted());
  EXPECT_TRUE(observer_b_ls_ctx->are_all_tablet_lock_rows_deleted());
  EXPECT_EQ(1, observer_a_ctx->get_deleted_tablet_lock_row_count()
      + observer_b_ctx->get_deleted_tablet_lock_row_count());
  EXPECT_EQ(OB_ITER_END, read_ret);
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_executor_keeps_non_deletable_rows_and_continues)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(107);
  const ObTabletID unmarked_tablet_id(1007000);
  const ObTabletID inner_tablet_id = LS_TX_DATA_TABLET;
  const ObTabletID eligible_tablet_id(1007002);
  const ObTabletID missing_tablet_id(1007003);
  const SCN unmarked_reorg_scn = make_scn(200);
  const SCN inner_reorg_scn = make_scn(201);
  const SCN eligible_reorg_scn = make_scn(202);
  const SCN missing_reorg_scn = make_scn(203);
  const SCN meta_scn = make_scn(150);
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, unmarked_tablet_id.id(),
      unmarked_reorg_scn, meta_scn, false));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, inner_tablet_id.id(),
      inner_reorg_scn, meta_scn, false));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, eligible_tablet_id.id(),
      eligible_reorg_scn, meta_scn, true));

  ObArenaAllocator dag_allocator("LockExecKeep");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  // A marked-deleted LS makes an unmarked inner Tablet lock eligible for the
  // remaining checks, while an unmarked ordinary Tablet remains ineligible.
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(unmarked_tablet_id, unmarked_reorg_scn)));
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(inner_tablet_id, inner_reorg_scn)));
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(eligible_tablet_id, eligible_reorg_scn)));
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(missing_tablet_id, missing_reorg_scn)));
  ScriptedSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_SUCCESS, false));
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_SUCCESS, true));
  executor->deleted_lock_row_counts_.push_back(1);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(2, executor->check_count_);
  ASSERT_EQ(1, executor->delete_count_);
  ASSERT_EQ(2, executor->checked_tablet_ids_.size());
  ASSERT_EQ(inner_tablet_id.id(), executor->checked_tablet_ids_.at(0));
  ASSERT_EQ(eligible_tablet_id.id(), executor->checked_tablet_ids_.at(1));
  ASSERT_FALSE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(1, ctx->get_deleted_tablet_lock_row_count());
}

TEST_F(TestSSLockRowGCTask,
    lock_row_gc_executor_absorbs_business_error_and_stops_batch)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(108);
  const int64_t first_tablet_id = 1008000;
  const SCN meta_scn = make_scn(250);
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  for (int64_t i = 0; i < 3; ++i) {
    const SCN reorg_scn = make_scn(300 + i);
    ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
        OB_SYS_TENANT_ID, ls_id, first_tablet_id + i,
        reorg_scn, meta_scn, true));
    ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
        ObSSGCTabletBaseInfo(
            ObTabletID(first_tablet_id + i), reorg_scn)));
  }

  ObArenaAllocator dag_allocator("LockExecTout");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ScriptedSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_SUCCESS, true));
  executor->delete_results_.push_back(OB_TRANS_STMT_TIMEOUT);
  executor->deleted_lock_row_counts_.push_back(0);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->check_count_);
  ASSERT_EQ(1, executor->delete_count_);
  ASSERT_FALSE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_TRANS_STMT_TIMEOUT, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_TRANS_STMT_TIMEOUT, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
  ASSERT_EQ(0, ctx->get_deleted_tablet_lock_row_count());
}

TEST_F(TestSSLockRowGCTask,
    lock_row_gc_executor_absorbs_unexpected_business_error)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(109);
  const SCN meta_scn = make_scn(350);
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  for (int64_t i = 0; i < 2; ++i) {
    const int64_t tablet_id = 1009000 + i;
    const SCN reorg_scn = make_scn(400 + i);
    ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
        OB_SYS_TENANT_ID, ls_id, tablet_id, reorg_scn, meta_scn, true));
    ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
        ObSSGCTabletBaseInfo(ObTabletID(tablet_id), reorg_scn)));
  }

  ObArenaAllocator dag_allocator("LockExecError");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ScriptedSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_ERR_UNEXPECTED, false));
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_SUCCESS, true));
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  ASSERT_EQ(OB_SUCCESS, executor->process());

  ASSERT_EQ(1, executor->check_count_);
  ASSERT_EQ(0, executor->delete_count_);
  ASSERT_FALSE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_ERR_UNEXPECTED, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_ERR_UNEXPECTED, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
  ASSERT_EQ(0, ctx->get_deleted_tablet_lock_row_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_executor_stops_before_business_mutation)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(110);
  ObArenaAllocator dag_allocator("LockExecStop");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(ObTabletID(1010000), make_scn(500))));
  ScriptedSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  executor->check_results_.push_back(
      LockRowGCCheckResult(OB_SUCCESS, true));
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED, executor->process());

  ASSERT_EQ(0, executor->check_count_);
  ASSERT_EQ(0, executor->delete_count_);
  ASSERT_TRUE(ls_ctx->are_all_tablet_lock_rows_deleted());
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(0, ctx->get_deleted_tablet_lock_row_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_executor_checks_tablet_ctrl_value_prefix)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(111);
  const ObTabletID tablet_id = LS_TX_DATA_TABLET;
  const SCN reorg_scn(SCN::min_scn());
  ObArenaAllocator dag_allocator("LockExecCtrl");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(tablet_id, reorg_scn)));
  ObSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  const ObSSMetaCtrlKey lock_key =
      make_tablet_lock_row_key(ls_id, tablet_id, reorg_scn);
  bool ctrl_value_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor->check_tablet_ctrl_value_exist_(
      *ctx, lock_key, ctrl_value_exist));
  ASSERT_FALSE(ctrl_value_exist);
  ASSERT_EQ(OB_SUCCESS, insert_tablet_meta_row_(
      OB_SYS_TENANT_ID, ls_id, tablet_id, make_scn(550)));
  ASSERT_EQ(OB_SUCCESS, executor->check_tablet_ctrl_value_exist_(
      *ctx, lock_key, ctrl_value_exist));
  ASSERT_TRUE(ctrl_value_exist);
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_executor_checks_user_tablet_ctrl_value_prefix)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID ls_id(133);
  const ObTabletID tablet_id(200090);
  const SCN reorg_scn(SCN::min_scn());
  ObArenaAllocator dag_allocator("LockExecUsrCtrl");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      init_lock_row_gc_dag_with_ls(dag, ls_id, ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, set_lock_row_gc_ls_lock_value(*ls_ctx, true));
  ObArray<ObSSGCTabletBaseInfo> tablet_infos;
  ASSERT_EQ(OB_SUCCESS, tablet_infos.push_back(
      ObSSGCTabletBaseInfo(tablet_id, reorg_scn)));
  ObSSLockRowGCExecutorTask *executor = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(executor));
  ASSERT_NE(nullptr, executor);
  ASSERT_EQ(OB_SUCCESS, executor->init(ls_ctx, tablet_infos));
  const ObSSMetaCtrlKey lock_key =
      make_tablet_lock_row_key(ls_id, tablet_id, reorg_scn);
  bool ctrl_value_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor->check_tablet_ctrl_value_exist_(
      *ctx, lock_key, ctrl_value_exist));
  ASSERT_FALSE(ctrl_value_exist);
  ASSERT_EQ(OB_SUCCESS, insert_tablet_meta_row_(
      OB_SYS_TENANT_ID, ls_id, tablet_id, make_scn(551)));
  ASSERT_EQ(OB_SUCCESS, executor->check_tablet_ctrl_value_exist_(
      *ctx, lock_key, ctrl_value_exist));
  ASSERT_TRUE(ctrl_value_exist);
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_barrier_deletes_only_eligible_ls_lock)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockBarGate");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *scan_unfinished_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(112), ctx, scan_unfinished_ctx));
  ASSERT_EQ(OB_SUCCESS,
      set_lock_row_gc_ls_lock_value(*scan_unfinished_ctx, true));

  ObSSLockRowGCLSCtx *scan_failed_ctx = nullptr;
  ObSSLockRowGCLSCtx *unmarked_ctx = nullptr;
  ObSSLockRowGCLSCtx *tablet_rows_remain_ctx = nullptr;
  ObSSLockRowGCLSCtx *eligible_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(113), scan_failed_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(114), unmarked_ctx));
  ASSERT_EQ(OB_SUCCESS,
      ctx->alloc_ls_ctx(ObLSID(115), tablet_rows_remain_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(116), eligible_ctx));
  ASSERT_EQ(OB_SUCCESS,
      make_lock_row_gc_ls_barrier_eligible(*scan_failed_ctx));
  scan_failed_ctx->scan_failed_ = true;
  ASSERT_EQ(OB_SUCCESS,
      set_lock_row_gc_ls_lock_value(*unmarked_ctx, false));
  unmarked_ctx->scan_finished_ = true;
  ASSERT_EQ(OB_SUCCESS,
      make_lock_row_gc_ls_barrier_eligible(*tablet_rows_remain_ctx));
  tablet_rows_remain_ctx->mark_tablet_lock_rows_not_all_deleted();
  ASSERT_EQ(OB_SUCCESS,
      make_lock_row_gc_ls_barrier_eligible(*eligible_ctx));

  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->delete_results_.push_back(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_SUCCESS, barrier->process());

  ASSERT_EQ(1, barrier->delete_count_);
  ASSERT_EQ(0, barrier->check_count_);
  ASSERT_EQ(1, barrier->delete_attempt_ls_ids_.size());
  ASSERT_EQ(eligible_ctx->ls_id_.id(),
      barrier->delete_attempt_ls_ids_.at(0));
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(0, ctx->get_deleted_ls_lock_row_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_barrier_deletes_absent_meta_and_keeps_existing_meta)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  const ObLSID absent_meta_ls_id(117);
  const ObLSID existing_meta_ls_id(118);
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(
      OB_SYS_TENANT_ID, absent_meta_ls_id, true));
  ASSERT_EQ(OB_SUCCESS, insert_ls_lock_row_(
      OB_SYS_TENANT_ID, existing_meta_ls_id, true));

  ObArenaAllocator dag_allocator("LockBarDelete");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *absent_meta_ctx = nullptr;
  ObSSLockRowGCLSCtx *existing_meta_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, absent_meta_ls_id, ctx, absent_meta_ctx));
  ASSERT_EQ(OB_SUCCESS,
      ctx->alloc_ls_ctx(existing_meta_ls_id, existing_meta_ctx));
  ASSERT_EQ(OB_SUCCESS,
      make_lock_row_gc_ls_barrier_eligible(*absent_meta_ctx));
  ASSERT_EQ(OB_SUCCESS,
      make_lock_row_gc_ls_barrier_eligible(*existing_meta_ctx));

  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->call_base_delete_ = true;
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_SUCCESS, false));
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_SUCCESS, true));
  // Repeat once to cover the affected_rows == 0 concurrent-delete branch.
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_SUCCESS, false));
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_SUCCESS, true));
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_SUCCESS, barrier->process());
  ASSERT_EQ(OB_SUCCESS, barrier->process());

  ASSERT_EQ(4, barrier->delete_count_);
  ASSERT_EQ(4, barrier->check_count_);
  ASSERT_EQ(absent_meta_ls_id.id(), barrier->checked_ls_ids_.at(0));
  ASSERT_EQ(existing_meta_ls_id.id(), barrier->checked_ls_ids_.at(1));
  ObSSMetaCtrlLockRowValue lock_value;
  ASSERT_EQ(OB_ITER_END, read_lock_row_value_(
      OB_SYS_TENANT_ID, absent_meta_ls_id,
      absent_meta_ctx->ls_lock_row_key_, lock_value));
  ASSERT_EQ(OB_SUCCESS, read_lock_row_value_(
      OB_SYS_TENANT_ID, existing_meta_ls_id,
      existing_meta_ctx->ls_lock_row_key_, lock_value));
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
  ASSERT_EQ(1, ctx->get_deleted_ls_lock_row_count());

  // Remove the deliberately preserved row so shuffled/repeated runs do not
  // expose it to a later LS iterator test.
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_SUCCESS, false));
  int64_t cleanup_deleted_lock_row_count = 0;
  ASSERT_EQ(OB_SUCCESS,
      barrier->delete_ls_lock_row_(
          *ctx, *existing_meta_ctx, cleanup_deleted_lock_row_count));
  ASSERT_EQ(1, cleanup_deleted_lock_row_count);
  ASSERT_EQ(OB_ITER_END, read_lock_row_value_(
      OB_SYS_TENANT_ID, existing_meta_ls_id,
      existing_meta_ctx->ls_lock_row_key_, lock_value));
}

TEST_F(TestSSLockRowGCTask,
    lock_row_gc_barrier_absorbs_business_errors_and_continues)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockBarTout");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctxs[4] = {nullptr, nullptr, nullptr, nullptr};
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(119), ctx, ls_ctxs[0]));
  for (int64_t i = 1; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        ctx->alloc_ls_ctx(ObLSID(119 + i), ls_ctxs[i]));
  }
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS,
        make_lock_row_gc_ls_barrier_eligible(*ls_ctxs[i]));
  }

  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->delete_results_.push_back(OB_EAGAIN);
  barrier->delete_results_.push_back(OB_TRANS_TIMEOUT);
  barrier->delete_results_.push_back(OB_TRANS_STMT_TIMEOUT);
  barrier->delete_results_.push_back(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_SUCCESS, barrier->process());

  ASSERT_EQ(4, barrier->delete_count_);
  ASSERT_EQ(OB_EAGAIN, ls_ctxs[0]->get_first_error_code());
  ASSERT_EQ(OB_TRANS_TIMEOUT, ls_ctxs[1]->get_first_error_code());
  ASSERT_EQ(OB_TRANS_STMT_TIMEOUT, ls_ctxs[2]->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ls_ctxs[3]->get_first_error_code());
  ASSERT_EQ(OB_EAGAIN, ctx->get_first_error_code());
  ASSERT_EQ(3, ctx->get_failed_ls_count());
  ASSERT_EQ(0, ctx->get_deleted_ls_lock_row_count());
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(ls_ctxs[i]->are_all_tablet_lock_rows_deleted());
  }
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_barrier_absorbs_meta_check_timeout_and_continues)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockBarMetaTout");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *timeout_ctx = nullptr;
  ObSSLockRowGCLSCtx *next_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(128), ctx, timeout_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(129), next_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*timeout_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*next_ctx));

  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->call_base_delete_ = true;
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_TIMEOUT, false));
  barrier->check_results_.push_back(
      LockRowGCMetaCheckResult(OB_SUCCESS, true));
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_SUCCESS, barrier->process());

  ASSERT_EQ(2, barrier->delete_count_);
  ASSERT_EQ(2, barrier->check_count_);
  ASSERT_EQ(timeout_ctx->ls_id_.id(), barrier->checked_ls_ids_.at(0));
  ASSERT_EQ(next_ctx->ls_id_.id(), barrier->checked_ls_ids_.at(1));
  ASSERT_EQ(OB_TIMEOUT, timeout_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, next_ctx->get_first_error_code());
  ASSERT_EQ(OB_TIMEOUT, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
}

TEST_F(TestSSLockRowGCTask,
    lock_row_gc_barrier_absorbs_unexpected_business_error_and_continues)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockBarError");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *first_ctx = nullptr;
  ObSSLockRowGCLSCtx *second_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(123), ctx, first_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(124), second_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*first_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*second_ctx));

  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->delete_results_.push_back(OB_ERR_UNEXPECTED);
  barrier->delete_results_.push_back(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_SUCCESS, barrier->process());

  ASSERT_EQ(2, barrier->delete_count_);
  ASSERT_EQ(first_ctx->ls_id_.id(),
      barrier->delete_attempt_ls_ids_.at(0));
  ASSERT_EQ(second_ctx->ls_id_.id(),
      barrier->delete_attempt_ls_ids_.at(1));
  ASSERT_EQ(OB_ERR_UNEXPECTED, first_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, second_ctx->get_first_error_code());
  ASSERT_EQ(OB_ERR_UNEXPECTED, ctx->get_first_error_code());
  ASSERT_EQ(1, ctx->get_failed_ls_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_barrier_stops_before_business_work)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockBarStop");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(125), ctx, ls_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*ls_ctx));
  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->delete_results_.push_back(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  dag.simply_set_stop();
  ASSERT_EQ(OB_CANCELED, barrier->process());

  ASSERT_EQ(0, barrier->delete_count_);
  ASSERT_EQ(OB_SUCCESS, ls_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_barrier_checks_stop_at_each_ls)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObArenaAllocator dag_allocator("LockBarLoopStop");
  ObSSLockRowGCDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLockRowGCDagCtx *ctx = nullptr;
  ObSSLockRowGCLSCtx *first_ctx = nullptr;
  ObSSLockRowGCLSCtx *second_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, init_lock_row_gc_dag_with_ls(
      dag, ObLSID(126), ctx, first_ctx));
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ObLSID(127), second_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*first_ctx));
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*second_ctx));

  ScriptedSSLockRowGCDeleteLSLockTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(barrier));
  ASSERT_NE(nullptr, barrier);
  barrier->delete_results_.push_back(OB_SUCCESS);
  barrier->delete_results_.push_back(OB_SUCCESS);
  barrier->stop_after_delete_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_CANCELED, barrier->process());

  ASSERT_EQ(1, barrier->delete_count_);
  ASSERT_EQ(first_ctx->ls_id_.id(),
      barrier->delete_attempt_ls_ids_.at(0));
  ASSERT_EQ(OB_SUCCESS, first_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, second_ctx->get_first_error_code());
  ASSERT_EQ(OB_SUCCESS, ctx->get_first_error_code());
  ASSERT_EQ(0, ctx->get_failed_ls_count());
}

TEST_F(TestSSLockRowGCTask,
       lock_row_gc_scheduler_waits_for_parallel_executors_before_barrier)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  int64_t old_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, old_limit));
  ScopedLockRowGCDagAdaptiveLimit limit_guard(*scheduler, old_limit);
  ASSERT_EQ(OB_SUCCESS, limit_guard.set(old_limit + 2));

  const ObLSID ls_id(130);
  const ObTabletID kept_tablet_id(1013000);
  const ObTabletID deleted_tablet_id(1013001);
  const SCN kept_reorg_scn = make_scn(600);
  const SCN deleted_reorg_scn = make_scn(601);
  const SCN meta_scn = make_scn(580);
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, kept_tablet_id.id(), kept_reorg_scn,
      meta_scn, true));
  ASSERT_EQ(OB_SUCCESS, insert_tablet_lock_row_(
      OB_SYS_TENANT_ID, ls_id, deleted_tablet_id.id(),
      deleted_reorg_scn, meta_scn, true));

  ObSSLockRowGCDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  ASSERT_NE(nullptr, dag);
  ScopedUnsubmittedLockRowGCDag dag_guard(*scheduler);
  dag_guard.hold(dag);
  ObSSLockRowGCDagParam param;
  const int64_t event_begin_ts = ObTimeUtility::current_time();
  const int64_t schedule_ts = event_begin_ts;
  ASSERT_EQ(OB_SUCCESS, param.init(OB_SYS_TENANT_ID, schedule_ts));
  ASSERT_EQ(OB_SUCCESS, dag->init_by_param(&param));
  ObSSLockRowGCDagCtx *ctx = dag->get_ctx();
  ASSERT_NE(nullptr, ctx);
  ctx->sql_timeout_us_ = TestSSLockRowGCTask::SQL_TIMEOUT_US;
  ctx->add_scanned_tablet_count(2);
  ObSSLockRowGCLSCtx *ls_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ctx->alloc_ls_ctx(ls_id, ls_ctx));
  ASSERT_NE(nullptr, ls_ctx);
  ASSERT_EQ(OB_SUCCESS, make_lock_row_gc_ls_barrier_eligible(*ls_ctx));

  LockRowGCConcurrentTestState *test_state =
      new LockRowGCConcurrentTestState();
  ASSERT_NE(nullptr, test_state);
  ConcurrentSSLockRowGCExecutorTask *kept_executor = nullptr;
  ConcurrentSSLockRowGCExecutorTask *deleted_executor = nullptr;
  ObservedSSLockRowGCBarrierTask *barrier = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(kept_executor,
      false, test_state->arrived_count_, test_state->both_entered_,
      test_state->release_executors_, test_state->completed_count_));
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(deleted_executor,
      true, test_state->arrived_count_, test_state->both_entered_,
      test_state->release_executors_, test_state->completed_count_));
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(barrier, *ls_ctx,
      test_state->completed_count_, test_state->observed_completed_count_,
      test_state->observed_aggregate_, test_state->barrier_entered_));
  ASSERT_NE(nullptr, kept_executor);
  ASSERT_NE(nullptr, deleted_executor);
  ASSERT_NE(nullptr, barrier);
  ObArray<ObSSGCTabletBaseInfo> kept_infos;
  ObArray<ObSSGCTabletBaseInfo> deleted_infos;
  ASSERT_EQ(OB_SUCCESS, kept_infos.push_back(
      ObSSGCTabletBaseInfo(kept_tablet_id, kept_reorg_scn)));
  ASSERT_EQ(OB_SUCCESS, deleted_infos.push_back(
      ObSSGCTabletBaseInfo(deleted_tablet_id, deleted_reorg_scn)));
  ASSERT_EQ(OB_SUCCESS, kept_executor->init(ls_ctx, kept_infos, 0));
  ASSERT_EQ(OB_SUCCESS, deleted_executor->init(ls_ctx, deleted_infos, 1));
  ASSERT_EQ(OB_SUCCESS, barrier->init());
  ASSERT_EQ(OB_SUCCESS, kept_executor->add_child(*barrier));
  ASSERT_EQ(OB_SUCCESS, deleted_executor->add_child(*barrier));
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*kept_executor));
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*deleted_executor));
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*barrier));
  ASSERT_EQ(2, barrier->get_indegree());

  const int add_ret = scheduler->add_dag(dag);
  if (OB_SUCCESS == add_ret) {
    dag_guard.release();
  }
  ASSERT_EQ(OB_SUCCESS, add_ret);
  scheduler->notify();

  // This deadline is intentionally much shorter than the Executor-side wait.
  // A single-worker scheduler must fail this assertion before the first
  // Executor can time out and let the second one run.
  const bool executors_are_parallel =
      test_state->both_entered_.wait(10 * 1000L);
  EXPECT_TRUE(executors_are_parallel);
  EXPECT_EQ(2,
      test_state->arrived_count_.load(std::memory_order_acquire));
  EXPECT_FALSE(test_state->barrier_entered_.is_signaled());
  test_state->release_executors_.signal();
  const bool barrier_was_entered =
      test_state->barrier_entered_.wait(30 * 1000L);
  EXPECT_TRUE(barrier_was_entered);
  if (barrier_was_entered) {
    EXPECT_EQ(2,
        test_state->observed_completed_count_.load(
            std::memory_order_acquire));
    EXPECT_FALSE(test_state->observed_aggregate_.load(
        std::memory_order_acquire));
  }

  ObSSLockRowGCDag probe;
  probe.set_tenant_id(OB_SYS_TENANT_ID);
  bool dag_exist = true;
  bool is_emergency = false;
  int check_ret = OB_SUCCESS;
  const auto wait_for_dag_drain =
      [&scheduler, &probe, &dag_exist, &is_emergency, &check_ret](
          const int64_t timeout_us) {
    const int64_t deadline_us =
        ObTimeUtility::current_time() + timeout_us;
    dag_exist = true;
    check_ret = OB_SUCCESS;
    while (dag_exist && OB_SUCCESS == check_ret
        && ObTimeUtility::current_time() < deadline_us) {
      check_ret = scheduler->check_dag_exist(
          &probe, dag_exist, is_emergency);
      if (OB_SUCCESS == check_ret && dag_exist) {
        usleep(10 * 1000L);
      }
    }
    return OB_SUCCESS == check_ret && !dag_exist;
  };
  bool dag_drained = wait_for_dag_drain(10L * 1000L * 1000L);
  if (!dag_drained) {
    (void)scheduler->cancel_dag(&probe, true /* force_cancel */);
    // cancel_dag() only publishes stop. Keep the test-owned synchronization
    // state alive until the Scheduler has actually removed the DAG.
    dag_drained = wait_for_dag_drain(30L * 1000L * 1000L);
  }
  EXPECT_EQ(OB_SUCCESS, check_ret);
  EXPECT_TRUE(dag_drained);
  EXPECT_EQ(2,
      test_state->completed_count_.load(std::memory_order_acquire));

  if (dag_drained) {
    int64_t finish_event_count = 0;
    int64_t reported_dag_ret = OB_ERR_UNEXPECTED;
    int64_t reported_dag_status = -1;
    int64_t reported_first_error_code = OB_ERR_UNEXPECTED;
    char extra_info[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
    const int64_t event_deadline_us =
        ObTimeUtility::current_time() + 10L * 1000L * 1000L;
    int event_ret = OB_SUCCESS;
    do {
      event_ret = get_lock_row_gc_finish_event(
          event_begin_ts, schedule_ts, finish_event_count,
          reported_dag_ret, reported_dag_status,
          reported_first_error_code, extra_info, sizeof(extra_info));
      if (OB_SUCCESS == event_ret && 0 == finish_event_count) {
        usleep(100 * 1000L);
      }
    } while (OB_SUCCESS == event_ret && 0 == finish_event_count
        && ObTimeUtility::current_time() < event_deadline_us);
    EXPECT_EQ(OB_SUCCESS, event_ret);
    // Give the asynchronous server-event writer a stabilization window so a
    // duplicate FINISH event cannot arrive just after the first count query.
    if (OB_SUCCESS == event_ret && 1 == finish_event_count) {
      usleep(500 * 1000L);
      event_ret = get_lock_row_gc_finish_event(
          event_begin_ts, schedule_ts, finish_event_count,
          reported_dag_ret, reported_dag_status,
          reported_first_error_code, extra_info, sizeof(extra_info));
    }
    EXPECT_EQ(OB_SUCCESS, event_ret);
    EXPECT_EQ(1, finish_event_count);
    EXPECT_EQ(OB_SUCCESS, reported_dag_ret);
    EXPECT_EQ(ObIDag::DAG_STATUS_FINISH, reported_dag_status);
    EXPECT_EQ(OB_SUCCESS, reported_first_error_code);
    EXPECT_NE(nullptr, std::strstr(extra_info, "failed_ls_count=0,"));
    EXPECT_NE(nullptr, std::strstr(extra_info, "ls_count=1,"));
    EXPECT_NE(nullptr,
        std::strstr(extra_info, "scanned_tablet_count=2,"));
    EXPECT_NE(nullptr,
        std::strstr(extra_info, "deleted_tablet_lock_row_count=1,"));
    EXPECT_NE(nullptr,
        std::strstr(extra_info, "deleted_ls_lock_row_count=0,"));

    EXPECT_EQ(OB_SUCCESS, delete_lock_row_exact_(
        OB_SYS_TENANT_ID, ls_id,
        make_tablet_lock_row_key(ls_id, kept_tablet_id, kept_reorg_scn)));
    EXPECT_EQ(OB_SUCCESS, delete_lock_row_exact_(
        OB_SYS_TENANT_ID, ls_id,
        make_tablet_lock_row_key(
            ls_id, deleted_tablet_id, deleted_reorg_scn)));
    delete test_state;
    test_state = nullptr;
  }
  // If drain itself fails, deliberately retain the heap-backed state: tasks
  // still owned by the Scheduler may reference it, and leaking on a failing
  // test is safer than a use-after-free.
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
