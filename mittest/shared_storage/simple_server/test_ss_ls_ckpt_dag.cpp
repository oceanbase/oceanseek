/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <atomic>
#include <thread>
#include "test_shared_meta_common.h"

namespace oceanbase
{
namespace unittest
{

#if defined(ERRSIM) && __ENABLE_TRACEPOINT__
// Install callbacks before submitting a DAG; cancel and drain it before guards restore the original
// callbacks.
enum BootstrapUploadMode
{
  BOOTSTRAP_UPLOAD_DISABLED = 0,
  BOOTSTRAP_UPLOAD_FAIL_AFTER_FIRST,
  BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED,
};

static constexpr int64_t BOOTSTRAP_MIN_CONCURRENT_UPLOAD_COUNT = 2;
static constexpr int64_t BOOTSTRAP_CONCURRENT_UPLOAD_WAIT_TIMEOUT_US = 5L * 1000L * 1000L;
static constexpr int64_t BOOTSTRAP_UPLOAD_WAIT_TIMEOUT_US = 25L * 1000L * 1000L;

class BootstrapUploadGuard
{
public:
  BootstrapUploadGuard()
    : mode_(BOOTSTRAP_UPLOAD_DISABLED), released_(false), entered_(0), canceled_(0),
      succeeded_(0), errors_(0)
  {}
  ~BootstrapUploadGuard() { reset(); }

  int init(const BootstrapUploadMode mode)
  {
    int ret = OB_SUCCESS;
    BootstrapUploadGuard *expected = nullptr;
    if (BOOTSTRAP_UPLOAD_DISABLED == mode) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!active_.compare_exchange_strong(expected, this)) {
      ret = OB_INIT_TWICE;
    } else {
      mode_ = mode;
      released_.store(false);
      entered_.store(0);
      canceled_.store(0);
      succeeded_.store(0);
      errors_.store(0);
      if (OB_FAIL(before_upload_.init("ob_ss_ls_ckpt_dag.cpp", "process", "before_upload",
                                     reinterpret_cast<void *>(&before_upload)))) {
      } else if (OB_FAIL(after_upload_.init("ob_ss_ls_ckpt_dag.cpp", "process", "after_upload",
                                           reinterpret_cast<void *>(&after_upload)))) {
      }
      if (OB_FAIL(ret)) {
        reset();
      }
    }
    return ret;
  }

  void reset()
  {
    release();
    before_upload_.reset();
    after_upload_.reset();
    BootstrapUploadGuard *expected = this;
    active_.compare_exchange_strong(expected, nullptr);
  }

  void release() { released_.store(true); }
  int64_t get_enter_count() const { return entered_.load(); }
  int64_t get_cancel_count() const { return canceled_.load(); }
  int64_t get_success_count() const { return succeeded_.load(); }
  int64_t get_error_count() const { return errors_.load(); }

  int wait_for_entered(
      const int64_t expected_count = 1,
      const int64_t timeout_us = BOOTSTRAP_UPLOAD_WAIT_TIMEOUT_US) const
  {
    int ret = OB_SUCCESS;
    static constexpr int64_t WAIT_INTERVAL_US = 10L * 1000L;
    const int64_t deadline_us = ObTimeUtility::current_time() + timeout_us;
    while (OB_SUCC(ret) && get_enter_count() < expected_count) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
      } else if (ObTimeUtility::current_time() >= deadline_us) {
        ret = OB_TIMEOUT;
      } else {
        ob_usleep(WAIT_INTERVAL_US);
      }
    }
    return ret;
  }

private:
  static int before_upload(ObSSLsCkptWriteCtx *ctx, const int64_t tablet_idx)
  {
    int ret = OB_SUCCESS;
    BootstrapUploadGuard *guard = active_.load();
    if (nullptr != guard && nullptr != ctx && OB_SYS_TENANT_ID == MTL_ID()
        && ObSSLsCkptMode::BOOTSTRAP == ctx->get_mode()) {
      const int64_t deadline_us = ObTimeUtility::current_time() + 10L * 1000L * 1000L;
      if (BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED == guard->mode_) {
        guard->entered_.fetch_add(1);
        // Keep the task running until the test releases it or real DAG cancellation makes
        // check_status fail.
        while (OB_SUCC(ret) && !guard->released_.load()) {
          if (OB_FAIL(ctx->check_status())) {
            if (OB_CANCELED == ret) {
              guard->canceled_.fetch_add(1);
            }
          } else if (ObTimeUtility::current_time() >= deadline_us) {
            ret = OB_TIMEOUT;
          } else {
            ob_usleep(10L * 1000L);
          }
        }
      } else if (BOOTSTRAP_UPLOAD_FAIL_AFTER_FIRST == guard->mode_ && tablet_idx > 0) {
        // Parallel tasks may finish out of order; wait for at least one tablet upload to succeed.
        while (OB_SUCC(ret) && 0 == guard->succeeded_.load()) {
          if (OB_FAIL(ctx->check_status())) {
          } else if (ObTimeUtility::current_time() >= deadline_us) {
            ret = OB_TIMEOUT;
          } else {
            ob_usleep(10L * 1000L);
          }
        }
        int64_t expected = 0;
        if (OB_SUCC(ret) && guard->errors_.compare_exchange_strong(expected, 1)) {
          ret = OB_IO_ERROR;
        }
      }
    }
    return ret;
  }

  static int after_upload(ObSSLsCkptWriteCtx *ctx, const int64_t tablet_idx)
  {
    UNUSED(tablet_idx);
    BootstrapUploadGuard *guard = active_.load();
    if (nullptr != guard && nullptr != ctx && OB_SYS_TENANT_ID == MTL_ID()
        && ObSSLsCkptMode::BOOTSTRAP == ctx->get_mode()) {
      guard->succeeded_.fetch_add(1);
    }
    return OB_SUCCESS;
  }

  BootstrapUploadGuard(const BootstrapUploadGuard &) = delete;
  BootstrapUploadGuard &operator=(const BootstrapUploadGuard &) = delete;
  static std::atomic<BootstrapUploadGuard *> active_;
  ScopedTracepoint before_upload_;
  ScopedTracepoint after_upload_;
  BootstrapUploadMode mode_;
  std::atomic<bool> released_;
  std::atomic<int64_t> entered_;
  std::atomic<int64_t> canceled_;
  std::atomic<int64_t> succeeded_;
  std::atomic<int64_t> errors_;
};

std::atomic<BootstrapUploadGuard *> BootstrapUploadGuard::active_(nullptr);

class BootstrapPublicationSyncGuard
{
public:
  BootstrapPublicationSyncGuard()
    : upload_guard_(nullptr), submit_thread_(0), wait_ret_(OB_NOT_INIT)
  {}
  ~BootstrapPublicationSyncGuard() { reset(); }

  int init(const BootstrapUploadGuard &upload_guard)
  {
    int ret = OB_SUCCESS;
    BootstrapPublicationSyncGuard *expected = nullptr;
    if (!active_.compare_exchange_strong(expected, this)) {
      ret = OB_INIT_TWICE;
    } else {
      upload_guard_ = &upload_guard;
      submit_thread_ = GETTID();
      wait_ret_ = OB_NOT_INIT;
      if (OB_FAIL(before_publication_check_.init(
              "ob_ss_ls_ckpt_task.cpp", "wait_bootstrap_ckpt_published_",
              "before_publication_check",
              reinterpret_cast<void *>(&wait_for_upload)))) {
        reset();
      }
    }
    return ret;
  }

  int wait_for_upload_entered() const { return wait_ret_; }

  void reset()
  {
    before_publication_check_.reset();
    BootstrapPublicationSyncGuard *expected = this;
    active_.compare_exchange_strong(expected, nullptr);
    upload_guard_ = nullptr;
  }

private:
  static int wait_for_upload()
  {
    int ret = OB_SUCCESS;
    BootstrapPublicationSyncGuard *guard = active_.load();
    if (nullptr != guard && OB_SYS_TENANT_ID == MTL_ID() && guard->submit_thread_ == GETTID()) {
      // Wait for Upload on the calling thread, without using event fields or a helper thread for
      // synchronization.
      ret = guard->upload_guard_->wait_for_entered();
      guard->wait_ret_ = ret;
    }
    return ret;
  }

  BootstrapPublicationSyncGuard(const BootstrapPublicationSyncGuard &) = delete;
  BootstrapPublicationSyncGuard &operator=(const BootstrapPublicationSyncGuard &) = delete;
  static std::atomic<BootstrapPublicationSyncGuard *> active_;
  ScopedTracepoint before_publication_check_;
  const BootstrapUploadGuard *upload_guard_;
  int64_t submit_thread_;
  int wait_ret_;
};

std::atomic<BootstrapPublicationSyncGuard *> BootstrapPublicationSyncGuard::active_(nullptr);
#endif

#ifdef ERRSIM
class BootstrapPalfRecheckErrorGuard
{
public:
  int init(const int error_code)
  {
    int ret = OB_SUCCESS;
    first_get_error_.reset();
    recheck_error_.reset();
    if (OB_FAIL(first_get_error_.init(
            "EN_SS_LS_CKPT_PUBLISHED_PALF_KV_GET", OB_ENTRY_NOT_EXIST, 1))) {
    } else if (OB_FAIL(recheck_error_.init("EN_SS_LS_CKPT_PALF_KV_GET", error_code, 1))) {
      first_get_error_.reset();
    }
    return ret;
  }

private:
  NamedEventGuard first_get_error_;
  NamedEventGuard recheck_error_;
};
#endif

#if defined(ERRSIM) && __ENABLE_TRACEPOINT__
class BootstrapPublicationErrorGuard
{
public:
  ~BootstrapPublicationErrorGuard() { reset(); }

  int init(
      const BootstrapUploadGuard &upload_guard,
      const char *event_name,
      const int error_code)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(old_palf_error_.init("EN_SS_LS_CKPT_OLD_PALF_KV_GET", OB_ENTRY_NOT_EXIST, 1))) {
    } else if (OB_FAIL(publication_error_.init(event_name, error_code, 1))) {
    } else if (OB_FAIL(sync_.init(upload_guard))) {
    }
    if (OB_FAIL(ret)) {
      reset();
    }
    return ret;
  }

  int wait_for_upload_entered() { return sync_.wait_for_upload_entered(); }

  void reset()
  {
    sync_.reset();
    old_palf_error_.reset();
    publication_error_.reset();
  }

private:
  NamedEventGuard old_palf_error_;
  NamedEventGuard publication_error_;
  BootstrapPublicationSyncGuard sync_;
};

class BootstrapScheduleDagGuard
{
public:
  BootstrapScheduleDagGuard() : submit_thread_(0), eagain_count_(0), visits_(0) {}
  ~BootstrapScheduleDagGuard() { reset(); }

  int init(const int64_t eagain_count)
  {
    int ret = OB_SUCCESS;
    BootstrapScheduleDagGuard *expected = nullptr;
    if (eagain_count < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!active_.compare_exchange_strong(expected, this)) {
      ret = OB_INIT_TWICE;
    } else {
      submit_thread_ = GETTID();
      eagain_count_ = eagain_count;
      visits_.store(0);
      if (OB_FAIL(before_schedule_.init(
              "ob_ss_ls_ckpt_task.cpp", "schedule_checkpoint_dag", "before_schedule",
              reinterpret_cast<void *>(&before_schedule)))) {
        reset();
      }
    }
    return ret;
  }

  void reset()
  {
    before_schedule_.reset();
    BootstrapScheduleDagGuard *expected = this;
    active_.compare_exchange_strong(expected, nullptr);
  }

  int64_t get_call_count() const { return visits_.load(); }

private:
  static int before_schedule(const ObSSLsCkptMode mode)
  {
    int ret = OB_SUCCESS;
    BootstrapScheduleDagGuard *guard = active_.load();
    if (nullptr != guard && OB_SYS_TENANT_ID == MTL_ID() && guard->submit_thread_ == GETTID()
        && ObSSLsCkptMode::BOOTSTRAP == mode) {
      if (guard->visits_.fetch_add(1) < guard->eagain_count_) {
        ret = OB_EAGAIN;
      }
    }
    return ret;
  }

  BootstrapScheduleDagGuard(const BootstrapScheduleDagGuard &) = delete;
  BootstrapScheduleDagGuard &operator=(const BootstrapScheduleDagGuard &) = delete;
  static std::atomic<BootstrapScheduleDagGuard *> active_;
  ScopedTracepoint before_schedule_;
  int64_t submit_thread_;
  int64_t eagain_count_;
  std::atomic<int64_t> visits_;
};

std::atomic<BootstrapScheduleDagGuard *> BootstrapScheduleDagGuard::active_(nullptr);

class BootstrapPublishedPalfKvValueGuard
{
public:
  BootstrapPublishedPalfKvValueGuard() : submit_thread_(0), root_scn_(0), injected_(false) {}
  ~BootstrapPublishedPalfKvValueGuard() { reset(); }

  int init(const BootstrapUploadGuard &upload_guard, const int64_t root_scn = 0)
  {
    int ret = OB_SUCCESS;
    BootstrapPublishedPalfKvValueGuard *expected = nullptr;
    if (root_scn < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!active_.compare_exchange_strong(expected, this)) {
      ret = OB_INIT_TWICE;
    } else {
      submit_thread_ = GETTID();
      root_scn_ = root_scn;
      injected_ = false;
      if (OB_FAIL(old_palf_error_.init("EN_SS_LS_CKPT_OLD_PALF_KV_GET", OB_ENTRY_NOT_EXIST, 1))) {
      } else if (OB_FAIL(read_error_.init("EN_SS_LS_CKPT_PALF_KV_GET", OB_ENTRY_NOT_EXIST, 1))) {
      } else if (OB_FAIL(value_.init(
              "ob_ss_ls_ckpt_task.cpp", "get_published_ckpt_palf_kv_", "after_palf_read",
              reinterpret_cast<void *>(&inject_value)))) {
      } else if (OB_FAIL(sync_.init(upload_guard))) {
      }
      if (OB_FAIL(ret)) {
        reset();
      }
    }
    return ret;
  }

  int wait_for_upload_entered() { return sync_.wait_for_upload_entered(); }
  bool is_value_injected() const { return injected_; }

  void reset()
  {
    sync_.reset();
    old_palf_error_.reset();
    read_error_.reset();
    value_.reset();
    BootstrapPublishedPalfKvValueGuard *expected = this;
    active_.compare_exchange_strong(expected, nullptr);
  }

private:
  static int inject_value(const ObLSID *ls_id, ObStringBuffer *value_buffer, int *read_ret)
  {
    int ret = OB_SUCCESS;
    BootstrapPublishedPalfKvValueGuard *guard = active_.load();
    if (nullptr != guard && OB_SYS_TENANT_ID == MTL_ID() && guard->submit_thread_ == GETTID()
        && !guard->injected_) {
      if (OB_ISNULL(ls_id) || OB_ISNULL(value_buffer) || OB_ISNULL(read_ret)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        guard->injected_ = true;
        // The test event bypasses the read; replace its result and payload here, then run the real
        // validation.
        value_buffer->reuse();
        if (0 == guard->root_scn_) {
          ret = value_buffer->append("invalid-palf-value");
        } else {
          ObSSMetaLSCkptKey ckpt_key;
          char value_buf[128] = {0};
          int64_t value_len = 0;
          ckpt_key.ls_id_ = *ls_id;
          if (OB_FAIL(ckpt_key.meta_scn_.convert_for_inner_table_field(guard->root_scn_))) {
          } else if (OB_FAIL(ckpt_key.convert_to_macro_id(false /*is_in_table*/).serialize(
                         value_buf, sizeof(value_buf), value_len))) {
          } else {
            ret = value_buffer->append(value_buf, value_len);
          }
        }
        *read_ret = ret;
      }
    }
    return ret;
  }

  BootstrapPublishedPalfKvValueGuard(const BootstrapPublishedPalfKvValueGuard &) = delete;
  BootstrapPublishedPalfKvValueGuard &operator=(
      const BootstrapPublishedPalfKvValueGuard &) = delete;
  static std::atomic<BootstrapPublishedPalfKvValueGuard *> active_;
  NamedEventGuard old_palf_error_;
  NamedEventGuard read_error_;
  ScopedTracepoint value_;
  BootstrapPublicationSyncGuard sync_;
  int64_t submit_thread_;
  int64_t root_scn_;
  bool injected_;
};

std::atomic<BootstrapPublishedPalfKvValueGuard *>
    BootstrapPublishedPalfKvValueGuard::active_(nullptr);

class NormalLeaseCheckGuard
{
public:
  NormalLeaseCheckGuard() : submit_thread_(0), total_count_(0), submit_count_(0) {}
  ~NormalLeaseCheckGuard() { reset(); }

  int init()
  {
    int ret = OB_SUCCESS;
    NormalLeaseCheckGuard *expected = nullptr;
    if (!active_.compare_exchange_strong(expected, this)) {
      ret = OB_INIT_TWICE;
    } else {
      submit_thread_ = GETTID();
      total_count_.store(0);
      submit_count_.store(0);
      if (OB_FAIL(check_lease_.init("ob_ss_ls_ckpt_dag.cpp", "check_lease_valid", "check_lease",
                                   reinterpret_cast<void *>(&record_check)))) {
        reset();
      }
    }
    return ret;
  }

  void reset()
  {
    check_lease_.reset();
    NormalLeaseCheckGuard *expected = this;
    active_.compare_exchange_strong(expected, nullptr);
  }

  int64_t get_total_count() const { return total_count_.load(); }
  int64_t get_submit_count() const { return submit_count_.load(); }

private:
  static int record_check(const ObSSLsCkptWriteCtx *ctx)
  {
    NormalLeaseCheckGuard *guard = active_.load();
    if (nullptr != guard && nullptr != ctx && OB_SYS_TENANT_ID == MTL_ID()
        && ObSSLsCkptMode::NORMAL == ctx->get_mode()) {
      guard->total_count_.fetch_add(1);
      if (guard->submit_thread_ == GETTID()) {
        guard->submit_count_.fetch_add(1);
      }
    }
    return OB_SUCCESS;
  }

  NormalLeaseCheckGuard(const NormalLeaseCheckGuard &) = delete;
  NormalLeaseCheckGuard &operator=(const NormalLeaseCheckGuard &) = delete;
  static std::atomic<NormalLeaseCheckGuard *> active_;
  ScopedTracepoint check_lease_;
  int64_t submit_thread_;
  std::atomic<int64_t> total_count_;
  std::atomic<int64_t> submit_count_;
};

std::atomic<NormalLeaseCheckGuard *> NormalLeaseCheckGuard::active_(nullptr);

#endif

/**
 * Summary: Verify that a NORMAL checkpoint uses one DAG and produces a readable published
 * checkpoint.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::do_checkpoint: Submit a NORMAL checkpoint DAG.
 * 2. ObSSLsCkptDag: Connect the Prepare, Upload, and Finish stages.
 * 3. ObSSMetaService: Read the checkpoint and verify PALF publication when logservice is enabled.
 *
 * Steps:
 * 1. Pause background checkpoints and record the current checkpoint SCN.
 * 2. Submit a NORMAL checkpoint through the production entry point and wait for the DAG to exit.
 * 3. In Debug ERRSIM builds, verify context lease checks in Prepare, every Upload, and Finish.
 * 4. Read the new checkpoint and verify valid LS meta and a nonempty tablet list.
 * 5. When logservice is enabled, verify that the published PALF SCN matches the new checkpoint SCN.
 */
TEST_F(TestSharedMetaService, normal_ls_ckpt_uses_single_write_dag_and_root_object_path)
{
#ifndef ERRSIM
  if (!GCONF.enable_logservice) {
    SUCCEED() << "当前构建无 ERRSIM 且未启用 logservice，无法打开 checkpoint 测试通路";
    return;
  }
#endif
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  // Prevent concurrent background NORMAL DAG scheduling so only this test's checkpoint is observed.
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());
  // The bootstrap flag still blocks background scheduling; restore the lease only for this explicit
  // NORMAL run.
  ATOMIC_STORE(&mock_sswriter_enabled, true);

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  // Record the latest SCN to verify that this run actually creates a new checkpoint.
  share::SCN previous_ckpt_scn;
  const int previous_ret = get_latest_ls_ckpt_scn(
      *meta_svr, SYS_LS, previous_ckpt_scn);
  ASSERT_TRUE(OB_SUCCESS == previous_ret || OB_ITER_END == previous_ret);

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
#if defined(ERRSIM) && __ENABLE_TRACEPOINT__
  NormalLeaseCheckGuard lease_check_guard;
  ASSERT_EQ(OB_SUCCESS, lease_check_guard.init());
  // Enable BOOTSTRAP-only injections to verify that they neither fail NORMAL scheduling nor block
  // its uploads.
  BootstrapScheduleDagGuard bootstrap_schedule_guard;
  ASSERT_EQ(OB_SUCCESS, bootstrap_schedule_guard.init(1 /*eagain_count*/));
  BootstrapUploadGuard bootstrap_upload_guard;
  ASSERT_EQ(OB_SUCCESS, bootstrap_upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
#endif
  // Cancel and drain the asynchronous DAG even if an assertion fails, keeping subsequent tests
  // isolated.
  DEFER(executor.cancel_checkpoint_dag_and_wait());
  // simple_server disables logservice by default; enable the checkpoint data path through a test
  // switch.
  // This covers only the object path unless logservice is enabled, in which case PALF publication
  // is also checked.
  // NORMAL only submits the DAG; explicitly wait for it to exit before checking the written
  // checkpoint.
  ASSERT_EQ(OB_SUCCESS, executor.do_checkpoint(ObSSLsCkptMode::NORMAL));
  ASSERT_EQ(OB_SUCCESS, wait_ls_ckpt_write_dag_exit(executor));

  // After the DAG exits, query the latest SCN and read LS meta and the tablet list from its root
  // object.
  share::SCN latest_ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, get_latest_ls_ckpt_scn(*meta_svr, SYS_LS, latest_ckpt_scn));
  ASSERT_TRUE(latest_ckpt_scn.is_valid());
  if (OB_SUCCESS == previous_ret) {
    ASSERT_GT(latest_ckpt_scn, previous_ckpt_scn);
  }
  ObSSLSMeta ls_meta;
  ObSEArray<blocksstable::MacroBlockId, 64> tablet_meta_ids;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_ls_ckpt_data(
      SYS_LS, latest_ckpt_scn, ls_meta, tablet_meta_ids));
  ASSERT_TRUE(ls_meta.is_valid());
  ASSERT_EQ(SYS_LS, ls_meta.ls_id_);
  ASSERT_GT(tablet_meta_ids.count(), 0);
#if defined(ERRSIM) && __ENABLE_TRACEPOINT__
  // Expect one context lease check in Prepare, one per tablet, and one in Finish.
  static constexpr int64_t NORMAL_FIXED_LEASE_CHECK_COUNT = 2;
  ASSERT_EQ(tablet_meta_ids.count() + NORMAL_FIXED_LEASE_CHECK_COUNT,
            lease_check_guard.get_total_count());
  ASSERT_EQ(0, lease_check_guard.get_submit_count());
  ASSERT_EQ(0, bootstrap_schedule_guard.get_call_count());
  ASSERT_EQ(0, bootstrap_upload_guard.get_enter_count());
  ASSERT_EQ(0, bootstrap_upload_guard.get_success_count());
  ASSERT_EQ(0, bootstrap_upload_guard.get_error_count());
#endif
  if (GCONF.enable_logservice) {
    share::SCN published_ckpt_scn;
    ASSERT_EQ(OB_SUCCESS,
        meta_svr->get_ls_ckpt_meta_scn(SYS_LS, published_ckpt_scn));
    ASSERT_EQ(latest_ckpt_scn, published_ckpt_scn);
  }
}

#ifdef ERRSIM
/**
 * Summary: Verify that a NORMAL publication failure leaves the written root intact without
 * advancing PALF.
 *
 * Steps:
 * 1. Arm a NORMAL publication error, run BOOTSTRAP first, and verify that it does not consume the
 *    event.
 * 2. Record the latest root SCN and, when logservice is enabled, the PALF SCN.
 * 3. Inject OB_IO_ERROR after writing the NORMAL root but before PALF publication, then wait for
 *    the DAG to exit.
 * 4. Verify that the new root is readable and, when logservice is enabled, PALF is unchanged.
 */
TEST_F(TestSharedMetaService, normal_ls_ckpt_root_write_precedes_palf_publish_failure)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());
  ATOMIC_STORE(&mock_sswriter_enabled, true);

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  NamedEventGuard publish_error_guard;
  ASSERT_EQ(OB_SUCCESS, publish_error_guard.init_for_condition(
      "EN_SS_LS_CKPT_PALF_PUBLISH", OB_IO_ERROR, 1, static_cast<int64_t>(ObSSLsCkptMode::NORMAL)));
  // Drain the DAG before removing the injection so a timeout cannot restore real PALF publication
  // prematurely.
  DEFER(executor.cancel_checkpoint_dag_and_wait());
  share::SCN bootstrap_ckpt_scn_before;
  const int bootstrap_ret_before = get_latest_ls_ckpt_scn(
      *meta_svr, SYS_LS, bootstrap_ckpt_scn_before);
  ASSERT_TRUE(OB_SUCCESS == bootstrap_ret_before || OB_ITER_END == bootstrap_ret_before);
  // BOOTSTRAP reaches the same publication point but must not consume the NORMAL-only error.
  bool is_scheduled = false;
  ASSERT_EQ(OB_SUCCESS, executor.schedule_checkpoint_dag(ObSSLsCkptMode::BOOTSTRAP, is_scheduled));
  ASSERT_TRUE(is_scheduled);
  ASSERT_EQ(OB_SUCCESS, wait_ls_ckpt_write_dag_exit(executor));
  ASSERT_EQ(0, publish_error_guard.get_trigger_count());

  // Take the baseline after BOOTSTRAP so subsequent checks observe only this NORMAL write and
  // publication.
  share::SCN previous_ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, get_latest_ls_ckpt_scn(*meta_svr, SYS_LS, previous_ckpt_scn));
  ASSERT_TRUE(previous_ckpt_scn.is_valid());
  if (OB_SUCCESS == bootstrap_ret_before) {
    ASSERT_GT(previous_ckpt_scn, bootstrap_ckpt_scn_before);
  }

  share::SCN published_scn_before;
  int published_ret_before = OB_NOT_SUPPORTED;
  if (GCONF.enable_logservice) {
    published_ret_before = meta_svr->get_ls_ckpt_meta_scn(
        SYS_LS, published_scn_before);
    ASSERT_EQ(OB_SUCCESS, published_ret_before);
    ASSERT_EQ(previous_ckpt_scn, published_scn_before);
  }

  ASSERT_EQ(OB_SUCCESS, executor.do_checkpoint(ObSSLsCkptMode::NORMAL));
  ASSERT_EQ(OB_SUCCESS, wait_ls_ckpt_write_dag_exit(executor));
  ASSERT_EQ(1, publish_error_guard.get_trigger_count());

  // Background scheduling is paused and only one NORMAL DAG follows the baseline, so all changes
  // belong to it.
  share::SCN latest_ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, get_latest_ls_ckpt_scn(
      *meta_svr, SYS_LS, latest_ckpt_scn));
  ASSERT_TRUE(latest_ckpt_scn.is_valid());
  ASSERT_GT(latest_ckpt_scn, previous_ckpt_scn);

  ObSSLSMeta ls_meta;
  ObSEArray<blocksstable::MacroBlockId, 64> tablet_meta_ids;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_ls_ckpt_data(
      SYS_LS, latest_ckpt_scn, ls_meta, tablet_meta_ids));
  ASSERT_TRUE(ls_meta.is_valid());
  ASSERT_EQ(SYS_LS, ls_meta.ls_id_);
  ASSERT_GT(tablet_meta_ids.count(), 0);

  if (GCONF.enable_logservice) {
    share::SCN published_scn_after;
    const int published_ret_after = meta_svr->get_ls_ckpt_meta_scn(
        SYS_LS, published_scn_after);
    ASSERT_EQ(published_ret_before, published_ret_after);
    if (OB_SUCCESS == published_ret_before) {
      ASSERT_EQ(published_scn_before, published_scn_after);
    }
  } else {
    SUCCEED() << "未启用 logservice，本次只验证 Root 写入早于发布错误";
  }
}

/**
 * Summary: Reject PALF publication when the DAG has failed but its checkpoint context has not.
 *
 * Steps:
 * 1. Prepare valid checkpoint data and record an error only in the owning DAG.
 * 2. Arm a different error at the PALF publication point to detect an incorrect fallthrough.
 * 3. Verify that Finish writes the root, returns the DAG error, and skips the publication point.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_finish_rejects_dag_error_before_palf_publish)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ObArenaAllocator dag_allocator("CkptDagError");
  ObSSLsCkptDag ckpt_dag;
  ASSERT_EQ(OB_SUCCESS, ckpt_dag.basic_init(dag_allocator));
  ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, ckpt_dag.init_by_param(&param));
  ObSSLsCkptWriteCtx &ctx = ckpt_dag.ctx_;
  ObSSLsCkptData &ckpt_data = ctx.ckpt_data_;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(SYS_LS, ckpt_data.ls_meta_));
  ASSERT_EQ(OB_SUCCESS, make_scn(
      static_cast<uint64_t>(ObTimeUtility::current_time_ns()), ckpt_data.ckpt_scn_));
  ckpt_data.reserved_meta_scn_ = ckpt_data.ckpt_scn_;
  ckpt_data.ckpt_ts_ = ObTimeUtility::current_time();
  ASSERT_TRUE(ckpt_data.is_valid());

  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = SYS_LS;
  ckpt_key.meta_scn_ = ckpt_data.ckpt_scn_;
  const blocksstable::MacroBlockId root_id = ckpt_key.convert_to_macro_id(false /*is_in_table*/);
  ASSERT_TRUE(root_id.is_valid());
  DEFER(EXPECT_EQ(OB_SUCCESS, delete_test_object(root_id)));

  ObSSLsCkptFinishTask finish_task;
  finish_task.set_dag(ckpt_dag);
  ASSERT_EQ(OB_SUCCESS, finish_task.init(ctx));
  // Keep the context successful so its existing status checks cannot catch this error.
  ckpt_dag.set_dag_ret(OB_IO_ERROR);
  ASSERT_EQ(OB_SUCCESS, ctx.get_first_error());
  NamedEventGuard publish_error_guard;
  ASSERT_EQ(OB_SUCCESS, publish_error_guard.init_for_condition(
      "EN_SS_LS_CKPT_PALF_PUBLISH", OB_TIMEOUT, 1, static_cast<int64_t>(ObSSLsCkptMode::BOOTSTRAP)));

  ASSERT_EQ(OB_IO_ERROR, finish_task.process());
  ASSERT_EQ(OB_IO_ERROR, ctx.get_first_error());
  ASSERT_EQ(0, publish_error_guard.get_trigger_count());
  bool root_exists = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
      root_id, 0 /*ls_epoch*/, root_exists, SYS_LS.id()));
  ASSERT_TRUE(root_exists);
}

#if __ENABLE_TRACEPOINT__
/**
 * Summary: Verify that a partial BOOTSTRAP upload failure publishes no incomplete checkpoint and
 * allows a retry.
 *
 * Coverage:
 * 1. ObSSLsCkptUploadTask: Record the first error across parallel uploads.
 * 2. ObSSLsCkptFinishTask: Clean up this run's reservation on failure without publishing the
 *    checkpoint.
 * 3. ObSSLsCkptExecutor: Clear the old PALF KV and wait for publication of the current checkpoint.
 *
 * Steps:
 * 1. Record existing reservations and, when logservice is enabled, prepare an old PALF KV entry.
 * 2. Allow the first tablet through and inject one OB_IO_ERROR into a subsequent upload.
 * 3. Wait for the DAG to exit and verify that it published no PALF KV and left no reservation.
 * 4. Remove the fault, retry with the same executor, and read the successful checkpoint.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_partial_upload_failure_can_retry)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  const ObSharedMetaReservedType ls_ckpt_type(
      ObSharedMetaReservedTypeEnum::LS_CKPT);
  ObSharedMetaReservedOperator reserved_operator;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      10L * 1000L * 1000L /*sql_timeout_us*/));
  // Record the reservation baseline; the count must return to it after the failed DAG exits.
  ObArray<ObSharedMetaReservedInfo> reservations_before;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, reservations_before));

  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  char palf_key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t palf_key_len = 0;
  ObString palf_key;
  ObArenaAllocator original_palf_value_allocator("OldBootPalf");
  ObStringBuffer original_palf_value(&original_palf_value_allocator);
  int original_palf_get_ret = OB_NOT_SUPPORTED;
  bool restore_original_palf_kv = false;
  blocksstable::MacroBlockId stale_ckpt_id;
  bool cleanup_stale_ckpt_object = false;
  DEFER({
    if (restore_original_palf_kv) {
      const int delete_ret = palf_kv_adapter.delete_kv(palf_key);
      EXPECT_TRUE(OB_SUCCESS == delete_ret || OB_ENTRY_NOT_EXIST == delete_ret);
      if (OB_SUCCESS == original_palf_get_ret) {
        EXPECT_EQ(OB_SUCCESS, palf_kv_adapter.put(palf_key, original_palf_value.string()));
      }
    }
    if (cleanup_stale_ckpt_object) {
      EXPECT_EQ(OB_SUCCESS, delete_test_object(stale_ckpt_id));
    }
  });
  // Save the current PALF KV and prepare a valid old entry to verify its removal before the next
  // scheduling attempt.
  if (GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
        SYS_LS, palf_key_buf, sizeof(palf_key_buf), palf_key_len));
    palf_key.assign_ptr(palf_key_buf, static_cast<int32_t>(palf_key_len));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.init(GCONF.cluster_id, OB_SYS_TENANT_ID));
    original_palf_get_ret = palf_kv_adapter.get(palf_key, original_palf_value);
    ASSERT_TRUE(OB_SUCCESS == original_palf_get_ret
                || OB_ENTRY_NOT_EXIST == original_palf_get_ret);
    restore_original_palf_kv = true;
    const int delete_ret = palf_kv_adapter.delete_kv(palf_key);
    ASSERT_TRUE(OB_SUCCESS == delete_ret || OB_ENTRY_NOT_EXIST == delete_ret);

    // Simulate a completed bootstrap with a valid root object and PALF publication.
    // Without deleting the old key first, an upload failure could incorrectly reuse this old result
    // as success.
    share::SCN stale_meta_scn;
    ASSERT_EQ(OB_SUCCESS, make_scn(
        static_cast<uint64_t>(ObTimeUtility::current_time_ns()), stale_meta_scn));
    ObSSMetaLSCkptKey stale_key;
    stale_key.ls_id_ = SYS_LS;
    stale_key.meta_scn_ = stale_meta_scn;
    stale_ckpt_id = stale_key.convert_to_macro_id(false /*is_in_table*/);
    ASSERT_TRUE(stale_ckpt_id.is_valid());

    ObSSLsCkptData stale_ckpt_data;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(SYS_LS, stale_ckpt_data.ls_meta_));
    stale_ckpt_data.ckpt_scn_ = stale_meta_scn;
    stale_ckpt_data.ckpt_ts_ = ObTimeUtility::current_time();
    stale_ckpt_data.reserved_meta_scn_ = stale_meta_scn;
    ASSERT_TRUE(stale_ckpt_data.is_valid());
    ASSERT_EQ(OB_SUCCESS, delete_test_object(stale_ckpt_id));
    ObSSLsCkptWriter stale_ckpt_writer(true /*need_check_existing_object*/);
    ASSERT_EQ(OB_SUCCESS, stale_ckpt_writer.write_checkpoint_object(
        SYS_LS, stale_ckpt_data));
    cleanup_stale_ckpt_object = true;
    bool stale_ckpt_object_exist = false;
    ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
        stale_ckpt_id, 0 /*ls_epoch*/, stale_ckpt_object_exist, SYS_LS.id()));
    ASSERT_TRUE(stale_ckpt_object_exist);

    char stale_value_buf[128] = {0};
    int64_t stale_value_len = 0;
    ASSERT_EQ(OB_SUCCESS, stale_ckpt_id.serialize(
        stale_value_buf, sizeof(stale_value_buf), stale_value_len));
    ASSERT_EQ(OB_SUCCESS, palf_kv_adapter.put(
        palf_key, ObString(static_cast<int32_t>(stale_value_len), stale_value_buf)));
  }

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  // Fail once after at least one tablet succeeds, leaving some objects uploaded while failing the
  // checkpoint.
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS, upload_guard.init(BOOTSTRAP_UPLOAD_FAIL_AFTER_FIRST));
  DEFER(executor.cancel_checkpoint_dag_and_wait());

  // Run the failure path first; with logservice enabled, the waiter checks that no new result was
  // published.
  bool is_scheduled = false;
  if (GCONF.enable_logservice) {
    // A DAG that exits without publishing PALF KV leaves an unexpected state.
    ASSERT_EQ(OB_ERR_UNEXPECTED, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  } else {
    ASSERT_EQ(OB_SUCCESS, executor.schedule_checkpoint_dag(
        ObSSLsCkptMode::BOOTSTRAP, is_scheduled));
    ASSERT_TRUE(is_scheduled);
    ASSERT_EQ(OB_SUCCESS, wait_ls_ckpt_write_dag_exit(executor));
  }
  // Verify a real partial upload failure and cleanup of the failed run's reservation.
  ASSERT_EQ(1, upload_guard.get_error_count());
  ASSERT_GE(upload_guard.get_success_count(), 1);
  upload_guard.reset();
  ObArray<ObSharedMetaReservedInfo> reservations_after_failure;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, reservations_after_failure));
  ASSERT_EQ(reservations_before.count(), reservations_after_failure.count());

  // The old publication was deleted before scheduling, and the failed DAG must not publish a new
  // PALF KV.
  if (GCONF.enable_logservice) {
    ObArenaAllocator missing_palf_value_allocator("MissBootPalf");
    ObStringBuffer missing_palf_value(&missing_palf_value_allocator);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, palf_kv_adapter.get(palf_key, missing_palf_value));
  }

  // Retry with the same executor after removing the fault; previously uploaded objects must not
  // block it.
  is_scheduled = false;
  if (GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  } else {
    ASSERT_EQ(OB_SUCCESS, executor.schedule_checkpoint_dag(
        ObSSLsCkptMode::BOOTSTRAP, is_scheduled));
    ASSERT_TRUE(is_scheduled);
    ASSERT_EQ(OB_SUCCESS, wait_ls_ckpt_write_dag_exit(executor));
  }
  // Read the retried checkpoint and, when logservice is enabled, verify the final published SCN.
  share::SCN retried_ckpt_scn;
  ASSERT_EQ(OB_SUCCESS, get_latest_ls_ckpt_scn(*meta_svr, SYS_LS, retried_ckpt_scn));
  ObSSLSMeta retried_ls_meta;
  ObSEArray<blocksstable::MacroBlockId, 64> retried_tablet_meta_ids;
  ASSERT_EQ(OB_SUCCESS, meta_svr->read_ls_ckpt_data(
      SYS_LS, retried_ckpt_scn, retried_ls_meta, retried_tablet_meta_ids));
  ASSERT_TRUE(retried_ls_meta.is_valid());
  ASSERT_EQ(SYS_LS, retried_ls_meta.ls_id_);
  ASSERT_GT(retried_tablet_meta_ids.count(), 0);
  if (GCONF.enable_logservice) {
    share::SCN published_scn_after_retry;
    ASSERT_EQ(OB_SUCCESS,
        meta_svr->get_ls_ckpt_meta_scn(SYS_LS, published_scn_after_retry));
    ASSERT_EQ(retried_ckpt_scn, published_scn_after_retry);
    restore_original_palf_kv = false;
  }
}

/**
 * Summary: Verify that a BOOTSTRAP publication timeout cancels and drains the DAG before returning
 * the timeout.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::wait_bootstrap_ckpt_published_: Handle publication wait timeouts.
 * 2. ObSSLsCkptExecutor::cancel_checkpoint_dag_and_wait: Cancel the DAG and wait for scheduler
 *    reclamation.
 * 3. ObSSLsCkptUploadTask: Stop uploading on cancellation.
 *
 * Steps:
 * 1. Block UploadTask to prevent checkpoint publication.
 * 2. Inject OB_TIMEOUT while waiting for publication.
 * 3. Verify that do_checkpoint returns OB_TIMEOUT only after the DAG exits.
 * 4. Verify that an upload task observed cancellation.
 * 5. Verify that cancellation retains the reservation for later successful checkpoint cleanup.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_timeout_cancels_and_waits_dag_exit)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  const ObSharedMetaReservedType ls_ckpt_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  ObSharedMetaReservedOperator reserved_operator;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      10L * 1000L * 1000L /*sql_timeout_us*/));
  ObArray<ObSharedMetaReservedInfo> reservations_before;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, reservations_before));

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  // Keep UploadTask running so the DAG cannot finish naturally before the timeout is injected.
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS,
      upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
  // Return OB_TIMEOUT only after UploadTask blocks, reliably exercising cancellation after a failed
  // wait.
  BootstrapPublicationErrorGuard wait_guard;
  ASSERT_EQ(OB_SUCCESS, wait_guard.init(upload_guard, "EN_SS_LS_CKPT_BOOTSTRAP_WAIT", OB_TIMEOUT));
  // Release the blocked task and drain the DAG even if an assertion fails, avoiding hangs in later
  // tests.
  DEFER({
    upload_guard.release();
    executor.cancel_checkpoint_dag_and_wait();
  });

  // do_checkpoint must preserve the timeout and finish canceling and draining the DAG before
  // returning.
  ASSERT_EQ(OB_TIMEOUT, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  ASSERT_EQ(OB_SUCCESS, wait_guard.wait_for_upload_entered());

  // On return, the scheduler has reclaimed the DAG and a running UploadTask has observed
  // cancellation.
  bool dag_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_FALSE(dag_exist);
  ASSERT_GT(upload_guard.get_cancel_count(), 0);

  // Force cancellation skips Finish, so retaining this reservation is expected and GC-safe.
  // A later successful checkpoint removes it via bounded range cleanup; see the
  // ls_ckpt_success_cleanup_removes_* cases. No immediate cleanup is required on cancellation.
  ObArray<ObSharedMetaReservedInfo> reservations_after_cancel;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, reservations_after_cancel));
  ASSERT_EQ(reservations_before.count() + 1, reservations_after_cancel.count());
}

/**
 * Summary: Verify that a permanent BOOTSTRAP PALF KV read error is preserved while the running DAG
 * is cleaned up.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::check_bootstrap_ckpt_published_once_: Distinguish retryable and permanent
 *    errors.
 * 2. ObSSLsCkptExecutor::cancel_checkpoint_dag_and_wait: Cancel and drain the scheduled DAG.
 *
 * Steps:
 * 1. Block UploadTask to keep the DAG running during the PALF KV read.
 * 2. Inject OB_IO_ERROR into the PALF KV read.
 * 3. Verify that do_checkpoint returns the original OB_IO_ERROR after the DAG exits.
 * 4. Verify that an upload task observed cancellation.
 * 5. Verify that cancellation retains the reservation for later successful checkpoint cleanup.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_nonretry_palf_error_cancels_and_waits_dag_exit)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  const ObSharedMetaReservedType ls_ckpt_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  ObSharedMetaReservedOperator reserved_operator;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.init(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      10L * 1000L * 1000L /*sql_timeout_us*/));
  ObArray<ObSharedMetaReservedInfo> reservations_before;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, reservations_before));

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  // Keep UploadTask running so the DAG has not exited when the PALF KV read fails.
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS,
      upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
  // Inject OB_IO_ERROR only after UploadTask blocks, reliably exercising the publication read
  // failure path.
  BootstrapPublicationErrorGuard palf_error_guard;
  ASSERT_EQ(OB_SUCCESS, palf_error_guard.init(
      upload_guard, "EN_SS_LS_CKPT_PUBLISHED_PALF_KV_GET", OB_IO_ERROR));
  // Release the blocked task and drain the DAG even if an assertion fails, avoiding hangs in later
  // tests.
  DEFER({
    upload_guard.release();
    executor.cancel_checkpoint_dag_and_wait();
  });

  // Cleanup must not overwrite a permanent error; return it unchanged after canceling the DAG.
  ASSERT_EQ(OB_IO_ERROR, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  ASSERT_EQ(OB_SUCCESS, palf_error_guard.wait_for_upload_entered());

  // On return, the scheduler has reclaimed the DAG and a running UploadTask has observed
  // cancellation.
  bool dag_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_FALSE(dag_exist);
  ASSERT_GT(upload_guard.get_cancel_count(), 0);

  // The same deferred-cleanup contract applies to cancellation after a permanent PALF error.
  // The retained reservation only delays GC until a later successful checkpoint cleans it up.
  ObArray<ObSharedMetaReservedInfo> reservations_after_cancel;
  ASSERT_EQ(OB_SUCCESS, reserved_operator.scan_rows(
      OB_SYS_TENANT_ID, ls_ckpt_type, 0 /*limit_count*/, reservations_after_cancel));
  ASSERT_EQ(reservations_before.count() + 1, reservations_after_cancel.count());
}

#endif

/**
 * Summary: Verify that a transient PALF recheck error after DAG exit leaves the outer polling loop
 * running.
 *
 * Steps:
 * 1. Return OB_ENTRY_NOT_EXIST on the first read and verify that the scheduler has no checkpoint
 *    DAG.
 * 2. Return each retryable error in turn on the second read.
 * 3. Retryable errors must yield success with is_published=false so the outer loop waits and
 *    retries.
 * 4. OB_ENTRY_NOT_EXIST must yield OB_ERR_UNEXPECTED; permanent errors must remain unchanged.
 */
TEST_F(TestSharedMetaService, bootstrap_palf_recheck_retryable_error_keeps_waiting)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  bool dag_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_FALSE(dag_exist);

  BootstrapPalfRecheckErrorGuard palf_error_guard;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  const int retryable_errors[] = {
      OB_EAGAIN,
      OB_TIMEOUT,
      OB_NOT_MASTER,
      OB_LOGSERVICE_RPC_ERROR,
      OB_STATE_NOT_MATCH,
  };
  const int64_t error_count = sizeof(retryable_errors) / sizeof(retryable_errors[0]);
  for (int64_t i = 0; i < error_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, palf_error_guard.init(retryable_errors[i]));
    bool is_published = true;
    ASSERT_EQ(OB_SUCCESS, executor.check_bootstrap_ckpt_published_once_(
        palf_kv_adapter, is_published));
    ASSERT_FALSE(is_published);
  }
  ASSERT_EQ(OB_SUCCESS, palf_error_guard.init(OB_ENTRY_NOT_EXIST));
  bool is_published = true;
  ASSERT_EQ(OB_ERR_UNEXPECTED, executor.check_bootstrap_ckpt_published_once_(
      palf_kv_adapter, is_published));
  ASSERT_FALSE(is_published);

  ASSERT_EQ(OB_SUCCESS, palf_error_guard.init(OB_IO_ERROR));
  is_published = true;
  ASSERT_EQ(OB_IO_ERROR, executor.check_bootstrap_ckpt_published_once_(
      palf_kv_adapter, is_published));
  ASSERT_FALSE(is_published);
}

#if __ENABLE_TRACEPOINT__
/**
 * Summary: Verify that BOOTSTRAP rejects a PALF KV value that cannot be deserialized as a valid
 * publication.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::get_published_ckpt_palf_kv_: Parse and validate the PALF KV publication
 *    value.
 * 2. ObSSLsCkptExecutor: Cancel and drain the running DAG when the publication value is invalid.
 *
 * Steps:
 * 1. Block UploadTask to keep the checkpoint DAG running.
 * 2. Inject a PALF KV payload that cannot be deserialized.
 * 3. Verify that do_checkpoint returns OB_INVALID_DATA after the DAG exits.
 * 4. Verify that an upload task observed cancellation.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_rejects_invalid_palf_value_and_cancels_dag)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  // Block UploadTask so the DAG remains running while the publication value is validated.
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS,
      upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
  // Return an invalid payload after UploadTask enters, causing PALF KV deserialization to fail.
  BootstrapPublishedPalfKvValueGuard palf_value_guard;
  ASSERT_EQ(OB_SUCCESS, palf_value_guard.init(upload_guard));
  DEFER({
    upload_guard.release();
    executor.cancel_checkpoint_dag_and_wait();
  });

  // Preserve OB_INVALID_DATA when publication parsing fails and cancel this DAG.
  ASSERT_EQ(OB_INVALID_DATA, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  ASSERT_EQ(OB_SUCCESS, palf_value_guard.wait_for_upload_entered());
  ASSERT_TRUE(palf_value_guard.is_value_injected());

  // Wait for the DAG to exit completely before returning; no background upload task may remain.
  bool dag_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_FALSE(dag_exist);
  ASSERT_GT(upload_guard.get_cancel_count(), 0);
}

/**
 * Summary: Verify that a well-formed object ID in PALF KV does not count as publication if its root
 * is missing.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::get_published_ckpt_palf_kv_: Check whether the checkpoint root object
 *    exists.
 * 2. ObSSLsCkptExecutor: Cancel and drain the running DAG when the published result is invalid.
 *
 * Steps:
 * 1. Build a valid checkpoint object ID and verify that its root object is absent.
 * 2. Return this object ID from the PALF KV read.
 * 3. Verify that do_checkpoint returns OB_INVALID_DATA after the DAG exits.
 * 4. Verify that an upload task observed cancellation.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_rejects_missing_published_root_and_cancels_dag)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  // Build a well-formed SYS_LS checkpoint root object ID using a unique SCN.
  const int64_t missing_root_scn = ObTimeUtility::current_time_ns();
  ObSSMetaLSCkptKey missing_root_key;
  missing_root_key.ls_id_ = SYS_LS;
  ASSERT_EQ(OB_SUCCESS, missing_root_key.meta_scn_.convert_for_inner_table_field(missing_root_scn));
  const blocksstable::MacroBlockId missing_root_id =
      missing_root_key.convert_to_macro_id(false /*is_in_table*/);
  ASSERT_TRUE(missing_root_id.is_valid());
  DEFER({
    EXPECT_EQ(OB_SUCCESS, delete_test_object(missing_root_id));
  });
  // Delete any object with this ID and verify its absence so leftover objects cannot affect the
  // result.
  const int delete_ret = OB_STORAGE_OBJECT_MGR.delete_object(
      missing_root_id, 0 /*ls_epoch_id*/, SYS_LS.id());
  ASSERT_TRUE(OB_SUCCESS == delete_ret || OB_OBJECT_NOT_EXIST == delete_ret);
  bool root_object_exist = true;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
      missing_root_id, 0 /*ls_epoch*/, root_object_exist, SYS_LS.id()));
  ASSERT_FALSE(root_object_exist);

  // Keep the DAG running, then return an ID with no corresponding root object as the published
  // result.
  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS,
      upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
  BootstrapPublishedPalfKvValueGuard palf_value_guard;
  ASSERT_EQ(OB_SUCCESS, palf_value_guard.init(upload_guard, missing_root_scn));
  DEFER({
    upload_guard.release();
    executor.cancel_checkpoint_dag_and_wait();
  });

  // Successful parsing is insufficient; a missing root object must still yield invalid data.
  ASSERT_EQ(OB_INVALID_DATA, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  ASSERT_EQ(OB_SUCCESS, palf_value_guard.wait_for_upload_entered());
  ASSERT_TRUE(palf_value_guard.is_value_injected());

  // Cancel and drain this DAG after validation fails.
  bool dag_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_FALSE(dag_exist);
  ASSERT_GT(upload_guard.get_cancel_count(), 0);
}

/**
 * Summary: Verify that BOOTSTRAP returns immediately if clearing old PALF KV fails, without
 * scheduling a new DAG.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::clear_old_bootstrap_palf_kv_: Handle read errors on the old publication
 *    entry.
 * 2. ObSSLsCkptExecutor::wait_bootstrap_ckpt_: Prevent scheduling when preliminary cleanup fails.
 *
 * Steps:
 * 1. Inject OB_IO_ERROR when reading the old PALF KV.
 * 2. Run a BOOTSTRAP checkpoint and verify that it returns the original OB_IO_ERROR.
 * 3. Verify that scheduling was never called and no checkpoint DAG remains.
 */
TEST_F(TestSharedMetaService, bootstrap_old_palf_error_returns_without_scheduling_dag)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  // Return a permanent error while clearing the old publication; scheduling must not continue.
  NamedEventGuard old_palf_error;
  ASSERT_EQ(OB_SUCCESS, old_palf_error.init("EN_SS_LS_CKPT_OLD_PALF_KV_GET", OB_IO_ERROR, 1));
  // Count scheduling calls to verify that no DAG is created after the old-value read fails.
  BootstrapScheduleDagGuard schedule_guard;
  ASSERT_EQ(OB_SUCCESS, schedule_guard.init(0 /*eagain_count*/));
  DEFER(executor.cancel_checkpoint_dag_and_wait());
  // Preserve the old-value read error; later steps must not overwrite it.
  ASSERT_EQ(OB_IO_ERROR, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  ASSERT_EQ(0, schedule_guard.get_call_count());

  // With no scheduling call, the scheduler must contain no checkpoint DAG.
  bool dag_exist = true;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_FALSE(dag_exist);
}

/**
 * Summary: Verify that BOOTSTRAP backs off and retries scheduling when the scheduler is briefly
 * busy with no conflict.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::schedule_bootstrap_ckpt_dag_with_retry_: Handle transient OB_EAGAIN.
 * 2. ObSSLsCkptExecutor::check_ckpt_dag_exist_: Distinguish a busy scheduler from a DAG conflict.
 *
 * Steps:
 * 1. Inject OB_EAGAIN on the first scheduling attempt while no DAG of the same type exists.
 * 2. Retry after local backoff and verify that the second attempt succeeds.
 * 3. Verify two scheduling calls and at least two concurrently running UploadTasks.
 */
TEST_F(TestSharedMetaService, bootstrap_retries_transient_schedule_eagain)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  static constexpr int64_t WAIT_CONFIG_TIMEOUT_US = 30L * 1000L * 1000L;
  static constexpr int64_t WAIT_CONFIG_INTERVAL_US = 10L * 1000L;
  static constexpr int64_t DEFAULT_THREAD_SCORE =
      ObSSLsCkptModePolicy::DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  const int64_t priority = share::ObDagPrio::DAG_PRIO_LS_CKPT;
  const int64_t saved_thread_score = GCONF.sys_ckpt_thread_score;
  auto wait_thread_score_and_limit = [&](const int64_t config_score,
                                         const int64_t expected_limit) -> int {
    int ret = OB_SUCCESS;
    bool matched = false;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + WAIT_CONFIG_TIMEOUT_US;
    while (OB_SUCC(ret) && !matched
           && ObTimeUtility::current_time() < deadline_us) {
      if (config_score == GCONF.sys_ckpt_thread_score) {
        int64_t thread_limit = 0;
        int64_t adaptive_limit = 0;
        if (OB_FAIL(scheduler->get_limit(priority, thread_limit))) {
          LOG_WARN("failed to get ls checkpoint thread limit", KR(ret),
                   K(priority));
        } else if (OB_FAIL(scheduler->get_adaptive_limit(
                       priority, adaptive_limit))) {
          LOG_WARN("failed to get ls checkpoint adaptive limit", KR(ret),
                   K(priority));
        } else {
          matched = expected_limit == thread_limit
              && expected_limit == adaptive_limit;
        }
      }
      if (OB_SUCC(ret) && !matched) {
        ob_usleep(WAIT_CONFIG_INTERVAL_US);
      }
    }
    if (OB_SUCC(ret) && !matched) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait sys checkpoint thread score timeout", KR(ret),
               K(config_score), K(expected_limit));
    }
    return ret;
  };
  DEFER({
    int ret = OB_SUCCESS;
    ObSqlString reset_sql;
    const int64_t saved_effective_score = 0 == saved_thread_score
        ? DEFAULT_THREAD_SCORE : saved_thread_score;
    if (OB_FAIL(reset_sql.assign_fmt(
            "alter system set sys_ckpt_thread_score = %ld",
            saved_thread_score))) {
      LOG_WARN("failed to build sys checkpoint thread score reset sql",
               KR(ret), K(saved_thread_score));
    } else if (OB_FAIL(sys_exe_sql(reset_sql.ptr()))) {
      LOG_WARN("failed to reset sys checkpoint thread score",
               KR(ret), K(saved_thread_score));
    } else if (OB_FAIL(wait_thread_score_and_limit(
                   saved_thread_score, saved_effective_score))) {
      LOG_WARN("failed to wait reset sys checkpoint thread score",
               KR(ret), K(saved_thread_score), K(saved_effective_score));
    }
  });
  // The same setting permits two lazily generated UploadTasks and two running slots in the LS_CKPT
  // queue.
  ASSERT_EQ(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = 2"));
  ASSERT_EQ(OB_SUCCESS,
      wait_thread_score_and_limit(BOOTSTRAP_MIN_CONCURRENT_UPLOAD_COUNT,
                                  BOOTSTRAP_MIN_CONCURRENT_UPLOAD_COUNT));

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  // Block uploads after the second scheduling attempt succeeds to verify that the DAG has started
  // running.
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS,
      upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
  // Simulate a busy scheduler on the first attempt; subsequent calls run normally.
  BootstrapScheduleDagGuard schedule_guard;
  ASSERT_EQ(OB_SUCCESS, schedule_guard.init(1 /*eagain_count*/));
  DEFER({
    upload_guard.release();
    executor.cancel_checkpoint_dag_and_wait();
  });

  // With no DAG of the same type, treat OB_EAGAIN as transient and retry locally.
  bool is_scheduled = false;
  ASSERT_EQ(OB_SUCCESS, executor.schedule_bootstrap_ckpt_dag_with_retry_(is_scheduled));
  ASSERT_TRUE(is_scheduled);
  // Two scheduling calls and a running UploadTask prove that the retry actually submitted the DAG.
  ASSERT_EQ(2, schedule_guard.get_call_count());
  ASSERT_EQ(OB_SUCCESS, upload_guard.wait_for_entered());
  // The first task remains blocked at the hook, so a second entry must come from another concurrent
  // UploadTask.
  ASSERT_EQ(OB_SUCCESS, upload_guard.wait_for_entered(
      BOOTSTRAP_MIN_CONCURRENT_UPLOAD_COUNT,
      BOOTSTRAP_CONCURRENT_UPLOAD_WAIT_TIMEOUT_US));
  ASSERT_GE(upload_guard.get_enter_count(),
      BOOTSTRAP_MIN_CONCURRENT_UPLOAD_COUNT);
}

/**
 * Summary: Verify that BOOTSTRAP reports an unexpected call sequence if a checkpoint DAG already
 * exists.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor::schedule_checkpoint_dag: Return OB_EAGAIN on a duplicate scheduling
 *    attempt.
 * 2. ObSSLsCkptExecutor::schedule_bootstrap_ckpt_dag_with_retry_: Treat an existing DAG as
 *    unexpected.
 * 3. ObSSLsCkptExecutor::wait_bootstrap_ckpt_: Leave the original DAG running on this error.
 *
 * Steps:
 * 1. Schedule and block a BOOTSTRAP checkpoint DAG.
 * 2. Schedule it again directly and verify OB_EAGAIN with no new DAG.
 * 3. Call the BOOTSTRAP entry point and verify OB_ERR_UNEXPECTED without another scheduling call.
 * 4. Verify that the original DAG is still running and has not observed cancellation.
 */
TEST_F(TestSharedMetaService, bootstrap_reports_unexpected_existing_dag)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  NamedEventGuard feature_guard;
  if (!GCONF.enable_logservice) {
    ASSERT_EQ(OB_SUCCESS, feature_guard.init_always_for_all_threads(
        "EN_SS_LS_CKPT_FORCE_ENABLE_WITHOUT_LOGSERVICE", OB_EAGAIN));
  }
  const bool saved_in_bootstrap = GCTX.in_bootstrap_;
  GCTX.in_bootstrap_ = true;
  DEFER(GCTX.in_bootstrap_ = saved_in_bootstrap);
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_SUCCESS, executor.init());
  BootstrapUploadGuard upload_guard;
  ASSERT_EQ(OB_SUCCESS,
      upload_guard.init(BOOTSTRAP_UPLOAD_BLOCK_UNTIL_DAG_CANCELED));
  NamedEventGuard old_palf_error;
  BootstrapScheduleDagGuard schedule_guard;
  DEFER({
    upload_guard.release();
    executor.cancel_checkpoint_dag_and_wait();
  });

  // Submit a real DAG and block its uploads to occupy the checkpoint singleton key.
  bool is_scheduled = false;
  ASSERT_EQ(OB_SUCCESS, executor.schedule_checkpoint_dag(
      ObSSLsCkptMode::BOOTSTRAP, is_scheduled));
  ASSERT_TRUE(is_scheduled);
  ASSERT_EQ(OB_SUCCESS, upload_guard.wait_for_entered());

  // A direct duplicate submission returns OB_EAGAIN because a DAG of the same type already exists.
  is_scheduled = true;
  ASSERT_EQ(OB_EAGAIN, executor.schedule_checkpoint_dag(
      ObSSLsCkptMode::BOOTSTRAP, is_scheduled));
  ASSERT_FALSE(is_scheduled);

  // The full BOOTSTRAP entry point must detect the existing DAG before scheduling and report an
  // invalid sequence.
  ASSERT_EQ(OB_SUCCESS, old_palf_error.init(
      "EN_SS_LS_CKPT_OLD_PALF_KV_GET", OB_ENTRY_NOT_EXIST, 1));
  ASSERT_EQ(OB_SUCCESS, schedule_guard.init(0 /*eagain_count*/));
  // The existing-DAG check must fail directly without calling create_and_add_dag again.
  ASSERT_EQ(OB_ERR_UNEXPECTED, executor.do_checkpoint(ObSSLsCkptMode::BOOTSTRAP));
  ASSERT_EQ(0, schedule_guard.get_call_count());

  // No new DAG was submitted, so the original running DAG must not be canceled.
  bool dag_exist = false;
  ASSERT_EQ(OB_SUCCESS, executor.check_ckpt_dag_exist_(dag_exist));
  ASSERT_TRUE(dag_exist);
  ASSERT_EQ(0, upload_guard.get_cancel_count());
}
#endif
#endif

static ObSSLsCkptUploadTask *find_ls_ckpt_upload_task(
    ObSSLsCkptDag &dag,
    const int64_t worker_idx)
{
  ObSSLsCkptUploadTask *upload_task = nullptr;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr == upload_task && nullptr != task && task_list_head != task) {
    ObSSLsCkptUploadTask *candidate =
        dynamic_cast<ObSSLsCkptUploadTask *>(task);
    if (nullptr != candidate && worker_idx == candidate->worker_idx_) {
      upload_task = candidate;
    }
    task = task->get_next();
  }
  return upload_task;
}

static int materialize_ls_ckpt_upload_tasks(ObSSLsCkptDag &dag)
{
  int ret = OB_SUCCESS;
  for (int64_t worker_idx = 0;
       OB_SUCC(ret) && worker_idx + 1 < dag.ctx_.get_upload_parallelism();
       ++worker_idx) {
    ObSSLsCkptUploadTask *upload_task =
        find_ls_ckpt_upload_task(dag, worker_idx);
    if (OB_ISNULL(upload_task)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(
        static_cast<ObITask *>(upload_task)->generate_next_task())) {
    } else {
      ObITask *duplicate_next_task = nullptr;
      if (OB_UNLIKELY(OB_ITER_END !=
          upload_task->generate_next_task(duplicate_next_task)
          || nullptr != duplicate_next_task)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSSLsCkptUploadTask *last_upload_task = find_ls_ckpt_upload_task(
        dag, dag.ctx_.get_upload_parallelism() - 1);
    const int64_t task_count = dag.get_task_list_count();
    if (OB_ISNULL(last_upload_task)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(
        static_cast<ObITask *>(last_upload_task)->generate_next_task())) {
    } else {
      ObITask *unexpected_next_task = nullptr;
      if (OB_UNLIKELY(task_count != dag.get_task_list_count()
                      || !last_upload_task->next_task_generated_
                      || OB_ITER_END != last_upload_task->generate_next_task(unexpected_next_task)
                      || nullptr != unexpected_next_task)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

/**
 * Summary: Verify that BOOTSTRAP and NORMAL share one DAG type and singleton key, using the
 * effective
 * sys_ckpt_thread_score and one UploadTask, respectively.
 *
 * Coverage:
 * 1. ObSSLsCkptExecutor: Reject invalid modes at the entry point.
 * 2. ObSSLsCkptDag: Check lazy task counts, dependencies, concurrency limits, and priorities for
 *    both modes.
 * 3. DAG: Use identical equality/hash across modes to provide one singleton key for scheduler
 *    deduplication.
 *
 * Steps:
 * 1. Verify that the execution entry point rejects MAX_MODE.
 * 2. Create a BOOTSTRAP DAG and check initial tasks and lazy Prepare -> Upload -> Finish
 *    dependencies.
 * 3. Create a NORMAL DAG and verify one UploadTask and the same key as the BOOTSTRAP DAG.
 */
TEST_F(TestSharedMetaService, ls_ckpt_modes_have_expected_single_dag_topology_and_singleton_key)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));

  // Reject MAX_MODE at the execution entry point so invalid modes never reach scheduling.
  ObSSLsCkptExecutor executor;
  ASSERT_EQ(OB_INVALID_ARGUMENT, executor.do_checkpoint(ObSSLsCkptMode::MAX_MODE));

  ObArenaAllocator dag_allocator("BtCkptDag");
  ObSSLsCkptDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  // DAG parameters must validate the LS ID as well as the mode.
  ObSSLsCkptDagParam invalid_param(
      ObSSLsCkptMode::BOOTSTRAP, -1, ObLSID());
  ASSERT_FALSE(invalid_param.is_valid());
  // BOOTSTRAP initially creates Prepare, the first Upload, and Finish; the scheduler creates other
  // UploadTasks lazily.
  ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  const int64_t bootstrap_parallelism = dag.ctx_.get_upload_parallelism();
  ASSERT_GE(bootstrap_parallelism, 1);
  ASSERT_EQ(3, dag.get_task_list_count());
  ASSERT_EQ(bootstrap_parallelism, dag.get_max_concurrent_task_cnt());
  ASSERT_EQ(share::ObDagPrio::DAG_PRIO_LS_CKPT, dag.get_priority());
  ASSERT_EQ(OB_SUCCESS, materialize_ls_ckpt_upload_tasks(dag));
  ASSERT_EQ(bootstrap_parallelism + 2, dag.get_task_list_count());

  // After full expansion, collect all three task types and verify that UploadTask worker indices
  // are unique.
  ObSSLsCkptPrepareTask *prepare_task = nullptr;
  ObSSLsCkptFinishTask *finish_task = nullptr;
  ObSEArray<ObSSLsCkptUploadTask *,
      ObSSLsCkptModePolicy::DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM>
      upload_tasks;
  ObSEArray<int64_t, ObSSLsCkptModePolicy::DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM>
      worker_indices;
  ObITask *task = dag.task_list_.get_first();
  const ObITask *task_list_head = dag.task_list_.get_header();
  while (nullptr != task && task_list_head != task) {
    ASSERT_EQ(&dag, task->get_dag());
    ASSERT_EQ(ObITask::TASK_STATUS_WAITING, task->get_status());
    ASSERT_EQ(ObITask::TASK_TYPE_SS_LS_CKPT, task->get_type());
    if (nullptr != dynamic_cast<ObSSLsCkptPrepareTask *>(task)) {
      ASSERT_EQ(nullptr, prepare_task);
      prepare_task = static_cast<ObSSLsCkptPrepareTask *>(task);
    } else if (nullptr != dynamic_cast<ObSSLsCkptUploadTask *>(task)) {
      ObSSLsCkptUploadTask *upload_task =
          static_cast<ObSSLsCkptUploadTask *>(task);
      ASSERT_GE(upload_task->worker_idx_, 0);
      ASSERT_LT(upload_task->worker_idx_, bootstrap_parallelism);
      for (int64_t i = 0; i < worker_indices.count(); ++i) {
        ASSERT_NE(worker_indices.at(i), upload_task->worker_idx_);
      }
      ASSERT_EQ(OB_SUCCESS, worker_indices.push_back(upload_task->worker_idx_));
      ASSERT_EQ(OB_SUCCESS, upload_tasks.push_back(upload_task));
    } else if (nullptr != dynamic_cast<ObSSLsCkptFinishTask *>(task)) {
      ASSERT_EQ(nullptr, finish_task);
      finish_task = static_cast<ObSSLsCkptFinishTask *>(task);
    } else {
      FAIL() << "unexpected bootstrap ls checkpoint task type";
    }
    task = task->get_next();
  }
  // Prepare releases only the first Upload; later UploadTasks inherit the Finish dependency but not
  // each other.
  ASSERT_NE(nullptr, prepare_task);
  ASSERT_NE(nullptr, finish_task);
  ASSERT_EQ(bootstrap_parallelism, upload_tasks.count());
  ASSERT_EQ(0, prepare_task->get_indegree());
  ASSERT_EQ(bootstrap_parallelism, finish_task->get_indegree());

  ObSEArray<ObINodeWithChild *,
      ObSSLsCkptModePolicy::DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM> children;
  ASSERT_EQ(OB_SUCCESS, prepare_task->copy_child_nodes(children));
  ASSERT_EQ(1, children.count());
  ASSERT_EQ(static_cast<ObINodeWithChild *>(
      find_ls_ckpt_upload_task(dag, 0)), children.at(0));
  for (int64_t i = 0; i < upload_tasks.count(); ++i) {
    ASSERT_EQ(0 == upload_tasks.at(i)->worker_idx_ ? 1 : 0,
        upload_tasks.at(i)->get_indegree());
    children.reset();
    ASSERT_EQ(OB_SUCCESS, upload_tasks.at(i)->copy_child_nodes(children));
    ASSERT_EQ(1, children.count());
    ASSERT_EQ(static_cast<ObINodeWithChild *>(finish_task), children.at(0));
  }
  children.reset();
  ASSERT_EQ(OB_SUCCESS, finish_task->copy_child_nodes(children));
  ASSERT_EQ(0, children.count());

  // NORMAL uses the same DAG type with the UploadTask count reduced to one.
  ObArenaAllocator normal_write_allocator("NormalCkptWrite");
  ObSSLsCkptDag normal_write_dag;
  ASSERT_EQ(OB_SUCCESS, normal_write_dag.basic_init(normal_write_allocator));
  ObSSLsCkptDagParam normal_write_param(ObSSLsCkptMode::NORMAL, 1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, normal_write_dag.init_by_param(&normal_write_param));
  ASSERT_EQ(OB_SUCCESS, normal_write_dag.create_first_task());
  ASSERT_EQ(ObSSLsCkptModePolicy::NORMAL_UPLOAD_PARALLELISM + 2,
      normal_write_dag.get_task_list_count());
  ASSERT_EQ(ObSSLsCkptModePolicy::NORMAL_UPLOAD_PARALLELISM,
      normal_write_dag.get_max_concurrent_task_cnt());
  ASSERT_EQ(share::ObDagPrio::DAG_PRIO_LS_CKPT, normal_write_dag.get_priority());
  // The singleton key excludes mode, so the scheduler must treat both modes as the same DAG.
  ASSERT_TRUE(dag == normal_write_dag);
  ASSERT_TRUE(normal_write_dag == dag);
  ASSERT_EQ(dag.hash(), normal_write_dag.hash());
  ASSERT_EQ(share::ObDagType::DAG_TYPE_SS_LS_CKPT, dag.get_type());
  ASSERT_EQ(share::ObDagType::DAG_TYPE_SS_LS_CKPT, normal_write_dag.get_type());
}

/**
 * Summary: Verify that sys_ckpt_thread_score controls both LS_CKPT queue concurrency and BOOTSTRAP
 * uploads.
 *
 * Coverage:
 * 1. sys_ckpt_thread_score: Check the startup value, valid range, zero semantics, and BOOTSTRAP DAG
 *    snapshot.
 * 2. ObTenantDagScheduler: Check the initial LS_CKPT queue limits and automatic cluster
 *    configuration refresh.
 *
 * Steps:
 * 1. Verify that 32, loaded through the observer -o parsing path, applies to the scheduler and
 *    first BOOTSTRAP DAG.
 * 2. Reject -1 and 101, accept 0, 1, 3, and 100, and match queue concurrency to the effective
 *    value.
 * 3. After changing 3 to 1, keep an initialized BOOTSTRAP DAG at 3 while a new DAG uses 1.
 * 4. With 0 and 100, lazily create 16 and 100 UploadTasks, respectively, in new BOOTSTRAP DAGs.
 * 5. With 100, keep NORMAL at one UploadTask; restore the original configuration on exit.
 */
TEST_F(TestSharedMetaService, sys_ckpt_thread_score_controls_scheduler_and_bootstrap)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  static constexpr int64_t DEFAULT_THREAD_SCORE = 16;
  static constexpr int64_t WAIT_CONFIG_TIMEOUT_US = 30L * 1000L * 1000L;
  static constexpr int64_t WAIT_CONFIG_INTERVAL_US = 10L * 1000L;
  const int64_t priority = share::ObDagPrio::DAG_PRIO_LS_CKPT;
  ASSERT_EQ(DEFAULT_THREAD_SCORE, share::OB_DAG_PRIOS[priority].score_);

  // main() supplies 32 through the observer -o parsing entry point before starting the simple
  // server.
  ASSERT_EQ(SYS_CKPT_STARTUP_THREAD_SCORE,
      static_cast<int64_t>(GCONF.sys_ckpt_thread_score));
  int64_t startup_limit = 0;
  int64_t startup_adaptive_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(priority, startup_limit));
  ASSERT_EQ(OB_SUCCESS,
      scheduler->get_adaptive_limit(priority, startup_adaptive_limit));
  ASSERT_EQ(SYS_CKPT_STARTUP_THREAD_SCORE, startup_limit);
  ASSERT_EQ(SYS_CKPT_STARTUP_THREAD_SCORE, startup_adaptive_limit);

  ObSSLsCkptDagParam startup_param(
      ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ObArenaAllocator startup_allocator("CkptCfgStart");
  ObSSLsCkptDag startup_dag;
  ASSERT_EQ(OB_SUCCESS, startup_dag.basic_init(startup_allocator));
  ASSERT_EQ(OB_SUCCESS, startup_dag.init_by_param(&startup_param));
  ASSERT_EQ(OB_SUCCESS, startup_dag.create_first_task());
  ASSERT_EQ(SYS_CKPT_STARTUP_THREAD_SCORE,
      startup_dag.ctx_.get_upload_parallelism());
  ASSERT_EQ(SYS_CKPT_STARTUP_THREAD_SCORE,
      startup_dag.get_max_concurrent_task_cnt());
  ASSERT_EQ(OB_SUCCESS, materialize_ls_ckpt_upload_tasks(startup_dag));
  ASSERT_EQ(SYS_CKPT_STARTUP_THREAD_SCORE + 2,
      startup_dag.get_task_list_count());

  // Save the original cluster setting before this test changes it.
  const int64_t saved_config_score = GCONF.sys_ckpt_thread_score;

  // Configuration refresh is asynchronous; wait for the new value, refresh the scheduler, then
  // check both limits.
  auto wait_config_and_limit = [&](const int64_t config_score,
                                   const int64_t expected_limit) -> int {
    int ret = OB_SUCCESS;
    bool matched = false;
    const int64_t deadline_us =
        ObTimeUtility::current_time() + WAIT_CONFIG_TIMEOUT_US;
    while (OB_SUCC(ret) && !matched
           && ObTimeUtility::current_time() < deadline_us) {
      if (config_score == GCONF.sys_ckpt_thread_score) {
        int64_t actual_limit = 0;
        if (OB_FAIL(scheduler->get_limit(priority, actual_limit))) {
          LOG_WARN("failed to get ls checkpoint thread limit", KR(ret),
                   K(priority));
        } else {
          int64_t actual_adaptive_limit = 0;
          if (OB_FAIL(scheduler->get_adaptive_limit(
                  priority, actual_adaptive_limit))) {
            LOG_WARN("failed to get ls checkpoint adaptive limit", KR(ret),
                     K(priority));
          } else {
            matched = expected_limit == actual_limit
                && expected_limit == actual_adaptive_limit;
          }
        }
      }
      if (OB_SUCC(ret) && !matched) {
        ob_usleep(WAIT_CONFIG_INTERVAL_US);
      }
    }
    if (OB_SUCC(ret) && !matched) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait sys checkpoint thread score timeout", KR(ret),
               K(config_score), K(expected_limit));
    }
    return ret;
  };
  // Restore the setting and concurrency limits on exit to keep subsequent tests isolated.
  DEFER({
    int ret = OB_SUCCESS;
    ObSqlString reset_sql;
    const int64_t saved_effective_score = 0 == saved_config_score
        ? DEFAULT_THREAD_SCORE : saved_config_score;
    if (OB_FAIL(reset_sql.assign_fmt(
            "alter system set sys_ckpt_thread_score = %ld",
            saved_config_score))) {
      LOG_WARN("failed to build reset sys checkpoint thread score sql",
               KR(ret), K(saved_config_score));
    } else if (OB_FAIL(sys_exe_sql(reset_sql.ptr()))) {
      LOG_WARN("failed to reset sys checkpoint thread score",
               KR(ret), K(saved_config_score));
    } else if (OB_FAIL(wait_config_and_limit(
                   saved_config_score, saved_effective_score))) {
      LOG_WARN("failed to wait reset sys checkpoint thread score",
               KR(ret), K(saved_config_score), K(saved_effective_score));
    }
  });

  // Reject -1 and 101 during parameter validation because they exceed the lower and upper bounds.
  ASSERT_NE(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = -1"));
  ASSERT_NE(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = 101"));

  // DAG initialization snapshots the setting; later changes must not alter this DAG's UploadTask
  // count.
  ASSERT_EQ(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = 3"));
  ASSERT_EQ(OB_SUCCESS, wait_config_and_limit(3, 3));
  ObSSLsCkptDagParam bootstrap_param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ObArenaAllocator snapshot_allocator("CkptCfgSnap");
  ObSSLsCkptDag snapshot_dag;
  ASSERT_EQ(OB_SUCCESS, snapshot_dag.basic_init(snapshot_allocator));
  ASSERT_EQ(OB_SUCCESS, snapshot_dag.init_by_param(&bootstrap_param));
  ASSERT_EQ(3, snapshot_dag.ctx_.get_upload_parallelism());
  ASSERT_EQ(3, snapshot_dag.get_max_concurrent_task_cnt());

  ASSERT_EQ(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = 1"));
  ASSERT_EQ(OB_SUCCESS, wait_config_and_limit(1, 1));
  ASSERT_EQ(OB_SUCCESS, snapshot_dag.create_first_task());
  ASSERT_EQ(3, snapshot_dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, materialize_ls_ckpt_upload_tasks(snapshot_dag));
  ASSERT_EQ(5, snapshot_dag.get_task_list_count());
  ASSERT_EQ(3, snapshot_dag.get_max_concurrent_task_cnt());

  ObArenaAllocator current_allocator("CkptCfgCur");
  ObSSLsCkptDag current_dag;
  ASSERT_EQ(OB_SUCCESS, current_dag.basic_init(current_allocator));
  ASSERT_EQ(OB_SUCCESS, current_dag.init_by_param(&bootstrap_param));
  ASSERT_EQ(OB_SUCCESS, current_dag.create_first_task());
  ASSERT_EQ(1, current_dag.ctx_.get_upload_parallelism());
  ASSERT_EQ(3, current_dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, materialize_ls_ckpt_upload_tasks(current_dag));
  ASSERT_EQ(3, current_dag.get_task_list_count());
  ASSERT_EQ(1, current_dag.get_max_concurrent_task_cnt());

  // Zero resets concurrency to the default of 16 for both the scheduler and new BOOTSTRAP DAGs.
  ASSERT_EQ(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = 0"));
  ASSERT_EQ(OB_SUCCESS,
      wait_config_and_limit(0, DEFAULT_THREAD_SCORE));
  ObArenaAllocator default_allocator("CkptCfgDef");
  ObSSLsCkptDag default_dag;
  ASSERT_EQ(OB_SUCCESS, default_dag.basic_init(default_allocator));
  ASSERT_EQ(OB_SUCCESS, default_dag.init_by_param(&bootstrap_param));
  ASSERT_EQ(OB_SUCCESS, default_dag.create_first_task());
  ASSERT_EQ(DEFAULT_THREAD_SCORE, default_dag.ctx_.get_upload_parallelism());
  ASSERT_EQ(3, default_dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, materialize_ls_ckpt_upload_tasks(default_dag));
  ASSERT_EQ(DEFAULT_THREAD_SCORE + 2, default_dag.get_task_list_count());
  ASSERT_EQ(DEFAULT_THREAD_SCORE, default_dag.get_max_concurrent_task_cnt());

  // Check the valid upper bound of 100; NORMAL still uses exactly one UploadTask.
  ASSERT_EQ(OB_SUCCESS,
      sys_exe_sql("alter system set sys_ckpt_thread_score = 100"));
  ASSERT_EQ(OB_SUCCESS,
      wait_config_and_limit(100, 100));
  ObArenaAllocator max_allocator("CkptCfgMax");
  ObSSLsCkptDag max_dag;
  ASSERT_EQ(OB_SUCCESS, max_dag.basic_init(max_allocator));
  ASSERT_EQ(OB_SUCCESS, max_dag.init_by_param(&bootstrap_param));
  ASSERT_EQ(OB_SUCCESS, max_dag.create_first_task());
  ASSERT_EQ(100, max_dag.ctx_.get_upload_parallelism());
  ASSERT_EQ(3, max_dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, materialize_ls_ckpt_upload_tasks(max_dag));
  ASSERT_EQ(102, max_dag.get_task_list_count());
  ASSERT_EQ(100, max_dag.get_max_concurrent_task_cnt());

  ObArenaAllocator normal_allocator("CkptCfgNorm");
  ObSSLsCkptDag normal_dag;
  ASSERT_EQ(OB_SUCCESS, normal_dag.basic_init(normal_allocator));
  ObSSLsCkptDagParam normal_param(ObSSLsCkptMode::NORMAL, 1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, normal_dag.init_by_param(&normal_param));
  ASSERT_EQ(OB_SUCCESS, normal_dag.create_first_task());
  ASSERT_EQ(ObSSLsCkptModePolicy::NORMAL_UPLOAD_PARALLELISM,
      normal_dag.ctx_.get_upload_parallelism());
  ASSERT_EQ(ObSSLsCkptModePolicy::NORMAL_UPLOAD_PARALLELISM + 2,
      normal_dag.get_task_list_count());
  ASSERT_EQ(ObSSLsCkptModePolicy::NORMAL_UPLOAD_PARALLELISM,
      normal_dag.get_max_concurrent_task_cnt());
}

/**
 * Summary: Verify that the DAG context retains only the first recorded error when UploadTasks fail
 * concurrently.
 *
 * Coverage:
 * 1. ObSSLsCkptWriteCtx::record_error: Record the first error using CAS.
 * 2. ObSSLsCkptWriteCtx::get_first_error: Read the retained error code.
 *
 * Steps:
 * 1. Record OB_IO_ERROR and OB_TIMEOUT concurrently from two threads.
 * 2. Verify that the final error is one of these two candidates.
 * 3. Record OB_CANCELED and verify that it does not overwrite the existing error.
 */
TEST_F(TestSharedMetaService, ls_ckpt_first_error_cas_keeps_one_concurrent_error)
{
  ObArenaAllocator dag_allocator("CkptErrCas");
  ObSSLsCkptDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSLsCkptWriteCtx &ctx = dag.ctx_;

  // Release both threads only after they are ready, creating actual CAS contention.
  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start(false);
  auto record_error = [&](const int error_code) {
    ready_count.fetch_add(1, std::memory_order_release);
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    ctx.record_error(error_code);
  };
  std::thread io_error_thread(record_error, OB_IO_ERROR);
  std::thread timeout_thread(record_error, OB_TIMEOUT);
  while (2 != ready_count.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  start.store(true, std::memory_order_release);
  io_error_thread.join();
  timeout_thread.join();

  // Either thread may win the CAS, but exactly one of their errors must be retained.
  const int concurrent_error = ctx.get_first_error();
  ASSERT_TRUE(OB_IO_ERROR == concurrent_error || OB_TIMEOUT == concurrent_error);
  // Once dag_ret_ holds a failure, cancellation must not overwrite the first error.
  ctx.record_error(OB_CANCELED);
  ASSERT_EQ(concurrent_error, ctx.get_first_error());
}

/**
 * Summary: Verify that other UploadTasks stop claiming tablets after a real upload failure in one
 * task.
 *
 * Coverage:
 * 1. ObSSLsCkptUploadTask: Record the upload_tablet_meta_ error in the shared context.
 * 2. ObSSLsCkptWriteCtx: Provide a shared stop state and first error to all UploadTasks.
 *
 * Steps:
 * 1. Prepare two invalid tablet keys so the first UploadTask fails at the real upload entry point.
 * 2. Verify that the context records the first error and advances the claim index only once.
 * 3. Run the second UploadTask and verify that it claims no tablet.
 * 4. Verify that the first upload error is still retained.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_upload_failure_stops_other_workers)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_NE(nullptr, GCTX.sql_proxy_);

  ObArenaAllocator dag_allocator("CkptUploadErr");
  ObSSLsCkptDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSLsCkptWriteCtx &ctx = dag.ctx_;

  // Use an invalid key to exercise a real upload failure in the first worker without changing
  // dag_ret_ directly.
  ObSSMetaTabletKey invalid_tablet_key;
  ASSERT_FALSE(invalid_tablet_key.is_valid());
  ASSERT_EQ(OB_SUCCESS, ctx.tablet_keys_.push_back(invalid_tablet_key));
  ASSERT_EQ(OB_SUCCESS, ctx.tablet_keys_.push_back(invalid_tablet_key));

  // UploadTask stores the operation error in the context and returns success so FinishTask can
  // still clean up.
  ObSSLsCkptUploadTask first_worker;
  ASSERT_EQ(OB_SUCCESS, first_worker.init(ctx, 0));
  ASSERT_EQ(OB_SUCCESS, first_worker.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.get_first_error());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.check_status());
  ASSERT_EQ(1, ctx.get_next_upload_idx());

  // The second worker must observe the first error before claiming the tablet at index 1.
  ObSSLsCkptUploadTask second_worker;
  ASSERT_EQ(OB_SUCCESS, second_worker.init(ctx, 1));
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ITER_END, second_worker.generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_TRUE(second_worker.next_task_generated_);
  ASSERT_EQ(OB_SUCCESS, second_worker.process());
  ASSERT_EQ(1, ctx.get_next_upload_idx());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.get_first_error());
}

/**
 * Summary: Verify that FinishTask builds the object ID list in scan order despite parallel tablet
 * uploads.
 *
 * Coverage:
 * 1. ObSSLsCkptFinishTask::collect_tablet_meta_object_ids_: Collect object IDs in tablet_keys_
 *    order.
 * 2. ObSSLsCkptFinishTask::process: Check the context error after writing the root, before publication.
 *
 * Steps:
 * 1. Prepare one more tablet than the upload concurrency and build the expected object ID list.
 * 2. Collect object IDs and verify their order one by one.
 * 3. Record OB_IO_ERROR in the context, then run FinishTask.
 * 4. Verify that FinishTask writes the root in scan order but returns the error before publication.
 */
TEST_F(TestSharedMetaService, bootstrap_ls_ckpt_finish_aggregates_in_scan_order)
{
  share::ObTenantSwitchGuard sys_guard;
  OK(sys_guard.switch_to(OB_SYS_TENANT_ID));
  SysLsCkptPauseGuard ckpt_pause_guard;
  ASSERT_EQ(OB_SUCCESS, ckpt_pause_guard.pause());

  ObArenaAllocator dag_allocator("CkptFinish");
  ObSSLsCkptDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  ObSSLsCkptDagParam param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSLsCkptWriteCtx &ctx = dag.ctx_;
  ObSSLsCkptData &ckpt_data = ctx.ckpt_data_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(SYS_LS, ckpt_data.ls_meta_));
  ckpt_data.reserved_meta_scn_ = share::SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, make_scn(
      static_cast<uint64_t>(ObTimeUtility::current_time_ns()), ckpt_data.ckpt_scn_));
  ckpt_data.ckpt_ts_ = ObTimeUtility::current_time();
  ASSERT_TRUE(ckpt_data.is_valid());

  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = SYS_LS;
  ckpt_key.meta_scn_ = ckpt_data.ckpt_scn_;
  const blocksstable::MacroBlockId root_id = ckpt_key.convert_to_macro_id(false /*is_in_table*/);
  ASSERT_TRUE(root_id.is_valid());
  DEFER(EXPECT_EQ(OB_SUCCESS, delete_test_object(root_id)));

  // Use more tablets than concurrent workers; the final list must still preserve scan order.
  const int64_t tablet_count = ctx.get_upload_parallelism() + 1;
  ObSEArray<blocksstable::MacroBlockId, 16> expected_ids;
  for (int64_t i = 0; i < tablet_count; ++i) {
    ObSSMetaTabletKey tablet_key;
    tablet_key.ls_id_ = SYS_LS;
    tablet_key.tablet_id_ = ObTabletID(1001 + i);
    tablet_key.reorganization_scn_ = share::SCN::min_scn();
    tablet_key.meta_scn_ = share::SCN::plus(ckpt_data.reserved_meta_scn_, i + 1);
    ASSERT_TRUE(tablet_key.is_valid());
    ASSERT_EQ(OB_SUCCESS, ctx.tablet_keys_.push_back(tablet_key));
    const blocksstable::MacroBlockId object_id =
        tablet_key.convert_to_macro_id(false /*is_in_table*/);
    ASSERT_TRUE(object_id.is_valid());
    ASSERT_EQ(OB_SUCCESS, expected_ids.push_back(object_id));
  }

  // FinishTask rebuilds the list from the original tablet_keys_, independently of worker completion
  // order.
  ObSSLsCkptFinishTask finish_task;
  finish_task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, finish_task.init(ctx));
  ASSERT_EQ(OB_SUCCESS, finish_task.collect_tablet_meta_object_ids_());
  ASSERT_EQ(expected_ids.count(), ckpt_data.tablet_meta_keys_.count());
  for (int64_t i = 0; i < expected_ids.count(); ++i) {
    ASSERT_EQ(expected_ids.at(i), ckpt_data.tablet_meta_keys_.at(i));
  }

  // Rebuild the root in scan order, then reject publication using the recorded context error.
  ckpt_data.tablet_meta_keys_.reset();
  ctx.record_error(OB_IO_ERROR);
#ifdef ERRSIM
  NamedEventGuard publish_error_guard;
  ASSERT_EQ(OB_SUCCESS, publish_error_guard.init_for_condition(
      "EN_SS_LS_CKPT_PALF_PUBLISH", OB_TIMEOUT, 1, static_cast<int64_t>(ObSSLsCkptMode::BOOTSTRAP)));
#endif
  ASSERT_EQ(OB_IO_ERROR, finish_task.process());
  ASSERT_EQ(OB_IO_ERROR, ctx.get_first_error());
#ifdef ERRSIM
  ASSERT_EQ(0, publish_error_guard.get_trigger_count());
#endif
  ASSERT_EQ(expected_ids.count(), ckpt_data.tablet_meta_keys_.count());
  for (int64_t i = 0; i < expected_ids.count(); ++i) {
    ASSERT_EQ(expected_ids.at(i), ckpt_data.tablet_meta_keys_.at(i));
  }
  bool root_exists = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(
      root_id, 0 /*ls_epoch*/, root_exists, SYS_LS.id()));
  ASSERT_TRUE(root_exists);
}

} // namespace unittest
} // namespace oceanbase
