/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX PL

#include "pl/external_routine/ob_java_udf_proxy.h"
#include "pl/external_routine/ob_java_udf.h"
#include "pl/external_routine/ob_java_udaf.h"
#include "pl/external_routine/ob_java_utils.h"
#include "pl/external_routine/proto/ob_pl_java_udf.pb-c.h"
#include "observer/ob_sandbox_manager.h"
#include "sql/ob_spi.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "share/config/ob_server_config.h"
#include "share/ob_lob_access_utils.h"
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <libgen.h>
#include <sys/stat.h>

namespace oceanbase
{
namespace pl
{

static const int64_t JAVA_SANDBOX_DRAIN_TIMEOUT_US = 30 * 1000 * 1000L;
static const int64_t JAVA_SANDBOX_ROLLOVER_WINDOW_US = 60 * 1000 * 1000L;
static const int64_t JAVA_SANDBOX_ROLLOVER_COOLDOWN_US = 10 * 1000 * 1000L;
static const int64_t JAVA_SANDBOX_ROLLOVER_THRESHOLD = 3;
// Leave enough time for the sandbox to report a cooperative LOAD_JAR timeout
// before the observer-side receive reaches the SQL deadline.
static const int64_t JAVA_SANDBOX_LOAD_JAR_RESPONSE_MARGIN_US = 200 * 1000L;

static bool is_java_sandbox_process_running(const JavaSandboxInstance *instance)
{
  bool is_running = false;
  if (OB_NOT_NULL(instance)) {
    observer::ObSandboxProcess *process = instance->get_sandbox_process();
    is_running = OB_NOT_NULL(process)
                 && process->get_pid() > 0
                 && observer::SandboxState::STATE_RUNNING == process->get_state();
  }
  return is_running;
}

JavaSandboxInstance::JavaSandboxInstance()
  : retired_epoch_(0),
    force_killed_(false),
    kill_reason_(JSDR_NONE),
    generation_id_(0),
    state_(JSS_CREATING),
    drain_reason_(JSDR_NONE),
    channel_pool_(),
    sandbox_process_(nullptr),
    ref_count_(0),
    inflight_count_(0),
    oom_count_(0),
    last_oom_ts_(0),
    timeout_count_(0),
    create_ts_(0),
    last_request_ts_(0),
    drain_deadline_ts_(0),
    max_inflight_deadline_ts_(0),
    process_destroyed_(false)
{
}

JavaSandboxInstance::~JavaSandboxInstance()
{
  destroy();
}

int JavaSandboxInstance::init(int64_t generation_id)
{
  int ret = OB_SUCCESS;
  generation_id_ = generation_id;
  ATOMIC_STORE(&state_, static_cast<int64_t>(JSS_CREATING));
  ATOMIC_STORE(&drain_reason_, static_cast<int64_t>(JSDR_NONE));
  ATOMIC_STORE(&ref_count_, 0);
  ATOMIC_STORE(&inflight_count_, 0);
  ATOMIC_STORE(&oom_count_, 0);
  ATOMIC_STORE(&last_oom_ts_, 0);
  ATOMIC_STORE(&timeout_count_, 0);
  create_ts_ = ObTimeUtility::current_time();
  last_request_ts_ = create_ts_;
  drain_deadline_ts_ = 0;
  max_inflight_deadline_ts_ = 0;
  process_destroyed_ = false;
  retired_epoch_ = 0;
  force_killed_ = false;
  kill_reason_ = JSDR_NONE;
  return ret;
}

void JavaSandboxInstance::destroy()
{
  channel_pool_.wait_all_released();
  if (OB_NOT_NULL(sandbox_process_)) {
    if (!process_destroyed_) {
      sandbox_process_->destroy();
      process_destroyed_ = true;
    }
    sandbox_process_->~ObSandboxProcess();
    ob_free(sandbox_process_);
    sandbox_process_ = nullptr;
  }
  channel_pool_.destroy();
  ATOMIC_STORE(&state_, static_cast<int64_t>(JSS_DEAD));
}

void JavaSandboxInstance::stop_process()
{
  if (OB_NOT_NULL(sandbox_process_) && !process_destroyed_) {
    sandbox_process_->destroy();
    process_destroyed_ = true;
  }
  channel_pool_.destroy();
  ATOMIC_STORE(&state_, static_cast<int64_t>(JSS_STOPPING));
}

void JavaSandboxInstance::update_max_inflight_deadline_ts(int64_t deadline_ts)
{
  int64_t old_deadline = ATOMIC_LOAD(&max_inflight_deadline_ts_);
  while (deadline_ts > old_deadline
         && !ATOMIC_BCAS(&max_inflight_deadline_ts_, old_deadline, deadline_ts)) {
    old_deadline = ATOMIC_LOAD(&max_inflight_deadline_ts_);
  }
}

int64_t JavaSandboxInstance::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(generation_id),
       "state", ATOMIC_LOAD(&state_),
       "drain_reason", ATOMIC_LOAD(&drain_reason_),
       "ref_count", ATOMIC_LOAD(&ref_count_),
       "inflight_count", ATOMIC_LOAD(&inflight_count_),
       "oom_count", ATOMIC_LOAD(&oom_count_),
       "last_oom_ts", ATOMIC_LOAD(&last_oom_ts_),
       "timeout_count", ATOMIC_LOAD(&timeout_count_),
       "drain_deadline_ts", ATOMIC_LOAD(&drain_deadline_ts_),
       K_(retired_epoch),
       K_(force_killed),
       K_(kill_reason));
  J_OBJ_END();
  return pos;
}

class ObJavaUDFProxy::ActiveReadGuard {
public:
  explicit ActiveReadGuard(ObJavaUDFProxy &proxy)
    : proxy_(proxy), epoch_(0), bucket_(-1), valid_(false)
  {
    if (OB_SUCCESS == proxy_.enter_active_read(epoch_, bucket_)) {
      valid_ = true;
    }
  }
  ~ActiveReadGuard()
  {
    if (valid_) {
      proxy_.exit_active_read(bucket_);
    }
  }
  bool is_valid() const { return valid_; }
  int64_t get_epoch() const { return epoch_; }

private:
  ObJavaUDFProxy &proxy_;
  int64_t epoch_;
  int64_t bucket_;
  bool valid_;
  DISALLOW_COPY_AND_ASSIGN(ActiveReadGuard);
};

class ObJavaUDFProxy::JavaSandboxRequestHandle {
public:
  JavaSandboxRequestHandle()
    : proxy_(nullptr), instance_(nullptr), generation_id_(0), released_(false)
  {
  }
  ~JavaSandboxRequestHandle()
  {
    release();
  }
  void bind(ObJavaUDFProxy *proxy, JavaSandboxInstance *instance)
  {
    proxy_ = proxy;
    instance_ = instance;
    generation_id_ = OB_NOT_NULL(instance) ? instance->get_generation_id() : 0;
    released_ = false;
  }
  void release()
  {
    if (OB_NOT_NULL(instance_) && !released_) {
      const int64_t inflight = instance_->dec_inflight();
      const bool need_cleanup = (0 == inflight && JSS_DRAINING == instance_->get_state());
      instance_->dec_ref();
      ObJavaUDFProxy *proxy = proxy_;
      released_ = true;
      instance_ = nullptr;
      generation_id_ = 0;
      proxy_ = nullptr;
      if (need_cleanup && OB_NOT_NULL(proxy)) {
        proxy->try_cleanup_after_request_finish();
      }
    }
  }
  JavaSandboxInstance *get_instance() const { return instance_; }
  int64_t get_generation_id() const { return generation_id_; }

private:
  ObJavaUDFProxy *proxy_;
  JavaSandboxInstance *instance_;
  int64_t generation_id_;
  bool released_;
  DISALLOW_COPY_AND_ASSIGN(JavaSandboxRequestHandle);
};

ObJavaUDFProxy::ObJavaUDFProxy()
  : next_ctx_id_(0),
    launch_mutex_(common::ObLatchIds::SANDBOX_LOCK),
    instances_(),
    deferred_free_list_(),
    deferred_free_count_(0),
    active_instance_(nullptr),
    active_generation_id_(0),
    active_read_epoch_(0),
    next_generation_id_(1),
    max_jvm_per_tenant_(2),
    rollover_window_start_ts_(0),
    rollover_count_in_window_(0),
    last_rollover_ts_(0),
    last_rollover_reason_(JSDR_NONE),
    proxy_ref_count_(1)
{
  active_reader_count_[0] = 0;
  active_reader_count_[1] = 0;
}

ObJavaUDFProxy::~ObJavaUDFProxy()
{
  shutdown();
}

common::hash::ObHashMap<uint64_t, ObJavaUDFProxy*> ObJavaUDFProxy::proxy_map_;
bool ObJavaUDFProxy::map_inited_ = false;
lib::ObMutex ObJavaUDFProxy::proxy_map_mutex_(common::ObLatchIds::SANDBOX_LOCK);

int ObJavaUDFProxy::get_tenant_proxy(uint64_t tenant_id, ObJavaUDFProxy *&proxy)
{
  int ret = OB_SUCCESS;
  proxy = nullptr;
  lib::ObMutexGuard guard(proxy_map_mutex_);
  if (OB_UNLIKELY(!ATOMIC_LOAD(&map_inited_))) {
    if (OB_FAIL(proxy_map_.create(16, common::ObMemAttr(common::OB_SERVER_TENANT_ID, "JavaProxyMap")))) {
      LOG_WARN("create proxy_map failed", K(ret));
    } else {
      ATOMIC_STORE(&map_inited_, true);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS == proxy_map_.get_refactored(tenant_id, proxy)) {
    proxy->inc_ref();
  } else {
    void *buf = ob_malloc(sizeof(ObJavaUDFProxy), SET_IGNORE_MEM_VERSION(common::ObMemAttr(tenant_id, "JavaUDFProxy")));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObJavaUDFProxy *new_proxy = new (buf) ObJavaUDFProxy();
      int tmp = proxy_map_.set_refactored(tenant_id, new_proxy, 0 /*no overwrite*/);
      if (OB_SUCCESS == tmp) {
        new_proxy->inc_ref();
        proxy = new_proxy;
      } else if (OB_HASH_EXIST == tmp) {
        new_proxy->~ObJavaUDFProxy();
        ob_free(new_proxy);
        if (OB_SUCCESS == (ret = proxy_map_.get_refactored(tenant_id, proxy))) {
          proxy->inc_ref();
        }
      } else {
        new_proxy->~ObJavaUDFProxy();
        ob_free(new_proxy);
        ret = tmp;
      }
    }
  }
  return ret;
}

void ObJavaUDFProxy::drop_tenant_proxy(uint64_t tenant_id)
{
  if (!ATOMIC_LOAD(&map_inited_)) { return; }
  ObJavaUDFProxy *proxy = nullptr;
  {
    lib::ObMutexGuard guard(proxy_map_mutex_);
    if (OB_SUCCESS == proxy_map_.get_refactored(tenant_id, proxy) && OB_NOT_NULL(proxy)) {
      proxy_map_.erase_refactored(tenant_id);
    }
  }
  if (OB_NOT_NULL(proxy)) {
    proxy->shutdown();
    LOG_INFO("dropped java udf proxy for tenant", K(tenant_id));
    proxy->dec_ref();
  }
}

int ObJavaUDFProxy::enter_active_read(int64_t &epoch, int64_t &bucket)
{
  int ret = OB_EAGAIN;
  for (int64_t i = 0; OB_EAGAIN == ret && i < 3; ++i) {
    epoch = ATOMIC_LOAD(&active_read_epoch_);
    if (0 == (epoch & 1)) {
      bucket = (epoch >> 1) & 1;
      ATOMIC_AAF(&active_reader_count_[bucket], 1);
      if (epoch == ATOMIC_LOAD(&active_read_epoch_)) {
        ret = OB_SUCCESS;
      } else {
        ATOMIC_AAF(&active_reader_count_[bucket], -1);
      }
    }
  }
  return ret;
}

void ObJavaUDFProxy::exit_active_read(int64_t bucket)
{
  if (bucket >= 0 && bucket < 2) {
    ATOMIC_AAF(&active_reader_count_[bucket], -1);
  }
}

static void best_effort_shutdown_sandbox_instance(JavaSandboxInstance *instance)
{
  if (OB_NOT_NULL(instance)) {
    ObJavaChannel *ch = nullptr;
    if (OB_SUCCESS == instance->get_channel_pool().acquire(ch) && OB_NOT_NULL(ch)) {
      uint32_t req_id = 0;
      ch->send_frame(JAVA_SANDBOX_SHUTDOWN, nullptr, 0, req_id);
      instance->get_channel_pool().release(ch);
    }
  }
}

int ObJavaUDFProxy::detach_instance_locked(JavaSandboxInstance *instance)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(instance)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t idx = -1;
    for (int64_t i = 0; idx < 0 && i < instances_.count(); ++i) {
      if (instances_.at(i) == instance) { idx = i; }
    }
    if (idx < 0) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("java sandbox instance not found when detach", K(ret),
               "generation_id", instance->get_generation_id());
    } else if (OB_FAIL(deferred_free_list_.push_back(instance))) {
      LOG_WARN("push java sandbox instance to deferred free list failed", K(ret),
               "generation_id", instance->get_generation_id());
    } else if (OB_FAIL(instances_.remove(idx))) {
      (void)deferred_free_list_.pop_back();
      LOG_WARN("remove java sandbox instance failed", K(ret), K(idx),
               "generation_id", instance->get_generation_id());
    } else {
      instance->set_state(JSS_STOPPING);
      ATOMIC_AAF(&deferred_free_count_, 1);
    }
  }
  return ret;
}

int ObJavaUDFProxy::cleanup_draining_instances_locked()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  for (int64_t i = 0; OB_SUCC(ret) && i < instances_.count(); ++i) {
    JavaSandboxInstance *instance = instances_.at(i);
    if (OB_NOT_NULL(instance) && JSS_DRAINING == instance->get_state()) {
      if (instance->get_inflight_count() <= 0) {
        LOG_INFO("java sandbox draining instance finished, defer free",
                 "generation_id", instance->get_generation_id(),
                 "ref_count", instance->get_ref_count());
        best_effort_shutdown_sandbox_instance(instance);
        if (OB_FAIL(detach_instance_locked(instance))) {
          LOG_WARN("detach drained java sandbox instance failed", K(ret),
                   "generation_id", instance->get_generation_id());
        } else {
          --i;
        }
      } else if (instance->get_drain_deadline_ts() > 0
                 && now > instance->get_drain_deadline_ts()) {
        instance->force_killed_ = true;
        instance->kill_reason_ = JSDR_DRAIN_DEADLINE_EXPIRED;
        instance->stop_process();
        LOG_WARN("java sandbox draining instance deadline expired, force stop",
                 "generation_id", instance->get_generation_id(),
                 "drain_reason", instance->get_drain_reason(),
                 "inflight_count", instance->get_inflight_count(),
                 "ref_count", instance->get_ref_count(),
                 "drain_deadline_ts", instance->get_drain_deadline_ts(),
                 K(now));
        if (OB_FAIL(detach_instance_locked(instance))) {
          LOG_WARN("detach expired java sandbox instance failed", K(ret),
                   "generation_id", instance->get_generation_id());
        } else {
          --i;
        }
      }
    }
  }
  return ret;
}

void ObJavaUDFProxy::try_cleanup_after_request_finish()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == launch_mutex_.trylock()) {
    if (OB_FAIL(cleanup_draining_instances_locked())) {
      LOG_WARN("cleanup draining java sandbox instances after request finish failed", K(ret));
    }
    launch_mutex_.unlock();
  }
  if (ATOMIC_LOAD(&deferred_free_count_) > 0) {
    reclaim_deferred_instances();
  }
}

int ObJavaUDFProxy::prepare_room_for_new_instance_locked()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cleanup_draining_instances_locked())) {
    LOG_WARN("cleanup draining java sandbox instances failed", K(ret));
  } else if (instances_.count() >= max_jvm_per_tenant_) {
    JavaSandboxInstance *victim = nullptr;
    for (int64_t i = 0; i < instances_.count(); ++i) {
      JavaSandboxInstance *instance = instances_.at(i);
      if (OB_NOT_NULL(instance) && JSS_DRAINING == instance->get_state()
          && (OB_ISNULL(victim)
              || instance->get_generation_id() < victim->get_generation_id())) {
        victim = instance;
      }
    }
    if (OB_ISNULL(victim)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("java sandbox instance limit reached and no draining victim",
               K(ret), "instance_count", instances_.count(), K_(max_jvm_per_tenant));
    } else {
      victim->force_killed_ = true;
      victim->kill_reason_ = JSDR_MAX_GENERATION_LIMIT;
      victim->stop_process();
      LOG_WARN("java sandbox force stop oldest draining instance due to max generation limit",
               "generation_id", victim->get_generation_id(),
               "drain_reason", victim->get_drain_reason(),
               "inflight_count", victim->get_inflight_count(),
               "ref_count", victim->get_ref_count(),
               K_(max_jvm_per_tenant));
      if (OB_FAIL(detach_instance_locked(victim))) {
        LOG_WARN("detach java sandbox victim failed", K(ret),
                 "generation_id", victim->get_generation_id());
      }
    }
  }
  return ret;
}

int ObJavaUDFProxy::check_rollover_cooldown_locked(int64_t now)
{
  int ret = OB_SUCCESS;
  if (rollover_window_start_ts_ > 0
      && now - rollover_window_start_ts_ <= JAVA_SANDBOX_ROLLOVER_WINDOW_US
      && rollover_count_in_window_ >= JAVA_SANDBOX_ROLLOVER_THRESHOLD
      && now - last_rollover_ts_ < JAVA_SANDBOX_ROLLOVER_COOLDOWN_US) {
    const ObString error_msg(
        "Java sandbox temporarily unavailable because frequent JVM rollovers triggered cooldown; "
        "please try again later");
    ret = OB_JNI_JAVA_EXCEPTION_ERROR;
    LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, error_msg.length(), error_msg.ptr());
    LOG_WARN("java sandbox rollover cooldown active",
             K(ret),
             K_(rollover_count_in_window),
             K_(rollover_window_start_ts),
             K_(last_rollover_ts),
             K_(last_rollover_reason),
             K(now));
  }
  return ret;
}

void ObJavaUDFProxy::record_rollover_locked(JavaSandboxDrainReason reason, int64_t now)
{
  if (0 == rollover_window_start_ts_
      || now - rollover_window_start_ts_ > JAVA_SANDBOX_ROLLOVER_WINDOW_US) {
    rollover_window_start_ts_ = now;
    rollover_count_in_window_ = 1;
  } else {
    ++rollover_count_in_window_;
  }
  last_rollover_ts_ = now;
  last_rollover_reason_ = static_cast<int64_t>(reason);
}

void ObJavaUDFProxy::reclaim_deferred_instances()
{
  int ret = OB_SUCCESS;
  common::ObSEArray<JavaSandboxInstance *, 4> free_list;
  {
    lib::ObMutexGuard guard(launch_mutex_);
    for (int64_t i = 0; OB_SUCC(ret) && i < deferred_free_list_.count(); ++i) {
      JavaSandboxInstance *instance = deferred_free_list_.at(i);
      if (OB_NOT_NULL(instance)) {
        const int64_t bucket = (instance->retired_epoch_ >> 1) & 1;
        if (instance->get_ref_count() <= 0
            && bucket >= 0 && bucket < 2
            && 0 == ATOMIC_LOAD(&active_reader_count_[bucket])) {
          if (OB_FAIL(deferred_free_list_.remove(i))) {
            LOG_WARN("remove java sandbox deferred instance failed", K(ret), K(i),
                     "generation_id", instance->get_generation_id());
          } else if (OB_FAIL(free_list.push_back(instance))) {
            // Put it back if the local free list cannot grow; ownership must
            // remain in exactly one list.
            int tmp_ret = deferred_free_list_.push_back(instance);
            LOG_WARN("push java sandbox free list failed", K(ret), K(tmp_ret),
                     "generation_id", instance->get_generation_id());
            if (OB_SUCCESS != tmp_ret) {
              LOG_ERROR("failed to rollback deferred java sandbox instance",
                        K(tmp_ret), "generation_id", instance->get_generation_id());
            }
          } else {
            ATOMIC_AAF(&deferred_free_count_, -1);
            --i;
          }
        }
      }
    }
  }

  for (int64_t i = 0; i < free_list.count(); ++i) {
    JavaSandboxInstance *instance = free_list.at(i);
    if (OB_NOT_NULL(instance)) {
      LOG_INFO("free deferred java sandbox instance",
               "generation_id", instance->get_generation_id(),
               "force_killed", instance->force_killed_,
               "kill_reason", instance->kill_reason_,
               "drain_reason", instance->get_drain_reason());
      instance->destroy();
      instance->~JavaSandboxInstance();
      ob_free(instance);
    }
  }
}

int ObJavaUDFProxy::create_active_instance_locked(JavaSandboxInstance *&instance)
{
  int ret = OB_SUCCESS;
  instance = nullptr;
  int64_t old_epoch = ATOMIC_LOAD(&active_read_epoch_);
  int64_t write_epoch = (0 == (old_epoch & 1)) ? old_epoch + 1 : old_epoch;
  void *buf = nullptr;
  JavaSandboxInstance *new_instance = nullptr;
  if (OB_FAIL(prepare_room_for_new_instance_locked())) {
    LOG_WARN("prepare room for new java sandbox instance failed", K(ret));
  } else if (OB_FAIL(check_rollover_cooldown_locked(ObTimeUtility::current_time()))) {
    LOG_WARN("java sandbox active instance creation blocked by cooldown", K(ret));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(JavaSandboxInstance),
                                       common::ObMemAttr(MTL_ID(), "JavaSbInst")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(new_instance = new (buf) JavaSandboxInstance())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(new_instance->init(next_generation_id_++))) {
    LOG_WARN("init java sandbox instance failed", K(ret));
  } else {
    ATOMIC_STORE(&active_read_epoch_, write_epoch);
    if (OB_FAIL(launch_sandbox(*new_instance))) {
      LOG_WARN("launch java sandbox instance failed", K(ret), "generation_id",
               new_instance->get_generation_id());
      ATOMIC_STORE(&active_read_epoch_, write_epoch + 1);
    } else if (OB_FAIL(instances_.push_back(new_instance))) {
      LOG_WARN("push java sandbox instance failed", K(ret));
      ATOMIC_STORE(&active_read_epoch_, write_epoch + 1);
    } else {
      new_instance->set_state(JSS_ACTIVE);
      ATOMIC_STORE(&active_instance_, new_instance);
      ATOMIC_STORE(&active_generation_id_, new_instance->get_generation_id());
      ATOMIC_STORE(&active_read_epoch_, write_epoch + 1);
      instance = new_instance;
      LOG_INFO("java sandbox active instance created",
               "generation_id", new_instance->get_generation_id(),
               "sandbox_pid", OB_NOT_NULL(new_instance->get_sandbox_process())
                                ? new_instance->get_sandbox_process()->get_pid() : -1);
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(new_instance)) {
    new_instance->destroy();
    new_instance->~JavaSandboxInstance();
    ob_free(new_instance);
  }
  return ret;
}

int ObJavaUDFProxy::acquire_instance_for_request(int64_t request_deadline_ts,
                                                 JavaSandboxRequestHandle &handle)
{
  int ret = OB_SUCCESS;
  JavaSandboxInstance *instance = nullptr;
  if (ATOMIC_LOAD(&deferred_free_count_) > 0) {
    reclaim_deferred_instances();
  }
  {
    ActiveReadGuard guard(*this);
    if (guard.is_valid()) {
      instance = get_active_instance_atomic();
      if (OB_NOT_NULL(instance) && JSS_ACTIVE == instance->get_state()
          && is_java_sandbox_process_running(instance)
          && !instance->get_channel_pool().needs_rollover()
          && instance->get_generation_id() == get_active_generation_atomic()) {
        instance->inc_ref();
        if (instance == get_active_instance_atomic()
            && JSS_ACTIVE == instance->get_state()
            && is_java_sandbox_process_running(instance)
            && !instance->get_channel_pool().needs_rollover()
            && instance->get_generation_id() == get_active_generation_atomic()) {
          instance->inc_inflight();
          instance->update_last_request_ts(ObTimeUtility::current_time());
          instance->update_max_inflight_deadline_ts(request_deadline_ts);
          handle.bind(this, instance);
        } else {
          instance->dec_ref();
          instance = nullptr;
        }
      }
    }
  }
  if (OB_ISNULL(handle.get_instance())) {
    lib::ObMutexGuard guard(launch_mutex_);
    instance = get_active_instance_atomic();
    if (OB_NOT_NULL(instance) && JSS_ACTIVE == instance->get_state()
        && (!is_java_sandbox_process_running(instance)
            || instance->get_channel_pool().needs_rollover())) {
      const JavaSandboxDrainReason reason =
          is_java_sandbox_process_running(instance)
              ? JSDR_CHANNEL_DEGRADED : JSDR_PROCESS_DEAD;
      retire_active_instance_locked(instance, reason);
      instance = nullptr;
    }
    if (OB_ISNULL(instance) || JSS_ACTIVE != instance->get_state()) {
      if (OB_FAIL(create_active_instance_locked(instance))) {
        LOG_WARN("create active java sandbox instance failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(instance) || JSS_ACTIVE != instance->get_state()
          || !is_java_sandbox_process_running(instance)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("active java sandbox instance invalid", K(ret), KP(instance));
      } else {
        instance->inc_ref();
        instance->inc_inflight();
        instance->update_last_request_ts(ObTimeUtility::current_time());
        instance->update_max_inflight_deadline_ts(request_deadline_ts);
        handle.bind(this, instance);
      }
    }
  }
  return ret;
}

int ObJavaUDFProxy::ensure_sandbox_ready()
{
  int ret = OB_SUCCESS;
  JavaSandboxRequestHandle handle;
  if (OB_FAIL(acquire_instance_for_request(0, handle))) {
    LOG_WARN("acquire java sandbox instance failed", K(ret));
  }
  return ret;
}

bool ObJavaUDFProxy::is_inited() const
{
  JavaSandboxInstance *instance =
      reinterpret_cast<JavaSandboxInstance *>(ATOMIC_LOAD(&active_instance_));
  return OB_NOT_NULL(instance)
         && JSS_ACTIVE == instance->get_state()
         && is_java_sandbox_process_running(instance);
}

bool ObJavaUDFProxy::check_sandbox_alive(JavaSandboxInstance &instance)
{
  bool alive = true;
  observer::ObSandboxProcess *process = instance.get_sandbox_process();
  if (OB_NOT_NULL(process) && process->get_pid() > 0) {
    pid_t pid = process->get_pid();
    int ret = observer::ObSandboxManager::get_instance().check_process_status(pid);
    LOG_TRACE("[SANDBOX_DIAG] check_sandbox_alive", K(pid), K(ret),
              "ret_meaning", ret == common::OB_SUCCESS ? "alive" :
                             ret == common::OB_ENTRY_NOT_EXIST ? "dead" :
                             ret == common::OB_SEARCH_NOT_FOUND ? "not_found_in_daemon" : "other_error");
    if (OB_ENTRY_NOT_EXIST == ret || OB_SEARCH_NOT_FOUND == ret) {
      alive = false;
      LOG_WARN("sandbox process confirmed dead or is no longer managed", K(ret), K(pid));
    } else if (OB_SUCCESS != ret) {
      LOG_WARN("check_process_status failed, assume alive", K(ret), K(pid));
    }
  } else {
    LOG_TRACE("[SANDBOX_DIAG] check_sandbox_alive: no sandbox_process or pid<=0",
              "has_process", OB_NOT_NULL(process),
              "pid", OB_NOT_NULL(process) ? process->get_pid() : -1);
  }
  return alive;
}

void ObJavaUDFProxy::cleanup_dead_sandbox(JavaSandboxInstance &instance)
{
  observer::ObSandboxProcess *process = instance.get_sandbox_process();
  pid_t old_pid = OB_NOT_NULL(process) ? process->get_pid() : -1;
  LOG_TRACE("[SANDBOX_DIAG] cleanup_dead_sandbox begin", K(old_pid),
            "channel_pool_inited", instance.get_channel_pool().is_inited(),
            "generation_id", instance.get_generation_id());
  instance.destroy();
  LOG_TRACE("[SANDBOX_DIAG] cleanup_dead_sandbox done", K(old_pid));
}

void ObJavaUDFProxy::on_channel_error(JavaSandboxInstance *instance)
{
  if (OB_NOT_NULL(instance) && JSS_ACTIVE == instance->get_state()) {
    observer::ObSandboxProcess *process = instance->get_sandbox_process();
    pid_t cur_pid = OB_NOT_NULL(process) ? process->get_pid() : -1;
    LOG_TRACE("[SANDBOX_DIAG] on_channel_error entered", K(cur_pid),
              "generation_id", instance->get_generation_id());
    bool alive = check_sandbox_alive(*instance);
    if (!alive) {
      retire_active_instance(instance, JSDR_PROCESS_DEAD);
      LOG_TRACE("[SANDBOX_DIAG] on_channel_error: marked sandbox draining", K(cur_pid));
    } else {
      retire_active_instance(instance, JSDR_CHANNEL_ERROR);
      LOG_TRACE("[SANDBOX_DIAG] on_channel_error: sandbox still alive, retire due to channel error",
                K(cur_pid));
    }
  }
}

static bool ob_java_sandbox_strcasestr(const common::ObString &str, const char *pattern)
{
  bool found = false;
  if (OB_NOT_NULL(str.ptr()) && OB_NOT_NULL(pattern)) {
    const int64_t pattern_len = strlen(pattern);
    if (pattern_len > 0 && str.length() >= pattern_len) {
      for (int64_t i = 0; !found && i <= str.length() - pattern_len; ++i) {
        found = (0 == strncasecmp(str.ptr() + i, pattern, pattern_len));
      }
    }
  }
  return found;
}

JavaSandboxDrainReason ObJavaUDFProxy::classify_sandbox_error(
    int32_t error_code,
    const common::ObString &error_msg)
{
  JavaSandboxDrainReason reason = JSDR_NONE;
  if (SANDBOX_ERR_JAVA_TIMEOUT_STUCK == error_code
      || SANDBOX_ERR_JAVA_TIMEOUT == error_code) {
    // Timeout, even with a Java worker that did not exit after interrupt, is
    // reported to the current SQL only. Resource exhaustion or fatal errors
    // will trigger rollover later.
  } else if (ob_java_sandbox_strcasestr(error_msg, "OutOfMemoryError")
             || ob_java_sandbox_strcasestr(error_msg, "Java heap space")
             || ob_java_sandbox_strcasestr(error_msg, "Metaspace")
             || ob_java_sandbox_strcasestr(error_msg, "GC overhead limit exceeded")
             || ob_java_sandbox_strcasestr(error_msg, "Compressed class space")
             || ob_java_sandbox_strcasestr(error_msg, "Direct buffer memory")
             || ob_java_sandbox_strcasestr(error_msg, "unable to create native thread")) {
    reason = JSDR_OOM;
  } else if (ob_java_sandbox_strcasestr(error_msg, "fatal error")
             || ob_java_sandbox_strcasestr(error_msg, "handler fatal")) {
    reason = JSDR_HANDLER_FATAL;
  }
  return reason;
}

bool ObJavaUDFProxy::is_sandbox_channel_failure(int ret)
{
  return OB_IO_ERROR == ret
         || OB_ERR_SYS == ret
         || OB_NOT_INIT == ret
         || OB_INVALID_DATA == ret
         || OB_SIZE_OVERFLOW == ret;
}

void ObJavaUDFProxy::retire_active_instance(JavaSandboxInstance *instance,
                                            JavaSandboxDrainReason reason)
{
  if (OB_NOT_NULL(instance) && JSDR_NONE != reason) {
    lib::ObMutexGuard guard(launch_mutex_);
    retire_active_instance_locked(instance, reason);
  }
}

void ObJavaUDFProxy::retire_active_instance_locked(JavaSandboxInstance *instance,
                                                   JavaSandboxDrainReason reason)
{
  int ret = OB_SUCCESS;
  UNUSED(ret);
  if (OB_ISNULL(instance) || JSDR_NONE == reason) {
    // nothing to do
  } else if (JSS_ACTIVE == instance->get_state()) {
    instance->set_state(JSS_DRAINING);
    instance->set_drain_reason(reason);
    const int64_t now = ObTimeUtility::current_time();
    const int64_t drain_deadline =
        std::max(now + JAVA_SANDBOX_DRAIN_TIMEOUT_US,
                 instance->get_max_inflight_deadline_ts());
    instance->set_drain_deadline_ts(drain_deadline);
    record_rollover_locked(reason, now);
    if (JSDR_OOM == reason) {
      instance->inc_oom_count();
      instance->update_last_oom_ts(now);
    } else if (JSDR_TIMEOUT_STUCK == reason) {
      instance->inc_timeout_count();
    }
    if (instance == get_active_instance_atomic()) {
      int64_t old_epoch = ATOMIC_LOAD(&active_read_epoch_);
      int64_t write_epoch = (0 == (old_epoch & 1)) ? old_epoch + 1 : old_epoch;
      int64_t retired_epoch = (0 == (old_epoch & 1)) ? old_epoch : old_epoch - 1;
      ATOMIC_STORE(&active_read_epoch_, write_epoch);
      ATOMIC_STORE(&active_instance_, nullptr);
      ATOMIC_STORE(&active_generation_id_, 0);
      instance->retired_epoch_ = retired_epoch;
      ATOMIC_STORE(&active_read_epoch_, write_epoch + 1);
    }
    LOG_WARN("java sandbox instance retired",
             "generation_id", instance->get_generation_id(),
             K(reason),
             "ref_count", instance->get_ref_count(),
             "inflight_count", instance->get_inflight_count(),
             "drain_deadline_ts", instance->get_drain_deadline_ts(),
             "sandbox_pid", OB_NOT_NULL(instance->get_sandbox_process())
                              ? instance->get_sandbox_process()->get_pid() : -1);
  }
}

void ObJavaUDFProxy::handle_request_failure(JavaSandboxInstance *instance,
                                            int ret,
                                            JavaSandboxDrainReason reason)
{
  if (OB_ISNULL(instance)) {
    // nothing to do
  } else if (JSDR_NONE != reason) {
    retire_active_instance(instance, reason);
  } else if (OB_TIMEOUT == ret) {
    instance->inc_timeout_count();
  } else if (OB_ERR_UNEXPECTED == ret) {
    retire_active_instance(instance, JSDR_PROTOCOL_ERROR);
  } else if (is_sandbox_channel_failure(ret)) {
    retire_active_instance(instance, JSDR_CHANNEL_ERROR);
  }
}

void ObJavaUDFProxy::shutdown()
{
  lib::ObMutexGuard guard(launch_mutex_);
  ATOMIC_STORE(&active_instance_, nullptr);
  ATOMIC_STORE(&active_generation_id_, 0);
  for (int64_t i = 0; i < instances_.count(); ++i) {
    JavaSandboxInstance *instance = instances_.at(i);
    if (OB_NOT_NULL(instance)) {
      ObJavaChannel *ch = nullptr;
      if (common::OB_SUCCESS == instance->get_channel_pool().acquire(ch) && OB_NOT_NULL(ch)) {
        uint32_t req_id = 0;
        ch->send_frame(JAVA_SANDBOX_SHUTDOWN, nullptr, 0, req_id);
        instance->get_channel_pool().release(ch);
      }
      instance->destroy();
      instance->~JavaSandboxInstance();
      ob_free(instance);
    }
  }
  instances_.reset();
  for (int64_t i = 0; i < deferred_free_list_.count(); ++i) {
    JavaSandboxInstance *instance = deferred_free_list_.at(i);
    if (OB_NOT_NULL(instance)) {
      uint32_t req_id = 0;
      ObJavaChannel *ch = nullptr;
      if (common::OB_SUCCESS == instance->get_channel_pool().acquire(ch) && OB_NOT_NULL(ch)) {
        ch->send_frame(JAVA_SANDBOX_SHUTDOWN, nullptr, 0, req_id);
        instance->get_channel_pool().release(ch);
      }
      instance->destroy();
      instance->~JavaSandboxInstance();
      ob_free(instance);
    }
  }
  deferred_free_list_.reset();
  ATOMIC_STORE(&deferred_free_count_, 0);
}

namespace
{
// Recursively scan `dir` for symlinks whose realpath target lies outside the
// JAVA_HOME tree, and bind-mount each target read-only into the sandbox.
// JDK9+ RPM layouts split the JDK across trees: $JAVA_HOME/conf -> /etc/java/...,
// lib/security -> /etc/java/..., lib/tzdb.dat -> /usr/share/javazi-*; any such
// target not mounted crashes the sandbox JVM at startup (e.g. it cannot load
// java.security). Discovering symlinks dynamically (rather than a hardcoded
// name list) keeps this robust across JDK distros/versions/upgrades. JDK8 has
// no such symlinks, so this is a no-op there.
void mount_external_symlink_targets(observer::ObSandboxProcess &process,
                                    const char *java_home, const char *dir, int depth)
{
  if (depth <= 0 || OB_ISNULL(dir)) {
    // depth budget exhausted
  } else {
    DIR *d = ::opendir(dir);
    if (OB_NOT_NULL(d)) {
      const int64_t home_len = strlen(java_home);
      struct dirent *ent = nullptr;
      while (OB_NOT_NULL(ent = ::readdir(d))) {
        if (0 == strcmp(ent->d_name, ".") || 0 == strcmp(ent->d_name, "..")) {
          continue;
        }
        char child[common::MAX_PATH_SIZE] = {0};
        struct stat st;
        if (snprintf(child, sizeof(child), "%s/%s", dir, ent->d_name) >= (int)sizeof(child)) {
          // path too long, skip
        } else if (0 != ::lstat(child, &st)) {
          // unreadable entry, skip
        } else if (S_ISLNK(st.st_mode)) {
          char *real_path = realpath(child, nullptr);  // malloc'd, NULL on failure
          if (OB_NOT_NULL(real_path)) {
            // mount only when the target falls outside the JAVA_HOME tree
            // (prefix match with a '/' boundary to avoid sibling-dir false positives)
            if (!(0 == strncmp(real_path, java_home, home_len) && real_path[home_len] == '/')) {
              process.mount_path(real_path, real_path, observer::ObSandboxProcess::MOUNT_RDONLY);
            }
            ::free(real_path);
          }
        } else if (S_ISDIR(st.st_mode)) {
          // recurse into real subdirectories only (lstat above did not follow links)
          mount_external_symlink_targets(process, java_home, child, depth - 1);
        }
      }
      ::closedir(d);
    }
  }
}
} // anonymous namespace

int ObJavaUDFProxy::launch_sandbox(JavaSandboxInstance &instance)
{
  int ret = common::OB_SUCCESS;
  observer::ObSandboxProcess *process = nullptr;

  if (OB_FAIL(instance.get_channel_pool().init(ObJavaChannelPool::DEFAULT_POOL_SIZE, MTL_ID()))) {
    LOG_WARN("failed to init java sandbox channel pool", K(ret));
  }

  common::ObSArray<int> child_fds;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(instance.get_channel_pool().get_child_fds(child_fds))) {
      LOG_WARN("failed to get child fds", K(ret));
    }
  }

  char java_home[common::MAX_PATH_SIZE] = {0};
  char java_path[common::MAX_PATH_SIZE] = {0};
  char sdk_jar[common::MAX_PATH_SIZE] = {0};
  char sdk_dir_buf[common::MAX_PATH_SIZE] = {0};
  char *sdk_dir = nullptr;
  char java_opts[4096] = {0};
  char execute_arg[8192] = {0};

  if (OB_SUCC(ret)) {
    char java_home_raw[common::MAX_PATH_SIZE] = {0};
    GCONF.ob_java_home.copy(java_home_raw, sizeof(java_home_raw));
    if (strlen(java_home_raw) == 0) {
      ret = common::OB_INVALID_CONFIG;
      LOG_WARN("ob_java_home is not configured", K(ret));
    } else {
      // Resolve ob_java_home to its realpath so that a symlinked JAVA_HOME
      // (e.g. /usr/lib/jvm/java-11 -> versioned dir) gets mounted and used as
      // the real target. Without this, mounting/exec'ing the symlink path would
      // fail inside the sandbox because the versioned target is not mounted.
      char *resolved = realpath(java_home_raw, nullptr);  // malloc'd, NULL on failure
      if (OB_ISNULL(resolved)) {
        ret = common::OB_FILE_NOT_EXIST;
        LOG_WARN("resolve ob_java_home failed", K(ret), "path", java_home_raw, K(errno));
      } else {
        int n = snprintf(java_home, sizeof(java_home), "%s", resolved);
        ::free(resolved);
        if (n < 0 || n >= (int)sizeof(java_home)) {
          ret = common::OB_SIZE_OVERFLOW;
          LOG_WARN("resolved java_home too long", K(ret), K(n));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    snprintf(java_path, sizeof(java_path), "%s/bin/java", java_home);
    if (0 != access(java_path, X_OK)) {
      snprintf(java_path, sizeof(java_path), "%s/jre/bin/java", java_home);
    }

    char connector_path[common::MAX_PATH_SIZE] = {0};
    GCONF.ob_java_connector_path.copy(connector_path, sizeof(connector_path));
    if (strlen(connector_path) == 0) {
      ret = common::OB_INVALID_CONFIG;
      LOG_WARN("ob_java_connector_path is not configured", K(ret));
    } else {
      char sdk_jar_rel[common::MAX_PATH_SIZE] = {0};
      snprintf(sdk_jar_rel, sizeof(sdk_jar_rel),
               "%s/java-udf/oceanbase-java-udf-jar-with-dependencies.jar", connector_path);
      // realpath into a malloc'd buffer (NULL form) to avoid the MAX_PATH_SIZE(1024)
      // < PATH_MAX(4096) fixed-buffer overflow, then copy back with truncation check.
      char *resolved_jar = realpath(sdk_jar_rel, nullptr);  // malloc'd, NULL on failure
      if (OB_ISNULL(resolved_jar)) {
        ret = common::OB_FILE_NOT_EXIST;
        LOG_WARN("java udf sdk jar not found", K(ret), "path", sdk_jar_rel, K(errno));
      } else {
        int n = snprintf(sdk_jar, sizeof(sdk_jar), "%s", resolved_jar);
        ::free(resolved_jar);
        if (n < 0 || n >= (int)sizeof(sdk_jar)) {
          ret = common::OB_SIZE_OVERFLOW;
          LOG_WARN("resolved sdk_jar too long", K(ret), K(n));
        } else {
          // Mount the installation directory, including compat/; only sdk_jar is on -cp.
          MEMCPY(sdk_dir_buf, sdk_jar, n + 1);
          sdk_dir = dirname(sdk_dir_buf);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    GCONF.ob_java_opts.copy(java_opts, sizeof(java_opts));
    // Lead with -XX:+IgnoreUnrecognizedVMOptions so the command-line launcher
    // tolerates unrecognized -XX: options (e.g. JDK8-era GC flags removed in
    // JDK9+), matching the JNI path's JavaVMInitArgs.ignoreUnrecognized=JNI_TRUE.
    // Must be first so it takes effect before the options it should cover.
    if (strlen(java_opts) > 0) {
      snprintf(execute_arg, sizeof(execute_arg),
               "-XX:+IgnoreUnrecognizedVMOptions -Dfile.encoding=UTF-8 %s -cp %s com.oceanbase.internal.sandbox.ObJavaUDFSandboxMain",
               java_opts, sdk_jar);
    } else {
      snprintf(execute_arg, sizeof(execute_arg),
               "-XX:+IgnoreUnrecognizedVMOptions -Dfile.encoding=UTF-8 -cp %s com.oceanbase.internal.sandbox.ObJavaUDFSandboxMain",
               sdk_jar);
    }

    void *proc_buf = ob_malloc(sizeof(observer::ObSandboxProcess),
                               common::ObMemAttr(MTL_ID(), "JavaSbProc"));
    if (OB_ISNULL(proc_buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ObSandboxProcess failed", K(ret));
    } else {
      process = new (proc_buf) observer::ObSandboxProcess();
      if (OB_FAIL(process->set_execute_path(java_path))) {
        LOG_WARN("set_execute_path failed", K(ret));
      } else if (OB_FAIL(process->set_execute_arg(execute_arg))) {
        LOG_WARN("set_execute_arg failed", K(ret));
      } else if (OB_FAIL(process->mount_path(java_home, java_home,
                                             observer::ObSandboxProcess::MOUNT_RDONLY))) {
        LOG_WARN("mount java_home failed", K(ret));
      } else if (OB_FAIL(process->mount_path(sdk_dir, sdk_dir,
                                             observer::ObSandboxProcess::MOUNT_RDONLY))) {
        LOG_WARN("mount java udf directory failed", K(ret), K(sdk_dir));
      }
      // mount system crypto-policies for JDK11+ java.security initialization (best-effort)
      if (OB_SUCC(ret) && 0 == access("/etc/crypto-policies", R_OK)) {
        int tmp_ret = process->mount_path("/etc/crypto-policies", "/etc/crypto-policies",
                                          observer::ObSandboxProcess::MOUNT_RDONLY);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("mount /etc/crypto-policies failed", K(tmp_ret));
        }
      }
      if (OB_SUCC(ret) && 0 == access("/usr/share/crypto-policies", R_OK)) {
        int tmp_ret = process->mount_path("/usr/share/crypto-policies", "/usr/share/crypto-policies",
                                          observer::ObSandboxProcess::MOUNT_RDONLY);
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("mount /usr/share/crypto-policies failed", K(tmp_ret));
        }
      }
      // JDK9+ RPM layouts split the JDK across trees via symlinks under JAVA_HOME
      // (conf -> /etc/java/..., lib/security -> /etc/java/..., lib/tzdb.dat ->
      // /usr/share/javazi-*). Dynamically discover all such outward symlinks and
      // mount their real targets, instead of hardcoding a fragile name list.
      if (OB_SUCC(ret)) {
        mount_external_symlink_targets(*process, java_home, java_home, 3 /*max_depth*/);
      }
      if (OB_SUCC(ret)) {
        process->set_tenant_id(MTL_ID());
        for (int64_t i = 0; OB_SUCC(ret) && i < child_fds.count(); ++i) {
          if (OB_FAIL(process->add_preserve_fd(child_fds.at(i)))) {
            LOG_WARN("push preserve_fd failed", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret)) {
          observer::ObSandboxManager &mgr = observer::ObSandboxManager::get_instance();
          if (OB_FAIL(mgr.create_sandbox_process(*process, -1, -1))) {
            LOG_WARN("create_sandbox_process failed", K(ret));
          } else {
            instance.get_channel_pool().close_child_fds();
            instance.set_sandbox_process(process);
            LOG_INFO("java sandbox launched", "sandbox_pid", process->get_pid(),
                     "channel_count", child_fds.count(),
                     "generation_id", instance.get_generation_id());
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(process) && process != instance.get_sandbox_process()) {
      process->~ObSandboxProcess();
      ob_free(process);
    }
    instance.get_channel_pool().destroy();
  }
  return ret;
}


// ---- unpack_values_to_obobj_ ----

int ObJavaUDFProxy::unpack_values_to_obobj(
    const ObPl__JavaUdf__Values &values,
    int64_t batch_size,
    const sql::ObExprResType &res_type,
    sql::ObSQLSessionInfo &session,
    common::ObIAllocator &alloc,
    common::ObIArray<common::ObObj> &result)
{
  int ret = OB_SUCCESS;

  // Procedures (void return) have ObNullType as res_type — no result to
  // deserialize. Just push null results, matching the JNI path which skips
  // handle_function_result entirely for procedures.
  if (ObNullType == res_type.get_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      common::ObObj null_obj;
      null_obj.set_null();
      if (OB_FAIL(result.push_back(null_obj))) {
        LOG_WARN("push_back null failed", K(ret), K(i));
      }
    }
  } else {
    // Reuse the JNI path's type mapper (ObFromJava*TypeMapper::operator()) via
    // create_for_sandbox — no JNIEnv needed, operator() is pure C++ (protobuf
    // Values → ObObj + spi_convert). This guarantees the sandbox and JNI paths
    // use identical deserialization logic (from_sci for BigDecimal, set_varbinary
    // for ByteBuffer, set_collation_type for String, etc.).
    ObFromJavaTypeMapperBase *functor = nullptr;
    if (OB_FAIL(ObFromJavaTypeMapperBase::create_for_sandbox(
            alloc, batch_size, res_type, session, functor))) {
      LOG_WARN("create_for_sandbox failed", K(ret), K(res_type));
    } else if (OB_ISNULL(functor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL functor", K(ret));
    } else if (OB_FAIL((*functor)(values, result))) {
      LOG_WARN("functor execute failed", K(ret));
    }

    // Cleanup: destruct functor (memory freed by caller's allocator)
    if (OB_NOT_NULL(functor)) {
      functor->~ObFromJavaTypeMapperBase();
    }
  }

  return ret;
}

static void encode_be64(char *buf, int64_t val)
{
  uint64_t n = htobe64(static_cast<uint64_t>(val));
  memcpy(buf, &n, 8);
}

static void encode_be16(char *buf, uint16_t val)
{
  uint16_t n = htons(val);
  memcpy(buf, &n, 2);
}

static void encode_be32(char *buf, int32_t val)
{
  uint32_t n = htonl(static_cast<uint32_t>(val));
  memcpy(buf, &n, 4);
}

static int64_t decode_be64(const char *buf)
{
  uint64_t n;
  memcpy(&n, buf, 8);
  return static_cast<int64_t>(be64toh(n));
}

static int32_t decode_be32(const char *buf)
{
  uint32_t n;
  memcpy(&n, buf, 4);
  return static_cast<int32_t>(ntohl(n));
}

// EXECUTE_REQ (MySQL):
// session_id(8B) + timeout_ts(8B) + mode(1B) + udf_id(8B) + ctx_id(8B)
// + class_name(2B length) + method_name(2B length) + result_type_name(2B length) + BatchedArgs.
int ObJavaUDFProxy::build_execute_req_payload(
    int64_t session_id,
    int64_t timeout_ts,
    int64_t udf_id,
    int64_t ctx_id,
    const common::ObString &class_name,
    const common::ObString &method_name,
    const common::ObString &result_type_name,
    const char *batched_args_buf,
    int64_t batched_args_len,
    common::ObIAllocator &alloc,
    char *&payload,
    int64_t &payload_len)
{
  int ret = OB_SUCCESS;
  // fixed: 8 + 8 + 1 + 8 + 8 = 33; strings: 2+len each; then batched_args
  int64_t total = 33
      + 2 + class_name.length()
      + 2 + method_name.length()
      + 2 + result_type_name.length()
      + batched_args_len;
  payload = static_cast<char*>(alloc.alloc(total));
  if (OB_ISNULL(payload)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t pos = 0;
    encode_be64(payload + pos, session_id); pos += 8;
    encode_be64(payload + pos, timeout_ts); pos += 8;
    payload[pos++] = 0; // mode = MySQL
    encode_be64(payload + pos, udf_id); pos += 8;
    encode_be64(payload + pos, ctx_id); pos += 8;

    encode_be16(payload + pos, static_cast<uint16_t>(class_name.length())); pos += 2;
    memcpy(payload + pos, class_name.ptr(), class_name.length()); pos += class_name.length();

    encode_be16(payload + pos, static_cast<uint16_t>(method_name.length())); pos += 2;
    memcpy(payload + pos, method_name.ptr(), method_name.length()); pos += method_name.length();

    encode_be16(payload + pos, static_cast<uint16_t>(result_type_name.length())); pos += 2;
    memcpy(payload + pos, result_type_name.ptr(), result_type_name.length()); pos += result_type_name.length();

    if (batched_args_len > 0 && OB_NOT_NULL(batched_args_buf)) {
      memcpy(payload + pos, batched_args_buf, batched_args_len); pos += batched_args_len;
    }
    payload_len = pos;
  }
  return ret;
}

// EXECUTE_ADAPTER_REQ:
// session_id(8B) + timeout_ts(8B) + mode(1B) + udf_id(8B) + ctx_id(8B)
// + method_name(2B length) + FunctionInfo(4B length) + BatchedArgs.
int ObJavaUDFProxy::build_execute_adapter_req_payload(
    int64_t session_id,
    int64_t timeout_ts,
    int64_t udf_id,
    int64_t ctx_id,
    const common::ObString &method_name,
    const char *function_info_buf,
    int64_t function_info_len,
    const char *batched_args_buf,
    int64_t batched_args_len,
    common::ObIAllocator &alloc,
    char *&payload,
    int64_t &payload_len)
{
  int ret = OB_SUCCESS;
  // Fixed context, method name, length-delimited FunctionInfo, then original BatchedArgs.
  int64_t total = 33
      + 2 + method_name.length()
      + 4 + function_info_len
      + batched_args_len;
  payload = static_cast<char*>(alloc.alloc(total));
  if (OB_ISNULL(payload)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t pos = 0;
    encode_be64(payload + pos, session_id); pos += 8;
    encode_be64(payload + pos, timeout_ts); pos += 8;
    payload[pos++] = 0; // mode = MySQL
    encode_be64(payload + pos, udf_id); pos += 8;
    encode_be64(payload + pos, ctx_id); pos += 8;

    encode_be16(payload + pos, static_cast<uint16_t>(method_name.length())); pos += 2;
    memcpy(payload + pos, method_name.ptr(), method_name.length()); pos += method_name.length();

    encode_be32(payload + pos, static_cast<int32_t>(function_info_len)); pos += 4;
    memcpy(payload + pos, function_info_buf, function_info_len); pos += function_info_len;

    if (batched_args_len > 0 && OB_NOT_NULL(batched_args_buf)) {
      memcpy(payload + pos, batched_args_buf, batched_args_len); pos += batched_args_len;
    }
    payload_len = pos;
  }
  return ret;
}

// ---- EXECUTE_RESP parsing ----
// Wire format: status(1B) + body
//   status=0: body = Values protobuf
//   status=1: body = error_code(4B) + error_msg(UTF-8)

int ObJavaUDFProxy::parse_execute_resp(
    const char *payload,
    int64_t payload_len,
    const sql::ObExprResType &res_type,
    sql::ObSQLSessionInfo &session,
    common::ObIAllocator &result_allocator,
    common::ObIArray<common::ObObj> &result,
    int64_t batch_size,
    bool need_infer_result_size,
    JavaSandboxDrainReason *reason)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(reason)) {
    *reason = JSDR_NONE;
  }
  if (OB_ISNULL(payload) || payload_len < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty execute resp", K(ret), K(payload_len));
  } else {
    uint8_t status = static_cast<uint8_t>(payload[0]);
    const char *body = payload + 1;
    int64_t body_len = payload_len - 1;

    if (status == 0) {
      // success: body is Values protobuf
      ProtobufCAllocator c_alloc = {ObJavaUtils::protobuf_c_allocator_alloc,
                                    ObJavaUtils::protobuf_c_allocator_free,
                                    &result_allocator};
      ObPl__JavaUdf__Values *values = ob_pl__java_udf__values__unpack(
          &c_alloc, body_len, reinterpret_cast<const uint8_t*>(body));
      if (OB_ISNULL(values)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("values unpack failed", K(ret));
      } else {
        int64_t actual_batch = batch_size;
        if (need_infer_result_size) {
          actual_batch = values->null_map.len;
        }
        if (actual_batch > 0) {
          ret = unpack_values_to_obobj(*values, actual_batch, res_type,
                                        session, result_allocator, result);
        }
        ob_pl__java_udf__values__free_unpacked(values, &c_alloc);
      }
    } else {
      // failure
      if (body_len >= 4) {
        int32_t error_code = decode_be32(body);
        common::ObString error_msg(body_len - 4, body + 4);
        if (error_code == SANDBOX_ERR_JAVA_TIMEOUT
            || error_code == SANDBOX_ERR_JAVA_TIMEOUT_STUCK) {
          ret = OB_TIMEOUT;
        } else {
          ret = OB_JNI_JAVA_EXCEPTION_ERROR;
          LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, error_msg.length(), error_msg.ptr());
        }
        if (OB_NOT_NULL(reason)) {
          *reason = classify_sandbox_error(error_code, error_msg);
        }
        LOG_WARN("java sandbox execute error", K(ret), K(error_code), K(error_msg));
      } else {
        ret = OB_JNI_JAVA_EXCEPTION_ERROR;
        LOG_WARN("java sandbox execute error with short body", K(ret), K(body_len));
      }
    }
  }
  return ret;
}

// ---- ensure_jar_loaded_ ----

int ObJavaUDFProxy::ensure_jar_loaded(
    sql::ObExecContext &exec_ctx,
    JavaSandboxInstance &instance,
    int64_t udf_id,
    ObExternalRoutineType type,
    const common::ObString &url,
    const common::ObString &resource,
    int64_t session_id,
    bool *retryable_failure)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(retryable_failure)) {
    *retryable_failure = false;
  }
  bool is_url = (ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_URL == type);
  // scope: 0 = STATEMENT (URL jar, per-statement), 1 = SESSION (RES jar, per-session)
  char scope = is_url ? 0 : 1;

  // ---- Cache (shared with the JNI path) ----
  // Reuse the existing external_url_resource_cache_ / external_resource_schema_cache_
  // void* fields (same lifecycle: lazily created, destroyed in ~ObExecContext /
  // ObSQLSessionInfo::destroy). The destructor dispatches on the cache's is_sandbox()
  // stamp — set below when the sandbox path creates the cache — rather than on live
  // GCONF, so a mid-session GCONF flip cannot cause type confusion at destruction.
  if (is_url) {
    ObExternalResourceCache<ObExternalURLJar> *cache = nullptr;
    if (OB_FAIL(ObJavaUDFExecutor::get_url_jar_cache(exec_ctx, cache))) {
      LOG_WARN("get_url_jar_cache failed", K(ret));
    } else if (OB_NOT_NULL(cache) && !cache->is_sandbox()) {
      cache->set_sandbox(true);
      cache->set_sandbox_ctx_id(ATOMIC_AAF(&next_ctx_id_, 1));
      cache->set_sandbox_generation_id(instance.get_generation_id());
    } else if (OB_NOT_NULL(cache)
               && cache->get_sandbox_generation_id() != instance.get_generation_id()) {
      const int64_t old_ctx_id = cache->get_sandbox_ctx_id();
      const int64_t old_generation_id = cache->get_sandbox_generation_id();
      if (old_ctx_id != 0 && old_generation_id != 0) {
        evict_statement(old_ctx_id, old_generation_id);
      }
      cache->clear_entries();
      cache->set_sandbox_ctx_id(ATOMIC_AAF(&next_ctx_id_, 1));
      cache->set_sandbox_generation_id(instance.get_generation_id());
    }
  } else {
    ObExternalResourceCache<ObExternalSchemaJar> *cache = nullptr;
    if (OB_FAIL(ObJavaUDFExecutor::get_schema_jar_cache(exec_ctx, cache))) {
      LOG_WARN("get_schema_jar_cache failed", K(ret));
    } else if (OB_NOT_NULL(cache) && !cache->is_sandbox()) {
      cache->set_sandbox(true);
      cache->set_sandbox_generation_id(instance.get_generation_id());
    } else if (OB_NOT_NULL(cache)
               && cache->get_sandbox_generation_id() != instance.get_generation_id()) {
      cache->clear_entries();
      cache->set_sandbox_generation_id(instance.get_generation_id());
    }
  }

  // ---- Dedup + RES version check ----
  bool need_load = true;
  if (OB_SUCC(ret)) {
    if (is_url) {
      ObExternalResourceCache<ObExternalURLJar> *cache =
        static_cast<ObExternalResourceCache<ObExternalURLJar>*>(exec_ctx.get_external_url_resource_cache());
      if (OB_NOT_NULL(cache)
          && cache->get_sandbox_generation_id() == instance.get_generation_id()
          && cache->contains(udf_id)) {
        // URL jar: always valid within the statement, skip reload
        need_load = false;
      }
    } else {
      ObExternalResourceCache<ObExternalSchemaJar> *cache =
        static_cast<ObExternalResourceCache<ObExternalSchemaJar>*>(
          exec_ctx.get_my_session()->get_external_resource_schema_cache());
      ObExternalSchemaJar *node = nullptr;
      if (OB_NOT_NULL(cache)
          && cache->get_sandbox_generation_id() == instance.get_generation_id()
          && OB_SUCCESS == cache->get(udf_id, node) && OB_NOT_NULL(node)) {
        share::schema::ObSchemaGetterGuard *schema_guard = exec_ctx.get_sql_ctx()
          ? exec_ctx.get_sql_ctx()->schema_guard_ : nullptr;
        if (OB_ISNULL(schema_guard)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null schema_guard", K(ret));
        } else {
          int tmp_ret = node->check_valid_impl(*schema_guard);
          if (OB_SUCCESS == tmp_ret) {
            need_load = false;  // same version, skip
          } else if (OB_OLD_SCHEMA_VERSION == tmp_ret) {
            // version changed (or dropped): evict stale node, will reload
            if (OB_SUCCESS != cache->erase(udf_id)) {
              LOG_WARN("erase stale schema jar failed", K(ret), K(udf_id));
            } else {
              node->~ObExternalSchemaJar();
              cache->get_alloc().free(node);
            }
          } else {
            ret = tmp_ret;
            LOG_WARN("check_valid_impl failed", K(ret), K(udf_id));
          }
        }
      }
    }
  }

  // ---- Fetch jar bytes + send LOAD_JAR ----
  // For RES we need resource_id + schema_version both for the LOAD_JAR payload
  // and for stamping the new cache node.
  uint64_t resource_id = OB_INVALID_ID;
  int64_t schema_version = 0;
  if (OB_SUCC(ret) && need_load) {
    common::ObArenaAllocator tmp_alloc(common::ObMemAttr(MTL_ID(), "JavaSbJar"));
    common::ObSqlString jar_data;

    if (is_url) {
      if (OB_FAIL(ObExternalURLJar::curl_fetch(url, jar_data))) {
        LOG_WARN("curl_fetch jar failed", K(ret), K(url));
      }
    } else {
      share::schema::ObSchemaGetterGuard *schema_guard = nullptr;
      const share::schema::ObSimpleExternalResourceSchema *schema = nullptr;
      uint64_t database_id = OB_INVALID_ID;
      if (OB_ISNULL(exec_ctx.get_sql_ctx()) ||
          OB_ISNULL(schema_guard = exec_ctx.get_sql_ctx()->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema guard is null", K(ret), KP(exec_ctx.get_sql_ctx()));
      } else if (OB_UNLIKELY(OB_INVALID_ID ==
                     (database_id = exec_ctx.get_my_session()->get_database_id()))) {
        ret = OB_ERR_NO_DB_SELECTED;
      } else if (OB_FAIL(schema_guard->get_external_resource_schema(
                     MTL_ID(), database_id, resource, schema))) {
        LOG_WARN("get_external_resource_schema failed", K(ret));
      } else if (OB_ISNULL(schema)) {
        ret = OB_ERR_OBJECT_NOT_EXIST;
        LOG_WARN("external resource not found", K(ret), K(resource));
      } else {
        resource_id = schema->get_resource_id();
        schema_version = schema->get_schema_version();
        // Query inner table for jar content (same logic as ObExternalSchemaJar::fetch_from_inner_table)
        common::sqlclient::ObISQLConnectionPool *pool = nullptr;
        common::sqlclient::ObISQLConnection *conn = nullptr;
        common::ObSqlString sql;
        if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(pool = GCTX.sql_proxy_->get_pool())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sql proxy or connection pool is null", K(ret), KP(GCTX.sql_proxy_), KP(pool));
        } else if (OB_FAIL(pool->acquire(conn, nullptr))) {
          LOG_WARN("acquire connection failed", K(ret));
        } else if (OB_ISNULL(conn)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("acquired connection is null", K(ret));
        } else if (OB_FAIL(sql.append_fmt(
                       "SELECT content FROM %s WHERE tenant_id=0 AND resource_id=%lu AND schema_version=%ld",
                       share::OB_ALL_EXTERNAL_RESOURCE_HISTORY_TNAME,
                       resource_id,
                       schema_version))) {
          LOG_WARN("append_fmt failed", K(ret));
        } else {
          SMART_VAR(ObMySQLProxy::MySQLResult, res) {
            common::sqlclient::ObMySQLResult *result = nullptr;
            if (OB_FAIL(conn->execute_read(MTL_ID(), sql.string(), res))) {
              LOG_WARN("execute_read failed", K(ret), K(sql));
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_ERR_OBJECT_NOT_EXIST;
              }
            } else {
              common::ObString content;
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "content", content);
              if (OB_SUCC(ret) && content.length() > 0) {
                if (OB_FAIL(jar_data.append(content))) {
                  LOG_WARN("append jar content failed", K(ret));
                }
              }
            }
          }
        }
        if (OB_NOT_NULL(conn) && OB_NOT_NULL(pool)) {
          pool->release(conn, OB_SUCCESS == ret);
        }
      }
    }

    // Send LOAD_JAR to sandbox
    if (OB_SUCC(ret)) {
      int64_t jar_len = jar_data.length();
      int64_t ctx_id = 0;
      const int64_t query_timeout_ts = exec_ctx.get_my_session()->get_query_timeout_ts();
      const int64_t worker_timeout_ts = THIS_WORKER.get_timeout_ts();
      const int64_t request_deadline_ts = std::min(query_timeout_ts, worker_timeout_ts);
      const int64_t load_deadline_ts =
          request_deadline_ts - JAVA_SANDBOX_LOAD_JAR_RESPONSE_MARGIN_US;
      if (is_url) {
        using Cache = ObExternalResourceCache<ObExternalURLJar>;
        Cache *c = static_cast<Cache *>(exec_ctx.get_external_url_resource_cache());
        if (OB_NOT_NULL(c)) { ctx_id = c->get_sandbox_ctx_id(); }
      }
      // payload: type(1) + udf_id(8) + schema_version(8) + session_id(8)
      //          + scope(1) + ctx_id(8) + load_deadline_ts(8) + jar_bytes
      int64_t payload_len = 1 + 8 + 8 + 8 + 1 + 8 + 8 + jar_len;
      char *payload = nullptr;
      if (load_deadline_ts <= ObTimeUtility::current_time()) {
        ret = OB_TIMEOUT;
        LOG_WARN("no time left to load jar in java sandbox",
                 K(ret), K(request_deadline_ts), K(load_deadline_ts));
      } else if (OB_ISNULL(payload = static_cast<char*>(tmp_alloc.alloc(payload_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t pos = 0;
        payload[pos++] = static_cast<char>(type);
        encode_be64(payload + pos, udf_id); pos += 8;
        encode_be64(payload + pos, schema_version); pos += 8;
        encode_be64(payload + pos, session_id); pos += 8;
        payload[pos++] = scope;
        encode_be64(payload + pos, ctx_id); pos += 8;
        encode_be64(payload + pos, load_deadline_ts); pos += 8;
        if (jar_len > 0) {
          memcpy(payload + pos, jar_data.ptr(), jar_len);
          pos += jar_len;
        }

        ObJavaChannel *ch = nullptr;
        JavaSandboxDrainReason failure_reason = JSDR_NONE;
        if (OB_FAIL(instance.get_channel_pool().acquire(ch))) {
          LOG_WARN("acquire channel failed", K(ret));
        } else {
          bool channel_reusable = true;
          bool channel_quarantined = false;
          uint32_t req_id = 0;
          if (OB_FAIL(ch->send_frame(JAVA_SANDBOX_LOAD_JAR, payload, payload_len, req_id))) {
            channel_reusable = false;
            if (OB_NOT_NULL(retryable_failure)
                && is_sandbox_channel_failure(ret)) {
              *retryable_failure = true;
            }
            LOG_WARN("send LOAD_JAR failed", K(ret));
          } else {
            ObJavaSandboxFrameHeader resp_header;
            char *resp_payload = nullptr;
            int64_t now = ObTimeUtility::current_time();
            int64_t timeout_us = request_deadline_ts - now;
            LOG_TRACE("[SANDBOX_DIAG] LOAD_JAR timeout calc",
                      K(query_timeout_ts), K(worker_timeout_ts), K(load_deadline_ts),
                      K(now), K(timeout_us));
            if (timeout_us <= 0) {
              ret = OB_TIMEOUT;
              channel_reusable = false;
              channel_quarantined = true;
            } else if (OB_FAIL(ch->recv_frame_matched(req_id, timeout_us, resp_header, tmp_alloc, resp_payload))) {
              if (OB_TIMEOUT == ret) {
                channel_quarantined = true;
              } else if (OB_NOT_NULL(retryable_failure)
                         && is_sandbox_channel_failure(ret)) {
                // Loading a jar has no user-code side effects, so a transport
                // failure can safely be retried on a replacement JVM.
                *retryable_failure = true;
              }
              channel_reusable = false;
              LOG_WARN("recv LOAD_JAR_RESP failed", K(ret));
            } else if (resp_header.msg_type_ != JAVA_SANDBOX_LOAD_JAR_RESP) {
              ret = OB_ERR_UNEXPECTED;
              failure_reason = JSDR_PROTOCOL_ERROR;
              channel_reusable = false;
              LOG_WARN("unexpected resp type", K(ret), K(resp_header.msg_type_));
            } else if (resp_header.payload_len_ > 0 && OB_NOT_NULL(resp_payload)
                       && static_cast<uint8_t>(resp_payload[0]) != 0) {
              if (resp_header.payload_len_ >= 5) {
                int32_t err = decode_be32(resp_payload + 1);
                int64_t msg_len = resp_header.payload_len_ - 5;
                common::ObString error_msg(msg_len, resp_payload + 5);
                ret = (err == SANDBOX_ERR_JAVA_TIMEOUT
                       || err == SANDBOX_ERR_JAVA_TIMEOUT_STUCK)
                          ? OB_TIMEOUT : OB_JNI_JAVA_EXCEPTION_ERROR;
                if (ret == OB_JNI_JAVA_EXCEPTION_ERROR) {
                  LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, error_msg.length(), error_msg.ptr());
                }
                failure_reason = classify_sandbox_error(err, error_msg);
                LOG_WARN("LOAD_JAR failed on sandbox side", K(ret), K(err), K(error_msg));
              } else {
                ret = OB_JNI_JAVA_EXCEPTION_ERROR;
                failure_reason = JSDR_PROTOCOL_ERROR;
                channel_reusable = false;
                LOG_WARN("LOAD_JAR failed on sandbox side with short body",
                         K(ret), K(resp_header.payload_len_));
              }
            }
          }
          if (channel_reusable) {
            instance.get_channel_pool().release(ch);
          } else if (channel_quarantined) {
            instance.get_channel_pool().quarantine(
                ch, req_id, JAVA_SANDBOX_LOAD_JAR_RESP, ctx_id, scope);
          } else {
            instance.get_channel_pool().discard(ch);
          }
          if (OB_FAIL(ret)) { handle_request_failure(&instance, ret, failure_reason); }
        }
      }
    }

    // Record in cache: node with data_ = nullptr (no in-process classloader;
    // the classloader lives in the sandbox JVM). Dedup table only.
    if (OB_SUCC(ret)) {
      if (is_url) {
        ObExternalResourceCache<ObExternalURLJar> *cache =
          static_cast<ObExternalResourceCache<ObExternalURLJar>*>(exec_ctx.get_external_url_resource_cache());
        if (OB_ISNULL(cache)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          void *buf = cache->get_alloc().alloc(sizeof(ObExternalURLJar));
          ObExternalURLJar *node = OB_ISNULL(buf) ? nullptr : new (buf) ObExternalURLJar(cache->get_alloc());
          if (OB_ISNULL(node)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc url jar node failed", K(ret));
          } else if (OB_FAIL(cache->insert(udf_id, node))) {
            LOG_WARN("insert url jar node failed", K(ret), K(udf_id));
            node->~ObExternalURLJar();
            cache->get_alloc().free(node);
          }
        }
      } else {
        ObExternalResourceCache<ObExternalSchemaJar> *cache =
          static_cast<ObExternalResourceCache<ObExternalSchemaJar>*>(
            exec_ctx.get_my_session()->get_external_resource_schema_cache());
        if (OB_ISNULL(cache)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          void *buf = cache->get_alloc().alloc(sizeof(ObExternalSchemaJar));
          ObExternalSchemaJar *node = OB_ISNULL(buf)
            ? nullptr
            : new (buf) ObExternalSchemaJar(cache->get_alloc(), resource_id, schema_version);
          if (OB_ISNULL(node)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc schema jar node failed", K(ret));
          } else if (OB_FAIL(cache->insert(udf_id, node))) {
            LOG_WARN("insert schema jar node failed", K(ret), K(udf_id));
            node->~ObExternalSchemaJar();
            cache->get_alloc().free(node);
          }
        }
      }
    }
  }
  return ret;
}

// ---- Phase 1: evict + cleanup ----

void ObJavaUDFProxy::evict_statement(int64_t ctx_id, int64_t generation_id)
{
  JavaSandboxInstance *instance = nullptr;
  if (ctx_id != 0) {
    lib::ObMutexGuard guard(launch_mutex_);
    if (generation_id != 0) {
      for (int64_t i = 0; OB_ISNULL(instance) && i < instances_.count(); ++i) {
        JavaSandboxInstance *candidate = instances_.at(i);
        if (OB_NOT_NULL(candidate)
            && candidate->get_generation_id() == generation_id
            && (JSS_ACTIVE == candidate->get_state()
                || JSS_DRAINING == candidate->get_state())) {
          candidate->inc_ref();
          instance = candidate;
        }
      }
    } else {
      JavaSandboxInstance *candidate = get_active_instance_atomic();
      if (OB_NOT_NULL(candidate) && JSS_ACTIVE == candidate->get_state()) {
        candidate->inc_ref();
        instance = candidate;
      }
    }
  }

  if (OB_NOT_NULL(instance)) {
    ObJavaChannel *ch = nullptr;
    if (OB_SUCCESS == instance->get_channel_pool().acquire(ch) && OB_NOT_NULL(ch)) {
      char payload[8];
      encode_be64(payload, ctx_id);
      uint32_t req_id = 0;
      ch->send_frame(JAVA_SANDBOX_EVICT_STATEMENT, payload, 8, req_id);
      instance->get_channel_pool().release(ch);
      LOG_TRACE("[SANDBOX_DIAG] evict_statement sent",
                K(ctx_id), K(generation_id), "target_generation", instance->get_generation_id());
    } else {
      LOG_TRACE("[SANDBOX_DIAG] evict_statement: acquire channel failed or sandbox not running",
                K(ctx_id), K(generation_id));
    }
    instance->dec_ref();
  } else if (ctx_id != 0) {
    LOG_TRACE("[SANDBOX_DIAG] evict_statement skipped, generation is not alive",
              K(ctx_id), K(generation_id));
  }
}

// ---- execute_mysql ----

int ObJavaUDFProxy::execute_mysql(
    sql::ObExecContext &exec_ctx,
    int64_t udf_id,
    ObExternalRoutineType type,
    const common::ObString &url,
    const common::ObString &resource,
    const common::ObString &entry,
    int64_t batch_size,
    const char *method_name,
    const common::ObIArray<sql::ObExprResType> &arg_types,
    const common::ObIArray<common::ObIArray<common::ObObj>*> &args,
    const sql::ObExprResType &res_type,
    common::ObIAllocator &result_allocator,
    common::ObIArray<common::ObObj> &result,
    bool need_infer_result_size)
{
  int ret = OB_SUCCESS;

  if (!GCONF.ob_enable_java_udf_sandbox) {
    ObJavaUDFExecutor executor(exec_ctx, entry);
    if (OB_FAIL(executor.init(udf_id, type, url, resource))) {
      LOG_WARN("failed to init ObJavaUDFExecutor", K(ret));
    } else {
      executor.set_need_infer_result_size(need_infer_result_size);
      if (OB_FAIL(executor.execute(batch_size, method_name, arg_types, args,
                                   res_type, result_allocator, result))) {
        LOG_WARN("failed to execute java UDF via JNI", K(ret));
      }
    }
  } else {
    static const int64_t MAX_SAFE_TRANSPORT_ATTEMPTS = 2;
    for (int64_t attempt = 0; attempt < MAX_SAFE_TRANSPORT_ATTEMPTS; ++attempt) {
      bool retryable_failure = false;
      ret = OB_SUCCESS;
      sql::ObSQLSessionInfo *session = exec_ctx.get_my_session();
      share::schema::ObSchemaGetterGuard *schema_guard = nullptr;

      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("session is null", K(ret));
      } else if (OB_ISNULL(exec_ctx.get_sql_ctx()) ||
                OB_ISNULL(schema_guard = exec_ctx.get_sql_ctx()->schema_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("schema_guard is null", K(ret));
      } else {
        JavaSandboxRequestHandle sandbox_handle;
        JavaSandboxInstance *instance = nullptr;
        const int64_t request_deadline_ts =
            std::min(session->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts());
        if (OB_FAIL(acquire_instance_for_request(request_deadline_ts, sandbox_handle))) {
          LOG_WARN("acquire java sandbox instance failed", K(ret));
        } else if (OB_ISNULL(instance = sandbox_handle.get_instance())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null java sandbox instance", K(ret));
        } else if (OB_FAIL(ensure_jar_loaded(
                       exec_ctx, *instance, udf_id, type, url, resource,
                       exec_ctx.get_my_session()->get_server_sid(),
                       &retryable_failure))) {
          LOG_WARN("ensure_jar_loaded failed", K(ret), K(udf_id),
                   K(retryable_failure), K(attempt));
        } else {
          common::ObArenaAllocator tmp_alloc(common::ObMemAttr(MTL_ID(), "JavaSbExec"));

          // 1. build protobuf args
          char *args_buf = nullptr;
          int64_t args_len = 0;
          char *function_info_buf = nullptr;
          int64_t function_info_len = 0;
          if (OB_FAIL(ObJavaUtils::build_function_info(
                  entry,
                  arg_types, res_type, tmp_alloc, function_info_buf, function_info_len))) {
            LOG_WARN("build function info failed", K(ret));
          } else if (OB_FAIL(ObJavaUtils::build_udf_args_to_buffer(*session, *schema_guard, batch_size,
                                                arg_types, args, tmp_alloc,
                                                args_buf, args_len))) {
            LOG_WARN("build_udf_args_to_buffer failed", K(ret));
          } else {
            // 2. build EXECUTE_ADAPTER_REQ payload
            common::ObString method_str(strlen(method_name), method_name);

            int64_t ctx_id = 0;
            {
              bool url_type = (ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_URL == type);
              if (url_type) {
                using Cache = ObExternalResourceCache<ObExternalURLJar>;
                Cache *c = static_cast<Cache *>(exec_ctx.get_external_url_resource_cache());
                if (OB_NOT_NULL(c)) { ctx_id = c->get_sandbox_ctx_id(); }
              }
            }

            char *payload = nullptr;
            int64_t payload_len = 0;
            if (OB_FAIL(build_execute_adapter_req_payload(
                    session->get_server_sid(),
                    std::min(session->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts()),
                    udf_id, ctx_id, method_str, function_info_buf, function_info_len,
                    args_buf, args_len, tmp_alloc, payload, payload_len))) {
              LOG_WARN("build payload failed", K(ret));
            }
            if (OB_SUCC(ret)) {
              // 3. send/recv via channel
              ObJavaChannel *ch = nullptr;
              JavaSandboxDrainReason failure_reason = JSDR_NONE;
              if (OB_FAIL(instance->get_channel_pool().acquire(ch))) {
                LOG_WARN("acquire channel failed", K(ret));
              } else {
                bool channel_reusable = true;
                bool channel_quarantined = false;
                uint32_t req_id = 0;
                LOG_TRACE("[SANDBOX_DIAG] execute_mysql send_frame", "fd", ch->get_fd(),
                          "sandbox_pid", OB_NOT_NULL(instance->get_sandbox_process())
                                          ? instance->get_sandbox_process()->get_pid() : -1,
                          "generation_id", instance->get_generation_id());
                if (OB_FAIL(ch->send_frame(JAVA_SANDBOX_EXECUTE_ADAPTER_REQ, payload, payload_len, req_id))) {
                  channel_reusable = false;
                  retryable_failure = is_sandbox_channel_failure(ret);
                  LOG_WARN("[SANDBOX_DIAG] send EXECUTE_REQ failed",
                           K(ret), "fd", ch->get_fd(), K(retryable_failure));
                } else {
                  ObJavaSandboxFrameHeader resp_header;
                  char *resp_payload = nullptr;
                  int64_t timeout_us = std::min(session->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts())
                                      - common::ObTimeUtility::current_time();
                  if (timeout_us <= 0) {
                    ret = OB_TIMEOUT;
                    channel_reusable = false;
                    channel_quarantined = true;
                    LOG_WARN("[SANDBOX_DIAG] execute_mysql timeout before recv", K(ret), K(timeout_us));
                  } else if (OB_FAIL(ch->recv_frame_matched(req_id, timeout_us, resp_header, result_allocator, resp_payload))) {
                    channel_reusable = false;
                    channel_quarantined = (OB_TIMEOUT == ret);
                    LOG_WARN("[SANDBOX_DIAG] recv EXECUTE_RESP failed", K(ret), "fd", ch->get_fd(),
                            K(timeout_us));
                  } else if (resp_header.msg_type_ != JAVA_SANDBOX_EXECUTE_RESP) {
                    ret = OB_ERR_UNEXPECTED;
                    failure_reason = JSDR_PROTOCOL_ERROR;
                    channel_reusable = false;
                    LOG_WARN("unexpected resp msg_type", K(ret), K(resp_header.msg_type_));
                  } else if (OB_FAIL(parse_execute_resp(resp_payload, resp_header.payload_len_,
                                                          res_type, *session, result_allocator,
                                                          result, batch_size, need_infer_result_size,
                                                          &failure_reason))) {
                    LOG_WARN("parse execute resp failed", K(ret));
                  }
                }
                if (channel_reusable) {
                  instance->get_channel_pool().release(ch);
                } else if (channel_quarantined) {
                  instance->get_channel_pool().quarantine(
                      ch, req_id, JAVA_SANDBOX_EXECUTE_RESP, 0, 0);
                } else {
                  instance->get_channel_pool().discard(ch);
                }
                if (OB_FAIL(ret)) {
                  LOG_TRACE("[SANDBOX_DIAG] execute_mysql request failed, handling channel state",
                            K(ret), "fd", ch->get_fd());
                  handle_request_failure(instance, ret, failure_reason);
                }
              }
            }
          }
        }
      }
      if (OB_FAIL(ret) && retryable_failure
          && 0 == attempt
          && ObTimeUtility::current_time()
             < std::min(session->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts())) {
        LOG_WARN("retry java udf on replacement sandbox after pre-execution transport failure",
                 K(ret), K(udf_id), K(attempt));
        ret = OB_SUCCESS;
      } else {
        break;
      }
    }
  } // else (sandbox path)
  return ret;
}

// ---- execute_mysql_udaf ----

int ObJavaUDFProxy::execute_mysql_udaf(
    const sql::ObAggrInfo &aggr_info,
    sql::ObEvalCtx &eval_ctx,
    void *extra_result_ptr,
    common::ObDatum &result)
{
  int ret = OB_SUCCESS;

  if (!GCONF.ob_enable_java_udf_sandbox) {
    using GroupConcatExtraResult = sql::ObAggregateProcessor::GroupConcatExtraResult;
    GroupConcatExtraResult &extra = *static_cast<GroupConcatExtraResult*>(extra_result_ptr);
    ObJavaUDAFExecutor executor(aggr_info, eval_ctx, extra);
    if (OB_FAIL(executor.init())) {
      LOG_WARN("failed to init java udaf executor", K(ret));
    } else if (OB_FAIL(executor.execute(result))) {
      LOG_WARN("failed to execute java UDAF via JNI", K(ret));
    }
  } else {

  using GroupConcatExtraResult = sql::ObAggregateProcessor::GroupConcatExtraResult;
  GroupConcatExtraResult &extra_result = *static_cast<GroupConcatExtraResult*>(extra_result_ptr);
  sql::ObSQLSessionInfo *session = eval_ctx.exec_ctx_.get_my_session();
  share::schema::ObSchemaGetterGuard *schema_guard = nullptr;

  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_ISNULL(eval_ctx.exec_ctx_.get_sql_ctx()) ||
             OB_ISNULL(schema_guard = eval_ctx.exec_ctx_.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_guard is null", K(ret));
  } else if (OB_ISNULL(aggr_info.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    JavaSandboxRequestHandle sandbox_handle;
    JavaSandboxInstance *instance = nullptr;
    const int64_t request_deadline_ts =
        std::min(session->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts());
    if (OB_FAIL(acquire_instance_for_request(request_deadline_ts, sandbox_handle))) {
      LOG_WARN("acquire java sandbox instance failed", K(ret));
    } else if (OB_ISNULL(instance = sandbox_handle.get_instance())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null java sandbox instance", K(ret));
    } else if (OB_FAIL(ensure_jar_loaded(eval_ctx.exec_ctx_,
                                         *instance,
                                         aggr_info.pl_agg_udf_type_id_,
                                         aggr_info.external_routine_type_,
                                         aggr_info.external_routine_url_,
                                         aggr_info.external_routine_resource_,
                                         eval_ctx.exec_ctx_.get_my_session()->get_server_sid()))) {
      LOG_WARN("ensure_jar_loaded failed", K(ret));
    } else {
    common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "JavaSbUdaf"));
    common::ObArenaAllocator batch_allocator(common::ObMemAttr(MTL_ID(), "JavaSbUdafB"));

    const int64_t arg_count = aggr_info.pl_agg_udf_params_type_.count();
    const int64_t batch_size = std::max(eval_ctx.get_batch_size(), 512L);

    const ObChunkDatumStore::StoredRow *stored_row = nullptr;

    using ColumnType = common::ObSEArray<common::ObObj, 512>;

    const common::ObIArray<sql::ObExprResType> &arg_types = aggr_info.pl_agg_udf_params_type_;
    common::ObSEArray<common::ObIArray<common::ObObj>*, 8> args;

    for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; ++i) {
      ColumnType *column = nullptr;
      if (OB_ISNULL(column = static_cast<ColumnType*>(allocator.alloc(sizeof(ColumnType))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_ISNULL(column = new(column)ColumnType())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(args.push_back(column))) {
        LOG_WARN("push_back args failed", K(ret));
      }
    }

    int64_t timeout_ts = std::min(session->get_query_timeout_ts(), THIS_WORKER.get_timeout_ts());

    int64_t udaf_ctx_id = 0;
    {
      bool url_type = (ObExternalRoutineType::EXTERNAL_JAVA_UDF_FROM_URL == aggr_info.external_routine_type_);
      if (url_type) {
        using Cache = ObExternalResourceCache<ObExternalURLJar>;
        Cache *c = static_cast<Cache *>(eval_ctx.exec_ctx_.get_external_url_resource_cache());
        if (OB_NOT_NULL(c)) { udaf_ctx_id = c->get_sandbox_ctx_id(); }
      }
    }

    common::ObString entry;
    char *function_info_buf = nullptr;
    int64_t function_info_len = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_write_string(allocator, aggr_info.external_routine_entry_, entry, true))) {
        LOG_WARN("ob_write_string failed", K(ret));
      } else if (OB_FAIL(ObJavaUtils::build_function_info(
                     entry, arg_types, aggr_info.pl_result_type_, allocator,
                     function_info_buf, function_info_len))) {
        LOG_WARN("build aggregate function info failed", K(ret));
      }
    }

    // Acquire channel and hold for entire UDAF lifetime
    ObJavaChannel *ch = nullptr;
    JavaSandboxDrainReason failure_reason = JSDR_NONE;
    bool channel_reusable = true;
    bool channel_quarantined = false;
    uint32_t pending_req_id = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(instance->get_channel_pool().acquire(ch))) {
        LOG_WARN("acquire channel failed", K(ret));
      }
    }

    // iterate phase
    while (OB_SUCC(ret)) {
      int64_t row_count = 0;
      batch_allocator.reuse();

      for (int64_t i = 0; OB_SUCC(ret) && i < arg_count; ++i) {
        args.at(i)->reuse();
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        if (OB_FAIL(extra_result.get_next_row(stored_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        } else if (OB_ISNULL(stored_row) || stored_row->cnt_ != arg_count) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < arg_count; ++j) {
            common::ObObj obj;
            if (OB_FAIL(stored_row->cells()[j].to_obj(obj, aggr_info.param_exprs_.at(j)->obj_meta_))) {
              LOG_WARN("to_obj failed", K(ret));
            } else if (OB_FAIL(args.at(j)->push_back(obj))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) ++row_count;
        }
      }

      if (OB_SUCC(ret) && row_count > 0) {
        char *args_buf = nullptr;
        int64_t args_len = 0;
        if (OB_FAIL(ObJavaUtils::build_udf_args_to_buffer(*session, *schema_guard, row_count,
                                               arg_types, args, batch_allocator,
                                               args_buf, args_len))) {
          LOG_WARN("build_udf_args_to_buffer failed", K(ret));
        } else {
          common::ObString method_str("iterate");
          char *payload = nullptr;
          int64_t payload_len = 0;
          if (OB_FAIL(build_execute_adapter_req_payload(
                  session->get_server_sid(), timeout_ts,
                  aggr_info.pl_agg_udf_type_id_, udaf_ctx_id, method_str,
                  function_info_buf, function_info_len, args_buf, args_len,
                  batch_allocator, payload, payload_len))) {
            LOG_WARN("build payload failed", K(ret));
          }
          if (OB_SUCC(ret)) {
            uint32_t req_id = 0;
            if (OB_FAIL(ch->send_frame(JAVA_SANDBOX_EXECUTE_ADAPTER_REQ, payload, payload_len, req_id))) {
              channel_reusable = false;
              LOG_WARN("send iterate failed", K(ret));
            } else {
              pending_req_id = req_id;
              ObJavaSandboxFrameHeader resp_header;
              char *resp_payload = nullptr;
              int64_t timeout_us = timeout_ts - ObTimeUtility::current_time();
              if (timeout_us <= 0) {
                ret = OB_TIMEOUT;
                channel_reusable = false;
                channel_quarantined = true;
              } else if (OB_FAIL(ch->recv_frame_matched(req_id, timeout_us, resp_header, batch_allocator, resp_payload))) {
                channel_reusable = false;
                channel_quarantined = (OB_TIMEOUT == ret);
                LOG_WARN("recv iterate resp failed", K(ret));
              } else {
                pending_req_id = 0;
                if (JAVA_SANDBOX_EXECUTE_RESP != resp_header.msg_type_) {
                  ret = OB_ERR_UNEXPECTED;
                  failure_reason = JSDR_PROTOCOL_ERROR;
                  channel_reusable = false;
                  LOG_WARN("unexpected iterate response", K(ret), K(resp_header.msg_type_),
                           K(resp_header.payload_len_));
                } else if (resp_header.payload_len_ > 0 && OB_ISNULL(resp_payload)) {
                  ret = OB_ERR_UNEXPECTED;
                  failure_reason = JSDR_PROTOCOL_ERROR;
                  channel_reusable = false;
                  LOG_WARN("iterate response payload is null", K(ret),
                           K(resp_header.payload_len_));
                } else if (resp_header.payload_len_ > 0
                           && static_cast<uint8_t>(resp_payload[0]) != 0) {
                  // iterate error
                  if (resp_header.payload_len_ >= 5) {
                    int32_t err = decode_be32(resp_payload + 1);
                    int64_t msg_len = resp_header.payload_len_ - 5;
                    common::ObString error_msg(msg_len, resp_payload + 5);
                    ret = (err == SANDBOX_ERR_JAVA_TIMEOUT
                           || err == SANDBOX_ERR_JAVA_TIMEOUT_STUCK)
                              ? OB_TIMEOUT : OB_JNI_JAVA_EXCEPTION_ERROR;
                    if (ret == OB_JNI_JAVA_EXCEPTION_ERROR) {
                      LOG_USER_ERROR(OB_JNI_JAVA_EXCEPTION_ERROR, error_msg.length(), error_msg.ptr());
                    }
                    failure_reason = classify_sandbox_error(err, error_msg);
                    LOG_WARN("iterate error from sandbox", K(ret), K(err), K(error_msg),
                             K(entry), K(aggr_info.pl_agg_udf_type_id_));
                  } else {
                    ret = OB_JNI_JAVA_EXCEPTION_ERROR;
                    failure_reason = JSDR_PROTOCOL_ERROR;
                    LOG_WARN("iterate error from sandbox with short body", K(ret),
                             K(resp_header.payload_len_), K(entry), K(aggr_info.pl_agg_udf_type_id_));
                  }
                }
              }
            }
          }
        }
      }

      if (batch_size != row_count) {
        break;
      }
    }

    // terminate phase
    if (OB_SUCC(ret)) {
      batch_allocator.reuse();
      common::ObSEArray<sql::ObExprResType, 1> empty_types;
      common::ObSEArray<common::ObIArray<common::ObObj>*, 1> empty_args;

      char *args_buf = nullptr;
      int64_t args_len = 0;
      if (OB_FAIL(ObJavaUtils::build_udf_args_to_buffer(*session, *schema_guard, 1,
                                             empty_types, empty_args, batch_allocator,
                                             args_buf, args_len))) {
        LOG_WARN("build terminate args failed", K(ret));
      } else {
        common::ObString method_str("terminate");
        char *payload = nullptr;
        int64_t payload_len = 0;
        if (OB_FAIL(build_execute_adapter_req_payload(
                session->get_server_sid(), timeout_ts,
                aggr_info.pl_agg_udf_type_id_, udaf_ctx_id, method_str,
                function_info_buf, function_info_len, args_buf, args_len,
                batch_allocator, payload, payload_len))) {
          LOG_WARN("build terminate payload failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          uint32_t req_id = 0;
          if (OB_FAIL(ch->send_frame(JAVA_SANDBOX_EXECUTE_ADAPTER_REQ, payload, payload_len, req_id))) {
            channel_reusable = false;
            LOG_WARN("send terminate failed", K(ret));
          } else {
            pending_req_id = req_id;
            ObJavaSandboxFrameHeader resp_header;
            char *resp_payload = nullptr;
            int64_t term_timeout_us = timeout_ts - ObTimeUtility::current_time();
            if (term_timeout_us <= 0) {
              ret = OB_TIMEOUT;
              channel_reusable = false;
              channel_quarantined = true;
            } else if (OB_FAIL(ch->recv_frame_matched(req_id, term_timeout_us, resp_header, batch_allocator, resp_payload))) {
              channel_reusable = false;
              channel_quarantined = (OB_TIMEOUT == ret);
              LOG_WARN("recv terminate resp failed", K(ret));
            } else if (resp_header.msg_type_ != JAVA_SANDBOX_EXECUTE_RESP) {
              pending_req_id = 0;
              ret = OB_ERR_UNEXPECTED;
              failure_reason = JSDR_PROTOCOL_ERROR;
              channel_reusable = false;
            } else {
              pending_req_id = 0;
              // Parse terminate result
              common::ObSEArray<common::ObObj, 1> tmp_result;
              if (OB_FAIL(parse_execute_resp(resp_payload, resp_header.payload_len_,
                                              aggr_info.pl_result_type_, *session,
                                              batch_allocator, tmp_result, 1, false,
                                              &failure_reason))) {
                LOG_WARN("parse terminate resp failed", K(ret));
              } else if (1 != tmp_result.count()) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected terminate result count", K(ret), K(tmp_result.count()));
              } else if (OB_FAIL(result.from_obj(tmp_result.at(0), aggr_info.expr_->obj_datum_map_))) {
                LOG_WARN("from_obj failed", K(ret));
              } else if (OB_FAIL(aggr_info.expr_->deep_copy_datum(eval_ctx, result))) {
                LOG_WARN("deep_copy_datum failed", K(ret));
              }
            }
          }
        }
      }
    }

    // Return only a synchronized channel to the pool. A channel with an
    // outstanding timed-out request is isolated until its terminal response.
    if (OB_NOT_NULL(ch)) {
      if (channel_reusable) {
        instance->get_channel_pool().release(ch);
      } else if (channel_quarantined) {
        instance->get_channel_pool().quarantine(
            ch, pending_req_id, JAVA_SANDBOX_EXECUTE_RESP, 0, 0);
      } else {
        instance->get_channel_pool().discard(ch);
      }
      if (OB_FAIL(ret)) { handle_request_failure(instance, ret, failure_reason); }
    }

    // Cleanup
    for (int64_t i = 0; i < args.count(); ++i) {
      if (OB_NOT_NULL(args.at(i))) {
        args.at(i)->~ObIArray();
        args.at(i) = nullptr;
      }
    }
    }
  }
  } // else (sandbox udaf path)
  return ret;
}

} // namespace pl
} // namespace oceanbase
