/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX TEST
#include <getopt.h>
#include <gtest/gtest.h>
#include <string>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <future>
#include <thread>
#define protected public
#define private public
#include "share/scheduler/test_dag_common.h"
#include "share/scheduler/ob_independent_dag.h"
#include "lib/random/ob_random.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_dag_monitor_mgr.h"
#include "storage/ddl/ob_ddl_dag_monitor_entry.h"
#include "storage/blocksstable/vector_index/ob_ivf_refresh_monitor.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_store_ctx.h"

const int64_t STRESS_ROUND_CNT = 32;
const int64_t STRESS_THREAD_CNT = 10;

namespace oceanbase
{
namespace unittest{
/*========================= TestIndependentDag ===================================*/
class TestIndependentDag : public ::testing::Test
{
public:
  class ObPriorityTask : public ObITask
  {
  public:
    ObPriorityTask() : ObITask(ObITaskType::TASK_TYPE_UT), priority_(ObITask::TASK_PRIO_1) {}
    virtual int process() { return OB_SUCCESS; }
    virtual ObITaskPriority get_priority() override { return priority_; }
    virtual void task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const override { 
      BUF_PRINTF("ObPriorityTask : prio: %d, 40 more chars: ", priority_);
      for (int i = 0; i < 40; i++) {
        BUF_PRINTF("%c", 'A' + i);
      }
    }
    TO_STRING_KV(K_(priority));
  private:
  public:
    ObITaskPriority priority_;
  };
public:
  TestIndependentDag() 
    : tenant_id_(1001),
      tenant_base_(1001),
      addr_(1683068975, 9999),
      allocator_("IndependentDag"),
      scheduler_(nullptr),
      is_inited_(false) {}
  virtual ~TestIndependentDag() {}
  void SetUp() override
  {
    if (!is_inited_) {
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
      is_inited_ = true;
    }
    scheduler_ = OB_NEW(ObTenantDagScheduler, ObModIds::TEST);
    tenant_base_.set(scheduler_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());

    ObMallocAllocator *ma = ObMallocAllocator::get_instance();
    ASSERT_EQ(OB_SUCCESS, ma->set_tenant_limit(tenant_id_, 1LL << 30));

    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().set_self_addr(addr_));
    ASSERT_EQ(OB_SUCCESS, scheduler_->init(MTL_ID(), 1000 /*check_period*/));
    COMMON_LOG(INFO, "SetUp TestIndependentDag success");
  }
  void TearDown() override
  {
    scheduler_->destroy();
    scheduler_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
public:
  void check_task_list_priority(ObIDag::TaskList &task_list);
  void alloc_and_add_priority_task(ObIDag *dag, const ObITask::ObITaskPriority priority);
private:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObAddr addr_;
  ObArenaAllocator allocator_;
  ObTenantDagScheduler *scheduler_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(TestIndependentDag);
};

void TestIndependentDag::check_task_list_priority(ObIDag::TaskList &task_list)
{
  const ObITask *head = task_list.get_header();
  ObITask *cur_task = task_list.get_first();
  ObITask *pre_task = nullptr;
  while (head != cur_task) {
    if (OB_NOT_NULL(pre_task)) {
      ASSERT_GE(pre_task->get_priority(), cur_task->get_priority());
    }
    pre_task = cur_task;
    cur_task = cur_task->get_next();
  }
}

void TestIndependentDag::alloc_and_add_priority_task(
    ObIDag *dag,
    const ObITask::ObITaskPriority priority)
{
  ASSERT_NE(nullptr, dag);
  ASSERT_LE(priority, ObITask::ObITaskPriority::TASK_PRIO_MAX);
  ObPriorityTask* task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag->alloc_task(task));
  ASSERT_NE(nullptr, task);
  task->priority_ = priority;
  ASSERT_EQ(OB_SUCCESS, dag->add_task(*task));
}

/*========================= ObIndependentExampleDag ===================================*/
class ObTestIndependentBaseDag : public ObIndependentDag
{
public:
  ObTestIndependentBaseDag() : ObIndependentDag(ObDagType::DAG_TYPE_DDL_INDEPENDENT) {}
  ~ObTestIndependentBaseDag() {}
public:
  virtual uint64_t hash() const override { return 0; }
  virtual bool operator == (const ObIDag &other) const { return get_type() == other.get_type(); }
  virtual lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTestIndependentBaseDag);
};

struct ObIndependentInitParam : public ObIDagInitParam
{
public:
  ObIndependentInitParam(int64_t *result1, int64_t *result2, int64_t x, int64_t y, bool random_abort = false, bool generate_before_process = true)
    : result1_(result1),
      result2_(result2),
      x_(x),
      y_(y),
      random_abort_(random_abort),
      generate_before_process_(generate_before_process)
  {}
  ~ObIndependentInitParam() {}
  bool is_valid() const override
  {
    return OB_NOT_NULL(result1_) 
        && OB_NOT_NULL(result2_)
        && *result1_ == *result2_
        && x_ >= 0
        && y_ >= 0;
  }
  TO_STRING_KV(KP_(result1), KP_(result2), K_(x), K_(y), K_(random_abort), K_(generate_before_process));
public:
  int64_t *result1_;
  int64_t *result2_;
  int64_t x_;
  int64_t y_;
  bool random_abort_;
  bool generate_before_process_;
};

class ObIndependentExampleDag : public ObTestIndependentBaseDag
{
public:
  ObIndependentExampleDag() 
    : ObTestIndependentBaseDag(),
      result1_(nullptr),
      result2_(nullptr),
      x_(-1),
      y_(-1),
      random_abort_(false),
      generate_before_process_(true),
      is_abort_(false)
   {}
  ~ObIndependentExampleDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  int inner_init(const ObIndependentInitParam *param);
  int handle_random_abort();
  bool need_random_abort() const { return random_abort_; }
  bool is_abort() const { return ATOMIC_LOAD(&is_abort_); }
  void set_abort() { ATOMIC_SET(&is_abort_, true); }
private:
  int64_t *result1_;
  int64_t *result2_;
  int64_t x_;
  int64_t y_;
  bool random_abort_;
  bool generate_before_process_;
  bool is_abort_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentExampleDag);
};

class ObIndependentPrepareTask : public ObITask
{
public:
  ObIndependentPrepareTask() : ObITask(ObITaskType::TASK_TYPE_UT), is_inited_(false) {}
  ~ObIndependentPrepareTask() {}
  int init(int64_t *result1, int64_t *result2, int64_t x, int64_t y);
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPrepareTask);
private:
  int64_t *result1_;
  int64_t *result2_;
  int64_t x_;
  int64_t y_;
  bool is_inited_;
};

class ObIndependentAddTask : public TestAddTask
{
public:
  ObIndependentAddTask() : TestAddTask() {}
  ~ObIndependentAddTask() {}
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int post_generate_next_task(ObITask *&next_task) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentAddTask);
};

class ObIndependentMulTask : public TestMulTask
{
public:
  ObIndependentMulTask() : TestMulTask() {}
  ~ObIndependentMulTask() {}
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentMulTask);
};

class ObIndependentCheckTask : public ObITask
{
public:
  ObIndependentCheckTask() : ObITask(ObITaskType::TASK_TYPE_UT), is_inited_(false) {}
  ~ObIndependentCheckTask() {}
  int init(const int expected_result, int64_t *result1, int64_t *result2);
  virtual int process() override;
private:
  int64_t expected_result_;
  int64_t *result1_;
  int64_t *result2_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentCheckTask);
};

class ObIndependentPowerDag : public ObIndependentExampleDag
{
public:
  ObIndependentPowerDag() : ObIndependentExampleDag() {}
  ~ObIndependentPowerDag() {}
  virtual int create_first_task() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPowerDag);
};

class ObIndependentPowerPrepareTask : public ObIndependentPrepareTask
{
public:
  ObIndependentPowerPrepareTask() : ObIndependentPrepareTask() {}
  ~ObIndependentPowerPrepareTask() {}
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPowerPrepareTask);
};

class ObIndependentPowerTask : public ObITask
{
public:
  ObIndependentPowerTask() 
    : ObITask(ObITaskType::TASK_TYPE_UT),
      is_inited_(false),
      counter_(nullptr),
      layer_(-1)
    {}
  ~ObIndependentPowerTask() {}
  int init(int64_t *counter, int64_t layer);
  virtual int process() override;
private:
  int generate_next_layer_task();
private:
  bool is_inited_;
  int64_t *counter_;
  int64_t layer_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPowerTask);
};

struct ObIndependentSuspendInitParam : public ObIDagInitParam
{
public:
  ObIndependentSuspendInitParam(int64_t *result, int64_t round, int64_t step)
   : result_(result),
     round_(round),
     step_(step)
  {}
  ~ObIndependentSuspendInitParam() {}
  bool is_valid() const override { return OB_NOT_NULL(result_) && round_ > 0 && step_ > 0; }
  TO_STRING_KV(KP_(result), K_(round), K_(step));
public:
  int64_t *result_;
  int64_t round_;
  int64_t step_;
};

// NOTE: can only run in single thread pool
class ObIndependentSuspendDag : public ObTestIndependentBaseDag
{
public:
  ObIndependentSuspendDag()
    : ObTestIndependentBaseDag(),
      result_(nullptr),
      round_(0),
      step_(0)
    {}
  ~ObIndependentSuspendDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  INHERIT_TO_STRING_KV("ObTestIndependentBaseDag", ObTestIndependentBaseDag, KP_(result), K_(round), K_(step));
protected:
  int64_t *result_;
  int64_t round_;
  int64_t step_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentSuspendDag);
};

class ObIndependentSuspendTask : public ObITask
{
public:
  ObIndependentSuspendTask()
    : ObITask(ObITaskType::TASK_TYPE_UT), counter_(nullptr), round_(0), step_(0), cur_round_(0)
  {}
  ~ObIndependentSuspendTask() {}
  int init(int64_t *counter, int64_t round, int64_t step);
  virtual int process() override;
  INHERIT_TO_STRING_KV("ObITask", ObITask, KP_(counter), K_(round), K_(step), K_(cur_round));
private:
  int64_t *counter_;
  int64_t round_;
  int64_t step_;
  int64_t cur_round_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentSuspendTask);
};

class ObIndependentSuspendDagV2 : public ObIndependentSuspendDag
{
public:
  ObIndependentSuspendDagV2() : ObIndependentSuspendDag() {}
  ~ObIndependentSuspendDagV2() {}
  virtual int create_first_task() override;
};

class ObIndependentSuspendTaskV2 : public ObIndependentSuspendTask
{
public:
  ObIndependentSuspendTaskV2() : ObIndependentSuspendTask() {}
  ~ObIndependentSuspendTaskV2() {}
  int init(int64_t *counter, int64_t round, int64_t step, int64_t cur_round);
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int process() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentSuspendTaskV2);
};

/* ========================= ObIndependentPostGenerateDag ========================= */
struct ObIndependentPostGenerateDagInitParam : public ObIDagInitParam
{
  ObIndependentPostGenerateDagInitParam() = default;
  virtual bool is_valid() const override { return true; }
};

class ObIndependentPostGenerateDag : public ObTestIndependentBaseDag
{
public:
  ObIndependentPostGenerateDag(): ObTestIndependentBaseDag(), counter_(0) {}
  ~ObIndependentPostGenerateDag() {}
  virtual int init_by_param(const ObIDagInitParam *param) override { return OB_SUCCESS; }
  virtual int create_first_task() override;
  int64_t* get_counter_ptr() { return &counter_; }
  int64_t get_counter() const { return counter_; }
  TO_STRING_KV(K_(counter));
public:
  static constexpr int64_t LOOP_TASK_NUM = 1000;
  static constexpr int64_t LOOP_TASK_POST_GENERATE_CNT = 100;
private:
  int64_t counter_;
  DISALLOW_COPY_AND_ASSIGN(ObIndependentPostGenerateDag);
};

class ObHighConcurrencyPrepareTask : public ObITask
{
public:
  ObHighConcurrencyPrepareTask() 
    : ObITask(ObITaskType::TASK_TYPE_UT),
      task_num_(0),
      task_generate_cnt_(0),
      is_inited_(false)
  {}
  ~ObHighConcurrencyPrepareTask() {}
  int init(const int64_t task_num, const int64_t task_generate_cnt);
  virtual int process() override;
private:
  int64_t task_num_;
  int64_t task_generate_cnt_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObHighConcurrencyPrepareTask);
};

class ObHighConcurrencyFastLoopTask : public ObITask
{
public:
  ObHighConcurrencyFastLoopTask() 
    : ObITask(ObITaskType::TASK_TYPE_UT),
      counter_(nullptr),
      idx_(-1),
      seq_(0),
      task_cnt_(0),
      is_inited_(false)
  {}
  ~ObHighConcurrencyFastLoopTask() {}
  int init(int64_t *counter, const int64_t idx, const int64_t seq, const int64_t task_cnt);
  virtual int process() override;
  virtual int post_generate_next_task(ObITask *&next_task) override;
  TO_STRING_KV(K_(idx), K_(seq), K_(task_cnt), K_(is_inited));
public:
  static constexpr int64_t LOOP_TIMEOUT = 1 * 1000; // 10 ms
  static constexpr int64_t SLEEP_TIME = 1 * 1000; // 1ms
private:
  int64_t *counter_;
  int64_t idx_;
  int64_t seq_;
  int64_t task_cnt_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObHighConcurrencyFastLoopTask);
};

class ObHighConcurrencyFinsihTask : public ObITask
{
public:
  ObHighConcurrencyFinsihTask() : ObITask(ObITaskType::TASK_TYPE_UT), expected_result_(0) {}
  ~ObHighConcurrencyFinsihTask() {}
  int init(const int64_t expected_result) { 
    expected_result_ = expected_result; 
    return OB_SUCCESS;
  }
  virtual int process() override;
private:
  int64_t expected_result_;
  DISALLOW_COPY_AND_ASSIGN(ObHighConcurrencyFinsihTask);
};

/* ========================= ObIndependentExampleDag =================================== */
int ObIndependentExampleDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObIndependentInitParam *independent_param = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(independent_param = static_cast<const ObIndependentInitParam *>(param))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg to init ObIndependentExampleDag", K(ret), KP(param), KPC(independent_param));
  } else if (OB_FAIL(inner_init(independent_param))) {
    COMMON_LOG(WARN, "failed to inner init", K(ret), KPC(independent_param));
  }
  return ret;
}

int ObIndependentExampleDag::inner_init(const ObIndependentInitParam *param)
{
  int ret = OB_SUCCESS;
  // is_inited_ is set int ObIDag::basic_init
  if (OB_UNLIKELY(!param->is_valid() )) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg", K(ret), KPC(param));
  } else {
    result1_ = param->result1_;
    result2_ = param->result2_;
    x_ = param->x_;
    y_ = param->y_;
    random_abort_ = param->random_abort_;
    generate_before_process_ = param->generate_before_process_;
  }
  return ret;
}

int ObIndependentExampleDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentPrepareTask *prepare_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentExampleDag is not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, prepare_task, result1_, result2_, x_, y_))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIndependentExampleDag create first task success", KPC(this));
  }
  return ret;
}

int ObIndependentExampleDag::handle_random_abort()
{
  int ret = OB_SUCCESS;
  if (random_abort_) {
    if (is_abort()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is abort", K(ret));
    } else {
      const int64_t random_number = ObRandom::rand(0, 6);
      if (random_number < 3) {
        ret = OB_ERR_UNEXPECTED;
        set_abort();
        COMMON_LOG(WARN, "random abort", K(ret), K(random_number));
      }
    }
  }
  return ret;
}

/* ========================= ObIndependentPrepareTask =================================== */
int ObIndependentPrepareTask::init(
    int64_t *result1,
    int64_t *result2,
    int64_t x,
    int64_t y)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "task is inited", K(ret));
  } else {
    result1_ = result1;
    result2_ = result2;
    x_ = x;
    y_ = y;
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPrepareTask is not inited", K(ret));
  } else {
    ObIndependentAddTask *add_task1 = nullptr;
    ObIndependentAddTask *add_task2 = nullptr;
    ObIndependentMulTask *multi_task1 = nullptr;
    ObIndependentMulTask *multi_task2 = nullptr;
    ObIndependentCheckTask *check_task = nullptr;
    const int64_t expected_result = (*result1_ + (x_ * y_)) * 2;
    if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
    } else if (OB_FAIL(dag_->create_task(this, add_task1, result1_, x_, 0, y_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(add_task1));
    } else if (OB_FAIL(dag_->create_task(this, add_task2, result2_, y_, 0, x_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(add_task2));
    } else if (OB_FAIL(dag_->create_task(add_task1, multi_task1, result1_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(multi_task1));
    } else if (OB_FAIL(dag_->create_task(add_task2, multi_task2, result2_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(multi_task2));
    } else {
      if (OB_FAIL(dag_->alloc_task(check_task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret), KPC(check_task));
      } else if (OB_FAIL(check_task->init(expected_result, result1_, result2_))) {
        COMMON_LOG(WARN, "failed to init task", K(ret), KPC(check_task));
      } else if (OB_FAIL(multi_task1->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(multi_task2->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(dag_->add_task(*check_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(check_task));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(check_task)) {
        check_task->reset_node();
        dag_->remove_task(*check_task);
        check_task = nullptr;
      }
    }
    COMMON_LOG(INFO, "ObIndependentPrepareTask process finished", K(ret));
  }
  return ret;
};

/* ========================= ObIndependentAddTask/ObIndependentMulTask =================================== */
int ObIndependentAddTask::process()
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->handle_random_abort())) {
    COMMON_LOG(WARN, "failed to handle random abort", K(ret));
  } else {
    ret = TestAddTask::process();
  }
  return ret;
}

int ObIndependentAddTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (dag->generate_before_process_) {
    ret = TestAddTask::generate_next_task(next_task);
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObIndependentAddTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (dag->generate_before_process_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(TestAddTask::generate_next_task(next_task))) {
    COMMON_LOG(WARN, "failed to generate next task", K(ret));
  } else if (OB_ISNULL(next_task)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "next task is null", K(ret));
  } else if (OB_FAIL(copy_children_to(*next_task))) {
    COMMON_LOG(WARN, "failed to copy dependency to new task", K(ret));
  } else {
    COMMON_LOG(INFO, "finish post generate next task", K(ret));
  }
  return ret;
}

int ObIndependentMulTask::process()
{
  int ret = OB_SUCCESS;
  ObIndependentExampleDag *dag = static_cast<ObIndependentExampleDag *>(dag_);
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->handle_random_abort())) {
    COMMON_LOG(WARN, "failed to handle random abort", K(ret));
  } else {
    ret = TestMulTask::process();
  }
  return ret;
}

/* ========================= ObIndependentCheckTask =================================== */
int ObIndependentCheckTask::init(
  const int expected_result,
  int64_t *result1,
  int64_t *result2)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "task is inited", K(ret));
  } else if (OB_ISNULL(result1) || OB_ISNULL(result2)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(result1), KP(result2));
  } else {
    expected_result_ = expected_result;
    result1_ = result1;
    result2_ = result2;
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentCheckTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result1_) || OB_ISNULL(result2_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(result1_), KP(result2_));
  } else {
    const int64_t res1 = *result1_;
    const int64_t res2 = *result2_;
    if (res1 != expected_result_ || res2 != expected_result_) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "unexpected result", K(ret), K(expected_result_), K(res1), K(res2));
    } else {
      COMMON_LOG(INFO, "check success", K_(expected_result), K(res1), K(res2));
    }
  }
  return ret;
}

/* ========================= ObIndependentPowerDag ===================================*/
int ObIndependentPowerDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentPowerPrepareTask *prepare_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPowerDag is not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, prepare_task, result1_, result2_, x_, y_))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIndependentPowerDag create first task success", KPC(this));
  }
  return ret;
}

int ObIndependentPowerPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPowerPrepareTask is not inited", K(ret));
  } else {
    ObIndependentPowerTask *power_task1 = nullptr;
    ObIndependentPowerTask *power_task2 = nullptr;
    ObIndependentCheckTask *check_task = nullptr;
    *result1_ = x_;
    *result2_ = x_;
    const int64_t expected_result = x_ * std::pow(2, std::pow(2, y_) - 1);
    if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
    } else if (OB_FAIL(dag_->create_task(this, power_task1, result1_, y_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(power_task1));
    } else if (OB_FAIL(dag_->create_task(this, power_task2, result2_, y_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret), KPC(power_task2));
    } else {
      if (OB_FAIL(dag_->alloc_task(check_task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret), KPC(check_task));
      } else if (OB_FAIL(check_task->init(expected_result, result1_, result2_))) {
        COMMON_LOG(WARN, "failed to init task", K(ret), KPC(check_task));
      } else if (OB_FAIL(power_task1->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(power_task2->add_child(*check_task))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(check_task));
      } else if (OB_FAIL(dag_->add_task(*check_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(check_task));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(check_task)) {
        check_task->reset_node();
        dag_->remove_task(*check_task);
        check_task = nullptr;
      }
    } 
  }
  return ret;
}

int ObIndependentPowerTask::init(
    int64_t *counter, 
    int64_t layer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObIndependentPowerTask is inited", K(ret));
  } else if (OB_ISNULL(counter) || layer <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), KP(counter), K(layer));
  } else {
    counter_ = counter;
    layer_ = layer;
    is_inited_ = true;
  }
  return ret;
}

int ObIndependentPowerTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPowerTask is not inited", K(ret));
  } else {
    int64_t old_val, new_val;
    do {
      old_val = ATOMIC_LOAD(counter_);
      new_val = old_val * 2;
    } while (ATOMIC_VCAS(counter_, old_val, new_val) != old_val);
        
    if (OB_FAIL(generate_next_layer_task())) {
      COMMON_LOG(WARN, "failed to generate next layer task", K(ret), KPC(this));
    } else if (OB_FAIL(generate_next_layer_task())) {
      COMMON_LOG(WARN, "failed to generate next layer task", K(ret), KPC(this));
    }
  }
  return ret;
}

 int ObIndependentPowerTask::generate_next_layer_task()
 {
  int ret = OB_SUCCESS;
  const int64_t next_layer = layer_ - 1;
  if (next_layer > 0) {
    ObIndependentPowerTask *power_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(power_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret), KPC(this));
    } else if (OB_FAIL(power_task->init(counter_, next_layer))) {
      COMMON_LOG(WARN, "failed to init task", K(ret), KPC(power_task));
    } else if (copy_children_to(*power_task)) {
      COMMON_LOG(WARN, "failed to copy children", K(ret), KPC(power_task));
    } else if (OB_FAIL(dag_->add_task(*power_task))) {
      COMMON_LOG(WARN, "failed to add task", K(ret), KPC(power_task));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(power_task)) {
      power_task->reset_node();
      dag_->remove_task(*power_task);
      power_task = nullptr;
    }
  }
  return ret;
 }

/* ========================= ObIndependentPostGenerateDag ========================= */
int ObIndependentPostGenerateDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObHighConcurrencyPrepareTask *prepare_task = nullptr;
  const int64_t task_num = LOOP_TASK_NUM;
  const int64_t post_generate_cnt = LOOP_TASK_POST_GENERATE_CNT;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentPostGenerateDag is not inited", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, prepare_task, task_num, post_generate_cnt))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    COMMON_LOG(INFO, "ObIndependentPostGenerateDag create first task success", KPC(this));
  }
  return ret;
}

int ObHighConcurrencyPrepareTask::init(const int64_t task_num, const int64_t task_generate_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "is inited", K(ret));
  } else if (OB_UNLIKELY(task_num <= 0 || task_generate_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(task_num), K(task_generate_cnt));
  } else {
    task_num_ = task_num;
    task_generate_cnt_ = task_generate_cnt;
    is_inited_ = true;
    COMMON_LOG(INFO, "ObHighConcurrencyPrepareTask init success", KPC(this));
  }
  return ret;
}

int ObHighConcurrencyPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObHighConcurrencyFinsihTask *finish_task = nullptr;
  if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag_ is null", K(ret));
  } else if (OB_FAIL(dag_->create_task(this, finish_task, task_num_ * task_generate_cnt_))) {
    COMMON_LOG(WARN, "failed to create finish task", K(ret));
  } else {
    ObIndependentPostGenerateDag *post_generate_dag = static_cast<ObIndependentPostGenerateDag *>(dag_);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_num_; ++i) {
      ObHighConcurrencyFastLoopTask *loop_task = nullptr;
      if (OB_FAIL(dag_->create_task(this, loop_task, post_generate_dag->get_counter_ptr(), i /*idx*/, 0 /*seq*/, task_generate_cnt_))) {
        COMMON_LOG(WARN, "failed to create loop task", K(ret));
      } else if (OB_FAIL(loop_task->add_child(*finish_task, false /*check_child_task_status*/))) {
        COMMON_LOG(WARN, "failed to add child for loop task", K(ret), KPC(loop_task), KPC(finish_task));
      }
    }
  }
  return ret;
}

int ObHighConcurrencyFastLoopTask::init(
    int64_t *counter, 
    const int64_t idx,
    const int64_t seq,
    const int64_t task_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(counter)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid counter", K(ret));
  } else {
    counter_ = counter;
    idx_ = idx;
    seq_ = seq;
    task_cnt_ = task_cnt;
    is_inited_ = true;
  }
  return ret;
}

int ObHighConcurrencyFastLoopTask::process()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t diff = 0;
  COMMON_LOG(INFO, "start loop task process", K(ret), K(start_time));
  while (true) {
    int64_t current_time = ObTimeUtility::current_time();
    diff = current_time - start_time;
    if (diff > LOOP_TIMEOUT) {
      ATOMIC_AAF(counter_, 1);
      break;
    } else {
      COMMON_LOG(INFO, "loop task process, sleep it", K_(idx), K_(seq), K_(task_cnt), K(current_time), K(start_time), K(diff));
      // ::usleep(SLEEP_TIME);
      for (int64_t i = 0; i < 1000 * 100; i++) { // no sleep
        if (i % 1000 == 0) {
          COMMON_LOG(INFO, "loop task process, print log", K_(idx), K_(seq), K_(task_cnt), K(i), K(current_time), K(start_time), K(diff));
        }
      }
    }
  }
  COMMON_LOG(INFO, "loop task process end", K(ret), K_(idx), K_(seq), K(diff));
  return ret;
}

int ObHighConcurrencyFastLoopTask::post_generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObHighConcurrencyFastLoopTask *ntask = nullptr;
  if (seq_ >= task_cnt_ - 1) {
    ret = OB_ITER_END;
    COMMON_LOG(INFO, "generate task end", K_(idx), K_(seq), K_(task_cnt));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(ntask))) {
    COMMON_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_ISNULL(ntask)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "task is null", K(ret));
  } else if (OB_FAIL(ntask->init(counter_, idx_, seq_ + 1, task_cnt_))) {
    COMMON_LOG(WARN, "failed to init task", K(ret));
  } else {
    next_task = ntask;
    COMMON_LOG(INFO, "next task created", KPC(next_task));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ntask)) {
    ObITask *tmp_task = ntask;
    dag_->free_task(tmp_task);
  }
  return ret;
}

int ObHighConcurrencyFinsihTask::process() 
{
  int ret = OB_SUCCESS;
  ObIndependentPostGenerateDag *post_generate_dag = nullptr;
  constexpr int64_t expected_result = ObIndependentPostGenerateDag::LOOP_TASK_NUM * ObIndependentPostGenerateDag::LOOP_TASK_POST_GENERATE_CNT; 
  if (OB_ISNULL(post_generate_dag = static_cast<ObIndependentPostGenerateDag *>(dag_))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_UNLIKELY(expected_result != post_generate_dag->get_counter())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "unexpected counter", K(ret), K(expected_result), KPC(post_generate_dag));
  } else {
    COMMON_LOG(INFO, "ObHighConcurrencyFinsihTask check finished", K(ret));
  } 
  return ret = OB_SUCCESS;
}

/* ========================= ObIndependentSuspendDag ===================================*/
int ObIndependentSuspendDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObIndependentSuspendInitParam *init_param = nullptr;
  if (OB_ISNULL(param) || OB_ISNULL(init_param = static_cast<const ObIndependentSuspendInitParam *>(param)) 
      || OB_UNLIKELY(!init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arg to init ObIndependentSuspendDag", K(ret), KP(param), KPC(init_param));
  } else {
    result_ = init_param->result_;
    round_ = init_param->round_;
    step_ = init_param->step_;
    COMMON_LOG(INFO, "successfully init ObIndependentSuspendDag", K(ret), KPC(init_param));
  }
  return ret;
}

int ObIndependentSuspendDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentSuspendTask *suspend_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentSuspendDag not init", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, suspend_task, result_, round_, step_))) {
    COMMON_LOG(WARN, "fail to create suspend task", K(ret));
  } else {
    COMMON_LOG(INFO, "success to create suspend task", K(ret), KPC(suspend_task));
  }
  return ret;
}

int ObIndependentSuspendTask::init(
    int64_t *counter,
    int64_t round,
    int64_t step )
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(counter) || round <= 0 || step <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(counter), K(round), K(step));
  } else {
    counter_ = counter;
    round_ = round;
    step_ = step;
    cur_round_ = 0;
  }
  return ret;
}

int ObIndependentSuspendTask::process()
{
  int ret = OB_SUCCESS;
  if (cur_round_ < round_) {
    (void)ATOMIC_AAF(counter_, step_);
    TestMulTask *multi_task = nullptr;
    if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
    } else if (OB_FAIL(dag_->create_task(nullptr /*parent*/, multi_task, counter_))) {
      COMMON_LOG(WARN, "failed to create task", K(ret));
    } else {
      cur_round_++;
      ret = OB_DAG_TASK_IS_SUSPENDED;
      COMMON_LOG(INFO, "suspend task", K(ret));
    } 
  } else {
    ret = OB_SUCCESS;
    COMMON_LOG(INFO, "finish all rounds", K(*counter_));
  }
  return ret;
}

int ObIndependentSuspendDagV2::create_first_task()
{
  int ret = OB_SUCCESS;
  ObIndependentSuspendTaskV2 *suspend_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObIndependentSuspendDagV2 not init", K(ret));
  } else if (OB_FAIL(create_task(nullptr /*parent*/, suspend_task, result_, round_, step_, 1))) {
    COMMON_LOG(WARN, "fail to create suspend task", K(ret));
  } else {
    COMMON_LOG(INFO, "success to create suspend task", K(ret), KPC(suspend_task));
  }
  return ret;
}

int ObIndependentSuspendTaskV2::init(
  int64_t *counter,
  int64_t round,
  int64_t step,
  int64_t cur_round)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIndependentSuspendTask::init(counter, round, step))) {
    COMMON_LOG(WARN, "failed to init ObIndependentSuspendTask", K(ret));
  } else {
    cur_round_ = cur_round;
  }
  return ret;
}

int ObIndependentSuspendTaskV2::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObIndependentSuspendTaskV2 *suspend_task = nullptr;
  if (cur_round_ >= round_) {
    ret = OB_ITER_END;
    COMMON_LOG(INFO, "generate next task end", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
  } else if (OB_FAIL(dag_->create_task(nullptr /*parent*/, suspend_task, counter_, round_, step_, cur_round_ + 1))) {
    COMMON_LOG(WARN, "failed to create task", K(ret));
  } else {
    next_task = suspend_task;
  }
  return ret;
}

int ObIndependentSuspendTaskV2::process()
{
  int ret = OB_SUCCESS;
  int rand_num = ObRandom::rand(0, 10);
  if (rand_num < 5) {
    ret = OB_DAG_TASK_IS_SUSPENDED;
    COMMON_LOG(INFO, "suspend task", K(ret), K_(cur_round));
  } else {
    (void) ATOMIC_AAF(counter_, step_);
    COMMON_LOG(INFO, "finish task", K_(cur_round));
  }
  return ret;
}

/* ========================= DagWorkerThreadPool ===================================*/
class DagWorkerThreadPool : public lib::ThreadPool
{
public:
  DagWorkerThreadPool() {}
  ~DagWorkerThreadPool() {}
  int init(ObDagExecutor *dag_executor, const int64_t thread_cnt);
  void run1() final;
private:
  ObDagExecutor *dag_executor_;
};

int DagWorkerThreadPool::init(ObDagExecutor *dag_executor, const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag_executor) || thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag_executor), K(thread_cnt));
  } else if (OB_FAIL(set_thread_count(thread_cnt))) {
    COMMON_LOG(WARN, "set thread count failed", K(ret), K(thread_cnt));
  } else {
    dag_executor_ = dag_executor;
  }
  return ret;
}

void DagWorkerThreadPool::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t thread_id = get_thread_idx();
  const int64_t start_time = ObTimeUtility::current_time();
  COMMON_LOG(INFO, "DagWorkerThread start", K(thread_id));
  if (OB_ISNULL(dag_executor_) || OB_UNLIKELY(!dag_executor_->is_inited())) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag_executor is null or not inited", K(ret), KPC_(dag_executor));
  } else if (OB_FAIL(dag_executor_->run())) {
    COMMON_LOG(WARN, "run dag failed", K(ret), KPC(dag_executor_));
  }
  const int64_t elapsed_time = (ObTimeUtility::current_time() - start_time) / 1000; 
  COMMON_LOG(INFO, "DagWorkerThread run finished", K(thread_id), K(elapsed_time));
}

class PxWorkerSimulator : public lib::ThreadPool
{
public:
  PxWorkerSimulator() {}
  ~PxWorkerSimulator() {}
  int init(ObIndependentDag *dag, const int64_t thread_cnt);
  void run1() final;
private:
  ObIndependentDag *dag_;
};

int PxWorkerSimulator::init(ObIndependentDag *dag, const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag) || thread_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KPC(dag), K(thread_cnt));
  } else if (OB_FAIL(set_thread_count(thread_cnt))) {
    COMMON_LOG(WARN, "set thread count failed", K(ret), K(thread_cnt));
  } else {
    dag_ = dag;
  }
  return ret;
}

void PxWorkerSimulator::run1()
{
  int ret = OB_SUCCESS;
  const uint64_t thread_id = get_thread_idx();
  const int64_t start_time = ObTimeUtility::current_time();
  COMMON_LOG(INFO, "PxWorkerSimulator start", K(thread_id));
  if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "dag is null", K(ret), KPC_(dag));
  } else if (OB_FAIL(dag_->process())) {
    COMMON_LOG(WARN, "process dag failed", K(ret), KPC(dag_));
  }
  const int64_t elapsed_time = (ObTimeUtility::current_time() - start_time) / 1000; 
  COMMON_LOG(INFO, "PxWorkerSimulator run finished", K(thread_id), K(elapsed_time));
}

// Fail monitoring allocations without exhausting business task memory.
class ObTestMonitorAllocator : public ObIAllocator
{
public:
  ObTestMonitorAllocator() : arena_("TestDagMonitor"), fail_(false), failures_(0), successes_(0), fail_after_(-1) {}
  void *alloc(const int64_t size) override
  {
    if (fail_ || (fail_after_ >= 0 && successes_ >= fail_after_)) {
      ++failures_;
      return nullptr;
    }
    void *ptr = arena_.alloc(size);
    if (nullptr != ptr) { ++successes_; }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override { arena_.free(ptr); }
  ObArenaAllocator arena_;
  bool fail_;
  int64_t failures_;
  int64_t successes_;
  int64_t fail_after_;
};

// Supply an existing node so the real DDL monitor initialization uses the failing allocator.
class ObTestMonitorRegistration
{
public:
  explicit ObTestMonitorRegistration(ObTenantBase &tenant) : tenant_(tenant) {}
  ~ObTestMonitorRegistration()
  {
    tenant_.set(static_cast<storage::ObDDLDagMonitorMgr *>(nullptr));
    ObTenantEnv::set_tenant(&tenant_);
    mgr_.node_map_.destroy(); // The test owns the node and its lifetime.
    mgr_.is_inited_ = false;
  }
  int init(storage::ObDDLDagMonitorNode &node)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(mgr_.node_map_.create(16, "TestDagMonitor"))) {
    } else if (OB_FAIL(mgr_.node_map_.set_refactored(node.get_key(), &node))) {
    } else {
      mgr_.is_inited_ = true;
      tenant_.set(&mgr_);
      ObTenantEnv::set_tenant(&tenant_);
    }
    return ret;
  }
  ObTenantBase &tenant_;
  storage::ObDDLDagMonitorMgr mgr_;
};

struct ObTestRefreshMonitorParam : public storage::ObDDLIndependentDagInitParam
{
  bool is_valid() const override { return true; }
};

class ObTestRefreshMonitorDag : public storage::ObDDLIndependentDag
{
public:
  ObTestRefreshMonitorDag() : business_ret_(OB_SUCCESS), param_inited_(false) {}
  bool is_scan_finished() override { return true; }
  int init_by_param(const ObIDagInitParam *) override { param_inited_ = true; return business_ret_; }
  int create_first_task() override { return OB_SUCCESS; }
  int business_ret_;
  bool param_inited_;
};

class ObTestMonitoredTask : public ObITaskWithMonitor
{
public:
  ObTestMonitoredTask() : ObITaskWithMonitor(ObITask::TASK_TYPE_UT), result_(nullptr), ret_(OB_SUCCESS),
      destroyed_after_finish_(nullptr), node_(nullptr) {}
  ~ObTestMonitoredTask() override
  {
    if (nullptr != destroyed_after_finish_) {
      *destroyed_after_finish_ = node_->is_finished();
    }
  }
  int process() override { ++*result_; return ret_; }
  int64_t *result_;
  int ret_;
  bool *destroyed_after_finish_;
  storage::ObDDLDagMonitorNode *node_;
};

TEST_F(TestIndependentDag, monitor_registration_replaces_finished_node)
{
  omt::ObMultiTenant omt;
  auto *saved_omt = GCTX.omt_;
  GCTX.omt_ = &omt;
  DEFER(GCTX.omt_ = saved_omt);
  for (int drain_history = 0; drain_history < 2; ++drain_history) {
    storage::ObDDLDagMonitorMgr mgr;
    ASSERT_EQ(OB_SUCCESS, mgr.allocator_.init(&mgr, OB_MALLOC_NORMAL_BLOCK_SIZE,
        ObMemAttr(MTL_ID(), "TestDagMonitor"), 1024 * 1024));
    ASSERT_EQ(OB_SUCCESS, mgr.node_map_.create(16, "TestDagMonitor"));
    mgr.is_inited_ = true;
    ObDagId id;
    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
    int dag_identity = 0;
    storage::ObDDLDagMonitorNode *history_node = nullptr;
    ASSERT_EQ(OB_SUCCESS, mgr.register_node(&dag_identity, id, history_node));
    storage::ObDDLDagMonitorInfo *history_info = nullptr;
    ASSERT_EQ(OB_SUCCESS, history_node->alloc_monitor_info(nullptr, history_info));
    history_info->mark_finished();
    history_node->mark_finished();
    if (drain_history) {
      ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
    }
    // A reader keeps the old object observable after it leaves the map.
    ObSEArray<storage::ObDDLDagMonitorNode *, 1> readers;
    DEFER(for (int64_t i = 0; i < readers.count(); ++i) { readers.at(i)->dec_ref(); });
    ASSERT_EQ(OB_SUCCESS, mgr.get_all_nodes(readers));
    storage::ObDDLDagMonitorNode *node = nullptr;
    ASSERT_EQ(OB_SUCCESS, mgr.register_node(&dag_identity, id, node));
    ASSERT_NE(history_node, node);
    ASSERT_TRUE(history_node->is_finished());
    ASSERT_EQ(drain_history ? 0 : 1, history_node->info_list_.get_size());
    ASSERT_FALSE(node->is_finished());
    ASSERT_TRUE(node->is_info_list_empty());

    // Cleanup between register_node returning and root-info insertion must preserve it.
    ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_EXPIRED_NODE));
    ASSERT_EQ(1, mgr.get_node_count());
    ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
    ASSERT_EQ(1, mgr.get_node_count());
    storage::ObDDLDagMonitorNode *same_node = nullptr;
    ASSERT_EQ(OB_SUCCESS, mgr.register_node(&dag_identity, id, same_node));
    ASSERT_EQ(node, same_node);

    storage::ObDDLDagMonitorInfo *root_info = nullptr;
    ASSERT_EQ(OB_SUCCESS, node->alloc_monitor_info(nullptr, root_info));
    root_info->mark_finished();
    ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
    ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_EXPIRED_NODE));
    ASSERT_EQ(1, mgr.get_node_count()); // Still unfinished even though infos are empty.
    node->mark_finished();
    ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_EXPIRED_NODE));
    ASSERT_EQ(0, mgr.get_node_count());
    readers.at(0)->dec_ref();
    readers.reset();
    ASSERT_EQ(0, mgr.allocator_.used());
  }
}

TEST_F(TestIndependentDag, table_load_monitor_initialization)
{
  for (int fail = 0; fail < 2; ++fail) {
    ObTestMonitorAllocator allocator;
    allocator.fail_ = fail;
    storage::ObDDLDagMonitorNode node(&allocator);
    ObTestMonitorRegistration registration(tenant_base_);
    observer::ObTableLoadStoreCtx store_ctx(nullptr);
    observer::ObTableLoadDag dag;
    ObDagId id;
    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
    ObTraceIdGuard trace_id_guard(id);
    ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
    ASSERT_EQ(OB_SUCCESS, registration.init(node));
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
    // TableLoad uses its own init param and must preserve non-refresh monitoring errors.
    ASSERT_EQ(fail ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS, dag.init(&store_ctx));
    ASSERT_EQ(&node, dag.get_monitor_node());
    ASSERT_FALSE(node.is_async_index_refresh_);
    ASSERT_EQ(bool(fail), nullptr == dag.root_monitor_info_);
    if (fail) {
      ASSERT_GT(allocator.failures_, 0);
      ASSERT_EQ(0, allocator.successes_);
    } else {
      ASSERT_EQ(0, allocator.failures_);
      ASSERT_GT(allocator.successes_, 0);
    }
  }
}

TEST_F(TestIndependentDag, ddl_build_and_refresh_node_allocation_failure)
{
  // No tenant unit is registered: cleanup cannot raise the test allocator's limit.
  omt::ObMultiTenant omt;
  auto *saved_omt = GCTX.omt_;
  GCTX.omt_ = &omt;
  DEFER(GCTX.omt_ = saved_omt);
  for (int async = 0; async < 2; ++async) {
    ObTestMonitorAllocator allocator;
    storage::ObDDLDagMonitorNode history_node(&allocator);
    ObTestMonitorRegistration registration(tenant_base_);
    ObDagId history_id;
    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(history_id));
    ASSERT_EQ(OB_SUCCESS, history_node.init(storage::ObDDLDagMonitorNode::Key(&history_node, history_id)));
    storage::ObDDLDagMonitorInfo *history_info = nullptr;
    ASSERT_EQ(OB_SUCCESS, history_node.alloc_monitor_info(nullptr, history_info));
    history_info->mark_finished();
    ASSERT_EQ(1, history_node.info_list_.get_size());
    ASSERT_EQ(OB_SUCCESS, registration.init(history_node));
    ASSERT_EQ(OB_SUCCESS, registration.mgr_.allocator_.init(&registration.mgr_,
        OB_MALLOC_NORMAL_BLOCK_SIZE, ObMemAttr(MTL_ID(), "TestDagMonitor"), 1));
    ObTestRefreshMonitorDag dag;
    ObTestRefreshMonitorParam param;
    param.ddl_task_param_.is_async_index_refresh_ = async;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
    ASSERT_EQ(async ? OB_SUCCESS : OB_ALLOCATE_MEMORY_FAILED, dag.init(&param));
    ASSERT_EQ(nullptr, dag.get_monitor_node());
    ASSERT_EQ(nullptr, dag.root_monitor_info_);
    ASSERT_EQ(bool(async), dag.param_inited_);
    // A different key misses the map; the real failed allocation cleans history before retrying.
    ASSERT_EQ(0, history_node.info_list_.get_size());
    ASSERT_EQ(1, registration.mgr_.get_node_count());
    ASSERT_EQ(0, registration.mgr_.allocator_.used());
    if (async) {
      int64_t result = 0;
      ObTestMonitoredTask *task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
      task->result_ = &result;
      ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
      ASSERT_EQ(nullptr, task->get_monitor_info());
      ASSERT_EQ(OB_SUCCESS, dag.process());
      ASSERT_EQ(1, result);
    }
  }
}

TEST_F(TestIndependentDag, ddl_build_and_refresh_root_monitor_allocation_failure)
{
  for (int async = 0; async < 2; ++async) {
    ObTestMonitorAllocator allocator;
    allocator.fail_ = true;
    storage::ObDDLDagMonitorNode node(&allocator);
    ObTestMonitorRegistration registration(tenant_base_);
    {
      ObTestRefreshMonitorDag dag;
      ObDagId id;
      ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
      ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
      ASSERT_EQ(OB_SUCCESS, registration.init(node));
      ObTestRefreshMonitorParam param;
      param.ddl_task_param_.is_async_index_refresh_ = async;
      ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
      ASSERT_EQ(async ? OB_SUCCESS : OB_ALLOCATE_MEMORY_FAILED, dag.init(&param, &id));
      ASSERT_EQ(1, allocator.failures_);
      ASSERT_EQ(bool(async), dag.param_inited_);
      ASSERT_EQ(nullptr, dag.root_monitor_info_);
      if (async) {
        int64_t result = 0;
        ObTestMonitoredTask *task = nullptr;
        ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
        task->result_ = &result;
        ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
        ASSERT_EQ(OB_SUCCESS, dag.process());
        ASSERT_EQ(1, result);
      }
    }
    ASSERT_TRUE(node.is_finished());
  }
}

TEST_F(TestIndependentDag, ddl_build_and_refresh_task_monitor_allocation_failure)
{
  for (int async = 0; async < 2; ++async) {
    for (int batch = 0; batch < 2; ++batch) {
      ObTestMonitorAllocator allocator;
      storage::ObDDLDagMonitorNode node(&allocator);
      ObTestMonitorRegistration registration(tenant_base_);
      {
        ObTestRefreshMonitorDag dag;
        ObDagId id;
        ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
        ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
        ASSERT_EQ(OB_SUCCESS, registration.init(node));
        ObTestRefreshMonitorParam param;
        param.ddl_task_param_.is_async_index_refresh_ = async;
        ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
        ASSERT_EQ(OB_SUCCESS, dag.init(&param, &id));
        ASSERT_NE(nullptr, dag.root_monitor_info_);
        int64_t result = 0;
        ObTestMonitoredTask *first = nullptr;
        ASSERT_EQ(OB_SUCCESS, dag.alloc_task(first));
        first->result_ = &result;
        ASSERT_EQ(OB_SUCCESS, dag.add_task(*first));
        ASSERT_NE(nullptr, first->get_monitor_info());
        allocator.fail_ = true;
        ObTestMonitoredTask *task = nullptr;
        ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
        task->result_ = &result;
        ObSEArray<ObITask *, 1> tasks;
        ASSERT_EQ(OB_SUCCESS, tasks.push_back(task));
        const int ret = batch ? dag.batch_add_task(tasks) : dag.add_task(*task);
        ASSERT_EQ(async ? OB_SUCCESS : OB_ALLOCATE_MEMORY_FAILED, ret);
        ASSERT_EQ(1, allocator.failures_);
        ASSERT_EQ(nullptr, task->get_monitor_info());
        if (async) {
          ASSERT_EQ(OB_SUCCESS, dag.process());
          ASSERT_EQ(2, result);
        } else {
          ASSERT_EQ(0, result);
        }
      }
      ASSERT_TRUE(node.is_finished());
    }
  }
}

TEST_F(TestIndependentDag, ddl_build_and_refresh_batch_monitor_partial_allocation_failure)
{
  for (int async = 0; async < 2; ++async) {
    ObTestMonitorAllocator allocator;
    storage::ObDDLDagMonitorNode node(&allocator);
    ObTestMonitorRegistration registration(tenant_base_);
    storage::ObDDLDagMonitorInfo *infos[2] = {nullptr, nullptr};
    storage::ObDDLDagMonitorInfo *root_info = nullptr;
    int64_t results[4] = {0, 0, 0, 0};
    {
      ObTestRefreshMonitorDag dag;
      ObDagId id;
      ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
      ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
      ASSERT_EQ(OB_SUCCESS, registration.init(node));
      ObTestRefreshMonitorParam param;
      param.ddl_task_param_.is_async_index_refresh_ = async;
      ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
      ASSERT_EQ(OB_SUCCESS, dag.init(&param, &id));
      root_info = dag.root_monitor_info_;
      ASSERT_NE(nullptr, root_info);
      allocator.fail_after_ = allocator.successes_ + 2;
      ObSEArray<ObITask *, 4> tasks;
      ObTestMonitoredTask *task_ptrs[4] = {nullptr, nullptr, nullptr, nullptr};
      for (int i = 0; i < 4; ++i) {
        ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task_ptrs[i]));
        task_ptrs[i]->result_ = &results[i];
        ASSERT_EQ(OB_SUCCESS, tasks.push_back(task_ptrs[i]));
      }
      ASSERT_EQ(async ? OB_SUCCESS : OB_ALLOCATE_MEMORY_FAILED, dag.batch_add_task(tasks));
      ASSERT_EQ(async ? 2 : 1, allocator.failures_);
      for (int i = 0; i < 2; ++i) {
        infos[i] = task_ptrs[i]->get_monitor_info();
        ASSERT_NE(nullptr, infos[i]);
      }
      ASSERT_EQ(nullptr, task_ptrs[2]->get_monitor_info());
      ASSERT_EQ(nullptr, task_ptrs[3]->get_monitor_info());
      if (async) { ASSERT_EQ(OB_SUCCESS, dag.process()); }
      for (int i = 0; i < 4; ++i) { ASSERT_EQ(async ? 1 : 0, results[i]); }
    }
    ASSERT_TRUE(node.is_finished());
    ASSERT_GT(root_info->get_task_finish_timestamp(), 0);
    for (int i = 0; i < 2; ++i) { ASSERT_GT(infos[i]->get_task_finish_timestamp(), 0); }
  }
}

TEST_F(TestIndependentDag, ddl_monitor_preserves_other_errors)
{
  ObTestRefreshMonitorParam param;
  param.ddl_task_param_.is_async_index_refresh_ = true;
  // An unavailable monitor manager still returns the original non-allocation error.
  ObTestRefreshMonitorDag unregistered_dag;
  ASSERT_EQ(OB_ERR_UNEXPECTED, unregistered_dag.init_monitor_node(&param));
  for (int phase = 0; phase < 2; ++phase) {
    ObTestMonitorAllocator allocator;
    allocator.fail_ = true;
    storage::ObDDLDagMonitorNode node(&allocator);
    ObTestMonitorRegistration registration(tenant_base_);
    ObTestRefreshMonitorDag dag;
    ObDagId id;
    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
    ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
    ASSERT_EQ(OB_SUCCESS, registration.init(node));
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
    if (0 == phase) {
      dag.business_ret_ = OB_ALLOCATE_MEMORY_FAILED;
      ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, dag.init(&param, &id));
    } else {
      ASSERT_EQ(OB_SUCCESS, dag.init(&param, &id));
      int64_t result = 0;
      ObTestMonitoredTask *task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
      task->result_ = &result;
      task->ret_ = OB_ALLOCATE_MEMORY_FAILED;
      ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
      ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, dag.process());
      ASSERT_EQ(1, result);
      node.is_inited_ = false;
      storage::ObDDLDagMonitorInfo *info = nullptr;
      ASSERT_EQ(OB_NOT_INIT, node.alloc_monitor_info(nullptr, info));
      node.is_inited_ = true;
    }
  }
}

static bool read_monitor_log_counter(const std::string &text, const char *key, int64_t &value)
{
  const size_t pos = text.find(key);
  return pos != std::string::npos
      && 1 == sscanf(text.c_str() + pos + strlen(key), "%" SCNd64, &value);
}

TEST_F(TestIndependentDag, monitor_node_log_snapshot_format)
{
  storage::ObDDLDagMonitorNode node(&allocator_);
  ObDagId id;
  ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
  ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&node, id)));
  char buf[2048] = {};
  ASSERT_GT(node.to_string(buf, sizeof(buf)), 0);
  int64_t value = -1;
  ASSERT_TRUE(read_monitor_log_counter(buf, "ref_cnt:", value));
  ASSERT_EQ(0, value);
  ASSERT_TRUE(read_monitor_log_counter(buf, "finish_timestamp:", value));
  ASSERT_EQ(0, value);
  ASSERT_TRUE(read_monitor_log_counter(buf, "info_list_.get_size():", value));
  ASSERT_EQ(0, value);
  storage::ObDDLDagMonitorInfo *info = nullptr;
  ASSERT_EQ(OB_SUCCESS, node.alloc_monitor_info(nullptr, info));
  node.mark_finished();
  ASSERT_GT(node.to_string(buf, sizeof(buf)), 0);
  ASSERT_TRUE(read_monitor_log_counter(buf, "finish_timestamp:", value));
  ASSERT_EQ(node.get_finish_timestamp(), value);
  ASSERT_TRUE(read_monitor_log_counter(buf, "info_list_.get_size():", value));
  ASSERT_EQ(1, value);
  char small_buf[16];
  MEMSET(small_buf, 'x', sizeof(small_buf));
  node.to_string(small_buf, 8);
  for (int i = 8; i < sizeof(small_buf); ++i) {
    ASSERT_EQ('x', small_buf[i]);
  }
}

static int64_t print_monitor_node_log(
    storage::ObDDLDagMonitorNode *node, std::promise<void> *entered)
{
  char buf[2048] = {};
  entered->set_value();
  return node->to_string(buf, sizeof(buf));
}

TEST_F(TestIndependentDag, monitor_node_log_waits_for_info_list_lock)
{
  storage::ObDDLDagMonitorNode node(&allocator_);
  ObDagId id;
  ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
  ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&node, id)));
  std::promise<void> entered;
  std::future<void> started = entered.get_future();
  std::future<int64_t> printed;
  std::future_status status;
  {
    SpinWLockGuard guard(node.lock_);
    printed = std::async(std::launch::async, print_monitor_node_log, &node, &entered);
    started.wait();
    // Logging must wait for a concurrent list mutation, not read its size without the lock.
    status = printed.wait_for(std::chrono::milliseconds(100));
  }
  const int64_t printed_size = printed.get();
  ASSERT_EQ(std::future_status::timeout, status);
  ASSERT_GT(printed_size, 0);
}

struct ObTestMonitorLogContext
{
  explicit ObTestMonitorLogContext(storage::ObDDLDagMonitorNode *node)
    : node_(node), start_(false), ready_(0), failures_(0) {}
  storage::ObDDLDagMonitorNode *node_;
  std::atomic<bool> start_;
  std::atomic<int> ready_;
  std::atomic<int> failures_;
};

static void wait_for_monitor_log_start(ObTestMonitorLogContext &ctx)
{
  ctx.ready_.fetch_add(1);
  while (!ctx.start_.load()) {
    std::this_thread::yield();
  }
}

static void update_monitor_log_references(ObTestMonitorLogContext *ctx)
{
  wait_for_monitor_log_start(*ctx);
  ctx->node_->mark_finished();
  for (int i = 0; i < 4000; ++i) {
    ctx->node_->inc_ref();
    ctx->node_->dec_ref();
  }
}

static void update_monitor_log_infos(ObTestMonitorLogContext *ctx)
{
  wait_for_monitor_log_start(*ctx);
  for (int i = 0; i < 2000; ++i) {
    storage::ObDDLDagMonitorInfo *info = nullptr;
    if (OB_SUCCESS != ctx->node_->alloc_monitor_info(nullptr, info)
        || nullptr == info || OB_SUCCESS != ctx->node_->clean_infos(false)) {
      ctx->failures_.fetch_add(1);
      break;
    }
  }
}

static void read_monitor_log_snapshots(ObTestMonitorLogContext *ctx)
{
  wait_for_monitor_log_start(*ctx);
  for (int i = 0; i < 4000; ++i) {
    char text[2048] = {};
    int64_t ref_cnt = -1;
    int64_t finish_timestamp = -1;
    int64_t info_count = -1;
    if (ctx->node_->to_string(text, sizeof(text)) <= 0
        || !read_monitor_log_counter(text, "ref_cnt:", ref_cnt)
        || !read_monitor_log_counter(text, "finish_timestamp:", finish_timestamp)
        || !read_monitor_log_counter(text, "info_list_.get_size():", info_count)
        || ref_cnt < 1 || ref_cnt > 2 || finish_timestamp < 0
        || info_count < 0 || info_count > 1) {
      ctx->failures_.fetch_add(1);
      break;
    }
  }
}

TEST_F(TestIndependentDag, monitor_node_log_concurrent_updates)
{
  void *buf = allocator_.alloc(sizeof(storage::ObDDLDagMonitorNode));
  ASSERT_NE(nullptr, buf);
  auto *node = new (buf) storage::ObDDLDagMonitorNode(&allocator_);
  node->inc_ref(); // Keep a base reference until every logging/writer thread has joined.
  DEFER(node->dec_ref());
  ObDagId id;
  ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
  ASSERT_EQ(OB_SUCCESS, node->init(storage::ObDDLDagMonitorNode::Key(node, id)));
  ObTestMonitorLogContext ctx(node);
  std::thread references(update_monitor_log_references, &ctx);
  std::thread infos(update_monitor_log_infos, &ctx);
  std::thread reader(read_monitor_log_snapshots, &ctx);
  while (ctx.ready_.load() < 3) {
    std::this_thread::yield();
  }
  ctx.start_.store(true);
  references.join();
  infos.join();
  reader.join();
  ASSERT_EQ(0, ctx.failures_.load());
  ASSERT_EQ(1, ATOMIC_LOAD(&node->ref_cnt_));
  ASSERT_TRUE(node->is_finished());
  ASSERT_EQ(0, node->info_list_.get_size());
}

TEST_F(TestIndependentDag, monitor_node_finishes_only_after_task_destruction)
{
  ObTestMonitorAllocator allocator;
  storage::ObDDLDagMonitorNode node(&allocator);
  bool destroyed_after_finish = true;
  {
    ObIndependentExampleDag dag;
    ObDagId id;
    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
    ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
    dag.monitor_node_ = &node;
    ObTestMonitoredTask *task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
    task->destroyed_after_finish_ = &destroyed_after_finish;
    task->node_ = &node;
    ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
  }
  ASSERT_FALSE(destroyed_after_finish);
  ASSERT_TRUE(node.is_finished());
}

TEST_F(TestIndependentDag, reset_finishes_monitor_after_both_task_lists_and_is_repeatable)
{
  ObTestMonitorAllocator allocator;
  storage::ObDDLDagMonitorNode node(&allocator);
  bool destroyed_after_finish[2] = {true, true};
  ObIndependentExampleDag dag;
  ObDagId id;
  ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
  ASSERT_EQ(OB_SUCCESS, node.init(storage::ObDDLDagMonitorNode::Key(&dag, id)));
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator_));
  dag.monitor_node_ = &node;
  for (int i = 0; i < 2; ++i) {
    ObTestMonitoredTask *task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
    task->destroyed_after_finish_ = &destroyed_after_finish[i];
    task->node_ = &node;
    ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
    if (0 == i) {
      // Move only the first task to the ready list; leave the second waiting.
      ASSERT_EQ(OB_SUCCESS, dag.update_ready_task_list());
    }
  }
  ASSERT_EQ(1, dag.task_list_.get_size());
  ASSERT_EQ(1, dag.waiting_task_list_.get_size());
  ASSERT_FALSE(node.is_finished());
  dag.reset();
  ASSERT_FALSE(destroyed_after_finish[0]);
  ASSERT_FALSE(destroyed_after_finish[1]);
  ASSERT_TRUE(dag.task_list_.is_empty());
  ASSERT_TRUE(dag.waiting_task_list_.is_empty());
  ASSERT_TRUE(node.is_finished());
  ASSERT_EQ(nullptr, dag.get_monitor_node());
  const int64_t finished_at = node.get_finish_timestamp();
  // Repeated reset and the subsequent destructor must not finish the node again.
  dag.reset();
  ASSERT_EQ(nullptr, dag.get_monitor_node());
  ASSERT_EQ(finished_at, node.get_finish_timestamp());
}

TEST_F(TestIndependentDag, monitor_cleanup_preserves_ttl_soft_limit_and_reader_refs)
{
  omt::ObMultiTenant omt;
  auto *saved_omt = GCTX.omt_;
  GCTX.omt_ = &omt;
  DEFER(GCTX.omt_ = saved_omt);
  storage::ObDDLDagMonitorMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.node_map_.create(16, "TestDagMonitor"));
  ASSERT_EQ(OB_SUCCESS, mgr.allocator_.init(&mgr, OB_MALLOC_NORMAL_BLOCK_SIZE,
      ObMemAttr(MTL_ID(), "TestDagMonitor"), 16 * 1024 * 1024));
  mgr.is_inited_ = true;
  mgr.set_max_node_count(1);
  int identities[3] = {0, 1, 2};
  ObDagId ids[3];
  storage::ObDDLDagMonitorNode *nodes[3] = {nullptr, nullptr, nullptr};
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS, mgr.register_node(&identities[0], ids[0], nodes[0]));
  // Keep monitor info to exercise TTL retention; finished empty nodes are reclaimed eagerly.
  storage::ObDDLDagMonitorInfo *retained_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, nodes[0]->alloc_monitor_info(nullptr, retained_info));
  nodes[0]->inc_ref(); // A virtual-table reader outlives removal from the map.
  DEFER(nodes[0]->dec_ref());
  nodes[0]->mark_finished();
  ASSERT_FALSE(nodes[0]->is_expired(ObTimeUtility::current_time(), mgr.ttl_us_));
  ASSERT_EQ(OB_SUCCESS, mgr.register_node(&identities[1], ids[1], nodes[1]));
  ASSERT_EQ(2, mgr.get_node_count()); // max count remains a cleanup threshold.
  ASSERT_EQ(2, nodes[0]->ref_cnt_);
  ASSERT_EQ(ids[0], nodes[0]->get_trace_id());
  ASSERT_EQ(OB_SUCCESS, mgr.register_node(&identities[2], ids[2], nodes[2]));
  ASSERT_NE(nullptr, nodes[2]);
  ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
  ASSERT_EQ(3, mgr.get_node_count()); // Finished, unexpired nodes stay in the map.
  ASSERT_FALSE(nodes[1]->is_finished());
  ATOMIC_STORE(&nodes[0]->finish_timestamp_, ObTimeUtility::current_time() - mgr.ttl_us_ - 1);
  ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_EXPIRED_NODE));
  ASSERT_EQ(2, mgr.get_node_count());
  ASSERT_EQ(1, nodes[0]->ref_cnt_); // Existing readers still own the expired node.
  ASSERT_EQ(ids[0], nodes[0]->get_trace_id());
}

TEST_F(TestIndependentDag, monitor_cleanup_keeps_refresh_commit_visible_after_dag_finishes)
{
  omt::ObMultiTenant omt;
  auto *saved_omt = GCTX.omt_;
  GCTX.omt_ = &omt;
  DEFER(GCTX.omt_ = saved_omt);
  storage::ObDDLDagMonitorMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.node_map_.create(16, "TestDagMonitor"));
  ASSERT_EQ(OB_SUCCESS, mgr.allocator_.init(&mgr, OB_MALLOC_NORMAL_BLOCK_SIZE,
      ObMemAttr(MTL_ID(), "TestDagMonitor"), 16 * 1024 * 1024));
  mgr.is_inited_ = true;
  int identity = 0;
  ObDagId id;
  ASSERT_EQ(OB_SUCCESS, ObSysTaskStatMgr::get_instance().generate_task_id(id));
  storage::ObDDLDagMonitorNode *node = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr.register_node(&identity, id, node));
  auto *refresh = OB_NEW(storage::ObIVFRefreshMonitor, ObMemAttr(MTL_ID(), "IVFRefreshPerf"));
  ASSERT_NE(nullptr, refresh);
  DEFER(refresh->dec_ref());
  refresh->init(node, 1, 1, 1001, 1, 2);
  DEFER(refresh->finish(OB_CANCELED)); // Drain monitor ownership even if an assertion fails.
  refresh->begin_stage(share::ObIVFRefreshProgress::COMMIT);
  node->mark_finished(); // SQL DAG ended; refresh transaction has not ended yet.
  ASSERT_EQ(2, node->ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
  ASSERT_EQ(1, mgr.get_node_count());
  storage::ObDDLDagMonitorNode *visible_node = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr.node_map_.get_refactored(node->get_key(), visible_node));
  ASSERT_EQ(node, visible_node);
  ObSEArray<storage::ObDDLDagMonitorEntry *, 16> entries;
  ASSERT_EQ(OB_SUCCESS, node->get_all_infos(allocator_, entries));
  ASSERT_GT(entries.count(), 0);
  bool commit_visible = false;
  for (auto *entry : entries) {
    const ObString &message = entry->get_message();
    std::string text(message.ptr(), message.length());
    commit_visible |= std::string::npos != text.find("\"stage\":\"COMMIT\"")
        && std::string::npos != text.find("\"state\":\"RUNNING\"");
    entry->~ObDDLDagMonitorEntry();
    allocator_.free(entry);
  }
  ASSERT_TRUE(commit_visible);
  refresh->finish(OB_SUCCESS);
  ASSERT_EQ(1, node->ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, mgr.clean_nodes(storage::ObDDLDagMonitorMgr::CLEAN_FINISHED_INFO));
  ASSERT_EQ(1, mgr.get_node_count());
  ASSERT_EQ(0, node->info_list_.get_size());
}

TEST_F(TestIndependentDag, test_independent_dag)
{
  ObDagExecutor dag_executor;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, dag_executor.init<ObIndependentExampleDag>(&allocator_, &param, nullptr /*dag_id*/));
  ASSERT_NE(nullptr, dag_executor.dag_);
  ASSERT_EQ(true, dag_executor.dag_->is_independent_);

  DagWorkerThreadPool dag_worker_thread_pool;
  dag_worker_thread_pool.init(&dag_executor, 10);
  dag_worker_thread_pool.start();
  dag_worker_thread_pool.wait();
  ASSERT_EQ(400, res1);
  ASSERT_EQ(400, res2);
}

TEST_F(TestIndependentDag, test_independent_dag_stress)
{
  for (int64_t i = 0; i < STRESS_ROUND_CNT; i++) {
    for (int64_t thread_cnt = 1; thread_cnt < STRESS_THREAD_CNT; thread_cnt++) {
      ObDagExecutor dag_executor;
      int64_t res1 = 0;
      int64_t res2 = 0;
      const int64_t x = 1 + ObRandom::rand(0, 20);
      const int64_t y = 1 + ObRandom::rand(0, 20);
      COMMON_LOG(INFO, "[TestIndependentDag] test_independent_dag_stress", K(i), K(thread_cnt), K(x), K(y));
      ObIndependentInitParam param(&res1, &res2, x, y);
      ASSERT_EQ(OB_SUCCESS, dag_executor.init<ObIndependentExampleDag>(&allocator_, &param, nullptr /*dag_id*/));
      ASSERT_NE(nullptr, dag_executor.dag_);
      ASSERT_EQ(true, dag_executor.dag_->is_independent_);

      DagWorkerThreadPool dag_worker_thread_pool;
      dag_worker_thread_pool.init(&dag_executor, thread_cnt);
      dag_worker_thread_pool.start();
      dag_worker_thread_pool.wait();
      ASSERT_EQ(x * y * 2, res1);
      ASSERT_EQ(x * y * 2, res2);
    }
  }
}

TEST_F(TestIndependentDag, test_pure_independent_dag)
{
  ObIndependentExampleDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_,false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 10);
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(400, res1);
  ASSERT_EQ(400, res2);

  dag->~ObIndependentExampleDag();
  allocator_.free(dag);
  dag = nullptr;
}

TEST_F(TestIndependentDag, test_pure_independent_dag_stress)
{
  for (int64_t i = 0; i < STRESS_ROUND_CNT; i++) {
    for (int64_t thread_cnt = 1; thread_cnt < STRESS_THREAD_CNT; thread_cnt++) {
      ObIndependentExampleDag *dag = nullptr;
      int64_t res1 = 0;
      int64_t res2 = 0;
      const int64_t x = 1 + ObRandom::rand(0, 20);
      const int64_t y = 1 + ObRandom::rand(0, 20);
      bool random_abort = i % 2 == 0;
      bool generate_before_process = i % 3 == 0;
      COMMON_LOG(INFO, "[TestIndependentDag] test_pure_independent_dag_stress", K(i), K(thread_cnt), K(x), K(y), K(random_abort), K(generate_before_process));
      ObIndependentInitParam param(&res1, &res2, x, y, random_abort, generate_before_process);
      ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
      ASSERT_NE(nullptr, dag);
      
      ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
      ASSERT_EQ(true, dag->is_independent_);

      PxWorkerSimulator px_worker_simulator;
      px_worker_simulator.init(dag, 10);
      px_worker_simulator.start();
      px_worker_simulator.wait();

      if (dag->is_abort()) {
        ASSERT_NE(ObIDag::DAG_STATUS_FINISH, dag->get_dag_status());
        ASSERT_EQ(false, res1 == x * y * 2 && res2 == x * y * 2);
      } else {
        ASSERT_EQ(x * y * 2, res1);
        ASSERT_EQ(x * y * 2, res2);
      }

      dag->~ObIndependentExampleDag();
      allocator_.free(dag);
      dag = nullptr;
    }
  }
}

TEST_F(TestIndependentDag, test_pure_power_dag)
{
  ObIndependentPowerDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t base = 10;
  const int64_t layer = 4;
  ObIndependentInitParam param(&res1, &res2, base, layer);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 10);
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(327680, res1);
  ASSERT_EQ(327680, res2);
}

TEST_F(TestIndependentDag, test_pure_power_dag_stress)
{
  for (int64_t i = 0; i < STRESS_ROUND_CNT; i++) {
    for (int64_t thread_cnt = 1; thread_cnt < STRESS_THREAD_CNT; thread_cnt++) {
      ObIndependentPowerDag *dag = nullptr;
      int64_t res1 = 0;
      int64_t res2 = 0;
      const int64_t base = 1 + ObRandom::rand(0, 20);
      const int64_t layer = 1 + ObRandom::rand(0, 3);
      COMMON_LOG(INFO, "[TestDagScheduler] test_pure_power_dag_stress", K(i), K(base), K(layer));
      ObIndependentInitParam param(&res1, &res2, base, layer);
      ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
      ASSERT_NE(nullptr, dag);
      
      ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
      ASSERT_EQ(true, dag->is_independent_);

      PxWorkerSimulator px_worker_simulator;
      px_worker_simulator.init(dag, 10);
      px_worker_simulator.start();
      px_worker_simulator.wait();
      ASSERT_EQ(base * std::pow(2, std::pow(2, layer) - 1), res1);
      ASSERT_EQ(base * std::pow(2, std::pow(2, layer) - 1), res2);
    }
  }
}

TEST_F(TestIndependentDag, test_independent_post_generate_dag)
{
  OB_LOGGER.set_log_level("ERROR");
  ObIndependentPostGenerateDag *dag = nullptr;
  ObIndependentPostGenerateDagInitParam param;
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 100);
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(dag->get_counter(), ObIndependentPostGenerateDag::LOOP_TASK_NUM * ObIndependentPostGenerateDag::LOOP_TASK_POST_GENERATE_CNT);
  OB_LOGGER.set_log_level("INFO");
}

TEST_F(TestIndependentDag, test_increasing_add_task)
{
  OB_LOGGER.set_log_level("TRACE");
  ObIndependentExampleDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(true, dag->is_independent_);
  ObIDag::TaskList task_list;
  
  ObPriorityTask tasks[5];
  ObPriorityTask dag_tasks[5];
  tasks[0].priority_ = ObITask::TASK_PRIO_3;
  tasks[1].priority_ = ObITask::TASK_PRIO_2;
  tasks[2].priority_ = ObITask::TASK_PRIO_4;
  tasks[3].priority_ = ObITask::TASK_PRIO_5;
  tasks[4].priority_ = ObITask::TASK_PRIO_1;

  ASSERT_TRUE(tasks[0] <= tasks[0]);
  ASSERT_TRUE(tasks[0] <= tasks[2]);
  ASSERT_TRUE(tasks[0] <= tasks[3]);
  ASSERT_TRUE(tasks[1] <= tasks[2]);
  ASSERT_TRUE(tasks[1] <= tasks[3]);
  ASSERT_TRUE(tasks[2] <= tasks[3]);
  ASSERT_TRUE(tasks[4] <= tasks[0]);
  ASSERT_TRUE(tasks[4] <= tasks[1]);
  ASSERT_TRUE(tasks[4] <= tasks[2]);
  ASSERT_TRUE(tasks[4] <= tasks[3]);

  for (int i = 0; i < 5; i++) {
    task_list.increasing_add(&tasks[i]);
    dag_tasks[i].priority_ = tasks[i].get_priority();
    dag->inner_add_task_into_list(&dag_tasks[i]);
  }
  dag->update_ready_task_list();
  check_task_list_priority(task_list);
  check_task_list_priority(dag->task_list_);
  OB_LOGGER.set_log_level("INFO");
}

TEST_F(TestIndependentDag, test_priority_task)
{
  OB_LOGGER.set_log_level("TRACE");
  ObIndependentExampleDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);
  
  dag->clear_task_list(); // when init dag, the first task will be created
  ObSEArray<int64_t, ObITask::TASK_PRIO_MAX> priority_task_cnts;
  for (uint8_t prio = ObITask::TASK_PRIO_0; prio < ObITask::TASK_PRIO_MAX; prio++) {
    priority_task_cnts.push_back(0);
  }

  for (int64_t i = 1; i <= 1049; i++) {
    const uint8_t priority = ObRandom::rand(ObITask::TASK_PRIO_0, ObITask::TASK_PRIO_MAX - 1);
    ASSERT_TRUE(priority >= 0 && priority < ObITask::TASK_PRIO_MAX);
    priority_task_cnts[priority]++;
    (void) alloc_and_add_priority_task(dag, static_cast<ObITask::ObITaskPriority>(priority));

    if (i % 50 == 0) {
      COMMON_LOG(INFO, "before update", K(dag->waiting_task_list_.get_size()), K(dag->task_list_.get_size()), K(dag->waiting_task_list_), K(dag->task_list_), K(priority_task_cnts));
      dag->update_ready_task_list();
      COMMON_LOG(INFO, "after update", K(dag->waiting_task_list_.get_size()), K(dag->task_list_.get_size()), K(dag->waiting_task_list_), K(dag->task_list_), K(priority_task_cnts));

      // check update task list
      int64_t can_schedule_task_cnt = 0;
      for (uint8_t prio = ObITask::TASK_PRIO_1; prio < ObITask::TASK_PRIO_MAX; prio++) {
        can_schedule_task_cnt += priority_task_cnts[prio];
      }
      ASSERT_EQ(can_schedule_task_cnt, dag->task_list_.get_size());
      check_task_list_priority(dag->task_list_);
      DLIST_FOREACH_NORET(cur, dag->waiting_task_list_) {
        ASSERT_EQ(ObITask::TASK_PRIO_0, cur->get_priority());
      }
      ASSERT_EQ(priority_task_cnts[ObITask::TASK_PRIO_0], dag->waiting_task_list_.get_size());

      // clear ready task list and priority_task_cnts
      dag->inner_clear_task_list(dag->task_list_);
      dag->task_list_.reset();
      for (uint8_t prio = ObITask::TASK_PRIO_1; prio < ObITask::TASK_PRIO_MAX; prio++) {
        priority_task_cnts[prio] = 0;
      }
    }
  }

  dag->~ObIndependentExampleDag();
  ASSERT_EQ(0, dag->task_list_.get_size());
  ASSERT_EQ(0, dag->waiting_task_list_.get_size());
  allocator_.free(dag);
  dag = nullptr;
  OB_LOGGER.set_log_level("INFO");
}


TEST_F(TestIndependentDag, test_independent_suspend_task)
{
  ObIndependentSuspendDag *dag = nullptr;
  int64_t result = 0;
  int64_t round = 20;
  int64_t step = 10;
  ObIndependentSuspendInitParam param(&result, round, step);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 1); // only one thread
  px_worker_simulator.start();
  px_worker_simulator.wait();

  int64_t expected_result = 0;
  int cur_round = 0;
  for (int64_t cur_round = 0; cur_round < round; ++cur_round) {
    expected_result += step;
    expected_result *= 2;
  }
  ASSERT_EQ(expected_result, result);
  ASSERT_EQ(20971500, result);
}

TEST_F(TestIndependentDag, test_independent_suspend_task_v2)
{
  ObIndependentSuspendDagV2 *dag = nullptr;
  int64_t result = 0;
  int64_t round = 20;
  int64_t step = 10;
  ObIndependentSuspendInitParam param(&result, round, step);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);

  PxWorkerSimulator px_worker_simulator;
  px_worker_simulator.init(dag, 1); // only one thread
  px_worker_simulator.start();
  px_worker_simulator.wait();
  ASSERT_EQ(200, result);
}

TEST_F(TestIndependentDag, test_independent_dag_print)
{
  ObIndependentExampleDag *dag = nullptr;
  int64_t res1 = 0;
  int64_t res2 = 0;
  const int64_t x = 10;
  const int64_t y = 20;
  ObIndependentInitParam param(&res1, &res2, x, y);
  ASSERT_EQ(OB_SUCCESS, ObTenantDagScheduler::alloc_dag(allocator_, false /*is_ha_dag*/, dag));
  ASSERT_NE(nullptr, dag);
  
  ASSERT_EQ(OB_SUCCESS, dag->init(&param, nullptr /*dag_id*/));
  ASSERT_EQ(true, dag->is_independent_);
  dag->clear_task_list(); // when init dag, the first task will be created
  dag->dump_dag_status();

  for (int64_t i = 1; i <= 50; i++) {
    const uint8_t priority = ObRandom::rand(ObITask::TASK_PRIO_0, ObITask::TASK_PRIO_MAX - 1);
    ASSERT_TRUE(priority >= 0 && priority < ObITask::TASK_PRIO_MAX);
    (void) alloc_and_add_priority_task(dag, static_cast<ObITask::ObITaskPriority>(priority));
    if (i % 25 == 0) {
      dag->update_ready_task_list();
      check_task_list_priority(dag->task_list_);
      DLIST_FOREACH_NORET(cur, dag->waiting_task_list_) {
        ASSERT_EQ(ObITask::TASK_PRIO_0, cur->get_priority());
      }
      dag->dump_dag_status();
      dag->inner_clear_task_list(dag->task_list_);
      dag->task_list_.reset();
    }
  }

  dag->~ObIndependentExampleDag();
  allocator_.free(dag);
  dag = nullptr;
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_independent_dag.log*");
  OB_LOGGER.set_file_name("test_independent_dag.log", true);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_max_file_size(256*1024*1024);
  return RUN_ALL_TESTS();
}
