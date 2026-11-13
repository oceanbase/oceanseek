/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX TEST
#include <getopt.h>
#include <gtest/gtest.h>
#include <thread>
#define protected public
#define private public
#include "share/scheduler/test_dag_common.h"
#include "storage/compaction/ob_batch_freeze_tablets_dag.h"
#include "lib/random/ob_random.h"
#include "lib/utility/ob_defer.h"
#include "share/ob_debug_sync.h"

int64_t dag_cnt = 1;
int64_t stress_time= 1; // 100ms
char log_level[20] = "INFO";
uint32_t time_slice = 1000;
uint32_t sleep_slice = 2 * time_slice;
const int64_t CHECK_TIMEOUT = 20 * 1000LL * 1000LL; // larger than SCHEDULER_WAIT_TIME_MS and ADAPT_WORK_THREAD_INTERVAL

#define CHECK_EQ_UTIL_TIMEOUT(expected, expr) \
  { \
    int64_t start_time = oceanbase::common::ObTimeUtility::current_time(); \
    auto expr_result = (expr); \
    do { \
      if ((expected) == (expr_result)) { \
        break; \
      } else { \
        expr_result = (expr); \
      } \
    } while(oceanbase::common::ObTimeUtility::current_time() - start_time < CHECK_TIMEOUT); \
    if ((expected) != (expr_result)) { \
      COMMON_LOG_RET(WARN, OB_TIMEOUT, "check timeout", K(expected), K(expr_result), K(start_time), K(CHECK_TIMEOUT), \
        "current time", oceanbase::common::ObTimeUtility::current_time()); \
    } \
    EXPECT_EQ((expected), (expr_result)); \
  }

namespace oceanbase
{
using namespace common;
using namespace lib;
using namespace share;
using namespace omt;
using namespace compaction;
namespace unittest
{

class LoopWaitTask : public ObITask
{
public:
  LoopWaitTask() :
    ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0)
  {}
  int init(int64_t seq, int64_t cnt, bool &finish_flag)
  {
    seq_ = seq;
    cnt_ = cnt;
    finish_flag_ = &finish_flag;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask *&task)
  {
    int ret = OB_SUCCESS;
    ObIDag *dag = NULL;
    LoopWaitTask *ntask = NULL;
    if (seq_ >= cnt_) {
      return OB_ITER_END;
    } else if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is NULL", K(ret));
    } else if (OB_FAIL(dag->alloc_task(ntask))){
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (OB_ISNULL(ntask)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ntask is NULL", K(ret));
    } else {
      COMMON_LOG(INFO, "a task is generated", K(seq_));
      ntask->init(seq_ + 1, cnt_, *finish_flag_);
      task = ntask;
    }
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    while (true) {
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED == ret) {
          COMMON_LOG(INFO, "Cancel this task since the whole dag is canceled", K(ret));
          break;
        } else {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
      if (nullptr != finish_flag_) {
        if (ATOMIC_LOAD(finish_flag_) == true) {
          break;
        }
      }
      ::usleep(SLEEP_SLICE);
    }
    return ret;
  }
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "LoopWait", K(*dag_), K_(seq), K_(cnt));
private:
  int64_t seq_;
  int64_t cnt_;
  bool *finish_flag_;
};

class MaybeCanceledLoopWaitTask : public ObITask
{
public:
  MaybeCanceledLoopWaitTask() :
    ObITask(ObITask::TASK_TYPE_UT), seq_(0), cnt_(0), cancel_seq_(0)
  {}
  int init(int64_t seq, int64_t cnt, int64_t cancel_seq, bool &finish_flag)
  {
    seq_ = seq;
    cnt_ = cnt;
    cancel_seq_ = cancel_seq;
    finish_flag_ = &finish_flag;
    return OB_SUCCESS;
  }
  virtual int generate_next_task(ObITask *&task)
  {
    int ret = OB_SUCCESS;
    ObIDag *dag = NULL;
    MaybeCanceledLoopWaitTask *ntask = NULL;
    if (seq_ >= cnt_) {
      return OB_ITER_END;
    } else if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is NULL", K(ret));
    } else if (OB_FAIL(dag->alloc_task(ntask))){
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (OB_ISNULL(ntask)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "ntask is NULL", K(ret));
    } else {
      COMMON_LOG(INFO, "a task is generated", K(seq_));
      ntask->init(seq_ + 1, cnt_, cancel_seq_, *finish_flag_);
      task = ntask;
    }
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    ObTenantDagScheduler *scheduler = nullptr;

    if (seq_ == cancel_seq_) {
      if (OB_FAIL(dag_yield())) {
        if (OB_CANCELED != ret) {
          COMMON_LOG(WARN, "Invalid return value for dag_yield", K(ret));
        }
      }
      ::usleep(2*SLEEP_SLICE);
      if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
        COMMON_LOG(WARN, "Failed to get tenant dag scheduler for this tenant", K_(seq), K_(cnt), K_(cancel_seq), KP(scheduler));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(scheduler->cancel_dag(dag_, true))) {
        COMMON_LOG(WARN, "Failed to triger cancel this (running) dag", K_(seq), K_(cnt), K_(cancel_seq));
      } else {
        ret = OB_CANCELED;
        COMMON_LOG(INFO, "Successfully triger cancel this (running) dag", K_(seq), K_(cnt), K_(cancel_seq));
      }
    } else {
      while (true) {
        if (OB_FAIL(dag_yield()) && OB_CANCELED == ret) {
          COMMON_LOG(INFO, "Cancel this task since the whole dag is canceled", K_(seq), K_(cnt), K_(cancel_seq));
          break;
        }
        if (nullptr != finish_flag_) {
          if (ATOMIC_LOAD(finish_flag_) == true || seq_ % 2 == 1) {
            break;
          }
        }
        ::usleep(SLEEP_SLICE);
      }
    }
    return ret;
  }
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "MaybeCanceledLoopWaitTask", K(*dag_), K_(seq), K_(cnt), K_(cancel_seq));
  
private:
  int64_t seq_;
  int64_t cnt_;
  int64_t cancel_seq_;
  bool *finish_flag_;
};

class TestLPDag : public TestDag
{
public:
  TestLPDag() : TestDag(ObDagType::DAG_TYPE_DDL) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestLPDag);
};

class TestMPDag : public TestDag
{
public:
  TestMPDag() : TestDag(ObDagType::DAG_TYPE_FAST_MIGRATE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestMPDag);
};

class TestHALowDag : public TestDag
{
public:
  TestHALowDag() : TestDag(ObDagType::DAG_TYPE_BACKUP_PREPARE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestHALowDag);
};

class TestHAHighDag : public TestDag
{
public:
  TestHAHighDag() : TestDag(ObDagType::DAG_TYPE_INITIAL_COMPLETE_MIGRATION) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestHAHighDag);
};

class TestCompMidDag : public TestDag
{
public:
  TestCompMidDag() : TestDag(ObDagType::DAG_TYPE_MERGE_EXECUTE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompMidDag);
};

class TestMemRelatedDag : public TestDag
{
public:
  TestMemRelatedDag() : TestDag(ObDagType::DAG_TYPE_MINI_MERGE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestMemRelatedDag);
};

class TestCompLowDag : public TestDag
{
public:
  TestCompLowDag() : TestDag(ObDagType::DAG_TYPE_MAJOR_MERGE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompLowDag);
};

class TestCompLowVerifyDag : public TestDag
{
public:
  TestCompLowVerifyDag() : TestDag(ObDagType::DAG_TYPE_VERIFY_CKM) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompLowVerifyDag);
};

#ifdef OB_BUILD_SHARED_STORAGE
class TestSSUploadDag : public TestDag
{
public:
  TestSSUploadDag() : TestDag(ObDagType::DAG_TYPE_INC_SSTABLE_UPLOAD) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestSSUploadDag);
};

class TestSSAttachDag : public TestDag
{
public:
  TestSSAttachDag() : TestDag(ObDagType::DAG_TYPE_LINK_SHARED_SSTABLE) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestSSAttachDag);
};
#endif

class TestDDLDag : public TestDag
{
public:
  TestDDLDag() : TestDag(ObDagType::DAG_TYPE_DDL) {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDDLDag);
};



class TestCancelDag : public TestDag
{
public:
  static int64_t test_round;
  typedef TestPrepareTask CancelFinishTask;
  typedef MaybeCanceledLoopWaitTask CancelPrepareTask;
  TestCancelDag() : TestDag() {}
  int init_dag(int64_t cnt, int64_t cancel_seq, bool &finish_flag, AtomicOperator *op) {
    int ret = OB_SUCCESS;
    test_round++;
    COMMON_LOG(INFO, "Start Testing TestCancelDag", K(test_round));

    CancelPrepareTask *cancel_task = nullptr;
    CancelFinishTask *finish_task = nullptr;

    EXPECT_EQ(OB_SUCCESS, alloc_task(cancel_task));
    EXPECT_EQ(OB_SUCCESS, cancel_task->init(0, cnt, cancel_seq, finish_flag));
    EXPECT_EQ(OB_SUCCESS, alloc_task(finish_task));
    EXPECT_EQ(OB_SUCCESS, finish_task->init(1, op));
    EXPECT_EQ(OB_SUCCESS, cancel_task->add_child(*finish_task));
    EXPECT_EQ(OB_SUCCESS, add_task(*cancel_task));
    EXPECT_EQ(OB_SUCCESS, add_task(*finish_task));
    return ret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(TestCancelDag);
};

int64_t TestCancelDag::test_round = 0;

class TestCyclePrepare : public ObITask
{
public:
  TestCyclePrepare()
    : ObITask(ObITask::TASK_TYPE_UT), op_(NULL) {}
  int init(AtomicOperator *op = NULL)
  {
    int ret = OB_SUCCESS;
    if (NULL != op) {
      op_ = op;
    } else {
      TestDag *dag = static_cast<TestDag*>(get_dag());
      if (OB_ISNULL(dag)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "dag is null", K(ret));
      } else {
        op_ = &dag->get_op();
      }
    }
    return OB_SUCCESS;
  }

  int process()
  {
    int ret = OB_SUCCESS;
    TestDag*dag = static_cast<TestDag*>(get_dag());
    AtomicIncTask *inc_task = NULL;
    AtomicMulTask *mul_task = NULL;
    AtomicMulTask *mul_task1 = NULL;
    if (OB_ISNULL(dag)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "dag is null", K(ret));
    } else if (OB_FAIL(alloc_task(*dag, inc_task))) {
      COMMON_LOG(WARN, "failed to alloc inc_task", K(ret));
    } else if (OB_FAIL(inc_task->init(1, 5, *op_))) {
    } else if (OB_FAIL(alloc_task(*dag, mul_task))){
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task->init(1, 5, *op_))){
    } else if (OB_FAIL(alloc_task(*dag, mul_task1))){
      COMMON_LOG(WARN, "failed to alloc mul task", K(ret));
    } else if (OB_FAIL(mul_task1->init(1, 5, *op_))){
    } else if (OB_FAIL(mul_task->add_child(*inc_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(mul_task1->add_child(*inc_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(inc_task->add_child(*mul_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(add_child(*mul_task))) {
      COMMON_LOG(WARN, "failed to add child to self", K(ret));
    } else if (OB_FAIL(add_child(*mul_task1))) {
      COMMON_LOG(WARN, "failed to add child to self", K(ret));
    } else if (OB_FAIL(dag->add_task(*inc_task))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task1))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task))) {
      COMMON_LOG(WARN, "failed to add_task", K(ret));
    }
    return ret;
  }
private:
  AtomicOperator *op_;
};

class ObGenerateNextFailTask : public ObITask
{
public:
  ObGenerateNextFailTask() : ObITask(ObITask::TASK_TYPE_UT), cnt_(0) {}
  virtual ~ObGenerateNextFailTask() {}
  virtual int generate_next_task(ObITask *&next_task) 
  { 
    int ret = OB_ERR_UNEXPECTED;
    cnt_++;
    COMMON_LOG(WARN, "failed to generate next task", K(ret), K_(cnt));
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
public:
  int64_t cnt_;
};

class ObGenerateNextFailDag: public TestDag
{
public:
  ObGenerateNextFailDag() {}
  virtual ~ObGenerateNextFailDag() {}
  int init()
  {
    int ret = OB_SUCCESS;
    COMMON_LOG(INFO, "Start testing ObGenerateNextFailDag", K(ret));
    ObGenerateNextFailTask *task = nullptr;
    EXPECT_EQ(OB_SUCCESS, alloc_task(task));
    EXPECT_EQ(OB_SUCCESS, add_task(*task));
    return ret;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGenerateNextFailDag);
};

class ObMaybeCycleTask: public ObITask
{
public:
  ObMaybeCycleTask()
    : ObITask(ObITask::TASK_TYPE_UT), task_cnt_(-1), cur_idx_(-1) {}
  virtual ~ObMaybeCycleTask() {}
  int init(int64_t task_cnt, int64_t cur_idx)
  {
    int ret = OB_SUCCESS;
    task_cnt_ = task_cnt;
    cur_idx_ = cur_idx;
    return ret;
  }
  virtual int process()
  {
    int ret = OB_SUCCESS;
    const int64_t random_number = ObRandom::rand(0, 6);
    COMMON_LOG(INFO, "Start process task", K(ret), K(random_number), KPC(this));
    if (OB_ISNULL(dag_) ) {
      COMMON_LOG(WARN, "task is invalid", K(ret), KP_(dag));
    } else if (random_number == 3) {
      ObITask *task1 = nullptr;
      ObITask *task2 = nullptr;
      ObITask *task3 = nullptr;
      if (OB_FAIL(generate_next_task(task1))) {
        COMMON_LOG(WARN, "failed to generate next task", K(ret), KPC(task1));
      } else if (OB_FAIL(generate_next_task(task2))) {
        COMMON_LOG(WARN, "failed to generate next task", K(ret), KPC(task1));
      } else if (OB_FAIL(generate_next_task(task3))) {
        COMMON_LOG(WARN, "failed to generate next task", K(ret), KPC(task1));
      } else if (OB_ISNULL(task1) || OB_ISNULL(task2) || OB_ISNULL(task3)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret), KP(task1), KP(task2), KP(task3));
      } else if (OB_FAIL(task1->add_child(*task2))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(task1), KPC(task2));
      } else if (OB_FAIL(task2->add_child(*task3))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(task2), KPC(task3));
      } else if (OB_FAIL(task3->add_child(*task1))) {
        COMMON_LOG(WARN, "failed to add child", K(ret), KPC(task3), KPC(task1));
      } else if (OB_FAIL(dag_->add_task(*task1))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(task1));
      } else if (OB_FAIL(dag_->add_task(*task2))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(task2));
      } else if (OB_FAIL(dag_->add_task(*task3))) {
        COMMON_LOG(WARN, "failed to add task", K(ret), KPC(task3));
      }
    } else {
      usleep(random_number);
    }
    COMMON_LOG(INFO, "Finish process task", K(ret), K(random_number), KPC(this));
    return ret;
  }
  virtual int generate_next_task(ObITask *&next_task)
  {
    int ret = OB_SUCCESS;
    if (cur_idx_ >= task_cnt_ - 1) {
      ret = OB_ITER_END;
      COMMON_LOG(WARN, "failed to generate next task", K(ret), K(cur_idx_));
    } else {
      ObMaybeCycleTask *task = nullptr;
      COMMON_LOG(INFO, "Start generate next task", K(ret), KPC(this));
      if (OB_ISNULL(dag_)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(dag_->alloc_task(task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else if (OB_FAIL(task->init(task_cnt_, cur_idx_ + 1))) {
        COMMON_LOG(WARN, "failed to init task", K(ret));
      } else {
        next_task = task;
      }
      COMMON_LOG(INFO, "Finish generate next task", K(ret), KPC(this));
    }
    return ret;
  }
  INHERIT_TO_STRING_KV("ObITask", ObITask, K_(task_cnt), K_(cur_idx));
public:
  int64_t task_cnt_;
  int64_t cur_idx_;
};

class ObMayCycleDag: public TestDag
{
public:
  ObMayCycleDag() {}
  virtual ~ObMayCycleDag() {}
  int init(const int64_t dag_idx, const int64_t sub_task_cnt)
  {
    int ret = OB_SUCCESS;
    dag_idx_ = dag_idx;
    sub_task_cnt_ = sub_task_cnt;
    COMMON_LOG(INFO, "Start testing ObMayCycleDag", K(ret), K(dag_idx));
    ObMaybeCycleTask *task = nullptr;
    EXPECT_EQ(OB_SUCCESS, alloc_task(task));
    EXPECT_EQ(OB_SUCCESS, task->init(sub_task_cnt_, 0));
    EXPECT_EQ(OB_SUCCESS, add_task(*task));
    return ret;
  }
  INHERIT_TO_STRING_KV("TestDag", TestDag, K_(dag_idx));
public:
  int64_t dag_idx_;
  int64_t sub_task_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMayCycleDag);
};

class DagSchedulerStressTester : public lib::ThreadPool
{
  static const int64_t STRESS_THREAD_NUM = 16;
public:
  DagSchedulerStressTester() : scheduler_(nullptr), test_time_(0) {}
  ~DagSchedulerStressTester() { scheduler_ = nullptr; }

  int init(ObTenantDagScheduler *scheduler, int64_t test_time)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(scheduler)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "get invalid args", K(ret), K(scheduler));
    } else {
      scheduler_ = scheduler;
      test_time_ = test_time * 1000;
    }
    return ret;
  }

  int do_stress()
  {
    int ret = OB_SUCCESS;
    set_thread_count(STRESS_THREAD_NUM);
    int64_t start_time = ObTimeUtility::current_time();
    COMMON_LOG(INFO, "stress test start", K(start_time), K_(counter));
    start();
    wait();
    int64_t elapsed_time = ObTimeUtility::current_time() - start_time;
    COMMON_LOG(INFO, "stress test finished", K(elapsed_time / 1000), K_(counter));
    int ret_code = system("grep ERROR test_dag_scheduler.log -q | grep -v 'Fail to lock' | grep -v 'invalid tg id'");
    ret_code = WEXITSTATUS(ret_code);
    if (ret_code == 0) {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  void run1()
  {
    int64_t start_time = ObTimeUtility::current_time();
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    while (!has_set_stop()
           && OB_SUCC(ret)
           && ATOMIC_LOAD(&counter_) < MAX_COUNTER) {
      const int64_t dag_id = get_dag_id();
      TestDag *dag = NULL;
      TestPrepareTask *task = NULL;
      int expect_ret = (dag_id % 10 == 0 ? OB_ERR_UNEXPECTED : OB_SUCCESS);
      int64_t expect_value = (dag_id % 10 == 0 ? 0 : 8);

      switch (dag_id % ObDagPrio::DAG_PRIO_MAX) {
      case ObDagPrio::DAG_PRIO_COMPACTION_MID: {
        TestCompMidDag *lp_dag = NULL;
        if (OB_SUCCESS != (tmp_ret = scheduler_->alloc_dag(lp_dag))) {
          if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
            ret = tmp_ret;
            COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
          } else {
            COMMON_LOG(WARN, "out of memory", K(scheduler_->get_cur_dag_cnt()));
          }
        } else {
          dag = lp_dag;
        }
        break;
      }
      case ObDagPrio::DAG_PRIO_COMPACTION_LOW: {
        TestMPDag *mp_dag= NULL;
        if (OB_SUCCESS != (tmp_ret = scheduler_->alloc_dag(mp_dag))) {
          if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
            ret = tmp_ret;
            COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
          } else {
            COMMON_LOG(WARN, "out of memory", K(scheduler_->get_cur_dag_cnt()));
          }
        } else {
          dag = mp_dag;
        }
        break;
      }
      default : {
        TestMPDag *hp_dag= NULL;
        if (OB_SUCCESS != (tmp_ret = scheduler_->alloc_dag(hp_dag))) {
          if (OB_ALLOCATE_MEMORY_FAILED != tmp_ret) {
            ret = tmp_ret;
            COMMON_LOG(ERROR, "failed to allocate dag", K(ret));
          } else {
            COMMON_LOG(WARN, "out of memory", K(scheduler_->get_cur_dag_cnt()));
          }
        } else {
          dag = hp_dag;
        }
        break;
      }
      }
      if (OB_SUCCESS != tmp_ret) {
        continue;
      }

      if (OB_FAIL(dag->init(dag_id, expect_ret, expect_value, this))) {
        COMMON_LOG(WARN, "failed to init dag", K(ret));
        scheduler_->free_dag(*dag);
      } else if (OB_SUCCESS != (tmp_ret = alloc_task(*dag, task))){
        COMMON_LOG(WARN, "failed to alloc task", K(tmp_ret));
        scheduler_->free_dag(*dag);
      } else if (OB_FAIL(task->init(dag_id, NULL, expect_ret != OB_SUCCESS))) {
        COMMON_LOG(WARN, "failed to init task", K(ret));
        scheduler_->free_dag(*dag);
      } else if (OB_FAIL(dag->add_task(*task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
        scheduler_->free_dag(*dag);
      } else {
        if (OB_SUCCESS != (tmp_ret = scheduler_->add_dag(dag))) {
          if (OB_SIZE_OVERFLOW != tmp_ret) {
            COMMON_LOG(ERROR, "failed to add dag", K(tmp_ret), K(*dag));
          }
          scheduler_->free_dag(*dag);
        }
      }
    } // end of while
    int64_t end_time = ObTimeUtility::fast_current_time();
    COMMON_LOG(INFO, "finish add dag", K(ret), K(end_time - start_time));
  }

  int64_t get_dag_id()
  {
    return ATOMIC_FAA(&counter_, 1);
  }
private:
  static const int64_t MAX_COUNTER = 10 * 1000;
  static int64_t counter_;
  ObTenantDagScheduler *scheduler_;
  int64_t test_time_;
};

int64_t DagSchedulerStressTester::counter_ = 0;

class ConcurrentDagAdder : public lib::ThreadPool
{
public:
  static const int64_t THREAD_CNT = 2;

  ConcurrentDagAdder()
    : scheduler_(nullptr),
      dags_(),
      rets_(),
      ready_cnt_(0)
  {
    for (int64_t i = 0; i < THREAD_CNT; ++i) {
      dags_[i] = nullptr;
      rets_[i] = OB_ERR_UNEXPECTED;
    }
  }

  int init(ObTenantDagScheduler &scheduler, ObIDag &dag1, ObIDag &dag2)
  {
    int ret = OB_SUCCESS;
    scheduler_ = &scheduler;
    dags_[0] = &dag1;
    dags_[1] = &dag2;
    if (OB_FAIL(set_thread_count(THREAD_CNT))) {
      COMMON_LOG(WARN, "failed to set concurrent dag adder thread count", K(ret));
    }
    return ret;
  }

  int get_ret(const int64_t idx) const
  {
    return idx >= 0 && idx < THREAD_CNT ? rets_[idx] : OB_INVALID_ARGUMENT;
  }

  void run1() override
  {
    const int64_t idx = get_thread_idx();
    if (idx < 0 || idx >= THREAD_CNT || OB_ISNULL(scheduler_) || OB_ISNULL(dags_[idx])) {
      if (idx >= 0 && idx < THREAD_CNT) {
        rets_[idx] = OB_INVALID_ARGUMENT;
      }
    } else {
      ATOMIC_AAF(&ready_cnt_, 1);
      while (ATOMIC_LOAD(&ready_cnt_) < THREAD_CNT) {
        PAUSE();
      }
      rets_[idx] = scheduler_->add_dag(dags_[idx]);
    }
  }

private:
  ObTenantDagScheduler *scheduler_;
  ObIDag *dags_[THREAD_CNT];
  int rets_[THREAD_CNT];
  int64_t ready_cnt_;
};

#ifdef ERRSIM
// Exercise the real switch/stop-scan methods without background schedulers or
// another in-flight switch that could accidentally rescue a late paused worker.
class TestDagSwitchStop : public TestDagScheduler
{
public:
  TestDagSwitchStop() : need_pause_(false), old_sync_timeout_(0) {}
  void SetUp() override
  {
    TestDagScheduler::SetUp();
    old_sync_timeout_ = GCONF.debug_sync_timeout;
    ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
    scheduler_->set_stop(false);
    ObDagPrioScheduler &prio = scheduler_->prio_sche_[ObDagPrio::DAG_PRIO_DDL];
    prio.scheduler_ = scheduler_;
    prio.priority_ = ObDagPrio::DAG_PRIO_DDL;
    scheduler_->clear_running_dag_cnts(ObDagType::DAG_TYPE_DDL);
    for (int64_t i = 0; i < 2; ++i) {
      tasks_[i].dag_ = &dags_[i];
      ASSERT_EQ(OB_SUCCESS, workers_[i].cond_.init(ObWaitEventIds::DAG_WORKER_COND_WAIT));
      workers_[i].set_task(&tasks_[i]);
      workers_[i].status_ = ObTenantDagWorker::DWS_RUNNING;
      ASSERT_TRUE(prio.running_workers_.add_last(&workers_[i]));
      ++prio.running_task_cnts_;
      scheduler_->add_total_running_task_cnt();
      scheduler_->add_running_dag_cnts(ObDagType::DAG_TYPE_DDL);
    }
    // A legal 2 -> 1 limit reduction leaves two running workers temporarily over
    // quota. Only workers_[0] calls try_switch; workers_[1] cannot wake it.
    int64_t old_limit = 0;
    int64_t new_limit = 0;
    ASSERT_EQ(OB_SUCCESS, prio.set_thread_score(2, old_limit, new_limit));
    ASSERT_EQ(OB_SUCCESS, prio.set_thread_score(1, old_limit, new_limit));
    ASSERT_EQ(2, old_limit);
    ASSERT_EQ(1, new_limit);
    ASSERT_GT(prio.get_running_task_cnt(), prio.get_adaptive_limit());
  }

  void TearDown() override
  {
    // Always release/join on assertion failures; a broken stop check must fail
    // the assertions below rather than hang the test process.
    GDS.event_control_.signal("dag_switch_go");
    if (switch_thread_.joinable()) {
      switch_thread_.join();
    }
    ObDebugSyncAction action;
    action.sync_point_ = DAG_WORKER_BEFORE_SWITCH_LOCK;
    GDS.set_global_action(false, true, action);
    action.sync_point_ = DAG_WORKER_AFTER_SWITCH_LOCK;
    GDS.set_global_action(false, true, action);
    GDS.event_control_.clear_event();
    char timeout[64];
    snprintf(timeout, sizeof(timeout), "%ldus", old_sync_timeout_);
    GCONF.debug_sync_timeout.set_value(timeout);
    scheduler_->prio_sche_[ObDagPrio::DAG_PRIO_DDL].running_workers_.reset();
    scheduler_->prio_sche_[ObDagPrio::DAG_PRIO_DDL].waiting_workers_.reset();
    scheduler_->set_stop(true);
    TestDagScheduler::TearDown();
  }

  void start_switch(const ObDebugSyncPoint point)
  {
    ObDebugSyncAction action;
    action.sync_point_ = point;
    action.signal_ = "dag_switch_enter";
    action.wait_ = "dag_switch_go";
    action.execute_ = 1;
    action.timeout_ = 30 * 1000 * 1000L;
    ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, action));
    switch_thread_ = std::thread([this]() {
      ObTenantEnv::set_tenant(&tenant_base_);
      // yield() holds this lock through try_switch and setting DWS_WAITING.
      ObThreadCondGuard guard(workers_[0].cond_);
      need_pause_ = scheduler_->try_switch(workers_[0]);
      if (need_pause_) {
        workers_[0].status_ = ObTenantDagWorker::DWS_WAITING;
      }
      ObTenantEnv::set_tenant(nullptr);
    });
  }

  bool switch_hook_reached()
  {
    ObDSEventControl &events = GDS.event_control_;
    events.wait("dag_switch_enter", CHECK_TIMEOUT, false /*clear*/);
    // Debug-sync wait returns success even on timeout; require the actual signal.
    ObThreadCondGuard guard(events.cond_);
    ObDSEventControl::Event *event = nullptr;
    return OB_SUCCESS == events.find("dag_switch_enter", event)
        && nullptr != event && event->signal_cnt_ > 0;
  }

  void release_switch()
  {
    ASSERT_EQ(OB_SUCCESS, GDS.event_control_.signal("dag_switch_go"));
    switch_thread_.join();
  }

protected:
  TestLPDag dags_[2];
  AtomicIncTask tasks_[2];
  ObTenantDagWorker workers_[2];
  std::thread switch_thread_;
  bool need_pause_;
  int64_t old_sync_timeout_;
};

TEST_F(TestDagSwitchStop, stop_scan_before_late_switch)
{
  start_switch(DAG_WORKER_BEFORE_SWITCH_LOCK);
  ASSERT_TRUE(switch_hook_reached());
  ObDagPrioScheduler &prio = scheduler_->prio_sche_[ObDagPrio::DAG_PRIO_DDL];
  scheduler_->set_stop(true);
  prio.destroy_workers(); // The sole stop scan has completed before lock acquisition.
  release_switch();
  EXPECT_FALSE(need_pause_);
  EXPECT_EQ(0, prio.waiting_workers_.get_size());
  EXPECT_EQ(2, prio.running_workers_.get_size());
  EXPECT_EQ(ObTenantDagWorker::DWS_RUNNING, workers_[0].status_);
}

TEST_F(TestDagSwitchStop, pause_before_stop_scan)
{
  start_switch(DAG_WORKER_BEFORE_SWITCH_LOCK);
  ASSERT_TRUE(switch_hook_reached());
  release_switch();
  ObDagPrioScheduler &prio = scheduler_->prio_sche_[ObDagPrio::DAG_PRIO_DDL];
  ASSERT_TRUE(need_pause_);
  ASSERT_EQ(1, prio.waiting_workers_.get_size());
  ASSERT_EQ(ObTenantDagWorker::DWS_WAITING, workers_[0].status_);
  scheduler_->set_stop(true);
  prio.destroy_workers();
  EXPECT_EQ(ObTenantDagWorker::DWS_RUNNABLE, workers_[0].status_);
}

TEST_F(TestDagSwitchStop, stop_rechecked_after_priority_lock)
{
  start_switch(DAG_WORKER_AFTER_SWITCH_LOCK);
  ASSERT_TRUE(switch_hook_reached());
  // Stop after lock acquisition: checking only before the lock is insufficient.
  scheduler_->set_stop(true);
  release_switch();
  ObDagPrioScheduler &prio = scheduler_->prio_sche_[ObDagPrio::DAG_PRIO_DDL];
  EXPECT_FALSE(need_pause_);
  EXPECT_EQ(0, prio.waiting_workers_.get_size());
  EXPECT_EQ(2, prio.running_workers_.get_size());
  prio.destroy_workers();
}
#endif

TEST_F(TestDagScheduler, test_init)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  // invalid thread cnt
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler->init(MTL_ID(), time_slice, time_slice, -1));

  EXPECT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(),time_slice, time_slice, 100));
  EXPECT_EQ(OB_INIT_TWICE, scheduler->init(MTL_ID()));
}

// 场景：scheduler 从未初始化切换到已初始化，使用预设 DAG ID 查询参数、空 map 和锁超时。
// 预期：非法参数和未初始化状态返回对应错误，空 map 不持有 DAG，读锁超时不改变 exist。
TEST_F(TestDagScheduler, test_check_dag_exist_by_id_argument_and_timeout)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  const ObAddr addr(1683068975, 9999);
  ObDagId dag_id;
  dag_id.init(addr);
  ObDagId invalid_dag_id;
  bool exist = true;

  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler->check_dag_exist(invalid_dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);
  exist = true;
  EXPECT_EQ(OB_NOT_INIT, scheduler->check_dag_exist(dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);

  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  exist = true;
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);

  ASSERT_EQ(OB_SUCCESS, scheduler->dag_id_map_rwlock_.wrlock());
  const int query_ret = scheduler->check_dag_exist(
      dag_id, exist, ObTimeUtility::current_time());
  EXPECT_EQ(OB_SUCCESS, scheduler->dag_id_map_rwlock_.unlock());
  EXPECT_EQ(OB_TIMEOUT, query_ret);
  EXPECT_FALSE(exist);
}

// 场景：停止 scheduler 后，先验证 DAG 必须设置 ID 才能登记，再覆盖自动 ID、业务键和跨 priority 去重。
// 预期：未设置 ID 的 DAG 不进入 map；map 始终由首个成功 DAG 持有，取消持有者后 map 清空。
TEST_F(TestDagScheduler, test_dag_id_map_lifecycle_and_uniqueness)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->stop();
  scheduler->notify();
  scheduler->wait();

  bool exist = false;
  ObIDag *stored_dag = nullptr;
  ObDagId unassigned_dag_id;
  unassigned_dag_id.init(ObAddr(1683068975, 9999));
  TestLPDag *unassigned_id_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(unassigned_id_dag));
  ASSERT_EQ(OB_SUCCESS, unassigned_id_dag->init(1000));
  EXPECT_TRUE(unassigned_id_dag->get_dag_id().is_invalid());
  EXPECT_EQ(OB_STATE_NOT_MATCH,
      scheduler->register_dag_id_(unassigned_dag_id, *unassigned_id_dag));
  EXPECT_EQ(0, scheduler->dag_id_map_.size());
  scheduler->free_dag(*unassigned_id_dag);

  TestLPDag *auto_id_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(auto_id_dag));
  ASSERT_EQ(OB_SUCCESS, auto_id_dag->init(1001));
  EXPECT_TRUE(auto_id_dag->get_dag_id().is_invalid());
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(auto_id_dag));
  const ObDagId auto_dag_id = auto_id_dag->get_dag_id();
  EXPECT_TRUE(auto_dag_id.is_valid());
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(auto_dag_id, exist, INT64_MAX));
  EXPECT_TRUE(exist);
  EXPECT_EQ(1, scheduler->dag_id_map_.size());
  ASSERT_EQ(OB_SUCCESS, scheduler->dag_id_map_.get_refactored(auto_dag_id, stored_dag));
  EXPECT_EQ(auto_id_dag, stored_dag);

  TestLPDag *same_business_key_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(same_business_key_dag));
  ASSERT_EQ(OB_SUCCESS, same_business_key_dag->init(1001));
  EXPECT_EQ(OB_EAGAIN, scheduler->add_dag(same_business_key_dag));
  EXPECT_TRUE(same_business_key_dag->get_dag_id().is_invalid());
  EXPECT_EQ(1, scheduler->dag_id_map_.size());
  scheduler->free_dag(*same_business_key_dag);

  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(auto_id_dag, true));
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(auto_dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);
  EXPECT_EQ(0, scheduler->dag_id_map_.size());

  ObDagId preset_dag_id;
  preset_dag_id.init(ObAddr(1683068975, 9999));
  TestLPDag *first_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(first_dag));
  ASSERT_EQ(OB_SUCCESS, first_dag->init(2001));
  ASSERT_EQ(OB_SUCCESS, first_dag->set_dag_id(preset_dag_id));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(first_dag));
  EXPECT_TRUE(first_dag->get_dag_id().equals(preset_dag_id));

  TestLPDag *same_prio_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(same_prio_dag));
  ASSERT_EQ(OB_SUCCESS, same_prio_dag->init(2002));
  ASSERT_EQ(OB_SUCCESS, same_prio_dag->set_dag_id(preset_dag_id));
  EXPECT_EQ(OB_ENTRY_EXIST, scheduler->add_dag(same_prio_dag));
  EXPECT_EQ(1, scheduler->dag_id_map_.size());
  ASSERT_EQ(OB_SUCCESS, scheduler->dag_id_map_.get_refactored(preset_dag_id, stored_dag));
  EXPECT_EQ(first_dag, stored_dag);
  scheduler->free_dag(*same_prio_dag);
  same_prio_dag = nullptr;

  TestMPDag *cross_prio_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(cross_prio_dag));
  ASSERT_EQ(OB_SUCCESS, cross_prio_dag->init(3001));
  ASSERT_EQ(OB_SUCCESS, cross_prio_dag->set_dag_id(preset_dag_id));
  EXPECT_EQ(OB_ENTRY_EXIST, scheduler->add_dag(cross_prio_dag));
  EXPECT_EQ(1, scheduler->dag_id_map_.size());
  ASSERT_EQ(OB_SUCCESS, scheduler->dag_id_map_.get_refactored(preset_dag_id, stored_dag));
  EXPECT_EQ(first_dag, stored_dag);
  scheduler->free_dag(*cross_prio_dag);
  cross_prio_dag = nullptr;

  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(preset_dag_id, exist, INT64_MAX));
  EXPECT_TRUE(exist);
  ObDagId sub_task_dag_id = preset_dag_id;
  sub_task_dag_id.set_sub_id(1);
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(sub_task_dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);

  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(first_dag, true));
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(preset_dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);
  EXPECT_EQ(0, scheduler->dag_id_map_.size());
}

// 场景：停止 scheduler，添加预设 ID 的 DAG，再注入仅有自指 parent、无对应 child 的异常依赖。
// 预期：destroy 中 finish_dag_ 失败后仍完成清理，reset 正常返回且 DAG ID 索引被销毁。
TEST_F(TestDagScheduler, test_destroy_with_finish_dag_failure_cleanup)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->stop();
  scheduler->notify();
  scheduler->wait();

  ObDagId dag_id;
  dag_id.init(ObAddr(1683068975, 9999));

  TestLPDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  ASSERT_EQ(OB_SUCCESS, dag->init(6001));
  ASSERT_EQ(OB_SUCCESS, dag->set_dag_id(dag_id));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(dag));

  ObIDag *stored_dag = nullptr;
  ASSERT_EQ(1, scheduler->dag_id_map_.size());
  ASSERT_EQ(OB_SUCCESS, scheduler->dag_id_map_.get_refactored(dag_id, stored_dag));
  ASSERT_EQ(dag, stored_dag);
  ASSERT_EQ(0, dag->get_running_task_count());

  // Add an inconsistent parent edge after admission so finish_dag_ fails while
  // removing this DAG from its parent's children array.
  ASSERT_EQ(OB_SUCCESS, dag->add_parent_node(*dag));
  ASSERT_EQ(1, dag->get_indegree());
  ASSERT_EQ(1, dag->parent_.count());
  ASSERT_TRUE(dag->children_.empty());

  scheduler->destroy();
  EXPECT_FALSE(scheduler->is_inited_);
  EXPECT_FALSE(scheduler->dag_id_map_.created());
  EXPECT_EQ(0, scheduler->get_cur_dag_cnt());
  EXPECT_EQ(-1, scheduler->tg_id_);
}

// 场景：停止 scheduler，添加未预设 ID 的 DAG；先触发自动生成 ID 失败，再恢复地址重试。
// 预期：首次失败完整回滚 DAG/map 计数，重试成功取得所有权，取消后 DAG ID map 清空。
TEST_F(TestDagScheduler, test_generate_dag_id_failure_rollback)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->stop();
  scheduler->notify();
  scheduler->wait();

  TestLPDag *dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  dag->set_id(4001);
  EXPECT_TRUE(dag->get_dag_id().is_invalid());

  ObSysTaskStatMgr &sys_task_mgr = ObSysTaskStatMgr::get_instance();
  sys_task_mgr.self_addr_.reset();
  const int add_ret = scheduler->add_dag(dag);
  EXPECT_EQ(OB_INVALID_ERROR, add_ret);
  EXPECT_TRUE(dag->get_dag_id().is_invalid());
  EXPECT_EQ(0, scheduler->dag_id_map_.size());
  EXPECT_EQ(0, scheduler->prio_sche_[dag->get_priority()].dag_map_.size());
  EXPECT_EQ(0, scheduler->get_cur_dag_cnt());

  const ObAddr valid_addr(1683068975, 9999);
  ASSERT_EQ(OB_SUCCESS, sys_task_mgr.set_self_addr(valid_addr));

  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  const ObDagId dag_id = dag->get_dag_id();
  bool exist = false;
  EXPECT_TRUE(dag_id.is_valid());
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(dag_id, exist, INT64_MAX));
  EXPECT_TRUE(exist);
  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(dag, true));
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);
}

// 场景：停止 scheduler，DAG 由 add_dag 自动生成 ID，分别触发 sys task 注册失败和重复启停。
// 预期：失败不置注册标记，重复 start/finish 幂等，标记与 sys task 实际存在状态保持一致。
TEST_F(TestDagScheduler, test_sys_task_registration_state)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->stop();
  scheduler->notify();
  scheduler->wait();

  ObSysTaskStatMgr &sys_task_mgr = ObSysTaskStatMgr::get_instance();
  const ObAddr valid_addr(1683068975, 9999);

  TestLPDag *failed_start_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(failed_start_dag));
  ASSERT_EQ(OB_SUCCESS, failed_start_dag->init(5001));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(failed_start_dag));
  ObDagPrioScheduler &prio_scheduler =
      scheduler->prio_sche_[failed_start_dag->get_priority()];
  EXPECT_FALSE(failed_start_dag->is_sys_task_registered());

  sys_task_mgr.self_addr_.reset();
  EXPECT_EQ(OB_INVALID_ERROR, prio_scheduler.sys_task_start(*failed_start_dag));
  EXPECT_FALSE(failed_start_dag->is_sys_task_registered());
  EXPECT_EQ(OB_SUCCESS, prio_scheduler.sys_task_finish(*failed_start_dag));
  ASSERT_EQ(OB_SUCCESS, sys_task_mgr.set_self_addr(valid_addr));
  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(failed_start_dag, true));

  TestLPDag *success_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(success_dag));
  ASSERT_EQ(OB_SUCCESS, success_dag->init(5002));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(success_dag));
  ObDagPrioScheduler &success_prio_scheduler =
      scheduler->prio_sche_[success_dag->get_priority()];
  const ObDagId success_dag_id = success_dag->get_dag_id();
  bool sys_task_exist = false;

  EXPECT_EQ(OB_SUCCESS, success_prio_scheduler.sys_task_start(*success_dag));
  EXPECT_TRUE(success_dag->is_sys_task_registered());
  EXPECT_EQ(OB_SUCCESS, success_prio_scheduler.sys_task_start(*success_dag));
  EXPECT_EQ(OB_SUCCESS, sys_task_mgr.task_exist(success_dag_id, sys_task_exist));
  EXPECT_TRUE(sys_task_exist);

  EXPECT_EQ(OB_SUCCESS, success_prio_scheduler.sys_task_finish(*success_dag));
  EXPECT_FALSE(success_dag->is_sys_task_registered());
  EXPECT_EQ(OB_SUCCESS, success_prio_scheduler.sys_task_finish(*success_dag));
  EXPECT_EQ(OB_SUCCESS, sys_task_mgr.task_exist(success_dag_id, sys_task_exist));
  EXPECT_FALSE(sys_task_exist);
  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(success_dag, true));
}

// 场景：停止 scheduler，两个跨 priority DAG 预设相同 TraceID，并发竞争 tenant 级 ID 所有权。
// 预期：仅一个 DAG 入 map，失败者返回 OB_ENTRY_EXIST，取消胜者后 map 不再持有该 ID。
TEST_F(TestDagScheduler, test_concurrent_duplicate_trace_id)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->stop();
  scheduler->notify();
  scheduler->wait();

  ObDagId dag_id;
  dag_id.init(ObAddr(1683068975, 9999));
  TestLPDag *ddl_dag = nullptr;
  TestMPDag *ha_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(ddl_dag));
  ASSERT_EQ(OB_SUCCESS, ddl_dag->init(5001));
  ASSERT_EQ(OB_SUCCESS, ddl_dag->set_dag_id(dag_id));
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(ha_dag));
  ASSERT_EQ(OB_SUCCESS, ha_dag->init(5002));
  ASSERT_EQ(OB_SUCCESS, ha_dag->set_dag_id(dag_id));

  ConcurrentDagAdder adder;
  ASSERT_EQ(OB_SUCCESS, adder.init(*scheduler, *ddl_dag, *ha_dag));
  ASSERT_EQ(OB_SUCCESS, adder.start());
  adder.wait();

  const int ddl_ret = adder.get_ret(0);
  const int ha_ret = adder.get_ret(1);
  ASSERT_TRUE(
      (OB_SUCCESS == ddl_ret && OB_ENTRY_EXIST == ha_ret)
      || (OB_ENTRY_EXIST == ddl_ret && OB_SUCCESS == ha_ret));
  EXPECT_EQ(1, scheduler->dag_id_map_.size());
  ObIDag *stored_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->dag_id_map_.get_refactored(dag_id, stored_dag));
  ObIDag *added_dag = OB_SUCCESS == ddl_ret
      ? static_cast<ObIDag *>(ddl_dag)
      : static_cast<ObIDag *>(ha_dag);
  ObIDag *rejected_dag = OB_SUCCESS == ddl_ret
      ? static_cast<ObIDag *>(ha_dag)
      : static_cast<ObIDag *>(ddl_dag);
  EXPECT_EQ(added_dag, stored_dag);
  bool exist = false;
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(dag_id, exist, INT64_MAX));
  EXPECT_TRUE(exist);

  scheduler->free_dag(*rejected_dag);
  ASSERT_EQ(OB_SUCCESS, scheduler->cancel_dag(added_dag, true));
  EXPECT_EQ(OB_SUCCESS, scheduler->check_dag_exist(dag_id, exist, INT64_MAX));
  EXPECT_FALSE(exist);
  EXPECT_EQ(0, scheduler->dag_id_map_.size());
}

TEST_F(TestDagScheduler, basic_test)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  TestDag *dup_dag = NULL;
  int64_t counter = 0;
  //simple two-level dag
  if (OB_FAIL(scheduler->alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestAddTask *add_task = NULL;
    TestMulTask *mul_task = NULL;
    if (OB_FAIL(dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(mul_task->init(&counter))) {
      COMMON_LOG(WARN, "failed to init add task", K(ret));
    } else if (OB_FAIL(dag->alloc_task(add_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == add_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else if (OB_FAIL(add_task->init(&counter, 1, 0, 10, 200 * 1000))) {
      COMMON_LOG(WARN, "failed to init add task", K(ret));
    } else if (OB_FAIL(add_task->add_child(*mul_task))) {
      COMMON_LOG(WARN, "failed to add child", K(ret));
    } else if (OB_FAIL(dag->add_task(*mul_task))) {
      COMMON_LOG(WARN, "failed to add task", K(ret));
    } else if (OB_FAIL(dag->add_task(*add_task))) {
      COMMON_LOG(WARN, "failed to add task");
    }
  }
  // check deduplication functionality
  ASSERT_EQ(OB_SUCCESS, ret);
  if (OB_FAIL(scheduler->alloc_dag(dup_dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dup_dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dup_dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestMulTask *mul_task = NULL;
    if (OB_FAIL(dup_dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter))) {
        COMMON_LOG(WARN, "failed to init add task", K(ret));
      } else if (OB_FAIL(dup_dag->add_task(*mul_task))) {
        COMMON_LOG(WARN, "failed to add task", K(ret));
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  EXPECT_EQ(OB_EAGAIN, scheduler->add_dag(dup_dag));
  if (OB_NOT_NULL(dup_dag)) {
    scheduler->free_dag(*dup_dag);
  }
  wait_scheduler();
  EXPECT_EQ(counter, 20);

  // three level dag that each level would generate dynamic tasks
  AtomicOperator op(0);
  TestDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  AtomicIncTask *inc_task1 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));

  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 4, op));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task1->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(170, op.value());

  // two-level dag with 2 tasks on the first level
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  //add mul task
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 4, op));
  // add inc task
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, inc_task->add_child(*mul_task));
  // add another inc task
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, inc_task1->add_child(*mul_task));

  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(320, op.value());

  // a dag with single task which generate all other tasks while processing
  TestPrepareTask *prepare_task = NULL;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(8, op.value());

  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, false, 1000*1000));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  wait_scheduler();
  EXPECT_EQ(8, op.value());
}

TEST_F(TestDagScheduler, test_cycle)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  int64_t counter = 0;
  if (OB_FAIL(scheduler->alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestAddTask *add_task = NULL;
    TestMulTask *mul_task = NULL;
    if (OB_FAIL(dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter))) {
        COMMON_LOG(WARN, "failed to init add task", K(ret));
      }
      EXPECT_EQ(OB_INVALID_ARGUMENT, mul_task->add_child(*mul_task));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dag->alloc_task(add_task))) {
          COMMON_LOG(WARN, "failed to alloc task", K(ret));
        } else if (NULL == add_task) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "task is null", K(ret));
        } else {
          add_task->init(&counter, 1, 0, 3);
          add_task->add_child(*mul_task);
          mul_task->add_child(*add_task);
        }
        EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
        EXPECT_EQ(OB_INVALID_ARGUMENT, dag->add_task(*add_task));
        scheduler->free_dag(*dag);
      }
    }
  }
  TestCyclePrepare *prepare_task = NULL;
  AtomicOperator op;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1, OB_INVALID_ARGUMENT, 0));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, prepare_task));
  EXPECT_EQ(OB_SUCCESS, prepare_task->init(&op));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*prepare_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());
}

TEST_F(TestDagScheduler, test_priority)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  AtomicOperator op(0);

  int32_t thread_cnt = scheduler->get_work_thread_num();

  int32_t concurrency = 60;
  bool finish_flag[5] = {false, false, false, false, false};

  TestDDLDag *dag1 = NULL;
  LoopWaitTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[0]));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestHALowDag *dag2 = NULL;
  LoopWaitTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, concurrency, finish_flag[1]));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));
  TestHAHighDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[2]));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));
  TestMemRelatedDag *dag4 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag4));
  EXPECT_EQ(OB_SUCCESS, dag4->init(4));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag4, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[3]));
  EXPECT_EQ(OB_SUCCESS, dag4->add_task(*inc_task));
  TestCompMidDag *dag5 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag5));
  EXPECT_EQ(OB_SUCCESS, dag5->init(5));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag5, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[4]));
  EXPECT_EQ(OB_SUCCESS, dag5->add_task(*inc_task));

  const int64_t uplimit = 10;
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_DDL, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_HA_LOW, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_HA_HIGH, uplimit));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, uplimit));

  // 13 threads in total
  int64_t max_uplimit = uplimit;
  int64_t threads_sum = 0;
  int64_t work_thread_num = 0;
  for (int64_t i = 0; i < ObDagPrio::DAG_PRIO_MAX; ++i) { // calc sum of default_low_limit
    threads_sum += scheduler->prio_sche_[i].limits_;
  }
  work_thread_num = threads_sum;

  EXPECT_EQ(work_thread_num, scheduler->get_work_thread_num());
  scheduler->dump_dag_status();

  // low priority run at max speed
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag4));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag5));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));

  for (int i = 0; i < 5; ++i) {
    finish_flag[i] = true;
  }

  wait_scheduler();
}

// TEST_F(TestDagScheduler, test_error_handling)
// {
//   ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
//   ASSERT_TRUE(nullptr != scheduler);
//   ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

//   AtomicOperator op(0);
//   TestDag *dag = NULL;
//   AtomicMulTask *mul_task = NULL;
//   AtomicIncTask *inc_task = NULL;
//   EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
//   EXPECT_EQ(OB_SUCCESS, dag->init(1));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
//   EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 10, op, 0, 8));
//   EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
//   EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
//   EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
//   EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
//   EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
//   wait_scheduler();
//   EXPECT_EQ(0, op.value());

//   EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
//   EXPECT_EQ(OB_SUCCESS, dag->init(1));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, mul_task));
//   EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 1, op, 0, 1));
//   EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
//   EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
//   EXPECT_EQ(OB_SUCCESS, inc_task->add_child(*mul_task));
//   EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
//   EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
//   EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
//   EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
//   EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
//   wait_scheduler();
//   EXPECT_EQ(10, op.value());

//   TestDag *dag1 = NULL;
//   TestPrepareTask *prepare_task = NULL;
//   op.reset();
//   EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
//   EXPECT_EQ(OB_SUCCESS, dag1->init(1));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
//   EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true));
//   EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
//   EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
//   wait_scheduler();
//   EXPECT_EQ(0, op.value());

//   op.reset();
//   EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
//   EXPECT_EQ(OB_SUCCESS, dag1->init(1));
//   EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, prepare_task));
//   EXPECT_EQ(OB_SUCCESS, prepare_task->init(1, &op, true, 1000*1000));
//   EXPECT_EQ(OB_SUCCESS, dag1->add_task(*prepare_task));
//   EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
//   wait_scheduler();
//   EXPECT_EQ(0, op.value());
// }

void print_state(int64_t idx)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  COMMON_LOG(INFO, "scheduler state: ", K(scheduler->get_total_running_task_cnt()), K(scheduler->work_thread_num_),
      K(scheduler->total_worker_cnt_), K(scheduler->prio_sche_[idx].limits_), 
      K(scheduler->prio_sche_[idx].running_task_cnts_));
}

TEST_F(TestDagScheduler, test_set_concurrency)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  int64_t prio = ObDagPrio::DAG_PRIO_HA_MID;
  int32_t uplimit = OB_DAG_PRIOS[prio].score_;
  EXPECT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int cnt = 30;

  scheduler->dump_dag_status();
  TestMPDag *dag = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, dag->get_op(), 10 * sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));

  CHECK_EQ_UTIL_TIMEOUT(OB_DAG_PRIOS[prio].score_, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(uplimit, scheduler->get_total_running_task_cnt());
  EXPECT_TRUE(uplimit <= scheduler->work_thread_num_);
  // set max to 20
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 20));
  EXPECT_EQ(20, scheduler->prio_sche_[prio].limits_);
  //EXPECT_EQ(41, scheduler->work_thread_num_);
  // set to 1
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 1));
  print_state(prio);
  EXPECT_EQ(1, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_total_running_task_cnt());
  EXPECT_TRUE(1 <= scheduler->work_thread_num_);
  // set to 2
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 2));
  print_state(prio);
  EXPECT_EQ(2, scheduler->prio_sche_[prio].limits_);
  CHECK_EQ_UTIL_TIMEOUT(2, scheduler->get_total_running_task_cnt());
  EXPECT_TRUE(2 <= scheduler->work_thread_num_);
  // set to 5
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 5));
  print_state(prio);
  EXPECT_EQ(5, scheduler->prio_sche_[prio].limits_);
  EXPECT_TRUE(5 <= scheduler->work_thread_num_);
  // set to 0
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(prio, 0));
  print_state(prio);
  EXPECT_EQ(uplimit, scheduler->prio_sche_[prio].limits_);
  EXPECT_TRUE(uplimit <= scheduler->work_thread_num_);
  wait_scheduler();
}

TEST_F(TestDagScheduler, stress_test)
{
  
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64, 100 * 1000));
  DagSchedulerStressTester tester;
  tester.init(scheduler, stress_time);
  int64_t start_time = ObTimeUtility::fast_current_time();
  EXPECT_EQ(OB_SUCCESS, tester.do_stress());
  wait_scheduler();
  int64_t finish_time = ObTimeUtility::fast_current_time();
  COMMON_LOG(INFO, "finish running dag", K(finish_time - start_time));
}

TEST_F(TestDagScheduler, test_get_dag_count)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  TestMPDag *dag = NULL;
  TestMPDag *dag2 = NULL;
  TestMulTask *mul_task = NULL;
  TestMulTask *mul_task2 = NULL;
  int64_t counter = 1;

  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));

  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  sleep(10);
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  CHECK_EQ_UTIL_TIMEOUT(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));
  scheduler->stop();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init(1));
  EXPECT_EQ(OB_SUCCESS, dag->alloc_task(mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(1, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, dag2->alloc_task(mul_task2));
  EXPECT_EQ(OB_SUCCESS, mul_task2->init(&counter));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task2));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(2, scheduler->get_dag_count(ObDagType::DAG_TYPE_FAST_MIGRATE));
  EXPECT_EQ(0, scheduler->get_dag_count(ObDagType::DAG_TYPE_DDL));
  EXPECT_EQ(-1, scheduler->get_dag_count(ObDagType::DAG_TYPE_MAX));
}

TEST_F(TestDagScheduler, test_get_priority_dag_count)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_NE(nullptr, scheduler);

  int64_t priority_dag_count = 1;
  EXPECT_EQ(OB_NOT_INIT, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_COMPACTION_LOW, priority_dag_count));
  EXPECT_EQ(0, priority_dag_count);

  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  priority_dag_count = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler->get_priority_dag_count(-1, priority_dag_count));
  EXPECT_EQ(0, priority_dag_count);
  priority_dag_count = 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_MAX, priority_dag_count));
  EXPECT_EQ(0, priority_dag_count);
  EXPECT_EQ(OB_SUCCESS, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_COMPACTION_LOW, priority_dag_count));
  EXPECT_EQ(0, priority_dag_count);

  scheduler->stop();
  int64_t counter = 1;
  TestMulTask *task = nullptr;
  TestCompLowDag *major_dag = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(major_dag));
  ASSERT_EQ(OB_SUCCESS, major_dag->init(1));
  ASSERT_EQ(OB_SUCCESS, major_dag->alloc_task(task));
  ASSERT_EQ(OB_SUCCESS, task->init(&counter));
  ASSERT_EQ(OB_SUCCESS, major_dag->add_task(*task));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(major_dag));
  EXPECT_EQ(OB_SUCCESS, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_COMPACTION_LOW, priority_dag_count));
  EXPECT_EQ(1, priority_dag_count);

  TestCompLowVerifyDag *verify_dag = nullptr;
  task = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(verify_dag));
  ASSERT_EQ(OB_SUCCESS, verify_dag->init(2));
  ASSERT_EQ(OB_SUCCESS, verify_dag->alloc_task(task));
  ASSERT_EQ(OB_SUCCESS, task->init(&counter));
  ASSERT_EQ(OB_SUCCESS, verify_dag->add_task(*task));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(verify_dag));
  EXPECT_EQ(OB_SUCCESS, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_COMPACTION_LOW, priority_dag_count));
  EXPECT_EQ(2, priority_dag_count);

  TestCompMidDag *minor_dag = nullptr;
  task = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag(minor_dag));
  ASSERT_EQ(OB_SUCCESS, minor_dag->init(3));
  ASSERT_EQ(OB_SUCCESS, minor_dag->alloc_task(task));
  ASSERT_EQ(OB_SUCCESS, task->init(&counter));
  ASSERT_EQ(OB_SUCCESS, minor_dag->add_task(*task));
  ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(minor_dag));
  EXPECT_EQ(OB_SUCCESS, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_COMPACTION_LOW, priority_dag_count));
  EXPECT_EQ(2, priority_dag_count);
  EXPECT_EQ(OB_SUCCESS, scheduler->get_priority_dag_count(
      ObDagPrio::DAG_PRIO_COMPACTION_MID, priority_dag_count));
  EXPECT_EQ(1, priority_dag_count);
}

TEST_F(TestDagScheduler, test_destroy_when_running)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  void *buf = nullptr;
  ASSERT_TRUE(nullptr != (buf = allocator_.alloc(sizeof(AtomicOperator))));
  AtomicOperator *op = new(buf) AtomicOperator();

  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  int32_t thread_cnt = 64;
  int32_t lp_max = OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_DDL].score_;
  scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, 100);
  // thread cnt is 121
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, thread_cnt * 3, *op, 2*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  TestMPDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, thread_cnt * 10, *op, 3*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));

  #ifndef BUILD_COVERAGE
  // not participate in coverage compilation to fix hang problem
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(lp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  // CHECK_EQ_UTIL_TIMEOUT(hp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  CHECK_EQ_UTIL_TIMEOUT(lp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL));
  #endif
}

TEST_F(TestDagScheduler, test_up_limit)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  AtomicOperator op(0);

  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  const int32_t lp_max = scheduler->prio_sche_[ObDagPrio::DAG_PRIO_HA_LOW].limits_;
  const int32_t mp_max = scheduler->prio_sche_[ObDagPrio::DAG_PRIO_HA_MID].limits_;
  const int32_t hp_max = scheduler->prio_sche_[ObDagPrio::DAG_PRIO_HA_HIGH].limits_;
  const int64_t cnt = 64;
  scheduler->dump_dag_status();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  dag1->set_priority(ObDagPrio::DAG_PRIO_HA_LOW);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, 2*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));

  TestMPDag *dag2 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  dag2->set_priority(ObDagPrio::DAG_PRIO_HA_HIGH);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, cnt, op, 4*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*mul_task));

  TestMPDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  dag3->set_priority(ObDagPrio::DAG_PRIO_HA_MID);
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, cnt, op, 3*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(lp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  // high priority preempt quotas from low priority, low priority run at min thread
  CHECK_EQ_UTIL_TIMEOUT(hp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(MIN(scheduler->work_thread_num_ - hp_max, lp_max), scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_LOW));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3));
  CHECK_EQ_UTIL_TIMEOUT(hp_max, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_HIGH));
  CHECK_EQ_UTIL_TIMEOUT(MIN(scheduler->work_thread_num_ - hp_max, mp_max), scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_HA_MID));

  wait_scheduler();
}

TEST_F(TestDagScheduler, test_emergency_task)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));

  AtomicOperator op(0);

  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_DDL, 1));
  EXPECT_EQ(1, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_DDL].limits_);
  TestLPDag *dag1 = NULL;
  AtomicIncTask *inc_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 1, op, 20*sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));

  TestLPDag *dag2 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 1, op, sleep_slice));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*inc_task));

  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));

  while (scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_DDL) == 0) {
    usleep(10);
  }

  TestLPDag *dag3 = NULL;
  AtomicMulTask *mul_task = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 1, op, 0));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3, true));

  wait_scheduler();

  EXPECT_EQ(3, op.value());
}

class TestCompMidCancelDag : public compaction::ObTabletMergeDag
{
public:
  TestCompMidCancelDag()
    : compaction::ObTabletMergeDag(ObDagType::DAG_TYPE_MERGE_EXECUTE){}
  virtual const share::ObLSID & get_ls_id() const override { return ls_id_; }
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
private:
  DISALLOW_COPY_AND_ASSIGN(TestCompMidCancelDag);
};

TEST_F(TestDagScheduler, test_check_ls_compaction_dag_exist_with_cancel)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, 1));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_LOW, 1));
  EXPECT_EQ(1, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_MID].limits_);
  EXPECT_EQ(1, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_LOW].limits_);

  LoopWaitTask *wait_task = nullptr;
  LoopWaitTask *wait_task2 = nullptr;
  const int64_t dag_cnt = 6;
  // add 6 dag at prio = DAG_PRIO_COMPACTION_MID
  const int64_t ls_cnt = 2;
  ObLSID ls_ids[ls_cnt] = {ObLSID(1), ObLSID(2)};
  bool finish_flag[ls_cnt] = {false, false};
  for (int64_t i = 0; i < dag_cnt; ++i) {
    const int64_t idx = i % ls_cnt;
    TestCompMidCancelDag *dag = NULL;
    EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
    dag->ls_id_ = ls_ids[idx];
    dag->tablet_id_ = ObTabletID(i);
    EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, wait_task));
    EXPECT_EQ(OB_SUCCESS, wait_task->init(1, 2, finish_flag[idx]));
    EXPECT_EQ(OB_SUCCESS, dag->add_task(*wait_task));
    EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  }
  // add 2 dag at prio = DAG_PRIO_COMPACTION_LOW
  for (int64_t i = 0; i < ls_cnt; ++i) {
    ObBatchFreezeTabletsDag *batch_freeze_dag = NULL;
    EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(batch_freeze_dag));
    batch_freeze_dag->param_.ls_id_ = ls_ids[i];
    EXPECT_EQ(OB_SUCCESS, alloc_task(*batch_freeze_dag, wait_task2));
    EXPECT_EQ(OB_SUCCESS, wait_task2->init(1, 2, finish_flag[i]));
    EXPECT_EQ(OB_SUCCESS, batch_freeze_dag->add_task(*wait_task2));
    EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(batch_freeze_dag));
  }
  EXPECT_EQ(dag_cnt, scheduler->dag_cnts_[ObDagType::DAG_TYPE_MERGE_EXECUTE]);
  CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_LOW));

  // cancel waiting dag of ls_ids[0], all dag of ls_ids[1] will be destroyed when check_cancel
  bool exist = false;
  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[0], exist));
  EXPECT_EQ(exist, true);
  EXPECT_EQ(4, scheduler->dag_cnts_[ObDagType::DAG_TYPE_MERGE_EXECUTE]);

  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[1], exist));
  EXPECT_EQ(exist, false);

  finish_flag[0] = true;
  wait_scheduler();

  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[0], exist));
  EXPECT_EQ(exist, false);
  EXPECT_EQ(OB_SUCCESS, scheduler->check_ls_compaction_dag_exist_with_cancel(ls_ids[0], exist));
  EXPECT_EQ(exist, false);
}

TEST_F(TestDagScheduler, test_cancel_running_dag)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int64_t cnt = 10;
  int64_t cancel_seq = 0;
  bool finish_flag = false;
  AtomicOperator op(0);
  TestCancelDag *cancel_dag = nullptr;
  TestCancelDag *cancel_dag_key = nullptr;

  // The dag canceled in prepare task so the finish task can not finish
  // 1. Cancel in the first round of prepare task
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 2. Cancel in the middle round of prepare task
  cancel_seq = 5;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 3. Cancel in the last round of prepare task
  cancel_seq = 9;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 4. Cancel even all other prepare task finsihed
  finish_flag = true;
  cancel_seq = 9;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 5. Can not cancel if prepare task does not call yield
  finish_flag = true;
  cancel_seq = 11;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(8, op.value());

  // 6. Test cancel ready dag
  finish_flag = false;
  cancel_seq = 3;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, scheduler->cancel_dag(cancel_dag, true));
  wait_scheduler();
  EXPECT_EQ(0, op.value());

  // 7. Test cancel dag after dag finish(running ok)
  finish_flag = true;
  cancel_seq = 11;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(8, op.value());
  // Example for canceling a maybe finished dag 
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag_key));
  EXPECT_EQ(OB_SUCCESS, cancel_dag_key->init(1)); // the same hash key with cancel_dag
  EXPECT_EQ(OB_SUCCESS, scheduler->cancel_dag(cancel_dag_key, true));
  scheduler->free_dag(*cancel_dag_key);


  // 8. Test cancel dag after dag finish(canceled)
  finish_flag = true;
  cancel_seq = 9;
  op.reset();
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init(1));
  EXPECT_EQ(OB_SUCCESS, cancel_dag->init_dag(cnt, cancel_seq, finish_flag, &op));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(cancel_dag));
  wait_scheduler();
  EXPECT_EQ(0, op.value());
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(cancel_dag_key));
  EXPECT_EQ(OB_SUCCESS, cancel_dag_key->init(1));
  EXPECT_EQ(OB_SUCCESS, scheduler->cancel_dag(cancel_dag_key, true));
  scheduler->free_dag(*cancel_dag_key);
}

TEST_F(TestDagScheduler, test_generate_next_task_failed)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  ObGenerateNextFailDag *dag = nullptr;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
  EXPECT_EQ(OB_SUCCESS, dag->init());
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag)); 
  wait_scheduler();
}

TEST_F(TestDagScheduler, test_maybe_cycle_tasks)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < 500; i++) {
    ObMayCycleDag *dag = nullptr;
    EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
    EXPECT_EQ(OB_SUCCESS, dag->init(i, 20));
    if (OB_FAIL(scheduler->add_dag(dag))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        i--;
        usleep(10 * 1000 /*10 ms*/);
        if (OB_NOT_NULL(dag)) {
          (void) scheduler->free_dag(*dag);
          dag = nullptr;
        }
      } else {
        EXPECT_EQ(OB_SUCCESS, ret); 
      }
    }
  }
  wait_scheduler();
}

/**
  *  We schedule batch freeze tablets dag when do major merge, which usually contains batch of tablets (default 1000)
  *  We regard batch freeze tablets dag is same when satify following conditions:
  *     1. ls_id is same
  *     2. compaction_scn is same
  *     3. loop_cnt is different
  *  This Test simply test dag deduplication logic of ObBatchFreezeTabletsDag
 **/
TEST_F(TestDagScheduler, test_batch_freeze_tablets_dag)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_LOW, 1));
  EXPECT_EQ(1, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_LOW].limits_);

  int ret = OB_SUCCESS;
  // 1. create and add first dag, expect success
  LoopWaitTask *wait_task1 = nullptr;
  ObBatchFreezeTabletsDag *batch_freeze_dag1 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(batch_freeze_dag1));
  ObBatchFreezeTabletsParam param1;
  param1.ls_id_ = ObLSID(1);
  param1.compaction_scn_ = 100;
  param1.loop_cnt_ = 1;
  ObCOMajorMergeStrategy strategy;
  ObTabletSchedulePair tablet_pair(ObTabletID(1001), 100, strategy);
  EXPECT_EQ(OB_SUCCESS, param1.tablet_info_array_.push_back(tablet_pair));
  EXPECT_EQ(OB_SUCCESS, batch_freeze_dag1->init_by_param(&param1));

  bool finish_flag = false;
  EXPECT_EQ(OB_SUCCESS, alloc_task(*batch_freeze_dag1, wait_task1));
  EXPECT_EQ(OB_SUCCESS, wait_task1->init(1, 2, finish_flag));
  EXPECT_EQ(OB_SUCCESS, batch_freeze_dag1->add_task(*wait_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(batch_freeze_dag1));

  // 2. create and add second dag, expect success, because loop_cnt is the same while
  LoopWaitTask *wait_task2 = nullptr;
  ObBatchFreezeTabletsDag *batch_freeze_dag2 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(batch_freeze_dag2));
  ObBatchFreezeTabletsParam param2;
  param2.ls_id_ = ObLSID(1);
  param2.compaction_scn_ = 100;
  param2.loop_cnt_ = 1;
  EXPECT_EQ(OB_SUCCESS, param2.tablet_info_array_.push_back(tablet_pair));
  EXPECT_EQ(OB_SUCCESS, batch_freeze_dag2->init_by_param(&param2));

  bool finish_flag2 = false;
  EXPECT_EQ(OB_SUCCESS, alloc_task(*batch_freeze_dag2, wait_task2));
  EXPECT_EQ(OB_SUCCESS, wait_task2->init(1, 2, finish_flag2));
  EXPECT_EQ(OB_SUCCESS, batch_freeze_dag2->add_task(*wait_task2));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(batch_freeze_dag2));


  // 3. create and add third dag, expect OB_EAGAIN, because loop_cnt is the different though ls_id and compaction_scn is the same
  LoopWaitTask *wait_task3 = nullptr;
  ObBatchFreezeTabletsDag *batch_freeze_dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(batch_freeze_dag3));
  ObBatchFreezeTabletsParam param3;
  param3.ls_id_ = ObLSID(1);
  param3.compaction_scn_ = 100;
  param3.loop_cnt_ = 2;
  EXPECT_EQ(OB_SUCCESS, param3.tablet_info_array_.push_back(tablet_pair));
  EXPECT_EQ(OB_SUCCESS, batch_freeze_dag3->init_by_param(&param3));

  bool finish_flag3 = false;
  EXPECT_EQ(OB_SUCCESS, alloc_task(*batch_freeze_dag3, wait_task3));
  EXPECT_EQ(OB_SUCCESS, wait_task3->init(1, 2, finish_flag3));
  EXPECT_EQ(OB_SUCCESS, batch_freeze_dag3->add_task(*wait_task3));
  EXPECT_EQ(OB_EAGAIN, scheduler->add_dag(batch_freeze_dag3));
}

TEST_F(TestDagScheduler, test_max_concurrent_task)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice, 64));
  const int64_t MAX_MINOR_THREAD_CNT = 10;
  EXPECT_EQ(OB_SUCCESS, scheduler->set_thread_score(ObDagPrio::DAG_PRIO_COMPACTION_MID, MAX_MINOR_THREAD_CNT));
  EXPECT_EQ(MAX_MINOR_THREAD_CNT, scheduler->prio_sche_[ObDagPrio::DAG_PRIO_COMPACTION_MID].limits_);
  
  const int64_t dag_cnt = 3;
  ObLSID ls_id(1001);
  bool finish_flag[dag_cnt] = {false, false, false};
  for (int64_t idx = 0; idx < dag_cnt; ++idx) {
    TestCompMidCancelDag *dag = nullptr;
    LoopWaitTask *wait_task = nullptr;
    ObTabletID tablet_id(200001 + idx);
    EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag));
    dag->max_concurrent_task_cnt_ = 2;
    dag->ls_id_ = ls_id;
    dag->tablet_id_ = tablet_id;
    EXPECT_EQ(OB_SUCCESS, alloc_task(*dag, wait_task));
    EXPECT_EQ(OB_SUCCESS, wait_task->init(1, 10 /*cnt*/, finish_flag[idx]));
    EXPECT_EQ(OB_SUCCESS, dag->add_task(*wait_task));
    EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
  }
  CHECK_EQ_UTIL_TIMEOUT(6, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  
  AtomicOperator op(0);
  TestDag *dag1 = nullptr;
  AtomicIncTask *inc_task = nullptr;
  AtomicIncTask *inc_task1 = nullptr;
  AtomicMulTask *mul_task = nullptr;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, mul_task));
  EXPECT_EQ(OB_SUCCESS, mul_task->init(1, 4, op));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task1));
  EXPECT_EQ(OB_SUCCESS, inc_task1->init(1, 10, op));
  EXPECT_EQ(OB_SUCCESS, mul_task->add_child(*inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task1->add_child(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*mul_task));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  CHECK_EQ_UTIL_TIMEOUT(170, op.value());

  int64_t start_time = oceanbase::common::ObTimeUtility::current_time();
  while (oceanbase::common::ObTimeUtility::current_time() - start_time < CHECK_TIMEOUT) {
    const int64_t cnt = scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID);
    EXPECT_LE(cnt, MAX_MINOR_THREAD_CNT);
    if (cnt == 6) {
      break;
    } else {
      usleep(10 * 1000 /*10 ms*/);
    }
  }
  EXPECT_EQ(6, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
  finish_flag[0] = true;
  finish_flag[1] = true;
  finish_flag[2] = true;
  wait_scheduler();
  EXPECT_EQ(0, scheduler->get_running_task_cnt(ObDagPrio::DAG_PRIO_COMPACTION_MID));
}

#ifdef OB_BUILD_SHARED_STORAGE
class TestSSFailGCPriorityDag final : public TestDag
{
public:
  TestSSFailGCPriorityDag() : TestDag(ObDagType::DAG_TYPE_SS_FAIL_GC) {}
};

class TestSSLockRowGCPriorityDag final : public TestDag
{
public:
  TestSSLockRowGCPriorityDag()
    : TestDag(ObDagType::DAG_TYPE_SS_LOCK_ROW_GC) {}
};

class TestSSLSMetaGCPriorityDag final : public TestDag
{
public:
  TestSSLSMetaGCPriorityDag()
    : TestDag(ObDagType::DAG_TYPE_SS_LS_META_GC) {}
};

class TestSSMarkSweepPriorityDag final : public TestDag
{
public:
  TestSSMarkSweepPriorityDag()
    : TestDag(ObDagType::DAG_TYPE_MARK_SWEEP) {}
};

class TestSSDataCheckPriorityDag final : public TestDag
{
public:
  TestSSDataCheckPriorityDag()
    : TestDag(ObDagType::DAG_TYPE_SS_DATA_CHECK) {}
};

class TestSSGCExecPriorityDag final : public TestDag
{
public:
  TestSSGCExecPriorityDag()
    : TestDag(ObDagType::DAG_TYPE_SS_GC_EXEC) {}
};

template <typename DagType>
int add_ss_gc_priority_test_dag(
    ObTenantDagScheduler &scheduler,
    const int64_t dag_id,
    bool &finish_flag)
{
  int ret = OB_SUCCESS;
  DagType *dag = nullptr;
  LoopWaitTask *task = nullptr;
  bool scheduler_owned = false;
  if (OB_FAIL(scheduler.alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to allocate shared gc priority test dag",
        K(ret), K(dag_id));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "shared gc priority test dag is null", K(ret),
        K(dag_id));
  } else if (OB_FAIL(dag->init(dag_id))) {
    COMMON_LOG(WARN, "failed to init shared gc priority test dag", K(ret),
        K(dag_id));
  } else if (OB_FAIL(alloc_task(*dag, task))) {
    COMMON_LOG(WARN, "failed to allocate shared gc priority test task",
        K(ret), K(dag_id));
  } else if (OB_FAIL(task->init(1, 1, finish_flag))) {
    COMMON_LOG(WARN, "failed to init shared gc priority test task", K(ret),
        K(dag_id));
  } else if (OB_FAIL(dag->add_task(*task))) {
    COMMON_LOG(WARN, "failed to add shared gc priority test task", K(ret),
        K(dag_id));
  } else if (OB_FAIL(scheduler.add_dag(dag))) {
    // add_dag() 可能已插入 DAG，但在唤醒调度器时失败。四条调度通道的
    // limit 均为 0 时，已插入 DAG 仍会挂在调度器中，其所有权已转移。
    scheduler_owned = OB_NOT_NULL(dag->get_prev())
        || OB_NOT_NULL(dag->get_next());
    COMMON_LOG(WARN, "failed to add shared gc priority test dag", K(ret),
        K(dag_id), K(scheduler_owned));
  } else {
    scheduler_owned = true;
  }

  if (OB_SUCCESS != ret && OB_NOT_NULL(dag) && !scheduler_owned) {
    scheduler.free_dag(*dag);
  }
  return ret;
}

TEST_F(TestDagScheduler, test_ss_gc_priority_lanes)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_FAIL_GC].init_dag_prio_);
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_LOCK_ROW_GC].init_dag_prio_);
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_GEN,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_LS_META_GC].init_dag_prio_);
  ASSERT_EQ(3, OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_GC_GEN].score_);
  ASSERT_EQ(ObDagPrio::DAG_PRIO_MARK_SWEEP,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_MARK_SWEEP].init_dag_prio_);
  ASSERT_EQ(1, OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_MARK_SWEEP].score_);
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_DATA_CHECK,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_DATA_CHECK].init_dag_prio_);
  ASSERT_EQ(3, OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_DATA_CHECK].score_);
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_GC_EXEC,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_GC_EXEC].init_dag_prio_);
  ASSERT_EQ(1, OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_GC_EXEC].score_);

  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, 0));
  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_MARK_SWEEP, 0));
  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, 0));
  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_EXEC, 0));

  bool *finish_flags = new bool[6]();
  ASSERT_NE(nullptr, finish_flags);
  const int fail_add_ret = add_ss_gc_priority_test_dag<
      TestSSFailGCPriorityDag>(*scheduler, 1001, finish_flags[0]);
  const int lock_add_ret = add_ss_gc_priority_test_dag<
      TestSSLockRowGCPriorityDag>(*scheduler, 1002, finish_flags[1]);
  const int ls_meta_add_ret = add_ss_gc_priority_test_dag<
      TestSSLSMetaGCPriorityDag>(*scheduler, 1003, finish_flags[2]);
  const int mark_sweep_add_ret = add_ss_gc_priority_test_dag<
      TestSSMarkSweepPriorityDag>(*scheduler, 1004, finish_flags[3]);
  const int gc_exec_add_ret = add_ss_gc_priority_test_dag<
      TestSSGCExecPriorityDag>(*scheduler, 1005, finish_flags[4]);
  const int data_check_add_ret = add_ss_gc_priority_test_dag<
      TestSSDataCheckPriorityDag>(*scheduler, 1006, finish_flags[5]);
  const bool all_dags_added = OB_SUCCESS == fail_add_ret
      && OB_SUCCESS == lock_add_ret
      && OB_SUCCESS == ls_meta_add_ret
      && OB_SUCCESS == mark_sweep_add_ret
      && OB_SUCCESS == gc_exec_add_ret
      && OB_SUCCESS == data_check_add_ret;

  const int gen_limit_ret = scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, 1);
  const int mark_sweep_limit_ret = scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_MARK_SWEEP, 1);
  const int data_check_limit_ret = scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, 1);
  const int gc_exec_limit_ret = scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_EXEC, 1);
  const bool all_limits_enabled = OB_SUCCESS == gen_limit_ret
      && OB_SUCCESS == mark_sweep_limit_ret
      && OB_SUCCESS == gc_exec_limit_ret
      && OB_SUCCESS == data_check_limit_ret;
  scheduler->notify();

  if (all_dags_added && all_limits_enabled) {
    CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_SS_GC_GEN));
    CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_MARK_SWEEP));
    CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_SS_DATA_CHECK));
    CHECK_EQ_UTIL_TIMEOUT(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_SS_GC_EXEC));
    EXPECT_EQ(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_SS_GC_GEN));
    EXPECT_EQ(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_MARK_SWEEP));
    EXPECT_EQ(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_SS_DATA_CHECK));
    EXPECT_EQ(1, scheduler->get_running_task_cnt(
        ObDagPrio::DAG_PRIO_SS_GC_EXEC));
    EXPECT_EQ(4, scheduler->get_total_running_task_cnt());
    EXPECT_EQ(1, scheduler->get_dag_count(
        ObDagType::DAG_TYPE_SS_FAIL_GC));
    EXPECT_EQ(1, scheduler->get_dag_count(
        ObDagType::DAG_TYPE_SS_LOCK_ROW_GC));
    EXPECT_EQ(1, scheduler->get_dag_count(
        ObDagType::DAG_TYPE_SS_LS_META_GC));
    EXPECT_EQ(1, scheduler->get_dag_count(
        ObDagType::DAG_TYPE_MARK_SWEEP));
    EXPECT_EQ(1, scheduler->get_dag_count(
        ObDagType::DAG_TYPE_SS_DATA_CHECK));
    EXPECT_EQ(1, scheduler->get_dag_count(
        ObDagType::DAG_TYPE_SS_GC_EXEC));
  }

  for (int64_t i = 0; i < 6; ++i) {
    ATOMIC_STORE(&finish_flags[i], true);
  }
  scheduler->notify();
  CHECK_EQ_UTIL_TIMEOUT(true, scheduler->is_empty());
  const bool scheduler_drained = scheduler->is_empty();
  // Task 借用 finish_flags，释放测试状态前需要先销毁 Scheduler；
  // fixture teardown 中的第二次 destroy 是空操作。
  scheduler->destroy();
  delete [] finish_flags;
  finish_flags = nullptr;

  EXPECT_EQ(OB_SUCCESS, fail_add_ret);
  EXPECT_EQ(OB_SUCCESS, lock_add_ret);
  EXPECT_EQ(OB_SUCCESS, ls_meta_add_ret);
  EXPECT_EQ(OB_SUCCESS, mark_sweep_add_ret);
  EXPECT_EQ(OB_SUCCESS, gc_exec_add_ret);
  EXPECT_EQ(OB_SUCCESS, data_check_add_ret);
  EXPECT_EQ(OB_SUCCESS, gen_limit_ret);
  EXPECT_EQ(OB_SUCCESS, mark_sweep_limit_ret);
  EXPECT_EQ(OB_SUCCESS, gc_exec_limit_ret);
  EXPECT_EQ(OB_SUCCESS, data_check_limit_ret);
  EXPECT_TRUE(scheduler_drained);
}

TEST_F(TestDagScheduler, test_auto_adaptive_uses_actual_priority)
{
  ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
  DEFER(ObClockGenerator::destroy());
  ASSERT_EQ(OB_SUCCESS, ObTenantConfigMgr::get_instance().add_tenant_config(MTL_ID()));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_NE(nullptr, scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->stop();
  scheduler->wait();
  scheduler->dump_dag_status(true);

  const auto upload_prio = ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD;
  const auto minor_prio = ObDagPrio::DAG_PRIO_COMPACTION_MID;
  ObDagPrioScheduler &upload_scheduler = scheduler->prio_sche_[upload_prio];
  int64_t counter = 1;
  TestCompMidDag *dags[3] = {nullptr, nullptr, nullptr};
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, scheduler->alloc_dag_with_priority(upload_prio, dags[i]));
    ASSERT_EQ(OB_SUCCESS, dags[i]->init(i + 1));
    TestMulTask *task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dags[i]->alloc_task(task));
    ASSERT_EQ(OB_SUCCESS, task->init(&counter));
    ASSERT_EQ(OB_SUCCESS, dags[i]->add_task(*task));
    ASSERT_EQ(OB_SUCCESS, scheduler->add_dag(dags[i]));
  }
  EXPECT_EQ(3, scheduler->get_type_dag_cnt(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(3, upload_scheduler.get_dag_count());
  EXPECT_EQ(0, scheduler->prio_sche_[minor_prio].get_dag_count());

  {
    common::SpinWLockGuard guard(upload_scheduler.prio_rwlock_);
    ASSERT_EQ(OB_SUCCESS, upload_scheduler.move_dag_to_list_(
        *dags[2], dags[2]->get_list_idx(), WAITING_DAG_LIST));
    for (int64_t i = 0; i < 2; ++i) {
      bool move_to_waiting = false;
      ASSERT_EQ(OB_SUCCESS, upload_scheduler.schedule_dag_(*dags[i], move_to_waiting));
    }
  }
  // Include waiting DAGs; preserve type statistics while charging the actual queue.
  EXPECT_EQ(3, upload_scheduler.get_dag_count());
  EXPECT_EQ(2, scheduler->get_scheduled_dag_cnts(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(2, scheduler->get_scheduled_dag_cnts_by_prio(upload_prio));
  EXPECT_EQ(0, scheduler->get_scheduled_dag_cnts_by_prio(minor_prio));

  scheduler->adapt_ss_work_thread();
  usleep(11 * 1000 * 1000);
  scheduler->adapt_ss_work_thread();
  int64_t adaptive_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(upload_prio, adaptive_limit));
  EXPECT_EQ(scheduler->get_ss_total_thread_num_() - 3, adaptive_limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(minor_prio, adaptive_limit));
  EXPECT_EQ(1, adaptive_limit);

  scheduler->dump_dag_status(true);
  EXPECT_EQ(0, scheduler->get_scheduled_dag_cnts_by_prio(upload_prio));
  EXPECT_EQ(0, scheduler->get_scheduled_dag_cnts(ObDagType::DAG_TYPE_MERGE_EXECUTE));
  EXPECT_EQ(3, upload_scheduler.get_dag_count());
  scheduler->destroy();
}

TEST_F(TestDagScheduler, test_auto_adaptive)
{
  EXPECT_EQ(OB_SUCCESS, ObClockGenerator::init());
  EXPECT_EQ(OB_SUCCESS, ObTenantConfigMgr::get_instance().add_tenant_config(MTL_ID()));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ASSERT_TRUE(tenant_config.is_valid());
  ASSERT_TRUE(tenant_config->_ob_enable_background_thread_auto_adapt);
  const int64_t generator_score =
      tenant_config->shared_gc_generator_thread_score;
  const int64_t executor_score =
      tenant_config->shared_gc_executor_thread_score;
  const int64_t data_check_score =
      tenant_config->shared_storage_data_check_thread_score;
  const int64_t expected_mark_sweep_limit = 0 == generator_score
      ? OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_MARK_SWEEP].score_
      : generator_score;
  const int64_t expected_generator_limit = 0 == generator_score
      ? OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_GC_GEN].score_
      : generator_score;
  const int64_t expected_executor_limit = 0 == executor_score
      ? OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_GC_EXEC].score_
      : executor_score;
  const int64_t expected_data_check_limit = 0 == data_check_score
      ? OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_DATA_CHECK].score_
      : data_check_score;
  const int64_t mark_sweep_poison = 1 == expected_mark_sweep_limit ? 2 : 1;
  const int64_t generator_poison = 1 == expected_generator_limit ? 2 : 1;
  const int64_t executor_poison = 1 == expected_executor_limit ? 2 : 1;
  const int64_t data_check_poison = 1 == expected_data_check_limit ? 2 : 1;
  ASSERT_EQ(OB_SUCCESS, scheduler->set_thread_score(
      ObDagPrio::DAG_PRIO_MARK_SWEEP, mark_sweep_poison));
  ASSERT_EQ(OB_SUCCESS, scheduler->set_thread_score(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, generator_poison));
  ASSERT_EQ(OB_SUCCESS, scheduler->set_thread_score(
      ObDagPrio::DAG_PRIO_SS_GC_EXEC, executor_poison));
  ASSERT_EQ(OB_SUCCESS, scheduler->set_thread_score(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, data_check_poison));
  int64_t old_compaction_high_adaptive_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_COMPACTION_HIGH,
      old_compaction_high_adaptive_limit));
  const int64_t compaction_high_poison =
      1 == old_compaction_high_adaptive_limit ? 2 : 1;
  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_COMPACTION_HIGH, compaction_high_poison));
  scheduler->ss_reload_config(false /* force_reload */);
  int64_t limit = 0;
  int64_t adaptive_limit = 0;
  ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(
      ObDagPrio::DAG_PRIO_MARK_SWEEP, limit));
  ASSERT_EQ(expected_mark_sweep_limit, limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_MARK_SWEEP, adaptive_limit));
  ASSERT_EQ(expected_mark_sweep_limit, adaptive_limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, limit));
  ASSERT_EQ(expected_generator_limit, limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_GEN, adaptive_limit));
  ASSERT_EQ(expected_generator_limit, adaptive_limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(
      ObDagPrio::DAG_PRIO_SS_GC_EXEC, limit));
  ASSERT_EQ(expected_executor_limit, limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_GC_EXEC, adaptive_limit));
  ASSERT_EQ(expected_executor_limit, adaptive_limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, limit));
  ASSERT_EQ(expected_data_check_limit, limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_SS_DATA_CHECK, adaptive_limit));
  ASSERT_EQ(expected_data_check_limit, adaptive_limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(
      ObDagPrio::DAG_PRIO_COMPACTION_HIGH, adaptive_limit));
  ASSERT_EQ(compaction_high_poison, adaptive_limit);
  ASSERT_EQ(OB_SUCCESS, scheduler->set_adaptive_limit(
      ObDagPrio::DAG_PRIO_COMPACTION_HIGH,
      old_compaction_high_adaptive_limit));
  scheduler->stop();
  bool finish_flag[6] = {false, false, false, false, false, false};
  int32_t concurrency = 1;
  LoopWaitTask *inc_task = NULL;

  TestMemRelatedDag *dag1 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, dag1->init(1));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag1, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[0]));
  EXPECT_EQ(OB_SUCCESS, dag1->add_task(*inc_task));

  TestSSUploadDag *dag2 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, dag2->init(2));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag2, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[1]));
  EXPECT_EQ(OB_SUCCESS, dag2->add_task(*inc_task));

  TestSSUploadDag *dag3 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, dag3->init(3));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag3, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[2]));
  EXPECT_EQ(OB_SUCCESS, dag3->add_task(*inc_task));

  TestSSAttachDag *dag4 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag4));
  EXPECT_EQ(OB_SUCCESS, dag4->init(4));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag4, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[3]));
  EXPECT_EQ(OB_SUCCESS, dag4->add_task(*inc_task));

  TestCompMidDag *dag5 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag5));
  EXPECT_EQ(OB_SUCCESS, dag5->init(5));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag5, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[4]));
  EXPECT_EQ(OB_SUCCESS, dag5->add_task(*inc_task));

  TestCompMidDag *dag6 = NULL;
  EXPECT_EQ(OB_SUCCESS, scheduler->alloc_dag(dag6));
  EXPECT_EQ(OB_SUCCESS, dag6->init(6));
  EXPECT_EQ(OB_SUCCESS, alloc_task(*dag6, inc_task));
  EXPECT_EQ(OB_SUCCESS, inc_task->init(1, concurrency, finish_flag[5]));
  EXPECT_EQ(OB_SUCCESS, dag6->add_task(*inc_task));

  // default thread num is 24
  EXPECT_EQ(24, scheduler->get_ss_total_thread_num_());
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag1));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag2));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag3));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag4));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag5));
  EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag6));

  if (!tenant_config.is_valid()) {
    COMMON_LOG(INFO, "tenant config invalid");
  } else if (!tenant_config->_ob_enable_background_thread_auto_adapt) {
    COMMON_LOG(INFO, "_ob_enable_background_thread_auto_adapt not enabled");
  } else {
    COMMON_LOG(INFO, "auto adapt ss thread");
  }
  scheduler->adapt_ss_work_thread();
  usleep(11 * 1000 * 1000);  // 11 s
  scheduler->adapt_ss_work_thread();
  scheduler->dump_dag_status();
  int64_t curr_limit = 0;
  EXPECT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(ObDagPrio::DAG_PRIO_COMPACTION_HIGH, curr_limit));
  EXPECT_EQ(6, curr_limit);
  EXPECT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(ObDagPrio::DAG_PRIO_COMPACTION_MID, curr_limit));
  EXPECT_EQ(7, curr_limit);
  EXPECT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(ObDagPrio::DAG_PRIO_INC_SSTABLE_UPLOAD, curr_limit));
  EXPECT_EQ(7, curr_limit);
  EXPECT_EQ(OB_SUCCESS, scheduler->get_adaptive_limit(ObDagPrio::DAG_PRIO_LINK_SHARED_SSTABLE, curr_limit));
  EXPECT_EQ(4, curr_limit);
}
#endif
/*
TEST_F(TestDagScheduler, test_large_thread_cnt)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  const int64_t cpu_cnt = 128;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt;
  scheduler->get_default_config(cpu_cnt);
  scheduler->dump_dag_status();
  const int64_t max_concurrency = 100;
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(max_concurrency, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  const int64_t max_concurrency_2 = 10;
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_2 = 64;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_2;
  scheduler->get_default_config(cpu_cnt_2);
  scheduler->dump_dag_status();
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(cpu_cnt_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_3 = 512;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_3;
  scheduler->get_default_config(cpu_cnt_3);
  scheduler->dump_dag_status();
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(max_concurrency, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  const int64_t max_concurrency_3 = 256;
  scheduler->set_major_merge_concurrency(max_concurrency_3);
  EXPECT_EQ(max_concurrency_3, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_3 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  scheduler->set_major_merge_concurrency(0);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MAJOR_MERGE]
      , scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_LOW_LIMIT[ObDagPrio::DAG_PRIO_COMPACTION_LOW],
      scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();
}

TEST_F(TestDagScheduler, test_large_thread_cnt_2)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);

  const int64_t cpu_cnt = 256;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt;
  scheduler->get_default_config(cpu_cnt);
  scheduler->dump_dag_status();
  int64_t max_concurrency = 500;
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(cpu_cnt, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  int64_t max_concurrency_2 = 10;
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_2 = 64;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_2;
  scheduler->get_default_config(cpu_cnt_2);
  scheduler->dump_dag_status();
  max_concurrency = 200;
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(cpu_cnt_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(cpu_cnt_2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();

  const int64_t cpu_cnt_3 = 512;
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));
  scheduler->cpu_cnt_ = cpu_cnt_3;
  scheduler->get_default_config(cpu_cnt_3);
  scheduler->dump_dag_status();
  scheduler->set_major_merge_concurrency(max_concurrency);
  EXPECT_EQ(max_concurrency, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->set_major_merge_concurrency(max_concurrency_2);
  EXPECT_EQ(max_concurrency_2, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_2 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  const int64_t max_concurrency_3 = 800;
  scheduler->set_major_merge_concurrency(max_concurrency_3);
  EXPECT_EQ(cpu_cnt_3, scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(max_concurrency_3 / 2, scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);

  scheduler->set_major_merge_concurrency(0);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_UP_LIMIT[ObIDag::DAG_ULT_MAJOR_MERGE]
      , scheduler->up_limits_[ObIDag::DAG_ULT_MAJOR_MERGE]);
  EXPECT_EQ(ObTenantDagScheduler::DEFAULT_LOW_LIMIT[ObDagPrio::DAG_PRIO_COMPACTION_LOW],
      scheduler->low_limits_[ObDagPrio::DAG_PRIO_COMPACTION_LOW]);
  scheduler->destroy();
}
*/

class TestSleepAfterMulTask : public TestMulTask
{
public:
  TestSleepAfterMulTask() = default;
  ~TestSleepAfterMulTask() = default;

  virtual int process() {
    *counter_ = *counter_ * 2;
    ::usleep(sleep_us_);
    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("ObITask", ObITask, "type", "SleepAfterMulTask", KP_(counter));
private:
  DISALLOW_COPY_AND_ASSIGN(TestSleepAfterMulTask);
};

TEST_F(TestDagScheduler, test_cancel_task)
{
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler*);
  ASSERT_TRUE(nullptr != scheduler);
  ASSERT_EQ(OB_SUCCESS, scheduler->init(MTL_ID(), time_slice));

  int ret = OB_SUCCESS;
  TestDag *dag = NULL;
  int64_t counter = 1;
  if (OB_FAIL(scheduler->alloc_dag(dag))) {
    COMMON_LOG(WARN, "failed to alloc dag");
  } else if (NULL == dag) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "dag is null", K(ret));
  } else if (OB_FAIL(dag->init(1))) {
    COMMON_LOG(WARN, "failed to init dag", K(ret));
  } else {
    TestAddTask *add_task = NULL;
    TestSleepAfterMulTask *mul_task = NULL;
    if (OB_FAIL(dag->alloc_task(mul_task))) {
      COMMON_LOG(WARN, "failed to alloc task", K(ret));
    } else if (NULL == mul_task) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "task is null", K(ret));
    } else {
      if (OB_FAIL(mul_task->init(&counter, 10 * 1000 * 1000))) {
        COMMON_LOG(WARN, "failed to init mul task", K(ret));
      } else if (OB_FAIL(dag->alloc_task(add_task))) {
        COMMON_LOG(WARN, "failed to alloc task", K(ret));
      } else if (NULL == add_task) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "task is null", K(ret));
      } else {
        if (OB_FAIL(add_task->init(&counter, 1, 0, 0))) {
          COMMON_LOG(WARN, "failed to init add task", K(ret));
        } else if (OB_FAIL(mul_task->add_child(*add_task))) {
          COMMON_LOG(WARN, "failed to add child", K(ret));
        } else if (OB_FAIL(dag->add_task(*add_task))) {
          COMMON_LOG(WARN, "failed to add task");
        } else if (OB_FAIL(dag->add_task(*mul_task))) {
          COMMON_LOG(WARN, "failed to add task", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      EXPECT_EQ(OB_SUCCESS, scheduler->add_dag(dag));
      //wait mul task running
      while (true) {
        if (ATOMIC_LOAD(&counter) == 2) {
          break;
        }
      }
      EXPECT_EQ(OB_SUCCESS, scheduler->cancel_task(dag, ObITask::TASK_TYPE_UT));
      EXPECT_EQ(counter, 2);
    }
  }
}

}
}

void parse_cmd_arg(int argc, char **argv)
{
  int opt = 0;
  const char *opt_string = "p:s:l:";

  struct option longopts[] = {
      {"dag cnt for performance test", 1, NULL, 'p'},
      {"stress test time", 1, NULL, 's'},
      {"log level", 1, NULL, 'l'},
      {0,0,0,0} };

  while (-1 != (opt = getopt_long(argc, argv, opt_string, longopts, NULL))) {
    switch(opt) {
    case 'p':
      dag_cnt = strtoll(optarg, NULL, 10);
      break;
    case 's':
      stress_time = strtoll(optarg, NULL, 10);
      break;
    case 'l':
      snprintf(log_level, 20, "%s", optarg);
      break;
    default:
      break;
    }
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  parse_cmd_arg(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_max_file_size(256*1024*1024);
  system("rm -f test_dag_scheduler.log*");
  OB_LOGGER.set_file_name("test_dag_scheduler.log", true);
  return RUN_ALL_TESTS();
}
