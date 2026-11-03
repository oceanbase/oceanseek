/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include "lib/allocator/ob_malloc.h"
#include "lib/queue/ob_priority_queue.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace std;

class TestQueue: public ThreadPool
{
public:
  enum { BATCH = 64 };
  struct QData: public ObLink
  {
    QData(): val_(0) {}
    QData(int64_t x): val_(x) {}
    ~QData() {}
    int64_t val_;
  };
  typedef ObPriorityQueue2<1, 2> Queue;
  TestQueue(): push_seq_(0), pop_seq_(0) {
    limit_ = atoll(getenv("limit")?: "100000");
  }
  virtual ~TestQueue() {}
  void do_stress()
  {
    set_thread_count(atoi(getenv("n_thread")?: "8"));
    n_pusher_ = atoi(getenv("n_pusher")?: "4");
    queue_.set_limit(65536);
    int ret = OB_SUCCESS;
    if (OB_FAIL(start())) {
      LIB_LOG(ERROR, "start fail", K(ret), K(errno));
      exit(-1);
    }
    print();
    wait();
  }
  void print()
  {
    int64_t last_seq = ATOMIC_LOAD(&pop_seq_);
    while(ATOMIC_LOAD(&pop_seq_) < limit_) {
      sleep(1);
      int64_t cur_seq = ATOMIC_LOAD(&pop_seq_);
      LIB_LOG(INFO, "queue", "tps", BATCH * (cur_seq - last_seq));
      last_seq = cur_seq;
    }
  }
  int64_t get_seq(int64_t &seq) {
    return ATOMIC_FAA(&seq, 1);
  }
  int insert(int64_t seq) {
    int err = 0;
    QData* data = new QData(seq);
    err = queue_.push(data, data->val_ % 3);
    return err;
  }
  int del(uint64_t idx) {
    int err;
    QData* data = NULL;
    if (idx == 0) {
      err = queue_.pop_high((ObLink*&)data, 10000);
    } else {
      err = queue_.pop((ObLink*&)data, 10000);
    }
    // auto now = ObTimeUtility::current_time();
    // if (data) {
    //   if (now - data->val_ > 100) {
    //     cout << now - data->val_ << endl;
    //   }
    //   usleep(500);
    // }
    delete data;
    return err;
  }

  void run1() override
  {
    int ret = OB_SUCCESS;
    int64_t seq = 0;
    const uint64_t idx = get_thread_idx();
    cout << "idx: " << idx << endl;
    if (idx >= get_thread_count() - n_pusher_) {
      while((seq = get_seq(push_seq_)) < limit_) {
        for(int i = 0; i < BATCH; i++) {
          //::usleep(10000);
          auto now = ObTimeUtility::current_time();
          do { ret = insert(now); } while (OB_FAIL(ret));
        }
      }
      stop_ = true;
    } else {
      while((seq = get_seq(pop_seq_)) < limit_) {
        for(int i = 0; i < BATCH; i++) {
          do { ret = del(idx); } while (OB_FAIL(ret) && !stop_);
        }
      }
    }
    std::cout << idx << " finished" << std::endl;
  }
private:
  int64_t push_seq_ CACHE_ALIGNED;
  int64_t pop_seq_ CACHE_ALIGNED;
  int64_t limit_;
  int64_t n_pusher_;
  Queue queue_;
  bool stop_ = false;
}; // end of class Consumer

// A signal after prepare must reach the chosen group slot, including wrapped
// indices, even if the consumer has not entered futex wait yet.
TEST(TestPriorityQueue, GroupIndexedWakeup)
{
  const int32_t indices[] = {0, SCondTemp<3>::COND_COUNT - 1,
                             SCondTemp<3>::COND_COUNT + 3};
  for (int32_t index : indices) {
    SCondTemp<3> cond(ObWaitEventIds::PRIORITY_QUEUE_COND_WAIT);
    std::atomic<bool> prepared(false);
    int wait_ret = OB_ERR_UNEXPECTED;
    std::thread consumer([&]() {
      cond.prepare(2, index);
      prepared.store(true);
      wait_ret = cond.wait(1000000);
    });
    while (!prepared.load()) {
      std::this_thread::yield();
    }
    cond.signal(1, 0, true);
    consumer.join();
    EXPECT_EQ(OB_SUCCESS, wait_ret) << "group index: " << index;
  }
}

TEST(TestPriorityQueue, WithCoro)
{
  TestQueue tq;
  tq.do_stress();
}

int main(int argc, char *argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc,argv);
  //testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
