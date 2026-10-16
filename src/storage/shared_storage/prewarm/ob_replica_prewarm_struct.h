/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_REPLICA_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_REPLICA_PREWARM_STRUCT_H_

#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase
{
namespace storage
{

class ObReplicaPrewarmHandler : public lib::TGTaskHandler
{
public:
  ObReplicaPrewarmHandler();
  virtual ~ObReplicaPrewarmHandler();
  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;

public:
  static const int64_t MIN_THREAD_COUNT = 1;
  static const int64_t MAX_THREAD_COUNT = 5;
  static const int64_t MAX_TASK_NUM = 128;

private:
  static const int64_t INVALID_TG_ID = -1;
  bool is_inited_;
  int tg_id_;
};

class ObLSPrewarmManager
{
public:
  static ObLSPrewarmManager &get_instance();
  int init();
  void stop();
  void wait();
  void destroy();

private:
  ObLSPrewarmManager();
  ~ObLSPrewarmManager();

private:
  bool is_inited_;
  volatile bool is_stop_;
};

#define OB_LS_PREWARM_MGR (oceanbase::storage::ObLSPrewarmManager::get_instance())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_REPLICA_PREWARM_STRUCT_H_ */
