/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_SS_MICRO_CACHE_PREWARM_SERVICE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_SS_MICRO_CACHE_PREWARM_SERVICE_H_

#include <stdint.h>
#include "storage/shared_storage/prewarm/ob_replica_prewarm_struct.h"
#include "storage/shared_storage/task/ob_mc_prewarm_level_refresh_task.h"

namespace oceanbase
{
namespace compaction
{
enum class ObSSMajorPrewarmLevel : uint8;
}
namespace storage
{

class ObSSLocalCachePrewarmService
{
public:
  static const int64_t OB_BATCH_MICRO_PREWARM_META_BUCKET_NUM = 1001;
  ObSSLocalCachePrewarmService();
  virtual ~ObSSLocalCachePrewarmService();
  static int mtl_init(ObSSLocalCachePrewarmService *&prewarm_service);
  int init(const uint64_t tenant_id);
  int start();
  void stop();
  void wait();
  void destroy();
  ObReplicaPrewarmHandler &get_replica_prewarm_handler();
  compaction::ObSSMajorPrewarmLevel get_major_prewarm_level() const;
  void set_major_prewarm_level(const compaction::ObSSMajorPrewarmLevel prewarm_level);

private:
  const static int64_t INVALID_TG_ID = -1;

private:
  bool is_inited_;
  bool is_stopped_;
  uint64_t tenant_id_;
  ObReplicaPrewarmHandler replica_prewarm_handler_;
  compaction::ObSSMajorPrewarmLevel major_prewarm_level_;
  int tg_id_;
  ObMCPrewarmLevelRefreshTask mc_prewarm_level_refresh_task_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_SS_MICRO_CACHE_PREWARM_SERVICE_H_ */
