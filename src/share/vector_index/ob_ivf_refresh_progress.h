/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_IVF_REFRESH_PROGRESS_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_IVF_REFRESH_PROGRESS_H_
#include <stdint.h>
namespace oceanbase
{
namespace share
{
// Optional, non-owning observer. Callbacks must not affect execution results.
class ObIVFRefreshProgress
{
public:
  enum Stage {
    CAPTURE, SAMPLE_MERGE, SAMPLE_LOAD, TRAIN_PREPARE, TRAIN_INIT,
    TRAIN_ITERATE, ASSIGN_ENCODE, SORT, WRITE, PUBLISH, COMMIT, STAGE_COUNT
  };
  virtual ~ObIVFRefreshProgress() = default;
  virtual void begin_stage(Stage stage) = 0;
  virtual void add_completed(Stage stage, int64_t count) = 0;
  virtual void set_total(Stage stage, int64_t count) = 0;
  virtual void iteration_finished(int64_t iteration, int64_t elapsed_us,
                                  double diff, double imbalance) = 0;
};
} // namespace share
} // namespace oceanbase
#endif
