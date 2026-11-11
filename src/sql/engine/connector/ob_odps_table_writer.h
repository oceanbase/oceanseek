/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_TABLE_WRITER_H_
#define SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_TABLE_WRITER_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObOdpsUploadSessionMgr;
struct ObEvalCtx;
class ObExpr;
class ObBatchRows;
struct ObODPSGeneralFormat;
struct ObODPSArrayHelper;

struct ObODPSWriterOptions
{
  ObODPSWriterOptions(const common::ObIArray<ObExpr *> &select_exprs,
                      const common::ObString &partition, bool overwrite, int64_t task_id)
      : select_exprs_(select_exprs), partition_(partition),
        overwrite_(overwrite), task_id_(task_id)
  {}

  const common::ObIArray<ObExpr *> &select_exprs_;
  common::ObString partition_;
  bool overwrite_;
  int64_t task_id_;
};

// Owns one worker's upload resources. PX session ownership and its commit
// barrier remain in the SQC uploader manager shared by all workers.
class ObODPSTableWriter
{
public:
  static int create(ObExecContext &ctx, ObEvalCtx &eval_ctx,
                    const ObODPSWriterOptions &options, ObODPSGeneralFormat &format,
                    ObODPSTableWriter *&writer);
  virtual ~ObODPSTableWriter();

  int open();
  int write_row();
  int write_batch(const ObBatchRows &brs);
  int close(bool execution_succeeded);
  void set_failed() { need_commit_ = false; }

protected:
  ObODPSTableWriter(ObExecContext &ctx, ObEvalCtx &eval_ctx,
                   const ObODPSWriterOptions &options, ObODPSGeneralFormat &format);
  virtual int open_impl() = 0;
  virtual int write_row_impl() = 0;
  virtual int write_batch_impl(const ObBatchRows &brs) = 0;
  virtual int close_impl() = 0;
  void release_array_helpers();

  ObExecContext &ctx_;
  ObEvalCtx &eval_ctx_;
  const ObODPSWriterOptions options_;
  ObODPSGeneralFormat &format_;
  uint32_t block_id_;
  bool need_commit_;
  ObOdpsUploadSessionMgr *upload_mgr_;
  common::ObSEArray<ObODPSArrayHelper *, 8> array_helpers_;
  static const int32_t ODPS_DATE_MIN_VAL = -719162; // '0001-1-1'

private:
  bool closed_;
  int close_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObODPSTableWriter);
};
} // namespace sql
} // namespace oceanbase

#endif // SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_TABLE_WRITER_H_
