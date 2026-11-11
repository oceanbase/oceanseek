/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_UPLOAD_SESSION_MGR_H_
#define SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_UPLOAD_SESSION_MGR_H_

#include "lib/atomic/ob_atomic.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}
namespace sql {

struct ObODPSGeneralFormat;

// One SQC owns a session manager shared by all of its ODPS write tasks.
// Initialization precedes task execution; destruction follows task shutdown.
class ObOdpsUploadSessionMgr
{
public:
  enum class ConnectorType { CPP, JNI };

  static int create(common::ObIAllocator &allocator, bool use_jni,
                    ObOdpsUploadSessionMgr *&mgr);
  virtual ~ObOdpsUploadSessionMgr() = default;

  int init(const ObODPSGeneralFormat &format, const common::ObString &partition,
           bool overwrite, int64_t task_count);
  // Called once per task, including tasks whose open/write/close failed.
  int finish_task(bool succeeded);
  // Release resources without committing, including partial initialization.
  void reset();

  ConnectorType get_connector_type() const { return connector_type_; }
  bool is_inited() const { return inited_; }

protected:
  explicit ObOdpsUploadSessionMgr(ConnectorType connector_type)
      : connector_type_(connector_type), inited_(false),
        remaining_tasks_(0), need_commit_(true)
  {}
  virtual int init_impl(const ObODPSGeneralFormat &format,
                       const common::ObString &partition,
                       bool overwrite, int64_t task_count) = 0;
  virtual int finalize_impl(bool can_commit) = 0;
  virtual void release_resources() = 0;

private:
  const ConnectorType connector_type_;
  bool inited_;
  int64_t remaining_tasks_;
  bool need_commit_;
  DISALLOW_COPY_AND_ASSIGN(ObOdpsUploadSessionMgr);
};

} // sql
} // oceanbase
#endif // SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_UPLOAD_SESSION_MGR_H_
