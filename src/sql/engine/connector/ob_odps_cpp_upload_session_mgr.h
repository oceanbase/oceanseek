/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_CPP_UPLOAD_SESSION_MGR_H_
#define SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_CPP_UPLOAD_SESSION_MGR_H_

#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_tunnel.h>
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/connector/ob_odps_upload_session_mgr.h"

namespace oceanbase {
namespace sql {

class ObOdpsCppUploadSessionMgr final : public ObOdpsUploadSessionMgr
{
public:
  ObOdpsCppUploadSessionMgr() : ObOdpsUploadSessionMgr(ConnectorType::CPP) {}
  ~ObOdpsCppUploadSessionMgr() override { reset(); }

  static int create_upload_session(const ObODPSGeneralFormat &format,
                                   const common::ObString &partition, bool overwrite,
                                   apsara::odps::sdk::IUploadPtr &upload);
  int get_odps_uploader(int64_t task_id, apsara::odps::sdk::IUploadPtr &upload,
                        apsara::odps::sdk::IRecordWriterPtr &record_writer);

private:
  int init_impl(const ObODPSGeneralFormat &format, const common::ObString &partition,
                bool overwrite, int64_t task_count) override;
  int finalize_impl(bool can_commit) override;
  void release_resources() override;

  struct OdpsUploader {
    OdpsUploader() : upload_(NULL), record_writer_(NULL) {}
    ~OdpsUploader() {
      upload_.reset();
      record_writer_.reset();
    }
    apsara::odps::sdk::IUploadPtr upload_;
    apsara::odps::sdk::IRecordWriterPtr record_writer_;
  };
  typedef common::hash::ObHashMap<int64_t, int64_t,
      common::hash::SpinReadWriteDefendMode> OdpsMgrMap;
  OdpsMgrMap odps_mgr_map_;
  common::ObArenaAllocator arena_alloc_;
};

} // sql
} // oceanbase
#endif // OB_BUILD_CPP_ODPS
#endif // SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_CPP_UPLOAD_SESSION_MGR_H_
