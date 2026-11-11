/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_JNI_UPLOAD_SESSION_MGR_H_
#define SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_JNI_UPLOAD_SESSION_MGR_H_

#ifdef OB_BUILD_JNI_ODPS
#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/connector/ob_odps_jni_writer.h"
#include "sql/engine/connector/ob_odps_upload_session_mgr.h"

namespace oceanbase {
namespace sql {

class ObOdpsJniUploadSessionMgr final : public ObOdpsUploadSessionMgr
{
public:
  struct OdpsUploader {
    OdpsUploader() : writer_ptr(nullptr) {}
    JNIWriterPtr writer_ptr;
  };

  ObOdpsJniUploadSessionMgr()
      : ObOdpsUploadSessionMgr(ConnectorType::JNI),
        write_arena_alloc_("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        session_holder_ptr_(nullptr)
  {}
  ~ObOdpsJniUploadSessionMgr() override { reset(); }

  static int create_writer_params_map(common::ObIAllocator &alloc,
      const ObODPSGeneralFormat &format, const common::ObString &partition,
      bool overwrite, common::hash::ObHashMap<common::ObString, common::ObString> &params);
  int get_odps_uploader_in_px(int task_id, OdpsUploader &uploader);
  int append_block_id(long block_id);

private:
  int init_impl(const ObODPSGeneralFormat &format, const common::ObString &partition,
                bool overwrite, int64_t task_count) override;
  int finalize_impl(bool can_commit) override;
  void release_resources() override;
  int get_writer_sid(common::ObIAllocator &alloc,
      const common::hash::ObHashMap<common::ObString, common::ObString> &params,
      common::ObString &sid);

  // Construct first and release after the session holder and parameter map.
  common::ObArenaAllocator write_arena_alloc_;
  common::ObString sid_;
  common::hash::ObHashMap<common::ObString, common::ObString> odps_params_map_;
  static const int64_t MAX_PARAMS_SIZE = 16;
  JNIWriterPtr session_holder_ptr_;
};

} // sql
} // oceanbase
#endif // OB_BUILD_JNI_ODPS
#endif // SRC_SQL_ENGINE_CONNECTOR_OB_ODPS_JNI_UPLOAD_SESSION_MGR_H_
