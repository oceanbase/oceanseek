// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_SS_INC_CHECKPOINT_H_
#define OB_SS_INC_CHECKPOINT_H_

#include "share/scn.h"

namespace oceanbase {
namespace logservice {
class ObICheckpointSubHandler;
}

namespace storage {
class ObLS;

enum SSIncCheckpointType {
  SS_INC_META = 0,
  SS_INC_SSTABLE,
  MAX_SS_INC_CHECKPOINT_TYPE
};

enum SSIncSSTableType { 
  MINI_SSTABLE = 0,       // for dump normal mini sstable
  MDS_SSTABLE,            // for dump mds sstable
  DDL_SSTABLE,            // for dump ddl sstable
  ANY_INC_SSTABLE,        // for restart or migrate
  INC_MAJOR_DDL_SSTABLE,  // for inc major ddl dump sstable
  MAX_INC_SSTABLE_TYPE,
};

enum SSIncMetaType {
  GC_HANDLER = 0,
  MAX_COMMON_META_TYPE,

  // exists on sys logstreams of all tenants
  GTS,
  TX_IDS,
  MAX_ALL_SYS_LS_META_TYPE,

  // only exists on sys logstreams of sys/meta tenant
  DAS_IDS,
  MAX_NON_SSLOG_LS_META_TYPE,
  // only for sslog
  SSLOG_GTS, // 7  // FARM COMPAT WHITELIST
  SSLOG_UID, // 8  // FARM COMPAT WHITELIST
  MAX_INC_META_TYPE
};

enum SSIncMetaUploadType {
  GC_HANDLER_UPLOAD_TYPE = 0,
  ID_SERVICE_UPLOAD_TYPE,
  MAX_META_UPLOAD_TYPE
};

#define ASYNC_UPLOAD_INC_SSTABLE(INC_SST_TYPE, REG_HANDLE, ...)                          \
  do {                                                                                         \
    STATIC_ASSERT(INC_SST_TYPE >= SSIncSSTableType::MINI_SSTABLE, "invalid ckpt type");        \
    STATIC_ASSERT(INC_SST_TYPE < SSIncSSTableType::MAX_INC_SSTABLE_TYPE, "invalid ckpt type"); \
    (void)REG_HANDLE.do_register(__VA_ARGS__);                                                   \
  } while (0)

#define SYNC_UPLOAD_INC_META_WITH_RET(META_UPLOAD_TYPE, ret, META_FUNC_NAME, META_SERVICE, ls_id, ...)          \
  do {                                                                                                          \
    STATIC_ASSERT(META_UPLOAD_TYPE >= SSIncMetaUploadType::GC_HANDLER_UPLOAD_TYPE, "invalid ckpt type");        \
    STATIC_ASSERT(META_UPLOAD_TYPE < SSIncMetaUploadType::MAX_META_UPLOAD_TYPE, "invalid ckpt type");           \
    ObSSWriterKey key(ObSSWriterType::META,                                                                     \
                      ls_id);                                                                                   \
    bool is_sswriter = false;                                                                                   \
    int64_t epoch = -1;                                                                                         \
    if (OB_FAIL(ret)) {                                                                                         \
    } else if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, is_sswriter, epoch))) {                        \
      STORAGE_LOG(WARN, "check lease failed", KR(ret), K(key), K(is_sswriter), K(epoch));                       \
    } else if (is_sswriter) {                                                                                   \
      ret = META_SERVICE.update_##META_FUNC_NAME(ls_id,  __VA_ARGS__);                                          \
    } else {                                                                                                    \
      STORAGE_LOG(INFO, "I am not sswriter, do nothing", K(ls_id));                                             \
    }                                                                                                           \
  } while (0)

class ObSSIncCheckpoint {
public:
  ObSSIncCheckpoint();
  virtual share::SCN get_ss_rec_scn() = 0;
  static const char *get_inc_ckpt_string(int64_t type_idx);
};

enum SSIncMetaTenantType {
  SYS_OR_META_TENANT_TYPE = 0,
  USER_TENANT_TYPE = 1,
  MAX_TENANT_TYPE = 2,
};

enum SSIncMetaLSType {
  SYS_LS_TYPE = 0,
  USER_LS_TYPE = 1,
  MAX_LS_TYPE = 2,
};

}  // namespace storage
}  // namespace oceanbase
#endif
