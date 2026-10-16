/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_CLUSTER_INFO_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_CLUSTER_INFO_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_ss_common_header.h"
#include "storage/shared_storage/ob_ss_base_format.h"
#include "share/backup/ob_backup_struct.h"
#include "common/ob_region.h"

namespace oceanbase
{
namespace storage
{

struct ObSSClusterInfoBody final
{
public:
  static const int32_t SS_CLUSTER_INFO_VERSION = 1;
  const char *const VERSION_ID_STR = "version_id=";
  const char *const LOGSERVICE_CLUSTER_ID_STR = "logservice_cluster_id=";
  const char *const CLUSTER_REGION_STR = "cluster_region=";
  ObSSClusterInfoBody();
  ~ObSSClusterInfoBody() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(version),
               K_(logservice_cluster_id),
               K_(cluster_region));
  OB_UNIS_VERSION(SS_CLUSTER_INFO_VERSION);
public:
  int64_t version_;
  uint64_t logservice_cluster_id_;
  common::ObRegion cluster_region_;
};

struct ObSSClusterInfo final : public ObSSBaseFormat<ObSSClusterInfoBody>
{
public:
  ObSSClusterInfo();
  ~ObSSClusterInfo() = default;
  int init(const uint64_t logservice_cluster_id, const common::ObRegion &cluster_region);
};

class ObSSClusterInfoUtil : public ObSSBaseUtil<ObSSClusterInfo>
{
public:
  static int write_ss_cluster_info(const share::ObBackupDest &storage_dest,
                                   const ObSSClusterInfo &ss_cluster_info);
  static int read_ss_cluster_info(const share::ObBackupDest &storage_dest,
                                  ObSSClusterInfo &ss_cluster_info);
  static int is_exist_ss_cluster_info(const share::ObBackupDest &storage_dest,
                                      bool &is_exist);
  static int get_ss_cluster_info_path(const share::ObBackupDest &storage_dest,
                                      char *path, const int64_t length);
private:
  static const char *const SS_CLUSTER_INFO_FILE_NAME;
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_CLUSTER_INFO_H_ */
