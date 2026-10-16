/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_META_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_META_H_

#include "storage/shared_storage/ob_ss_common_header.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/macro_cache/ob_storage_disk_space_meta.h"

namespace oceanbase
{
namespace storage
{

struct ObTenantDiskSpaceMetaBody final
{
public:
  static const int32_t TENANT_DISK_SPACE_VERSION = 1;
  OB_UNIS_VERSION(TENANT_DISK_SPACE_VERSION);

public:
  ObTenantDiskSpaceMetaBody();
  ~ObTenantDiskSpaceMetaBody() = default;
  int set_macro_cache_stats();
  int set_dir_infos();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(macro_cache_stats), K_(dir_sizes), K_(dir_nums));

public:
  uint64_t tenant_id_;
  ObSEArray<ObSSMacroCacheStat, SS_MACRO_CACHE_MAX_TYPE_VAL> macro_cache_stats_;
  ObSEArray<int64_t, SS_MACRO_CACHE_MAX_TYPE_VAL> dir_sizes_;
  ObSEArray<int64_t, SS_MACRO_CACHE_MAX_TYPE_VAL> dir_nums_;
};

struct ObTenantDiskSpaceMeta final
{
public:
  ObTenantDiskSpaceMeta();
  ObTenantDiskSpaceMeta(const uint64_t tenant_id);
  ~ObTenantDiskSpaceMeta() = default;
  ObTenantDiskSpaceMeta &operator==(const ObTenantDiskSpaceMeta &other) = delete;
  ObTenantDiskSpaceMeta &operator!=(const ObTenantDiskSpaceMeta &other) = delete;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(header),
               K_(body));
  OB_UNIS_VERSION(1);
public:
  ObSSCommonHeader header_;
  ObTenantDiskSpaceMetaBody body_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DISK_SPACE_META_H_ */
