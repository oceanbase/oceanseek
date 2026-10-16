// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OCEANBASE_STORAGE_META_SHARED_STORAGE_OBJ_META_
#define OCEANBASE_STORAGE_META_SHARED_STORAGE_OBJ_META_

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
struct ObGCTabletMetaInfo final
{
public:
  OB_UNIS_VERSION(1);
public:
  ObGCTabletMetaInfo():scn_() {}
  explicit ObGCTabletMetaInfo(
      const share::SCN &scn)
    : scn_(scn),
      tablet_meta_create_ts_(INT64_MAX)
  {}
  explicit ObGCTabletMetaInfo(
      const share::SCN &scn,
      const int64_t tablet_meta_create_ts)
    : scn_(scn),
      tablet_meta_create_ts_(tablet_meta_create_ts)
  {}
  ~ObGCTabletMetaInfo() = default;
  bool is_valid() { return scn_.is_valid() && tablet_meta_create_ts_ > 0; }
  TO_STRING_KV(K_(scn), K_(tablet_meta_create_ts));
public:
  share::SCN scn_; // snapshot_version
  int64_t tablet_meta_create_ts_;
};
struct ObGCTabletMetaInfoList final 
{
public:
  OB_UNIS_VERSION(1);
public:
  ObGCTabletMetaInfoList():tablet_version_arr_() {}
  ~ObGCTabletMetaInfoList() = default;
  bool is_valid() const { return tablet_version_arr_.count() >= 0; };
  TO_STRING_KV(K_(tablet_version_arr));
public:
  ObSEArray<ObGCTabletMetaInfo, 16> tablet_version_arr_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_META_SHARED_STORAGE_OBJ_META_
