// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#include "meta_store/ob_shared_storage_obj_meta.h"

namespace oceanbase
{
namespace storage
{

  OB_SERIALIZE_MEMBER(ObGCTabletMetaInfo, scn_, tablet_meta_create_ts_);
  OB_SERIALIZE_MEMBER(ObGCTabletMetaInfoList, tablet_version_arr_);


} // namespace storage
} // namespace oceanbase

