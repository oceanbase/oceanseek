/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_INFO_READER
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_INFO_READER

#include "storage/mds_table/ob_mds_multikey_reader.h"
#include "storage/truncate_info/ob_truncate_info.h"

namespace oceanbase
{
namespace storage
{

using ObTabletTruncateInfoReader = ObTabletMDSInfoReader<ObTruncateInfoKey, ObTruncateInfo>;

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_INFO_READER
