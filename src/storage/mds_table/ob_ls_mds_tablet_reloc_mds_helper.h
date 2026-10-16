/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_LS_MDS_TABLET_RELOC_MDS_HELPER_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_LS_MDS_TABLET_RELOC_MDS_HELPER_H_

#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObLsMdsTabletRelocMdsHelper
{
public:
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);

private:
  static int apply_rows_(const bool for_replay,
                         const share::SCN &replay_scn,
                         class ObLsMdsTabletRelocBatchArg &arg,
                         mds::BufferCtx &ctx);
};

} // namespace storage
} // namespace oceanbase

#endif
