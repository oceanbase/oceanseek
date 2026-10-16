/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_HA_TABLET_POINTER_H_
#define OCEANBASE_SHARED_STORAGE_HA_TABLET_POINTER_H_

#include "storage/incremental/share/ob_shared_meta_iter_guard.h"

namespace oceanbase
{
namespace storage
{

class ObSSTabletPointerReader final
{
public:
  ObSSTabletPointerReader();
  ~ObSSTabletPointerReader() = default;
  int init(const ObMetaDiskAddr &load_addr);
  int get_tablet_raw_buffer(
      common::ObIAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len);
private:
  int get_local_sslog_readable_scn_(share::SCN &readable_scn);
  int get_tablet_version_iter(
      const share::SCN &readable_scn,
      ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard);
  int get_tablet_raw_buffer_(
      common::ObIAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len,
      ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard);
  int inner_get_tablet_raw_buffer_(
      common::ObIAllocator &allocator,
      const char *src_buf,
      const int64_t &src_buf_len,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len);
  int try_read_from_single_version_iter_(
      common::ObIAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len);
  int try_read_from_multi_version_iter_(
      common::ObIAllocator &allocator,
      char *&tablet_raw_buf,
      int64_t &tablet_raw_buf_len);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorg_scn_;
  share::SCN row_scn_;
  uint64_t offset_;
  uint64_t size_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTabletPointerReader);
};


} //storage
} //oceanbase

#endif
