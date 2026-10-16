/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_MULTIKEY_READER_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_MULTIKEY_READER_H_

#include "share/scn.h"
#include "storage/tablet/ob_mds_cl_range_query_iterator.h"

namespace oceanbase
{
namespace storage
{

template <typename Key, typename Value>
class ObTabletMDSInfoReader final
{
public:
  ObTabletMDSInfoReader() : is_inited_(false), iter_() {}

public:
  int init(const ObTablet &tablet, const share::SCN &read_snapshot);

  /// Caller must reset @p key / @p value / @p meta before each call; outputs are valid only on OB_SUCCESS.
  int get_next_mds_kv(ObIAllocator &allocator, Key &key, Value &value, ObMdsValueMeta &meta);

private:
  bool is_inited_;
  ObMdsRangeQueryIterator<Key, Value> iter_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_MULTIKEY_READER_H_
