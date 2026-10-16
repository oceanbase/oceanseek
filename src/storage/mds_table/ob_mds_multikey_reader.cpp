/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "storage/mds_table/ob_mds_multikey_reader.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/mds_table/ob_test_mds_types.h"
#include "storage/tablet/ob_tablet_fork_src_mds_data.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

template <typename Key, typename Value>
int ObTabletMDSInfoReader<Key, Value>::init(const ObTablet &tablet, const share::SCN &read_snapshot)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_FAIL(tablet.mds_range_query(read_snapshot, iter_))) {
    LOG_WARN("Fail to init mds range query iterator", KR(ret), K(read_snapshot), K(tablet));
  } else {
    is_inited_ = true;
  }
  return ret;
}

template <typename Key, typename Value>
int ObTabletMDSInfoReader<Key, Value>::get_next_mds_kv(ObIAllocator &allocator,
                                                       Key &key,
                                                       Value &value,
                                                       ObMdsValueMeta &meta)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret));
  } else if (OB_FAIL(iter_.get_next(allocator, key, value, meta))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Fail to get next mds row", KR(ret));
    }
  }
  return ret;
}

template class ObTabletMDSInfoReader<ObTruncateInfoKey, ObTruncateInfo>;
template class ObTabletMDSInfoReader<ObTTLFilterInfoKey, ObTTLFilterInfo>;
template class ObTabletMDSInfoReader<ObTabletDDLCompleteMdsUserDataKey, ObTabletDDLCompleteMdsUserData>;
template class ObTabletMDSInfoReader<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>;
template class ObTabletMDSInfoReader<mds::ObTestMdsMultiRowKey, mds::ObTestMdsMultiRowValue>;
template class ObTabletMDSInfoReader<ObTabletForkSrcMdsKey, ObTabletForkSrcMdsData>;

} // namespace storage
} // namespace oceanbase
