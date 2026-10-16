/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_FORK_SRC_MDS_DATA_H_
#define OCEANBASE_STORAGE_OB_TABLET_FORK_SRC_MDS_DATA_H_

#include "common/ob_tablet_id.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace storage
{

class ObTabletForkSrcMdsKey final
{
  OB_UNIS_VERSION(1);
public:
  static constexpr uint8_t MAGIC_NUMBER = 0xFF;

  ObTabletForkSrcMdsKey() : dst_tablet_id_() {}
  explicit ObTabletForkSrcMdsKey(const common::ObTabletID &dst_tablet_id)
    : dst_tablet_id_(dst_tablet_id)
  {}
  ~ObTabletForkSrcMdsKey() = default;

  void reset() { dst_tablet_id_.reset(); }
  bool is_valid() const { return dst_tablet_id_.is_valid(); }
  bool operator==(const ObTabletForkSrcMdsKey &other) const
  {
    return dst_tablet_id_ == other.dst_tablet_id_;
  }
  bool operator<(const ObTabletForkSrcMdsKey &other) const
  {
    return dst_tablet_id_.id() < other.dst_tablet_id_.id();
  }

  int mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos);
  int64_t mds_get_serialize_size() const;

  TO_STRING_KV(K_(dst_tablet_id));

  common::ObTabletID dst_tablet_id_;
};

class ObTabletForkSrcMdsData
{
  OB_UNIS_VERSION(1);
public:
  ObTabletForkSrcMdsData() : fork_snapshot_version_(0) {}
  ~ObTabletForkSrcMdsData() = default;

  bool is_valid() const
  {
    return fork_snapshot_version_ > 0;
  }
  void reset()
  {
    fork_snapshot_version_ = 0;
  }
  void set_invalid()
  {
    reset();
  }
  int assign(const ObTabletForkSrcMdsData &other)
  {
    fork_snapshot_version_ = other.fork_snapshot_version_;
    return common::OB_SUCCESS;
  }

  TO_STRING_KV(K_(fork_snapshot_version));

  int64_t fork_snapshot_version_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FORK_SRC_MDS_DATA_H_
