/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_fork_src_mds_data.h"
#include "storage/multi_data_source/mds_key_serialize_util.h"

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER_SIMPLE(ObTabletForkSrcMdsKey, dst_tablet_id_);
OB_SERIALIZE_MEMBER(ObTabletForkSrcMdsData, fork_snapshot_version_);

int ObTabletForkSrcMdsKey::mds_serialize(
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || OB_ISNULL(buf) || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork src mds key serialize argument",
        KR(ret), K(*this), KP(buf), K(buf_len), K(pos));
  } else if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf[pos++] = static_cast<char>(MAGIC_NUMBER);
    if (OB_FAIL(mds::ObMdsSerializeUtil::mds_key_serialize(
            static_cast<int64_t>(dst_tablet_id_.id()), buf, buf_len, pos))) {
      LOG_WARN("failed to serialize fork dst tablet id", KR(ret), K_(dst_tablet_id));
    }
  }
  return ret;
}

int ObTabletForkSrcMdsKey::mds_deserialize(
    const char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = 0;
  uint8_t magic_number = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork src mds key deserialize argument", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (FALSE_IT(magic_number = static_cast<uint8_t>(buf[pos++]))) {
  } else if (OB_UNLIKELY(MAGIC_NUMBER != magic_number)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fork src mds key magic number mismatch", KR(ret), K(magic_number), K(MAGIC_NUMBER));
  } else if (OB_FAIL(mds::ObMdsSerializeUtil::mds_key_deserialize(buf, buf_len, pos, tablet_id))) {
    LOG_WARN("failed to deserialize fork dst tablet id", KR(ret));
  } else if (OB_UNLIKELY(tablet_id <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fork dst tablet id", KR(ret), K(tablet_id));
  } else {
    dst_tablet_id_ = static_cast<uint64_t>(tablet_id);
  }
  return ret;
}

int64_t ObTabletForkSrcMdsKey::mds_get_serialize_size() const
{
  return sizeof(MAGIC_NUMBER)
      + mds::ObMdsSerializeUtil::mds_key_get_serialize_size(
          static_cast<int64_t>(dst_tablet_id_.id()));
}

} // namespace storage
} // namespace oceanbase
