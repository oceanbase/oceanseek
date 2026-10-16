/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

const char *const ObSSFormatUtil::SS_FORMAT_FILE_NAME = "ss_format";

/**
 * --------------------------------ObSSFormatBody------------------------------------
 */

DEFINE_SERIALIZE(ObSSFormatBody)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%lu\n", CLUSTER_VERSION_STR, cluster_version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", CREATE_TIMESTAMP_STR, create_timestamp_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else {
    // Note: databuff_printf print '\0' in the end, therefore plus 1 here
    pos = (new_pos + 1);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSFormatBody)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else {
    char tmp[SS_FORMAT_META_BUFF_SIZE] = { 0 };
    char *token = NULL;
    char *saved_ptr = NULL;
    const int64_t str_len = strlen(buf + pos);
    MEMCPY(tmp, buf + pos, str_len);
    tmp[str_len] = '\0';
    token = tmp;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "\n", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(VERSION_ID_STR, token, strlen(VERSION_ID_STR))) {
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(VERSION_ID_STR), version_))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        }
      } else if (0 == strncmp(CLUSTER_VERSION_STR, token, strlen(CLUSTER_VERSION_STR))) {
        int64_t value = 0;
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(CLUSTER_VERSION_STR), value))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        } else {
          cluster_version_ = static_cast<uint64_t>(value);
        }
      } else if (0 == strncmp(CREATE_TIMESTAMP_STR, token, strlen(CREATE_TIMESTAMP_STR))) {
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(CREATE_TIMESTAMP_STR), create_timestamp_))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", KR(ret), K(token));
      }
    }
    if (OB_SUCC(ret)) {
      // Note: databuff_printf print '\0' in the end when serialize, therefore plus 1 here
      new_pos += (str_len + 1);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss format body", KR(ret), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSFormatBody)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  char buf[SS_FORMAT_META_BUFF_SIZE] = {0};
  int64_t buf_len = sizeof(buf);
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", VERSION_ID_STR, version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%lu\n", CLUSTER_VERSION_STR, cluster_version_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%ld\n", CREATE_TIMESTAMP_STR, create_timestamp_))) {
    LOG_WARN("write data buff failed", KR(ret));
  } else {
    // Note: databuff_printf print '\0' in the end, therefore plus 1 here
    len += (pos + 1);
  }
  return len;
}

ObSSFormatBody::ObSSFormatBody()
{
  reset();
}

bool ObSSFormatBody::is_valid() const
{
  bool is_valid = ((version_ >= SS_FORMAT_VERSION)
                  && (cluster_version_ != OB_INVALID_ID)
                  && (create_timestamp_ >= 0));
  return is_valid;
}

void ObSSFormatBody::reset()
{
  version_ = SS_FORMAT_VERSION;
  cluster_version_ = OB_INVALID_ID;
  create_timestamp_ = 0;
}

/**
 * --------------------------------ObSSFormat------------------------------------
 */

ObSSFormat::ObSSFormat()
{
  reset();
}

int ObSSFormat::init(const uint64_t cluster_version, int64_t create_timestamp)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!(cluster_version != OB_INVALID_ID && create_timestamp >= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cluster_version), K(create_timestamp));
  } else {
    body_.cluster_version_ = cluster_version;
    body_.create_timestamp_ = create_timestamp;
    if (OB_FAIL(header_.construct_header(body_))) {
      LOG_WARN("fail to construct header", KR(ret), K(body_));
    }
  }
  return ret;
}


/**
 * --------------------------------ObSSFormatUtil------------------------------------
 */

int ObSSFormatUtil::write_ss_format(const ObBackupDest &storage_dest,
                                    const ObSSFormat &ss_format)
{
  return ObSSBaseUtil<ObSSFormat>::write_ss_format(storage_dest, ss_format, SS_FORMAT_FILE_NAME);
}

int ObSSFormatUtil::read_ss_format(const ObBackupDest &storage_dest, ObSSFormat &ss_format)
{
  return ObSSBaseUtil<ObSSFormat>::read_ss_format(storage_dest, ss_format, SS_FORMAT_FILE_NAME);
}

int ObSSFormatUtil::is_exist_ss_format(const ObBackupDest &storage_dest,
                                       bool &is_exist)
{
  return ObSSBaseUtil<ObSSFormat>::is_exist_ss_format(storage_dest, is_exist, SS_FORMAT_FILE_NAME);
}

int ObSSFormatUtil::get_ss_format_path(const ObBackupDest &storage_dest,
                                       char *path, const int64_t length)
{
  return ObSSBaseUtil<ObSSFormat>::get_ss_format_path(storage_dest, path, length, SS_FORMAT_FILE_NAME);
}

} // namespace storage
} // namespace oceanbase
