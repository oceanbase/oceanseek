/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_cluster_info.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::common;

const char *const ObSSClusterInfoUtil::SS_CLUSTER_INFO_FILE_NAME = "ss_cluster_info";

/**
 * --------------------------------ObSSClusterInfoBody------------------------------------
 */

#define WRITE_SS_CLUSTER_INFO_DATA_BUFF                                                                                                       \
do {                                                                                                                                          \
  if (OB_SUCC(ret)) {                                                                                                                         \
    if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%ld\n", VERSION_ID_STR, version_))) {                                               \
      LOG_WARN("write data buff failed", KR(ret));                                                                                            \
    } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%lu\n", LOGSERVICE_CLUSTER_ID_STR, logservice_cluster_id_))) {               \
      LOG_WARN("write data buff failed", KR(ret));                                                                                            \
    } else if (OB_FAIL(databuff_printf(buf, buf_len, new_pos, "%s%s\n", CLUSTER_REGION_STR, cluster_region_.ptr()))) {                        \
      LOG_WARN("write data buff failed", KR(ret));                                                                                            \
    }                                                                                                                                         \
  }                                                                                                                                           \
} while (0);                                                                                                                                  \

DEFINE_SERIALIZE(ObSSClusterInfoBody)
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  }
  WRITE_SS_CLUSTER_INFO_DATA_BUFF;
  if (OB_SUCC(ret)) {
    // Note: databuff_printf print '\0' in the end, therefore plus 1 here
    pos = (new_pos + 1);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSClusterInfoBody)
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
      } else if (0 == strncmp(LOGSERVICE_CLUSTER_ID_STR, token, strlen(LOGSERVICE_CLUSTER_ID_STR))) {
        int64_t value = 0;
        if (OB_FAIL(ObSSCommonHeader::parse_int_value(token + strlen(LOGSERVICE_CLUSTER_ID_STR), value))) {
          LOG_WARN("fail to parse int value", KR(ret), K(token));
        } else {
          logservice_cluster_id_ = static_cast<uint64_t>(value);
        }
      } else if (0 == strncmp(CLUSTER_REGION_STR, token, strlen(CLUSTER_REGION_STR))) {
        const char *region_str = token + strlen(CLUSTER_REGION_STR);
        if (OB_FAIL(cluster_region_.assign(region_str))) {
          LOG_WARN("fail to assign cluster region", KR(ret), K(region_str));
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
    LOG_WARN("invalid ss cluster info body", KR(ret), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSClusterInfoBody)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  char buf[SS_FORMAT_META_BUFF_SIZE] = {0};
  int64_t buf_len = sizeof(buf);
  int64_t new_pos = 0;
  WRITE_SS_CLUSTER_INFO_DATA_BUFF;
  if (OB_SUCC(ret)) {
    // Note: databuff_printf print '\0' in the end, therefore plus 1 here
    len += (new_pos + 1);
  }
  return len;
}

ObSSClusterInfoBody::ObSSClusterInfoBody()
{
  reset();
}

bool ObSSClusterInfoBody::is_valid() const
{
  bool is_valid = ((version_ >= SS_CLUSTER_INFO_VERSION)
                  && (logservice_cluster_id_ != OB_INVALID_ID)
                  && (!cluster_region_.is_empty()));
  return is_valid;
}

void ObSSClusterInfoBody::reset()
{
  version_ = SS_CLUSTER_INFO_VERSION;
  logservice_cluster_id_ = OB_INVALID_ID;
  cluster_region_.reset();
}

/**
 * --------------------------------ObSSClusterInfo------------------------------------
 */

ObSSClusterInfo::ObSSClusterInfo()
{
  reset();
}

int ObSSClusterInfo::init(const uint64_t logservice_cluster_id, const common::ObRegion &cluster_region)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!(logservice_cluster_id != OB_INVALID_ID && !cluster_region.is_empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(logservice_cluster_id), K(cluster_region));
  } else {
    body_.logservice_cluster_id_ = logservice_cluster_id;
    if (OB_FAIL(body_.cluster_region_.assign(cluster_region))) {
      LOG_WARN("fail to assign cluster region", KR(ret), K(cluster_region));
    } else if (OB_FAIL(header_.construct_header(body_))) {
      LOG_WARN("fail to construct header", KR(ret), K(body_));
    }
  }
  return ret;
}

/**
 * --------------------------------ObSSClusterInfoUtil------------------------------------
 */

int ObSSClusterInfoUtil::write_ss_cluster_info(const ObBackupDest &storage_dest,
                                               const ObSSClusterInfo &ss_cluster_info)
{
  return ObSSBaseUtil<ObSSClusterInfo>::write_ss_format(storage_dest, ss_cluster_info, SS_CLUSTER_INFO_FILE_NAME);
}

int ObSSClusterInfoUtil::read_ss_cluster_info(const ObBackupDest &storage_dest, ObSSClusterInfo &ss_cluster_info)
{
  return ObSSBaseUtil<ObSSClusterInfo>::read_ss_format(storage_dest, ss_cluster_info, SS_CLUSTER_INFO_FILE_NAME);
}

int ObSSClusterInfoUtil::is_exist_ss_cluster_info(const ObBackupDest &storage_dest,
                                                  bool &is_exist)
{
  return ObSSBaseUtil<ObSSClusterInfo>::is_exist_ss_format(storage_dest, is_exist, SS_CLUSTER_INFO_FILE_NAME);
}

int ObSSClusterInfoUtil::get_ss_cluster_info_path(const ObBackupDest &storage_dest,
                                                  char *path, const int64_t length)
{
  return ObSSBaseUtil<ObSSClusterInfo>::get_ss_format_path(storage_dest, path, length, SS_CLUSTER_INFO_FILE_NAME);
}

} // namespace storage
} // namespace oceanbase
