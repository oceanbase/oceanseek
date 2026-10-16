/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_base_format.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

/**
 * --------------------------------ObSSBaseFormat Template Implementation------------------------------------
 */

template<typename BodyType>
void ObSSBaseFormat<BodyType>::reset()
{
  header_.reset();
  body_.reset();
}

template<typename BodyType>
bool ObSSBaseFormat<BodyType>::is_valid() const
{
  bool is_valid = header_.is_valid() && body_.is_valid();
  return is_valid;
}

template<typename BodyType>
int ObSSBaseFormat<BodyType>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t ser_len = get_serialize_size();
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0 || (buf_len - new_pos) < ser_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), K(new_pos), K(ser_len));
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to encode meta header", K(ret), K(buf_len), K(new_pos), K(*this));
  } else if (OB_FAIL(body_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to encode meta body", K(ret), K(buf_len), K(new_pos), K(*this));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<typename BodyType>
int ObSSBaseFormat<BodyType>::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  uint32_t calc_crc = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta head", KR(ret));
  } else if (OB_UNLIKELY(header_.body_crc_ !=
      (calc_crc = static_cast<uint32_t>(ob_crc64(buf + new_pos, header_.body_size_))))) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "failed to check crc", KR(ret), KP(buf), K(data_len), K(pos), K_(header), K(calc_crc));
  } else if (OB_FAIL(body_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("fail to deserialize meta body", KR(ret));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ss format", KR(ret), K_(header), K_(body));
  } else {
    pos = new_pos;
  }
  return ret;
}

template<typename BodyType>
int64_t ObSSBaseFormat<BodyType>::get_serialize_size() const
{
  int64_t len = 0;
  len += header_.get_serialize_size();
  len += body_.get_serialize_size();
  return len;
}

/**
 * --------------------------------ObSSBaseUtil Template Implementation------------------------------------
 */

template<typename FormatType>
int ObSSBaseUtil<FormatType>::write_ss_format(const ObBackupDest &storage_dest,
                                              const FormatType &ss_format,
                                              const char *file_name)
{
  int ret = OB_SUCCESS;
  char ss_format_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_UNLIKELY(!storage_dest.is_valid() || !ss_format.is_valid() || OB_ISNULL(file_name))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ss_format), K(storage_dest), KP(file_name));
  } else if (OB_FAIL(get_ss_format_path(storage_dest, ss_format_path, sizeof(ss_format_path), file_name))) {
    LOG_WARN("fail to get ss_format path", KR(ret), K(ss_format_path));
  } else {
    ObBackupIoAdapter io_adapter;
    DefaultPageAllocator allocator("SSFormat", OB_SERVER_TENANT_ID);
    const int64_t serialize_size = ss_format.get_serialize_size();
    const int64_t buf_len = upper_align(serialize_size, DIO_ALIGN_SIZE);
    int64_t pos = 0;
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
    } else if (OB_FAIL(ss_format.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize ss_format", KR(ret), K(ss_format));
    } else if (OB_UNLIKELY(pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos", KR(ret), K(pos), K(serialize_size));
    } else if (FALSE_IT(memset(buf + pos, 0, buf_len - pos))) {
    } else if (OB_FAIL(io_adapter.write_single_file(ss_format_path, storage_dest.get_storage_info(),
                                                    buf, buf_len, common::ObStorageIdMod::get_default_id_mod()))) {
      LOG_WARN("fail to write single file", KR(ret), K(ss_format_path), K(storage_dest));
    } else {
      LOG_INFO("succ to write ss_format file", K(ss_format_path));
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

template<typename FormatType>
int ObSSBaseUtil<FormatType>::read_ss_format(const ObBackupDest &storage_dest, 
                                             FormatType &ss_format,
                                             const char *file_name)
{
  int ret = OB_SUCCESS;
  char ss_format_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_UNLIKELY(!storage_dest.is_valid() || OB_ISNULL(file_name))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), KP(file_name));
  } else if (OB_FAIL(get_ss_format_path(storage_dest, ss_format_path, sizeof(ss_format_path), file_name))) {
    LOG_WARN("fail to get ss_format path", KR(ret), K(ss_format_path));
  } else {
    ObBackupIoAdapter io_adapter;
    DefaultPageAllocator allocator("SSFormat", OB_SERVER_TENANT_ID);
    char *buf = nullptr;
    const int64_t file_len = DIO_ALIGN_SIZE;
    int64_t pos = 0;
    int64_t read_size = 0;
    if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(file_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", KR(ret), K(file_len));
    } else if (OB_FAIL(io_adapter.read_single_file(ss_format_path, storage_dest.get_storage_info(), buf,
                                            file_len, read_size, common::ObStorageIdMod::get_default_id_mod()))) {
      LOG_WARN("fail to read single file", KR(ret), K(ss_format_path), K(storage_dest));
    } else if (OB_FAIL(ss_format.deserialize(buf, read_size, pos))) {
      LOG_WARN("fail to deserialize", KR(ret), K(read_size));
    } else if (OB_UNLIKELY(!ss_format.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ss_format", KR(ret), K(ss_format));
    } else {
      LOG_INFO("succ to read ss_format file", KR(ret), K(ss_format), K(pos));
    }
    if (OB_NOT_NULL(buf)) {
      allocator.free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

template<typename FormatType>
int ObSSBaseUtil<FormatType>::is_exist_ss_format(const ObBackupDest &storage_dest,
                                                 bool &is_exist,
                                                 const char *file_name)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  char ss_format_path[common::MAX_PATH_SIZE] = { 0 };
  if (OB_UNLIKELY(!storage_dest.is_valid() || OB_ISNULL(file_name))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), KP(file_name));
  } else if (OB_FAIL(get_ss_format_path(storage_dest, ss_format_path, sizeof(ss_format_path), file_name))) {
    LOG_WARN("fail to get ss_format path", KR(ret), K(ss_format_path));
  } else if (OB_FAIL(io_adapter.is_exist(ss_format_path, storage_dest.get_storage_info(), is_exist))) {
    LOG_WARN("fail to list files", KR(ret), K(ss_format_path));
  }
  return ret;
}

template<typename FormatType>
int ObSSBaseUtil<FormatType>::get_ss_format_path(const ObBackupDest &storage_dest,
                                                 char *path, const int64_t length,
                                                 const char *file_name)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_UNLIKELY(!storage_dest.is_valid()) || OB_ISNULL(path) || OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), KP(path), KP(file_name));
  } else if (OB_FAIL(databuff_printf(path, length, "%s/%s_%ld/%s",
             storage_dest.get_root_path().ptr(), CLUSTER_DIR_STR,
             cluster_id, file_name))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(storage_dest));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

// Include necessary headers for explicit template instantiation
// These are needed because template classes require complete type definitions
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/shared_storage/ob_ss_cluster_info.h"

// Explicit template instantiation to resolve linker errors
template void oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSFormatBody>::reset();
template bool oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSFormatBody>::is_valid() const;
template int oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSFormatBody>::serialize(char*, const int64_t, int64_t&) const;
template int oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSFormatBody>::deserialize(const char*, const int64_t, int64_t&);
template int64_t oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSFormatBody>::get_serialize_size() const;

template void oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSClusterInfoBody>::reset();
template bool oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSClusterInfoBody>::is_valid() const;
template int oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSClusterInfoBody>::serialize(char*, const int64_t, int64_t&) const;
template int oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSClusterInfoBody>::deserialize(const char*, const int64_t, int64_t&);
template int64_t oceanbase::storage::ObSSBaseFormat<oceanbase::storage::ObSSClusterInfoBody>::get_serialize_size() const;

template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSFormat>::write_ss_format(const oceanbase::share::ObBackupDest&, const oceanbase::storage::ObSSFormat&, const char*);
template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSFormat>::read_ss_format(const oceanbase::share::ObBackupDest&, oceanbase::storage::ObSSFormat&, const char*);
template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSFormat>::is_exist_ss_format(const oceanbase::share::ObBackupDest&, bool&, const char*);
template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSFormat>::get_ss_format_path(const oceanbase::share::ObBackupDest&, char*, const int64_t, const char*);

template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSClusterInfo>::write_ss_format(const oceanbase::share::ObBackupDest&, const oceanbase::storage::ObSSClusterInfo&, const char*);
template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSClusterInfo>::read_ss_format(const oceanbase::share::ObBackupDest&, oceanbase::storage::ObSSClusterInfo&, const char*);
template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSClusterInfo>::is_exist_ss_format(const oceanbase::share::ObBackupDest&, bool&, const char*);
template int oceanbase::storage::ObSSBaseUtil<oceanbase::storage::ObSSClusterInfo>::get_ss_format_path(const oceanbase::share::ObBackupDest&, char*, const int64_t, const char*);

