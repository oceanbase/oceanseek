/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "palf_ffi.h"
#include "libpalf_kv_instance.h"
#include "lib/oblog/ob_log_module.h"
#include "libpalf_common_define.h"
#include "lib/string/ob_string_buffer.h"
#include <iostream>

namespace oceanbase
{
namespace libpalf
{

int LibPalfKVIteraotr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    PALF_LOG(WARN, "LibPalfKVIterator already inited");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi_))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_list_open(libpalf_env_ffi_, dir_.ptr(), dir_.length(), sub_dir_.ptr(), sub_dir_.length(), prefix_.ptr(), prefix_.length(), 0, &libpalf_kv_pairs_)))) {
    PALF_LOG(WARN, "libpalf_kv_list_open failed", K(prefix_), K(ret));
  } else {
    is_inited_ = true;
    PALF_LOG(INFO, "libpalf_kv_list_open init success", K_(prefix));
  }
  return ret;
}

bool LibPalfKVIteraotr::has_next() const {
  return is_inited_ && libpalf_kv_pairs_ != nullptr;
}

int LibPalfKVIteraotr::next(common::ObStringBuffer &key, common::ObStringBuffer &value) {
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LibPalfKVIterator not initialized", K(ret));
  } else {
    LibPalfKVPair libpalf_kv_pair;
    libpalf_kv_pair.key.ptr = NULL;
    libpalf_kv_pair.value.ptr = NULL;
    if (NULL == (libpalf_kv_pair.key.ptr = static_cast<char*>(allocator_.alloc(METAKV_MAX_KEY_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "fail to allocate memory", K(ret), K(METAKV_MAX_KEY_LEN));
    } else if (FALSE_IT(libpalf_kv_pair.key.buf_size = METAKV_MAX_KEY_LEN)) {
    } else if (NULL == (libpalf_kv_pair.value.ptr = static_cast<char*>(allocator_.alloc(METAKV_MAX_VALUE_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      PALF_LOG(WARN, "fail to allocate memory", K(ret), K(METAKV_MAX_VALUE_LEN));
    } else if (FALSE_IT(libpalf_kv_pair.value.buf_size = METAKV_MAX_VALUE_LEN)) {
    } else {
      ret = LIBPALF_ERRNO_CAST(libpalf_kv_list_next(libpalf_kv_pairs_, &libpalf_kv_pair));
      if (OB_SUCC(ret)) {
        key.append(libpalf_kv_pair.key.ptr, libpalf_kv_pair.key.data_length);
        value.append(libpalf_kv_pair.value.ptr, libpalf_kv_pair.value.data_length);
        PALF_LOG(TRACE, "libpalf_kv_list_next successfully", K(key), K(value));
      } else if (ret == OB_ITER_END) {
        reset();
      } else {
        PALF_LOG(WARN, "failed to libpalf_kv_list_next", K(ret), K(key), K(value));
      }
    }

    if (NULL != libpalf_kv_pair.key.ptr) {
      allocator_.free(libpalf_kv_pair.key.ptr);
    }

    if (NULL != libpalf_kv_pair.value.ptr) {
      allocator_.free(libpalf_kv_pair.value.ptr);
    }
  }

  return ret;
}

void LibPalfKVIteraotr::reset() {
  int ret = OB_SUCCESS;
  if (libpalf_kv_pairs_ != nullptr) {
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_list_close(libpalf_kv_pairs_)))) {
      PALF_LOG(WARN, "libpalf_kv_list_close failed", K(ret));
    }
    libpalf_kv_pairs_ = nullptr;
  }
  is_inited_ = false;
}

LibPalfKVInstance &LibPalfKVInstance::get_instance()
{
  static LibPalfKVInstance libpalf_kv_instance;
  return libpalf_kv_instance;
}

LibPalfKVInstance::LibPalfKVInstance() {}


int LibPalfKVInstance::get(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, common::ObStringBuffer &value)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;
  LibPalfKVBuf val;
  val.ptr = NULL;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (NULL == (val.ptr = static_cast<char*>(value.get_allocator()->alloc(libpalf::METAKV_MAX_VALUE_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "fail to allocate memory", K(ret), K(METAKV_MAX_VALUE_LEN));
  } else if (FALSE_IT(val.buf_size = libpalf::METAKV_MAX_VALUE_LEN)) {
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_get(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), val.ptr, &val.data_length)))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_get failed", K(key), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_get failed", K(key), K(ret));
    }
  } else {
    value.append(val.ptr, val.data_length);
    PALF_LOG(INFO, "libpalf_kv_get successfully", K(key), K(value));
  }

  if (NULL != val.ptr) {
    value.get_allocator()->free(val.ptr);
  }

  return ret;
}

int LibPalfKVInstance::insert(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;

  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_insert(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), value.ptr(), value.length())))) {
    if (OB_ENTRY_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_insert failed", K(key), K(value), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_insert failed", K(key), K(value), K(ret));
    }
  } else {
    PALF_LOG(INFO, "libpalf_kv_insert successfully", K(key), K(value), K(ret));
  }
  return ret;
}

int LibPalfKVInstance::update(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;

  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_update(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), value.ptr(), value.length())))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_update failed", K(key), K(value), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_update failed", K(key), K(value), K(ret));
    }
  } else {
    PALF_LOG(INFO, "libpalf_kv_update successfully", K(key), K(value), K(ret));
  }
  return ret;
}

int LibPalfKVInstance::cas(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &old_value, const common::ObString &new_value, bool *expected)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_cas(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), old_value.ptr(), old_value.length(),
      new_value.ptr(), new_value.length(), expected)))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_cas failed", K(key), K(old_value), K(new_value), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_cas failed", K(key), K(old_value), K(new_value), K(ret));
    }
  } else {
    PALF_LOG(INFO, "libpalf_kv_cas successfully", K(key), K(old_value),  K(ret));
  }
  return ret;
}

int LibPalfKVInstance::watch_cas(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &watch_key, const common::ObString &old_value, const common::ObString &new_value, bool *expected)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_watch_cas(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), watch_key.ptr(), watch_key.length(), old_value.ptr(), old_value.length(),
      new_value.ptr(), new_value.length(), expected)))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_watch_cas failed", K(key), K(watch_key), K(old_value), K(new_value), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_watch_cas failed", K(key), K(watch_key), K(old_value), K(new_value), K(ret));
    }
  } else {
    PALF_LOG(INFO, "libpalf_kv_watch_cas successfully", K(key), K(watch_key), K(old_value), K(new_value), K(ret));
  }
  return ret;
}

int LibPalfKVInstance::del(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_delete(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length())))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_del failed", K(key), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_del failed", K(key), K(ret));
    }
  } else {
    PALF_LOG(INFO, "libpalf_kv_del successfully", K(key), K(ret));
  }
  return ret;
}

int LibPalfKVInstance::insert_encrypted(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;

  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_insert_encrypted(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), value.ptr(), value.length())))) {
    if (OB_ENTRY_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_insert_encrypted failed", K(key), K(value), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_insert_encrypted failed", K(key), K(value), K(ret));
    }
  } else {
    PALF_LOG(INFO, "libpalf_kv_insert_encrypted successfully", K(key), K(value), K(ret));
  }
  return ret;
}

int LibPalfKVInstance::get_decrypted(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, common::ObStringBuffer &value)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;
  LibPalfKVBuf val;
  val.ptr = NULL;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
    PALF_LOG(ERROR, "LibPalfEnvFFIInstance init failed", K(ret));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (NULL == (val.ptr = static_cast<char*>(value.get_allocator()->alloc(libpalf::METAKV_MAX_VALUE_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "fail to allocate memory", K(ret), K(METAKV_MAX_VALUE_LEN));
  } else if (FALSE_IT(val.buf_size = libpalf::METAKV_MAX_VALUE_LEN)) {
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_kv_get_decrypted(libpalf_env_ffi, dir.ptr(), dir.length(), sub_dir.ptr(), sub_dir.length(), key.ptr(), key.length(), val.ptr, &val.data_length)))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_LOGSERVICE_RPC_ERROR == ret) {
      PALF_LOG(WARN, "libpalf_kv_get_decrypted failed", K(key), K(ret));
    } else {
      PALF_LOG(ERROR, "libpalf_kv_get_decrypted failed", K(key), K(ret));
    }
  } else {
    value.append(val.ptr, val.data_length);
    PALF_LOG(INFO, "libpalf_kv_get_decrypted successfully", K(key), K(value));
  }

  if (NULL != val.ptr) {
    value.get_allocator()->free(val.ptr);
  }

  return ret;
}

int LibPalfKVInstance::seek(LibPalfKVIteraotr &iterator)
{
  return iterator.init();
}

}
}
