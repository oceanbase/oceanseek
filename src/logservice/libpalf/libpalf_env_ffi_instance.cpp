/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/string/ob_hex_utils_base.h"
#include "lib/allocator/ob_malloc.h"
#include "share/config/ob_server_config.h"
#include "share/backup/ob_backup_struct.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "share/ob_server_struct.h"
#include "share/ob_encryption_util.h"
#include "lib/signal/ob_signal_handlers.h"

#include "libpalf_common_define.h"
#include "libpalf_env_ffi_instance.h"
extern "C" {
#include "ussl-hook.h"
}

namespace oceanbase
{
namespace libpalf
{
LibPalfEnvFFIInstance &LibPalfEnvFFIInstance::get_instance()
{
    static LibPalfEnvFFIInstance libpalf_env_ffi_instance;
    return libpalf_env_ffi_instance;
}

LibPalfEnvFFIInstance::LibPalfEnvFFIInstance(): update_lock_(common::ObLatchIds::LIB_PALF_ENV_F_F_I_INSTANCE_LOCK),
    lock_(common::ObLatchIds::LIB_PALF_ENV_F_F_I_INSTANCE_LOCK), libpalf_env_ffi_(NULL),
    logservice_access_point_(), is_inited_(false) {}

int LibPalfEnvFFIInstance::parse_param_field_(const char *field_prefix, const char* const data,
        const int64_t data_len, LogserviceAccessPointBuf& out_buf)
{
    int ret = OB_SEARCH_NOT_FOUND;
    out_buf.reset();
    if (OB_ISNULL(data) || data_len <= 0 || OB_ISNULL(field_prefix)) {
        ret = OB_INVALID_ARGUMENT;
    } else {
        const char *end = data + data_len;
        const char *query = static_cast<const char *>(memchr(data, '?', data_len));
        const int64_t prefix_len = strlen(field_prefix);
        for (const char *token = nullptr == query ? end : query + 1; token < end; ) {
            const char *next = static_cast<const char *>(memchr(token, '&', end - token));
            next = nullptr == next ? end : next;
            const int64_t token_len = next - token;
            if (token_len >= prefix_len && 0 == memcmp(token, field_prefix, prefix_len)) {
                if (OB_SUCCESS == ret || token_len == prefix_len || token_len >= out_buf.capacity()) {
                    return OB_INVALID_ARGUMENT;
                }
                memcpy(out_buf.ptr(), token, token_len);
                out_buf.ptr()[token_len] = '\0';
                ret = OB_SUCCESS;
            }
            token = next == end ? end : next + 1;
        }
    }
    return ret;
}

int LibPalfEnvFFIInstance::encrypt_logservice_access_point(const char* const data,
        const int64_t data_len, char* const buf, const int64_t buf_len, int64_t& out_len)
{
    int ret = OB_SUCCESS;
    ObString access_point;
    obrpc::ObAdminStorageArg arg;
    share::ObBackupDest backup_dest;
    share::ObBackupStorageInfo *storage_info = nullptr;
    int64_t pos = 0;
    int64_t auth_len = 0;
    out_len = 0;
    HEAP_VAR(LogserviceAccessPointBuf, cluster_id) {
        if (OB_ISNULL(data) || data_len <= 0 || OB_ISNULL(buf) || buf_len <= 0) {
            ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(parse_param_field_(logservice_cluster_id_prefix, data, data_len, cluster_id))) {
            PALF_LOG(WARN, "invalid logservice cluster ID parameter", KR(ret));
        } else if (FALSE_IT(access_point.assign(const_cast<char*>(data), data_len))) {
        } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(access_point, arg, backup_dest))) {
            PALF_LOG(WARN, "parse logservice access point failed", KR(ret));
        } else if (OB_ISNULL(storage_info = backup_dest.get_storage_info())) {
            ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s&%s", arg.path_.ptr(), cluster_id.ptr()))) {
            PALF_LOG(WARN, "format logservice access point failed", KR(ret));
        } else if ('\0' != storage_info->extension_[0] &&
            OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s", storage_info->extension_))) {
            // Preserve all parsed extensions, including s3_region and addressing_model.
            PALF_LOG(WARN, "format storage extensions failed", KR(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s&", arg.attribute_.ptr()))) {
            PALF_LOG(WARN, "format storage attributes failed", KR(ret));
        } else if (OB_FAIL(storage_info->get_authorization_info(buf + pos, buf_len - pos, auth_len))) {
            PALF_LOG(WARN, "encrypt storage authorization failed", KR(ret));
        } else if (pos + auth_len + 1 > buf_len) {
            ret = OB_BUF_NOT_ENOUGH;
        } else {
            buf[pos + auth_len] = '\0';
            out_len = pos + auth_len + 1;
        }
    } else {
        PALF_LOG(WARN, "allocate cluster ID buffer failed", KR(ret));
    }
    return ret;
}

int LibPalfEnvFFIInstance::decrypt_logservice_access_point(const char* const data,
        const int64_t data_len, char* const buf, const int64_t buf_len, int64_t& out_len)
{
    int ret = OB_SUCCESS;
    ObString access_point;
    obrpc::ObAdminStorageArg arg;
    share::ObBackupDest backup_dest;
    share::ObBackupStorageInfo *storage_info = nullptr;
    int64_t pos = 0;
    int64_t auth_len = 0;
    out_len = 0;
    HEAP_VAR(LogserviceAccessPointBuf, cluster_id) {
        if (OB_ISNULL(data) || data_len <= 0 || OB_ISNULL(buf) || buf_len <= 0) {
            ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(parse_param_field_(logservice_cluster_id_prefix, data, data_len, cluster_id))) {
            PALF_LOG(WARN, "invalid logservice cluster ID parameter", KR(ret));
        } else if (FALSE_IT(access_point.assign(const_cast<char*>(data), data_len))) {
        } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(access_point, arg, backup_dest))) {
            PALF_LOG(WARN, "parse logservice access point failed", KR(ret));
        } else if (OB_ISNULL(storage_info = backup_dest.get_storage_info())) {
            ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s&%s", arg.path_.ptr(), cluster_id.ptr()))) {
            PALF_LOG(WARN, "format logservice access point failed", KR(ret));
        } else if ('\0' != storage_info->extension_[0] &&
            OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s", storage_info->extension_))) {
            // Preserve all parsed extensions, including s3_region and addressing_model.
            PALF_LOG(WARN, "format storage extensions failed", KR(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s&", arg.attribute_.ptr()))) {
            PALF_LOG(WARN, "format storage attributes failed", KR(ret));
        } else if (OB_FAIL(storage_info->get_unencrypted_authorization_info(buf + pos, buf_len - pos, auth_len))) {
            PALF_LOG(WARN, "decrypt storage authorization failed", KR(ret));
        } else if (pos + auth_len + 1 > buf_len) {
            ret = OB_BUF_NOT_ENOUGH;
        } else {
            buf[pos + auth_len] = '\0';
            out_len = pos + auth_len + 1;
        }
    } else {
        PALF_LOG(WARN, "allocate cluster ID buffer failed", KR(ret));
    }
    return ret;
}

int LibPalfEnvFFIInstance::do_refresh_logservice_access_point_()
{
    int ret = OB_SUCCESS;
    int64_t out_len = 0;
    if (logservice_access_point_.size() > 0) { // do nothing
    } else if (0 == GCONF.logservice_access_point.size()) {
        ret = OB_STATE_NOT_MATCH;
        PALF_LOG(WARN, "there is no available logservice_access_point", KR(ret));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::decrypt_logservice_access_point(
        GCONF.logservice_access_point.str(),
        strlen(GCONF.logservice_access_point.str()),
        logservice_access_point_.ptr(),
        logservice_access_point_.capacity(),
        out_len
    ))) {
        PALF_LOG(WARN, "decrypt_logservice_access_point failed", KR(ret));
    } else {
        PALF_LOG(INFO, "decrypt_logservice_access_point success", KR(ret));
    }
    return ret;
}

int LibPalfEnvFFIInstance::do_init_()
{
    int ret = OB_SUCCESS;
    char addr_buffer[MAX_IP_PORT_LENGTH] = {0};
    const common::ObAddr &self_addr = GCTX.self_addr();
    LibPalfAccessPointType access_point_type = LibPalfAccessPointType::ObjectStorage;
    int listen_pipe_fd = -1;
    if (IS_INIT) {
        PALF_LOG(INFO, "double check init LibPalfEnvFFIInstance again, do nothing");
    } else if (!self_addr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "self addr is invalid", K(ret), K(self_addr), K(addr_buffer));
    } else if (OB_FAIL(self_addr.ip_port_to_string(addr_buffer, sizeof(addr_buffer)))) {
        PALF_LOG(WARN, "self addr to string failed", KR(ret), K(self_addr), K(addr_buffer));
    } else if (OB_FAIL(do_refresh_logservice_access_point_())) {
        PALF_LOG(WARN, "refresh logservice_access_point failed", KR(ret));
    } else if ((listen_pipe_fd = ussl_loop_add_http_listen()) < 0) {
        ret = OB_ERR_SYS;
        PALF_LOG(WARN, "ussl_loop_add_http_listen failed", K(ret), K(self_addr),
            K(listen_pipe_fd));
    } else if (FALSE_IT(common::need_close_socket_fd_when_core_dump = false)) { // do nothing
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_open_env(
        logservice_access_point_.ptr(),
        access_point_type,
        addr_buffer,
        listen_pipe_fd,
        &libpalf_env_ffi_)))) {
        PALF_LOG(ERROR, "fail to open libpalf env", KR(ret), K(addr_buffer), K(listen_pipe_fd));
        ret = OB_INIT_FAIL;
    } else {
        is_inited_ = true;
        PALF_LOG(INFO, "succeed to open libpalf env", K(libpalf_env_ffi_));
    }
    return ret;
}

int LibPalfEnvFFIInstance::set_logservice_access_point(const char *logservice_access_point)
{
    int ret = OB_SUCCESS;
    lib::ObMutexGuard update_guard(update_lock_);
    common::RWLock::WLockGuard guard(lock_);
    if (OB_ISNULL(logservice_access_point)) {
        PALF_LOG(WARN, "setted logservice_access_point is NULL");
        ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(this->logservice_access_point_.assign(logservice_access_point))) {
        PALF_LOG(WARN, "setted logservice_access_point is too long, "\
            "longer than buf`s capacity",
            KR(ret), K(this->logservice_access_point_.capacity()));
    } else { } // do nothing
    return ret;
}

int LibPalfEnvFFIInstance::get_logservice_access_point(
    LogserviceAccessPointBuf& out_logservice_access_point) const
{
    int ret = OB_SUCCESS;
    common::RWLock::RLockGuard guard(lock_);
    if (OB_FAIL(out_logservice_access_point.assign(logservice_access_point_))) {
        PALF_LOG(WARN, "get logservice_access_point failed", KR(ret));
    } else { } // do nothing
    return ret;
}

int LibPalfEnvFFIInstance::init()
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        PALF_LOG(INFO, "init LibPalfEnvFFIInstance again, do nothing");
    } else {
        common::RWLock::WLockGuard guard(lock_);
        ret = do_init_();
    }
    return ret;
}

int LibPalfEnvFFIInstance::get_ffi(const LibPalfEnvFFI *& ffi)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        common::RWLock::WLockGuard guard(lock_);
        if (OB_FAIL(do_init_())) {
            PALF_LOG(WARN, "fail to init libpalf env", K(ret));
        }
    }
    common::RWLock::RLockGuard guard(lock_);
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfEnvFFIInstance is not inited", K(ret));
    } else if (OB_NOT_NULL(ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(ffi));
    } else {
        ffi = libpalf_env_ffi_;
    }
    return ret;
}

int LibPalfEnvFFIInstance::validate_object_store_url(const char *encrypted_access_point)
{
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *ffi = nullptr;
    HEAP_VAR(LogserviceAccessPointBuf, plaintext) {
        int64_t out_len = 0;
        if (OB_ISNULL(encrypted_access_point)) {
            ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(decrypt_logservice_access_point(encrypted_access_point,
            strlen(encrypted_access_point), plaintext.ptr(), plaintext.capacity(), out_len))) {
            PALF_LOG(WARN, "decrypt candidate access point failed", KR(ret));
        } else {
            {
                common::RWLock::RLockGuard guard(lock_);
                ffi = libpalf_env_ffi_;
            }
            if (OB_ISNULL(ffi)) {
                ret = OB_NOT_INIT;
            } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_validate_object_store_url(ffi, plaintext.ptr())))) {
                PALF_LOG(WARN, "validate candidate access point failed", KR(ret));
            }
        }
    } else {
        PALF_LOG(WARN, "allocate candidate access point failed", KR(ret));
    }
    return ret;
}

int LibPalfEnvFFIInstance::update_object_store_url(const char *encrypted_access_point)
{
    int ret = OB_SUCCESS;
    HEAP_VAR(LogserviceAccessPointBuf, plaintext) {
        int64_t out_len = 0;
        if (OB_ISNULL(encrypted_access_point) || '\0' == encrypted_access_point[0]) {
            ret = OB_INVALID_ARGUMENT;
        } else if (OB_FAIL(decrypt_logservice_access_point(encrypted_access_point,
            strlen(encrypted_access_point), plaintext.ptr(), plaintext.capacity(), out_len))) {
            PALF_LOG(WARN, "decrypt candidate access point failed", KR(ret));
        } else {
            lib::ObMutexGuard update_guard(update_lock_);
            const LibPalfEnvFFI *ffi = nullptr;
            {
                common::RWLock::RLockGuard guard(lock_);
                if (IS_NOT_INIT) {
                    ret = OB_NOT_INIT;
                } else if (0 == STRCMP(logservice_access_point_.ptr(), plaintext.ptr())) {
                    // The current configuration has already been applied.
                } else {
                    // The environment remains valid for the process lifetime.
                    ffi = libpalf_env_ffi_;
                }
            }
            if (OB_SUCC(ret) && OB_NOT_NULL(ffi)) {
                // Keep object-storage I/O outside lock_ so readers can continue.
                if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_update_object_store_url(ffi, plaintext.ptr())))) {
                    PALF_LOG(WARN, "update logservice access point failed", KR(ret));
                } else {
                    common::RWLock::WLockGuard guard(lock_);
                    if (OB_FAIL(logservice_access_point_.assign(plaintext))) {
                        // Both buffers have the same capacity; no allocation is needed.
                        PALF_LOG(ERROR, "cache updated logservice access point failed", KR(ret));
                    } else {
                        PALF_LOG(INFO, "update logservice access point succeeded");
                    }
                }
            }
        }
    } else {
        PALF_LOG(WARN, "allocate candidate access point failed", KR(ret));
    }
    return ret;
}

} // end namespace libpalf
} // end namespace oceanbase
