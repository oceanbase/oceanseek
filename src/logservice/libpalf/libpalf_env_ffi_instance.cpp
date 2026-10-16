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

LibPalfEnvFFIInstance::LibPalfEnvFFIInstance(): lock_(common::ObLatchIds::LIB_PALF_ENV_F_F_I_INSTANCE_LOCK), libpalf_env_ffi_(NULL),
    logservice_access_point_(), is_inited_(false) {}

int LibPalfEnvFFIInstance::parse_param_field_(const char *field_prefix, const char* const data,
        const int64_t data_len, LogserviceAccessPointBuf& out_buf)
{
    int ret = OB_SUCCESS;
    char* field_begin = NULL;
    char* field_end = NULL;
    if (OB_ISNULL(field_begin = std::strstr(
        const_cast<char*>(data), const_cast<char*>(field_prefix)))) {
        ret = OB_SEARCH_NOT_FOUND;
        PALF_LOG(WARN, "invalid argument, do not find logservice_cluster_id", KR(ret));
    } else if (field_begin + 1 >= data + data_len) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument, field_begin + 1 >= data + data_len", KR(ret));
    } else if (OB_ISNULL(field_end = std::strchr(field_begin + 1, '&')) &&
        FALSE_IT(field_end = const_cast<char*>(data) + data_len)) { // do nothing
    } else if (field_end - field_begin + 1 > out_buf.capacity()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument, cluster_id too long", KR(ret), KP(field_begin), KP(field_end));
    } else {
        memcpy(out_buf.ptr(), field_begin, field_end - field_begin);
        out_buf.ptr()[field_end - field_begin] = '\0';
        PALF_LOG(INFO, "parse_cluster_id success", K(out_buf));
    }
    return ret;
}

int LibPalfEnvFFIInstance::encrypt_logservice_access_point(const char* const data,
        const int64_t data_len, char* const buf, const int64_t buf_len, int64_t& out_len)
{
    int ret = OB_SUCCESS;
    ObString logservice_access_point;
    obrpc::ObAdminStorageArg arg;
    share::ObBackupDest backup_dest;
    share::ObBackupStorageInfo *storage_info = NULL;
    int64_t pos = 0;
    int64_t inner_out_len = 0;
    HEAP_VAR(LogserviceAccessPointBuf, cluster_id) {
    HEAP_VAR(LogserviceAccessPointBuf, s3_region) {
        if (OB_ISNULL(data) || data_len <= 0 || OB_ISNULL(buf) || buf_len <= 0) {
            ret = OB_INVALID_ARGUMENT;
            PALF_LOG(WARN, "invalid argument", K(OB_ISNULL(data)), K(data_len),
                K(OB_ISNULL(buf)), K(buf_len));
        } else if (FALSE_IT(logservice_access_point.assign(const_cast<char*>(data), data_len))) {
        } else if (OB_FAIL(parse_param_field_(LibPalfEnvFFIInstance::logservice_cluster_id_prefix,
            data, data_len, cluster_id))) {
            PALF_LOG(WARN, "parse_cluster_id_ failed", KR(ret));
        } else if (OB_FAIL(parse_param_field_(LibPalfEnvFFIInstance::s3_region_prefix,
            data, data_len, s3_region)) && ret != OB_SEARCH_NOT_FOUND) {
            PALF_LOG(WARN, "parse_s3_region failed", KR(ret));
        } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(
            logservice_access_point, arg, backup_dest))) {
            PALF_LOG(WARN, "parse_shared_storage_info failed", KR(ret));
        } else if (FALSE_IT(storage_info = backup_dest.get_storage_info())) { // do nothing
        } else if (OB_ISNULL(storage_info)) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(WARN, "storage_info is NULL", KR(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", arg.path_.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(arg.path_));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s", cluster_id.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(cluster_id));
        } else if (!s3_region.is_empty() &&
            OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s", s3_region.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(s3_region));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s&", arg.attribute_.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(arg.attribute_));
        } else if (OB_FAIL(storage_info->get_authorization_info(buf + pos, buf_len - pos, inner_out_len))) {
            PALF_LOG(WARN, "get_authorization_info failed", KR(ret), K(pos), K(inner_out_len));
        } else if (pos + inner_out_len + 1 > buf_len) {
            ret = OB_BUF_NOT_ENOUGH;
            PALF_LOG(WARN, "buf is not enough", K(pos), K(inner_out_len), K(buf_len));
        } else {
            buf[pos + inner_out_len] = '\0';
            out_len = pos + inner_out_len + 1;
            PALF_LOG(INFO, "encrypt_logservice_access_point success");
        }
    } else {
        PALF_LOG(WARN, "alloc s3_region failed", KR(ret));
    }
    } else {
        PALF_LOG(WARN, "alloc cluster_id failed", KR(ret));
    }
    return ret;
}

int LibPalfEnvFFIInstance::decrypt_logservice_access_point(const char* const data,
        const int64_t data_len, char* const buf, const int64_t buf_len, int64_t& out_len)
{
    int ret = OB_SUCCESS;
    ObString logservice_access_point;
    obrpc::ObAdminStorageArg arg;
    share::ObBackupDest backup_dest;
    share::ObBackupStorageInfo *storage_info = NULL;
    int64_t pos = 0;
    int64_t inner_out_len = 0;
    HEAP_VAR(LogserviceAccessPointBuf, cluster_id) {
    HEAP_VAR(LogserviceAccessPointBuf, s3_region) {
        if (OB_ISNULL(data) || data_len <= 0 || OB_ISNULL(buf) || buf_len <= 0) {
            ret = OB_INVALID_ARGUMENT;
            PALF_LOG(WARN, "invalid argument", K(OB_ISNULL(data)), K(data_len),
                K(OB_ISNULL(buf)), K(buf_len));
        } else if (FALSE_IT(logservice_access_point.assign(const_cast<char*>(data), data_len))) {
        } else if (OB_FAIL(parse_param_field_(LibPalfEnvFFIInstance::logservice_cluster_id_prefix,
            data, data_len, cluster_id))) {
            PALF_LOG(WARN, "parse_cluster_id_ failed", KR(ret));
        } else if (OB_FAIL(parse_param_field_(LibPalfEnvFFIInstance::s3_region_prefix,
            data, data_len, s3_region)) && ret != OB_SEARCH_NOT_FOUND) {
            PALF_LOG(WARN, "parse_s3_region failed", KR(ret));
        } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(
            logservice_access_point, arg, backup_dest))) {
            PALF_LOG(WARN, "parse_shared_storage_info failed", KR(ret));
        } else if (FALSE_IT(storage_info = backup_dest.get_storage_info())) { // do nothing
        } else if (OB_ISNULL(storage_info)) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(WARN, "storage_info is NULL", KR(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", arg.path_.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(arg.path_));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s", cluster_id.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(cluster_id));
        } else if (!s3_region.is_empty() &&
            OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s", s3_region.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(s3_region));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "&%s&", arg.attribute_.ptr()))) {
            PALF_LOG(WARN, "databuff_printf failed", KR(ret), K(pos), K(arg.attribute_));
        } else if (OB_FAIL(storage_info->get_unencrypted_authorization_info(buf + pos, buf_len - pos,
            inner_out_len))) {
            PALF_LOG(WARN, "get_authorization_info failed", KR(ret), K(pos), K(inner_out_len));
        } else if (pos + inner_out_len + 1 > buf_len) {
            ret = OB_BUF_NOT_ENOUGH;
            PALF_LOG(WARN, "buf is not enough", K(pos), K(inner_out_len), K(buf_len));
        } else {
            buf[pos + inner_out_len] = '\0';
            out_len = pos + inner_out_len + 1;
            PALF_LOG(INFO, "decrypt_logservice_access_point success");
        }
    } else {
        PALF_LOG(WARN, "alloc s3_region failed", KR(ret));
    }
    } else {
        PALF_LOG(WARN, "alloc cluster_id failed", KR(ret));
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

int LibPalfEnvFFIInstance::check_non_aksk_fields_match(const char *old_encrypted, int64_t old_len,
                                                       const char *new_encrypted, int64_t new_len)
{
    int ret = OB_SUCCESS;
    HEAP_VAR(LogserviceAccessPointBuf, old_decrypted) {
    HEAP_VAR(LogserviceAccessPointBuf, new_decrypted) {
    HEAP_VAR(LogserviceAccessPointBuf, old_cluster_id) {
    HEAP_VAR(LogserviceAccessPointBuf, new_cluster_id) {
    HEAP_VAR(LogserviceAccessPointBuf, old_s3_region) {
    HEAP_VAR(LogserviceAccessPointBuf, new_s3_region) {
        int64_t old_out_len = 0;
        int64_t new_out_len = 0;
        ObString old_access_point_str;
        ObString new_access_point_str;
        obrpc::ObAdminStorageArg old_arg;
        obrpc::ObAdminStorageArg new_arg;
        share::ObBackupDest old_backup_dest;
        share::ObBackupDest new_backup_dest;
        if (OB_ISNULL(old_encrypted) || old_len <= 0 || OB_ISNULL(new_encrypted) || new_len <= 0) {
            ret = OB_INVALID_ARGUMENT;
            PALF_LOG(WARN, "invalid argument", KR(ret), KP(old_encrypted), K(old_len),
                KP(new_encrypted), K(new_len));
        } else if (OB_FAIL(decrypt_logservice_access_point(
            old_encrypted, old_len,
            old_decrypted.ptr(), old_decrypted.capacity(), old_out_len))) {
            PALF_LOG(WARN, "decrypt old logservice_access_point failed", KR(ret));
        } else if (OB_FAIL(decrypt_logservice_access_point(
            new_encrypted, new_len,
            new_decrypted.ptr(), new_decrypted.capacity(), new_out_len))) {
            PALF_LOG(WARN, "decrypt new logservice_access_point failed", KR(ret));
        } else if (FALSE_IT(old_access_point_str.assign(old_decrypted.ptr(), static_cast<int32_t>(old_out_len - 1)))) {
        } else if (FALSE_IT(new_access_point_str.assign(new_decrypted.ptr(), static_cast<int32_t>(new_out_len - 1)))) {
        } else if (OB_FAIL(parse_param_field_(logservice_cluster_id_prefix,
            old_decrypted.ptr(), old_out_len - 1, old_cluster_id))) {
            PALF_LOG(WARN, "parse old logservice_cluster_id failed", KR(ret));
        } else if (OB_FAIL(parse_param_field_(logservice_cluster_id_prefix,
            new_decrypted.ptr(), new_out_len - 1, new_cluster_id))) {
            PALF_LOG(WARN, "parse new logservice_cluster_id failed", KR(ret));
        } else if (OB_FAIL(parse_param_field_(s3_region_prefix,
            old_decrypted.ptr(), old_out_len - 1, old_s3_region))
            && OB_SEARCH_NOT_FOUND != ret) {
            PALF_LOG(WARN, "parse old s3_region failed", KR(ret));
        } else if (FALSE_IT(ret = OB_SUCCESS)) {
        } else if (OB_FAIL(parse_param_field_(s3_region_prefix,
            new_decrypted.ptr(), new_out_len - 1, new_s3_region))
            && OB_SEARCH_NOT_FOUND != ret) {
            PALF_LOG(WARN, "parse new s3_region failed", KR(ret));
        } else if (FALSE_IT(ret = OB_SUCCESS)) {
        } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(
            old_access_point_str, old_arg, old_backup_dest))) {
            PALF_LOG(WARN, "parse old shared_storage_info failed", KR(ret));
        } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(
            new_access_point_str, new_arg, new_backup_dest))) {
            PALF_LOG(WARN, "parse new shared_storage_info failed", KR(ret));
        } else {
            // Compare path
            if (0 != STRCMP(old_arg.path_.ptr(), new_arg.path_.ptr())) {
                ret = OB_INVALID_ARGUMENT;
                PALF_LOG(WARN, "path mismatch during AK/SK rotation",
                    KR(ret), K(old_arg.path_), K(new_arg.path_));
            }
            // Compare attribute
            if (OB_SUCC(ret) && 0 != STRCMP(old_arg.attribute_.ptr(), new_arg.attribute_.ptr())) {
                ret = OB_INVALID_ARGUMENT;
                PALF_LOG(WARN, "attribute mismatch during AK/SK rotation",
                    KR(ret), K(old_arg.attribute_), K(new_arg.attribute_));
            }
            // Compare logservice_cluster_id
            if (OB_SUCC(ret) && 0 != STRCMP(old_cluster_id.ptr(), new_cluster_id.ptr())) {
                ret = OB_INVALID_ARGUMENT;
                PALF_LOG(WARN, "logservice_cluster_id mismatch during AK/SK rotation",
                    KR(ret), K(old_cluster_id), K(new_cluster_id));
            }
            // Compare s3_region
            if (OB_SUCC(ret) && 0 != STRCMP(old_s3_region.ptr(), new_s3_region.ptr())) {
                ret = OB_INVALID_ARGUMENT;
                PALF_LOG(WARN, "s3_region mismatch during AK/SK rotation",
                    KR(ret), K(old_s3_region), K(new_s3_region));
            }
        }
    } else { PALF_LOG(WARN, "alloc new_s3_region failed", KR(ret)); }
    } else { PALF_LOG(WARN, "alloc old_s3_region failed", KR(ret)); }
    } else { PALF_LOG(WARN, "alloc new_cluster_id failed", KR(ret)); }
    } else { PALF_LOG(WARN, "alloc old_cluster_id failed", KR(ret)); }
    } else { PALF_LOG(WARN, "alloc new_decrypted failed", KR(ret)); }
    } else { PALF_LOG(WARN, "alloc old_decrypted failed", KR(ret)); }
    return ret;
}

int LibPalfEnvFFIInstance::parse_aksk_from_encrypted_access_point_(
    const char *encrypted_access_point,
    LogserviceAccessPointBuf &out_decrypted, int64_t &out_decrypted_len,
    char *out_raw_ak_id, int64_t ak_id_buf_len,
    char *out_raw_ak_secret, int64_t ak_secret_buf_len)
{
    int ret = OB_SUCCESS;
    ObString access_point_str;
    obrpc::ObAdminStorageArg arg;
    share::ObBackupDest backup_dest;
    share::ObBackupStorageInfo *storage_info = NULL;
    if (OB_ISNULL(encrypted_access_point) || 0 == strlen(encrypted_access_point)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", KR(ret), KP(encrypted_access_point));
    } else if (OB_FAIL(decrypt_logservice_access_point(
        encrypted_access_point, strlen(encrypted_access_point),
        out_decrypted.ptr(), out_decrypted.capacity(), out_decrypted_len))) {
        PALF_LOG(WARN, "decrypt logservice_access_point failed", KR(ret));
    } else if (FALSE_IT(access_point_str.assign(
        out_decrypted.ptr(), static_cast<int32_t>(out_decrypted_len - 1)))) {
    } else if (OB_FAIL(share::ObStorageDestCheck::parse_shared_storage_info(
        access_point_str, arg, backup_dest))) {
        PALF_LOG(WARN, "parse_shared_storage_info failed", KR(ret));
    } else if (FALSE_IT(storage_info = backup_dest.get_storage_info())) {
    } else if (OB_ISNULL(storage_info)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "storage_info is NULL", KR(ret));
    } else {
        const char *ak_id_with_prefix = storage_info->access_id_;
        const char *ak_key_with_prefix = storage_info->access_key_;
        const int64_t access_id_prefix_len = strlen(common::ACCESS_ID);
        const int64_t access_key_prefix_len = strlen(common::ACCESS_KEY);
        const char *raw_ak_id = ak_id_with_prefix;
        const char *raw_ak_secret = ak_key_with_prefix;
        if (0 == strncmp(ak_id_with_prefix, common::ACCESS_ID, access_id_prefix_len)) {
            raw_ak_id = ak_id_with_prefix + access_id_prefix_len;
        }
        if (0 == strncmp(ak_key_with_prefix, common::ACCESS_KEY, access_key_prefix_len)) {
            raw_ak_secret = ak_key_with_prefix + access_key_prefix_len;
        }
        if (OB_FAIL(databuff_printf(out_raw_ak_id, ak_id_buf_len, "%s", raw_ak_id))) {
            PALF_LOG(WARN, "copy raw_ak_id failed", KR(ret));
        } else if (OB_FAIL(databuff_printf(out_raw_ak_secret, ak_secret_buf_len, "%s", raw_ak_secret))) {
            PALF_LOG(WARN, "copy raw_ak_secret failed", KR(ret));
        }
    }
    return ret;
}

int LibPalfEnvFFIInstance::rotate_object_store_ak(const char *encrypted_new_access_point)
{
    int ret = OB_SUCCESS;
    HEAP_VAR(LogserviceAccessPointBuf, new_decrypted) {
        int64_t new_out_len = 0;
        char raw_ak_id[OB_MAX_BACKUP_ACCESSID_LENGTH] = {0};
        char raw_ak_secret[OB_MAX_BACKUP_ACCESSKEY_LENGTH] = {0};
        if (OB_FAIL(parse_aksk_from_encrypted_access_point_(
            encrypted_new_access_point, new_decrypted, new_out_len,
            raw_ak_id, sizeof(raw_ak_id), raw_ak_secret, sizeof(raw_ak_secret)))) {
            PALF_LOG(WARN, "parse AK/SK from encrypted access point failed", KR(ret));
        } else {
            common::RWLock::WLockGuard guard(lock_);
            if (IS_NOT_INIT) {
                ret = OB_NOT_INIT;
                PALF_LOG(WARN, "LibPalfEnvFFIInstance is not inited, cannot rotate AK/SK", KR(ret));
            } else if (0 == STRCMP(new_decrypted.ptr(), logservice_access_point_.ptr())) {
                PALF_LOG(DEBUG, "logservice_access_point unchanged, skip rotation");
            } else if (OB_FAIL(LIBPALF_ERRNO_CAST(
                libpalf_rotate_object_store_ak(libpalf_env_ffi_, raw_ak_id, raw_ak_secret)))) {
                PALF_LOG(WARN, "libpalf_rotate_object_store_ak failed", KR(ret));
            } else if (OB_FAIL(logservice_access_point_.assign(new_decrypted.ptr()))) {
                PALF_LOG(WARN, "update logservice_access_point_ cache failed", KR(ret));
            } else {
                PALF_LOG(INFO, "rotate logservice AK/SK success");
            }
        }
    } else {
        PALF_LOG(WARN, "alloc new_decrypted failed", KR(ret));
    }
    return ret;
}

int LibPalfEnvFFIInstance::validate_object_store_ak(const char *encrypted_new_access_point)
{
    int ret = OB_SUCCESS;
    HEAP_VAR(LogserviceAccessPointBuf, new_decrypted) {
        int64_t new_out_len = 0;
        char raw_ak_id[OB_MAX_BACKUP_ACCESSID_LENGTH] = {0};
        char raw_ak_secret[OB_MAX_BACKUP_ACCESSKEY_LENGTH] = {0};
        if (OB_FAIL(parse_aksk_from_encrypted_access_point_(
            encrypted_new_access_point, new_decrypted, new_out_len,
            raw_ak_id, sizeof(raw_ak_id), raw_ak_secret, sizeof(raw_ak_secret)))) {
            PALF_LOG(WARN, "parse AK/SK from encrypted access point failed", KR(ret));
        } else {
            common::RWLock::RLockGuard guard(lock_);
            if (IS_NOT_INIT) {
                ret = OB_NOT_INIT;
                PALF_LOG(WARN, "LibPalfEnvFFIInstance is not inited, cannot validate AK/SK", KR(ret));
            } else if (OB_FAIL(LIBPALF_ERRNO_CAST(
                libpalf_validate_object_store_ak(libpalf_env_ffi_, raw_ak_id, raw_ak_secret)))) {
                PALF_LOG(WARN, "libpalf_validate_object_store_ak failed", KR(ret));
            } else {
                PALF_LOG(INFO, "validate logservice AK/SK success");
            }
        }
    } else {
        PALF_LOG(WARN, "alloc new_decrypted failed", KR(ret));
    }
    return ret;
}

} // end namespace libpalf
} // end namespace oceanbase
