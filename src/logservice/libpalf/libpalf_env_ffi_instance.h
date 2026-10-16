/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_ENV_FFI_INSTANCE_
#define OCEANBASE_LOGSERVICE_LIBPALF_ENV_FFI_INSTANCE_

#include "lib/lock/ob_tc_rwlock.h"
#include "palf_ffi.h"

namespace oceanbase
{
namespace libpalf
{
class LibPalfEnvFFIInstance
{
public:
    typedef common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> LogserviceAccessPointBuf;
    static constexpr char encrypt_prefix[] = "encrypted_access_info=";
    static constexpr char logservice_cluster_id_prefix[] = "logservice_cluster_id=";
    static constexpr char s3_region_prefix[] = "s3_region=";
public:
    static int encrypt_logservice_access_point(const char* const data,
        const int64_t data_len, char* const buf, const int64_t buf_len, int64_t& out_len);
    static int decrypt_logservice_access_point(const char* const data,
        const int64_t data_len, char* const buf, const int64_t buf_len, int64_t& out_len);
private:
    static int parse_param_field_(const char *field_prefix, const char* const data,
        const int64_t data_len, LogserviceAccessPointBuf& out_cluster_id);
public:
    static LibPalfEnvFFIInstance &get_instance();

    LibPalfEnvFFIInstance();
    int init();
    int get_ffi(const LibPalfEnvFFI *& ffi);
    int set_logservice_access_point(const char *logservice_access_point);
    int get_logservice_access_point(LogserviceAccessPointBuf& out_logservice_access_point) const;
    static int check_non_aksk_fields_match(const char *old_encrypted, int64_t old_len,
                                           const char *new_encrypted, int64_t new_len);
    int rotate_object_store_ak(const char *encrypted_new_access_point);
    int validate_object_store_ak(const char *encrypted_new_access_point);
private:
    int do_init_();
    int do_refresh_logservice_access_point_();
    static int parse_aksk_from_encrypted_access_point_(const char *encrypted_access_point,
        LogserviceAccessPointBuf &out_decrypted, int64_t &out_decrypted_len,
        char *out_raw_ak_id, int64_t ak_id_buf_len,
        char *out_raw_ak_secret, int64_t ak_secret_buf_len);
private:
    mutable common::RWLock lock_;
    const LibPalfEnvFFI* libpalf_env_ffi_;
    LogserviceAccessPointBuf logservice_access_point_;

    bool is_inited_;
};
} // end namespace libpalf
} // end namespace oceanbase

#endif