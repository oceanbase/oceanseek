/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_ENV_FFI_INSTANCE_
#define OCEANBASE_LOGSERVICE_LIBPALF_ENV_FFI_INSTANCE_

#include "lib/lock/ob_mutex.h"
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
    int validate_object_store_url(const char *encrypted_access_point);
    int update_object_store_url(const char *encrypted_access_point);
private:
    int do_init_();
    int do_refresh_logservice_access_point_();
private:
    // Serialize access-point writes; acquire before lock_.
    lib::ObMutex update_lock_;
    mutable common::RWLock lock_;
    const LibPalfEnvFFI* libpalf_env_ffi_;
    LogserviceAccessPointBuf logservice_access_point_;

    bool is_inited_;
};
} // end namespace libpalf
} // end namespace oceanbase

#endif
