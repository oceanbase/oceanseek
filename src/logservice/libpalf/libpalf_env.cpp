/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "palf_ffi.h"
#include "libpalf_common_define.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ls/ob_ls_status_operator.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "logservice/ipalf/ipalf_define.h"
#include "logservice/ipalf/ipalf_env.h"
#include "logservice/ipalf/interface_structs.h"
#include "logservice/palf/log_define.h"
#include "share/config/ob_server_config.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"

#include "libpalf_env_ffi_instance.h"
#include "libpalf_env.h"
#include "libpalf_handle.h"

#define RETRY_LIBPALF_FUNC_ON_ERROR(timeout_us, func, args...) \
    do { \
        int64_t abs_timeout_ts = (timeout_us > 0) ? (timeout_us + ObTimeUtility::current_time()) : INT64_MAX; \
        int retry_cnt = 0; \
        int last_ret = OB_SUCCESS; \
        do { \
            last_ret = LIBPALF_ERRNO_CAST(func(args)); \
            if (OB_FAIL(last_ret)) { /* assign last_ret to ret in OB_FAIL */\
                if (OB_ENTRY_NOT_EXIST == ret) { \
                    PALF_LOG(WARN, #func" execute failed and entry not exist", KR(last_ret), K(retry_cnt)); \
                    break; \
                } else if (ObTimeUtility::current_time() >= abs_timeout_ts) { \
                    PALF_LOG(WARN, #func" execute failed and timeout/reach max retry", \
                            KR(last_ret), K(retry_cnt), K(timeout_us)); \
                    break; \
                } else { \
                    PALF_LOG(WARN, #func" execute failed, retrying", KR(last_ret), K(retry_cnt)); \
                    retry_cnt++; \
                    ob_usleep(100 * 1000 * (1 << (retry_cnt % 3))); /* 指数退避 */ \
                } \
            } \
        } while (OB_FAIL(last_ret)); \
    } while (0)

namespace oceanbase
{
namespace libpalf
{

void LibPalfEnv::destroy_palf_env(LibPalfEnv *&palf_env)
{
    MTL_DELETE(LibPalfEnv, "libpalf_env", palf_env);
    PALF_LOG_RET(WARN, OB_SUCCESS, "destroy_palf_env success");
}

int LibPalfEnv::create_palf_env(ipalf::LibPalfEnvCreateParams *params, LibPalfEnv *&libpalf_env)
{
    int ret = OB_SUCCESS;
    if (!GCONF.enable_logservice) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "logservice is disabled, cannot create libpalf env", K(ret), K(GCONF.enable_logservice));
    } else if (OB_ISNULL(params) || OB_NOT_NULL(libpalf_env)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(params), K(libpalf_env));
    } else if (OB_ISNULL(libpalf_env = MTL_NEW(LibPalfEnv, "LibPalfEnv"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(WARN, "alloc mem for LibPalfEnv failed", K(ret));
    } else if (OB_FAIL(libpalf_env->do_init_(params))) {
        PALF_LOG(WARN, "init libpalf env failed", K(ret), K(libpalf_env));
    } else {
        PALF_LOG(INFO, "create LibPalfEnv success");
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_env)) {
        MTL_DELETE(LibPalfEnv, "LibPalfEnv", libpalf_env);
    }
    return ret;
}

LibPalfEnv::LibPalfEnv() :
    is_inited_(false),
    is_loaded_(false),
    monitor_(NULL),
    tenant_id_(OB_INVALID_TENANT_ID),
    log_updater_()
{}

LibPalfEnv::~LibPalfEnv()
{
    destroy();
}

void LibPalfEnv::destroy()
{
    is_inited_ = false;
    is_loaded_ = false;
    monitor_ = NULL;
    tenant_id_ = OB_INVALID_TENANT_ID;
    log_updater_.destroy();
}

int LibPalfEnv::do_init_(ipalf::LibPalfEnvCreateParams *params)
{
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    if (OB_ISNULL(params) || OB_ISNULL(params->monitor_)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument!!!", K(ret), K(params));
    } else {
        monitor_ = params->monitor_;
        tenant_id_ = MTL_ID();
        is_inited_ = true;
        PALF_LOG(INFO, "init LibPalfEnv success");
    }
    return ret;
}

// this function is concurrent safe
// cause destroy() is guaranteed to be called without concurrency of other interfaces of LibPalfEnv
int LibPalfEnv::do_lazy_load_()
{
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    char cluster_id_str[INTEGER_BUF_LEN] = {0};
    char tenant_id_str[INTEGER_BUF_LEN] = {0};
    int timeout_us = 30000000; // 30 secs
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfEnv not init", K(ret));
    } else if (is_loaded_) { // do nothing
    } else if (OB_FAIL(generate_sub_path_(MTL_ID(), cluster_id_str, tenant_id_str))) {
        PALF_LOG(WARN, "generate_sub_path_ failed", K(ret));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().init())) {
        PALF_LOG(ERROR, "init LibPalfEnvFFIInstance error", K(ret));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
        PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
    } else {
        RETRY_LIBPALF_FUNC_ON_ERROR(timeout_us, libpalf_mkdir, libpalf_env_ffi, cluster_id_str, tenant_id_str);
        if (OB_FAIL(ret)) {
            PALF_LOG(WARN, "libpalf_mkdir failed", K(ret), K(timeout_us));
        } else if (OB_FAIL(log_updater_.init())) {
            PALF_LOG(WARN, "log_updater_ init failed", KR(ret));
        } else {
            is_loaded_ = true;
            PALF_LOG(INFO, "lazy load LibPalfEnv success");
        }
    }

    return ret;
}

bool LibPalfEnv::operator==(const ipalf::IPalfEnv &rhs) const
{
    return this == static_cast<const LibPalfEnv*>(&rhs);
}

int LibPalfEnv::do_create_libpalf_handle_(const int64_t palf_id,
                                          const LibPalfAccessMode &libpalf_access_mode,
                                          const LibPalfBaseInfo &libpalf_base_info,
                                          const LibPalfHandleFFI *&out_libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    const LibPalfHandleFFI *libpalf_handle_ffi = NULL;
    char cluster_id_str[INTEGER_BUF_LEN] = {0};
    char tenant_id_str[INTEGER_BUF_LEN] = {0};
    char palf_id_str[INTEGER_BUF_LEN] = {0};
    if (OB_FAIL(generate_file_path_(tenant_id_, palf_id, cluster_id_str, tenant_id_str, palf_id_str))) {
        PALF_LOG(WARN, "generate_cluster_tenant_str_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
        PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret), K(palf_id));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_create(libpalf_env_ffi, cluster_id_str,
        tenant_id_str, palf_id_str, libpalf_access_mode, &libpalf_base_info, &libpalf_handle_ffi)))) {
        PALF_LOG(WARN, "libpalf_create failed", K(ret), K(cluster_id_str),
            K(tenant_id_str), K(palf_id_str), K(libpalf_access_mode));
    } else if (OB_ISNULL(libpalf_handle_ffi)) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "libpalf_handle_ffi is null", K(ret), K(palf_id), K(libpalf_handle_ffi));
    } else {
        out_libpalf_handle_ffi = libpalf_handle_ffi;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle_ffi)) {
        close_libpalf_handle_(libpalf_handle_ffi);
    }
    return ret;
}

int LibPalfEnv::create(const int64_t palf_id,
                       const ipalf::AccessMode &access_mode,
                       const palf::PalfBaseInfo &palf_base_info,
                       ipalf::IPalfHandle *&handle)
{
    int ret = OB_SUCCESS;
    const LibPalfHandleFFI *libpalf_handle_ffi = NULL;
    LibPalfHandle *libpalf_handle = NULL;

    LibPalfAccessMode libpalf_access_mode;
    LibPalfBaseInfo libpalf_base_info(0, LibPalfLogInfo(0, 0, 0, 0, 0));
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "LibPalfEnv is not inited", K(ret), K(palf_id));
    } else if (OB_FAIL(do_lazy_load_())) {
        PALF_LOG(WARN, "lazy load libpalf env failed", K(ret), K(palf_id));
    } else if (false == ipalf::is_valid_palf_id(palf_id)
            || OB_FAIL(transfer_access_mode(access_mode, libpalf_access_mode))
            || OB_NOT_NULL(handle)
            || OB_FAIL(transfer_palf_base_info_to_libpalf_base_info(
                palf_base_info, libpalf_base_info))) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument",
            K(ret), K(palf_id), K(access_mode), K(palf_base_info), K(handle));
    } else if (OB_FAIL(do_create_libpalf_handle_(
        palf_id,
        libpalf_access_mode,
        libpalf_base_info,
        libpalf_handle_ffi))) {
        PALF_LOG(WARN, "do_create_libpalf_handle_ failed",
            K(ret), K(palf_id), K(libpalf_access_mode), K(libpalf_handle_ffi));
    } else if (OB_ISNULL(libpalf_handle = MTL_NEW(LibPalfHandle, "LibPalfHandle"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(WARN, "alloc LibPalfHandle failed", K(ret), K(palf_id));
    } else if (OB_FAIL(libpalf_handle->init(palf_id, libpalf_handle_ffi))) {
        PALF_LOG(WARN, "init LibPalfHandle failed", K(ret), K(palf_id), K(libpalf_handle_ffi));
    } else {
        // TODO: tc options?
        // TODO: enable log cache?
        (void) libpalf_handle->set_monitor_cb(monitor_);
        handle = libpalf_handle;
        PALF_LOG(INFO, "create libpalf handle success", K(ret), K(palf_id),
            K(access_mode), K(palf_base_info), K(libpalf_handle_ffi));
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle)) {
        MTL_DELETE(LibPalfHandle, "LibPalfHandle", libpalf_handle);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle_ffi)) {
        close_libpalf_handle_(libpalf_handle_ffi);
    }
    return ret;
}

int LibPalfEnv::start()
{
    int ret = OB_SUCCESS;
    return ret;
}

int LibPalfEnv::do_open_libpalf_handle_(const int64_t palf_id, const bool open_remote, const LibPalfHandleFFI *&out_libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    char cluster_id_str[INTEGER_BUF_LEN] = {0};
    char tenant_id_str[INTEGER_BUF_LEN] = {0};
    char palf_id_str[INTEGER_BUF_LEN] = {0};
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    const LibPalfHandleFFI *libpalf_handle_ffi = NULL;
    int timeout_us = 30000000; // 30 secs

    if (OB_FAIL(generate_file_path_(tenant_id_, palf_id, cluster_id_str, tenant_id_str, palf_id_str))) {
        PALF_LOG(WARN, "generate_cluster_tenant_str_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
        PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret), K(palf_id));
    } else if (true == open_remote) {
        RETRY_LIBPALF_FUNC_ON_ERROR(timeout_us, libpalf_open, libpalf_env_ffi, cluster_id_str, tenant_id_str, palf_id_str, &libpalf_handle_ffi);
        if (OB_FAIL(ret)) {
            PALF_LOG(WARN, "libpalf_open failed", K(ret), K(palf_id), K(timeout_us));
        }
    } else if (false == open_remote &&
        OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_get_file(libpalf_env_ffi, cluster_id_str, tenant_id_str,
            palf_id_str, &libpalf_handle_ffi)))) {
        PALF_LOG(WARN, "libpalf_get_file failed", K(ret), K(palf_id));
    }

    if (OB_SUCC(ret)) {
        if (OB_ISNULL(libpalf_handle_ffi)) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "libpalf_handle_ffi is NULL", K(ret), K(palf_id), K(open_remote));
        } else {
            out_libpalf_handle_ffi = libpalf_handle_ffi;
        }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle_ffi)) {
        close_libpalf_handle_(libpalf_handle_ffi);
    }
    return ret;
}

int LibPalfEnv::load(const int64_t id, ipalf::IPalfHandle *&handle)
{
    int ret = OB_SUCCESS;
    const LibPalfHandleFFI *libpalf_handle_ffi = NULL;
    LibPalfHandle *libpalf_handle = NULL;
    const bool open_remote = true;

    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(ERROR, "not init", K(ret), K(id));
    } else if (OB_FAIL(do_lazy_load_())) {
        PALF_LOG(WARN, "lazy load libpalf env failed", K(ret));
    } else if (false == ipalf::is_valid_palf_id(id) || OB_NOT_NULL(handle)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(id), K(handle));
    } else if (OB_ISNULL(libpalf_handle = MTL_NEW(LibPalfHandle, "LibPalfHandle"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(ERROR, "alloc LibPalfHandle failed", K(ret), K(id));
    } else if (OB_FAIL(do_open_libpalf_handle_(id, open_remote, libpalf_handle_ffi))) {
        PALF_LOG(WARN, "open libpalf handle failed", K(ret), K(id));
    } else if (OB_FAIL(libpalf_handle->init(id, libpalf_handle_ffi))) {
        PALF_LOG(WARN, "init LibPalfHandle failed", K(ret), K(id), KP(libpalf_handle_ffi));
    } else {
        (void) libpalf_handle->set_monitor_cb(monitor_);
        handle = libpalf_handle;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle)) {
        MTL_DELETE(LibPalfHandle, "LibPalfHandle", libpalf_handle);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle_ffi)) {
        close_libpalf_handle_(libpalf_handle_ffi);
    }
    return ret;
}

int LibPalfEnv::open(const int64_t id, ipalf::IPalfHandle *&handle)
{
    int ret = OB_SUCCESS;
    const LibPalfHandleFFI *libpalf_handle_ffi = NULL;
    LibPalfHandle *libpalf_handle = NULL;
    const bool open_remote = false;

    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(ERROR, "not init", K(ret), K(id));
    } else if (!is_loaded_) {
        ret = OB_ENTRY_NOT_EXIST;
    } else if (false == ipalf::is_valid_palf_id(id) || OB_NOT_NULL(handle)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(id), K(handle));
    } else if (OB_ISNULL(libpalf_handle = MTL_NEW(LibPalfHandle, "LibPalfHandle"))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PALF_LOG(ERROR, "alloc LibPalfHandle failed", K(ret), K(id));
    } else if (OB_FAIL(do_open_libpalf_handle_(id, open_remote, libpalf_handle_ffi))) {
        PALF_LOG(WARN, "open libpalf handle failed", K(ret), K(id));
    } else if (OB_FAIL(libpalf_handle->init(id, libpalf_handle_ffi))) {
        PALF_LOG(WARN, "init LibPalfHandle failed", K(ret), K(id), KP(libpalf_handle_ffi));
    } else {
        (void) libpalf_handle->set_monitor_cb(monitor_);
        handle = libpalf_handle;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle)) {
        MTL_DELETE(LibPalfHandle, "LibPalfHandle", libpalf_handle);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(libpalf_handle_ffi)) {
        close_libpalf_handle_(libpalf_handle_ffi);
    }
    return ret;
}

int LibPalfEnv::close_libpalf_handle_(const LibPalfHandleFFI *libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
        PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret), KP(libpalf_handle_ffi));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_close(libpalf_env_ffi, libpalf_handle_ffi)))) {
        PALF_LOG(ERROR, "close libpalf handle error", K(ret), KP(libpalf_handle_ffi));
    } else {
    }
    return ret;
}

int LibPalfEnv::wait_libpalf_handle_instance_remove_(const int64_t palf_id)
{
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    char cluster_id_str[INTEGER_BUF_LEN] = {0};
    char tenant_id_str[INTEGER_BUF_LEN] = {0};
    char palf_id_str[INTEGER_BUF_LEN] = {0};
    int64_t now_ts = common::ObTimeUtil::current_monotonic_time();
    const int64_t begin_ts = now_ts;

    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "not init", K(ret), K(palf_id));
    } else if (OB_FAIL(generate_file_path_(tenant_id_, palf_id, cluster_id_str, tenant_id_str, palf_id_str))) {
        PALF_LOG(WARN, "generate_cluster_tenant_str_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
        PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret), K(palf_id));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_remove_locally(libpalf_env_ffi, cluster_id_str,
        tenant_id_str, palf_id_str)))) {
        now_ts = common::ObTimeUtil::current_monotonic_time();
        PALF_LOG(WARN, "libpalf_remove_locally failed", K(ret), K(palf_id), K(begin_ts), K(now_ts), K(now_ts - begin_ts));
    } else {
        now_ts = common::ObTimeUtil::current_monotonic_time();
        PALF_LOG(INFO, "libpalf_remove_locally success", K(ret), K(palf_id), K(begin_ts), K(now_ts), K(now_ts - begin_ts));
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
    }
    return ret;
}

int LibPalfEnv::close(ipalf::IPalfHandle *&handle)
{
    int ret = OB_SUCCESS;
    LibPalfHandle *libpalf_handle = static_cast<LibPalfHandle*>(handle);
    int64_t palf_id = 0;
    const LibPalfHandleFFI *ffi = NULL;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "not init", K(ret));
    } else if (OB_FAIL(do_lazy_load_())) {
        PALF_LOG(WARN, "lazy load libpalf env failed", K(ret));
    } else if (OB_ISNULL(libpalf_handle)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(handle));
    } else if (!libpalf_handle->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "invalid libpalf handle", K(ret), K(libpalf_handle));
    } else if (FALSE_IT(palf_id = libpalf_handle->palf_id_)
               || FALSE_IT(ffi = libpalf_handle->ffi_)
               || FALSE_IT(libpalf_handle->destroy())) { // do nothing
    } else if (OB_FAIL(close_libpalf_handle_(ffi))) {
        PALF_LOG(WARN, "close libpalf handle failed", K(ret), K(palf_id), K(libpalf_handle));
    } else {
        handle = NULL;
    }
    if (OB_NOT_NULL(libpalf_handle)) {
        MTL_DELETE(LibPalfHandle, "LibPalfHandle", libpalf_handle);
        handle = NULL;
    }
    return ret;
}

int LibPalfEnv::remove(int64_t id)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "not init", K(ret), K(id));
    } else if (OB_FAIL(do_lazy_load_())) {
        PALF_LOG(WARN, "lazy load libpalf env failed", K(ret), K(id));
    } else if (OB_FAIL(wait_libpalf_handle_instance_remove_(id))) {
        PALF_LOG(WARN, "wait for libpalf handle instance remove failed", K(ret), K(id));
    } else {
        PALF_LOG(INFO, "remove libpalf handle instance success", K(id));
    }
    return ret;
}


int LibPalfEnv::for_each(const ObFunction<int(const ipalf::IPalfHandle&)> &func)
{
    int ret = OB_SUCCESS;
    PalfIdSet palf_id_set;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "not init", K(ret));
    } else if (!is_loaded_) {
        // do nothing
    } else if (OB_FAIL(palf_id_set.create(100))) {
        PALF_LOG(WARN, "create hashset failed", K(ret));
    } else if (OB_FAIL(get_palf_id_set_(palf_id_set))) {
        PALF_LOG(WARN, "get_palf_id_set_ failed", K(ret));
    } else {
        int tmp_ret = OB_SUCCESS;
        common::hash::ObHashSet<int64_t>::const_iterator iter;
        for (iter = palf_id_set.begin(); iter != palf_id_set.end(); ++iter) {
            ipalf::IPalfHandle *ipalf_handle = NULL;
            const int64_t palf_id = iter->first;
            if (OB_TMP_FAIL(open(palf_id, ipalf_handle)) || OB_ISNULL(ipalf_handle)) {
                PALF_LOG(WARN, "open libpalf handle failed", K(ret), K(palf_id), KP(ipalf_handle));
            } else {
                func(*ipalf_handle);
            }
            if (OB_NOT_NULL(ipalf_handle) && ipalf_handle->is_valid()) {
                close(ipalf_handle);
            }
        }
    }
    return ret;
}

int LibPalfEnv::get_palf_id_set_(PalfIdSet &palf_id_set) const
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = NULL;
  libpalf::LibPalfDirInfo *local_entry_info = nullptr;
  char cluster_id_str[INTEGER_BUF_LEN] = {0};
  char tenant_id_str[INTEGER_BUF_LEN] = {0};
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "get LibPalfEnvFFIInstance failed", K(ret));
  } else if (OB_FAIL(generate_sub_path_(tenant_id_, cluster_id_str, tenant_id_str))) {
    PALF_LOG(WARN, "generate_sub_path_ failed", K(ret), K_(tenant_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_open_dir_locally(libpalf_env_ffi,
      cluster_id_str, tenant_id_str, &local_entry_info)))) {
    PALF_LOG(WARN, "libpalf_open_dir_locally failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    libpalf::LibPalfDirEntry dir_entry;
    int64_t palf_id = 0;
    while (OB_SUCCESS == (tmp_ret = LIBPALF_ERRNO_CAST(libpalf_read_dir(local_entry_info, &dir_entry)))) {
        PALF_LOG(TRACE, "libpalf_read_dir", "entry_name", dir_entry.name, "entry_type", dir_entry.entry_type);
        if (dir_entry.entry_type == LibPalfDirEntryType::File) {
            if (OB_SUCCESS == (tmp_ret = ob_atoll(dir_entry.name, palf_id))) {
                palf_id_set.set_refactored(palf_id);
            } else {
                PALF_LOG(ERROR, "unexpected palf_id string", "entry_name", K(dir_entry.name),
                    "entry_type", K(dir_entry.entry_type));
            }
        }
    }
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_close_dir(local_entry_info)))) {
        PALF_LOG(WARN, "libpalf_close_dir failed", KP(local_entry_info));
    }
  }
  return ret;
}

int LibPalfEnv::generate_sub_path_(const uint64_t tenant_id, char *cluster_id_str, char *tenant_id_str) const
{
    int ret = OB_SUCCESS;
    const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    if (tenant_id <= 0 || cluster_id <= 0) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", KR(ret), K(cluster_id), K(tenant_id));
    } else if (OB_FAIL(databuff_printf(cluster_id_str, INTEGER_BUF_LEN, "%ld", cluster_id))) {
        PALF_LOG(WARN, "cluster_id to string error", K(ret), K(cluster_id));
    } else if (OB_FAIL(databuff_printf(tenant_id_str, INTEGER_BUF_LEN, "%ld", tenant_id))) {
        PALF_LOG(WARN, "tenant_id to string error", K(ret), K(tenant_id));
    }
    return ret;
}

int LibPalfEnv::generate_file_path_(
    const uint64_t tenant_id,
    const int64_t palf_id,
    char *cluster_id_str,
    char *tenant_id_str,
    char *palf_id_str) const
{
    int ret = OB_SUCCESS;
    if (OB_FAIL(generate_sub_path_(tenant_id, cluster_id_str, tenant_id_str))) {
    } else if (OB_FAIL(databuff_printf(palf_id_str, INTEGER_BUF_LEN, "%ld", palf_id))) {
        PALF_LOG(WARN, "palf_id to string error", K(ret), K(palf_id));
    }
    return ret;
}

int LibPalfEnv::update_replayable_point(const share::SCN &replayable_scn)
{
    // after reboot, tenant info loader will set this value
    int ret = OB_SUCCESS;
    const LibPalfEnvFFI *libpalf_env_ffi = NULL;
    char cluster_id_str[INTEGER_BUF_LEN] = {0};
    char tenant_id_str[INTEGER_BUF_LEN] = {0};
    if (OB_FAIL(generate_sub_path_(tenant_id_, cluster_id_str, tenant_id_str))) {
      PALF_LOG(WARN, "failed to generate sub path", K(ret), K(replayable_scn), K_(tenant_id));
    } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
      PALF_LOG(WARN, "failed to get libpalf_env_ffi", K(ret), K(replayable_scn));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_update_readable_scn(libpalf_env_ffi,
        cluster_id_str, tenant_id_str, replayable_scn.get_val_for_logservice())))) {
      PALF_LOG(WARN, "failed to update replayable point", K(ret), K(replayable_scn),
          K(cluster_id_str), K(tenant_id_str));
    }
    return ret;
}

int64_t LibPalfEnv::get_tenant_id()
{
    return tenant_id_;
}

int LibPalfEnv::list_dir_in_logservice(ObArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;

  const LibPalfEnvFFI *libpalf_env_ffi = nullptr;
  LibPalfDirInfo *dir_info = nullptr;
  char cluster_id_str[DIR_ENTRY_MAX_NAME_LEN];
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "failed to get libpalf_env_ffi");
  } else if (OB_FAIL(databuff_printf(cluster_id_str, DIR_ENTRY_MAX_NAME_LEN ,"%ld", cluster_id))) {
    PALF_LOG(WARN, "failed to get cluster_id_str", K(cluster_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_open_dir(libpalf_env_ffi, cluster_id_str, "",&dir_info)))) {
    PALF_LOG(WARN, "failed to open dir", K(cluster_id_str));
  } else {
    int tmp_ret = OB_SUCCESS;
    LibPalfDirEntry entry;
    char *endptr = nullptr;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    while (OB_SUCC(ret) && OB_SUCC(LIBPALF_ERRNO_CAST(libpalf_read_dir(dir_info, &entry)))) {
      if (LibPalfDirEntryType::Directory == entry.entry_type) {
        if (OB_FAIL(ob_strtoull(entry.name, endptr, tenant_id))) {
          PALF_LOG(WARN, "failed to transform entry to tenant_id", K(entry.name));
        } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          PALF_LOG(WARN, "failed to pushback tenant_id to result", K(tenant_id), K(tenant_ids));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "get non-dir items in list_dir", K(entry.name), K(entry.entry_type));
      }
    }
    if (OB_ITER_END != ret) {
      PALF_LOG(WARN, "failed to read dir", KP(dir_info));
    } else {
      ret = OB_SUCCESS;
    }

    if (OB_TMP_FAIL(LIBPALF_ERRNO_CAST(libpalf_close_dir(dir_info)))) {
        PALF_LOG_RET(WARN, tmp_ret, "failed to closedir", KP(dir_info));
    }
  }

  return ret;
}

int LibPalfEnv::list_ls_in_logservice(const uint64_t tenant_id, ObArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;

  const LibPalfEnvFFI *libpalf_env_ffi = nullptr;
  LibPalfDirInfo *dir_info = nullptr;
  char cluster_id_str[DIR_ENTRY_MAX_NAME_LEN], tenant_id_str[DIR_ENTRY_MAX_NAME_LEN];
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "failed to get libpalf_env_ffi");
  } else if (OB_FAIL(databuff_printf(cluster_id_str, DIR_ENTRY_MAX_NAME_LEN ,"%ld", cluster_id))) {
    PALF_LOG(WARN, "failed to get cluster_id_str", K(cluster_id));
  } else if (OB_FAIL(databuff_printf(tenant_id_str, DIR_ENTRY_MAX_NAME_LEN, "%lu", tenant_id))) {
    PALF_LOG(WARN, "failed to get tenant_id_str", K(cluster_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_open_dir(libpalf_env_ffi, cluster_id_str, tenant_id_str,&dir_info)))) {
    PALF_LOG(WARN, "failed to open dir", K(cluster_id_str));
  } else {
    int tmp_ret = OB_SUCCESS;
    LibPalfDirEntry entry;
    char *endptr = nullptr;
    uint64_t ls_id = OB_INVALID_TENANT_ID;
    while (OB_SUCC(ret) && OB_SUCC(LIBPALF_ERRNO_CAST(libpalf_read_dir(dir_info, &entry)))) {
      if (LibPalfDirEntryType::File == entry.entry_type) {
        if (OB_FAIL(ob_strtoull(entry.name, endptr, ls_id))) {
          PALF_LOG(WARN, "failed to transform entry to tenant_id", K(entry.name));
        } else if (OB_FAIL(ls_ids.push_back(ObLSID(ls_id)))) {
          PALF_LOG(WARN, "failed to pushback tenant_id to result", K(ls_id));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "get non-file items in list_ls", K(entry.name), K(entry.entry_type));
      }
    }
    if (OB_ITER_END != ret) {
      PALF_LOG(WARN, "failed to read dir", KP(dir_info));
    } else {
      ret = OB_SUCCESS;
    }

    if (OB_TMP_FAIL(LIBPALF_ERRNO_CAST(libpalf_close_dir(dir_info)))) {
        PALF_LOG_RET(WARN, tmp_ret, "failed to closedir", KP(dir_info));
    }
  }

  return ret;
}

int LibPalfEnv::remove_dir(const uint64_t id)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = nullptr;
  char cluster_id_str[DIR_ENTRY_MAX_NAME_LEN], tenant_id_str[DIR_ENTRY_MAX_NAME_LEN];
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "failed to get libpalf_env_ffi");
  } else if (OB_FAIL(databuff_printf(cluster_id_str, DIR_ENTRY_MAX_NAME_LEN ,"%ld", cluster_id))) {
    PALF_LOG(WARN, "failed to get cluster_id_str", K(cluster_id));
  } else if (OB_FAIL(databuff_printf(tenant_id_str, DIR_ENTRY_MAX_NAME_LEN, "%lu", id))) {
    PALF_LOG(WARN, "failed to get tenant_id_str", K(cluster_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_rmdir(libpalf_env_ffi, cluster_id_str, tenant_id_str)))) {
    PALF_LOG(WARN, "failed to rmdir in logservice", K(cluster_id_str), K(tenant_id_str));
  }
  return ret;
}

int LibPalfEnv::remove_stream(const uint64_t dir, const uint64_t id)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = nullptr;
  char cluster_id_str[DIR_ENTRY_MAX_NAME_LEN], tenant_id_str[DIR_ENTRY_MAX_NAME_LEN], stream_id_str[DIR_ENTRY_MAX_NAME_LEN];
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "failed to get libpalf_env_ffi");
  } else if (OB_FAIL(databuff_printf(cluster_id_str, DIR_ENTRY_MAX_NAME_LEN ,"%ld", cluster_id))) {
    PALF_LOG(WARN, "failed to get cluster_id_str", K(cluster_id));
  } else if (OB_FAIL(databuff_printf(tenant_id_str, DIR_ENTRY_MAX_NAME_LEN, "%lu", dir))) {
    PALF_LOG(WARN, "failed to get tenant_id_str", K(cluster_id));
  } else if (OB_FAIL(databuff_printf(stream_id_str, DIR_ENTRY_MAX_NAME_LEN, "%lu", id))) {
    PALF_LOG(WARN, "failed to get tenant_id_str", K(cluster_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_remove(libpalf_env_ffi, cluster_id_str, tenant_id_str, stream_id_str)))) {
    PALF_LOG(WARN, "failed to rmdir in logservice", K(cluster_id_str), K(tenant_id_str));
  }
  return ret;
}

int LibPalfEnv::advance_base_lsn(int64_t id, palf::LSN lsn)
{
  int ret = OB_SUCCESS;

  const LibPalfEnvFFI *libpalf_env_ffi = nullptr;
  char cluster_id_str[DIR_ENTRY_MAX_NAME_LEN], tenant_id_str[DIR_ENTRY_MAX_NAME_LEN], stream_id_str[DIR_ENTRY_MAX_NAME_LEN];
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "failed to get libpalf_env_ffi");
  } else if (OB_FAIL(databuff_printf(cluster_id_str, DIR_ENTRY_MAX_NAME_LEN ,"%ld", cluster_id))) {
    PALF_LOG(WARN, "failed to get cluster_id_str", K(cluster_id));
  } else if (OB_FAIL(databuff_printf(tenant_id_str, DIR_ENTRY_MAX_NAME_LEN, "%lu", tenant_id_))) {
    PALF_LOG(WARN, "failed to get tenant_id_str", K(cluster_id));
  } else if (OB_FAIL(databuff_printf(stream_id_str, DIR_ENTRY_MAX_NAME_LEN, "%ld", id))) {
    PALF_LOG(WARN, "failed to get tenant_id_str", K(cluster_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_advance_base_lsn(libpalf_env_ffi, cluster_id_str, tenant_id_str, stream_id_str, lsn.val_)))) {
    PALF_LOG(WARN, "failed to advance_base_lsn  in logservice", K(cluster_id_str), K(tenant_id_str));
  }

    return ret;
}

int LibPalfEnv::flashback(const uint64_t tenant_id,
                          const share::SCN &flashback_scn,
                          const int64_t timeout_us,
                          const share::ObLSStatusInfoArray &ls_array)
{
  int ret = OB_SUCCESS;
  const LibPalfEnvFFI *libpalf_env_ffi = nullptr;
  char cluster_id_str[DIR_ENTRY_MAX_NAME_LEN] = {0};
  char tenant_id_str[DIR_ENTRY_MAX_NAME_LEN] = {0};
  const int64_t cluster_id = GCONF.cluster_id;
  libpalf::LibPalfDirInfo *local_dir_info = nullptr;
  if (OB_INVALID_TENANT_ID == tenant_id ||
      !flashback_scn.is_valid() ||
      timeout_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(flashback_scn), K(timeout_us), K(ls_array));
  } else if (ls_array.count() == 0) {
    PALF_LOG(INFO, "no ls need to be flashbacked, return success", K(tenant_id), K(flashback_scn), K(timeout_us), K(ls_array));
  } else if (true == is_meta_tenant(tenant_id) || true == is_sys_tenant(tenant_id)) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(WARN, "can't flashback meta tenant/sys tenant", K(ret), K(tenant_id), K(flashback_scn), K(timeout_us));
  } else if (OB_FAIL(LibPalfEnvFFIInstance::get_instance().get_ffi(libpalf_env_ffi))) {
    PALF_LOG(WARN, "failed to get libpalf_env_ffi");
  } else if (OB_FAIL(generate_sub_path_(tenant_id, cluster_id_str, tenant_id_str))) {
    PALF_LOG(WARN, "failed to generate sub path", K(ret), K(tenant_id));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(create_libpalf_dir_info(&local_dir_info)))){
    PALF_LOG(WARN, "failed to create libpalf dir info", K(ret), K(cluster_id_str), K(tenant_id_str));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < ls_array.count(); ++i) {
      const share::ObLSStatusInfo &ls_status = ls_array.at(i);
      int64_t ls_id = ls_status.ls_id_.id();
      char file_id_str[DIR_ENTRY_MAX_NAME_LEN];
      if (OB_TMP_FAIL(databuff_printf(file_id_str, DIR_ENTRY_MAX_NAME_LEN, "%ld", ls_id))) {
        PALF_LOG(WARN, "failed to get file_id_str", K(tmp_ret), K(ls_id));
      } else if (OB_TMP_FAIL(LIBPALF_ERRNO_CAST(libpalf_dir_info_add_entry(local_dir_info, file_id_str)))) {
        PALF_LOG(WARN, "failed to add entry to libpalf dir info", K(tmp_ret), K(file_id_str));
      }
    }
    if (OB_FAIL(tmp_ret)) {
      ret = tmp_ret;
      PALF_LOG(WARN, "failed to add entries to libpalf dir info", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_flashback(libpalf_env_ffi, cluster_id_str, tenant_id_str, local_dir_info, flashback_scn.get_val_for_logservice(), timeout_us)))) {
      PALF_LOG(WARN, "failed to flashback in logservice", K(tenant_id), K(flashback_scn), K(timeout_us));
    } else {
      PALF_LOG(INFO, "flashback success", K(tenant_id), K(flashback_scn), K(timeout_us));
    }
    
    if (OB_TMP_FAIL(LIBPALF_ERRNO_CAST(release_libpalf_dir_info(local_dir_info)))) {
      PALF_LOG(WARN, "failed to release dir_info", K(tmp_ret), K(tenant_id), K(flashback_scn), KP(local_dir_info));
    }
  }
  return ret;
}

} // end namespace libpalf
} // end namespace oceanbase
