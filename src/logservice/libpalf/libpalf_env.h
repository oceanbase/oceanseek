/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_ENV_
#define OCEANBASE_LOGSERVICE_LIBPALF_ENV_

#include "lib/hash/ob_hashset.h"
#include "palf_ffi.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_options.h"
#include "share/ls/ob_ls_status_operator.h"
#include "logservice/ipalf/ipalf_options.h"
#include "logservice/ipalf/ipalf_define.h"
#include "logservice/ipalf/ipalf_env.h"
#include "libpalf_log_updater.h"

namespace oceanbase
{
namespace ipalf
{
struct LSKey;
class IPalfEnv;
class IPalfHandle;
class LibPalfEnvCreateParams;
enum class AccessMode;
};
namespace libpalf
{

enum class LibPalfAccessMode;
struct LibPalfBaseInfo;
class LibPalfHandle;
class LibPalfEnv;

class LibPalfEnv : public ipalf::IPalfEnv {
public:
  typedef common::hash::ObHashSet<int64_t> PalfIdSet;
public:
  friend class LibPalfHandle;
  friend class LibPalfHandleFFIValue;
  // static interface
  // create the palf env with the specified "base_dir".
  // store a pointer to a heap-allocated(may be allocate by a specified allocator) in "palf_env",
  // and return OB_SUCCESS on success.
  // store a NULL pointer ) in "palf_env", and return errno on fail.
  // caller should used destroy_palf_env to delete "palf_env" when it is no longer used.
  static int create_palf_env(ipalf::LibPalfEnvCreateParams *params,
                             LibPalfEnv *&palf_env);
  // static interface
  // destroy the palf env, and set "palf_env" to NULL.
  static void destroy_palf_env(LibPalfEnv *&palf_env);
public:
  LibPalfEnv();
  virtual ~LibPalfEnv();

public:
  virtual bool operator==(const ipalf::IPalfEnv &rhs) const override final;

  // 迁移场景目的端副本创建接口
  // @param [in] id，待创建日志流的标识符
  // @param [in] access_mode，palf access mode
  // @param [in] palf_base_info，palf的日志起点信息
  // @param [out] handle，创建成功后生成的palf_handle对象
  virtual int create(const int64_t id,
                     const ipalf::AccessMode &access_mode,
                     const palf::PalfBaseInfo &palf_base_info,
                     ipalf::IPalfHandle *&handle) override final;

  virtual int start() override final;

  // Load a palf replica handle which has been created in logservice,
  // Returned `handle` should be closed by invoking `close`.
  virtual int load(const int64_t id, ipalf::IPalfHandle *&handle) override final;

  // 打开一个 id 对应的，返回文件句柄
  virtual int open(const int64_t id, ipalf::IPalfHandle *&handle) override final;

  // 关闭一个句柄
  virtual int close(ipalf::IPalfHandle *&handle) override final;

  // 删除id对应的Paxos Replica，会同时删除物理文件；
  virtual int remove(int64_t id) override final;

  // @brief iterate each IPalfHandle of PalfEnv and execute 'func'
  virtual int for_each(const ObFunction<int(const ipalf::IPalfHandle&)> &func) override final;
  // just for LogRpc
  // palf::IPalfEnvImpl *get_palf_env_impl() { return &palf_env_impl_; }
  // should be removed in version 4.2.0.0
  virtual int update_replayable_point(const share::SCN &replayable_scn) override final;

  virtual int64_t get_tenant_id() override final;

  virtual int advance_base_lsn(int64_t id, palf::LSN lsn) override final;
  virtual int flashback(const uint64_t tenant_id,
                        const share::SCN &flashback_scn,
                        const int64_t timeout_us,
                        const share::ObLSStatusInfoArray &ls_array) override final;
public:
  int list_dir_in_logservice(ObArray<uint64_t> &tenant_ids);
  int list_ls_in_logservice(const uint64_t tenant_id, ObArray<ObLSID> &ls_ids);
  int remove_dir(const uint64_t id);
  int remove_stream(const uint64_t dir, const uint64_t id);
private:
  int do_init_(ipalf::LibPalfEnvCreateParams *params);
  int do_lazy_load_();
  int close_libpalf_handle_(const LibPalfHandleFFI *libpalf_handle_ffi);
  int wait_libpalf_handle_instance_remove_(const int64_t palf_id);
  void destroy();
  int do_create_libpalf_handle_(const int64_t palf_id,
                                const LibPalfAccessMode &libpalf_access_mode,
                                const LibPalfBaseInfo &libpalf_base_info,
                                const LibPalfHandleFFI *&libpalf_handle_ffi);
  int do_open_libpalf_handle_(const int64_t palf_id, const bool open_remote, const LibPalfHandleFFI *&libpalf_handle_ffi);
  int do_close_libpalf_handle_ffi_once_(const LibPalfHandleFFI *&libpalf_handle_ffi);
  int get_palf_id_set_(PalfIdSet &palf_id_set) const;
  int generate_sub_path_(const uint64_t tenant_id, char *cluster_id_str, char *tenant_id_str) const;
  int generate_file_path_(const uint64_t tenant_id, const int64_t palf_id,
      char *cluster_id_str, char *tenant_id_str, char *palf_id_str) const;
private:
  static constexpr int INTEGER_BUF_LEN = 32;
private:
  bool is_inited_;
  bool is_loaded_;
  // lib palf env params
  palf::PalfMonitorCb *monitor_;
  uint64_t tenant_id_;
  LibPalfLogUpdater log_updater_;
};

} // end namespace libpalf
} // end namespace oceanbase

#endif