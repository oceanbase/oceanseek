/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_WRITER_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_WRITER_H_

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_ls_id.h"
#include "share/ob_thread_pool.h"                           // ObThreadPool
#include "ob_log_restore_define.h"

namespace oceanbase
{
namespace storage
{
class ObLSService;
}
namespace share
{
class ObLSID;
class SCN;
}
namespace palf
{
class LSN;
}
namespace logservice
{
class ObFetchLogTask;
class ObRemoteFetchWorker;
class ObLogRestoreService;
class ObRemoteFetchTaskStat;
class ObRemoteLogWriter : public share::ObThreadPool
{
public:
  ObRemoteLogWriter();
  virtual ~ObRemoteLogWriter();

public:
  int init(const uint64_t tenant_id,
      storage::ObLSService *ls_svr,
      ObLogRestoreService *restore_service,
      ObRemoteFetchWorker *worker);
  void destroy();
  int start();
  void stop();
  void wait();
  void notify_task();

private:
  void run1();
  // The following helpers return whether a rate-limited task was retained for fast retry.
  bool do_thread_task_();
  bool submit_ls_tasks_(const share::ObLSID &id);
  int submit_entries_(ObFetchLogTask &task, ObRestoreWriteRetryReason &retry_reason);
  int submit_log_(const share::ObLSID &id, const int64_t proposal_id, const palf::LSN &lsn,
      const share::SCN &scn, const char *buf, const int64_t buf_size,
      ObRestoreWriteRetryReason &retry_reason);
  void finish_rate_limit_wait_(ObFetchLogTask &task, const bool record_elapsed);
  int update_max_fetch_info_(const share::ObLSID &id, const int64_t proposal_id,
      const palf::LSN &lsn, const share::SCN &scn);
  int record_submit_stat_(const share::ObLSID &id, const int64_t proposal_id,
      const share::SCN &scn, const ObRemoteFetchTaskStat &fetch_stat,
      const bool is_first_submit_round,
      const int64_t fetch_to_submit_time_us, const int64_t submit_log_time_us);
  int try_retire_(ObFetchLogTask *&task);
  void inner_free_task_(ObFetchLogTask &task);
  void report_error_(const share::ObLSID &id,
                     const int ret_code,
                     const palf::LSN &lsn,
                     const ObLogRestoreErrorContext::ErrorType &error_type);

private:
  bool inited_;
  uint64_t tenant_id_;
  storage::ObLSService *ls_svr_;
  ObLogRestoreService *restore_service_;
  ObRemoteFetchWorker *worker_;
  ObCond task_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogWriter);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_WRITER_H_ */
