/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SS_USAGE_REPORTER_H
#define OCEANBASE_SS_USAGE_REPORTER_H

#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/task/ob_timer.h"
#include "src/storage/incremental/ob_ss_storage_stat_operator.h"
#include "deps/oblib/src/lib/literals/ob_literals.h"

namespace oceanbase
{
namespace common
{
  class ObMySQLProxy;
}

namespace storage
{

template<typename T>
class ObSSMetaIterGuard;
class ObSSMetaTabletIter;
class ObSSMetaLSIDIter;

class ObSSUsageReportTask : public common::ObTimerTask
{
public:
  static const int64_t SS_USAGE_REPORT_INTERVAL = 30_s;
  ObSSUsageReportTask()
    : is_inited_(false),
      sql_proxy_(NULL),
      last_report_(0)
  { 
    reset_result_array(); 
  }

  virtual ~ObSSUsageReportTask() 
  { 
    is_inited_ = false;
    sql_proxy_ = NULL;
    reset_result_array(); 
  }
  
  int init(common::ObMySQLProxy &sql_proxy);

  virtual void runTimerTask();

  void reset_result_array()
  { memset(result_array_, 0, ReportType::MAX_TYPE * 8); }

  TO_STRING_KV(K_(is_inited), KP_(sql_proxy), K_(last_report), "public data", result_array_[ReportType::PUBLIC_DATA], 
      "public meta", result_array_[ReportType::PUBLIC_META], "private all", result_array_[ReportType::PRIVATE_ALL]);

private:
  int report_usage_();
  int fill_private_usage_();
  int fill_public_usage_();
  int calculate_tenant_public_usage_(
      const share::SCN &snapshot, 
      const common::ObIArray<share::ObLSID> &ls_id_list);
  int calculate_ls_public_usage_(
      const share::SCN &snapshot, 
      const share::ObLSID &ls_id,
      ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard);
  int get_ls_id_iter_(
      const share::SCN &snapshot,
      ObSSMetaIterGuard<ObSSMetaLSIDIter> &ls_id_iter_guard);
  int get_ls_id_list_(
      const share::SCN &snapshot, 
      common::ObIArray<share::ObLSID> &ls_id_list);
  int get_tablet_iter_(
      const share::SCN &snapshot,
      const share::ObLSID &ls_id,
      ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard);
  bool is_sys_ls_leader_();
  bool need_report_();

private:
  static const int64_t DEFAULT_STORAGE_STAT_REPORT_INTERVAL = 10_min;
  bool is_inited_;
  int64_t result_array_[ReportType::MAX_TYPE];
  common::ObMySQLProxy *sql_proxy_;
  int64_t last_report_;
};
}
}

# endif // OCEANBASE_SHARE_OB_SS_STORAGE_STAT_OPERATOR_
