/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "share/diagnosis/ob_sql_monitor_statname.h"
#include <stdint.h>
#include "share/diagnosis/ob_runtime_metrics.h"

namespace oceanbase
{
namespace sql
{
ObSqlMonitorStatIds::ObSqlMonitorStatIds()
{
#define SQL_MONITOR_STATNAME_DEF(def, unit, name, desc, agg_type, level)                           \
  static_assert(sizeof(name) <= MAX_MONITOR_STAT_NAME_LENGTH,                                      \
                "metric name length is bigger than MAX_MONITOR_STAT_NAME_LENGTH");
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF

  // UINT16_MAX is reserved for an unregistered slot in ObOpProfile::metrics_id_map_.
  static_assert(
      ObSqlMonitorStatEnum::MONITOR_STATNAME_END < UINT16_MAX,
      "metric define more than UINT16_MAX, please modify ObOpProfile::metrics_id_map_ also");
}

const ObMonitorStat OB_MONITOR_STATS[] = {
#define SQL_MONITOR_STATNAME_DEF(def, unit, name, desc, agg_type, level)                           \
  {{static_cast<int>(unit)}, name, desc, agg_type, static_cast<int>(level)},
#include "share/diagnosis/ob_sql_monitor_statname.h"
#undef SQL_MONITOR_STATNAME_DEF
};
} // namespace sql
} // namespace oceanbase
