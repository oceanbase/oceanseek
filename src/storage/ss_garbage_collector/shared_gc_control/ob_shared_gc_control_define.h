/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_SHARED_GC_CONTROL_OB_SHARED_GC_CONTROL_DEFINE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_SHARED_GC_CONTROL_OB_SHARED_GC_CONTROL_DEFINE_H_

#include <cstdint>
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

enum class ObSharedGCControlStatus : uint8_t
{
  IDLE  = 0,
  DOING = 1,
  MAX
};

bool is_valid_shared_gc_control_status(const ObSharedGCControlStatus status);
const char *shared_gc_control_status_to_str(const ObSharedGCControlStatus status);
int shared_gc_control_status_from_str(const common::ObString &str, ObSharedGCControlStatus &status);

class ObSharedGCControlInfo final
{
public:
  ObSharedGCControlInfo();
  ObSharedGCControlInfo(const uint64_t tenant_id,
                        const int64_t executor_server_id,
                        const ObSharedGCControlStatus status,
                        const int64_t gc_round_id,
                        const int64_t allow_gc_round_id,
                        const share::SCN gc_meta_scn,
                        const share::SCN last_succ_gc_meta_scn);
  ~ObSharedGCControlInfo() = default;
  void reset();
  bool is_valid() const;
  int  assign(const ObSharedGCControlInfo &other);
  TO_STRING_KV(K_(tenant_id), K_(executor_server_id), "status",
      shared_gc_control_status_to_str(status_), K_(gc_round_id), K_(allow_gc_round_id),
      K_(gc_meta_scn), K_(last_succ_gc_meta_scn));

public:
  uint64_t tenant_id_;
  int64_t executor_server_id_;
  ObSharedGCControlStatus status_;
  int64_t gc_round_id_;
  int64_t allow_gc_round_id_;
  share::SCN gc_meta_scn_;
  share::SCN last_succ_gc_meta_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObSharedGCControlInfo);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_SHARED_GC_CONTROL_OB_SHARED_GC_CONTROL_DEFINE_H_
