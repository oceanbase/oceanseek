/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_STAT_
#define OCEANBASE_STORAGE_SSWRITER_STAT_

#include "common/ob_simple_iterator.h"
#include "common/ob_region.h"
#include "lib/allocator/ob_mod_define.h"
#include "storage/incremental/ob_inc_ss_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSWriterGroupStat
{
public:
  ObSSWriterGroupStat() { reset(); }
  ~ObSSWriterGroupStat() { reset(); }
  void reset();
  int init(
      const ObLsSSWriterKey &key,
      const int64_t group_id,
      const common::ObAddr &grant_server,
      const int64_t current_ts,
      const int64_t remaining_lease_us,
      const int64_t lease_expire_ts,
      const int64_t create_ts,
      const int64_t epoch,
      const int64_t last_request_lease_ts);
public:
  ObLsSSWriterKey &get_key() { return key_; }
  int64_t get_group_id() const { return group_id_; }
  const common::ObAddr &get_grant_server() const { return grant_server_; }
  int64_t get_current_ts() const { return current_ts_; }
  int64_t get_remaining_lease_us() const { return remaining_lease_us_; }
  int64_t get_lease_expire_ts() const { return lease_expire_ts_; }
  int64_t get_create_ts() const { return create_ts_; }
  int64_t get_epoch() const { return epoch_; }
  int64_t get_last_request_lease_ts() const { return last_request_lease_ts_; }
public:
  TO_STRING_KV(K_(is_inited), K_(key), K_(group_id), K_(grant_server),
      K_(current_ts), K_(remaining_lease_us), K_(lease_expire_ts),
      K_(create_ts), K_(epoch), K_(last_request_lease_ts));
private:
  bool is_inited_;
  ObLsSSWriterKey key_;
  int64_t group_id_;
  common::ObAddr grant_server_;
  int64_t current_ts_;
  int64_t remaining_lease_us_;
  int64_t lease_expire_ts_;
  int64_t create_ts_;
  int64_t epoch_;
  int64_t last_request_lease_ts_;
};

class ObSSWriterMgrStat
{
public:
  ObSSWriterMgrStat() { reset(); }
  ~ObSSWriterMgrStat() { reset(); }
  void reset();
  int init(
      const share::ObLSID ls_id,
      const ObSSWriterType sswriter_type,
      const int64_t group_id,
      const common::ObRegion &region,
      const ObSSWriterGroupState state,
      const common::ObAddr &target_server,
      const int64_t current_ts,
      const int64_t last_update_state_ts,
      const int64_t lease_expire_ts,
      const int64_t epoch);
public:
  share::ObLSID get_ls_id() const { return ls_id_; }
  ObSSWriterType get_sswriter_type() const { return sswriter_type_; }
  int64_t get_group_id() const { return group_id_; }
  const common::ObRegion &get_region() const { return region_; }
  ObSSWriterGroupState get_sswriter_group_state() { return state_; }
  const common::ObAddr &get_target_server() const { return target_server_; }
  int64_t get_current_ts() const { return current_ts_; }
  int64_t get_last_update_state_ts() { return last_update_state_ts_; }
  int64_t get_lease_expire_ts() { return lease_expire_ts_; }
  int64_t get_epoch() { return epoch_; }
public:
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(sswriter_type),
      K_(group_id), K_(region), K_(state), K_(target_server),
      K_(current_ts), K_(last_update_state_ts), K_(lease_expire_ts),
      K_(epoch));
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObSSWriterType sswriter_type_;
  int64_t group_id_;
  common::ObRegion region_;
  ObSSWriterGroupState state_;
  common::ObAddr target_server_;
  int64_t current_ts_;
  int64_t last_update_state_ts_;
  int64_t lease_expire_ts_;
  int64_t epoch_;
};

struct SSWriterModID
{
  static constexpr const char OB_SSWRITER_GROUP_STAT[] = "SSWriterGroupStat";
  static constexpr const char OB_SSWRITER_MGR_STAT[] = "SSWriterMgrStat";
};

typedef common::ObSimpleIterator<ObSSWriterGroupStat,
        SSWriterModID::OB_SSWRITER_GROUP_STAT, 10> ObSSWriterGroupStatIterator;
typedef common::ObSimpleIterator<ObSSWriterMgrStat,
        SSWriterModID::OB_SSWRITER_MGR_STAT, 10> ObSSWriterMgrStatIterator;

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_STAT_
