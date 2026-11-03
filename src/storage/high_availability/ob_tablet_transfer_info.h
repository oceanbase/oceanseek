/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_TABLET_TRANSFER_INFO_
#define OCEABASE_STORAGE_TABLET_TRANSFER_INFO_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
struct ObTabletTransferInfo final
{
  OB_UNIS_VERSION(1);
public:
  static bool is_private_transfer_epoch_valid(const int64_t private_transfer_epoch);
public:
  ObTabletTransferInfo();
  ~ObTabletTransferInfo() = default;
  int init();
  int init(
      const share::ObLSID &ls_id,
      const share::SCN &transfer_start_scn,
      const int64_t transfer_seq,
      const int32_t private_transfer_epoch,
      const share::SCN &src_reorganization_scn);
  void reset();
  bool is_valid() const;
  bool has_transfer_table() const;
  void reset_transfer_table();
  bool is_transfer_out_deleted() const;
  bool is_transfer_upload_completed() const { return transfer_upload_completed_; }
  void set_transfer_upload_completed(const bool completed) { transfer_upload_completed_ = completed; }
  int get_private_transfer_epoch(int32_t &private_transfer_epoch) const;
  int set_private_transfer_epoch(const int32_t private_transfer_epoch);

  TO_STRING_KV(K_(ls_id), K_(transfer_start_scn), K_(transfer_seq), K_(has_transfer_table), 
    K_(unused_is_transfer_out_deleted), K_(src_reorganization_scn), K_(private_transfer_epoch),
    K_(transfer_upload_completed));
public:
  share::ObLSID ls_id_;
  share::SCN transfer_start_scn_;
  int64_t transfer_seq_;
  bool has_transfer_table_;
  bool unused_is_transfer_out_deleted_; // ATTENTION: this field is no longer used
  share::SCN src_reorganization_scn_;
  static const int64_t TRANSFER_INIT_SEQ = 0;
private:
  static const int64_t TRANSFER_INIT_LS_ID = 0;
  /**
   * @brief Private transfer epoch.
   *
   * An int32 generation used to fence stale tasks across transfers. It mirrors
   * transfer_seq_ and no longer participates in MacroBlockId encoding.
   */ 
  // TODO(jyx441808): 待所有跨 transfer 的旧任务隔离逻辑统一使用 transfer_seq_ 后，删除该字段。
  int32_t private_transfer_epoch_;
  // Set only on the source shared tablet after transfer upload finishes.
  bool transfer_upload_completed_;
};


}
}
#endif
