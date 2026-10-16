/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_OB_START_TRANSFER_IN_MDS_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_OB_START_TRANSFER_IN_MDS_CTX_H

#include "mds_ctx.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

class MdsTableHandle;

struct ObStartTransferInMdsCtxVersion
{
  enum VERSION {
    START_TRANSFER_IN_MDS_CTX_VERSION_V1 = 1,
    START_TRANSFER_IN_MDS_CTX_VERSION_V2 = 2,
    START_TRANSFER_IN_MDS_CTX_VERSION_V3 = 3,
    MAX
  };
  static const VERSION CURRENT_CTX_VERSION = START_TRANSFER_IN_MDS_CTX_VERSION_V3;
  static bool is_valid(const ObStartTransferInMdsCtxVersion::VERSION &version) {
    return version >= START_TRANSFER_IN_MDS_CTX_VERSION_V1
        && version < MAX;
  }
};

class ObStartTransferInMdsCtx : public MdsCtx
{
public:
  ObStartTransferInMdsCtx();
  explicit ObStartTransferInMdsCtx(const MdsWriter &writer);
  virtual ~ObStartTransferInMdsCtx();
public:
  // Marks each transfer-in tablet in tablet_ids_ as un-persisted via
  // ObLSTabletService::add_to_dirty_tablet_list_(redo_scn). Leader-only;
  // replay marks explicitly in create_transfer_in_tablets_.
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_prepare(const share::SCN &prepare_version) override;
  virtual void on_abort(const share::SCN &abort_scn) override;

  virtual int serialize(char *buf, const int64_t len, int64_t &pos) const override;
  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos) override;
  virtual int64_t get_serialize_size(void) const override;
public:
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  int add_tablet_id(const common::ObTabletID &tablet_id);
private:
  ObStartTransferInMdsCtxVersion::VERSION version_;
  share::ObLSID ls_id_;
  // Runtime-only: not serialized. Leader populates via add_tablet_id after
  // each create_transfer_in_tablet_ succeeds.
  common::ObSEArray<common::ObTabletID, 8> tablet_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObStartTransferInMdsCtx);
};

} //mds
} //storage
} //oceanbase

#endif //SHARE_STORAGE_MULTI_DATA_SOURCE_OB_TRANSFER_IN_MDS_CTX_H
