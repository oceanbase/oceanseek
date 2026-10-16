/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_CREATE_TABLET_MDS_CTX
#define OCEANBASE_STORAGE_OB_CREATE_TABLET_MDS_CTX

#include "storage/multi_data_source/mds_ctx.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObTabletCreateMdsCtx : public MdsCtx
{
public:
  ObTabletCreateMdsCtx();
  explicit ObTabletCreateMdsCtx(const MdsWriter &writer);
  virtual ~ObTabletCreateMdsCtx() = default;
public:
  // Marks each tablet in tablet_ids_ as un-persisted via
  // ObLSTabletService::add_to_dirty_tablet_list_(redo_scn). Leader-only path;
  // replay does an equivalent explicit mark in ObTabletCreateMdsHelper::
  // replay_process because tablet_ids_ is runtime-only (not serialized).
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override;
  virtual int deserialize(const char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int64_t get_serialize_size() const override;
public:
  void set_ls_id(const share::ObLSID &ls_id);
  // Append one tablet_id created under this ctx. Called from
  // ObTabletCreateMdsHelper::register_process after create_tablets succeeds
  // (leader path only — replay doesn't walk through this method).
  int add_tablet_id(const common::ObTabletID &tablet_id);
private:
  static constexpr int32_t MAGIC = 0xdead;
  static constexpr int32_t VERSION = 1;

  const int32_t magic_;
  int32_t version_;
  share::ObLSID ls_id_;
  // Runtime-only: not serialized, not restored on replay. Populated on the
  // leader after register_process creates tablets; used by on_redo to mark
  // those tablets as un-persisted before any MDS flush can advance memtable
  // rec_scn past the redo_scn.
  common::ObSEArray<common::ObTabletID, 8> tablet_ids_;
};

inline void ObTabletCreateMdsCtx::set_ls_id(const share::ObLSID &ls_id)
{
  ls_id_ = ls_id;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_CREATE_TABLET_MDS_CTX
