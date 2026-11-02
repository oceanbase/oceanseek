/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_MDS_HELPER_H
#define OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_MDS_HELPER_H

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "storage/tablet/ob_async_index_refresh_info.h"

namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObTimeoutCtx;
}
namespace share
{
class SCN;
namespace schema
{
class ObTableSchema;
}
}

namespace storage
{
namespace mds
{
struct BufferCtx;
}

class ObTabletAsyncIndexRefreshInfoArg
{
  OB_UNIS_VERSION(2);
public:
  ObTabletAsyncIndexRefreshInfoArg()
      : ls_id_(), tablet_id_(), info_(), floor_pin_only_(false) {}
  ~ObTabletAsyncIndexRefreshInfoArg() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(info), K_(floor_pin_only));
public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObTabletAsyncIndexRefreshInfo info_;
  bool floor_pin_only_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletAsyncIndexRefreshInfoArg);
};

class ObTabletAsyncIndexRefreshInfoMdsHelper
{
public:
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN scn, mds::BufferCtx &ctx);
  static int process(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx, bool for_replay);
  // Register an explicit membership value for every data tablet in the same
  // SQL transaction that changes the table schema flag. This method also
  // serializes membership changes with background refresh-info maintenance.
  // Pass the build snapshot for the first online async index to pin it atomically.
  // Zero protects the commit version for new empty tablets; hidden tables only
  // initialize membership.
  static int register_membership(
      const share::schema::ObTableSchema &data_table_schema,
      common::ObMySQLTransaction &trans,
      const int64_t initial_snapshot = 0);
  // Register membership for tablets whose LS mapping is already known.
  static int register_ls_tablets_membership(
      const uint64_t tenant_id,
      const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
      common::ObMySQLTransaction &trans);
  // Register published_version for async-index TARGET tablets whose LS mapping is already known.
  static int register_ls_tablets_initial_published_versions(
      const uint64_t tenant_id,
      const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
      common::ObMySQLTransaction &trans);
  // Register initial published_version for every async-index target tablet in table_schemas.
  static int register_initial_published_versions(
      const uint64_t tenant_id,
      const common::ObIArray<share::schema::ObTableSchema> &table_schemas,
      const int64_t snapshot_version,
      common::ObMySQLTransaction &trans);
  // The caller holds the target Tablet refresh lock in this transaction.
  static int register_published_version(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t published_version,
      common::ObMySQLTransaction &trans);
  // Write floor_pin to data tablet's refresh_snapshot_versions_[0] as the GC
  // anchor.  Uses floor_pin_only semantics: the LS-side process() skips the
  // write if the tablet's committed pin[0] is already ≤ floor_pin.
  static int pin_one_data_tablet(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t floor_pin,
      common::ObMySQLTransaction &trans);
  // Apply floor-pin to every data tablet for data_table_schema in one pass.
  static int pin_data_tablets(
      const uint64_t tenant_id,
      const share::schema::ObTableSchema &data_table_schema,
      const int64_t floor_pin,
      common::ObMySQLTransaction &trans);
private:
  // Keep the caller's tablet order and timeout context. All locks must succeed
  // in the caller's transaction before any refresh-info MDS is registered.
  static int lock_async_index_refresh_tablets_(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      const common::ObTimeoutCtx &timeout_ctx,
      common::ObMySQLTransaction &trans);
  static int register_membership_(
      const share::schema::ObTableSchema &data_table_schema,
      common::ObMySQLTransaction &trans,
      const int64_t initial_snapshot);
  static int register_refresh_info_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObTabletAsyncIndexRefreshInfo &refresh_info,
      common::ObMySQLTransaction &trans,
      common::ObIAllocator &allocator,
      const bool need_flush_redo_instantly,
      const bool floor_pin_only = false);
  static int register_tablets_refresh_info_(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      const ObTabletAsyncIndexRefreshInfo &refresh_info,
      common::ObMySQLTransaction &trans);
  static int register_ls_tablets_membership_(
      const uint64_t tenant_id,
      const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
      common::ObMySQLTransaction &trans);
  static int register_ls_tablets_refresh_info_(
      const uint64_t tenant_id,
      const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
      const ObTabletAsyncIndexRefreshInfo &refresh_info,
      common::ObMySQLTransaction &trans,
      const char *allocator_label);
  static int register_ls_tablets_initial_published_versions_(
      const uint64_t tenant_id,
      const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
      common::ObMySQLTransaction &trans);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_MDS_HELPER_H
