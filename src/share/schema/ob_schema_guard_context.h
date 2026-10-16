/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_GUARD_CONTEXT_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_GUARD_CONTEXT_H_

#include <stdint.h>
#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_mgr_cache.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_simple_schema_mode.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class MockSchemaService;
class ObMultiVersionSchemaService;
class ObOriginalSchemaGetterGuard;
class ObOnDemandSchemaGetterGuard;
class ObSimpleSchemaOnDemandCoordinator;
class ObSchemaGetterGuard;
class ObTenantSchemaPublishState;

class ObSchemaMgrInfo
{
public:
  ObSchemaMgrInfo();
  ObSchemaMgrInfo(
      const uint64_t tenant_id,
      const int64_t snapshot_version,
      const ObSchemaMgr *&schema_mgr,
      const ObSchemaMgrHandle &mgr_handle,
      const ObRefreshSchemaStatus &schema_status);
  ObSchemaMgrInfo(const ObSchemaMgrInfo &other);
  virtual ~ObSchemaMgrInfo();
  ObSchemaMgrInfo &operator=(const ObSchemaMgrInfo &other);

  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_snapshot_version() const { return snapshot_version_; }
  void set_schema_mgr(const ObSchemaMgr *schema_mgr) { schema_mgr_ = schema_mgr; }
  const ObSchemaMgr *get_schema_mgr() const { return schema_mgr_; }
  void set_publish_state(ObTenantSchemaPublishState *publish_state)
  { publish_state_ = publish_state; }
  ObTenantSchemaPublishState *get_publish_state() const { return publish_state_; }
  ObRefreshSchemaStatus get_schema_status() const { return schema_status_; }
  ObSchemaMgrHandle &get_schema_mgr_handle() { return mgr_handle_; }
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(snapshot_version), KP_(schema_mgr),
               KP_(publish_state), K_(schema_status));

private:
  uint64_t tenant_id_;
  int64_t snapshot_version_;
  const ObSchemaMgr *schema_mgr_;
  ObTenantSchemaPublishState *publish_state_;
  ObSchemaMgrHandle mgr_handle_;
  ObRefreshSchemaStatus schema_status_;
};

// Shared lifetime state of a schema guard.  Table access strategies are
// intentionally stateless and operate on the owning facade/context.
class ObSchemaGuardContext
{
  friend class MockSchemaService;
  friend class ObMultiVersionSchemaService;
  friend class ObOriginalSchemaGetterGuard;
  friend class ObOnDemandSchemaGetterGuard;
  friend class ObSimpleSchemaOnDemandCoordinator;
  friend class ObSchemaGetterGuard;
protected:
  static const int DEFAULT_RESERVE_SIZE = 2;
  typedef common::ObSEArray<SchemaObj, DEFAULT_RESERVE_SIZE> SchemaObjs;
  typedef common::ObSEArray<ObSchemaMgrInfo, DEFAULT_RESERVE_SIZE> SchemaMgrInfos;

  explicit ObSchemaGuardContext(const ObSchemaMgrItem::Mod mod);
  ~ObSchemaGuardContext();

  common::ObArenaAllocator local_allocator_;
  ObMultiVersionSchemaService *schema_service_;
  uint64_t session_id_;
  uint64_t tenant_id_;
  SchemaMgrInfos schema_mgr_infos_;
  SchemaObjs schema_objs_;
  ObSchemaMgrItem::Mod mod_;
  int schema_guard_type_;
  bool restore_tenant_exist_;
  bool is_inited_;
  int64_t pin_cache_size_;
  ObSimpleSchemaMode schema_mode_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaGuardContext);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OB_OCEANBASE_SCHEMA_OB_SCHEMA_GUARD_CONTEXT_H_
