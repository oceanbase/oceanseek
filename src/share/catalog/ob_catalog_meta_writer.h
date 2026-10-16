/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef __SHARE_OB_CATALOG_META_WRITER_H__
#define __SHARE_OB_CATALOG_META_WRITER_H__

#include "share/catalog/ob_catalog_location_schema_provider.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{

class ObCatalogMetaWriter final : public ObICatalogMetaWriter
{
public:
  ObCatalogMetaWriter(schema::ObSchemaGetterGuard &schema_getter_guard, ObIAllocator &allocator)
      : schema_getter_guard_(schema_getter_guard), location_schema_provider_(schema_getter_guard_),
        allocator_(allocator)
  {
  }

  ~ObCatalogMetaWriter() override;

  int create_namespace(const uint64_t tenant_id,
                       const uint64_t catalog_id,
                       const common::ObString &ns_name) override;
  int drop_namespace(const uint64_t tenant_id,
                     const uint64_t catalog_id,
                     const common::ObString &ns_name,
                     bool delete_data,
                     bool cascade) override;
  int create_table(const uint64_t tenant_id,
                   const uint64_t catalog_id,
                   const common::ObString &db_name,
                   const share::schema::ObTableSchema &table_schema) override;
  int alter_table(const uint64_t tenant_id,
                  const uint64_t catalog_id,
                  const common::ObString &db_name,
                  const share::schema::ObTableSchema &table_schema) override;
  int rename_table(const uint64_t tenant_id,
                   const uint64_t catalog_id,
                   const common::ObString &origin_db_name,
                   const common::ObString &origin_table_name,
                   const common::ObString &new_db_name,
                   const common::ObString &new_table_name,
                   const share::schema::ObTableSchema &table_schema) override;
  int reconcile_partitions(
      const uint64_t tenant_id,
      const uint64_t catalog_id,
      const common::ObString &db_name,
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<CatalogPartitionValues> &expected_partition_values) override;
  int drop_table(const uint64_t tenant_id,
                 const uint64_t catalog_id,
                 const common::ObString &db_name,
                 const common::ObString &table_name) override;

  int get_catalog(const uint64_t tenant_id, const uint64_t catalog_id, ObIExternalCatalog *&catalog);

private:
  schema::ObSchemaGetterGuard &schema_getter_guard_;
  ObCatalogLocationSchemaProvider location_schema_provider_;
  ObArray<CatalogEntry> created_catalog_;
  ObIAllocator &allocator_;
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_CATALOG_META_WRITER_H__
