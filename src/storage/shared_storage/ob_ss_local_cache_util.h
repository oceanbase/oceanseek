/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_UTIL_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_UTIL_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_array.h"
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
class ObTableSchema;
}
}
namespace blocksstable
{
class ObStorageObjectHandle;
}
namespace common
{
class ObTabletID;
class ObString;
}

namespace storage
{

class ObSSLocalCacheUtil
{
public:
  ObSSLocalCacheUtil() {}
  virtual ~ObSSLocalCacheUtil() {}
  static int get_table_schema(
      const uint64_t tenant_id, 
      const uint64_t table_id,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard, 
      const share::schema::ObTableSchema *&table_schema);

  static int get_table_schema(
      const uint64_t tenant_id, 
      const common::ObString &db_name,
      const common::ObString &table_name,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard, 
      const share::schema::ObTableSchema *&table_schema,
      const bool is_index = false);

  static int get_table_index_array(
      const uint64_t tenant_id,
      const uint64_t table_id,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard, 
      uint64_t *index_tid_array,
      int64_t &index_arr_cnt,
      const bool with_mv = false);

  static int get_table_tablet_ids(
      const uint64_t tenant_id,
      const share::schema::ObTableSchema *table_schema,
      common::ObIArray<common::ObTabletID> &tablet_ids);

  static int get_table_id_by_tablet_id(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const int64_t schema_version,
      share::schema::ObMultiVersionSchemaService *schema_service,
      uint64_t &table_id);

  static int get_tablet_part_idx(
      const share::schema::ObTableSchema *table_schema,
      const common::ObTabletID &tablet_id,
      int64_t &part_idx,
      int64_t &sub_part_idx);

  static int get_local_index_table_tablet_by_part_idx(
      const uint64_t tenant_id,
      const uint64_t table_id,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard, 
      const int64_t part_idx,
      const int64_t sub_part_idx,
      common::ObIArray<common::ObTabletID> &tablet_ids);

  static int get_tablets_from_all_index_tables(
      const uint64_t tenant_id,
      const bool need_local_index,
      const bool need_global_index,
      const share::schema::ObTableSchema *table_schema,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObTabletID> &tablet_ids);
  
  static int get_tablet_from_all_local_index_tables(
      const uint64_t tenant_id,
      const common::ObTabletID &tablet_id,
      const int64_t schema_version,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard, 
      common::ObIArray<common::ObTabletID> &tablet_ids);
  
  // Process callback or copy data directly when cache hit in memory.
  static int handle_hit_cache_in_memory(
      const char *io_buf,
      const int64_t data_size,
      common::ObIOInfo &io_info,
      blocksstable::ObStorageObjectHandle &object_handle);
  
  static int get_tenant_memory_info(const uint64_t tenant_id, int64_t &total_mem, int64_t &used_mem);

  DISALLOW_COPY_AND_ASSIGN(ObSSLocalCacheUtil);

private:
  static int create_schema_guard_if_need(
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::schema::ObSchemaGetterGuard &schema_guard);
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LOCAL_CACHE_UTIL_H_ */
