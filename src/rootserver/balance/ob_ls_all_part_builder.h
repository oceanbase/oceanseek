/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_LS_ALL_PART_BUILDER_H
#define OCEANBASE_ROOTSERVER_OB_LS_ALL_PART_BUILDER_H

#include "lib/hash/ob_hashmap.h"                // ObHashMap
#include "share/schema/ob_schema_struct.h"      // ObPartitionLevel
#include "share/transfer/ob_transfer_info.h"    // ObTransferPartList, ObTransferPartInfo

namespace oceanbase
{
namespace common
{
class ObISQLClient;
} // ns common

namespace share
{
class ObLSID;
class ObTabletToLSInfo;

namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
} // ns schema
} // ns share

namespace rootserver
{

// table level judgement of one table, built from one scan of the tenant's tables
struct ObLSPartTableInfo
{
  ObLSPartTableInfo()
    : need_balance_(false), part_level_(share::schema::PARTITION_LEVEL_ZERO) {}
  bool need_balance_;
  share::schema::ObPartitionLevel part_level_;
  TO_STRING_KV(K_(need_balance), K_(part_level));
};
// table_id -> table level judgement
typedef common::hash::ObHashMap<uint64_t, ObLSPartTableInfo> ObLSPartTableMap;
// tablet_id -> part object id: part_id for level one, sub_part_id for level two
typedef common::hash::ObHashMap<uint64_t, uint64_t> ObLSPartTabletPartMap;

class ObLSAllPartBuilder
{
public:
  // build current all part list on specific LS, table/part info is loaded by schema scans once
  //
  // @return OB_NEED_RETRY     schema not latest or schema changed during handling
  static int build(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObISQLClient &sql_proxy,
      share::ObTransferPartList &part_list);

private:
  static int get_latest_schema_guard_(
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObISQLClient &sql_proxy,
      share::schema::ObSchemaGetterGuard &schema_guard);
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_LS_ALL_PART_BUILDER_H */
