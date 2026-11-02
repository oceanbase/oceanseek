/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_ID_UTIL_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_ID_UTIL_H_

#include <stdint.h>
#include "share/ob_max_id_fetcher.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
}
namespace storage
{

// Allocates shared GC IDs and calibrates USER fetchers stored in META.
// Stateless; transactions and SQL clients remain owned by the caller.
class ObSSGCIdUtil
{
public:
  // Allocate USER IDs from META fetchers; SYS/META use their legacy fetchers.
  // Initialize all five USER fetchers atomically on first use.
  static int fetch_gc_id(
      const uint64_t tenant_id,
      const share::ObMaxIdType id_type,
      int64_t &id);
  // USER IDs reuse the caller's transaction, locking control before fetchers.
  // The caller must commit USER IDs. SYS/META use independent legacy allocation, ignoring trans.
  static int fetch_gc_id(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const share::ObMaxIdType id_type,
      int64_t &id);
  // The upgrade caller must disable and drain GC before calibrating all five fetchers.
  // Preserve every control field and use SQL only; the USER tenant need not be local.
  static int upgrade_user_gc_ids(
      common::ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id);

private:
  struct GCIdTypePair
  {
    share::ObMaxIdType legacy_id_type_;
    share::ObMaxIdType user_id_type_;
  };
  static const int64_t GC_ID_TYPE_COUNT = 5;
  ObSSGCIdUtil() = delete;
  ~ObSSGCIdUtil() = delete;

  static int read_user_gc_max_ids_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      uint64_t *max_ids,
      const int64_t max_id_count,
      int64_t &missing_count);
  static int prepare_user_gc_max_ids_(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const uint64_t initial,
      uint64_t *max_ids,
      const int64_t max_id_count);
  static int read_gc_max_id_(
      common::ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id,
      const share::ObMaxIdType id_type,
      uint64_t &max_id);

  static const GCIdTypePair GC_ID_TYPE_PAIRS_[GC_ID_TYPE_COUNT];

  DISALLOW_COPY_AND_ASSIGN(ObSSGCIdUtil);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_GC_ID_UTIL_H_
