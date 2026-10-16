/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_SCAN_ITERATOR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_SCAN_ITERATOR_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_scan_operator.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObSimpleTableSchemaV2;
class ObTableSchema;

enum class ObSchemaScanIterMode : int8_t
{
  INVALID = 0,
  BY_ID = 1,      // new path: ids_ → get_next 按需 get schema
  BY_SCHEMA = 2,  // old path: schemas_ 已在 init 挂好，get_next 按下标返回
};

/**
 * Dual-path schema iterator (接口设计.md §2.1).
 *
 * - BY_ID: batch-fetch object ids, then load schema on get_next().
 * - BY_SCHEMA: init with a resident schema pointer array (mgr path);
 *   get_next() returns schemas_.at(idx_++).
 *
 * Lifecycle: the pointer returned by get_next() is only valid until the next
 * get_next() call or iterator destruction. Callers must not stash pointers into
 * long-lived arrays (even on BY_SCHEMA, do not rely on mgr residency).
 */
class ObSchemaIdScanIterator
{
public:
  ObSchemaIdScanIterator();
  virtual ~ObSchemaIdScanIterator() { reset(); }

  int init(ObSchemaGetterGuard &guard, const ObSchemaScanSpec &spec);
  int init(ObSchemaGetterGuard &guard, const common::ObIArray<uint64_t> &ids);
  void reset();
  bool is_inited() const { return is_inited_; }
  ObSchemaScanIterMode get_mode() const { return mode_; }
  bool is_by_id() const { return ObSchemaScanIterMode::BY_ID == mode_; }
  bool is_by_schema() const { return ObSchemaScanIterMode::BY_SCHEMA == mode_; }

  int get_next_id(uint64_t &id);
  int64_t count() const;
  const common::ObIArray<uint64_t> &get_ids() const { return ids_; }
  ObSchemaGetterGuard *get_guard() { return guard_; }

  TO_STRING_KV(K_(is_inited), "mode", static_cast<int>(mode_), K_(idx),
               "id_cnt", ids_.count(), "schema_cnt", schemas_.count());

protected:
  int init_common_(ObSchemaGetterGuard &guard, const uint64_t tenant_id);
  // Old path: store schema pointers (typed pointer cast to const void* by caller).
  int init_by_schemas_(const common::ObIArray<const void *> &schemas);
  int get_next_schema_ptr_(const void *&schema);

protected:
  bool is_inited_;
  ObSchemaScanIterMode mode_;
  ObSchemaGetterGuard *guard_;
  uint64_t tenant_id_;
  int64_t idx_;
  common::ObArray<uint64_t> ids_;                 // BY_ID
  common::ObArray<const void *> schemas_;         // BY_SCHEMA
};

class ObSimpleTableSchemaIterator : public ObSchemaIdScanIterator
{
public:
  ObSimpleTableSchemaIterator() {}
  virtual ~ObSimpleTableSchemaIterator() {}
  using ObSchemaIdScanIterator::init;
  // Old path: mgr-resident schema list.
  int init(const common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas);
  // Returned pointer valid only until next get_next() / destruction.
  int get_next(const ObSimpleTableSchemaV2 *&table_schema);
};

class ObTableSchemaIterator : public ObSchemaIdScanIterator
{
public:
  ObTableSchemaIterator() {}
  virtual ~ObTableSchemaIterator() {}
  using ObSchemaIdScanIterator::init;
  int init(const common::ObIArray<const ObTableSchema *> &schemas);
  // Returned pointer valid only until next get_next() / destruction.
  int get_next(const ObTableSchema *&table_schema);
};

} // namespace schema
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_SCAN_ITERATOR_H_
