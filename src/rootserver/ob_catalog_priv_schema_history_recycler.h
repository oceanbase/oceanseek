/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_CATALOG_PRIV_SCHEMA_HISTORY_RECYCLER_H_
#define OCEANBASE_ROOTSERVER_OB_CATALOG_PRIV_SCHEMA_HISTORY_RECYCLER_H_

#include "rootserver/ob_schema_history_recycler.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace rootserver
{

struct ObCatalogDatabasePrivSchemaKey
{
public:
  ObCatalogDatabasePrivSchemaKey();
  ObCatalogDatabasePrivSchemaKey(const ObCatalogDatabasePrivSchemaKey &other);
  ~ObCatalogDatabasePrivSchemaKey();
  bool operator==(const ObCatalogDatabasePrivSchemaKey &other) const;
  bool operator!=(const ObCatalogDatabasePrivSchemaKey &other) const;
  bool operator<(const ObCatalogDatabasePrivSchemaKey &other) const;
  ObCatalogDatabasePrivSchemaKey &operator=(const ObCatalogDatabasePrivSchemaKey &other);
  int assign(const ObCatalogDatabasePrivSchemaKey &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  TO_STRING_KV(K_(user_id), K_(catalog_name), K_(database_name));
public:
  int64_t user_id_;
  char catalog_name_ptr_[OB_MAX_CATALOG_NAME_BINARY_LENGTH];
  common::ObString catalog_name_;
  char database_name_ptr_[OB_MAX_CATALOG_OBJECT_NAME_BINARY_LENGTH];
  common::ObString database_name_;
};

struct ObCatalogDatabasePrivCompressSchemaInfo
{
public:
  ObCatalogDatabasePrivCompressSchemaInfo()
      : key_(), max_schema_version_(common::OB_INVALID_VERSION)
  {}
  ~ObCatalogDatabasePrivCompressSchemaInfo() {}
  TO_STRING_KV(K_(key), K_(max_schema_version));
public:
  ObCatalogDatabasePrivSchemaKey key_;
  int64_t max_schema_version_;
};

class ObCatalogDatabasePrivRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObCatalogDatabasePrivRecycleSchemaExecutor() = delete;
  ObCatalogDatabasePrivRecycleSchemaExecutor(
      const uint64_t tenant_id,
      const int64_t schema_version,
      const char *table_name,
      common::ObMySQLProxy *sql_proxy,
      ObSchemaHistoryRecycler *recycler);
  virtual ~ObCatalogDatabasePrivRecycleSchemaExecutor();
private:
  virtual bool is_valid() const;
  virtual int fill_schema_history_map();
  virtual int recycle_schema_history();
  virtual int compress_schema_history();

  virtual int gen_fill_schema_history_sql(int64_t start_idx, common::ObSqlString &sql);
  virtual int retrieve_schema_history(
      common::sqlclient::ObMySQLResult &result,
      ObCatalogDatabasePrivSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObCatalogDatabasePrivSchemaKey &cur_key,
      ObCatalogDatabasePrivSchemaKey &key);
  virtual int fill_schema_history(
      const ObCatalogDatabasePrivSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
      const common::ObIArray<ObCatalogDatabasePrivSchemaKey> &dropped_schema_keys,
      common::ObSqlString &sql);
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObCatalogDatabasePrivSchemaKey> &dropped_schema_keys);

  virtual int gen_batch_compress_schema_history_sql(
      const common::ObIArray<ObCatalogDatabasePrivCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObCatalogDatabasePrivCompressSchemaInfo> &compress_schema_infos);
private:
  common::hash::ObHashMap<ObCatalogDatabasePrivSchemaKey,
                          ObRecycleSchemaValue,
                          common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObCatalogDatabasePrivRecycleSchemaExecutor);
};

struct ObCatalogTablePrivSchemaKey
{
public:
  ObCatalogTablePrivSchemaKey();
  ObCatalogTablePrivSchemaKey(const ObCatalogTablePrivSchemaKey &other);
  ~ObCatalogTablePrivSchemaKey();
  bool operator==(const ObCatalogTablePrivSchemaKey &other) const;
  bool operator!=(const ObCatalogTablePrivSchemaKey &other) const;
  bool operator<(const ObCatalogTablePrivSchemaKey &other) const;
  ObCatalogTablePrivSchemaKey &operator=(const ObCatalogTablePrivSchemaKey &other);
  int assign(const ObCatalogTablePrivSchemaKey &other);
  void reset();
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  TO_STRING_KV(K_(user_id), K_(catalog_name), K_(database_name), K_(table_name));
public:
  int64_t user_id_;
  char catalog_name_ptr_[OB_MAX_CATALOG_NAME_BINARY_LENGTH];
  common::ObString catalog_name_;
  char database_name_ptr_[OB_MAX_CATALOG_OBJECT_NAME_BINARY_LENGTH];
  common::ObString database_name_;
  char table_name_ptr_[OB_MAX_CATALOG_OBJECT_NAME_BINARY_LENGTH];
  common::ObString table_name_;
};

struct ObCatalogTablePrivCompressSchemaInfo
{
public:
  ObCatalogTablePrivCompressSchemaInfo()
      : key_(), max_schema_version_(common::OB_INVALID_VERSION)
  {}
  ~ObCatalogTablePrivCompressSchemaInfo() {}
  TO_STRING_KV(K_(key), K_(max_schema_version));
public:
  ObCatalogTablePrivSchemaKey key_;
  int64_t max_schema_version_;
};

class ObCatalogTablePrivRecycleSchemaExecutor : public ObIRecycleSchemaExecutor
{
public:
  ObCatalogTablePrivRecycleSchemaExecutor() = delete;
  ObCatalogTablePrivRecycleSchemaExecutor(
      const uint64_t tenant_id,
      const int64_t schema_version,
      const char *table_name,
      common::ObMySQLProxy *sql_proxy,
      ObSchemaHistoryRecycler *recycler);
  virtual ~ObCatalogTablePrivRecycleSchemaExecutor();
private:
  virtual bool is_valid() const;
  virtual int fill_schema_history_map();
  virtual int recycle_schema_history();
  virtual int compress_schema_history();

  virtual int gen_fill_schema_history_sql(int64_t start_idx, common::ObSqlString &sql);
  virtual int retrieve_schema_history(
      common::sqlclient::ObMySQLResult &result,
      ObCatalogTablePrivSchemaKey &key,
      ObRecycleSchemaValue &value);
  virtual int fill_schema_history_key(
      const ObCatalogTablePrivSchemaKey &cur_key,
      ObCatalogTablePrivSchemaKey &key);
  virtual int fill_schema_history(
      const ObCatalogTablePrivSchemaKey &key,
      const ObRecycleSchemaValue &value);

  virtual int gen_batch_recycle_schema_history_sql(
      const common::ObIArray<ObCatalogTablePrivSchemaKey> &dropped_schema_keys,
      common::ObSqlString &sql);
  virtual int batch_recycle_schema_history(
      const common::ObIArray<ObCatalogTablePrivSchemaKey> &dropped_schema_keys);

  virtual int gen_batch_compress_schema_history_sql(
      const common::ObIArray<ObCatalogTablePrivCompressSchemaInfo> &compress_schema_infos,
      common::ObSqlString &sql);
  virtual int batch_compress_schema_history(
      const common::ObIArray<ObCatalogTablePrivCompressSchemaInfo> &compress_schema_infos);
private:
  common::hash::ObHashMap<ObCatalogTablePrivSchemaKey,
                          ObRecycleSchemaValue,
                          common::hash::NoPthreadDefendMode> schema_history_map_;
  DISALLOW_COPY_AND_ASSIGN(ObCatalogTablePrivRecycleSchemaExecutor);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_CATALOG_PRIV_SCHEMA_HISTORY_RECYCLER_H_
