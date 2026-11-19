/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_H_
#define OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace share
{
class ObUnifiedCatalogSyncTarget;
}
namespace obrpc
{
struct ObAlterTableArg;
struct ObCreateDatabaseArg;
struct ObCreateTableArg;
struct ObCreateTableLikeArg;
struct ObDropDatabaseArg;
struct ObDropTableArg;
struct ObFlashBackTableFromRecyclebinArg;
struct ObForkTableArg;
struct ObRenameTableArg;
}
namespace sql
{
class ObCreateTableStmt;
class ObSQLSessionInfo;

// Native DDL execution state reported by the Executor.
enum ObCatalogSyncDDLState
{
  CATALOG_SYNC_DDL_NOT_STARTED = 0, // The native DDL was not issued.
  CATALOG_SYNC_DDL_SUCCEEDED,       // The native DDL completion is confirmed.
  CATALOG_SYNC_DDL_NOOP,            // The native DDL completed without changing an object.
  CATALOG_SYNC_DDL_REJECTED,        // The native DDL was explicitly rejected.
  CATALOG_SYNC_DDL_UNKNOWN          // The native DDL result cannot be determined.
};

struct ObCatalogSyncPrepareResult
{
  ObCatalogSyncPrepareResult()
    : skip_local_ddl_(false), source_object_id_(OB_INVALID_ID)
  {}
  // prepare() has already resolved the operation as a no-op.
  bool skip_local_ddl_;
  // Source identity captured before the native DDL changes local metadata.
  uint64_t source_object_id_;
};

struct ObCatalogSyncDDLResult
{
  ObCatalogSyncDDLResult()
    : state_(CATALOG_SYNC_DDL_NOT_STARTED),
      ddl_ret_(OB_SUCCESS),
      object_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      schema_sync_timeout_us_(0)
  {}
  ObCatalogSyncDDLState state_;
  int ddl_ret_;                 // Native DDL return code.
  uint64_t object_id_;          // Result object identity when the Executor has it.
  int64_t schema_version_;      // Result schema version when the Executor has it.
  // Existing RPC timeout for post-DDL schema synchronization.
  int64_t schema_sync_timeout_us_;
};

/**
 * Lifecycle owned by one native DDL execution.
 *
 * prepare() runs before the native DDL and may validate or reserve remote state.
 * complete() consumes the explicit native result reported by the Executor.
 * compensate() follows native cleanup and its failure must not replace the primary DDL error.
 */
class ObCatalogSyncOperation
{
public:
  virtual ~ObCatalogSyncOperation() {}
  virtual int prepare(ObCatalogSyncPrepareResult &result) = 0;
  virtual int complete(const ObCatalogSyncDDLResult &result) = 0;
  virtual int compensate(const int /*ddl_ret*/) { return OB_SUCCESS; }

protected:
  static int prepare_unified_catalog_target(
      share::ObUnifiedCatalogSyncTarget &target,
      common::ObIAllocator &allocator);
};

class ObCatalogSyncOperationHandle
{
public:
  ObCatalogSyncOperationHandle();
  ~ObCatalogSyncOperationHandle();
  bool is_valid() const { return NULL != operation_; }
  bool is_active() const { return is_valid() || OB_SUCCESS != deferred_ret_; }
  int get_deferred_ret() const { return deferred_ret_; }
  ObCatalogSyncOperation *operator->() const { return operation_; }

private:
  friend class ObCatalogSync;
  void set(common::ObIAllocator &allocator,
           ObCatalogSyncOperation *operation);
  void set_deferred_error(const int deferred_ret);
  void reset();

private:
  common::ObIAllocator *allocator_;
  ObCatalogSyncOperation *operation_;
  int deferred_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObCatalogSyncOperationHandle);
};

enum class ObCatalogSyncBackend
{
  NONE = 0,
  HMS,
  UNIFIED_CATALOG
};

class ObCatalogSyncPrecheckResult
{
public:
  ObCatalogSyncPrecheckResult()
      : backend_(ObCatalogSyncBackend::NONE),
        source_object_id_(OB_INVALID_ID)
  {
  }
  bool need_operation() const { return ObCatalogSyncBackend::NONE != backend_; }
  bool is_unified_catalog() const { return ObCatalogSyncBackend::UNIFIED_CATALOG == backend_; }
  bool need_access_token() const { return is_unified_catalog(); }
  uint64_t get_source_object_id() const { return source_object_id_; }

private:
  friend class ObCatalogSync;
  ObCatalogSyncBackend backend_;
  uint64_t source_object_id_;
};

class ObCatalogSync
{
public:
  explicit ObCatalogSync(common::ObIAllocator &allocator) : allocator_(allocator) {}
  static ObCatalogSyncBackend snapshot_backend(const uint64_t tenant_id);
  int get_create_database_operator(const ObCatalogSyncBackend backend,
                                   const common::ObString &access_token,
                                   const obrpc::ObCreateDatabaseArg &create_database_arg,
                                   ObCatalogSyncOperationHandle &handle);
  int get_create_table_operator(const ObCatalogSyncBackend backend,
                                const common::ObString &access_token,
                                const common::ObString &database_name,
                                const obrpc::ObCreateTableArg &create_table_arg,
                                ObCatalogSyncOperationHandle &handle);
  int get_create_table_as_select_operator(const ObCatalogSyncBackend backend,
                                          const common::ObString &access_token,
                                          ObCreateTableStmt &stmt,
                                          ObCatalogSyncOperationHandle &handle);
  int get_create_table_like_operator(const ObCatalogSyncBackend backend,
                                     const common::ObString &access_token,
                                     const obrpc::ObCreateTableLikeArg &create_table_like_arg,
                                     ObCatalogSyncOperationHandle &handle);
  int get_fork_table_operator(const ObCatalogSyncBackend backend,
                              const common::ObString &access_token,
                              const obrpc::ObForkTableArg &fork_table_arg,
                              ObCatalogSyncOperationHandle &handle);
  int precheck_alter_table(const obrpc::ObAlterTableArg &alter_table_arg,
                           ObCatalogSyncPrecheckResult &precheck_result);
  int get_alter_table_operator(const ObCatalogSyncPrecheckResult &precheck_result,
                               const common::ObString &access_token,
                               ObSQLSessionInfo *session,
                               const obrpc::ObAlterTableArg &alter_table_arg,
                               ObCatalogSyncOperationHandle &handle);
  int precheck_rename_table(const obrpc::ObRenameTableArg &rename_table_arg,
                            ObCatalogSyncPrecheckResult &precheck_result);
  int get_rename_table_operator(const ObCatalogSyncPrecheckResult &precheck_result,
                                const common::ObString &access_token,
                                const obrpc::ObRenameTableArg &rename_table_arg,
                                ObCatalogSyncOperationHandle &handle);
  int get_flashback_table_operator(
      const obrpc::ObFlashBackTableFromRecyclebinArg &flashback_table_arg,
      ObCatalogSyncOperationHandle &handle);
  int get_drop_database_operator(const ObCatalogSyncBackend backend,
                                 const common::ObString &access_token,
                                 const obrpc::ObDropDatabaseArg &drop_database_arg,
                                 ObCatalogSyncOperationHandle &handle);
  int get_drop_table_operator(const ObSQLSessionInfo &session,
                              const obrpc::ObDropTableArg &drop_table_arg,
                              ObCatalogSyncOperationHandle &handle);

private:
  static bool is_supported_uc_partition_alter(const obrpc::ObAlterTableArg &arg);
  static bool is_supported_uc_column_alter(const obrpc::ObAlterTableArg &arg);
  static bool is_supported_uc_table_rename(const obrpc::ObAlterTableArg &arg);
  static bool is_supported_uc_alter(const obrpc::ObAlterTableArg &arg);
  static int validate_uc_alter_source(const uint64_t tenant_id,
                                      const obrpc::ObAlterTableArg &arg,
                                      uint64_t &source_table_id);
  static int precheck_unified_catalog_rename(
      const uint64_t tenant_id,
      const obrpc::ObRenameTableArg &rename_table_arg,
      uint64_t &source_table_id);

  common::ObIAllocator &allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObCatalogSync);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_CMD_OB_CATALOG_SYNC_H_
