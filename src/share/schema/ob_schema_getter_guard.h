/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
#define OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
#include <stdint.h>
#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_mgr_cache.h"
#include "share/schema/ob_package_info.h"
#include "share/schema/ob_routine_info.h"
#include "share/schema/ob_udt_info.h"
#include "share/schema/ob_outline_mgr.h"
#include "share/schema/ob_udt_mgr.h"
#include "share/schema/ob_catalog_schema_struct.h"
#include "share/schema/ob_external_resource_mgr.h"
#include "share/schema/ob_location_schema_struct.h"
#include "share/schema/ob_objpriv_mysql_schema_struct.h"
#include "share/schema/ob_ai_model_mgr.h"
#include "share/schema/ob_ai_provider_mgr.h"
#include "share/schema/ob_ai_gateway_mgr.h"
#include "share/schema/ob_ccl_schema_struct.h"
#include "share/schema/ob_ccl_rule_mgr.h"
#include "share/schema/ob_sensitive_rule_schema_struct.h"
#include "share/schema/ob_schema_guard_context.h"
#include "share/schema/ob_schema_scan_operator.h"
#include "share/schema/ob_java_policy_mgr.h"

namespace oceanbase
{
namespace lib
{
class Worker;
}
namespace common
{
class ObString;
class ObKVCacheHandle;
class ObISQLClient;
class ObMySQLTransaction;
template <class T>
	class ObIArray;
}
namespace sql {
class ObSQLSessionInfo;
}
namespace share
{
namespace schema
{

// Full-scan membership consistency for OnDemand path (old Array APIs ignore this).
// Default RELAXED. Multi-get on same Guard should set STRONG once on the Guard.
enum class ObSchemaScanConsistencyMode
{
  RELAXED = 0,
  STRONG = 1,
};

class IdVersion;
class ObColumnSchemaV2;
class ObDBPriv;
class ObDatabaseSchema;
class ObMultiVersionSchemaService;
class ObPrivMgr;
class ObSimpleDatabaseSchema;
class ObSimplePackageSchema;
class ObSimpleRoutineSchema;
class ObSimpleSynonymSchema;
class ObSimpleSysVariableSchema;
class ObSimpleTablegroupSchema;
class ObSimpleTenantSchema;
class ObSimpleTriggerSchema;
class ObSimpleJavaPolicySchema;
class ObSimpleMockFKParentTableSchema;
class ObMockFKParentTableSchema;
class ObSimpleTableSchemaV2;
class ObTablegroupSchema;
class ObTablePriv;
class ObTableSchema;
class ObTenantSchema;
class ObTriggerInfo;
class ObUDTTypeInfo;
class ObUDF;
class ObUserInfo;
class ObISchemaGetterGuardTableImpl;
class ObOriginalSchemaGetterGuard;
class ObOnDemandSchemaGetterGuard;
class ObSimpleSchemaOnDemandCoordinator;
class SchemaName;
struct ObNeedPriv;
struct ObSessionPrivInfo;
struct ObStmtNeedPrivs;
struct ObUserLoginInfo;


class ObSimpleTableSchemaIterator;
class ObTableSchemaIterator;
class ObSchemaLiteTableIterator;
class ObSchemaLitePartJoinIterator;
struct ObSchemaLiteScanSpec;
enum class ObSchemaLiteJoinSide : int8_t;
class ObTableSchemaIterator;

class ObSchemaGetterGuard : public ObSchemaGuardContext
{
friend class ObMultiVersionSchemaService;
friend class MockSchemaService;
friend class ObOriginalSchemaGetterGuard;
friend class ObOnDemandSchemaGetterGuard;
friend class ObSimpleSchemaOnDemandCoordinator;
friend class ObSchemaLiteTableIterator;
friend class ObSchemaLitePartJoinIterator;

public:

	enum CheckTableType
	{
		ALL_NON_HIDDEN_TYPES = 0,
		TEMP_TABLE_TYPE = 1,
		NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE = 2,
    USER_HIDDEN_TABLE_TYPE = 3,
	};

	enum SchemaGuardType
	{
		INVALID_SCHEMA_GUARD_TYPE = 0,
		SCHEMA_GUARD = 1,
		TENANT_SCHEMA_GUARD = 2,
		TABLE_SCHEMA_GUARD = 3
	};

	ObSchemaGetterGuard();
  explicit ObSchemaGetterGuard(const ObSchemaMgrItem::Mod mod);
	virtual ~ObSchemaGetterGuard();
  int reset();
  // Create a point-lookup guard with the same sys/tenant snapshots as source.
  // Object pins, local allocations and lite-scan transactions are not shared.
  // The caller may reset this guard after consuming one table and its dependents.
  int init_from_snapshot(const ObSchemaGetterGuard &source, const uint64_t tenant_id);
	void dump();
	OB_INLINE bool is_inited() const { return is_inited_; }

	int get_schema_version(const uint64_t tenant_id, int64_t &schema_version) const;

	/*
   * with_mv: if index_tid_array contains ematerialized view.
   * with_global_index: if index_tid_array contains global index.
   * with_domain_index: if index_tid_array contains domain index.
   */
	int get_can_read_index_array(
      const uint64_t tenant_id,
      const uint64_t table_id,
      uint64_t *index_tid_array,
      int64_t &size,
      bool with_mv,
      bool with_global_index = true,
      bool with_domain_index = true,
      bool with_spatial_index = true,
      bool with_vector_index = true,
      bool with_async_index = false);
  int get_table_mlog_schema(const uint64_t tenant_id,
                            const uint64_t data_table_id,
                            const ObTableSchema *&mlog_schema);
  int check_has_local_unique_index(
      const uint64_t tenant_id,
      const uint64_t table_id,
      bool &has_local_unique_index);
  int get_all_unique_index(const uint64_t tenant_id,
                           const uint64_t table_id,
                           ObIArray<uint64_t> &unique_index_ids);
	bool is_tenant_schema_valid(const int64_t tenant_id) const;
	/*
   interface for simple schema
   */
	int get_simple_table_schema(
      const uint64_t tenant_id,
      const uint64_t table_id,
			const ObSimpleTableSchemaV2 *&table_schema);
	int get_simple_table_schema(
      const uint64_t tenant_id,
			const uint64_t database_id,
			const ObString &table_name,
			const bool is_index,
			const ObSimpleTableSchemaV2 *&simple_table_schema,
      const bool with_hidden_flag = false,
      const bool is_built_in_index = false);
	int get_table_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
	// Get the data tables in the tenant that carry async indexes.
	// Original: ObSchemaMgr::table_with_async_index_infos_ (no full-table scan).
	// OnDemand: scan __all_table by async index_type, then load data-table Simple.
	int get_table_schemas_with_async_index_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_database_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObSimpleDatabaseSchema *> &database_schemas);
	int get_tablegroup_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObTablegroupSchema*> &tablegroup_schemas);

	int get_user_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObUserInfo *> &user_schemas);
	int get_database_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const  ObDatabaseSchema *> &database_schemas);
	int get_tablegroup_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObSimpleTablegroupSchema*> &tablegroup_schemas);
	int get_table_schemas_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_view_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObTableSchema *> &table_schemas);
	int get_outline_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObOutlineInfo *> &outline_infos);
	int get_package_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObPackageInfo *> &package_infos);
	int get_routine_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObRoutineInfo *> &routine_infos);
	int get_trigger_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObTriggerInfo *> &triger_infos);
	int get_udt_infos_in_tenant(const uint64_t tenant_id,
			common::ObIArray<const ObUDTTypeInfo *> &udt_info);

  int get_audit_schemas_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObSAuditSchema *> &security_audits);
  int get_audit_schema_in_tenant(const uint64_t tenant_id,
                                 const ObSAuditType audit_type,
                                 const uint64_t owner_id,
                                 const ObSAuditOperationType operation_type,
                                 const ObSAuditSchema *&audit_schema) const;
  int get_audit_schema_in_owner(const uint64_t tenant_id,
                                const ObSAuditType audit_type,
                                const uint64_t object_id,
                                common::ObIArray<const ObSAuditSchema *> &audit_schemas) const;
  int check_allow_audit(const uint64_t tenant_id,
                        ObSAuditType &audit_type,
                        const uint64_t owner_id,
                        ObSAuditOperationType &operation_type,
                        const int return_code,
                        uint64_t &audit_id,
                        bool &is_allow_audit);
  int check_allow_audit_by_default(const uint64_t tenant_id,
                                   ObSAuditType &audit_type,
                                   ObSAuditOperationType &operation_type,
                                   const int return_code,
                                   uint64_t &audit_id,
                                   bool &is_allow_audit);

  int get_synonym_infos_in_tenant(const uint64_t tenant_id,
                              common::ObIArray<const ObSynonymInfo *> &synonym_info);
  int get_table_schemas_in_database(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_table_schemas_in_database(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id,
                                      const uint64_t tablegroup_id,
                                      common::ObIArray<const ObTableSchema *> &table_schemas);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id,
                                      const uint64_t tablegroup_id,
                                      common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas);
  int get_primary_table_schema_in_tablegroup(const uint64_t tenant_id,
                                             const uint64_t tablegroup_id,
                                             const ObSimpleTableSchemaV2 *&primary_table_schema);

  int get_table_schemas_in_tablespace(const uint64_t tenant_id,
                                      const uint64_t tablespace_id,
                                      common::ObIArray<const ObTableSchema *> &table_schemas);

  // Iterator overloads (old path): fill Array from mgr, then iter.init(BY_SCHEMA).
  // Pointer from get_next() only valid until next get_next() / iter destruction.
  int get_table_schemas_in_tenant(const uint64_t tenant_id,
                                  ObSimpleTableSchemaIterator &iter);
  int get_table_schemas_in_tenant(const uint64_t tenant_id,
                                  ObTableSchemaIterator &iter);
  int get_view_schemas_in_tenant(const uint64_t tenant_id,
                                 ObTableSchemaIterator &iter);
  int get_table_schemas_in_database(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    ObSimpleTableSchemaIterator &iter);
  int get_table_schemas_in_database(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    ObTableSchemaIterator &iter);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id,
                                      const uint64_t tablegroup_id,
                                      ObSimpleTableSchemaIterator &iter);
  int get_table_schemas_in_tablegroup(const uint64_t tenant_id,
                                      const uint64_t tablegroup_id,
                                      ObTableSchemaIterator &iter);
  int get_table_schemas_in_tablespace(const uint64_t tenant_id,
                                      const uint64_t tablespace_id,
                                      ObTableSchemaIterator &iter);

  // 附6.1 lite full-scan: same call site for Original / OnDemand.
  // OnDemand -> stream inner SQL (table-only); part/subpart via scan_lite_part_rows.
  // Original -> Simple schemas + PartIter into the same ObSchemaLiteTableResult.
  int scan_lite_tables(const ObSchemaLiteScanSpec &spec, ObSchemaLiteTableIterator &iter);
  // COUNT_TABLES / COUNT_PART_NUM. Original sums Simple; OnDemand COUNT(*) on definition family
  // (COUNT_PART_NUM: JOIN + NOT EXISTS, not part_level WHERE).
  int count_lite_tables(const ObSchemaLiteScanSpec &spec, int64_t &count);
  // A-tier: INNER JOIN t ⋈ p|s. Original flattens Simple + PartIter.
  int scan_lite_part_rows(const ObSchemaLiteScanSpec &spec,
                          const ObSchemaLiteJoinSide join_side,
                          ObSchemaLitePartJoinIterator &iter);
  // Optional early cleanup. Without this call, guard reset/destruction
  // automatically ends the transaction and releases its connection.
  // Keeps the guard initialized and preserves pinned schema objects.
  // SQL cursors using this transaction must first reach OB_ITER_END, or their
  // iterators must be reset/destroyed.
  // Subsequent STRONG scans/enumerations start a new transaction and snapshot;
  // consistency across this call is not guaranteed.
  // A no-op if no transaction exists; repeated calls are safe.
  int release_schema_scan_trans_early();
  bool is_on_demand_schema_mode() const
  { return ObSimpleSchemaMode::ON_DEMAND == schema_mode_; }

  int get_simple_tenant_schemas(common::ObIArray<const ObSimpleTenantSchema *> &tenant_schemas) const;

  int get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) const;
  int get_user_tenant_count(int64_t &count) const;
  int get_available_tenant_ids(common::ObIArray<uint64_t> &tenant_ids) const;
  int get_tablegroup_ids_in_tenant(const uint64_t tenant_id,
                                   common::ObIArray<uint64_t> &tablegroup_id_array);
  int get_routine_ids_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &routine_id_array,
                                bool is_agent_mode = false);
  int get_package_ids_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &package_id_array);
  int get_udt_ids_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<uint64_t> &udt_id_array);
  int get_table_ids_in_tenant(const uint64_t tenant_id,
                              common::ObIArray<uint64_t> &table_ids);
  int get_table_ids_in_database(const uint64_t tenant_id,
                                const uint64_t dataspace_id,
                                common::ObIArray<uint64_t> &table_id_array);
  int get_table_ids_in_tablegroup(const uint64_t tenant_id,
                                  const uint64_t tablegroup_id,
                                  common::ObIArray<uint64_t> &table_id_array);
  // Sparse-filter ids
  int get_tmp_table_ids_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<uint64_t> &table_ids);
  int get_external_table_ids_in_tenant(const uint64_t tenant_id,
                                       common::ObIArray<uint64_t> &table_ids);
  int get_duplicate_table_ids_in_tenant(const uint64_t tenant_id,
                                        common::ObIArray<uint64_t> &table_ids);
  int get_encrypted_table_ids_in_tenant(const uint64_t tenant_id,
                                        common::ObIArray<uint64_t> &table_ids);
  int get_dynamic_partition_table_ids_in_tenant(const uint64_t tenant_id,
                                                common::ObIArray<uint64_t> &table_ids);
  int get_trigger_ids_in_database(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  common::ObIArray<uint64_t> &trigger_ids);
  int get_routine_ids_in_database(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  common::ObIArray<uint64_t> &routine_ids);
  int get_udt_ids_in_database(const uint64_t tenant_id,
                              const uint64_t database_id,
                              common::ObIArray<uint64_t> &udt_ids);
  int get_routine_info_in_udt(const uint64_t tenant_id,
                              const uint64_t udt_id,
                              const uint64_t subprogram_id,
                              const ObRoutineInfo *&routine_info);
  int get_routine_infos_in_udt(const uint64_t tenant_id,
                               const uint64_t udt_id,
                               common::ObIArray<const ObRoutineInfo *> &routine_infos);
  int get_routine_info_in_package(const uint64_t tenant_id,
                                  const uint64_t package_id,
                                  const uint64_t subprogram_id,
                                  const ObRoutineInfo *&routine_info);
  int get_routine_infos_in_package(const uint64_t tenant_id,
                                   const uint64_t package_id,
                                   common::ObIArray<const ObRoutineInfo *> &routine_infos);
  int get_sequence_schemas_in_database(const uint64_t tenant_id,
                                       const uint64_t database_id,
                                       common::ObIArray<const ObSequenceSchema*> &sequence_schemas);
  int get_label_se_policy_infos_in_tenant(const uint64_t tenant_id,
                                          common::ObIArray<const ObLabelSePolicySchema *> &label_se_policy_infos);
  int get_label_se_component_infos_in_tenant(const uint64_t tenant_id,
                                             common::ObIArray<const ObLabelSeComponentSchema *> &label_se_component_infos);
  int get_label_se_label_infos_in_tenant(const uint64_t tenant_id,
                                         common::ObIArray<const ObLabelSeLabelSchema *> &label_se_label_infos);
  int get_label_se_user_level_infos_in_tenant(const uint64_t tenant_id,
                                              common::ObIArray<const ObLabelSeUserLevelSchema *> &label_se_user_level_infos);
  int get_profile_infos_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObProfileSchema *> &profile_infos);

  // generate tablet-table map by specified tenant_id
  // @notice:
  // - schema_guard should be tenant schema guard, which is not lazy and is formal.
  //   (Get from ObMultiVersionSchemaService::get_tenant_schema_guard() without specified schema_version)
  // @param[in]:
  // - tenant_id: tenant id
  // @param[out]:
  // - tablet_map: pairs of tablet-table. map will be created by this function.
  int generate_tablet_table_map(
      const uint64_t tenant_id,
      common::hash::ObHashMap<common::ObTabletID, uint64_t> &tablet_map);
  /*
     get_id
  */
  int get_tenant_id(const common::ObString &tenant_name,
                    uint64_t &tenant_id);
  int get_user_id(uint64_t tenant_id,
                  const common::ObString &user_name,
                  const common::ObString &host_name,
                  uint64_t &user_id,
                  const bool is_role = false);
  int get_database_id(uint64_t tenant_id,
                      const common::ObString &database_name,
                      uint64_t &database_id);
  int get_tablegroup_id(uint64_t tenant_id,
                        const common::ObString &tablegroup_name,
                        uint64_t &tablegroup_id);
  int get_table_id(uint64_t tenant_id,
                   uint64_t database_id,
                   const common::ObString &table_name,
                   const bool is_index,
                   const CheckTableType check_type,  // if temporary table is visable
                   uint64_t &table_id,
                   const bool is_built_in_index = false);
  int get_table_id(uint64_t tenant_id,
                   const common::ObString &database_name,
                   const common::ObString &table_name,
                   const bool is_index,
                   const CheckTableType check_type,  // if temporary table is visable
                   uint64_t &table_id,
                   const bool is_built_in_index = false);
  int get_foreign_key_id(const uint64_t tenant_id,
                         const uint64_t database_id,
                         const common::ObString &foreign_key_name,
                         uint64_t &foreign_key_id);
  int get_foreign_key_info(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const common::ObString &foreign_key_name,
                          ObSimpleForeignKeyInfo &foreign_key_info);
  int get_constraint_id(const uint64_t tenant_id,
                        const uint64_t database_id,
                        const common::ObString &constraint_name,
                        uint64_t &constraint_id);
  int get_constraint_info(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const common::ObString &constraint_name,
                          ObSimpleConstraintInfo &constraint_info) const;
  int get_tenant_name_case_mode(const uint64_t tenant_id, common::ObNameCaseMode &mode);
  int get_tenant_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode &compat_mode);
  int get_tenant_read_only(const uint64_t tenant_id, bool &read_only);
  /*
     get_schema
  */
  // basic interface
  int get_tenant_info(uint64_t tenant_id,
                      const ObTenantSchema *&tenant_info);
  int get_tenant_info(uint64_t tenant_id,
                      const ObSimpleTenantSchema *&tenant_info);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const ObDatabaseSchema *&database_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const uint64_t database_id,
                          const ObSimpleDatabaseSchema *&database_schema);
  int get_database_schema(const uint64_t tenant_id,
                          const common::ObString &database_name,
                          const ObDatabaseSchema *&database_schema);
  int get_tablegroup_schema(const uint64_t tenant_id,
                            const uint64_t tablegroup_id,
                            const ObTablegroupSchema *&tablegourp_schema);
  int get_tablegroup_schema(const uint64_t tenant_id,
                            const uint64_t tablegroup_id,
                            const ObSimpleTablegroupSchema *&tablegroup_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t table_id,
                       const ObTableSchema *&table_schema);
  int get_table_schema(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &table_name,
                       const bool is_index,
                       const ObTableSchema *&table_schema,
                       const bool with_hidden_flag = false,
                       const bool is_built_in_index = false);
  int get_table_schema(const uint64_t tenant_id,
                       const common::ObString &database_name,
                       const common::ObString &table_name,
                       const bool is_index,
                       const ObTableSchema *&table_schema,
                       const bool with_hidden_flag = false,
                       const bool is_built_in_index = false);
  int get_index_meta_infos_with_data_table_id(const uint64_t tenant_id,
                                              const uint64_t data_table_id,
                                              ObIArray<ObAuxTableMetaInfo> &index_meta_infos);
  int get_index_schemas_with_data_table_id(const uint64_t tenant_id,
                                           const uint64_t data_table_id,
                                           ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas);
  int get_lob_schemas_with_data_table_id(const uint64_t tenant_id,
                                         const uint64_t data_table_id,
                                         ObIArray<const ObSimpleTableSchemaV2 *> &lob_schemas);
  int get_sys_variable_schema(const common::ObString &tenant_name,
                              const ObSysVariableSchema *&sys_variable_schema);
  int get_sys_variable_schema(const uint64_t tenant_id,
                              const ObSysVariableSchema *&sys_variable_schema);
  int get_sys_variable_schema(const uint64_t tenant_id,
                              const ObSimpleSysVariableSchema *&sys_variable_schema);
  int get_tenant_system_variable(uint64_t tenant_id,
                                 const common::ObString &var_name,
                                 const ObSysVarSchema *&var_schema);
  int get_tenant_system_variable(uint64_t tenant_id,
                                 ObSysVarClassType var_id,
                                 const ObSysVarSchema *&var_schema);
  int get_tenant_info(const common::ObString &tenant_name,
                      const ObTenantSchema *&tenant_schema);
  int get_user_info(const uint64_t tenant_id,
                    const uint64_t user_id,
                    const ObUserInfo *&user_info);
  int get_user_info(const uint64_t tenant_id,
                    const common::ObString &user_name,
                    const common::ObString &host_name,
                    const ObUserInfo *&user_info);
  int get_user_info(const uint64_t tenant_id,
                    const common::ObString &user_name,
                    common::ObIArray<const ObUserInfo *> &users_info);
  int get_column_schema(const uint64_t tenant_id,
                        const uint64_t table_id,
                        const uint64_t column_id,
                        const ObColumnSchemaV2 *&column_schema);
  int get_column_schema(const uint64_t tenant_id,
                        const uint64_t table_id,
                        const common::ObString &column_name,
                        const ObColumnSchemaV2 *&column_schema);

  // for resolver
  int get_can_write_index_array(const uint64_t tenant_id,
                                const uint64_t table_id,
                                uint64_t *index_tid_array,
                                int64_t &size,
                                bool only_global = false,
                                bool with_mlog = false);

  // for readonly
  int verify_read_only(const uint64_t tenant_id, const ObStmtNeedPrivs &stmt_need_privs);
  int is_user_empty_passwd(const ObUserLoginInfo &login_info, bool &is_empty_passwd_account);
  int check_user_access(const ObUserLoginInfo &login_info,
                        ObSessionPrivInfo &s_priv,
                        common::ObIArray<uint64_t> &enable_role_id_array,
                        SSL *ssl_st,
                        const ObUserInfo *&sel_user_info);
  int check_catalog_access(const ObSessionPrivInfo &session_priv,
                           const common::ObIArray<uint64_t> &enable_role_id_array,
                           const common::ObString &catalog_name);
  int check_catalog_access(const ObSessionPrivInfo &session_priv,
                           const common::ObIArray<uint64_t> &enable_role_id_array,
                           const uint64_t catalog_id);
  int check_catalog_db_access(const ObSessionPrivInfo &session_priv,
                              const common::ObIArray<uint64_t> &enable_role_id_array,
                              const common::ObString &catalog_name,
                              const common::ObString &database_name,
                              ObPrivSet &db_priv_set);
  int check_catalog_db_access(const ObSessionPrivInfo &session_priv,
                              const common::ObIArray<uint64_t> &enable_role_id_array,
                              const uint64_t catalog_id,
                              const common::ObString &database_name,
                              ObPrivSet &db_priv_set);
  int check_catalog_show(const ObSessionPrivInfo &session_priv,
                         const common::ObIArray<uint64_t> &enable_role_id_array,
                         const common::ObString &catalog_name,
                         bool &allow_show);
  int check_sensitive_rule_access(const ObSessionPrivInfo &session_priv,
                                  const common::ObIArray<uint64_t> &enable_role_id_array,
                                  const common::ObString &sensitive_rule_name);
  int check_sensitive_rule_access(const ObSessionPrivInfo &session_priv,
                                  const common::ObIArray<uint64_t> &enable_role_id_array,
                                  const uint64_t sensitive_rule_id);
  int check_sensitive_rule_db_access(const ObSessionPrivInfo &session_priv,
                                     const common::ObIArray<uint64_t> &enable_role_id_array,
                                     const common::ObString &sensitive_rule_name,
                                     const common::ObString &database_name);
  int check_sensitive_rule_db_access(const ObSessionPrivInfo &session_priv,
                                     const common::ObIArray<uint64_t> &enable_role_id_array,
                                     const uint64_t sensitive_rule_id,
                                     const common::ObString &database_name);
  int check_sensitive_rule_show(const ObSessionPrivInfo &session_priv,
                                const common::ObIArray<uint64_t> &enable_role_id_array,
                                const common::ObString &sensitive_rule_name,
                                bool &allow_show);
  int check_db_access(ObSessionPrivInfo &s_priv,
                      const common::ObIArray<uint64_t> &enable_role_id_array,
                      const uint64_t catalog_id,
                      const common::ObString &database_name);
  int check_db_access(ObSessionPrivInfo &s_priv,
                      const common::ObIArray<uint64_t> &enable_role_id_array,
                      const common::ObString& database_name);
  int check_db_show(const ObSessionPrivInfo &session_priv,
                    const common::ObIArray<uint64_t> &enable_role_id_array,
                    const common::ObString &db,
                    bool &allow_show);
  int check_table_show(const ObSessionPrivInfo &session_priv,
                       const common::ObIArray<uint64_t> &enable_role_id_array,
                       const uint64_t catalog_id,
                       const common::ObString &db,
                       const common::ObString &table,
                       bool &allow_show);
  int check_table_show(const ObSessionPrivInfo &session_priv,
                       const common::ObIArray<uint64_t> &enable_role_id_array,
                       const common::ObString &db,
                       const common::ObString &table,
                       bool &allow_show);
  int check_routine_show(const ObSessionPrivInfo &session_priv,
                         const common::ObIArray<uint64_t> &enable_role_id_array,
                         const common::ObString &db,
                         const common::ObString &routine,
                         bool &allow_show,
                         int64_t routine_type);

  int check_ora_priv(const uint64_t tenant_id,
                     const uint64_t uid,
                     const ObStmtOraNeedPrivs &stmt_need_privs,
                     const common::ObIArray<uint64_t> &role_id_array);
  int check_priv(const ObSessionPrivInfo &session_priv,
                 const common::ObIArray<uint64_t> &enable_role_id_array,
                 const ObStmtNeedPrivs &stmt_need_privs);
  int check_priv_or(const ObSessionPrivInfo &session_priv,
                    const common::ObIArray<uint64_t> &enable_role_id_array,
                    const ObStmtNeedPrivs &stmt_need_privs);
  int check_db_access(const ObSessionPrivInfo &session_priv,
                      const common::ObIArray<uint64_t> &enable_role_id_array,
                      const common::ObString &db,
                      ObPrivSet &db_priv_set,
                      bool print_warn = true);
  int check_single_table_priv(const ObSessionPrivInfo &session_priv,
                              const common::ObIArray<uint64_t> &enable_role_id_array,
                              const ObNeedPriv &table_need_priv);
  int check_single_table_priv_or(const ObSessionPrivInfo &session_priv,
                                 const common::ObIArray<uint64_t> &enable_role_id_array,
                                 const ObNeedPriv &table_need_priv);

  int check_priv_any_column_priv(const ObSessionPrivInfo &session_priv,
                                 const common::ObIArray<uint64_t> &enable_role_id_array,
                                 const common::ObString &db_name,
                                 const common::ObString &table_name,
                                 bool &pass);

  int collect_all_priv_for_column(const ObSessionPrivInfo &session_priv,
                                  const common::ObIArray<uint64_t> &enable_role_id_array,
                                  const common::ObString &db_name,
                                  const common::ObString &table_name,
                                  const common::ObString &column_name,
                                  ObPrivSet &column_priv_set);

  int get_session_priv_info(const uint64_t tenant_id,
                            const uint64_t user_id,
                            const ObString &database_name,
                            ObSessionPrivInfo &session_priv);
  int get_user_infos_with_tenant_id(const uint64_t tenant_id,
                                    common::ObIArray<const ObUserInfo *> &user_infos);
  int get_db_priv_with_tenant_id(const uint64_t tenant_id,
                                 common::ObIArray<const ObDBPriv *> &db_privs);
  int get_column_priv_in_table(const uint64_t tenant_id,
                              const uint64_t user_id,
                              const ObString &db,
                              const ObString &table,
                              ObIArray<const ObColumnPriv *> &column_privs);

  int get_column_priv_in_table(const ObTablePrivSortKey &table_priv_key,
                              ObIArray<const ObColumnPriv *> &column_privs);
  int get_column_priv(const ObColumnPrivSortKey &column_priv_key,
                          const ObColumnPriv *&column_priv);

  int get_column_priv_id(const uint64_t tenant_id,
                        const uint64_t user_id,
                        const ObString &db,
                        const ObString &table,
                        const ObString &column,
                        uint64_t &priv_id);
  int get_column_priv_with_user_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    common::ObIArray<const ObColumnPriv*> &column_privs);
  int get_column_priv_set(const ObColumnPrivSortKey &column_priv_key, ObPrivSet &priv_set);
  int get_catalog_priv_set(const ObCatalogPrivSortKey &catalog_priv_key,
                           ObPrivSet &priv_set);
  int get_catalog_priv_with_user_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    common::ObIArray<const ObCatalogPriv *> &catalog_privs);
  int get_sensitive_rule_priv_set(const ObSensitiveRulePrivSortKey &sensitive_rule_priv_key,
                                  ObPrivSet &priv_set);
  int get_sensitive_rule_priv_with_user_id(const uint64_t tenant_id,
                                           const uint64_t user_id,
                                           common::ObIArray<const ObSensitiveRulePriv *> &sensitive_rule_privs);
  int get_db_priv_with_user_id(const uint64_t tenant_id,
                               const uint64_t user_id,
                               common::ObIArray<const ObDBPriv*> &db_privs);

  int get_routine_priv_with_user_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    common::ObIArray<const ObRoutinePriv*> &routine_privs);
  int get_table_priv_with_tenant_id(const uint64_t tenant_id,
                                    common::ObIArray<const ObTablePriv *> &table_privs);
  int get_table_priv_with_user_id(const uint64_t tenant_id,
                                  const uint64_t user_id,
                                  common::ObIArray<const ObTablePriv *> &table_privs);
  int get_obj_priv_with_grantee_id(const uint64_t tenant_id,
                                   const uint64_t grnatee_id,
                                   common::ObIArray<const ObObjPriv *> &obj_privs);
  int get_obj_priv_with_grantor_id(const uint64_t tenant_id,
                                   const uint64_t grantor_id,
                                   common::ObIArray<const ObObjPriv *> &obj_privs,
                                   bool reset_flag);
  int get_obj_priv_with_obj_id(const uint64_t tenant_id,
                               const uint64_t obj_id,
                               const uint64_t obj_type,
                               common::ObIArray<const ObObjPriv *> &obj_privs,
                               bool reset_flag);
  int get_obj_privs_in_ur_and_obj(const uint64_t tenant_id,
                                  const ObObjPrivSortKey &obj_key,
                                  ObPackedObjPriv &obj_privs);
  int get_obj_privs_in_grantor_ur_obj_id(const uint64_t tenant_id,
                                         const ObObjPrivSortKey &obj_key,
                                         common::ObIArray<const ObObjPriv *> &obj_privs);
  int get_obj_privs_in_grantor_obj_id(const uint64_t tenant_id,
                                         const ObObjPrivSortKey &obj_key,
                                         common::ObIArray<const ObObjPriv *> &obj_privs);

  // 以真实库名查询鉴权选中的库级 ACL，匹配方式与 get_db_priv_set 的普通重载一致。
  int get_db_priv(const ObOriginalDBKey &db_priv_key,
                  const ObDBPriv *&db_priv);
  int get_db_priv_set(const uint64_t tenant_id,
                      const uint64_t user_id,
                      const common::ObString &db,
                      ObPrivSet &priv_set);
  // for compatible
  int get_db_priv_set(const ObOriginalDBKey &db_priv_key, ObPrivSet &priv_set, bool is_pattern = false);
  int get_table_priv_set(const ObTablePrivSortKey &table_priv_key, ObPrivSet &priv_set);
  int get_routine_priv_set(const ObRoutinePrivSortKey &routine_priv_key, ObPrivSet &priv_set);
  int get_obj_mysql_priv_set(const ObObjMysqlPrivSortKey &obj_mysql_priv_key, ObPrivSet &priv_set);
  int get_obj_privs( const ObObjPrivSortKey &obj_priv_key, ObPackedObjPriv &obj_privs);
  int get_obj_mysql_priv_with_user_id(const uint64_t tenant_id,
                                      const uint64_t user_id,
                                      ObIArray<const ObObjMysqlPriv *> &obj_mysql_privs);
  //TODO@xiyu: ObDDLOperator::drop_tablegroup
  int check_database_exists_in_tablegroup(
      const uint64_t tenant_id,
      const uint64_t tablegroup_id,
      bool &not_empty);

  // xiyu: just return pointer to save my life.
  const ObUserInfo *get_user_info(const uint64_t tenant_id, const uint64_t user_id);
  const ObTablegroupSchema *get_tablegroup_schema(const uint64_t tenant_id, const uint64_t tablegroup_id);
  const ObColumnSchemaV2 *get_column_schema(const uint64_t tenant_id,
                                            const uint64_t table_id,
                                            const uint64_t column_id);
  const ObTenantSchema *get_tenant_info(const common::ObString &tenant_name);

  // nijia.nj: check exist, for root_service/ddl_service/ddl_operator
  //
  int check_database_exist(const uint64_t tenant_id,
                           const common::ObString &database_name,
                           bool &is_exist,
                           uint64_t *database_id = NULL);
  int check_database_in_recyclebin(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   bool &in_recyclebin);
  int check_database_exist(const uint64_t tenant_id,
                           const uint64_t database_id,
                           bool &is_exist);
  int check_tablegroup_exist(const uint64_t tenant_id,
                             const common::ObString &tablegroup_name,
                             bool &is_exist,
                             uint64_t *tablegroup_id = NULL);
  int check_tablegroup_exist(const uint64_t tenant_id,
                             const uint64_t tablegroup_id,
                             bool &is_exist);
  int check_oracle_object_exist(const uint64_t tenant_id, const uint64_t db_id,
      const common::ObString &object_name, const ObSchemaType &schema_type,
      const ObRoutineType &routine_type, const bool is_or_replace,
      common::ObIArray<ObSchemaType> &conflict_schema_types);
  int check_table_exist(const uint64_t tenant_id,
                        const uint64_t database_id,
                        const common::ObString &table_name,
                        const bool is_index,
                        const CheckTableType check_type, // if temporary table is visable
                        bool &is_exist,
                        uint64_t *table_id = NULL);
  int check_table_exist(const uint64_t tenant_id,
                        const uint64_t table_id,
                        bool &is_exist);
  int check_tenant_exist(const uint64_t tenant_id,
                         bool &is_exist);
  int check_outline_exist_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &outline_name,
                                    const bool is_format,
                                    uint64_t &outline_id,
                                    bool &exist);
  int check_outline_exist_with_sql(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &paramlized_sql,
                                   const bool is_format,
                                   bool &exist);
  int check_outline_exist_with_sql_id(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &sql_id,
                                   const bool is_format,
                                   bool &exist) ;
  int get_outline_info_with_name(const uint64_t tenant_id,
                                 const uint64_t database_id,
                                 const common::ObString &name,
                                 const bool is_format,
                                 const ObOutlineInfo *&outline_info);
  int get_outline_info_with_name(const uint64_t tenant_id,
                                 const common::ObString &db_name,
                                 const common::ObString &outline_name,
                                 const bool is_format,
                                 const ObOutlineInfo *&outline_info);
  int get_outline_info_with_signature(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const common::ObString &signature,
                                      const bool is_format,
                                      const ObOutlineInfo *&outline_info);
  //package
  int check_package_exist(uint64_t tenant_id, uint64_t database_id,
                          const common::ObString &package_name,
                          ObPackageType package_type, int64_t compatible_mode, bool &exist) ;
  int get_package_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &package_name,
                     ObPackageType package_type, int64_t compatible_mode, uint64_t &package_id) ;
  int get_package_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &package_name,
                       ObPackageType package_type,
                       int64_t compatible_mode,
                       const ObPackageInfo *&package_info) ;
  int get_package_info(const uint64_t tenant_id,
                       const uint64_t package_id,
                       const ObPackageInfo *&package_info);
  int get_simple_package_info(const uint64_t tenant_id,
                              const uint64_t package_id,
                              const ObSimplePackageSchema *&package_info);
  int get_package_routine_infos(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                                const common::ObString &routine_name, ObRoutineType routine_type,
                                common::ObIArray<const ObIRoutineInfo *> &routine_infos,
   share::schema::ObRoutineType inside_routine_type = share::schema::ObRoutineType::ROUTINE_PACKAGE_TYPE);

  int get_trigger_info(const uint64_t tenant_id,
                       const uint64_t trigger_id,
                       const ObTriggerInfo *&trigger_info);
  int get_trigger_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &trigger_name,
                       const ObTriggerInfo *&trigger_info);
  int get_package_info_from_trigger(const uint64_t tenant_id,
                                    const uint64_t package_id,
                                    const ObPackageInfo *&package_spec_info,
                                    const ObPackageInfo *&package_body_info);
  int get_package_id_from_trigger(uint64_t tenant_id,
                                  uint64_t database_id,
                                  const common::ObString &pacakge_name,
                                  ObPackageType package_type,
                                  uint64_t &package_id);
  int get_simple_trigger_schema(const uint64_t tenant_id,
                                const uint64_t trigger_id,
                                const ObSimpleTriggerSchema *&simple_trigger);
  //procedure
  inline int check_standalone_procedure_exist(uint64_t tenant_id, uint64_t database_id,
                                              const common::ObString &procedure_name, bool &exist) const
  {
    return check_routine_exist(tenant_id, database_id, common::OB_INVALID_ID, procedure_name,
                               0, ROUTINE_PROCEDURE_TYPE, exist);
  }
  inline int get_standalone_procedure_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &procedure_name,
                                         uint64_t &procedure_id)
  {
    return get_routine_id(tenant_id, database_id, common::OB_INVALID_ID, procedure_name,
                          0, ROUTINE_PROCEDURE_TYPE, procedure_id);
  }
  inline int get_standalone_procedure_info(uint64_t tenant_id, uint64_t database_id, const common::ObString &procedure_name,
                                           const ObRoutineInfo *&procedure_info)
  {
    return get_routine_info(tenant_id, database_id, common::OB_INVALID_ID, procedure_name,
                            0, ROUTINE_PROCEDURE_TYPE, procedure_info);
  }
  //function
  inline int check_standalone_function_exist(uint64_t tenant_id, uint64_t database_id,
                                              const common::ObString &function_name, bool &exist) const
  {
    return check_routine_exist(tenant_id, database_id, common::OB_INVALID_ID, function_name,
                               0, ROUTINE_FUNCTION_TYPE, exist);
  }
  inline int get_standalone_function_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &function_name,
                                         uint64_t &function_id)
  {
    return get_routine_id(tenant_id, database_id, common::OB_INVALID_ID, function_name,
                          0, ROUTINE_FUNCTION_TYPE, function_id);
  }
  inline int get_standalone_function_info(uint64_t tenant_id, uint64_t database_id, const common::ObString &function_name,
                                          const ObRoutineInfo *&function_info)
  {
    return get_routine_info(tenant_id, database_id, common::OB_INVALID_ID, function_name,
                            0, ROUTINE_FUNCTION_TYPE, function_info);
  }
  //routine
  int get_routine_info(const uint64_t tenant_id,
                       uint64_t routine_id,
                       const ObRoutineInfo *&routine_info);

  // udt
  int get_udt_info(const uint64_t tenant_id, const uint64_t udt_id, const ObUDTTypeInfo*& udt_info);
  int get_udt_routine_infos(uint64_t tenant_id, uint64_t database_id, uint64_t udt_id,
                            const common::ObString &routine_name, ObRoutineType routine_type,
                            common::ObIArray<const ObIRoutineInfo *> &routine_infos,
  share::schema::ObRoutineType inside_routine_type = share::schema::ObRoutineType::ROUTINE_UDT_TYPE);

  int get_synonym_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &name,
                       const ObSynonymInfo *&synonym_info);
  int get_synonym_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const common::ObString &name,
                       const ObSimpleSynonymSchema *&synonym_info);
  int get_simple_synonym_info(const uint64_t tenant_id,
                              const uint64_t synonym_id,
                              const ObSimpleSynonymSchema *&synonym_info);
  int check_synonym_exist_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &synonym_name,
                                    bool &exist,
                                    uint64_t &synonym_id);
  int get_object_with_synonym(const uint64_t tenant_id,
                              const uint64_t syn_database_id,
                              const common::ObString &syn_name,
                              uint64_t &obj_database_id,
                              uint64_t &synonym_id,
                              common::ObString &obj_table_name,
                              bool &do_exist,
                              bool search_public_schema = true,
                              bool *is_public = NULL) const;
  int get_outline_info_with_sql_id(const uint64_t tenant_id,
                                      const uint64_t database_id,
                                      const common::ObString &sql_id,
                                      const bool is_format,
                                      const ObOutlineInfo *&outline_info) ;
  //about user define function
  int check_udf_exist_with_name(const uint64_t tenant_id,
                                const common::ObString &name,
                                bool &exist,
                                uint64_t &udf_id);
  int get_udf_info(const uint64_t tenant_id,
                   const common::ObString &name,
                   const ObUDF *&udf_info,
                   bool &exist);

  // java policy
  int get_java_policy_schema(const uint64_t tenant_id,
                             const uint64_t key,
                             const ObSimpleJavaPolicySchema *&schema);

  int get_java_policy_schemas_in_tenant(const uint64_t tenant_id,
                                        common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas);

  int get_java_policy_schemas_of_grantee(const uint64_t tenant_id,
                                         const uint64_t grantee_id,
                                         common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas);

  int check_java_policy_exist(const uint64_t tenant_id,
                              const ObSimpleJavaPolicySchema::JavaPolicyKind kind,
                              const uint64_t grantee,
                              const uint64_t type_schema,
                              const common::ObString &type_name,
                              const common::ObString &name,
                              const common::ObString &action,
                              bool &is_exist);

  // udt
  int check_udt_exist(uint64_t tenant_id, uint64_t database_id,
                      uint64_t package_id, share::schema::ObUDTTypeCode type_code,
                      const common::ObString &udt_name, bool &exist);
  int get_udt_info(const uint64_t tenant_id, const uint64_t database_id, const uint64_t package_id,
                   const common::ObString &udt_name, const ObUDTTypeInfo *&udt_info);
  int get_udt_info(const uint64_t tenant_id, const uint64_t database_id, const uint64_t package_id,
                   const common::ObString &udt_name, const share::schema::ObUDTTypeCode &type_code,
                   const ObUDTTypeInfo *&udt_info);
  int get_udt_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                 const common::ObString &udt_name, uint64_t &udt_id);
  int get_udt_schemas_in_tenant(const uint64_t tenant_id,
                                common::ObIArray<const ObUDTTypeInfo *> &udt_schemas);

  int check_sequence_exist_with_name(const uint64_t tenant_id,
                                     const uint64_t database_id,
                                     const common::ObString &sequence_name,
                                     bool &exist,
                                     uint64_t &sequence_id,
                                     bool &is_system_generated) const;
  int check_context_exist_with_name(const uint64_t tenant_id,
                                     const common::ObString &context_name,
                                     const ObContextSchema *&context_schema,
                                     bool &exist);
  int check_context_exist_by_id(const uint64_t tenant_id,
                                const uint64_t context_id,
                                const ObContextSchema *&context_schema,
                                bool &exist);
  int get_sequence_schema(const uint64_t tenant_id,
                          const uint64_t sequence_id,
                          const ObSequenceSchema *&schema);
  int get_sequence_schema_with_name(const uint64_t tenant_id,
                                    const uint64_t database_id,
                                    const common::ObString &sequence_name,
                                    const ObSequenceSchema *&sequence_schema);
  int get_context_schema_with_name(const uint64_t tenant_id,
                                   const common::ObString &context_name,
                                   const ObContextSchema *&context_schema);

  // mock_fk_parent_table begin
  int get_mock_fk_parent_table_ids_in_database(const uint64_t tenant_id,
                                               const uint64_t database_id,
                                               ObIArray<uint64_t> &mock_fk_parent_table_ids);
  int get_simple_mock_fk_parent_table_schema(const uint64_t tenant_id,
                                             const uint64_t database_id,
                                             const common::ObString &name,
                                             const ObSimpleMockFKParentTableSchema *&schema);
  int get_simple_mock_fk_parent_table_schema(const uint64_t tenant_id,
                                              const uint64_t mock_fk_parent_table_id,
                                              const ObSimpleMockFKParentTableSchema *&schema);
  int get_mock_fk_parent_table_schema_with_name(const uint64_t tenant_id,
                                                const uint64_t database_id,
                                                const common::ObString &name,
                                                const ObMockFKParentTableSchema *&schema);
  int get_mock_fk_parent_table_schema_with_id(const uint64_t tenant_id,
                                              const uint64_t mock_fk_parent_table_id,
                                              const ObMockFKParentTableSchema *&schema);
  // mock_fk_parent_table end

  //label security
  int get_label_se_policy_schema_by_id(const uint64_t tenant_id,
                                       const uint64_t label_se_policy_id,
                                       const ObLabelSePolicySchema *&schema);
  int get_label_se_policy_schema_by_name(const uint64_t tenant_id,
                                         const ObString &policy_name,
                                         const ObLabelSePolicySchema *&schema);
  int get_label_se_policy_schema_by_column_name(const uint64_t tenant_id,
                                                const ObString &column_name,
                                                const ObLabelSePolicySchema *&schema);
  int get_label_se_component_schema_by_id(const uint64_t tenant_id,
                                          const uint64_t label_se_comp_id,
                                          const ObLabelSeComponentSchema *&schema);
  int get_label_se_component_schema_by_short_name(const uint64_t tenant_id,
                                                  const uint64_t label_se_policy_id,
                                                  const int64_t comp_type,
                                                  const common::ObString &short_name,
                                                  const ObLabelSeComponentSchema *&schema);
  int get_label_se_component_schema_by_long_name(const uint64_t tenant_id,
                                                 const uint64_t label_se_policy_id,
                                                 const int64_t comp_type,
                                                 const common::ObString &long_name,
                                                 const ObLabelSeComponentSchema *&schema);
  int get_label_se_component_schema_by_comp_num(const uint64_t tenant_id,
                                                const uint64_t label_se_policy_id,
                                                const int64_t comp_type,
                                                const int64_t comp_num,
                                                const ObLabelSeComponentSchema *&schema);

  int get_label_se_label_by_label_tag(const uint64_t tenant_id,
                                      const int64_t label_tag,
                                      const ObLabelSeLabelSchema *&schema);

  int get_label_se_label_schema_by_id(const uint64_t tenant_id,
                                      const uint64_t label_se_label_id,
                                      const ObLabelSeLabelSchema *&schema);
  int get_label_se_label_schema_by_name(const uint64_t tenant_id,
                                        const ObString &short_name,
                                        const ObLabelSeLabelSchema *&schema);
  int get_label_se_user_level_by_id(const uint64_t tenant_id,
                                    const uint64_t user_id,
                                    const uint64_t policy_id,
                                    const ObLabelSeUserLevelSchema *&schema);
  // tablespace interface
  int get_tablespace_schema_with_name(const uint64_t tenant_id,
                                       const common::ObString &tablespace_name,
                                       const ObTablespaceSchema *&schema);
  int get_tablespace_schema(const uint64_t tenant_id,
                            const uint64_t tablespace_id,
                            const ObTablespaceSchema *&schema);
  int get_tablespace_schemas_in_tenant(const uint64_t tenant_id,
                                      common::ObIArray<const ObTablespaceSchema *> &tablespace_schemas);
  int get_tablespace_ids_in_tenant(const uint64_t tenant_id,
                                   common::ObIArray<uint64_t> &tablespace_ids_array);
  //end user define function

  ///begin oracle profile define function
  int get_profile_schema_by_name(const uint64_t tenant_id,
                                 const common::ObString &name,
                                 const ObProfileSchema *&schema);

  int get_profile_schema_by_id(const uint64_t tenant_id,
                               const uint64_t profile_id,
                               const ObProfileSchema *&schema);

  int get_user_profile_failed_login_limits(const uint64_t tenant_id,
                                           const uint64_t user_id,
                                           int64_t &failed_login_limit_num,
                                           int64_t &failed_login_limit_time);
  int get_user_password_expire_times(const uint64_t tenant_id,
                                     const uint64_t user_id,
                                     int64_t &password_last_change,
                                     int64_t &password_life_time,
                                     int64_t &password_grace_time);
  int get_user_profile_function_name(const uint64_t tenant_id,
                                     const uint64_t profile_id,
                                     common::ObString &function_name);
  ///end oracle profile define function

  // dblink function begin
  int check_dblink_exist(const uint64_t tenant_id,
                         const common::ObString &dblink_name,
                         bool &exist) const;
  int get_dblink_id(const uint64_t tenant_id,
                    const common::ObString &dblink_name,
                    uint64_t &dblink_id) const;
  int get_dblink_user(const uint64_t tenant_id,
                      const common::ObString &dblink_name,
                      common::ObString &dblink_user,
                      common::ObIAllocator &allocator);
  int get_dblink_schema(const uint64_t tenant_id,
                        const common::ObString &dblink_name,
                        const share::schema::ObDbLinkSchema *&dblink_schema) const;
  int get_dblink_schema(const uint64_t tenant_id,
                        const uint64_t dblink_id,
                        const share::schema::ObDbLinkSchema *&dblink_schema);
  int get_link_table_schema(const uint64_t tenant_id,
                            const uint64_t dblink_id,
                            const common::ObString &database_name,
                            const common::ObString &table_name,
                            common::ObIAllocator &allocator,
                            ObTableSchema *&table_schema,
                            sql::ObSQLSessionInfo *session_info,
                            const ObString &dblink_name,
                            bool is_reverse_link,
                            uint64_t *current_scn,
                            bool &is_under_oracle12c);
  // dblink function end

  // directory function begin
  int get_directory_schema_by_name(const uint64_t tenant_id,
                                   const common::ObString &name,
                                   const ObDirectorySchema *&schema) const;
  int get_directory_schema_by_id(const uint64_t tenant_id,
                                 const uint64_t directory_id,
                                 const ObDirectorySchema *&schema);
  int get_directory_schemas_in_tenant(const uint64_t tenant_id,
                                      common::ObIArray<const ObDirectorySchema *> &directory_schemas);
  // directory function end

  // location function begin
  int get_location_schema_by_name(const uint64_t tenant_id,
                                  const common::ObString &name,
                                  const ObLocationSchema *&schema);
  int get_location_schema_by_id(const uint64_t tenant_id,
                                const uint64_t location_id,
                                const ObLocationSchema *&schema);
  int get_location_schema_by_prefix_match_with_priv(const ObSessionPrivInfo &session_priv,
                                                    const common::ObIArray<uint64_t> &enable_role_id_array,
                                                    const uint64_t tenant_id,
                                                    const common::ObString &access_path,
                                                    const ObLocationSchema *&schema,
                                                    const bool is_need_write_priv = false);
  int get_location_schemas_in_tenant(const uint64_t tenant_id,
                                     common::ObIArray<const ObLocationSchema *> &location_schemas);

  int check_location_access(const ObSessionPrivInfo &session_priv,
                            const common::ObIArray<uint64_t> &enable_role_id_array,
                            const ObString &location_name,
                            const bool is_need_write_priv = false);
  int check_location_show(const ObSessionPrivInfo &session_priv,
                          const common::ObIArray<uint64_t> &enable_role_id_array,
                          const common::ObString &location_name,
                          bool &allow_show);
  // location function end

  // ai provider/gateway function begin
  int check_ai_provider_access(const ObSessionPrivInfo &session_priv,
                               const common::ObIArray<uint64_t> &enable_role_id_array,
                               ObPrivSet required_priv);
  int check_ai_gateway_access(const ObSessionPrivInfo &session_priv,
                              const common::ObIArray<uint64_t> &enable_role_id_array,
                              ObPrivSet required_priv);
  // ai provider/gateway function end

  // rls function begin
  int get_rls_policy_schema_by_name(const uint64_t tenant_id,
                                    const uint64_t table_id,
                                    const uint64_t rls_group_id,
                                    const common::ObString &name,
                                    const ObRlsPolicySchema *&schema);
  int get_rls_policy_schema_by_id(const uint64_t tenant_id,
                                  const uint64_t rls_policy_id,
                                  const ObRlsPolicySchema *&schema);
  int get_rls_policy_schemas_in_group(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      const uint64_t rls_group_id,
                                      common::ObIArray<const ObRlsPolicySchema *> &schemas);
  int get_rls_policy_schemas_in_table(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      common::ObIArray<const ObRlsPolicySchema *> &schemas);
  int get_rls_group_schema_by_name(const uint64_t tenant_id,
                                   const uint64_t table_id,
                                   const common::ObString &name,
                                   const ObRlsGroupSchema *&schema);
  int get_rls_group_schema_by_id(const uint64_t tenant_id,
                                 const uint64_t rls_group_id,
                                 const ObRlsGroupSchema *&schema);
  int get_rls_group_schemas_in_table(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     common::ObIArray<const ObRlsGroupSchema *> &schemas);
  int get_rls_context_schema_by_name(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const common::ObString &name,
                                     const common::ObString &attribute,
                                     const ObRlsContextSchema *&schema);
  int get_rls_context_schema_by_id(const uint64_t tenant_id,
                                   const uint64_t rls_context_id,
                                   const ObRlsContextSchema *&schema);
  int get_rls_context_schemas_in_table(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       common::ObIArray<const ObRlsContextSchema *> &schemas);
  // rls function end

  // catalog function begin
  int get_catalog_schema_by_name(const uint64_t tenant_id,
                                 const common::ObString &name,
                                 const ObCatalogSchema *&schema);
  int get_catalog_schema_by_id(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               const ObCatalogSchema *&schema);
  // catalog function end

  // external resource function begin
  int get_external_resource_schema(const uint64_t &tenant_id,
                                   const uint64_t &database_id,
                                   const ObString &name,
                                   const ObSimpleExternalResourceSchema *&schema);
  int get_external_resource_schema(const uint64_t &tenant_id,
                                   const uint64_t &external_resource_id,
                                   const ObSimpleExternalResourceSchema *&schema);
  int check_external_resource_exist(uint64_t tenant_id,
                                    uint64_t database_id,
                                    ObString name,
                                    bool &is_exist);
  // external resource function end

  // sensitive rule function begin
  int get_sensitive_rule_schema_count(const uint64_t tenant_id, int64_t &count);
  int get_sensitive_rule_schema_by_name(const uint64_t tenant_id,
                                        const common::ObString &name,
                                        const ObSensitiveRuleSchema *&schema);
  int get_sensitive_rule_schema_by_id(const uint64_t tenant_id,
                                      const uint64_t sensitive_rule_id,
                                      const ObSensitiveRuleSchema *&schema);
  int get_sensitive_column_schemas_by_id(const uint64_t tenant_id,
                                         const uint64_t sensitive_rule_id,
                                         common::ObIArray<const ObSensitiveColumnSchema *> &schemas);
  int get_sensitive_rule_schema_by_column(const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          const uint64_t column_id,
                                          const ObSensitiveRuleSchema *&schema);
  int get_sensitive_rule_schemas_by_table(const ObTableSchema &table_schema,
                                          ObIArray<const ObSensitiveRuleSchema *> &schemas);
  // sensitive rule function end

  int check_user_exist(const uint64_t tenant_id,
                       const common::ObString &user_name,
                       const common::ObString &host_name,
                       bool &is_exist,
                       uint64_t *user_id = NULL);
  int check_user_exist(const uint64_t tenant_id,
                       const uint64_t user_id,
                       bool &is_exist);

  template <typename SchemaType>
  int check_flashback_object_exist(const SchemaType &object_schema,
                                   const common::ObString &object_name,
                                   bool &object_exist);

  int get_schema_count(const uint64_t tenant_id, int64_t &schema_count);
  int get_schema_size(const uint64_t tenant_id, int64_t &schema_count);
  /*
   * get schema object's schema_version.
   * OB_INVALID_VERSION will be returned if schema object doesn't exist.
   * For TENANT_SCHEMA, tenant_id should be OB_SYS_TENANT_ID.
   * For SYS_VARIABLE_SCHEMA, schema_id should be equal with tenant_id.
   */
  int get_schema_version(const ObSchemaType schema_type,
                         const uint64_t tenant_id,
                         const uint64_t schema_id,
                         int64_t &schema_version,
                         uint64_t *schema_belong_db_id = nullptr);
  int get_idx_schema_by_origin_idx_name(uint64_t tenant_id,
                                        uint64_t database_id,
                                        const common::ObString &index_name,
                                        const ObTableSchema *&table_schema);

  int get_tenant_unavailable_index(const uint64_t tenant_id, common::ObIArray<uint64_t> &table_ids);
  int check_unavailable_index_exist(const uint64_t tenant_id, bool &exist);
  int check_restore_error_index_exist(const uint64_t tenant_id, bool &exist);

  inline uint64_t get_session_id() const { return session_id_; }
  inline void set_session_id(const uint64_t id)  { session_id_ = id; }

  bool is_tenant_schema_guard() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  // OnDemand full-scan only; old mgr Array path ignores. Default RELAXED.
  // Setting the mode does not start a transaction. The first SQL-backed STRONG
  // scan/enumeration lazily starts an RR + READ ONLY transaction, reused by
  // subsequent scans/enumerations on this guard.
  // The guard holds the transaction and connection until optional
  // release_schema_scan_trans_early(), reset(), or destruction. Transaction timeout
  // does not release the connection; GC snapshot protection follows transaction
  // timeout rules.
  // Before ending the transaction or resetting/destroying the guard, drain its
  // SQL cursors to OB_ITER_END, or reset/destroy the associated iterators.
  // Early end preserves the guard and pinned schemas. Later STRONG scans start
  // a new transaction/snapshot; consistency across early end is not guaranteed.
  void set_schema_scan_consistency_mode(const ObSchemaScanConsistencyMode mode)
  { schema_scan_consistency_mode_ = mode; }
  ObSchemaScanConsistencyMode get_schema_scan_consistency_mode() const
  { return schema_scan_consistency_mode_; }
  // Full-scan gate (latest + non-lazy + formal version). Used by OnDemand / Iterator.
  int check_can_full_scan(const uint64_t tenant_id) const;
  // CDC historical full-scan: allow non-latest Guard; still reject lazy / non-formal version.
  int check_can_cdc_full_scan(const uint64_t tenant_id) const;
  // Enumerate alive table_ids at Guard snapshot (for CDC). Original: mgr;
  // OnDemand: __all_table_history at Guard version (bypasses latest-only gate).
  int get_table_ids_in_tenant_for_cdc(const uint64_t tenant_id,
                                      common::ObIArray<uint64_t> &table_ids);
  bool is_latest_guard() const { return is_latest_guard_; }

  SchemaGuardType get_schema_guard_type() const
  { return static_cast<SchemaGuardType>(schema_guard_type_); }

  bool restore_tenant_exist() { return restore_tenant_exist_; }
  bool use_schema_status() { return restore_tenant_exist(); }

  int check_formal_guard() const;
  int is_lazy_mode(const uint64_t tenant_id, bool &is_lazy) const;

  int check_tenant_is_restore(const uint64_t tenant_id, bool &is_restore);
  int get_tenant_status(const uint64_t tenant_id, ObTenantStatus &status);
  int check_if_tenant_has_been_dropped(const uint64_t tenant_id, bool &is_dropped);
  int get_dropped_tenant_ids(common::ObIArray<uint64_t> &dropped_tenant_ids) const;
  int check_is_creating_standby_tenant(const uint64_t tenant_id, bool &is_creating_standby);

  int check_keystore_exist(const uint64_t tenant_id, bool &exist);
  int get_keystore_schema(const uint64_t tenant_id, const ObKeystoreSchema *&schema);

  int get_sys_priv_with_tenant_id(const uint64_t tenant_id,
                                  common::ObIArray<const ObSysPriv *> &sys_privs);
  int get_sys_priv_with_grantee_id(const uint64_t tenant_id,
                                   const uint64_t grantee_id,
                                   ObSysPriv *&sys_priv);
  int check_global_index_exist(const uint64_t tenant_id, const uint64_t table_id, bool &exist);

  int column_is_key(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const uint64_t column_id,
      bool &is_key);

  int get_range_part_high_bound(const ObTableSchema &table_schema,
                                const common::ObTabletID &tablet_id,
                                ObIAllocator &allocator,
                                common::ObRowkey &high_bound);
  int deep_copy_index_name_map(common::ObIAllocator &allocator,
                               ObIndexNameMap &index_name_cache);
  #define GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(SCHEMA, SCHEMA_TYPE)     \
    int get_simple_##SCHEMA##_schemas_in_database(const uint64_t tenant_id,    \
                                                  const uint64_t database_id,  \
                                                  common::ObIArray<const SCHEMA_TYPE *> &schema_array);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(udt, ObSimpleUDTSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(outline, ObSimpleOutlineSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(synonym, ObSimpleSynonymSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(package, ObSimplePackageSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(routine, ObSimpleRoutineSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(mock_fk_parent_table, ObSimpleMockFKParentTableSchema);
  GET_SIMPLE_SCHEMAS_IN_DATABASE_FUNC_DECLARE(external_resource, ObSimpleExternalResourceSchema);

  int get_vector_info_index_ids_in_tenant(const uint64_t tenant_id,
                                          bool &has_ivf_index,
                                          ObIArray<uint64_t> &table_ids);

  int check_routine_priv(const ObSessionPrivInfo &session_priv,
                         const common::ObIArray<uint64_t> &enable_role_id_array,
                         const ObNeedPriv &routine_need_priv);

  int check_routine_definer_existed(uint64_t tenant_id, const ObString &user_name, bool &existed);

  int check_obj_mysql_priv(const ObSessionPrivInfo &session_priv,
                           const common::ObIArray<uint64_t> &enable_role_id_array,
                           const ObNeedPriv &obj_mysql_need_priv);
  int get_obj_mysql_priv_with_obj_name(const uint64_t tenant_id,
                                       const ObString &obj_name,
                                       const uint64_t obj_type,
                                       ObIArray<const ObObjMysqlPriv *> &obj_privs,
                                       bool reset_flag);
  // ai function
  int get_ai_model_schema(const uint64_t tenant_id,
                          const uint64_t ai_model_id,
                          const ObAiModelSchema *&ai_model_schema);

  int get_ai_model_schema(const uint64_t tenant_id,
                          const ObString &ai_model_name,
                          const ObAiModelSchema *&ai_model_schema);

  // ai provider
  int get_ai_provider_schema(const uint64_t tenant_id,
                             const uint64_t provider_id,
                             const ObAIProviderSchema *&provider_schema);

  int get_ai_provider_schema(const uint64_t tenant_id,
                             const ObString &provider_name,
                             const ObAIProviderSchema *&provider_schema);

  // ai gateway
  int get_ai_gateway_schema(const uint64_t tenant_id,
                             const uint64_t gateway_id,
                             const ObAIGatewaySchema *&gateway_schema);

  int get_ai_gateway_schema(const uint64_t tenant_id,
                             const ObString &gateway_name,
                             const ObAIGatewaySchema *&gateway_schema);

  int get_ccl_rule_with_name(const uint64_t tenant_id,
                             const common::ObString &name,
                             const ObCCLRuleSchema *&ccl_rule_schema);

  int get_ccl_rule_with_ccl_rule_id(const uint64_t tenant_id,
                                    const uint64_t ccl_rule_id,
                                    const ObCCLRuleSchema *&ccl_rule_schema);

  int get_ccl_rule_infos(const uint64_t tenant_id, CclRuleContainsInfo,
                         ObCCLRuleMgr::CCLRuleInfos *&ccl_rule_infos);
  int get_ccl_rule_count(const uint64_t tenant_id, uint64_t & count);

private:
  // Framework-only client preparation. RELAXED returns the shared SQL proxy;
  // STRONG may allocate a transaction, acquire a connection and execute BEGIN,
  // or reuse the existing RR + READ ONLY transaction. The client is borrowed
  // and must not outlive its owning transaction/proxy.
  int prepare_schema_scan_sql_client_(const uint64_t tenant_id,
                                      common::ObISQLClient *&sql_client);
  int check_ssl_access(const ObUserInfo &user_info,
                       SSL *ssl_st);
  int check_catalog_priv(const ObSessionPrivInfo &session_priv,
                         const common::ObIArray<uint64_t> &enable_role_id_array,
                         const ObNeedPriv &need_priv);
  int check_catalog_priv(const ObSessionPrivInfo &session_priv,
                         const common::ObIArray<uint64_t> &enable_role_id_array,
                         const ObNeedPriv &need_priv,
                         ObPrivSet &user_catalog_priv_set);
  int check_sensitive_rule_priv(const ObSessionPrivInfo &session_priv,
                                const common::ObIArray<uint64_t> &enable_role_id_array,
                                const ObNeedPriv &need_priv);
  int check_sensitive_rule_priv(const ObSessionPrivInfo &session_priv,
                                const common::ObIArray<uint64_t> &enable_role_id_array,
                                const ObNeedPriv &need_priv,
                                ObPrivSet &user_sensitive_rule_priv_set);
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObIArray<uint64_t> &enable_role_id_array,
                    const common::ObString &db,
                    const ObPrivSet need_priv_set,
                    ObPrivSet &user_db_priv_set);
  int check_db_priv(const ObSessionPrivInfo &session_priv,
                    const common::ObIArray<uint64_t> &enable_role_id_array,
                    const common::ObString &db,
                    const ObPrivSet need_priv_set);
  int check_user_priv(const ObSessionPrivInfo &session_priv,
                      const common::ObIArray<uint64_t> &enable_role_id_array,
                      const ObPrivSet priv_set,
                      bool check_all = true);
  int verify_db_read_only(const uint64_t tenant_id,
                          const ObNeedPriv &need_priv);
  int verify_table_read_only(const uint64_t tenant_id,
                             const ObNeedPriv &need_priv);
  int check_single_obj_priv(const uint64_t tenant_id,
                            const uint64_t uid,
                            const ObOraNeedPriv &need_priv,
                            const common::ObIArray<uint64_t> &role_id_array);
  int check_ai_object_access(const ObSessionPrivInfo &session_priv,
                             const common::ObIArray<uint64_t> &enable_role_id_array,
                             const ObObjectType obj_type,
                             ObPrivSet required_priv);

  // for privilege
  int add_role_id_recursively(const uint64_t tenant_id,
                              const uint64_t role_id,
                              ObSessionPrivInfo &s_priv,
                              common::ObIArray<uint64_t> &enable_role_id_array);

  int get_simple_trigger_schema(const uint64_t tenant_id,
                                const uint64_t database_id,
                                const common::ObString &trigger_name,
                                const ObSimpleTriggerSchema *&simple_trigger);
  int get_package_info_from_trigger(const uint64_t tenant_id,
                                    const uint64_t package_id,
                                    const ObPackageInfo *&package_info);
  //routine
  int check_routine_exist(uint64_t tenant_id, uint64_t database_id,
                          uint64_t package_id, const common::ObString &routine_name,
                          uint64_t overload, ObRoutineType routine_type, bool &exist) const;
  int get_routine_info(const uint64_t tenant_id,
                       const uint64_t database_id,
                       const uint64_t package_id,
                       const common::ObString &routine_name, uint64_t overload,
                       ObRoutineType routine_type, const ObRoutineInfo *&routine_info);
  int get_routine_id(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                     const common::ObString &routine_name, uint64_t overload,
                     ObRoutineType routine_type, uint64_t &routine_id);

  int get_outline_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObOutlineInfo *> &outline_schemas);
  int get_routine_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObRoutineInfo *> &routine_schemas);
  int get_package_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObPackageInfo *> &package_schemas);
  int get_trigger_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObTriggerInfo*> &trigger_infos);
  int get_synonym_schemas_in_tenant(const uint64_t tenant_id,
                                    common::ObIArray<const ObSynonymInfo *> &synonym_schemas);

  // TODO: add this to all member functions
  bool check_inner_stat() const;

  // For TENANT_SCHEMA, tenant_id should be OB_SYS_TENANT_ID;
  // For SYS_VARIABLE_SCHEMA, tenant_id should be equal with schema_id;
  // specified_version should be invalid for lazy mode.
  template<typename T>
  int get_schema(const ObSchemaType schema_type,
                 const uint64_t tenant_id,
                 const uint64_t schema_id,
                 const T *&schema,
                 int64_t specified_version = common::OB_INVALID_VERSION);
  // Unified Guard-local pin: schema_objs_ holds KVCacheHandle so the object
  // cannot be evicted while this Guard is alive. Used by get_schema() and
  // ON_DEMAND simple/full loaders (TABLE_SIMPLE_SCHEMA / TABLE_SCHEMA).
  template<typename T>
  int get_from_local_cache(const ObSchemaType schema_type,
                           const uint64_t tenant_id,
                           const uint64_t schema_id,
                           const T *&schema);
  template<typename T>
  int put_to_local_cache(
      const ObSchemaType schema_type,
      const uint64_t tenant_id,
      const uint64_t schema_id,
      const T *&schema,
      common::ObKVCacheHandle &handle);

  int init(const ObSimpleSchemaMode schema_mode);
  int init_table_impl(const ObSimpleSchemaMode schema_mode);
  int fast_reset() {
    return is_inited_? reset(): common::OB_SUCCESS;
  }
  int check_tenant_schema_guard(const uint64_t tenant_id) const;
  int get_schema_mgr(const uint64_t tenant_id, const ObSchemaMgr *&schema_mgr) const;
  int get_schema_mgr_info(const uint64_t tenant_id, const ObSchemaMgrInfo *&schema_mgr_info) const;
  int check_lazy_guard(const uint64_t tenant_id, const ObSchemaMgr *&mgr) const;
  int get_schema_status(const uint64_t tenant_id, ObRefreshSchemaStatus &schema_status);

  int check_ora_conn_access(const uint64_t tenant_id,
      const uint64_t user_id,
      bool print_warn,
      const ObIArray<uint64_t> &role_id_array);
  bool ignore_tenant_not_exist_error(const uint64_t tenant_id);

  int check_priv_db_or_(const ObSessionPrivInfo &session_priv,
                        const common::ObIArray<uint64_t> &enable_role_id_array,
                        const ObNeedPriv &need_priv,
                        const ObPrivMgr &priv_mgr,
                        const uint64_t tenant_id,
                        const uint64_t user_id,
                        bool& pass);
  int check_priv_table_or_(const ObSessionPrivInfo &session_priv,
                           const common::ObIArray<uint64_t> &enable_role_id_array,
                           const ObNeedPriv &need_priv,
                           const ObPrivMgr &priv_mgr,
                           const uint64_t tenant_id,
                           const uint64_t user_id,
                           bool& pass);
  int get_table_schemas_in_tenant_(const uint64_t tenant_id,
                                   const bool only_view_schema,
                                   common::ObIArray<const ObTableSchema *> &table_schemas);
  int check_single_table_priv_for_update_(const ObSessionPrivInfo &session_priv,
                                          const common::ObIArray<uint64_t> &enable_role_id_array,
                                          const ObNeedPriv &table_need_priv,
                                          const ObPrivMgr &priv_mgr);
  int check_activate_all_role_var(uint64_t tenant_id, bool &activate_all_role);
private:
  static const int MAX_ID_SCHEMAS = 32;
  const static int64_t FULL_SCHEMA_MEM_THREHOLD = 100 * 1024 * 1024L;//100M
  const ObISchemaGetterGuardTableImpl *table_impl_;
  // true only when get_tenant_schema_guard() is called without specified versions
  bool is_latest_guard_;
  // Meaningful for OnDemand full-scan; unused by old Array get path.
  ObSchemaScanConsistencyMode schema_scan_consistency_mode_;
  // STRONG OnDemand enumeration and lite scans share this transaction's snapshot.
  // Released automatically on reset/destruction, or optionally via release_schema_scan_trans_early().
  common::ObMySQLTransaction *schema_scan_trans_;
  uint64_t schema_scan_trans_tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSchemaGetterGuard);
};

class ObISchemaGetterGuardTableImpl
{
public:
  virtual ~ObISchemaGetterGuardTableImpl() {}

  virtual int get_table_id(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &table_name, bool is_index,
      ObSchemaGetterGuard::CheckTableType check_type, uint64_t &table_id,
      bool is_built_in_index) const = 0;
  virtual int get_foreign_key_id(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &name, uint64_t &id) const = 0;
  virtual int get_foreign_key_info(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &name,
      ObSimpleForeignKeyInfo &info) const = 0;
  virtual int get_constraint_id(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &name, uint64_t &id) const = 0;
  virtual int get_constraint_info(const ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &name,
      ObSimpleConstraintInfo &info) const = 0;
  virtual int get_table_schema(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t table_id, const ObTableSchema *&schema) const = 0;
  virtual int get_simple_table_schema(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &table_name, bool is_index,
      const ObSimpleTableSchemaV2 *&schema, bool with_hidden_flag,
      bool is_built_in_index) const = 0;
  virtual int get_table_schema(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &table_name, bool is_index,
      const ObTableSchema *&schema, bool with_hidden_flag,
      bool is_built_in_index) const = 0;
  virtual int get_index_meta_infos_with_data_table_id(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t data_table_id,
      common::ObIArray<ObAuxTableMetaInfo> &index_meta_infos) const = 0;
  virtual int get_index_schemas_with_data_table_id(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t data_table_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &index_schemas) const = 0;
  virtual int get_lob_schemas_with_data_table_id(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t data_table_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &lob_schemas) const = 0;
  virtual int get_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &ids) const = 0;
  virtual int get_table_ids_in_tenant_for_cdc(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &ids) const = 0;
  virtual int get_table_ids_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, common::ObIArray<uint64_t> &ids) const = 0;
  virtual int get_table_ids_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, common::ObIArray<uint64_t> &ids) const = 0;
  virtual int get_table_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      bool only_view, common::ObIArray<const ObTableSchema *> &schemas) const = 0;
  virtual int get_simple_table_schemas_in_tenant(ObSchemaGetterGuard &guard,
      uint64_t tenant_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const = 0;
  virtual int get_table_schemas_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, common::ObIArray<const ObTableSchema *> &schemas) const = 0;
  virtual int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, common::ObIArray<const ObTableSchema *> &schemas) const = 0;
  virtual int get_table_schemas_in_tablespace(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablespace_id, common::ObIArray<const ObTableSchema *> &schemas) const = 0;
  virtual int get_simple_table_schemas_in_database(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t database_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const = 0;
  virtual int get_simple_table_schemas_in_tablegroup(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t tablegroup_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &schemas) const = 0;
  virtual int get_primary_table_schema_in_tablegroup(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t tablegroup_id,
      const ObSimpleTableSchemaV2 *&schema) const = 0;
  // Iterator overloads (old path): fill Array from mgr, then iter.init(BY_SCHEMA).
  virtual int get_table_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      ObSimpleTableSchemaIterator &iter) const = 0;
  virtual int get_table_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      ObTableSchemaIterator &iter) const = 0;
  virtual int get_view_schemas_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      ObTableSchemaIterator &iter) const = 0;
  virtual int get_table_schemas_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, ObSimpleTableSchemaIterator &iter) const = 0;
  virtual int get_table_schemas_in_database(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, ObTableSchemaIterator &iter) const = 0;
  virtual int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, ObSimpleTableSchemaIterator &iter) const = 0;
  virtual int get_table_schemas_in_tablegroup(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablegroup_id, ObTableSchemaIterator &iter) const = 0;
  virtual int get_table_schemas_in_tablespace(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t tablespace_id, ObTableSchemaIterator &iter) const = 0;
  // Sparse-filter ids
  virtual int get_tmp_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const = 0;
  virtual int get_external_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const = 0;
  virtual int get_duplicate_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const = 0;
  virtual int get_encrypted_table_ids_in_tenant(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::ObIArray<uint64_t> &table_ids) const = 0;
  virtual int get_dynamic_partition_table_ids_in_tenant(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, common::ObIArray<uint64_t> &table_ids) const = 0;
  virtual int check_table_exist(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t database_id, const common::ObString &table_name, bool is_index,
      ObSchemaGetterGuard::CheckTableType check_type, bool &is_exist,
      uint64_t *table_id) const = 0;
  virtual int check_table_exist(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t table_id, bool &is_exist) const = 0;
  virtual int get_table_schema_version(ObSchemaGetterGuard &guard,
      ObSchemaType schema_type, uint64_t tenant_id, uint64_t table_id,
      int64_t &schema_version, uint64_t *database_id) const = 0;
  virtual int get_simple_table_schema(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      uint64_t table_id, const ObSimpleTableSchemaV2 *&schema) const = 0;
  virtual int get_schema_count(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      int64_t &schema_count) const = 0;
  virtual int get_schema_size(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      int64_t &schema_size) const = 0;
  virtual int get_tenant_unavailable_index(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, common::ObIArray<uint64_t> &index_ids) const = 0;
  virtual int check_unavailable_index_exist(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, bool &exist) const = 0;
  virtual int check_restore_error_index_exist(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, bool &exist) const = 0;
  virtual int get_idx_schema_by_origin_idx_name(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, uint64_t database_id, const common::ObString &index_name,
      const ObTableSchema *&schema) const = 0;
  virtual int generate_tablet_table_map(ObSchemaGetterGuard &guard, uint64_t tenant_id,
      common::hash::ObHashMap<common::ObTabletID, uint64_t> &tablet_map) const = 0;
  virtual int deep_copy_index_name_map(ObSchemaGetterGuard &guard,
      common::ObIAllocator &allocator, ObIndexNameMap &index_name_cache) const = 0;
  virtual int get_vector_info_index_ids_in_tenant(ObSchemaGetterGuard &guard,
      uint64_t tenant_id, bool &has_ivf_index,
      common::ObIArray<uint64_t> &table_ids) const = 0;
  virtual int get_table_schemas_with_async_index_in_tenant(ObSchemaGetterGuard &guard,
      uint64_t tenant_id,
      common::ObIArray<const ObSimpleTableSchemaV2 *> &table_schemas) const = 0;
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase
#endif //OB_OCEANBASE_SCHEMA_OB_SCHEMA_GETTER_GUARD_H_
