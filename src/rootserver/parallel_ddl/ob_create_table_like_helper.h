/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_LIKE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_LIKE_HELPER_H_

#include "rootserver/parallel_ddl/ob_table_helper.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObMockFKParentTableSchema;
}
}
namespace rootserver
{
class MockFKParentTableNameWrapper;
class ObCreateTableLikeHelper : public ObTableHelper
{
public:
  ObCreateTableLikeHelper(share::schema::ObMultiVersionSchemaService *schema_service,
                          const uint64_t tenant_id,
                          const obrpc::ObCreateTableLikeArg &arg,
                          obrpc::ObCreateTableRes &res,
                          bool enable_ddl_parallel,
                          ObDDLSQLTransaction *external_trans);
  virtual ~ObCreateTableLikeHelper();

  virtual int init_() override;
  virtual int lock_objects_() override;
  virtual int operate_schemas_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;
  int check_schema_valid_(const ObTableSchema *&orig_table_schema, uint64_t &new_database_id);
  int check_and_set_parent_table_id_();
  virtual int generate_foreign_keys_() override;
  virtual int generate_table_schema_() override;
  virtual bool auto_grant_creator_privileges_() const override
  { return arg_.auto_grant_creator_privileges_; }
  virtual int generate_aux_table_schemas_() override;
  virtual int generate_sequence_object_() override { return OB_SUCCESS; };

private:
  int append_search_data_index_infos_(const ObIArray<uint64_t> &search_def_index_ids,
                                      ObIArray<share::schema::ObAuxTableMetaInfo> &simple_index_infos);
  int collect_available_index_info_(const ObIArray<share::schema::ObAuxTableMetaInfo> &simple_index_infos,
                                    ObIArray<uint64_t> &search_def_index_ids,
                                    bool &has_async_logical_index);
  int reconcile_async_index_attributes_();

private:
  const obrpc::ObCreateTableLikeArg &arg_;
  obrpc::ObCreateTableRes &res_;
  uint64_t orig_table_id_;
  uint64_t replace_mock_fk_parent_table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableLikeHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_LIKE_HELPER_H_
