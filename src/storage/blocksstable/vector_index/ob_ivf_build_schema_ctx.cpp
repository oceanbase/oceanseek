/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_build_schema_ctx.h"
#include "share/ob_index_builder_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;

ObIVFBuildSchemaCtx::ObIVFBuildSchemaCtx(
    common::ObIAllocator &allocator)
    : is_inited_(false),
      allocator_(allocator),
      vector_param_(),
      index_params_(),
      vector_dim_(0),
      index_column_count_(0),
      index_rowkey_column_count_(0)
{
}

int ObIVFBuildSchemaCtx::init(const ObIVFBuildParam &param)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService *schema_service = nullptr;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  const ObColumnSchemaV2 *source_column = nullptr;
  ObVectorIndexParam vector_param;
  ObString index_params;
  int64_t vector_dim = 0;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF build schema context init twice", K(ret), K(param));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF build schema parameter", K(ret), K(param));
  } else if (OB_ISNULL(schema_service = &ObMultiVersionSchemaService::get_instance())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null for IVF build", K(ret), K(param));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(
                 param.tenant_id_, schema_guard, param.schema_version_))) {
    LOG_WARN("failed to get IVF schema guard", K(ret), K(param));
  } else if (OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("IVF schema guard is not formal", K(ret), K(param));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 param.tenant_id_, param.data_table_id_, data_schema))) {
    LOG_WARN("failed to get IVF data schema", K(ret), K(param));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("IVF data table schema does not exist", K(ret), K(param));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 param.tenant_id_, param.index_table_id_, index_schema))) {
    LOG_WARN("failed to get IVF index schema", K(ret), K(param));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("IVF index table schema does not exist", K(ret), K(param));
  } else if (OB_UNLIKELY(!is_async_ivf_index(index_schema->get_index_type())
             || index_schema->get_data_table_id() != param.data_table_id_)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("unexpected IVF schema relationship", K(ret), K(param),
             KPC(index_schema));
  } else if (OB_FAIL(ObIndexBuilderUtil::get_async_ivf_source_column(
                 *data_schema, *index_schema, source_column))) {
    LOG_WARN("failed to resolve IVF source column", K(ret), K(param));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(
                 source_column->get_extended_type_info(), vector_dim))) {
    LOG_WARN("failed to resolve IVF vector dimension", K(ret),
             KPC(source_column));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                 index_schema->get_index_params(),
                 ObVectorIndexType::VIT_IVF_INDEX, vector_param, true))) {
    LOG_WARN("failed to parse IVF parameters", K(ret),
             "index_params", index_schema->get_index_params());
  } else if (OB_UNLIKELY(vector_dim <= 0 || vector_param.nlist_ <= 0)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid IVF dimension or nlist", K(ret), K(vector_dim),
             K(vector_param));
  } else if (OB_FAIL(validate_target_columns_(data_schema, index_schema))) {
    LOG_WARN("invalid IVF target row contract", K(ret), K(param));
  } else if (OB_FAIL(ob_write_string(
                 allocator_, index_schema->get_index_params(), index_params))) {
    LOG_WARN("failed to copy IVF index parameters", K(ret), K(param));
  } else {
    vector_param.dim_ = vector_dim;
    vector_param_ = vector_param;
    index_params_ = index_params;
    vector_dim_ = vector_dim;
    index_column_count_ = index_schema->get_column_count();
    index_rowkey_column_count_ = index_schema->get_rowkey_column_num();
    is_inited_ = true;
  }
  return ret;
}

int ObIVFBuildSchemaCtx::validate_target_columns_(
    const ObTableSchema *data_schema,
    const ObTableSchema *index_schema)
{
  int ret = OB_SUCCESS;
  static const int64_t IVF_ROWKEY_COLUMN_COUNT = 3;
  static const int64_t IVF_STORED_COLUMN_COUNT = 4;
  const ObRowkeyInfo &rowkey_info = index_schema->get_rowkey_info();
  const ObColumnSchemaV2 *rowkey_columns[IVF_ROWKEY_COLUMN_COUNT] = {
    nullptr, nullptr, nullptr
  };
  const ObColumnSchemaV2 *type_column = nullptr;
  const ObColumnSchemaV2 *cid_column = nullptr;
  const ObColumnSchemaV2 *data_column = nullptr;
  int64_t stored_column_count = 0;

  if (OB_UNLIKELY(IVF_ROWKEY_COLUMN_COUNT != rowkey_info.get_size()
                  || IVF_ROWKEY_COLUMN_COUNT
                       != index_schema->get_rowkey_column_num())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid IVF rowkey column count", K(ret),
             "rowkey_info_count", rowkey_info.get_size(),
             "schema_rowkey_count", index_schema->get_rowkey_column_num(),
             KPC(index_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < IVF_ROWKEY_COLUMN_COUNT; ++i) {
    uint64_t column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("failed to get IVF rowkey column id", K(ret), K(i));
    } else if (OB_ISNULL(rowkey_columns[i] =
                 index_schema->get_column_schema(column_id))) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("IVF rowkey column does not exist", K(ret), K(i),
               K(column_id), KPC(index_schema));
    }
  }
  // The role bits of physical async-IVF index columns are not persisted.
  // Resolve roles from data-table generated columns, then match the physical
  // columns by the stable column id/name pair.
  for (ObTableSchema::const_column_iterator it = data_schema->column_begin();
       OB_SUCC(ret) && it != data_schema->column_end(); ++it) {
    const ObColumnSchemaV2 *generated_column = *it;
    const ObColumnSchemaV2 *physical_column = nullptr;
    if (OB_ISNULL(generated_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null column in IVF data schema", K(ret), KPC(data_schema));
    } else if (!generated_column->is_virtual_generated_column()) {
    } else {
      const bool is_type = generated_column->is_vec_ivf_type_column();
      const bool is_cid = generated_column->is_vec_ivf_cid_column();
      const bool is_data = generated_column->is_vec_ivf_data_column();
      const int64_t role_count = static_cast<int64_t>(is_type)
                               + static_cast<int64_t>(is_cid)
                               + static_cast<int64_t>(is_data);
      if (0 == role_count) {
      } else if (OB_ISNULL(physical_column = index_schema->get_column_schema(
                     generated_column->get_column_id()))) {
        // The data table can own generated columns for other async IVF indexes.
      } else if (OB_UNLIKELY(1 != role_count
                             || physical_column->is_virtual_generated_column()
                             || physical_column->get_column_name_str()
                                  != generated_column->get_column_name_str())) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("invalid IVF generated-to-physical column mapping", K(ret),
                 K(role_count), KPC(generated_column), KPC(physical_column),
                 KPC(index_schema));
      } else if (is_type && OB_NOT_NULL(type_column)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("multiple IVF TYPE columns match the index", K(ret),
                 KPC(generated_column), KPC(type_column), KPC(index_schema));
      } else if (is_cid && OB_NOT_NULL(cid_column)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("multiple IVF CID columns match the index", K(ret),
                 KPC(generated_column), KPC(cid_column), KPC(index_schema));
      } else if (is_data && OB_NOT_NULL(data_column)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("multiple IVF DATA columns match the index", K(ret),
                 KPC(generated_column), KPC(data_column), KPC(index_schema));
      } else if (is_type) {
        type_column = physical_column;
      } else if (is_cid) {
        cid_column = physical_column;
      } else {
        data_column = physical_column;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_ISNULL(type_column) || OB_ISNULL(cid_column)
                         || OB_ISNULL(data_column))) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF role columns do not match the index", K(ret),
             KPC(type_column), KPC(cid_column), KPC(data_column),
             KPC(data_schema), KPC(index_schema));
  } else if (OB_UNLIKELY(type_column->get_column_id()
                              != rowkey_columns[0]->get_column_id()
                         || ObIntType != type_column->get_data_type()
                         || cid_column->get_column_id()
                              != rowkey_columns[1]->get_column_id()
                         || ObIntType != cid_column->get_data_type()
                         || OB_HIDDEN_PK_INCREMENT_COLUMN_ID
                              != rowkey_columns[2]->get_column_id()
                         || ObUInt64Type != rowkey_columns[2]->get_data_type())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF rowkey role contract mismatch", K(ret),
             KPC(type_column), KPC(cid_column), KPC(rowkey_columns[0]),
             KPC(rowkey_columns[1]), KPC(rowkey_columns[2]), KPC(index_schema));
  } else if (OB_FAIL(index_schema->get_store_column_count(
                 stored_column_count, true /*full_col*/))) {
    LOG_WARN("failed to get IVF stored column count", K(ret),
             KPC(index_schema));
  } else if (OB_UNLIKELY(IVF_STORED_COLUMN_COUNT
                         != stored_column_count)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("invalid IVF stored column count", K(ret),
             K(stored_column_count), KPC(index_schema));
  } else if (OB_UNLIKELY(ObVarcharType != data_column->get_data_type()
                         || data_column->get_rowkey_position() > 0)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF role column contract mismatch", K(ret),
             KPC(type_column), KPC(cid_column), KPC(data_column),
             KPC(index_schema));
  } else if (OB_UNLIKELY(data_column->get_column_id()
                         == rowkey_columns[0]->get_column_id()
                         || data_column->get_column_id()
                              == rowkey_columns[1]->get_column_id()
                         || data_column->get_column_id()
                              == rowkey_columns[2]->get_column_id())) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("IVF DATA role must not be a rowkey column", K(ret),
             KPC(data_column), KPC(index_schema));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
