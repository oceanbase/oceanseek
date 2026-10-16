/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE
#include "share/catalog/ob_catalog_type_mapping.h"
#include "share/catalog/ob_catalog_partition_values.h"
#include "share/catalog/hive/ob_hms_placeholder_constants.h"
#include "share/catalog/hive/ob_hms_partition_spec_builder.h"
#include "share/schema/ob_column_schema.h"

#include <exception>

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace share
{

// A small stack buffer covers common types; enum/set value lists may exceed it, in which case
// we retry with a larger heap buffer.
int ObCatalogTypeMapping::build_ob_column_type_str(const ObColumnSchemaV2 &col, std::string &out)
{
  int ret = OB_SUCCESS;
  const ObObjMeta &obj_meta = col.get_meta_type();
  const ObAccuracy &accuracy = col.get_accuracy();
  const ObIArray<ObString> &type_info = col.get_extended_type_info();
  const uint64_t sub_type = col.is_xmltype() ? col.get_sub_data_type()
                                             : static_cast<uint64_t>(col.get_geo_type());
  char stack_buf[OB_MAX_SYS_PARAM_NAME_LENGTH] = {0};
  int64_t pos = 0;
  ret = ob_sql_type_str(obj_meta, accuracy, type_info, LS_DEFAULT, stack_buf,
                        sizeof(stack_buf), pos, sub_type, col.is_string_lob());
  if (OB_SUCC(ret)) {
    out.assign(stack_buf, pos);
  } else if (OB_SIZE_OVERFLOW == ret) {
    ObArenaAllocator allocator;
    const int64_t heap_len = OB_MAX_EXTENDED_TYPE_INFO_LENGTH;
    char *heap_buf = static_cast<char *>(allocator.alloc(heap_len));
    pos = 0;
    if (OB_ISNULL(heap_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc heap buffer for ob type str", K(ret), K(heap_len));
    } else if (OB_FAIL(ob_sql_type_str(obj_meta, accuracy, type_info, LS_DEFAULT, heap_buf,
                                       heap_len, pos, sub_type, col.is_string_lob()))) {
      LOG_WARN("failed to build ob sql type str", K(ret));
    } else {
      out.assign(heap_buf, pos);
    }
  }
  return ret;
}

int ObCatalogTypeMapping::build_hms_default_value(const ObColumnSchemaV2 &col,
                                                  std::string &default_value)
{
  int ret = OB_SUCCESS;
  const ObObj &cur_default = col.get_cur_default_value();
  ObArenaAllocator allocator("HMSDefault");
  int64_t buf_len = OB_MAX_SYS_PARAM_NAME_LENGTH;
  char *buf = nullptr;
  int64_t pos = 0;
  if (IS_DEFAULT_NOW_OBJ(cur_default)) {
    default_value = "CURRENT_TIMESTAMP";
    if (col.get_data_scale() > 0) {
      default_value += "(" + std::to_string(col.get_data_scale()) + ")";
    }
  } else if (col.is_default_expr_v2_column()) {
    const ObString &expr = cur_default.get_string();
    default_value.assign(expr.ptr(), expr.length());
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hms default value buffer", K(ret), K(buf_len));
  } else {
    ObObj default_obj = cur_default;
    default_obj.set_scale(col.get_data_scale());
    if (ob_is_enum_or_set_type(default_obj.get_type())) {
      ret = default_obj.print_varchar_literal(
          col.get_extended_type_info(), buf, buf_len, pos);
      if (OB_SIZE_OVERFLOW == ret) {
        buf_len = OB_MAX_EXTENDED_TYPE_INFO_LENGTH;
        pos = 0;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to extend hms default value buffer", K(ret), K(buf_len));
        } else {
          ret = default_obj.print_varchar_literal(
              col.get_extended_type_info(), buf, buf_len, pos);
        }
      }
    } else {
      ret = default_obj.print_varchar_literal(buf, buf_len, pos, allocator);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to print hms default value", K(ret), K(col.get_column_name_str()));
    } else {
      default_value.assign(buf, pos);
    }
  }
  return ret;
}

int ObCatalogTypeMapping::add_hms_partition_key(
    const char *name,
    Apache::Hadoop::Hive::Table &hms_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name) || '\0' == name[0]) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid synthetic hms partition key name", K(ret));
  } else {
    Apache::Hadoop::Hive::FieldSchema field;
    field.name = name;
    field.type = ObHmsPlaceholderConstants::PARTITION_KEY_TYPE;
    field.comment = ObHmsPlaceholderConstants::PARTITION_KEY_COMMENT;
    hms_table.partitionKeys.push_back(field);
  }
  return ret;
}

int ObCatalogTypeMapping::build_hms_partition_info(const ObTableSchema &table_schema,
                                                   Apache::Hadoop::Hive::Table &hms_table)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
  } else {
    ObArenaAllocator allocator("HMSPartSpec");
    ObHmsPartitionSpec spec;
    ObString spec_json;
    hms_table.partitionKeys.clear();
    if (OB_FAIL(add_hms_partition_key(ObHmsPlaceholderConstants::PARTITION_KEY_NAME, hms_table))) {
      LOG_WARN("failed to add synthetic hms partition key", K(ret));
    } else if (PARTITION_LEVEL_TWO == table_schema.get_part_level()
               && OB_FAIL(add_hms_partition_key(ObHmsPlaceholderConstants::SUBPARTITION_KEY_NAME, hms_table))) {
      LOG_WARN("failed to add synthetic hms subpartition key", K(ret));
    } else if (OB_FAIL(ObHmsPartitionSpecBuilder::build(allocator, table_schema, spec))) {
      LOG_WARN("failed to build partition spec", K(ret));
    } else if (OB_FAIL(spec.to_json(allocator, spec_json))) {
      LOG_WARN("failed to serialize partition spec", K(ret));
    } else {
      hms_table.parameters[ObHmsPlaceholderConstants::DEFAULT_PARTITION_SPEC_PARAMETER]
          = std::string(spec_json.ptr(), spec_json.length());
    }
  }
  return ret;
}

void ObCatalogTypeMapping::append_hms_path_value(const ObString &value, std::string &path)
{
  for (int64_t i = 0; i < value.length(); ++i) {
    const unsigned char c = static_cast<unsigned char>(value.ptr()[i]);
    const bool is_safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                         || '_' == c || '-' == c || '.' == c;
    if (is_safe) {
      path.push_back(static_cast<char>(c));
    } else {
      path.push_back(ObHmsPlaceholderConstants::PATH_ESCAPE_PREFIX);
      path.push_back(ObHmsPlaceholderConstants::PATH_HEX_DIGITS[(c >> 4) & 0x0F]);
      path.push_back(ObHmsPlaceholderConstants::PATH_HEX_DIGITS[c & 0x0F]);
    }
  }
}

int ObCatalogTypeMapping::build_hms_partition_location(const Apache::Hadoop::Hive::Table &hms_table,
                                                       const ObString &partition_name,
                                                       const ObString *subpartition_name,
                                                       std::string &location)
{
  int ret = OB_SUCCESS;
  location = hms_table.sd.location;
  if (location.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hms table location is empty", K(ret));
  } else {
    if (ObHmsPlaceholderConstants::PARTITION_PATH_SEPARATOR != location[location.length() - 1]) {
      location.push_back(ObHmsPlaceholderConstants::PARTITION_PATH_SEPARATOR);
    }
    location.append(ObHmsPlaceholderConstants::PARTITION_KEY_NAME);
    location.push_back(ObHmsPlaceholderConstants::PARTITION_KEY_VALUE_SEPARATOR);
    append_hms_path_value(partition_name, location);
    if (OB_NOT_NULL(subpartition_name)) {
      location.push_back(ObHmsPlaceholderConstants::PARTITION_PATH_SEPARATOR);
      location.append(ObHmsPlaceholderConstants::SUBPARTITION_KEY_NAME);
      location.push_back(ObHmsPlaceholderConstants::PARTITION_KEY_VALUE_SEPARATOR);
      append_hms_path_value(*subpartition_name, location);
    }
  }
  return ret;
}

int ObCatalogTypeMapping::build_hms_partition(
    const ObTableSchema &ob_table,
    const Apache::Hadoop::Hive::Table &hms_table,
    const ObPartition &ob_partition,
    const ObSubPartition *ob_subpartition,
    Apache::Hadoop::Hive::Partition &hms_partition)
{
  int ret = OB_SUCCESS;
  const ObString &partition_name = ob_partition.get_part_name();
  const ObString *subpartition_name
      = OB_ISNULL(ob_subpartition) ? nullptr : &ob_subpartition->get_part_name();
  std::string location;
  if (partition_name.empty() || (OB_NOT_NULL(subpartition_name) && subpartition_name->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical partition name is empty", K(ret), K(partition_name));
  } else if (OB_FAIL(build_hms_partition_location(hms_table,
                                                  partition_name,
                                                  subpartition_name,
                                                  location))) {
    LOG_WARN("failed to build hms partition location", K(ret), K(partition_name));
  } else {
    hms_partition.dbName = hms_table.dbName;
    hms_partition.tableName = hms_table.tableName;
    hms_partition.values.push_back(std::string(partition_name.ptr(), partition_name.length()));
    if (OB_NOT_NULL(subpartition_name)) {
      hms_partition.values.push_back(
          std::string(subpartition_name->ptr(), subpartition_name->length()));
    }
    hms_partition.sd = hms_table.sd;
    hms_partition.sd.location = location;
    hms_partition.parameters[ObHmsPlaceholderConstants::OB_TYPE_PARAMETER]
        = ObHmsPlaceholderConstants::PLACEHOLDER_PARTITION_MARKER;
    hms_partition.parameters[ObHmsPlaceholderConstants::OB_TENANT_ID_PARAMETER]
        = std::to_string(ob_table.get_tenant_id());
    hms_partition.parameters[ObHmsPlaceholderConstants::OB_TABLE_ID_PARAMETER]
        = std::to_string(ob_table.get_table_id());
    hms_partition.parameters[ObHmsPlaceholderConstants::OB_PARTITION_ID_PARAMETER]
        = std::to_string(ob_partition.get_part_id());
    hms_partition.parameters[ObHmsPlaceholderConstants::OB_SCHEMA_VERSION_PARAMETER]
        = std::to_string(ob_table.get_schema_version());
    if (OB_NOT_NULL(ob_subpartition)) {
      hms_partition.parameters[ObHmsPlaceholderConstants::OB_SUBPARTITION_ID_PARAMETER]
          = std::to_string(ob_subpartition->get_sub_part_id());
    }
  }
  return ret;
}

int ObCatalogTypeMapping::append_partition_values(
    const ObPartition &partition,
    const ObSubPartition *subpartition,
    ObIArray<CatalogPartitionValues> &partition_values)
{
  int ret = OB_SUCCESS;
  const ObString &subpartition_name
      = OB_ISNULL(subpartition) ? ObString() : subpartition->get_part_name();
  const CatalogPartitionValues values(partition.get_part_name(), subpartition_name);
  if (OB_FAIL(partition_values.push_back(values))) {
    LOG_WARN("failed to append catalog partition values", K(ret), K(values));
  }
  return ret;
}

int ObCatalogTypeMapping::expand_partition_values(
    const ObPartitionLevel part_level,
    const ObPartition &partition,
    ObIArray<CatalogPartitionValues> &partition_values)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ONE == part_level) {
    if (OB_FAIL(append_partition_values(partition, nullptr, partition_values))) {
      LOG_WARN("failed to build partition values", K(ret), K(partition.get_part_name()));
    }
  } else {
    ObSubPartition **subpartitions = partition.get_subpart_array();
    const int64_t subpartition_num = partition.get_subpartition_num();
    if (subpartition_num <= 0 || OB_ISNULL(subpartitions)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("two-level partition has no physical subpartitions",
               K(ret),
               K(partition.get_part_name()),
               K(subpartition_num));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < subpartition_num; ++j) {
      const ObSubPartition *subpartition = subpartitions[j];
      if (OB_ISNULL(subpartition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("physical subpartition is null", K(ret), K(partition.get_part_name()), K(j));
      } else if (subpartition->is_normal_partition()
                 && OB_FAIL(append_partition_values(partition, subpartition, partition_values))) {
        LOG_WARN("failed to build subpartition values", K(ret), K(j));
      }
    }
  }
  return ret;
}

int ObCatalogTypeMapping::build_all_partition_values(
    const ObTableSchema &table_schema,
    ObIArray<CatalogPartitionValues> &partition_values)
{
  int ret = OB_SUCCESS;
  const ObPartitionLevel part_level = table_schema.get_part_level();
  partition_values.reset();
  if (PARTITION_LEVEL_ZERO == part_level) {
    // Non-partitioned table: nothing to sync.
  } else {
    ObPartition **partitions = table_schema.get_part_array();
    const int64_t partition_num = table_schema.get_partition_num();
    if (partition_num <= 0 || OB_ISNULL(partitions)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partitioned table has no physical partitions",
               K(ret),
               K(part_level),
               K(partition_num));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; ++i) {
      const ObPartition *partition = partitions[i];
      if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret), K(i));
      } else if (!partition->is_normal_partition()) {
        // skip non-normal (e.g. splitting/dropping) partitions
      } else if (OB_FAIL(expand_partition_values(part_level, *partition, partition_values))) {
        LOG_WARN("failed to expand partition values", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObCatalogTypeMapping::build_hms_partition_from_values(
    const ObTableSchema &ob_table,
    const CatalogPartitionValues &partition_values,
    const Apache::Hadoop::Hive::Table &hms_table,
    Apache::Hadoop::Hive::Partition &hms_partition)
{
  int ret = OB_SUCCESS;
  try {
    const ObPartitionLevel part_level = ob_table.get_part_level();
    const int64_t expected_key_count
        = PARTITION_LEVEL_TWO == part_level
              ? ObHmsPlaceholderConstants::TWO_LEVEL_PARTITION_KEY_COUNT
              : ObHmsPlaceholderConstants::ONE_LEVEL_PARTITION_KEY_COUNT;
    const bool value_level_matches = PARTITION_LEVEL_ONE == part_level
                                         ? !partition_values.is_subpartition()
                                         : partition_values.is_subpartition();
    const ObPartition *partition = nullptr;
    if (PARTITION_LEVEL_ZERO == part_level || partition_values.partition_name_.empty()
        || !value_level_matches) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid partition values", K(ret), K(part_level), K(partition_values));
    } else if (expected_key_count != static_cast<int64_t>(hms_table.partitionKeys.size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected hms synthetic partition key count",
               K(ret),
               K(expected_key_count),
               K(hms_table.partitionKeys.size()));
    } else if (OB_FAIL(
                   ob_table.get_partition_by_name(partition_values.partition_name_, partition))) {
      LOG_WARN("failed to find partition for hms values", K(ret), K(partition_values));
    } else if (OB_ISNULL(partition) || !partition->is_normal_partition()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition is not a visible physical partition", K(ret), KP(partition));
    } else if (PARTITION_LEVEL_ONE == part_level) {
      if (OB_FAIL(build_hms_partition(ob_table, hms_table, *partition, nullptr, hms_partition))) {
        LOG_WARN("failed to build hms partition", K(ret), K(partition_values));
      }
    } else {
      const ObPartition *subpartition_parent = nullptr;
      const ObSubPartition *subpartition = nullptr;
      if (OB_FAIL(ob_table.get_subpartition_by_name(partition_values.subpartition_name_,
                                                    subpartition_parent,
                                                    subpartition))) {
        LOG_WARN("failed to find subpartition for hms values", K(ret), K(partition_values));
      } else if (OB_ISNULL(subpartition_parent) || OB_ISNULL(subpartition)
                 || !subpartition->is_normal_partition()
                 || subpartition_parent->get_part_id() != partition->get_part_id()) {
        ret = OB_UNKNOWN_SUBPARTITION;
        LOG_WARN("subpartition does not belong to the expected partition",
                 K(ret),
                 K(partition_values),
                 KP(subpartition_parent),
                 KP(subpartition));
      } else if (OB_FAIL(build_hms_partition(ob_table,
                                             hms_table,
                                             *partition,
                                             subpartition,
                                             hms_partition))) {
        LOG_WARN("failed to build hms subpartition", K(ret), K(partition_values));
      }
    }
  } catch (const std::exception &e) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("std exception when building hms partition", K(ret), K(e.what()));
  } catch (...) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("unknown exception when building hms partition", K(ret));
  }
  return ret;
}

int ObCatalogTypeMapping::build_hms_placeholder_table(
    const ObTableSchema &ob_table,
    const ObString &db_name,
    const uint64_t tenant_id,
    Apache::Hadoop::Hive::Table &hms_table,
    Apache::Hadoop::Hive::SQLAllTableConstraints &hms_constraints)
{
  int ret = OB_SUCCESS;
  // Thrift string/collection operations may throw on allocation failure.
  try {
    hms_table.tableName = std::string(ob_table.get_table_name(), ob_table.get_table_name_str().length());
    hms_table.dbName = std::string(db_name.ptr(), db_name.length());
    hms_table.tableType = ObHmsPlaceholderConstants::EXTERNAL_TABLE_TYPE;
    hms_table.parameters[ObHmsPlaceholderConstants::OB_TYPE_PARAMETER]
        = ObHmsPlaceholderConstants::PLACEHOLDER_TABLE_MARKER;
    hms_table.parameters[ObHmsPlaceholderConstants::OB_TENANT_ID_PARAMETER] = std::to_string(tenant_id);
    hms_table.parameters[ObHmsPlaceholderConstants::OB_TABLE_ID_PARAMETER]
        = std::to_string(ob_table.get_table_id());
    hms_table.parameters[ObHmsPlaceholderConstants::OB_SCHEMA_VERSION_PARAMETER]
        = std::to_string(ob_table.get_schema_version());
    const std::string table_name(
        ob_table.get_table_name(), ob_table.get_table_name_str().length());
    const std::string database_name(db_name.ptr(), db_name.length());
    // sync OB table comment to the HMS placeholder, only when non-empty
    const ObString &tbl_comment = ob_table.get_comment_str();
    if (!tbl_comment.empty()) {
      hms_table.parameters[ObHmsPlaceholderConstants::COMMENT_PARAMETER]
          = std::string(tbl_comment.ptr(), tbl_comment.length());
    }

    Apache::Hadoop::Hive::StorageDescriptor &sd = hms_table.sd;
    sd.inputFormat = ObHmsPlaceholderConstants::TEXT_INPUT_FORMAT;
    sd.outputFormat = ObHmsPlaceholderConstants::TEXT_OUTPUT_FORMAT;
    sd.location = "";

    Apache::Hadoop::Hive::SerDeInfo serde;
    serde.serializationLib = ObHmsPlaceholderConstants::LAZY_SIMPLE_SERDE;
    sd.serdeInfo = serde;

    ObColumnIterByPrevNextID column_iter(ob_table);
    const ObColumnSchemaV2 *col = nullptr;
    int32_t heap_pk_seq = 0;
    while (OB_SUCC(ret) && OB_SUCC(column_iter.next(col))) {
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret));
      } else if (col->is_hidden() || col->is_shadow_column()) {
        // skip hidden/shadow columns
      } else {
        // Sync 建表只在同步功能开启时执行，且 Hive 侧已放宽建表类型校验，故每列都原样写 OB
        // 真类型串（保留 unsigned / 长度 / 精度 / enum·set 取值等），读回侧可无损重建。
        std::string ob_type_str;
        if (OB_FAIL(build_ob_column_type_str(*col, ob_type_str))) {
          LOG_WARN("failed to build ob column type str", K(ret),
                   K(col->get_column_name_str()), K(col->get_data_type()));
        } else {
          Apache::Hadoop::Hive::FieldSchema field;
          field.name = std::string(col->get_column_name(), col->get_column_name_str().length());
          field.type = ob_type_str;

          // sync OB column comment to the HMS placeholder, only when non-empty
          const ObString &col_comment = col->get_comment_str();
          if (!col_comment.empty()) {
            field.comment = std::string(col_comment.ptr(), col_comment.length());
          }
          sd.cols.push_back(field);

          if (ob_table.is_table_with_pk()
              && (col->is_original_rowkey_column() || col->is_heap_table_primary_key_column())) {
            int32_t key_seq = 0;
            if (col->is_original_rowkey_column()) {
              key_seq = static_cast<int32_t>(col->get_rowkey_position());
            } else {
              key_seq = ++heap_pk_seq;
            }
            Apache::Hadoop::Hive::SQLPrimaryKey primary_key;
            primary_key.__set_table_db(database_name);
            primary_key.__set_table_name(table_name);
            primary_key.__set_column_name(field.name);
            primary_key.__set_key_seq(key_seq);
            primary_key.__set_pk_name("PRIMARY");
            primary_key.__set_enable_cstr(false);
            primary_key.__set_validate_cstr(false);
            primary_key.__set_rely_cstr(true);
            hms_constraints.primaryKeys.push_back(primary_key);
            hms_constraints.__isset.primaryKeys = true;
          }

          if (!col->is_nullable()) {
            Apache::Hadoop::Hive::SQLNotNullConstraint not_null;
            not_null.__set_table_db(database_name);
            not_null.__set_table_name(table_name);
            not_null.__set_column_name(field.name);
            not_null.__set_nn_name(table_name + "_" + field.name + "_nn");
            not_null.__set_enable_cstr(false);
            not_null.__set_validate_cstr(false);
            not_null.__set_rely_cstr(true);
            hms_constraints.notNullConstraints.push_back(not_null);
            hms_constraints.__isset.notNullConstraints = true;
          }

          const ObObj &cur_default = col->get_cur_default_value();
          // 仅当列存在用户显式默认值时才下发 defaultConstraint，NULL 默认值不同步，
          // 避免读回侧还原成字面量 'NULL' 默认值，与内部表 DEFAULT NULL 行为不一致。
          if (!col->is_generated_column()
              && !col->is_autoincrement()
              && !cur_default.is_null()) {
            std::string default_value;
            if (OB_FAIL(build_hms_default_value(*col, default_value))) {
              LOG_WARN("failed to build hms default constraint",
                       K(ret), K(col->get_column_name_str()));
            } else {
              Apache::Hadoop::Hive::SQLDefaultConstraint default_constraint;
              default_constraint.__set_table_db(database_name);
              default_constraint.__set_table_name(table_name);
              default_constraint.__set_column_name(field.name);
              default_constraint.__set_default_value(default_value);
              default_constraint.__set_dc_name(table_name + "_" + field.name + "_dc");
              default_constraint.__set_enable_cstr(false);
              default_constraint.__set_validate_cstr(false);
              default_constraint.__set_rely_cstr(true);
              hms_constraints.defaultConstraints.push_back(default_constraint);
              hms_constraints.__isset.defaultConstraints = true;
            }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_SUCCESS != ret) {
      LOG_WARN("failed to iterate table columns in logical order", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(build_hms_partition_info(ob_table, hms_table))) {
      LOG_WARN("failed to build hms partition info", K(ret));
    }
  } catch (const std::exception &e) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("std exception when building hms placeholder table", K(ret), K(e.what()));
  } catch (...) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("unknown exception when building hms placeholder table", K(ret));
  }
  return ret;
}

int ObCatalogTypeMapping::unescape_hms_string_literal(ObIAllocator &allocator,
                                                      const char *src,
                                                      const int64_t src_len,
                                                      ObString &out)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(src) || OB_UNLIKELY(src_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hms string literal", K(ret), KP(src), K(src_len));
  } else if (0 == src_len) {
    out.assign_ptr(src, 0);
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(src_len)))) {
    // The unescaped string is never longer than the escaped input.
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate hms unescape buffer", K(ret), K(src_len));
  } else {
    for (int64_t i = 0; i < src_len; ++i) {
      if ('\\' == src[i] && i + 1 < src_len) {
        const char next = src[i + 1];
        char decoded = next;
        bool is_escape_seq = true;
        switch (next) {
          case '\\': decoded = '\\'; break;
          case '\'': decoded = '\''; break;
          case '\"': decoded = '\"'; break;
          case '0': decoded = '\0'; break;
          case 'n': decoded = '\n'; break;
          case 'r': decoded = '\r'; break;
          case 't': decoded = '\t'; break;
          // Only the sequences above are produced by the write path
          // (ObHexEscapeSqlStr). Any other '\x' was NOT an escape on the write
          // side, so keep the backslash and the following byte verbatim instead
          // of dropping the backslash (which would corrupt e.g. a raw 'a\b').
          default: is_escape_seq = false; break;
        }
        if (is_escape_seq) {
          buf[pos++] = decoded;
          ++i;
        } else {
          buf[pos++] = src[i];
        }
      } else {
        buf[pos++] = src[i];
      }
    }
    out.assign_ptr(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObCatalogTypeMapping::apply_hms_default_value(const std::string &default_value,
                                                  ObColumnSchemaV2 &column)
{
  int ret = OB_SUCCESS;
  ObObj default_obj;
  bool skip_set_default = false;
  // deep_copy_obj inside set_cur_default_value copies the string out, so this
  // arena only needs to outlive the set_cur_default_value call below.
  ObArenaAllocator allocator("HMSDefaultUn");
  const char *val = default_value.c_str();
  const int64_t len = static_cast<int64_t>(default_value.length());
  if (0 == default_value.length() || 0 == strcasecmp(val, "NULL")) {
    // 空串/字面量 NULL 视为无默认值，保持原 NULL 默认，走 DEFAULT NULL 打印路径。
    skip_set_default = true;
  } else if (0 == strcasecmp(val, "CURRENT_TIMESTAMP")
      || (len >= 18 && 0 == strncasecmp(val, "CURRENT_TIMESTAMP(", 18))) {
    default_obj.set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
  } else if (len >= 2 && '\'' == val[0] && '\'' == val[len - 1]) {
    // Strip the surrounding quotes, then reverse the backslash escaping the
    // write path applied via ObObj::print_varchar_literal; otherwise \' \\ \n
    // etc. would be restored verbatim and corrupt the default value.
    ObString str;
    if (OB_FAIL(unescape_hms_string_literal(allocator, val + 1, len - 2, str))) {
      LOG_WARN("failed to unescape hms string default", K(ret), K(column.get_column_name_str()));
    } else {
      default_obj.set_varchar(str);
      default_obj.set_collation_type(column.get_collation_type());
    }
  } else {
    char *end_ptr = NULL;
    const int64_t int_val = strtoll(val, &end_ptr, 10);
    if (end_ptr == val + len) {
      if (ObTinyIntType == column.get_data_type()) {
        default_obj.set_tinyint(static_cast<int8_t>(int_val));
      } else if (ObSmallIntType == column.get_data_type()) {
        default_obj.set_smallint(static_cast<int16_t>(int_val));
      } else if (ObMediumIntType == column.get_data_type()) {
        default_obj.set_mediumint(static_cast<int32_t>(int_val));
      } else if (ObInt32Type == column.get_data_type()) {
        default_obj.set_int32(static_cast<int32_t>(int_val));
      } else if (ObIntType == column.get_data_type()) {
        default_obj.set_int(int_val);
      } else {
        ObString str(static_cast<int32_t>(len), val);
        default_obj.set_varchar(str);
        default_obj.set_collation_type(column.get_collation_type());
      }
    } else {
      ObString str(static_cast<int32_t>(len), val);
      default_obj.set_varchar(str);
      default_obj.set_collation_type(column.get_collation_type());
    }
  }
  if (OB_FAIL(ret) || skip_set_default) {
  } else if (OB_FAIL(column.set_cur_default_value(default_obj, false))) {
    LOG_WARN("failed to set cur default value", K(ret), K(column.get_column_name_str()));
  }
  return ret;
}

int ObCatalogTypeMapping::apply_hms_table_constraints(
    const Apache::Hadoop::Hive::SQLAllTableConstraints &hms_constraints,
    ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (hms_constraints.__isset.notNullConstraints) {
    for (size_t i = 0;
         OB_SUCC(ret) && i < hms_constraints.notNullConstraints.size();
         ++i) {
      const Apache::Hadoop::Hive::SQLNotNullConstraint &nn
          = hms_constraints.notNullConstraints[i];
      ObColumnSchemaV2 *column = table_schema.get_column_schema(ObString(nn.column_name.c_str()));
      if (OB_ISNULL(column)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("not null column not found", K(ret), "column", nn.column_name.c_str());
      } else {
        column->set_nullable(false);
        if (OB_FAIL(table_schema.alter_column(*column, ObTableSchema::CHECK_MODE_ONLINE, false))) {
          LOG_WARN("failed to alter not null column", K(ret), K(nn.column_name.c_str()));
        }
      }
    }
  }
  if (OB_SUCC(ret) && hms_constraints.__isset.defaultConstraints) {
    for (size_t i = 0;
         OB_SUCC(ret) && i < hms_constraints.defaultConstraints.size();
         ++i) {
      const Apache::Hadoop::Hive::SQLDefaultConstraint &dc
          = hms_constraints.defaultConstraints[i];
      ObColumnSchemaV2 *column = table_schema.get_column_schema(ObString(dc.column_name.c_str()));
      if (OB_ISNULL(column)) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("default column not found", K(ret), "column", dc.column_name.c_str());
      } else if (OB_FAIL(apply_hms_default_value(dc.default_value, *column))) {
        LOG_WARN("failed to apply default value", K(ret), K(dc.column_name.c_str()));
      } else if (OB_FAIL(table_schema.alter_column(*column, ObTableSchema::CHECK_MODE_ONLINE, false))) {
        LOG_WARN("failed to alter default column", K(ret), K(dc.column_name.c_str()));
      }
    }
  }
  if (OB_SUCC(ret) && hms_constraints.__isset.primaryKeys) {
    // 外表的物理 rowkey/pk_exists 会在 ObILakeTableMetadata::build_table_schema 里被
    // 隐藏列覆写，这里只在用户主键列上打 rowkey_position 标记（不动 rowkey_info_/pk_exists_），
    // 由 show create table / desc 侧针对外表按列 rowkey_position 后处理展示。
    for (int32_t seq = 1;
         OB_SUCC(ret)
         && seq <= static_cast<int32_t>(hms_constraints.primaryKeys.size());
         ++seq) {
      const Apache::Hadoop::Hive::SQLPrimaryKey *pk = nullptr;
      for (size_t i = 0; i < hms_constraints.primaryKeys.size(); ++i) {
        if (hms_constraints.primaryKeys[i].key_seq == seq) {
          pk = &hms_constraints.primaryKeys[i];
          break;
        }
      }
      if (OB_ISNULL(pk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary key seq not found", K(ret), K(seq));
      } else {
        ObColumnSchemaV2 *column = table_schema.get_column_schema(ObString(pk->column_name.c_str()));
        if (OB_ISNULL(column)) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_WARN("primary key column not found", K(ret), "column", pk->column_name.c_str());
        } else {
          column->set_rowkey_position(seq);
          column->set_order_in_rowkey(ObOrderType::ASC);
          column->set_nullable(false);
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
