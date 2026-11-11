/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_catalog_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
// using namespace common;
// using namespace sql;
// using namespace rootserver;

ObCatalogSchema::ObCatalogSchema()
  : ObSchema()
{
  reset();
}

ObCatalogSchema::ObCatalogSchema(ObIAllocator *allocator)
  : ObSchema(allocator)
{
  reset();
}

ObCatalogSchema::ObCatalogSchema(const ObCatalogSchema &src_schema)
  : ObSchema()
{
  reset();
  *this = src_schema;
}

ObCatalogSchema::~ObCatalogSchema()
{
}

ObCatalogSchema &ObCatalogSchema::operator =(const ObCatalogSchema &src_schema)
{
  if (this != &src_schema) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = src_schema.error_ret_;
    set_tenant_id(src_schema.tenant_id_);
    set_catalog_id(src_schema.catalog_id_);
    set_schema_version(src_schema.schema_version_);
    set_name_case_mode(src_schema.name_case_mode_);

    if (OB_FAIL(set_catalog_name(src_schema.catalog_name_))) {
      LOG_WARN("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_catalog_properties(src_schema.catalog_properties_))) {
      LOG_WARN("set_tenant_name failed", K(ret));
    } else if (OB_FAIL(set_comment(src_schema.comment_))) {
      LOG_WARN("set_comment failed", K(ret));
    } else {} // no more to do

    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

int ObCatalogSchema::assign(const ObCatalogSchema &src_schema) {
  int ret = OB_SUCCESS;
  *this = src_schema;
  ret = get_err_ret();
  return ret;
}

int64_t ObCatalogSchema::get_convert_size() const
{
  int64_t convert_size = sizeof(*this);
  convert_size += catalog_name_.length() + 1;
  convert_size += catalog_properties_.length() + 1;
  convert_size += comment_.length() + 1;
  return convert_size;
}

bool ObCatalogSchema::is_valid() const
{
  bool ret = true;
  if (!ObSchema::is_valid()
      || !is_valid_tenant_id(tenant_id_)
      || !is_valid_id(catalog_id_)
      || schema_version_ < 0
      || catalog_name_.empty()) {
    ret = false;
  }
  return ret;
}

void ObCatalogSchema::reset()
{
  tenant_id_ = OB_INVALID_ID;
  catalog_id_ = OB_INVALID_ID;
  schema_version_ = OB_INVALID_VERSION;
  catalog_name_.reset();
  catalog_properties_.reset();
  comment_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
  ObSchema::reset();
}

ObCatalogSchema::ObCatalogSchema(bool is_mysql_mode)
  : ObSchema()
{
  tenant_id_ = OB_INVALID_ID;
  catalog_id_ = OB_INTERNAL_CATALOG_ID;
  schema_version_ = OB_INVALID_VERSION;
  catalog_name_ = is_mysql_mode ? OB_INTERNAL_CATALOG_NAME : OB_INTERNAL_CATALOG_NAME_UPPER;
  catalog_properties_.reset();
  comment_.reset();
  name_case_mode_ = OB_NAME_CASE_INVALID;
}

OB_SERIALIZE_MEMBER(ObCatalogSchema,
                    tenant_id_,
                    catalog_id_,
                    schema_version_,
                    catalog_name_,
                    catalog_properties_,
                    name_case_mode_,
                    comment_);


ObCatalogPriv& ObCatalogPriv::operator=(const ObCatalogPriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.catalog_, catalog_))) {
      LOG_WARN("failed to deep copy catalog", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObCatalogPriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid();
}

void ObCatalogPriv::reset()
{
  catalog_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObCatalogPriv::get_convert_size() const
{
  int64_t convert_size = 0;
  convert_size += ObPriv::get_convert_size();
  convert_size += sizeof(ObCatalogPriv) - sizeof(ObPriv);
  convert_size += catalog_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObCatalogPriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, catalog_);
  return ret;
}

OB_DEF_DESERIALIZE(ObCatalogPriv)
{
  int ret = OB_SUCCESS;
  ObString catalog;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, catalog);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(catalog, catalog_))) {
    LOG_WARN("failed to deep copy catalog", K(catalog), K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCatalogPriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, catalog_);
  return len;
}

ObCatalogDatabasePriv &ObCatalogDatabasePriv::operator=(const ObCatalogDatabasePriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.catalog_, catalog_))) {
      LOG_WARN("failed to deep copy catalog", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.database_, database_))) {
      LOG_WARN("failed to deep copy database", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObCatalogDatabasePriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid()
      && !catalog_.empty() && !database_.empty();
}

void ObCatalogDatabasePriv::reset()
{
  catalog_.reset();
  database_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObCatalogDatabasePriv::get_convert_size() const
{
  int64_t convert_size = ObPriv::get_convert_size();
  convert_size += sizeof(ObCatalogDatabasePriv) - sizeof(ObPriv);
  convert_size += catalog_.length() + 1;
  convert_size += database_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObCatalogDatabasePriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, catalog_, database_);
  return ret;
}

OB_DEF_DESERIALIZE(ObCatalogDatabasePriv)
{
  int ret = OB_SUCCESS;
  ObString catalog;
  ObString database;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, catalog, database);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(catalog, catalog_))) {
    LOG_WARN("failed to deep copy catalog", K(catalog), K(ret));
  } else if (OB_FAIL(deep_copy_str(database, database_))) {
    LOG_WARN("failed to deep copy database", K(database), K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCatalogDatabasePriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, catalog_, database_);
  return len;
}

ObCatalogTablePriv &ObCatalogTablePriv::operator=(const ObCatalogTablePriv &other)
{
  if (this != &other) {
    reset();
    int ret = OB_SUCCESS;
    error_ret_ = other.error_ret_;
    if (OB_FAIL(ObPriv::assign(other))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.catalog_, catalog_))) {
      LOG_WARN("failed to deep copy catalog", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.database_, database_))) {
      LOG_WARN("failed to deep copy database", K(ret));
    } else if (OB_FAIL(deep_copy_str(other.table_, table_))) {
      LOG_WARN("failed to deep copy table", K(ret));
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    }
  }
  return *this;
}

bool ObCatalogTablePriv::is_valid() const
{
  return ObSchema::is_valid() && ObPriv::is_valid()
      && !catalog_.empty() && !database_.empty() && !table_.empty();
}

void ObCatalogTablePriv::reset()
{
  catalog_.reset();
  database_.reset();
  table_.reset();
  ObSchema::reset();
  ObPriv::reset();
}

int64_t ObCatalogTablePriv::get_convert_size() const
{
  int64_t convert_size = ObPriv::get_convert_size();
  convert_size += sizeof(ObCatalogTablePriv) - sizeof(ObPriv);
  convert_size += catalog_.length() + 1;
  convert_size += database_.length() + 1;
  convert_size += table_.length() + 1;
  return convert_size;
}

OB_DEF_SERIALIZE(ObCatalogTablePriv)
{
  int ret = OB_SUCCESS;
  BASE_SER((, ObPriv));
  LST_DO_CODE(OB_UNIS_ENCODE, catalog_, database_, table_);
  return ret;
}

OB_DEF_DESERIALIZE(ObCatalogTablePriv)
{
  int ret = OB_SUCCESS;
  ObString catalog;
  ObString database;
  ObString table;
  BASE_DESER((, ObPriv));
  LST_DO_CODE(OB_UNIS_DECODE, catalog, database, table);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to deserialize data", K(ret));
  } else if (OB_FAIL(deep_copy_str(catalog, catalog_))) {
    LOG_WARN("failed to deep copy catalog", K(catalog), K(ret));
  } else if (OB_FAIL(deep_copy_str(database, database_))) {
    LOG_WARN("failed to deep copy database", K(database), K(ret));
  } else if (OB_FAIL(deep_copy_str(table, table_))) {
    LOG_WARN("failed to deep copy table", K(table), K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCatalogTablePriv)
{
  int64_t len = ObPriv::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN, catalog_, database_, table_);
  return len;
}

} //namespace schema
} //namespace share
} //namespace oceanbase
