/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/search_index/ob_search_index_lifecycle_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace schema;

int ObSearchIndexLifecycleUtil::validate_child_types(
    const ObIArray<ObSearchPhysicalChild> &children)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children.count(); ++i) {
    const ObSearchPhysicalChild &child = children.at(i);
    if (!is_async_search_aux_index(child.index_type_)
        || OB_INVALID_ID == child.table_id_ || OB_INVALID_ID == child.def_table_id_
        || (i > 0 && child.def_table_id_ != children.at(0).def_table_id_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid search child type, id or parent", KR(ret), K(child),
          "expected_def_id", children.at(0).def_table_id_);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      if (children.at(j).table_id_ == child.table_id_
          || children.at(j).index_type_ == child.index_type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("duplicate search child id or type", KR(ret), K(child),
            "previous_child", children.at(j));
      }
    }
  }
  return ret;
}

int ObSearchIndexLifecycleUtil::collect_search_children(
    const ObTableSchema &def_schema, ObIArray<ObSearchPhysicalChild> &children)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSearchPhysicalChild, 3> local;
  children.reset();
  if (!def_schema.is_async_search_def_index()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected async search def", KR(ret), "def_id", def_schema.get_table_id());
  } else {
    const ObIArray<ObAuxTableMetaInfo> &infos = def_schema.get_simple_index_infos();
    // Enumerate the registered family, including Text-only and empty families.
    // A DATA lookup cannot validate the family: DATA is optional for FULLTEXT-only.
    for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
      ObSearchPhysicalChild child;
      child.def_table_id_ = def_schema.get_table_id();
      child.table_id_ = infos.at(i).table_id_;
      child.index_type_ = infos.at(i).index_type_;
      if (OB_FAIL(local.push_back(child))) {
        LOG_WARN("failed to append search child", KR(ret), K(child));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(validate_child_types(local))) {
      LOG_WARN("invalid search child metadata", KR(ret), K(local));
    }
    const ObIndexType ordered_types[] = {
      INDEX_TYPE_ASYNC_SEARCH_DATA_INDEX_LOCAL,
      INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL,
      INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL
    };
    for (int64_t t = 0; OB_SUCC(ret) && t < ARRAYSIZEOF(ordered_types); ++t) {
      for (int64_t i = 0; OB_SUCC(ret) && i < local.count(); ++i) {
        if (ordered_types[t] == local.at(i).index_type_
            && OB_FAIL(children.push_back(local.at(i)))) {
          LOG_WARN("failed to copy search child", KR(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    children.reset();
  }
  return ret;
}

int ObSearchIndexLifecycleUtil::expand_async_index(
    const ObTableSchema &base_schema, const ObTableSchema &index_schema,
    ObIArray<ObAsyncPhysicalTarget> &targets)
{
  int ret = OB_SUCCESS;
  targets.reset();
  if (!is_async_search_def_index(index_schema.get_index_type())
      && !is_async_fts_index(index_schema.get_index_type())
      && !is_async_delete_index(index_schema.get_index_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected logical async index", KR(ret), "table_id", index_schema.get_table_id());
  } else if (index_schema.get_tenant_id() != base_schema.get_tenant_id()
      || index_schema.get_data_table_id() != base_schema.get_table_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical async index has an unexpected parent", KR(ret),
        "base_id", base_schema.get_table_id(), K(index_schema));
  } else if (index_schema.is_async_search_def_index()) {
    ObSEArray<ObSearchPhysicalChild, 3> children;
    if (OB_FAIL(collect_search_children(index_schema, children))) {
      LOG_WARN("failed to expand search def", KR(ret), "def_id", index_schema.get_table_id());
    } else if (children.empty()) {
      // Preserve get_search_data_index_tid()'s empty-registration error.
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("async search def has no registered children", KR(ret),
          "tenant_id", index_schema.get_tenant_id(), "def_id", index_schema.get_table_id());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < children.count(); ++i) {
      ObAsyncPhysicalTarget target;
      target.table_id_ = children.at(i).table_id_;
      target.def_table_id_ = children.at(i).def_table_id_;
      target.index_type_ = children.at(i).index_type_;
      if (OB_FAIL(targets.push_back(target))) {
        LOG_WARN("failed to append search target", KR(ret), K(target));
      }
    }
  } else {
    ObAsyncPhysicalTarget target;
    target.table_id_ = index_schema.get_table_id();
    target.index_type_ = index_schema.get_index_type();
    target.is_delete_ = is_async_delete_index(target.index_type_);
    if (OB_FAIL(targets.push_back(target))) {
      LOG_WARN("failed to append async target", KR(ret), K(target));
    }
  }
  if (OB_FAIL(ret)) {
    targets.reset();
  }
  return ret;
}

int ObSearchIndexLifecycleUtil::get_target_schema(
    ObSchemaGetterGuard &guard, const ObTableSchema &base_schema,
    const ObAsyncPhysicalTarget &target, const ObTableSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const uint64_t parent_id = OB_INVALID_ID == target.def_table_id_
      ? base_schema.get_table_id() : target.def_table_id_;
  if (OB_FAIL(guard.get_table_schema(base_schema.get_tenant_id(), target.table_id_, schema))) {
    LOG_WARN("failed to get physical target schema", KR(ret), K(target));
  } else if (nullptr != schema
      && (schema->get_tenant_id() != base_schema.get_tenant_id()
          || schema->get_table_id() != target.table_id_
          || schema->get_data_table_id() != parent_id
          || schema->get_index_type() != target.index_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical target relationship does not match metadata",
        KR(ret), K(target), KPC(schema));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
