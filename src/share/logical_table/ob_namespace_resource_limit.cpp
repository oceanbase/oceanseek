/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_namespace_resource_limit.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/allocator/ob_allocator.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;

namespace
{
enum class NsOpsFieldType
{
  ROW_LIMIT,
  SIZE_LIMIT,
  RATE_LIMIT_ENABLE,
  RU_RATE
};

bool is_known_config_key(const ObString &key)
{
  return key.case_compare("row_limit") == 0
      || key.case_compare("size_limit") == 0
      || key.case_compare("rate_limit_enable") == 0
      || key.case_compare("qps_burst") == 0
      || key.case_compare("qps_refill") == 0
      || key.case_compare("tps_burst") == 0
      || key.case_compare("tps_refill") == 0
      || key.case_compare("data_burst") == 0
      || key.case_compare("data_refill") == 0;
}

NsOpsFieldType resolve_field_type(const ObString &key)
{
  NsOpsFieldType type = NsOpsFieldType::RU_RATE;
  if (0 == key.case_compare("row_limit")) {
    type = NsOpsFieldType::ROW_LIMIT;
  } else if (0 == key.case_compare("size_limit")) {
    type = NsOpsFieldType::SIZE_LIMIT;
  } else if (0 == key.case_compare("rate_limit_enable")) {
    type = NsOpsFieldType::RATE_LIMIT_ENABLE;
  }
  return type;
}

int validate_json_number_field(const ObIJsonBase *node,
                               const ObString &key,
                               const NsOpsFieldType field_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json node is null", KR(ret), K(key));
  } else if (ObJsonNodeType::J_INT != node->json_type()
             && ObJsonNodeType::J_UINT != node->json_type()
             && ObJsonNodeType::J_DECIMAL != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("config value must be a number", KR(ret), K(key), "type", node->json_type());
  } else {
    int64_t value = 0;
    if (OB_FAIL(node->to_int(value))) {
      LOG_WARN("fail to convert json number", KR(ret), K(key));
    } else {
      switch (field_type) {
      case NsOpsFieldType::ROW_LIMIT:
        ret = ObNamespaceResourceLimitValidator::validate_row_limit(value);
        break;
      case NsOpsFieldType::SIZE_LIMIT:
        ret = ObNamespaceResourceLimitValidator::validate_size_limit(value);
        break;
      case NsOpsFieldType::RATE_LIMIT_ENABLE:
        ret = ObNamespaceResourceLimitValidator::validate_rate_limit_enable(value);
        break;
      case NsOpsFieldType::RU_RATE:
        ret = ObNamespaceResourceLimitValidator::validate_ru_rate(value, key);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected field type", KR(ret), K(key));
        break;
      }
    }
  }
  return ret;
}
} // namespace

int ObNamespaceResourceLimitValidator::validate_row_limit(const int64_t value)
{
  int ret = OB_SUCCESS;
  if (value < -1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row_limit must be -1, 0, or positive", KR(ret), K(value));
  }
  return ret;
}

int ObNamespaceResourceLimitValidator::validate_size_limit(const int64_t value)
{
  int ret = OB_SUCCESS;
  if (value < -1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("size_limit must be -1, 0, or positive", KR(ret), K(value));
  }
  return ret;
}

int ObNamespaceResourceLimitValidator::validate_rate_limit_enable(const int64_t value)
{
  int ret = OB_SUCCESS;
  if (value < 0 || value > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rate_limit_enable must be 0 or 1", KR(ret), K(value));
  }
  return ret;
}

int ObNamespaceResourceLimitValidator::validate_ru_rate(const int64_t value, const common::ObString &field_name)
{
  int ret = OB_SUCCESS;
  if (value < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ru rate must be non-negative", KR(ret), K(value), K(field_name));
  }
  return ret;
}

int ObNamespaceResourceLimitValidator::validate_patch_json(const ObString &json_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "NsOpsCfgVal"));
  ObJsonNode *root = nullptr;
  if (json_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ops config json is empty", KR(ret));
  } else if (OB_FAIL(ObJsonParser::get_tree(&allocator, json_str, root))) {
    LOG_WARN("invalid ops config json", KR(ret), K(json_str));
  } else if (OB_ISNULL(root) || ObJsonNodeType::J_OBJECT != root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ops config json must be an object", KR(ret));
  } else {
    const uint64_t count = root->element_count();
    if (0 == count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ops config json must contain at least one field", KR(ret));
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObString key;
      ObIJsonBase *val_node = nullptr;
      if (OB_FAIL(root->get_object_value(i, key, val_node))) {
        LOG_WARN("fail to get json object entry", KR(ret), K(i));
      } else if (!is_known_config_key(key)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported ops config key", KR(ret), K(key));
      } else if (OB_FAIL(validate_json_number_field(val_node, key, resolve_field_type(key)))) {
        LOG_WARN("invalid ops config field", KR(ret), K(key));
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
