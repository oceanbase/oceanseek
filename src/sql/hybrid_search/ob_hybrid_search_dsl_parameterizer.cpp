/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_PC
#include "sql/hybrid_search/ob_hybrid_search_dsl_parameterizer.h"
#include "sql/hybrid_search/ob_hybrid_search_plan_cache.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "share/ob_define.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
class ObHybridSearchDslParameterizer::TemplateBuilder
{
public:
  TemplateBuilder(ObIAllocator &allocator, ObIArray<ObPCParam *> &raw_params,
                  ObHybridDslRequestInfo &request)
      : template_(&allocator),
        allocator_(allocator),
        raw_params_(raw_params),
        request_(request)
  {}

  int build(ObIJsonBase &root)
  {
    int ret = OB_SUCCESS;
    // Unsupported input is a normal fallback, so leave logging to the caller.
    ret = visit(root, ROOT);
    return ret;
  }

  ObString get_template() const { return template_.string(); }
  static bool is_scalar(const ObIJsonBase &node)
  {
    return node.json_type() == ObJsonNodeType::J_STRING
        || node.json_type() == ObJsonNodeType::J_BOOLEAN
        || ObIJsonBase::is_json_number_type(node.json_type());
  }

private:
  static const int64_t MAX_FAST_DSL_DEPTH = 128;
  enum DslContext
  {
    FIXED, ROOT, CLAUSE, BOOL_QUERY, TEXT_FIELDS, TEXT_FIELD, TEXT_OPTIONS,
    QUERY_STRING, TERM_FIELDS, TERM_FIELD, TERM_OPTIONS, TERMS_FIELDS,
    TERMS_ELEMENTS, RANGE_FIELDS, RANGE_BOUNDS, KNN, TEXT_VALUE, VECTOR_VALUE,
    SCALAR_VALUE, TERMS_VALUE
  };

  static bool key_is(const ObString &key, const char *name)
  {
    return 0 == key.case_compare(name);
  }

  static bool is_supported_type(const ObJsonNodeType type)
  {
    bool is_supported = false;
    switch (type) {
      case ObJsonNodeType::J_NULL:
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_UINT:
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_DOUBLE:
      case ObJsonNodeType::J_STRING:
      case ObJsonNodeType::J_BOOLEAN:
      case ObJsonNodeType::J_ARRAY:
      case ObJsonNodeType::J_OBJECT:
        is_supported = true;
        break;
      default:
        break;
    }
    return is_supported;
  }

  static DslContext child_context(const DslContext context, const ObString &key)
  {
    DslContext child = FIXED;
    switch (context) {
      case ROOT:
        if (key_is(key, "query")) {
          child = CLAUSE;
        } else if (key_is(key, "knn")) {
          child = KNN;
        }
        break;
      case CLAUSE:
        if (key_is(key, "bool")) {
          child = BOOL_QUERY;
        } else if (key_is(key, "match") || key_is(key, "match_phrase")) {
          child = TEXT_FIELDS;
        } else if (key_is(key, "query_string") || key_is(key, "multi_match")) {
          child = QUERY_STRING;
        } else if (key_is(key, "term")) {
          child = TERM_FIELDS;
        } else if (key_is(key, "terms")) {
          child = TERMS_FIELDS;
        } else if (key_is(key, "range")) {
          child = RANGE_FIELDS;
        }
        break;
      case BOOL_QUERY:
        if (key_is(key, "must") || key_is(key, "should")
            || key_is(key, "filter") || key_is(key, "must_not")) {
          child = CLAUSE;
        }
        break;
      case TEXT_FIELDS:
        child = TEXT_FIELD;
        break;
      case TEXT_OPTIONS:
      case QUERY_STRING:
        if (key_is(key, "query")) {
          child = TEXT_VALUE;
        }
        break;
      case TERM_FIELDS:
        child = TERM_FIELD;
        break;
      case TERM_OPTIONS:
        if (key_is(key, "value")) {
          child = SCALAR_VALUE;
        }
        break;
      case TERMS_FIELDS:
        if (!key_is(key, "boost")) {
          child = TERMS_ELEMENTS;
        }
        break;
      case RANGE_FIELDS:
        child = RANGE_BOUNDS;
        break;
      case RANGE_BOUNDS:
        if (key_is(key, "gt") || key_is(key, "gte")
            || key_is(key, "lt") || key_is(key, "lte")) {
          child = SCALAR_VALUE;
        }
        break;
      case KNN:
        if (key_is(key, "query_vector")) {
          child = VECTOR_VALUE;
        } else if (key_is(key, "filter")) {
          child = CLAUSE;
        }
        break;
      default:
        break;
    }
    return child;
  }

  int visit(ObIJsonBase &node, const DslContext parent_context, const int64_t depth = 0)
  {
    int ret = OB_SUCCESS;
    const ObJsonNodeType type = node.json_type();
    DslContext context = parent_context;
    // Bound fast-path recursion; the full resolver validates deeper structures.
    if (depth > MAX_FAST_DSL_DEPTH || !is_supported_type(type)) {
      ret = OB_NOT_SUPPORTED;
    } else {
      if (TEXT_FIELD == context) {
        context = type == ObJsonNodeType::J_OBJECT ? TEXT_OPTIONS : TEXT_VALUE;
      } else if (TERM_FIELD == context) {
        context = type == ObJsonNodeType::J_OBJECT ? TERM_OPTIONS : SCALAR_VALUE;
      }
      if (TEXT_VALUE == context || VECTOR_VALUE == context
          || SCALAR_VALUE == context || TERMS_VALUE == context) {
        ret = append_parameter(node, context);
      } else if (TERMS_ELEMENTS == context && type != ObJsonNodeType::J_ARRAY) {
        ret = OB_NOT_SUPPORTED;
      } else if (type == ObJsonNodeType::J_OBJECT) {
        // JSON keys are sorted by the parser, so input key order does not affect numbering.
        OZ(template_.append("{"));
        for (uint64_t i = 0; OB_SUCC(ret) && i < node.element_count(); ++i) {
          ObString key;
          ObIJsonBase *value = nullptr;
          OZ(node.get_object_value(i, key, value));
          if (OB_SUCC(ret) && OB_ISNULL(value)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null DSL object value", K(ret), K(i));
          }
          if (OB_SUCC(ret)) {
            ObJsonString json_key(key);
            if (i > 0) {
              OZ(template_.append(","));
            }
            OZ(json_key.print(template_, true));
            OZ(template_.append(":"));
            if (OB_SUCC(ret)) {
              ret = visit(*value, child_context(context, key), depth + 1);
            }
          }
        }
        OZ(template_.append("}"));
      } else if (type == ObJsonNodeType::J_ARRAY) {
        // Preserve array order and length.
        OZ(template_.append("["));
        for (uint64_t i = 0; OB_SUCC(ret) && i < node.element_count(); ++i) {
          ObIJsonBase *value = nullptr;
          OZ(node.get_array_element(i, value));
          if (OB_SUCC(ret) && OB_ISNULL(value)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null DSL array element", K(ret), K(i));
          }
          if (i > 0) {
            OZ(template_.append(","));
          }
          const DslContext child = TERMS_ELEMENTS == context ? TERMS_VALUE
              : (CLAUSE == context || KNN == context ? context : FIXED);
          if (OB_SUCC(ret)) {
            ret = visit(*value, child, depth + 1);
          }
        }
        OZ(template_.append("]"));
      } else {
        OZ(node.print(template_, true));
      }
    }
    return ret;
  }

  int append_parameter(ObIJsonBase &node, const DslContext context)
  {
    int ret = OB_SUCCESS;
    const ObJsonNodeType type = node.json_type();
    const bool is_vector = VECTOR_VALUE == context;
    if ((TEXT_VALUE == context && type != ObJsonNodeType::J_STRING)
        || (is_vector && type != ObJsonNodeType::J_STRING && type != ObJsonNodeType::J_ARRAY)
        || (!is_vector && TEXT_VALUE != context && !is_scalar(node))) {
      ret = OB_NOT_SUPPORTED;
    } else {
      void *buf = allocator_.alloc(sizeof(ObPCParam));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObPCParam *param = new (buf) ObPCParam();
        OZ(ObHybridSearchDslParameterizer::create_input_node(allocator_, node, param->node_));
        OZ(template_.append("?"));
        OZ(raw_params_.push_back(param));
        OZ(request_.append_param_node(node));
      }
    }
    return ret;
  }

private:
  ObJsonBuffer template_;
  ObIAllocator &allocator_;
  ObIArray<ObPCParam *> &raw_params_;
  ObHybridDslRequestInfo &request_;
};

int ObHybridSearchDslParameterizer::parse_json(ObIAllocator &allocator, const ObString &text,
    ObJsonNode *&root, const char *&syntax_error,
    uint64_t &error_offset)
{
  int ret = OB_SUCCESS;
  ret = ObJsonParser::parse_json_text(&allocator, text.ptr(), text.length(), syntax_error,
      &error_offset, root, ObJsonParser::JSN_UNIQUE_FLAG);
  return ret;
}

int ObHybridSearchDslParameterizer::extract(ObIAllocator &allocator, const ObString &text,
    const ObHybridDslSource &source,
    ObHybridDslRequestInfo *&request,
    ObIArray<ObPCParam *> &raw_params)
{
  int ret = OB_SUCCESS;
  request = nullptr;
  ObHybridDslRequestInfo *result = nullptr;
  void *buf = allocator.alloc(sizeof(ObHybridDslRequestInfo));
  ObJsonNode *root = nullptr;
  const char *syntax_error = nullptr;
  uint64_t error_offset = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    result = new (buf) ObHybridDslRequestInfo(allocator);
    TemplateBuilder builder(allocator, raw_params, *result);
    result->get_source() = source;
    OZ(ob_write_string(allocator, text, result->get_dsl_snapshot()));
    OZ(parse_json(allocator, result->get_dsl_snapshot(), root, syntax_error, error_offset));
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(root) || root->json_type() != ObJsonNodeType::J_OBJECT
          || root->element_count() == 0) {
        ret = OB_NOT_SUPPORTED;
      } else {
        result->set_json_root(root);
        ret = builder.build(*root);
        if (OB_SUCC(ret) && raw_params.empty()) {
          // No supported query literal was extracted; use the original resolver path.
          ret = OB_NOT_SUPPORTED;
        }
        OZ(ob_write_string(allocator, builder.get_template(), result->get_dsl_template()));
        if (OB_SUCC(ret)) {
          request = result;
        }
      }
    }
  }
  return ret;
}

int ObHybridSearchDslParameterizer::remove_carrier_param(
    const ObHybridDslRequestInfo &request, const ObIArray<ObPCParam *> &raw_params,
    ObIArray<ObPCParam *> &special_params)
{
  int ret = OB_SUCCESS;
  const int64_t carrier = request.get_source().raw_param_idx_;
  if (carrier < 0 || carrier >= raw_params.count() || OB_ISNULL(raw_params.at(carrier))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DSL carrier index", K(ret), K(carrier), K(raw_params.count()));
  } else {
    bool is_removed = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_removed && i < special_params.count(); ++i) {
      if (special_params.at(i) == raw_params.at(carrier)) {
        OZ(special_params.remove(i));
        is_removed = OB_SUCC(ret);
      }
    }
  }
  return ret;
}

int ObHybridSearchDslParameterizer::create_input_node(ObIAllocator &allocator,
    const ObIJsonBase &node, ParseNode *&literal)
{
  int ret = OB_SUCCESS;
  const ObJsonNodeType type = node.json_type();
  ObJsonBuffer buffer(&allocator);
  ObString text;
  literal = static_cast<ParseNode *>(allocator.alloc(sizeof(ParseNode)));
  if (OB_ISNULL(literal)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(literal, 0, sizeof(ParseNode));
    if (type == ObJsonNodeType::J_STRING) {
      text.assign_ptr(node.get_data(), node.get_data_length());
    } else {
      OZ(node.print(buffer, false));
      OZ(buffer.get_result_string(text));
    }
    if (OB_SUCC(ret)) {
      literal->str_value_ = text.ptr();
      literal->str_len_ = text.length();
      literal->raw_text_ = text.ptr();
      literal->text_len_ = text.length();
      literal->raw_sql_offset_ = -1;
      literal->pos_ = -1;
      if (type == ObJsonNodeType::J_STRING || type == ObJsonNodeType::J_ARRAY) {
        literal->type_ = T_VARCHAR;
      } else if (type == ObJsonNodeType::J_INT) {
        literal->type_ = T_INT;
        literal->value_ = node.get_int();
      } else if (type == ObJsonNodeType::J_UINT) {
        literal->type_ = T_UINT64;
        literal->value_ = static_cast<int64_t>(node.get_uint());
      } else if (type == ObJsonNodeType::J_BOOLEAN) {
        literal->type_ = T_BOOL;
        literal->value_ = node.get_boolean();
      } else if (type == ObJsonNodeType::J_DOUBLE) {
        literal->type_ = T_DOUBLE;
      } else if (type == ObJsonNodeType::J_DECIMAL) {
        literal->type_ = T_NUMBER;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported DSL input literal", K(ret), K(type));
      }
    }
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
