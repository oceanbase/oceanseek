/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_search_index_config_filter.h"
#include "ob_search_index_config.h"
#include "common/object/ob_object.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/utility.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/charset/ob_charset.h"
#include "sql/parser/parse_node.h"
#include "share/ob_define.h" // OZ macro
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace share
{
using namespace common;
namespace
{
bool consume_filter_key(const ObString &raw, int64_t &pos, const char *key)
{
  const int64_t len = strlen(key);
  const bool matched = len <= raw.length() - pos && 0 == MEMCMP(raw.ptr() + pos, key, len);
  if (matched) {
    pos += len;
  }
  return matched;
}

// A null output skips the list when only NESTED_PATHS needs to be enumerated.
int read_filter_values(const ObString &raw, int64_t &pos, ObIArray<ObString> *values)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && pos < raw.length() && raw.ptr()[pos] >= '0' && raw.ptr()[pos] <= '9') {
    int64_t len = 0;
    while (OB_SUCC(ret) && pos < raw.length() && raw.ptr()[pos] >= '0' && raw.ptr()[pos] <= '9') {
      const int64_t digit = raw.ptr()[pos++] - '0';
      if (len > (INT64_MAX - digit) / 10) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("search index filter value length overflows", K(ret), K(len), K(digit), K(pos));
      } else {
        len = len * 10 + digit;
      }
    }
    if (OB_SUCC(ret)) {
      if (len > raw.length() - pos) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("search index filter value exceeds remaining input", K(ret), K(len), K(pos),
                 "input_length", raw.length());
      } else {
        if (values != nullptr) {
          OZ(values->push_back(ObString(static_cast<int32_t>(len), raw.ptr() + pos)));
        }
        pos += len;
      }
    }
  }
  return ret;
}

int print_quoted(const ObString &str, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const ObHexEscapeSqlStr escaped_str(str);
  const int64_t escaped_len = str.length() + escaped_str.get_extra_length();
  if (OB_FAIL(databuff_printf(buf, len, pos, "'"))) {
    LOG_WARN("failed to print opening quote", K(ret));
  } else if (escaped_str.to_string(buf + pos, len - pos) != escaped_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("failed to print escaped configuration string", K(ret), K(escaped_len));
  } else {
    pos += escaped_len;
    if (OB_FAIL(databuff_printf(buf, len, pos, "'"))) {
      LOG_WARN("failed to print closing quote", K(ret));
    }
  }
  return ret;
}
} // namespace

ObSearchIndexJsonPathMatcher::ObSearchIndexJsonPathMatcher(const uint64_t tenant_id)
  : tenant_id_(tenant_id),
    allocator_(ObMemAttr(tenant_id_, ObModIds::OB_JSON_PARSER)),
    inited_(false),
    root_(nullptr),
    all_nodes_(nullptr)
{
}

ObSearchIndexJsonPathMatcher::~ObSearchIndexJsonPathMatcher()
{
  reset();
}

void ObSearchIndexJsonPathMatcher::reset()
{
  Node *n = all_nodes_;
  while (n != nullptr) {
    (void)n->children_.destroy();
    n = n->next_all_;
  }
  root_ = nullptr;
  all_nodes_ = nullptr;
  inited_ = false;
  allocator_.reset();
}

int ObSearchIndexJsonPathMatcher::init(const ObIArray<ObString> &paths)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(new_node(root_))) {
    LOG_WARN("failed to new root node", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < paths.count(); ++i) {
      OZ(insert_path(paths.at(i)));
    }
    inited_ = OB_SUCC(ret);
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::match(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items, const bool index_sub_path,
    bool &matched, bool &is_terminal) const
{
  int ret = OB_SUCCESS;
  matched = false;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized or root is null", K(ret));
  } else {
    const Node *cur = root_;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < path_items.count(); ++idx) {
      Node *child = nullptr;
      if (OB_SUCCESS == (cur->children_.get_refactored("*", child))) {
        matched = true;
        is_terminal = false;
        break;
      }
      const ObSearchIndexPathEncoder::JsonPathItem &item = path_items.at(idx);
      if (item.is_array_path()) {
        matched = cur->terminal_;
        is_terminal = cur->terminal_;
        break;
      } else if (OB_FAIL(cur->children_.get_refactored(item.path, child))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get child", K(ret), K(item.path));
        }
      } else if ((index_sub_path || child->terminal_) && idx == path_items.count() - 1) {
        matched = true;
        is_terminal = true;
        break;
      } else {
        cur = child;
      }
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::new_node(Node *&node)
{
  int ret = OB_SUCCESS;
  node = OB_NEWx(Node, (&allocator_));
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    node->next_all_ = all_nodes_;
    all_nodes_ = node;

    const ObMemAttr bucket_attr(tenant_id_, ObModIds::OB_HASH_BUCKET);
    const ObMemAttr node_attr(tenant_id_, ObModIds::OB_HASH_NODE);
    if (OB_FAIL(node->children_.create(8 /*bucket_num*/, bucket_attr, node_attr))) {
      LOG_WARN("failed to create hashmap for node children", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::get_or_create_child(Node &node, const ObString &seg, Node *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  ret = const_cast<common::hash::ObHashMap<ObString, Node*>&>(node.children_).get_refactored(seg, child);
  if (ret == OB_HASH_NOT_EXIST) {
    ret = OB_SUCCESS;
    if (OB_FAIL(new_node(child))) {
      LOG_WARN("failed to new node", K(ret));
    } else if (OB_FAIL(node.children_.set_refactored(seg, child))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::insert_path(const ObString &path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("root node not initialized", K(ret));
  } else {
    ObJsonPath json_path(path, &allocator_);
    if (OB_FAIL(json_path.parse_path())) {
      LOG_WARN("failed to parse path", K(ret), K(path));
    } else {
      Node *cur = root_;
      JsonPathIterator it = json_path.begin();
      if (it == json_path.end()) {
        ObString wildcard(ObString::make_string("*"));
        Node *child = nullptr;
        if (OB_FAIL(get_or_create_child(*cur, wildcard, child))) {
          LOG_WARN("failed to get or create child for wildcard", K(ret));
        } else {
          cur = child;
        }
      } else {
        for (; OB_SUCC(ret) && it != json_path.end(); ++it) {
          ObJsonPathNode *node = *it;
          if (OB_ISNULL(node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null path node", K(ret));
            break;
          }
          ObJsonPathNodeType node_type = node->get_node_type();
          if (node_type == JPN_ROOT) {
          } else if (node_type == JPN_MEMBER) {
            ObJsonPathBasicNode *basic_node = static_cast<ObJsonPathBasicNode *>(node);
            ObString key_name(basic_node->get_object().len_, basic_node->get_object().object_name_);
            Node *child = nullptr;
            if (OB_FAIL(get_or_create_child(*cur, key_name, child))) {
              LOG_WARN("failed to get or create child", K(ret));
            } else {
              cur = child;
            }
          } else if (node_type == JPN_MEMBER_WILDCARD) {
            ObString wildcard(ObString::make_string("*"));
            Node *child = nullptr;
            if (OB_FAIL(get_or_create_child(*cur, wildcard, child))) {
              LOG_WARN("failed to get or create child for wildcard", K(ret));
            } else {
              cur = child;
            }
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not supported node type", K(ret), K(node_type));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        cur->terminal_ = true;
      }
    }
  }
  return ret;
}

ObSearchIndexConfigFilter::ObSearchIndexConfigFilter(const uint64_t tenant_id)
  : has_include_paths_(false),
    has_exclude_paths_(false),
    has_nested_paths_(false),
    type_mask_(0),
    include_matcher_(tenant_id),
    exclude_matcher_(tenant_id),
    nested_matcher_(tenant_id)
{
}

void ObSearchIndexConfigFilter::reset()
{
  has_include_paths_ = false;
  has_exclude_paths_ = false;
  has_nested_paths_ = false;
  type_mask_ = 0;
  include_matcher_.reset();
  exclude_matcher_.reset();
  nested_matcher_.reset();
}

// Checks whether the given path may use the search index according to INCLUDE_PATHS or EXCLUDE_PATHS.
//
// INCLUDE_PATHS:
//   - Exact path (e.g. "$.a.b.c"): only equality on that path uses the index; range on $.a.b.c does not.
//   - Path with wildcard (e.g. "$.a.b.c.*"): paths under that prefix use the index, same as defined semantics.
//
// EXCLUDE_PATHS (for consistent behavior when excluded subtree has no index rows):
//   - Exact path (e.g. "$.a.b.c"): $.a.b.c uses no index; ancestor paths $.a, $.a.b also cannot use range index.
//     Example: row {"a":{"b":{"c":2}}} has no index data when $.a.b.c is excluded, so json_extract(c1,'$.a')>1
//     must not use the index or results would differ from a non-index scan.
//   - Path with wildcard (e.g. "$.a.b.c.*"): $.a.b.c.* uses no index; $.a, $.a.b, $.a.b.c cannot use range index.
int ObSearchIndexConfigFilter::is_path_indexed(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    bool &passed, const bool index_sub_path) const
{
  int ret = OB_SUCCESS;
  passed = true;
  bool is_terminal = false;
  bool nested_matched = false;

  // NESTED_PATHS has highest priority: force-index nested paths and all their descendants,
  // overriding any EXCLUDE_PATHS rule
  if (has_nested_paths_ && OB_FAIL(nested_matcher_.match(path_items, false, nested_matched, is_terminal))) {
    LOG_WARN("failed to match nested paths", K(ret), K(path_items));
  } else if (nested_matched) {
    // force indexed regardless of include/exclude rules
    passed = true;


  } else {
    // Apply include/exclude rules for paths not covered by NESTED_PATHS.
    if (has_include_paths_) {
      if (OB_FAIL(include_matcher_.match(path_items, false, passed, is_terminal))) {
        LOG_WARN("failed to match include paths", K(ret), K(path_items));
      } else if (is_terminal && index_sub_path) {
        passed = false;
      }
    } else if (has_exclude_paths_) {
      if (OB_FAIL(exclude_matcher_.match(path_items, index_sub_path, passed, is_terminal))) {
        LOG_WARN("failed to match exclude paths", K(ret), K(path_items));
      } else {
        passed = !passed;
      }
    }
    // If neither include nor exclude (only NESTED_PATHS set), index everything.
  }
  return ret;
}

// During data generation, two checks apply:
//   1. Type must match the config.
//   2. Path must match the path in config; subpath relationship is not checked.
int ObSearchIndexConfigFilter::is_indexed(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    const ObJsonNodeType json_type, bool &passed) const
{
  int ret = OB_SUCCESS;
  passed = true;
  if (has_types() && !is_type_indexed(type_mask_, json_type)) {
    passed = false;
  } else if (has_paths() && OB_FAIL(is_path_indexed(path_items, passed))) {
    LOG_WARN("failed to check path", K(ret));
  }
  LOG_DEBUG("check_path_and_type", K(passed), K(has_types()), K(has_paths()), K(json_type));
  return ret;
}

// During query, type and subpath are checked as follows:
// For range queries, index_sub_path is true; both checks apply:
//   1. Pick type must match the config type.
//   2. Path must match the path in config; subpath relationship is checked too.
// For equality queries, index_sub_path is false:
//   1. Pick type is not checked.
//   2. Path must match the path in config; subpath relationship is not checked.
int ObSearchIndexConfigFilter::is_indexed(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    const ObItemType pick_type, bool &passed, const bool index_sub_path) const
{
  int ret = OB_SUCCESS;
  passed = true;
  const ObJsonNodeType json_type = get_json_type(pick_type);
  if (index_sub_path && has_types() && !is_type_indexed(type_mask_, json_type)) {
    passed = false;
  } else if (has_paths() && OB_FAIL(is_path_indexed(path_items, passed, index_sub_path))) {
    LOG_WARN("failed to check path", K(ret));
  }
  LOG_DEBUG("check_path_and_type", K(passed), K(has_types()), K(has_paths()), K(json_type));
  return ret;
}

bool ObSearchIndexConfigFilter::is_type_indexed(const uint8_t type_mask,
                                                const ObJsonNodeType json_type)
{
  bool bret = true;
  if (type_mask == 0) {
  } else if (json_type == ObJsonNodeType::J_STRING) {
    bret = ((type_mask & TYPE_MASK_STRING) != 0);
  } else if (ObIJsonBase::is_json_number_type(json_type)) {
    bret = ((type_mask & TYPE_MASK_NUMBER) != 0);
  } else {
    bret = false;
  }
  return bret;
}

ObJsonNodeType ObSearchIndexConfigFilter::get_json_type(const ObItemType pick_type)
{
  ObJsonNodeType json_type = ObJsonNodeType::J_NULL;
  if (pick_type == T_JSON_STRING) {
    json_type = ObJsonNodeType::J_STRING;
  } else if (pick_type == T_JSON_NUMBER) {
    json_type = ObJsonNodeType::J_DECIMAL;
  } else {
    json_type = ObJsonNodeType::J_NULL;
  }
  return json_type;
}

static int validate_nested_path_item(const ObString &path_str);

int append_search_index_list_items(const ParseNode *node,
                                   ObSqlString &out,
                                   const ObItemType option_type,
                                   ObIAllocator &allocator,
                                   const ObCollationType connection_collation,
                                   common::ObIArray<common::ObString> *collected = nullptr)
{
  int ret = OB_SUCCESS;
  const bool is_path = option_type != T_SEARCH_INDEX_INCLUDE_TYPES;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected search index column params", K(ret));
  } else if (node->type_ == T_LINK_NODE) {
    if (node->num_child_ != 2 || OB_ISNULL(node->children_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected search index column params", K(ret));
    } else if (OB_FAIL(append_search_index_list_items(
        node->children_[0], out, option_type, allocator, connection_collation, collected))) {
      LOG_WARN("failed to append search index list items", K(ret));
    } else if (OB_FAIL(append_search_index_list_items(
        node->children_[1], out, option_type, allocator, connection_collation, collected))) {
      LOG_WARN("failed to append search index list items", K(ret));
    }
  } else {
    ObString path_str(static_cast<int32_t>(node->str_len_), node->str_value_);
    // Binary input has no declared text encoding; preserve its original bytes.
    if (is_path && !path_str.empty()
        && connection_collation != CS_TYPE_BINARY
        && ObCharset::charset_type_by_coll(connection_collation) != CHARSET_UTF8MB4) {
      // Convert before parsing and length-prefixing; never replace unconvertible characters.
      const int64_t buf_len = static_cast<int64_t>(path_str.length()) * 4;
      char *buf = static_cast<char *>(allocator.alloc(buf_len));
      uint32_t converted_len = 0;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate path buffer", K(ret), K(buf_len));
      } else if (OB_FAIL(ObCharset::charset_convert(
          connection_collation, path_str.ptr(), path_str.length(), CS_TYPE_UTF8MB4_BIN,
          buf, buf_len, converted_len, false /* trim_incomplete_tail */, true /* report_error */))) {
        LOG_WARN("failed to convert search index path", K(ret), K(connection_collation));
      } else {
        path_str.assign_ptr(buf, converted_len);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (option_type == T_SEARCH_INDEX_NESTED_PATHS) {
      if (OB_FAIL(validate_nested_path_item(path_str))) {
        LOG_WARN("failed to validate nested path item", K(ret), K(path_str));
      }
    } else if (is_path) {
      ObArenaAllocator tmp_alloc("JsonPathValid");
      void *buf = tmp_alloc.alloc(sizeof(ObJsonPath));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc path", K(ret));
      } else {
        ObJsonPath *json_path = new (buf) ObJsonPath(path_str, &tmp_alloc);
        if (OB_FAIL(json_path->parse_path())) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "search index config");
        } else if (!ObSearchIndexConfigFilter::is_valid_config_path(json_path)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported jsonpath expression", K(ret), K(path_str));
        }
      }
    }
    if (OB_SUCC(ret) && collected != nullptr) {
      OZ (collected->push_back(path_str));
    }
    OZ (out.append_fmt("%d%.*s", path_str.length(), path_str.length(), path_str.ptr()));
  }
  return ret;
}

// Validates that a converted NESTED_PATHS entry contains only member nodes
// (no wildcards, no array notation) and does not exceed MAX_DEPTH segments.
static int validate_nested_path_item(const ObString &path_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc("JsonPathValid");
  void *buf = tmp_alloc.alloc(sizeof(ObJsonPath));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc path buf", K(ret));
  } else {
    ObJsonPath *json_path = new (buf) ObJsonPath(path_str, &tmp_alloc);
    if (OB_FAIL(json_path->parse_path())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "NESTED_PATHS path expression");
    } else {
      int64_t depth = 0;
      for (JsonPathIterator it = json_path->begin();
           OB_SUCC(ret) && it != json_path->end(); ++it) {
        ObJsonPathNode *pnode = *it;
        if (OB_ISNULL(pnode)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (pnode->get_node_type() == JPN_ROOT) {
          continue;
        } else if (pnode->get_node_type() == JPN_MEMBER_WILDCARD) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("wildcard not allowed in NESTED_PATHS", K(ret), K(path_str));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "wildcard in NESTED_PATHS is");
        } else if (pnode->get_node_type() != JPN_MEMBER) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only member paths allowed in NESTED_PATHS", K(ret), K(path_str));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-member node in NESTED_PATHS is");
        } else {
          ++depth;
        }
      }
      if (OB_SUCC(ret) && depth == 0) {
        // Reject a bare root path like '$' (or any path with no member segments).
        // NESTED_PATHS must point to a JSON sub-tree via at least one member step;
        // a root-only path would silently be turned into a wildcard by the matcher
        // and defeat the purpose of NESTED_PATHS.
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("root path is not allowed in NESTED_PATHS", K(ret), K(path_str));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "root path in NESTED_PATHS is");
      } else if (OB_SUCC(ret) && depth > ObSearchIndexNestedKeyEncoder::MAX_DEPTH) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("NESTED_PATHS depth exceeds max", K(ret), K(depth),
                 K(ObSearchIndexNestedKeyEncoder::MAX_DEPTH));
        char depth_buf[64];
        (void)snprintf(depth_buf, sizeof(depth_buf),
                       "NESTED_PATHS depth exceeding %ld is",
                       static_cast<long>(ObSearchIndexNestedKeyEncoder::MAX_DEPTH));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, depth_buf);
      }
    }
  }
  return ret;
}

// Emit a client-visible warning for each duplicated entry in a search-index option
// list (INCLUDE_PATHS / EXCLUDE_PATHS / NESTED_PATHS). Preserves callers' ret so the
// DDL keeps running: duplicates are harmless (runtime matcher trie dedupes), we just
// surface the redundancy. `list_name` is only used in the internal LOG_WARN.
static void warn_duplicate_option_items(const common::ObIArray<common::ObString> &items,
                                        const char *list_name)
{
  for (int64_t i = 1; i < items.count(); ++i) {
    bool is_dup = false;
    for (int64_t j = 0; j < i && !is_dup; ++j) {
      if (0 == items.at(i).compare(items.at(j))) {
        is_dup = true;
      }
    }
    if (is_dup) {
      char buf[common::OB_MAX_ERROR_MSG_LEN] = {0};
      (void)snprintf(buf, sizeof(buf), "%.*s",
                     items.at(i).length(), items.at(i).ptr());
      LOG_USER_WARN(OB_ERR_DUP_ARGUMENT, buf);
      LOG_WARN_RET(OB_SUCCESS, "duplicate search index option entry ignored",
                   K(list_name), K(items.at(i)));
    }
  }
}

int print_length_prefixed_values(const char *data, int64_t len, int64_t &idx,
                                 char *buf, int64_t buf_len, int64_t &pos,
                                 bool with_quotes, ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  bool first = true;
  ObString str;
  str.assign_ptr(data, static_cast<int32_t>(len));
  while (OB_SUCC(ret) && idx < len && isdigit(data[idx])) {
    int64_t value_len = 0;
    if (OB_FAIL(extract_int(str, 0, idx, value_len))) {
      LOG_WARN("failed to extract path length", K(ret));
    } else if (idx + value_len > len) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid config format", K(ret), K(idx), K(value_len), K(len));
    } else if (!first && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
      LOG_WARN("fail to print comma", K(ret));
    } else if (with_quotes) {
      OZ (databuff_printf(buf, buf_len, pos, "'"));
      for (int64_t i = 0; OB_SUCC(ret) && i < value_len; ++i) {
        const char c = data[idx + i];
        // Quote doubling works in both modes; backslashes are SQL escapes only in the default mode.
        if (c == '\'') {
          OZ (databuff_printf(buf, buf_len, pos, "''"));
        } else if (c == '\\' && !(sql_mode & SMO_NO_BACKSLASH_ESCAPES)) {
          OZ (databuff_printf(buf, buf_len, pos, "\\\\"));
        } else {
          OZ (databuff_printf(buf, buf_len, pos, "%c", c));
        }
      }
      OZ (databuff_printf(buf, buf_len, pos, "'"));
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s",
                                  static_cast<int>(value_len), data + idx))) {
        LOG_WARN("fail to print value", K(ret));
      }
    }
    idx += value_len;
    first = false;
  }
  return ret;
}

int ObSearchIndexConfigFilter::parse_filter(const ObString &raw, ParsedFilter &parsed, bool strict)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  parsed.paths_.reset();
  parsed.nested_paths_.reset();
  parsed.has_include_paths_ = false;
  parsed.has_exclude_paths_ = false;
  parsed.has_nested_paths_ = false;
  parsed.type_mask_ = 0;
  parsed.has_include_paths_ = consume_filter_key(raw, pos, "INCLUDE_PATHS=");
  parsed.has_exclude_paths_ = !parsed.has_include_paths_ && consume_filter_key(raw, pos, "EXCLUDE_PATHS=");
  if (parsed.has_include_paths_ || parsed.has_exclude_paths_) {
    if (OB_FAIL(read_filter_values(raw, pos, &parsed.paths_))) {
      LOG_WARN("failed to read search index filter paths", K(ret), K(raw), K(pos));
    } else if (strict && parsed.paths_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("search index filter path list is empty", K(ret), K(raw));
    }
  }
  if (OB_SUCC(ret) && consume_filter_key(raw, pos, "NESTED_PATHS=")) {
    parsed.has_nested_paths_ = true;
    if (OB_FAIL(read_filter_values(raw, pos, &parsed.nested_paths_))) {
      LOG_WARN("failed to read search index nested paths", K(ret), K(raw), K(pos));
    } else if (strict && parsed.nested_paths_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("search index nested path list is empty", K(ret), K(raw));
    }
  }
  if (OB_SUCC(ret) && consume_filter_key(raw, pos, "INCLUDE_TYPES=")) {
    ObSEArray<ObString, 2> types;
    if (OB_FAIL(read_filter_values(raw, pos, &types))) {
      LOG_WARN("failed to read search index filter types", K(ret), K(raw), K(pos));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < types.count(); ++i) {
      if (types.at(i) == "json_string") {
        parsed.type_mask_ |= TYPE_MASK_STRING;
      } else if (types.at(i) == "json_number") {
        parsed.type_mask_ |= TYPE_MASK_NUMBER;
      } else if (strict) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unknown search index filter type", K(ret), "type", types.at(i));
      }
    }
    if (OB_SUCC(ret) && strict && parsed.type_mask_ == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("search index filter type list is empty", K(ret), K(raw));
    }
  }
  if (OB_SUCC(ret) && strict && pos != raw.length()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected trailing search index filter content", K(ret), K(raw), K(pos));
  }
  return ret;
}

int ObSearchIndexConfigFilter::init_from_comment(const ObString &comment, const int64_t mode_idx)
{
  int ret = OB_SUCCESS;
  reset();
  ObSearchIndexConfig config;
  if (!ObSearchIndexConfig::is_versioned(comment)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected a normalized search index comment", K(ret));
  } else if (OB_FAIL(config.decode(comment))) {
    LOG_WARN("failed to decode normalized search index comment", K(ret));
  } else if (mode_idx < 0 || mode_idx >= config.get_modes().count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search index mode offset", K(ret), K(mode_idx), "mode_count", config.get_modes().count());
  } else {
    const ObSearchIndexModeConfig &mode = config.get_modes().at(mode_idx);
    ParsedFilter parsed;
    ParsedFilter nested;
    if (OB_FAIL(parse_filter(mode.filter_, parsed))) {
      LOG_WARN("failed to parse search index filter", K(ret), K(mode_idx));
    } else if (!mode.is_fulltext()
               && OB_FAIL(parse_filter(config.get_nested_paths_filter(), nested))) {
      LOG_WARN("failed to parse column nested paths", K(ret));
    } else {
      has_include_paths_ = parsed.has_include_paths_;
      has_exclude_paths_ = parsed.has_exclude_paths_;
      has_nested_paths_ = nested.has_nested_paths_;
      type_mask_ = mode.is_fulltext() && parsed.type_mask_ == 0 ? TYPE_MASK_STRING : parsed.type_mask_;
      if (has_include_paths_) {
        if (OB_FAIL(include_matcher_.init(parsed.paths_))) {
          LOG_WARN("failed to initialize search index include paths", K(ret));
        }
      } else if (has_exclude_paths_) {
        if (OB_FAIL(exclude_matcher_.init(parsed.paths_))) {
          LOG_WARN("failed to initialize search index exclude paths", K(ret));
        }
      }
      if (OB_SUCC(ret) && has_nested_paths_ && OB_FAIL(nested_matcher_.init(nested.nested_paths_))) {
        LOG_WARN("failed to initialize search index nested paths", K(ret));
      }
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::parse_nested_paths(const ObString &raw,
                                                  ObIArray<ObString> &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  int64_t pos = 0;
  if (consume_filter_key(raw, pos, "INCLUDE_PATHS=")
      || consume_filter_key(raw, pos, "EXCLUDE_PATHS=")) {
    OZ(read_filter_values(raw, pos, nullptr));
  }
  if (OB_SUCC(ret) && consume_filter_key(raw, pos, "NESTED_PATHS=")) {
    OZ(read_filter_values(raw, pos, &out));
  }
  return ret;
}

int ObSearchIndexConfigFilter::print_filter_option(const ParseNode *option,
                                                  ObCollationType connection_collation,
                                                  common::ObSqlString &out)
{
  int ret = OB_SUCCESS;
  out.reset();
  if (OB_ISNULL(option) || option->num_child_ != 1 || OB_ISNULL(option->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid search index filter option", K(ret), KP(option));
  } else {
    const ParseNode *list = option->children_[0];
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "SearchIdxPath"));
    if (option->type_ == T_SEARCH_INDEX_INCLUDE_PATHS) {
      common::ObSEArray<common::ObString, 8> items;
      OZ (out.append("INCLUDE_PATHS="));
      OZ (append_search_index_list_items(list, out, option->type_, allocator, connection_collation, &items));
      if (OB_SUCC(ret)) {
        warn_duplicate_option_items(items, "INCLUDE_PATHS");
      }
    } else if (option->type_ == T_SEARCH_INDEX_EXCLUDE_PATHS) {
      common::ObSEArray<common::ObString, 8> items;
      OZ (out.append("EXCLUDE_PATHS="));
      OZ (append_search_index_list_items(list, out, option->type_, allocator, connection_collation, &items));
      if (OB_SUCC(ret)) {
        warn_duplicate_option_items(items, "EXCLUDE_PATHS");
      }
    } else if (option->type_ == T_SEARCH_INDEX_INCLUDE_TYPES) {
      OZ (out.append("INCLUDE_TYPES="));
      OZ (append_search_index_list_items(list, out, option->type_, allocator, connection_collation));
    } else if (option->type_ == T_SEARCH_INDEX_NESTED_PATHS) {
      common::ObSEArray<common::ObString, 8> items;
      OZ (out.append("NESTED_PATHS="));
      OZ (append_search_index_list_items(list, out, option->type_, allocator, connection_collation, &items));
      if (OB_SUCC(ret)) {
        warn_duplicate_option_items(items, "NESTED_PATHS");
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported search index filter option type", K(ret), K(option->type_));
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::print_schema(const ObSearchIndexModeConfig &mode,
                                            char *buf,
                                            int64_t buf_len,
                                            int64_t &pos,
                                            ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  const char *name = mode.is_fulltext() ? "FULLTEXT" : "VALUE";
  if (mode.filter_.empty() && mode.parser_name_.empty() && mode.parser_properties_.empty()
      && mode.mode_ != ObSearchIndexModeConfig::FULLTEXT_MATCH) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", name))) {
      LOG_WARN("failed to print bare search index mode", K(ret));
    }
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s (", name))) {
      LOG_WARN("failed to print search index mode prefix", K(ret));
    }
    const int64_t start = pos;
    if (OB_SUCC(ret) && OB_FAIL(ObSearchIndexConfigFilter::print_schema(
        mode.filter_, buf, buf_len, pos, sql_mode, !mode.is_fulltext()))) {
      LOG_WARN("failed to print search index mode filter", K(ret));
    }
    if (OB_SUCC(ret) && mode.is_fulltext()) {
      if (!mode.parser_name_.empty()) {
        storage::ObFTParser parser;
        if (OB_FAIL(parser.parse_from_str(mode.parser_name_.ptr(), mode.parser_name_.length()))) {
          LOG_WARN("failed to parse search index parser name for schema printing", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%sPARSER = ", pos > start ? ", " : ""))) {
          LOG_WARN("failed to print search index parser option", K(ret));
        } else if (OB_FAIL(print_quoted(
            ObString(parser.get_parser_name().len(), parser.get_parser_name().str()), buf, buf_len, pos))) {
          LOG_WARN("failed to print search index parser name", K(ret));
        }
      }
      if (OB_SUCC(ret) && !mode.parser_properties_.empty()) {
        if (storage::ObFTParser::is_analyzer_parser_name(mode.parser_name_)) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%sPARSER_PROPERTIES = (analysis = ",
                                     pos > start ? ", " : ""))) {
            LOG_WARN("failed to print search index analyzer properties prefix", K(ret));
          } else if (OB_FAIL(print_quoted(mode.parser_properties_, buf, buf_len, pos))) {
            LOG_WARN("failed to print search index analyzer properties", K(ret));
          } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            LOG_WARN("failed to print search index analyzer properties suffix", K(ret));
          }
        } else {
          storage::ObFTParserJsonProps props;
          if (OB_FAIL(props.init())) {
            LOG_WARN("failed to initialize search index parser properties", K(ret));
          } else if (OB_FAIL(props.parse_from_valid_str(mode.parser_properties_))) {
            LOG_WARN("failed to parse search index parser properties for schema printing", K(ret));
          }
          const int64_t comma = pos;
          if (OB_SUCC(ret) && pos > start && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
            LOG_WARN("failed to print search index parser properties separator", K(ret));
          }
          const int64_t properties_start = pos;
          if (OB_SUCC(ret)) {
            if (OB_FAIL(storage::ObFTParserJsonProps::show_parser_properties(props, buf, buf_len, pos))) {
              LOG_WARN("failed to print search index parser properties", K(ret));
            } else if (pos == properties_start) {
              pos = comma;
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, "%sFTS_INDEX_TYPE = %s",
          pos > start ? ", " : "",
          mode.mode_ == ObSearchIndexModeConfig::FULLTEXT_MATCH ? "MATCH" : "PHRASE_MATCH"))) {
        LOG_WARN("failed to print search index fulltext type", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("failed to print search index mode suffix", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::print_nested_paths(const ObString &nested_paths_filter,
                                                 char *buf,
                                                 int64_t buf_len,
                                                 int64_t &pos,
                                                 ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  if (!nested_paths_filter.empty()) {
    const char *data = nested_paths_filter.ptr();
    const int64_t len = nested_paths_filter.length();
    int64_t idx = 13;
    if (len < idx || 0 != MEMCMP(data, "NESTED_PATHS=", idx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid column nested paths filter", K(ret), K(nested_paths_filter));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "NESTED_PATHS = ("))) {
      LOG_WARN("fail to print NESTED_PATHS key", K(ret));
    } else if (OB_FAIL(print_length_prefixed_values(data, len, idx, buf, buf_len, pos, true, sql_mode))) {
      LOG_WARN("fail to print nested paths value", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("fail to print closing paren for nested paths", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::print_schema(const ObString &comment,
                                            char *buf,
                                            int64_t buf_len,
                                            int64_t &pos,
                                            ObSQLMode sql_mode,
                                            bool print_types,
                                            const ObString &nested_paths_filter)
{
  int ret = OB_SUCCESS;
  if (!comment.empty() || !nested_paths_filter.empty()) {
    const char *data = comment.ptr();
    const int64_t len = comment.length();
    int64_t idx = 0;
    const char *paths_key = NULL;
    if (idx + 14 <= len && 0 == MEMCMP(data + idx, "INCLUDE_PATHS=", 14)) {
      paths_key = "INCLUDE_PATHS";
    } else if (idx + 14 <= len && 0 == MEMCMP(data + idx, "EXCLUDE_PATHS=", 14)) {
      paths_key = "EXCLUDE_PATHS";
    }

    bool section_printed = false;
    if (paths_key != NULL) {
      idx += 14;
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s = (", paths_key))) {
        LOG_WARN("fail to print paths key", K(ret));
      } else if (OB_FAIL(print_length_prefixed_values(data, len, idx, buf, buf_len, pos, true, sql_mode))) {
        LOG_WARN("fail to print paths value", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("fail to print closing paren", K(ret));
      } else {
        section_printed = true;
      }
    }

    if (OB_SUCC(ret) && !nested_paths_filter.empty()) {
      if (section_printed && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        LOG_WARN("failed to print column nested paths separator", K(ret));
      } else if (OB_FAIL(print_nested_paths(nested_paths_filter, buf, buf_len, pos, sql_mode))) {
        LOG_WARN("failed to print column nested paths", K(ret));
      } else {
        section_printed = true;
      }
    }

    // INCLUDE_TYPES is independent and always comes last if present.
    if (OB_SUCC(ret) && print_types && idx + 14 <= len
        && 0 == MEMCMP(data + idx, "INCLUDE_TYPES=", 14)) {
      idx += 14;
      if (section_printed && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        LOG_WARN("fail to print comma", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "INCLUDE_TYPES = ("))) {
        LOG_WARN("fail to print INCLUDE_TYPES", K(ret));
      } else if (OB_FAIL(print_length_prefixed_values(data, len, idx, buf, buf_len, pos, false, sql_mode))) {
        LOG_WARN("fail to print types value", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
        LOG_WARN("fail to print closing paren", K(ret));
      }
    }
  }
  return ret;
}

int ObSearchIndexJsonPathMatcher::match_exact_nested(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    bool &matched) const
{
  int ret = OB_SUCCESS;
  matched = false;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(root_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized or root is null", K(ret));
  } else {
    const Node *cur = root_;
    bool found = true;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < path_items.count() && found; ++idx) {
      const ObSearchIndexPathEncoder::JsonPathItem &item = path_items.at(idx);
      if (item.is_array_path()) {
        // An array marker means the prefix path (cur) is itself an array in the data.
        // We may only descend into (skip) this array when the prefix path is a declared
        // nested terminal. Otherwise a declared path like $.a.b must NOT match data shaped
        // like $.a[].b: that requires $.a to be declared nested as well (node_a terminal).
        if (!cur->terminal_) {
          found = false;
        }
        continue;
      }
      Node *child = nullptr;
      if (OB_SUCCESS == (cur->children_.get_refactored("*", child))) {
        cur = child;  // wildcard match
      } else if (OB_FAIL(cur->children_.get_refactored(item.path, child))) {
        if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          found = false;
        } else {
          LOG_WARN("failed to get child", K(ret), K(item.path));
        }
      } else {
        cur = child;
      }
    }
    if (OB_SUCC(ret) && found) {
      matched = cur->terminal_;
    }
  }
  return ret;
}

int ObSearchIndexConfigFilter::is_nested_path(
    const ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    bool &matched) const
{
  int ret = OB_SUCCESS;
  matched = false;
  if (has_nested_paths_) {
    if (OB_FAIL(nested_matcher_.match_exact_nested(path_items, matched))) {
      LOG_WARN("failed to match exact nested path", K(ret));
    }
  }
  return ret;
}

// Returns true if the path is valid for search index config: each node must be a member (key),
// and the last node may be a member wildcard (*).
// Example: "$.a.b.c" is valid; "$.a.b.*" is valid (wildcard only at the last segment).
bool ObSearchIndexConfigFilter::is_valid_config_path(common::ObJsonPath *json_path)
{
  bool ret_bool = true;
  int i = 0;
  int total_nodes = json_path->path_node_cnt();

  JsonPathIterator it = json_path->begin();
  for (; i < total_nodes && ret_bool == true && it != json_path->end(); ++i, ++it) {
    ObJsonPathNode *node = *it;
    if (OB_ISNULL(node) || node->get_node_type() != JPN_MEMBER) {
      ret_bool = false;
      break;
    }
  }

  if (ret_bool == false && i == total_nodes - 1 && it != json_path->end()) {
    ObJsonPathNode *node = *it;
    if (OB_NOT_NULL(node) && node->get_node_type() == JPN_MEMBER_WILDCARD) {
      ret_bool = true;
    }
  }
  
  return ret_bool;
}

} // namespace share
} // namespace oceanbase
