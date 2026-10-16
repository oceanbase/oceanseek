/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_FILTER_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_FILTER_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h" // ObArenaAllocator
#include "lib/hash/ob_hashmap.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/json_type/ob_json_base.h" // common::ObJsonNodeType
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "ob_search_index_encoder.h"
#include "sql/parser/parse_node.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace share
{

class ObSearchIndexJsonPathMatcher final
{
public:
  explicit ObSearchIndexJsonPathMatcher(const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  ~ObSearchIndexJsonPathMatcher();

  void reset();

  // Initialize from a single string spec.
  int init(const ObString &spec, int64_t &idx);

  // Segment-aware prefix match. Returns true if any include path is a prefix of target_path.
  int match(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
            const bool index_sub_path, bool &matched, bool &is_terminal) const;

  // Exact nested-path match. Returns matched=true only if path_items exactly equals a declared
  // terminal path, where each array marker may be crossed only when its prefix path is itself a
  // declared nested terminal (so $.a.b matches $.a.b but not $.a[].b unless $.a is also declared).
  int match_exact_nested(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                         bool &matched) const;


private:
  struct Node final
  {
    Node() : terminal_(false), children_(), next_all_(nullptr) {}
    bool terminal_;
    common::hash::ObHashMap<ObString, Node*> children_;
    Node *next_all_;
  };

private:
  int new_node(Node *&node);
  int get_or_create_child(Node &node, const ObString &seg, Node *&child);
  int insert_path(const ObString &path);
  int parse_and_insert(const ObString &spec, int64_t &pos);

private:
  uint64_t tenant_id_;
  mutable ObArenaAllocator allocator_;
  bool inited_;
  Node *root_;
  Node *all_nodes_;

  DISALLOW_COPY_AND_ASSIGN(ObSearchIndexJsonPathMatcher);
};

// ============================================================================
// ObSearchIndexConfigFilter - Column-level filter for JSON search index
// ============================================================================

class ObSearchIndexConfigFilter
{
public:
  static const uint8_t TYPE_MASK_STRING = (1 << 0);
  static const uint8_t TYPE_MASK_NUMBER = (1 << 1);

  explicit ObSearchIndexConfigFilter(const uint64_t tenant_id);

  // Initialize from a single string comment.
  int init_from_comment(const ObString &raw);
  void reset();

  // Combine path + type checks for JSON scalar nodes.
  int is_indexed(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                 const common::ObJsonNodeType json_type, bool &passed) const;
  int is_indexed(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                 const ObItemType pick_type, bool &passed, const bool index_sub_path) const;
  static bool is_type_indexed(const uint8_t type_mask, const common::ObJsonNodeType json_type);

  // Build search index configuration string from parse node
  static int print_comment(const ParseNode *param_node, common::ObSqlString &out);
  // Print search index column config (INCLUDE_PATHS/EXCLUDE_PATHS/INCLUDE_TYPES) to buf for SHOW CREATE TABLE.
  static int print_schema(const common::ObString &comment, char *buf, int64_t buf_len, int64_t &pos);
  static bool is_valid_config_path(common::ObJsonPath *json_path);
  bool has_types() const { return type_mask_ != 0; }
  uint8_t get_type_mask() const { return type_mask_; }
  bool has_nested_paths() const { return has_nested_paths_; }

  // Returns matched=true if path_items (ignoring array markers) exactly equals a declared NESTED_PATH.
  // Used by Generator to decide whether to push/pop the nested_key stack when entering a J_ARRAY.
  int is_nested_path(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                     bool &matched) const;

  // Parse the NESTED_PATHS=... section of a search index column comment into the flat list of
  // declared nested path strings (e.g. ["$.comments", "$.comments.replies"]). out entries point
  // into the `comment` buffer, so the caller must keep `comment` alive or deep-copy them. The
  // matcher trie is the runtime path of choice; this is for callers (e.g. the hybrid search DSL
  // resolver) that need to enumerate the declared paths rather than match a single one.
  static int parse_nested_paths(const common::ObString &comment,
                                common::ObIArray<common::ObString> &out);
  TO_STRING_KV(K_(has_include_paths), K_(has_exclude_paths), K_(has_nested_paths), K_(type_mask));
private:
  bool has_paths() const { return has_include_paths_ || has_exclude_paths_ || has_nested_paths_; }
  static common::ObJsonNodeType get_json_type(const ObItemType pick_type);
  int is_path_indexed(const common::ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                      bool &passed, const bool index_sub_path = false) const;

private:
  bool has_include_paths_;
  bool has_exclude_paths_;
  bool has_nested_paths_;
  uint8_t type_mask_;
  ObSearchIndexJsonPathMatcher include_matcher_;
  ObSearchIndexJsonPathMatcher exclude_matcher_;
  ObSearchIndexJsonPathMatcher nested_matcher_;
};

}
}

#endif // OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_FILTER_H_
