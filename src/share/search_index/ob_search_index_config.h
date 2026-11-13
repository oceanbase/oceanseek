/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_CONFIG_H_

#include "common/object/ob_object.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string_holder.h"
#include "share/schema/ob_schema_struct_fts.h"

namespace oceanbase
{
namespace share
{

struct ObSearchIndexModeConfig
{
  // Persisted directly in version 1 configuration comments.
  enum Mode { VALUE = 0, FULLTEXT_MATCH = 1, FULLTEXT_PHRASE = 2 };
  ObSearchIndexModeConfig()
    : mode_(VALUE), filter_(), parser_name_(), parser_properties_() {}

  bool is_fulltext() const { return mode_ == FULLTEXT_MATCH || mode_ == FULLTEXT_PHRASE; }
  int encode(common::ObSqlString &comment) const;
  // Path/type filter encoding; NESTED_PATHS is stored separately at column scope.
  // FULLTEXT implicitly selects JSON_STRING when no type filter is stored.
  Mode mode_;
  common::ObString filter_;
  // New FULLTEXT configurations persist the parser version and effective properties.
  // FULLTEXT requires a nonempty parser name; empty properties select fixed defaults.
  common::ObString parser_name_;
  common::ObString parser_properties_;
  TO_STRING_KV(K_(mode), K_(filter), K_(parser_name), K_(parser_properties));
};

// Stored in Search Def index-column comments and transported by ObColumnSortItem::column_comment_.
// Owns one complete v1 comment; all string fields below are views into that comment.
// Callers may release the input after init/decode. Views expire on init/decode/reset.
// Legacy comments decode to the same VALUE block and column NESTED fields as versioned comments.
// Legacy comments without a leading configuration key use the default VALUE configuration.
// In-memory encoding emits version 1; persisted/transported comments use the tenant data version.
class ObSearchIndexConfig final
{
public:
  ObSearchIndexConfig() : comment_(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "SearchIdxCfg")),
                          modes_(), nested_paths_filter_() {}
  static bool supports_fulltext(const common::ObObjMeta &meta);
  void reset();
  // Build the complete comment before binding views; semantic checks remain in validate().
  int init(const common::ObIArray<ObSearchIndexModeConfig> &modes,
           const common::ObString &nested_paths_filter = common::ObString());
  common::ObString get_comment() const { return comment_.get_ob_string(); }
  const common::ObIArray<ObSearchIndexModeConfig> &get_modes() const { return modes_; }
  // Check for VALUE without producing a mode comment; legacy input is normalized by decode.
  static int has_value_mode(const common::ObString &comment, bool &has_value);
  // Extract a complete v1 comment containing only VALUE and column NESTED_PATHS.
  // Legacy input is normalized by decode. has_value distinguishes an absent VALUE mode.
  static int get_value_mode_comment(const common::ObString &comment,
                                    common::ObSqlString &value_comment,
                                    bool &has_value);
  // Read column NESTED paths from either format. Returned strings borrow comment.
  // No VALUE block is required.
  static int get_nested_paths(const common::ObString &comment,
                              common::ObIArray<common::ObString> &paths);
  // Column-level declaration, encoded as a standalone legacy NESTED_PATHS section.
  const common::ObString &get_nested_paths_filter() const { return nested_paths_filter_; }
  static bool is_versioned(const common::ObString &comment);
  static int encode_header(int64_t mode_count, const common::ObString &nested_paths_filter,
                           common::ObSqlString &comment);
  int encode(common::ObSqlString &comment) const;
  // Before 4.6.3.1 only a single VALUE mode can be written, in the legacy format.
  int encode(common::ObSqlString &comment, const uint64_t tenant_data_version) const;
  // Decode persisted configuration with format checks only; DDL validates its semantics.
  // Legacy empty sections are omitted; INCLUDE_TYPES is sorted and deduplicated in v1.
  int decode(const common::ObString &comment);
  // Compare persisted comments directly when both use the same format.
  // For mixed formats, decode only the legacy side; decode failures leave equal false.
  static int is_equal(const common::ObString &left_comment,
                      const common::ObString &right_comment,
                      bool &equal);
  int validate() const;
  // Validate a complete v1 comment using borrowed fields, without copying the comment.
  static int validate(const common::ObString &comment);
  // Print v1 fields borrowed from comment; legacy comments retain compatibility normalization.
  static int print_schema(const common::ObString &comment, char *buf, int64_t buf_len, int64_t &pos,
                          ObSQLMode sql_mode = SMO_DEFAULT);
  TO_STRING_KV(K_(modes), K_(nested_paths_filter));

private:
  common::ObStringHolder comment_;
  common::ObSEArray<ObSearchIndexModeConfig, 4> modes_;
  common::ObString nested_paths_filter_;
  DISALLOW_COPY_AND_ASSIGN(ObSearchIndexConfig);
};

} // namespace share
} // namespace oceanbase
#endif
