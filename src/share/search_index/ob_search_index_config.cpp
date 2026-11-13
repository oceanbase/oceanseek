/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "ob_search_index_config.h"
#include "ob_search_index_config_filter.h"
#include "ob_search_index_encoder.h"
#include "lib/json_type/ob_json_parse.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "share/ob_define.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace schema;

const char CONFIG_PREFIX[] = "SEARCH_INDEX_CONFIG=";
const int64_t MAX_CONFIG_VERSION = 1;

bool ObSearchIndexConfig::supports_fulltext(const ObObjMeta &meta)
{
  return meta.is_json()
      || ((ob_is_varchar_or_char(meta.get_type(), meta.get_collation_type())
           || ob_is_text_tc(meta.get_type()))
          && ObSearchIndexValueEncoder::is_supported_search_index_collation(meta.get_collation_type()));
}

static bool starts_with(const ObString &str, const char *prefix)
{
  const int64_t len = strlen(prefix);
  return str.length() >= len && 0 == MEMCMP(str.ptr(), prefix, len);
}

// Numbers and lengths have an explicit delimiter; unlike the legacy path format,
// arbitrary parser-property strings (including digits and separators) are unambiguous.
static int read_number(const ObString &str, int64_t &pos, int64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  const int64_t begin = pos;
  while (OB_SUCC(ret) && pos < str.length() && str.ptr()[pos] >= '0' && str.ptr()[pos] <= '9') {
    const int64_t digit = str.ptr()[pos++] - '0';
    if (value > (INT64_MAX - digit) / 10) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("search index configuration number overflows", K(ret), K(value), K(digit), K(pos));
    } else {
      value = value * 10 + digit;
    }
  }
  if (OB_SUCC(ret) && (pos == begin || pos == str.length() || str.ptr()[pos++] != ':')) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search index configuration number format", K(ret), K(begin), K(pos),
        "str_length", str.length());
  }
  return ret;
}

static int read_string(const ObString &str, int64_t &pos, ObString &value)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_FAIL(read_number(str, pos, len))) {
    LOG_WARN("failed to read configuration string length", K(ret), K(pos));
  } else if (len > str.length() - pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search index configuration string exceeds remaining input", K(ret), K(len), K(pos),
        "str_length", str.length());
  } else {
    value.assign_ptr(str.ptr() + pos, static_cast<int32_t>(len));
    pos += len;
  }
  return ret;
}

static int write_string(ObSqlString &out, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out.append_fmt("%d:", value.length()))) {
    LOG_WARN("failed to write configuration string length", K(ret));
  } else if (OB_FAIL(out.append(value))) {
    LOG_WARN("failed to write configuration string", K(ret));
  }
  return ret;
}

struct PathPattern
{
  PathPattern() : members_(), wildcard_(false) {}
  int assign(const PathPattern &other)
  {
    wildcard_ = other.wildcard_;
    return members_.assign(other.members_);
  }
  ObSEArray<ObString, 4> members_;
  bool wildcard_;
  TO_STRING_KV(K_(members), K_(wildcard));
};

struct PathSet
{
  PathSet() : paths_(), nested_paths_(), exclude_(false), indexes_root_(true), type_mask_(0) {}
  int assign(const PathSet &other)
  {
    exclude_ = other.exclude_;
    indexes_root_ = other.indexes_root_;
    type_mask_ = other.type_mask_;
    int ret = OB_SUCCESS;
    if (OB_FAIL(paths_.assign(other.paths_))) {
      LOG_WARN("failed to copy search index paths", K(ret));
    } else if (OB_FAIL(nested_paths_.assign(other.nested_paths_))) {
      LOG_WARN("failed to copy search index nested paths", K(ret));
    }
    return ret;
  }
  ObSEArray<PathPattern, 4> paths_;
  ObSEArray<PathPattern, 4> nested_paths_;
  bool exclude_;
  bool indexes_root_;
  uint8_t type_mask_;
  TO_STRING_KV(K_(paths), K_(exclude), K_(indexes_root), K_(type_mask));
};

static int read_path(const ObString &str, ObIAllocator &allocator, PathPattern &pattern)
{
  int ret = OB_SUCCESS;
  ObJsonPath path(str, &allocator);
  if (OB_FAIL(path.parse_path())) {
    LOG_WARN("invalid search index path", K(ret), K(str));
  } else if (!ObSearchIndexConfigFilter::is_valid_config_path(&path)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported search index configuration path", K(ret), K(str));
  } else {
    pattern.wildcard_ = path.path_node_cnt() == 0;
    for (JsonPathIterator it = path.begin(); OB_SUCC(ret) && it != path.end(); ++it) {
      if ((*it)->get_node_type() == JPN_MEMBER_WILDCARD) {
        pattern.wildcard_ = true;
      } else {
        const ObPathMember &member = static_cast<ObJsonPathBasicNode *>(*it)->get_object();
        // Parsed member names belong to allocator and outlive this local path.
        ObString key(member.len_, member.object_name_);
        // The current VALUE matcher uses "*" as its wildcard trie key.
        if (key == "*") {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("literal wildcard member is not supported in search index mode paths", K(ret), K(str));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "literal '*' member in search index mode paths is");
        } else if (OB_FAIL(pattern.members_.push_back(key))) {
          LOG_WARN("failed to add search index path member", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

static int read_filter(const ObString &filter, ObIAllocator &allocator, PathSet &set)
{
  int ret = OB_SUCCESS;
  ObSearchIndexConfigFilter::ParsedFilter parsed;
  if (OB_FAIL(ObSearchIndexConfigFilter::parse_filter(filter, parsed))) {
    LOG_WARN("failed to parse search index filter", K(ret), K(filter));
  }
  set.exclude_ = parsed.has_exclude_paths_;
  // Explicit INCLUDE never matches the empty path of a root JSON string.
  set.indexes_root_ = !parsed.has_include_paths_;
  set.type_mask_ = parsed.type_mask_;
  for (int64_t i = 0; OB_SUCC(ret) && i < parsed.paths_.count(); ++i) {
    PathPattern pattern;
    if (OB_FAIL(read_path(parsed.paths_.at(i), allocator, pattern))) {
      LOG_WARN("failed to read search index filter path", K(ret), K(i));
    } else if (OB_FAIL(set.paths_.push_back(pattern))) {
      LOG_WARN("failed to add search index filter path", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && !parsed.has_include_paths_ && !parsed.has_exclude_paths_) {
    // No path option means the entire path space.
    PathPattern all;
    all.wildcard_ = true;
    if (OB_FAIL(set.paths_.push_back(all))) {
      LOG_WARN("failed to add search index wildcard path", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < parsed.nested_paths_.count(); ++i) {
    PathPattern pattern;
    if (OB_FAIL(read_path(parsed.nested_paths_.at(i), allocator, pattern))) {
      LOG_WARN("failed to read search index nested path", K(ret), K(i));
    } else if (pattern.wildcard_) {
      // Nested declarations identify an array member, not a wildcard subtree.
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("wildcard is not supported in search index nested paths", K(ret), K(i),
          "path", parsed.nested_paths_.at(i));
    } else if (OB_FAIL(set.nested_paths_.push_back(pattern))) {
      LOG_WARN("failed to add search index nested path", K(ret), K(i));
    }
  }
  return ret;
}

static bool prefix_equal(const PathPattern &a, const PathPattern &b)
{
  bool equal = true;
  for (int64_t i = 0; equal && i < MIN(a.members_.count(), b.members_.count()); ++i) {
    equal = a.members_.at(i) == b.members_.at(i);
  }
  return equal;
}

static bool covers(const PathPattern &a, const PathPattern &b)
{
  return prefix_equal(a, b) && (a.wildcard_
      ? (a.members_.empty() || a.members_.count() < b.members_.count()
          || (b.wildcard_ && a.members_.count() == b.members_.count()))
      : (!b.wildcard_ && a.members_.count() == b.members_.count()));
}

static bool intersects(const PathPattern &a, const PathPattern &b)
{
  // At the same prefix, an exact rule and prefix.* both select array elements.
  return prefix_equal(a, b) && (a.members_.count() == b.members_.count()
      || (a.members_.count() < b.members_.count() ? a.wildcard_ : b.wildcard_));
}

static bool base_overlaps(const PathSet &a, const PathSet &b)
{
  bool overlap = false;
  if (a.indexes_root_ && b.indexes_root_) {
    overlap = true;
  } else if (!a.exclude_ && !b.exclude_) {
    for (int64_t i = 0; !overlap && i < a.paths_.count(); ++i) {
      for (int64_t j = 0; !overlap && j < b.paths_.count(); ++j) {
        overlap = intersects(a.paths_.at(i), b.paths_.at(j));
      }
    }
  } else {
    const PathSet &inc = a.exclude_ ? b : a;
    const PathSet &exc = a.exclude_ ? a : b;
    for (int64_t i = 0; !overlap && i < inc.paths_.count(); ++i) {
      bool excluded = false;
      for (int64_t j = 0; !excluded && j < exc.paths_.count(); ++j) {
        excluded = covers(exc.paths_.at(j), inc.paths_.at(i));
      }
      overlap = !excluded;
    }
  }
  return overlap;
}

static int decode_comment_v1(const ObString &comment,
                             int64_t pos,
                             ObIArray<ObSearchIndexModeConfig> &modes,
                             ObString &nested)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_FAIL(read_number(comment, pos, count))) {
    LOG_WARN("failed to decode search index mode count", K(ret), K(pos));
  } else if (OB_FAIL(read_string(comment, pos, nested))) {
    LOG_WARN("failed to decode column nested paths", K(ret), K(pos));
  } else if (count <= 0 || count > (comment.length() - pos) / 8) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid search index mode count", K(ret), K(count), K(pos), "comment_length", comment.length());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ObSearchIndexModeConfig mode;
      int64_t mode_id = 0;
      if (OB_FAIL(read_number(comment, pos, mode_id))) {
        LOG_WARN("failed to decode search index mode", K(ret), K(i), K(pos));
      } else if (mode_id > ObSearchIndexModeConfig::FULLTEXT_PHRASE) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid search index mode", K(ret), K(mode_id));
      } else {
        mode.mode_ = static_cast<ObSearchIndexModeConfig::Mode>(mode_id);
        if (OB_FAIL(read_string(comment, pos, mode.filter_))) {
          LOG_WARN("failed to decode search index mode filter", K(ret), K(i), K(pos));
        } else if (OB_FAIL(read_string(comment, pos, mode.parser_name_))) {
          LOG_WARN("failed to decode search index parser name", K(ret), K(i), K(pos));
        } else if (OB_FAIL(read_string(comment, pos, mode.parser_properties_))) {
          LOG_WARN("failed to decode search index parser properties", K(ret), K(i), K(pos));
        } else if (OB_FAIL(modes.push_back(mode))) {
          LOG_WARN("failed to add decoded search index mode", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && pos != comment.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected trailing search index configuration data", K(ret), K(pos),
          "comment_length", comment.length());
    }
  }
  return ret;
}

static int decode_versioned(const ObString &comment, ObIArray<ObSearchIndexModeConfig> &modes, ObString &nested)
{
  int ret = OB_SUCCESS;
  int64_t pos = strlen(CONFIG_PREFIX);
  int64_t version = 0;
  if (!starts_with(comment, CONFIG_PREFIX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected versioned search index comment", K(ret));
  } else if (OB_FAIL(read_number(comment, pos, version))) {
    LOG_WARN("failed to decode search index configuration version", K(ret), K(pos));
  } else if (version == 1) {
    if (OB_FAIL(decode_comment_v1(comment, pos, modes, nested))) {
      LOG_WARN("failed to decode v1 search index configuration", K(ret));
    }
  } else {
    // Legacy comments have no version prefix; an explicit version 0 is unsupported.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported search index configuration version", K(ret), K(version),
            "max_supported_version", MAX_CONFIG_VERSION);
  }
  return ret;
}

static int validate_modes(const ObIArray<ObSearchIndexModeConfig> &modes, const ObString &nested_paths_filter)
{
  int ret = OB_SUCCESS;
  if (modes.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("search index configuration has no modes", K(ret));
  } else {
    ObArenaAllocator allocator("SearchIdxPath");
    ObSEArray<PathSet, 4> sets; // Only FULLTEXT modes participate in path overlap checks.
    ObSEArray<ObPluginName, 4> parser_names;
    bool has_value = false;
    if (!nested_paths_filter.empty()) {
      ObSearchIndexConfigFilter::ParsedFilter parsed;
      PathSet nested;
      if (OB_FAIL(ObSearchIndexConfigFilter::parse_filter(nested_paths_filter, parsed))) {
        LOG_WARN("failed to parse column nested paths filter", K(ret));
      } else if (!parsed.has_nested_paths_ || parsed.has_include_paths_ ||
                 parsed.has_exclude_paths_ || parsed.type_mask_ != 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid column nested paths filter", K(ret), K(nested_paths_filter));
      } else if (OB_FAIL(read_filter(nested_paths_filter, allocator, nested))) {
        LOG_WARN("failed to read column nested paths filter", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < modes.count(); ++i) {
      const ObSearchIndexModeConfig &mode = modes.at(i);
      PathSet set;
      if (OB_FAIL(read_filter(mode.filter_, allocator, set))) {
        LOG_WARN("failed to read search index mode filter", K(ret), K(i));
      } else if (!set.nested_paths_.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("nested paths are not supported inside a search index mode block", K(ret), K(i), K(mode));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "NESTED_PATHS inside a search index mode block is");
      } else if ((mode.mode_ < ObSearchIndexModeConfig::VALUE
          || mode.mode_ > ObSearchIndexModeConfig::FULLTEXT_PHRASE
          || (mode.is_fulltext() && set.type_mask_ != 0
              && set.type_mask_ != ObSearchIndexConfigFilter::TYPE_MASK_STRING)
          || (!mode.is_fulltext() && (!mode.parser_name_.empty() || !mode.parser_properties_.empty())))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid search index mode configuration", K(ret), K(i), K(mode), K(set));
      } else if (mode.is_fulltext() && !nested_paths_filter.empty()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                       "NESTED_PATHS together with FULLTEXT on the same search index column is");
      } else if (mode.mode_ == ObSearchIndexModeConfig::VALUE) {
        if (has_value) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("multiple VALUE blocks in search index column configuration", K(ret), K(i));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "multiple VALUE blocks for a search index column");
        } else {
          has_value = true;
        }
      } else {
        storage::ObFTParser parser;
        ObPluginName parser_name;
        ObJsonNode *properties = nullptr;
        if (mode.parser_name_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("empty search index fulltext parser name", K(ret), K(i));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "Search FULLTEXT PARSER must not be empty");
        } else if (OB_FAIL(parser.parse_from_str(mode.parser_name_.ptr(), mode.parser_name_.length()))) {
          LOG_WARN("failed to parse search index parser name", K(ret), K(i));
        } else {
          parser_name = parser.get_parser_name();
        }
        if (OB_SUCC(ret) && !mode.parser_properties_.empty()
            && OB_FAIL(ObJsonParser::get_tree(&allocator, mode.parser_properties_, properties))) {
          LOG_WARN("failed to parse search index parser properties", K(ret), K(i));
        }
        if (OB_FAIL(ret)) {
        } else if (storage::ObFTParser::is_analyzer_parser_name(mode.parser_name_)
                   && mode.parser_properties_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("analyzer requires explicit analysis properties", K(ret), K(i));
        } else if (properties != nullptr && properties->json_type() != ObJsonNodeType::J_OBJECT) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("search index parser properties must be a JSON object", K(ret), K(i), K(mode));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < parser_names.count(); ++j) {
            if (parser_name == parser_names.at(j)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("duplicate parser in search index FULLTEXT blocks", K(ret), K(i), K(j), K(mode));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate PARSER in FULLTEXT blocks for a search index column");
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(parser_names.push_back(parser_name))) {
            LOG_WARN("failed to add search index parser name", K(ret), K(i));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < sets.count(); ++j) {
            if (base_overlaps(set, sets.at(j))) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("overlapping paths in search index FULLTEXT blocks", K(ret), K(i), K(j), K(set),
                  "other_set", sets.at(j));
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "overlapping paths in FULLTEXT blocks for a search index column");
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(sets.push_back(set))) {
            LOG_WARN("failed to add search index mode path set", K(ret), K(i));
          }
        }
      }
    }
  }
  return ret;
}

static int print_modes_schema(const ObIArray<ObSearchIndexModeConfig> &modes,
                              const ObString &nested_paths_filter,
                              char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const bool single_value = modes.count() == 1
                            && modes.at(0).mode_ == ObSearchIndexModeConfig::VALUE;
  if (nested_paths_filter.empty() && single_value
      && modes.at(0).filter_.empty() && modes.at(0).parser_name_.empty()
      && modes.at(0).parser_properties_.empty()) {
    // A single default VALUE needs no column-level WITH clause.
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH ("))) {
      LOG_WARN("failed to print search index configuration prefix", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (single_value) {
      if (OB_FAIL(ObSearchIndexConfigFilter::print_schema(modes.at(0).filter_, buf, buf_len,
                                                          pos, true, nested_paths_filter))) {
        LOG_WARN("failed to print single VALUE search index configuration", K(ret));
      }
    } else {
      if (!nested_paths_filter.empty()) {
        if (OB_FAIL(ObSearchIndexConfigFilter::print_nested_paths(nested_paths_filter,
                                                                  buf, buf_len, pos))) {
          LOG_WARN("failed to print column nested paths", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
          LOG_WARN("failed to print column nested paths separator", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < modes.count(); ++i) {
        if (i > 0 && OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
          LOG_WARN("failed to print search index mode separator", K(ret), K(i));
        } else if (OB_FAIL(ObSearchIndexConfigFilter::print_schema(modes.at(i), buf, buf_len, pos))) {
          LOG_WARN("failed to print search index mode", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
      LOG_WARN("failed to print search index configuration suffix", K(ret));
    }
  }
  return ret;
}

void ObSearchIndexConfig::reset()
{
  modes_.reset();
  nested_paths_filter_.reset();
  comment_.reset();
}

int ObSearchIndexConfig::has_value_mode(const ObString &comment, bool &has_value)
{
  int ret = OB_SUCCESS;
  has_value = false;
  ObSearchIndexConfig config;
  if (OB_FAIL(config.decode(comment))) {
    LOG_WARN("failed to decode search index configuration", K(ret));
  } else {
    const ObIArray<ObSearchIndexModeConfig> &modes = config.get_modes();
    for (int64_t i = 0; !has_value && i < modes.count(); ++i) {
      has_value = modes.at(i).mode_ == ObSearchIndexModeConfig::VALUE;
    }
  }
  return ret;
}

int ObSearchIndexConfig::get_value_mode_comment(const ObString &comment,
                                               ObSqlString &value_comment,
                                               bool &has_value)
{
  int ret = OB_SUCCESS;
  value_comment.reset();
  has_value = false;
  ObSearchIndexConfig config;
  if (OB_FAIL(config.decode(comment))) {
    LOG_WARN("failed to decode search index configuration", K(ret));
  } else {
    const ObIArray<ObSearchIndexModeConfig> &modes = config.get_modes();
    for (int64_t i = 0; OB_SUCC(ret) && !has_value && i < modes.count(); ++i) {
      if (modes.at(i).mode_ == ObSearchIndexModeConfig::VALUE) {
        if (OB_FAIL(encode_header(1, config.get_nested_paths_filter(), value_comment))) {
          LOG_WARN("failed to encode search index VALUE header", K(ret));
        } else if (OB_FAIL(modes.at(i).encode(value_comment))) {
          LOG_WARN("failed to encode search index VALUE mode", K(ret));
        } else {
          has_value = true;
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexConfig::get_nested_paths(const ObString &comment, ObIArray<ObString> &paths)
{
  int ret = OB_SUCCESS;
  paths.reset();
  if (!is_versioned(comment)) {
    if (OB_FAIL(ObSearchIndexConfigFilter::parse_nested_paths(comment, paths))) {
      LOG_WARN("failed to parse legacy search index nested paths", K(ret));
    }
  } else {
    ObSEArray<ObSearchIndexModeConfig, 4> modes;
    ObString nested;
    if (OB_FAIL(decode_versioned(comment, modes, nested))) {
      LOG_WARN("failed to read search index column nested paths", K(ret));
    } else if (OB_FAIL(ObSearchIndexConfigFilter::parse_nested_paths(nested, paths))) {
      LOG_WARN("failed to parse search index column nested paths", K(ret));
    }
  }
  return ret;
}

bool ObSearchIndexConfig::is_versioned(const ObString &comment)
{
  return starts_with(comment, CONFIG_PREFIX);
}

int ObSearchIndexModeConfig::encode(ObSqlString &comment) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(comment.append_fmt("%d:", mode_))) {
    LOG_WARN("failed to encode search index mode", K(ret), K(mode_));
  } else if (OB_FAIL(write_string(comment, filter_))) {
    LOG_WARN("failed to encode search index mode filter", K(ret));
  } else if (OB_FAIL(write_string(comment, parser_name_))) {
    LOG_WARN("failed to encode search index parser name", K(ret));
  } else if (OB_FAIL(write_string(comment, parser_properties_))) {
    LOG_WARN("failed to encode search index parser properties", K(ret));
  }
  return ret;
}

int ObSearchIndexConfig::encode_header(int64_t mode_count, const ObString &nested_paths_filter,
                                       ObSqlString &comment)
{
  int ret = OB_SUCCESS;
  comment.reset();
  if (OB_FAIL(comment.append_fmt("%s%ld:%ld:", CONFIG_PREFIX, MAX_CONFIG_VERSION, mode_count))) {
    LOG_WARN("failed to encode search index configuration header", K(ret));
  } else if (OB_FAIL(write_string(comment, nested_paths_filter))) {
    LOG_WARN("failed to encode column nested paths", K(ret));
  }
  return ret;
}

int ObSearchIndexConfig::init(const ObIArray<ObSearchIndexModeConfig> &modes,
                             const ObString &nested_paths_filter)
{
  int ret = OB_SUCCESS;
  ObSqlString comment;
  if (OB_FAIL(encode_header(modes.count(), nested_paths_filter, comment))) {
    LOG_WARN("failed to encode search index configuration header", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < modes.count(); ++i) {
    if (OB_FAIL(modes.at(i).encode(comment))) {
      LOG_WARN("failed to encode search index mode", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(decode(comment.string()))) {
    LOG_WARN("failed to initialize search index configuration", K(ret));
  }
  return ret;
}

int ObSearchIndexConfig::encode(ObSqlString &comment) const
{
  int ret = OB_SUCCESS;
  comment.reset();
  if (OB_FAIL(validate())) {
    LOG_WARN("invalid search index configuration", K(ret));
  } else if (OB_FAIL(comment.append(get_comment()))) {
    LOG_WARN("failed to copy search index configuration", K(ret));
  }
  return ret;
}

int ObSearchIndexConfig::encode(ObSqlString &comment, const uint64_t tenant_data_version) const
{
  int ret = OB_SUCCESS;
  comment.reset();
  if (tenant_data_version >= DATA_VERSION_4_6_3_1) {
    if (OB_FAIL(encode(comment))) {
      LOG_WARN("failed to encode search index configuration", K(ret));
    }
  } else if (OB_FAIL(validate())) {
    LOG_WARN("invalid search index configuration", K(ret));
  } else if (modes_.count() != 1 || modes_.at(0).mode_ != ObSearchIndexModeConfig::VALUE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("JSON FULLTEXT search index not supported before 4.6.3.1", K(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "JSON FULLTEXT search index before version 4.6.3.1");
  } else {
    const ObString &filter = modes_.at(0).filter_;
    ObSearchIndexConfigFilter::ParsedFilter parsed;
    if (OB_FAIL(ObSearchIndexConfigFilter::parse_filter(filter, parsed))) {
      LOG_WARN("failed to parse VALUE filter for legacy encoding", K(ret));
    } else {
      // Preserve the legacy order: INCLUDE/EXCLUDE_PATHS, NESTED_PATHS, INCLUDE_TYPES.
      int64_t paths_end = 0;
      if (!parsed.paths_.empty()) {
        const ObString &last_path = parsed.paths_.at(parsed.paths_.count() - 1);
        paths_end = last_path.ptr() + last_path.length() - filter.ptr();
      }
      if (paths_end > 0 && OB_FAIL(comment.append(filter.ptr(), paths_end))) {
        LOG_WARN("failed to encode legacy search index paths", K(ret));
      } else if (OB_FAIL(comment.append(nested_paths_filter_))) {
        LOG_WARN("failed to encode legacy search index nested paths", K(ret));
      } else if (filter.length() > paths_end
                 && OB_FAIL(comment.append(filter.ptr() + paths_end, filter.length() - paths_end))) {
        LOG_WARN("failed to encode legacy search index types", K(ret));
      }
    }
  }
  return ret;
}

int ObSearchIndexConfig::decode(const ObString &comment)
{
  int ret = OB_SUCCESS;
  if (!is_versioned(comment)) {
    ObSearchIndexConfigFilter::ParsedFilter parsed;
    ObString paths;
    ObString nested;
    ObSqlString types;
    ObSqlString versioned_comment;
    if (OB_FAIL(ObSearchIndexConfigFilter::parse_filter(comment, parsed, false /* strict */))) {
      LOG_WARN("failed to parse legacy search index configuration", K(ret));
    } else {
      // Legacy sections are ordered as paths, NESTED_PATHS, then INCLUDE_TYPES.
      // Borrow path sections, and normalize types to match the SQL resolver's v1 output.
      if (!parsed.paths_.empty()) {
        const ObString &last_path = parsed.paths_.at(parsed.paths_.count() - 1);
        const int64_t end = last_path.ptr() + last_path.length() - comment.ptr();
        paths.assign_ptr(comment.ptr(), static_cast<int32_t>(end));
      }
      if (!parsed.nested_paths_.empty()) {
        const ObString &last_path = parsed.nested_paths_.at(parsed.nested_paths_.count() - 1);
        // Skip the preceding path section even when its list was empty and omitted above.
        int64_t start = 0;
        if (parsed.has_include_paths_ || parsed.has_exclude_paths_) {
          start = parsed.paths_.empty() ? strlen("INCLUDE_PATHS=") : paths.length();
        }
        const int64_t end = last_path.ptr() + last_path.length() - comment.ptr();
        nested.assign_ptr(comment.ptr() + start, static_cast<int32_t>(end - start));
      }
      if (parsed.type_mask_ != 0) {
        if (OB_FAIL(types.append("INCLUDE_TYPES="))) {
          LOG_WARN("failed to append legacy search index type key", K(ret));
        } else if ((parsed.type_mask_ & ObSearchIndexConfigFilter::TYPE_MASK_STRING) != 0
                   && OB_FAIL(types.append("11json_string"))) {
          LOG_WARN("failed to append legacy search index string type", K(ret));
        } else if ((parsed.type_mask_ & ObSearchIndexConfigFilter::TYPE_MASK_NUMBER) != 0
                   && OB_FAIL(types.append("11json_number"))) {
          LOG_WARN("failed to append legacy search index number type", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(encode_header(1, nested, versioned_comment))) {
        LOG_WARN("failed to encode legacy search index configuration header", K(ret));
      } else if (OB_FAIL(versioned_comment.append_fmt("%d:%ld:", ObSearchIndexModeConfig::VALUE,
                                                     static_cast<int64_t>(paths.length()) + types.length()))) {
        LOG_WARN("failed to encode legacy search index VALUE filter length", K(ret));
      } else if (OB_FAIL(versioned_comment.append(paths))) {
        LOG_WARN("failed to append legacy search index paths", K(ret));
      } else if (OB_FAIL(versioned_comment.append(types.string()))) {
        LOG_WARN("failed to append legacy search index types", K(ret));
      } else if (OB_FAIL(versioned_comment.append("0:0:"))) {
        LOG_WARN("failed to append empty search index parser fields", K(ret));
      } else if (OB_FAIL(decode(versioned_comment.string()))) {
        LOG_WARN("failed to decode converted search index configuration", K(ret));
      }
    }
  } else {
    ObStringHolder owned_comment(lib::ObMemAttr(OB_SERVER_TENANT_ID, "SearchIdxCfg"));
    if (OB_FAIL(owned_comment.assign(comment))) {
      LOG_WARN("failed to copy search index comment", K(ret));
    } else {
      reset();
      comment_.move_from(meta::ObMover<ObStringHolder>(owned_comment));
    }
    if (OB_SUCC(ret) && OB_FAIL(decode_versioned(get_comment(), modes_, nested_paths_filter_))) {
      LOG_WARN("failed to decode owned search index comment", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexConfig::is_equal(const ObString &left_comment,
                                 const ObString &right_comment,
                                 bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  const bool left_versioned = is_versioned(left_comment);
  const bool right_versioned = is_versioned(right_comment);
  if (left_versioned == right_versioned) {
    equal = left_comment == right_comment;
  } else {
    const ObString &legacy_comment = left_versioned ? right_comment : left_comment;
    const ObString &versioned_comment = left_versioned ? left_comment : right_comment;
    ObSearchIndexConfig legacy;
    if (OB_FAIL(legacy.decode(legacy_comment))) {
      LOG_WARN("failed to decode legacy search index configuration", K(ret));
    } else {
      equal = legacy.get_comment() == versioned_comment;
    }
  }
  return ret;
}

int ObSearchIndexConfig::validate() const
{
  return validate_modes(modes_, nested_paths_filter_);
}

int ObSearchIndexConfig::validate(const ObString &comment)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSearchIndexModeConfig, 4> modes;
  ObString nested;
  if (OB_FAIL(decode_versioned(comment, modes, nested))) {
    LOG_WARN("failed to parse search index comment for validation", K(ret));
  } else if (OB_FAIL(validate_modes(modes, nested))) {
    LOG_WARN("invalid search index configuration", K(ret));
  }
  return ret;
}

int ObSearchIndexConfig::print_schema(const ObString &comment, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (comment.empty()) {
    // Preserve the original empty-comment output.
  } else if (!is_versioned(comment)) {
    ObSearchIndexConfig config;
    if (OB_FAIL(config.decode(comment))) {
      LOG_WARN("failed to decode legacy search index configuration for printing", K(ret));
    } else if (OB_FAIL(print_modes_schema(config.get_modes(), config.get_nested_paths_filter(),
                                          buf, buf_len, pos))) {
      LOG_WARN("failed to print legacy search index configuration", K(ret));
    }
  } else {
    ObSEArray<ObSearchIndexModeConfig, 4> modes;
    ObString nested;
    if (OB_FAIL(decode_versioned(comment, modes, nested))) {
      LOG_WARN("failed to decode search index comment for printing", K(ret));
    } else if (OB_FAIL(print_modes_schema(modes, nested, buf, buf_len, pos))) {
      LOG_WARN("failed to print search index configuration", K(ret));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
