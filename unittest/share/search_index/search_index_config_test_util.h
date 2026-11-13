/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SHARE_SEARCH_INDEX_CONFIG_TEST_UTIL_H_
#define OCEANBASE_UNITTEST_SHARE_SEARCH_INDEX_CONFIG_TEST_UTIL_H_

#include "share/search_index/ob_search_index_config.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace unittest
{

// Extend test configurations through the production API without retaining views across init().
inline int append_search_index_mode(share::ObSearchIndexConfig &config,
                                    const share::ObSearchIndexModeConfig &mode)
{
  int ret = common::OB_SUCCESS;
  common::ObSEArray<share::ObSearchIndexModeConfig, 4> modes;
  if (OB_FAIL(modes.assign(config.get_modes()))) {
    SHARE_LOG(WARN, "failed to copy test configuration modes", K(ret));
  } else if (OB_FAIL(modes.push_back(mode))) {
    SHARE_LOG(WARN, "failed to append test configuration mode", K(ret));
  } else if (OB_FAIL(config.init(modes, config.get_nested_paths_filter()))) {
    SHARE_LOG(WARN, "failed to initialize test configuration", K(ret));
  }
  return ret;
}

// Build version 1 input for decoder/generator tests; validation is performed by production code.
inline int build_search_index_config_comment(const share::ObSearchIndexConfig &config,
                                             common::ObSqlString &comment)
{
  int ret = common::OB_SUCCESS;
  comment.reset();
  if (OB_FAIL(comment.append_fmt("SEARCH_INDEX_CONFIG=1:%ld:%d:", config.get_modes().count(),
                                 config.get_nested_paths_filter().length()))) {
    SHARE_LOG(WARN, "failed to append test configuration header", K(ret));
  } else if (OB_FAIL(comment.append(config.get_nested_paths_filter()))) {
    SHARE_LOG(WARN, "failed to append test column nested paths", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < config.get_modes().count(); ++i) {
    const share::ObSearchIndexModeConfig &mode = config.get_modes().at(i);
    if (OB_FAIL(comment.append_fmt("%d:", mode.mode_))) {
      SHARE_LOG(WARN, "failed to append test configuration mode", K(ret), K(i));
    }
    const common::ObString fields[] = {mode.filter_, mode.parser_name_, mode.parser_properties_};
    for (int64_t j = 0; OB_SUCC(ret) && j < 3; ++j) {
      if (OB_FAIL(comment.append_fmt("%d:", fields[j].length()))) {
        SHARE_LOG(WARN, "failed to append test configuration field length", K(ret), K(i), K(j));
      } else if (OB_FAIL(comment.append(fields[j]))) {
        SHARE_LOG(WARN, "failed to append test configuration field", K(ret), K(i), K(j));
      }
    }
  }
  return ret;
}

} // namespace unittest
} // namespace oceanbase
#endif
