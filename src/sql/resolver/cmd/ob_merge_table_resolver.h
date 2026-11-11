/*
 * Copyright (c) 2025 OceanBase.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_SQL_RESOLVER_CMD_OB_MERGE_TABLE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_CMD_OB_MERGE_TABLE_RESOLVER_H_

#include "sql/resolver/ob_stmt_resolver.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{
class ObMergeTableStmt;

class ObMergeTableResolver : public ObStmtResolver
{
public:
  explicit ObMergeTableResolver(ObResolverParams &params)
    : ObStmtResolver(params)
  {}
  virtual ~ObMergeTableResolver() {}
  virtual int resolve(const ParseNode &parse_tree);

private:
  static const int64_t INCOMING_TABLE_NODE = 0;
  static const int64_t CURRENT_TABLE_NODE = 1;
  static const int64_t STRATEGY_NODE = 2;
  static const int64_t HINT_NODE = 3;
  static const int64_t MERGE_TABLE_NODE_COUNT = 4;

  // Backquoted, NUL-terminated column names, ready to be printed into SQL.
  struct MergeColumns
  {
    common::ObSEArray<common::ObString, 8> pk_;      // primary key columns
    common::ObSEArray<common::ObString, 16> val_;    // non-key columns, compared
    common::ObSEArray<common::ObString, 16> insert_; // non-generated columns, written
    common::ObSEArray<common::ObString, 16> update_; // non-generated non-key columns
  };

  int resolve_table_names_and_strategy_(const ParseNode &parse_tree, ObMergeTableStmt &stmt);
  int get_table_schemas_(const ObMergeTableStmt &stmt,
                         const share::schema::ObTableSchema *&cur_schema,
                         const share::schema::ObTableSchema *&inc_schema);
  int collect_columns_(const share::schema::ObTableSchema &cur_schema,
                       const share::schema::ObTableSchema &inc_schema,
                       MergeColumns &cols);
  int build_merge_sql_(ObMergeTableStmt &stmt, const MergeColumns &cols, const ParseNode *hint_node);

  DISALLOW_COPY_AND_ASSIGN(ObMergeTableResolver);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_CMD_OB_MERGE_TABLE_RESOLVER_H_ */
