/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_INDEX_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_INDEX_

#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_range.h"
namespace oceanbase
{
namespace common
{
class ObString;
class ObObj;
}
namespace share
{
namespace schema
{
class ObTableSchema;
class ObDatabaseSchema;
}
}
namespace observer
{
class ObTableIndex : public common::ObVirtualTableScannerIterator
{
public:
  ObTableIndex();
  virtual ~ObTableIndex();
  virtual int inner_open();
  virtual int inner_close();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int init(uint64_t tenant_id);
private:
  void reset_table_state_();
  int get_current_table_schema_(const uint64_t table_id,
                                const share::schema::ObTableSchema *&table_schema);
  int get_current_index_schema_(const uint64_t tenant_id,
                                const uint64_t index_id,
                                const share::schema::ObTableSchema *&index_schema);
  int get_container_table_schema_(const share::schema::ObTableSchema &table_schema,
                                  const share::schema::ObTableSchema *&container_schema);
  int add_table_indexes(const share::schema::ObTableSchema &table_schema,
                        const common::ObString &database_name,
                        common::ObObj *cells,
                        int64_t col_count,
                        bool &is_end);
  int add_database_indexes(const share::schema::ObDatabaseSchema &database_schema,
                           common::ObObj *cells,
                           int64_t col_count,
                           bool &is_end);

  int add_rowkey_indexes(const share::schema::ObTableSchema &table_schema,
                         const common::ObString &database_name,
                         common::ObObj *cells,
                         int64_t col_count,
                         bool &is_end);
  int get_rowkey_index_column(const ObTableSchema &table_schema,
                              const ObColumnSchemaV2 *&column_schema,
                              bool &is_column_visible,
                              bool &is_end);
  int add_normal_indexes(const share::schema::ObTableSchema &table_schema,
                         const common::ObString &database_name,
                         common::ObObj *cells,
                         int64_t col_count,
                         bool &is_end);
  int get_normal_index_column(const ObTableSchema &table_schema,
                              const ObTableSchema *index_schema,
                              const ObColumnSchemaV2 *&column_schema,
                              bool &is_column_visible,
                              bool &is_end);
  int add_normal_index_column(const common::ObString &database_name,
                              const share::schema::ObTableSchema &table_schema,
                              const share::schema::ObTableSchema *index_schema,
                              common::ObObj *cells,
                              int64_t col_count,
                              bool &is_end);
  int add_fulltext_index_column(const common::ObString &database_name,
                                const share::schema::ObTableSchema &table_schema,
                                const share::schema::ObTableSchema *index_schema,
                                common::ObObj *cells,
                                int64_t col_count,
                                const uint64_t column_id);
  int add_async_index_column(const common::ObString &database_name,
                             const share::schema::ObTableSchema &table_schema,
                             const share::schema::ObTableSchema *index_schema,
                             common::ObObj *cells,
                             int64_t col_count,
                             bool &is_end);
  int add_vec_index_column(const common::ObString &database_name,
                           const share::schema::ObTableSchema &table_schema,
                           const share::schema::ObTableSchema *index_schema,
                           common::ObObj *cells,
                           int64_t col_count,
                           const uint64_t column_id);
  int get_show_column_name(const share::schema::ObTableSchema &table_schema,
                           const share::schema::ObColumnSchemaV2 &column_schema,
                           common::ObString &column_name);
private:
  uint64_t tenant_id_;
  uint64_t show_table_id_;
  common::ObSArray<const share::schema::ObDatabaseSchema *> database_schemas_;
  int64_t database_schema_idx_;
  common::ObSArray<uint64_t> table_ids_;
  int64_t table_schema_idx_;
  int64_t rowkey_info_idx_;
  int64_t index_tid_array_idx_;
  int64_t index_column_idx_;
  common::ObSArray<share::schema::ObAuxTableMetaInfo> simple_index_infos_;
  bool is_rowkey_end_;
  bool is_normal_end_;
  int64_t ft_dep_col_idx_;
  int64_t async_dep_col_idx_; // for async index, async fts / async search index etc.
  uint64_t min_data_version_;
  int64_t vec_dep_col_idx_;
  // The scan guard owns enumeration/database/virtual-table metadata. Only this
  // guard pins the current table and its index/container/auxiliary schemas.
  share::schema::ObSchemaGetterGuard table_guard_;
  const share::schema::ObTableSchema *current_table_schema_;
  const share::schema::ObTableSchema *current_index_schema_;
  const share::schema::ObTableSchema *container_table_schema_;
  bool is_iter_end_;
  DISALLOW_COPY_AND_ASSIGN(ObTableIndex);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_INDEX_ */
