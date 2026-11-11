/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_SCHEMA_CTX_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_SCHEMA_CTX_H_

#include "lib/allocator/ob_allocator.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/async_index/ob_async_index_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace storage
{

// Per-tablet data-plane context for the IVF build pipeline. The common
// async-index DDL task owns locking, transaction, attach/link/commit and
// published advancement; this context resolves source/index schema metadata
// for training and row materialization. Input rows come from the DDL pipeline.
struct ObIVFBuildParam final
{
  ObIVFBuildParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      data_table_id_(OB_INVALID_ID),
      index_table_id_(OB_INVALID_ID),
      index_tablet_id_(),
      schema_version_(0)
  {}
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && OB_INVALID_ID != data_table_id_
        && OB_INVALID_ID != index_table_id_
        && index_tablet_id_.is_valid()
        && schema_version_ > 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(data_table_id), K_(index_table_id),
               K_(index_tablet_id), K_(schema_version));

  uint64_t tenant_id_;
  uint64_t data_table_id_;
  uint64_t index_table_id_;
  common::ObTabletID index_tablet_id_;
  int64_t schema_version_;
};

class ObIVFBuildSchemaCtx final
{
public:
  explicit ObIVFBuildSchemaCtx(common::ObIAllocator &allocator);
  int init(const ObIVFBuildParam &param);

  const share::ObVectorIndexParam &get_vector_param() const
  { return vector_param_; }
  const common::ObString &get_index_params() const { return index_params_; }
  int64_t get_vector_dim() const { return vector_dim_; }
  int64_t get_index_column_count() const { return index_column_count_; }
  int64_t get_index_rowkey_column_count() const
  { return index_rowkey_column_count_; }

private:
  int validate_target_columns_(
      const share::schema::ObTableSchema *data_schema,
      const share::schema::ObTableSchema *index_schema);

private:
  bool is_inited_;
  common::ObIAllocator &allocator_;
  share::ObVectorIndexParam vector_param_;
  common::ObString index_params_;
  int64_t vector_dim_;
  int64_t index_column_count_;
  int64_t index_rowkey_column_count_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFBuildSchemaCtx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_BUILD_SCHEMA_CTX_H_
