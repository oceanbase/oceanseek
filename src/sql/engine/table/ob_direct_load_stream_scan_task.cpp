/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_direct_load_stream_scan_task.h"

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "share/table/ob_direct_load_stream_queue_list.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
namespace
{
int validate_direct_load_stream_target_schema_(
    const uint64_t tenant_id,
    const observer::ObDirectLoadStreamMeta &meta,
    const int64_t target_schema_version)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || meta.tenant_id_ != tenant_id
                  || OB_INVALID_ID == meta.table_id_
                  || target_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream target schema validation argument", K(ret),
             K(tenant_id), K(target_schema_version), K(meta));
  } else if (OB_UNLIKELY(meta.target_schema_version_ != target_schema_version)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("direct load stream target schema version mismatch", K(ret),
             K(target_schema_version), K(meta));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get current tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, meta.table_id_, table_schema))) {
    LOG_WARN("failed to get direct load stream target table schema", K(ret), K(meta));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("direct load stream target table does not exist", K(ret), K(meta));
  } else if (OB_UNLIKELY(table_schema->get_schema_version() != target_schema_version)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("direct load stream target schema version mismatch at runtime", K(ret),
             "current_schema_version", table_schema->get_schema_version(),
             K(target_schema_version), K(meta));
  }
  return ret;
}
}

int validate_direct_load_stream_target_schema(const uint64_t tenant_id,
                                              const int64_t stream_id,
                                              const int64_t target_schema_version)
{
  int ret = OB_SUCCESS;
  observer::ObDirectLoadStreamMeta meta;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !observer::is_valid_stream_id(stream_id)
                  || target_schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream target schema validation argument", K(ret),
             K(tenant_id), K(stream_id), K(target_schema_version));
  } else if (OB_FAIL(observer::ObDirectLoadStreamService::get_direct_load_stream(
                         stream_id, meta))) {
    LOG_WARN("failed to get direct load stream meta", K(ret), K(stream_id));
  } else if (OB_UNLIKELY(meta.stream_id_ != stream_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream meta identity mismatch", K(ret), K(stream_id), K(meta));
  } else if (OB_FAIL(validate_direct_load_stream_target_schema_(
                         tenant_id, meta, target_schema_version))) {
    LOG_WARN("failed to validate direct load stream target schema", K(ret), K(stream_id),
             K(target_schema_version), K(meta));
  }
  return ret;
}

int prepare_direct_load_stream_scan_task(const uint64_t tenant_id,
                                         const ObDASScanCtDef &das_ctdef,
                                         ObIAllocator &allocator,
                                         ObIArray<ObIExtTblScanTask *> &scan_tasks)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("DLSScanTask");
  ObExternalFileFormat format;
  observer::ObDirectLoadStreamMeta meta;
  ObSEArray<table::ObDirectLoadStreamQueueDesc, 16> queue_descs;
  ObSEArray<ObIExtTblScanTask *, 16> tmp_scan_tasks;
  scan_tasks.reset();
  if (OB_FAIL(format.load_from_string(das_ctdef.external_file_format_str_.str_, tmp_allocator))) {
    LOG_WARN("failed to parse direct load stream file format", K(ret),
             K(das_ctdef.external_file_format_str_.str_));
  } else if (OB_UNLIKELY(ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT != format.format_type_
                         || !format.direct_load_stream_format_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid direct load stream file format", K(ret), K(format));
  } else if (OB_FAIL(observer::ObDirectLoadStreamService::get_direct_load_stream(
                     format.direct_load_stream_format_.stream_id_, meta))) {
    LOG_WARN("failed to get direct load stream meta", K(ret),
             K(format.direct_load_stream_format_.stream_id_));
  } else if (OB_UNLIKELY(meta.tenant_id_ != tenant_id
                         || meta.stream_id_ != format.direct_load_stream_format_.stream_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("direct load stream meta identity mismatch", K(ret), K(tenant_id), K(meta),
             "format_stream_id", format.direct_load_stream_format_.stream_id_);
  } else if (OB_FAIL(validate_direct_load_stream_target_schema_(
                         tenant_id, meta, meta.target_schema_version_))) {
    LOG_WARN("failed to validate direct load stream target schema", K(ret), K(meta));
  } else if (OB_FAIL(table::ObDirectLoadStreamQueueListCodec::expand(
                         meta.queue_entries_, queue_descs))) {
    LOG_WARN("failed to expand direct load stream queue topology", K(ret), K(meta));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_descs.count(); ++i) {
      const table::ObDirectLoadStreamQueueDesc &queue_desc = queue_descs.at(i);
      ObDirectLoadStreamScanTask *scan_task = NULL;
      if (queue_desc.server_addr_ != GCTX.self_addr()) {
        // 每个 Stream SQC 只发布其执行 Server 持有的 queue。
      } else if (OB_UNLIKELY(!queue_desc.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expanded direct load stream queue is invalid", K(ret), K(i), K(queue_desc));
      } else if (OB_ISNULL(scan_task = OB_NEWx(ObDirectLoadStreamScanTask, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate direct load stream scan task", K(ret), K(i));
      } else {
        scan_task->stream_id_ = meta.stream_id_;
        scan_task->queue_id_ = queue_desc.queue_id_;
        scan_task->server_addr_ = queue_desc.server_addr_;
        scan_task->target_schema_version_ = meta.target_schema_version_;
        if (OB_FAIL(tmp_scan_tasks.push_back(scan_task))) {
          LOG_WARN("failed to stage direct load stream scan task", K(ret), K(i), KPC(scan_task));
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(tmp_scan_tasks.empty())) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("direct load stream topology has no queue for current server", K(ret),
               "self_addr", GCTX.self_addr(), K(meta));
    } else if (OB_SUCC(ret) && OB_FAIL(scan_tasks.assign(tmp_scan_tasks))) {
      LOG_WARN("failed to publish direct load stream scan tasks", K(ret),
               K(tmp_scan_tasks.count()));
    }
  }
  if (OB_FAIL(ret)) {
    scan_tasks.reset();
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
