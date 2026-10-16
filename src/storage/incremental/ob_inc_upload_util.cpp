/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_inc_upload_util.h"
#include "ob_copy_ss_macro_writer.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase {
namespace storage {

ERRSIM_POINT_DEF(SS_INC_UPLOAD_PARALLEL_FACTOR)

const char *ObUploadRangeCheckResult::get_str(const ObUploadRangeCheckResult &type)
{
  const char *str = "UNKNOWN";
  switch (type.type_) {
    case RANGE_MATCH:
      str = "RANGE_MATCH";
      break;
    case RANGE_CROSS:
      str = "RANGE_CROSS";
      break;
    case RANGE_GAP:
      str = "RANGE_GAP";
      break;
    case INVALID_RESULT:
      str = "INVALID_RESULT";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

int ObIncUploadUtil::classify_upload_range(
    const ObITable::TableType table_type,
    const share::SCN &start_scn,
    const ObTablet &ss_tablet,
    ObUploadRangeCheckResult &range_check_result)
{
  int ret = OB_SUCCESS;
  range_check_result = ObUploadRangeCheckResult::RANGE_MATCH;
  // tx_ctx/lock tablets only do mini merge then flush, no need to do replace upload
  if (ObITable::is_minor_sstable(table_type)
   && !ss_tablet.get_tablet_id().is_only_mini_merge_tablet()) {
    const share::SCN clog_checkpoint_scn = ss_tablet.get_clog_checkpoint_scn();
    if (start_scn == clog_checkpoint_scn) {
      range_check_result = ObUploadRangeCheckResult::RANGE_MATCH;
    } else if (start_scn < clog_checkpoint_scn) {
      range_check_result = ObUploadRangeCheckResult::RANGE_CROSS;
    } else {
      range_check_result = ObUploadRangeCheckResult::RANGE_GAP;
    }
  }
  return ret;
}

int ObIncUploadUtil::calc_upload_parallelism(
    const blocksstable::ObSSTable &sstable,
    int64_t &parallelism)
{
  int ret = OB_SUCCESS;
  const int64_t data_macro_block_count = sstable.get_data_macro_block_count();
  if (data_macro_block_count >= UINT32_MAX) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("data macro block count is too large", K(ret), K(data_macro_block_count), K(sstable));
  } else {
    int64_t default_per_thread_macro_count = 10;
    if (SS_INC_UPLOAD_PARALLEL_FACTOR) {
      default_per_thread_macro_count = -(SS_INC_UPLOAD_PARALLEL_FACTOR);
    }
    parallelism = MIN(128, MAX(1, data_macro_block_count / default_per_thread_macro_count));
  }
  return ret;
}

int ObIncUploadUtil::process_private_block_sstable(
    const share::ObLSID &ls_id,
    const ObITable::TableKey &table_key,
    ObTablet &tablet,
    blocksstable::ObSSTable &sstable,
    const ObStorageSchema &schema,
    const int64_t parallelism,
    const int64_t op_id,
    const uint64_t data_version,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &result_handle,
    const bool is_upload_minor)
{
  int ret = OB_SUCCESS;
  CopySSMacroBlockWriter *writer = NULL;
  bool writer_init_succ = false;
  writer = OB_NEWx(CopySSMacroBlockWriter, &allocator,
                   ls_id, table_key, tablet, sstable, schema,
                   parallelism, op_id, data_version,
                   is_upload_minor);
  if (OB_ISNULL(writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate copy ss macro block writer fail", K(ret));
  } else if (OB_FAIL(writer->init())) {
    LOG_WARN("init copy ss macro block writer fail", K(ret));
  } else {
    writer_init_succ = true;
  }

  blocksstable::ObSSTableSecMetaIterator *meta_iter = NULL;
  ObDatumRange range;
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(meta_iter = OB_NEWx(blocksstable::ObSSTableSecMetaIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate meta iterator fail", K(ret));
    } else {
      range.set_whole_range();
      const storage::ObITableReadInfo *index_read_info = NULL;
      if (OB_FAIL(tablet.get_sstable_read_info(&sstable, index_read_info))) {
        LOG_WARN("get index read info fail", K(ret), K(sstable));
      } else if (OB_FAIL(meta_iter->open(range, blocksstable::DATA_BLOCK_META,
                                         sstable, *index_read_info, allocator, false))) {
        LOG_WARN("open sstable meta iterator fail", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObDataMacroBlockMeta macro_meta;
    do {
      if (OB_FAIL(meta_iter->get_next(macro_meta))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("iter macro block fail", K(ret));
        }
      }
      if (FAILEDx(writer->process(macro_meta))) {
        LOG_WARN("process macro block fail", K(ret));
      }
    } while (OB_SUCC(ret));
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
  }

  if (meta_iter) {
    meta_iter->~ObSSTableSecMetaIterator();
    allocator.free(meta_iter);
    meta_iter = NULL;
  }
  if (writer_init_succ) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(writer->wait())) {
      LOG_WARN("wait async write finish fail", K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (FAILEDx(writer->close(allocator))) {
      LOG_WARN("close writer fail", K(ret));
    }
    result_handle = writer->get_result_sstable_handle();
  }
  if (writer) {
    writer->~CopySSMacroBlockWriter();
    allocator.free(writer);
    writer = NULL;
  }
  LOG_INFO("process private block sstable finish", K(ret), K(ls_id), K(table_key));
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
