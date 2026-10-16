/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INC_UPLOAD_UTIL_H_
#define OCEANBASE_STORAGE_INC_UPLOAD_UTIL_H_

#include "share/scn.h"
#include "storage/ob_i_table.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_define.h"

namespace oceanbase {
namespace blocksstable { class ObSSTable; }
namespace storage {

class ObTablet;
class ObStorageSchema;

// result of comparing the to-be-uploaded sstable's start_scn against the shared
// tablet's clog_checkpoint_scn (the uploaded right boundary):
//   RANGE_MATCH: start_scn == checkpoint, the new sstable is contiguous, do inc upload
//   RANGE_CROSS: start_scn <  checkpoint, ranges overlap, do replace upload
//   RANGE_GAP  : start_scn >  checkpoint, an scn gap exists below the new sstable,
//                do inc upload but fill the gap with an empty mini sstable
struct ObUploadRangeCheckResult
{
  enum TYPE
  {
    INVALID_RESULT = 0,
    RANGE_MATCH = 1,
    RANGE_CROSS = 2,
    RANGE_GAP = 3,
    MAX,
  };
  ObUploadRangeCheckResult() : type_(INVALID_RESULT) {}
  explicit ObUploadRangeCheckResult(const TYPE type) : type_(type) {}
  ObUploadRangeCheckResult &operator=(const TYPE type) { type_ = type; return *this; }
  operator TYPE() const { return type_; }
  bool operator==(const TYPE type) const { return type_ == type; }
  bool operator!=(const TYPE type) const { return type_ != type; }
  bool is_valid() const { return type_ > INVALID_RESULT && type_ < MAX; }
  static const char *get_str(const ObUploadRangeCheckResult &type);
  TO_STRING_KV("val", static_cast<int64_t>(type_), "str", get_str(*this));
  TYPE type_;
};

class ObIncUploadUtil {
public:
  static int classify_upload_range(
      const ObITable::TableType table_type,
      const share::SCN &start_scn,
      const ObTablet &ss_tablet,
      ObUploadRangeCheckResult &range_check_result);

  static int calc_upload_parallelism(
      const blocksstable::ObSSTable &sstable,
      int64_t &parallelism);

  static int process_private_block_sstable(
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
      const bool is_upload_minor = false);

};

}  // namespace storage
}  // namespace oceanbase

#endif
