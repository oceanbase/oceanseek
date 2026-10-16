/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/virtual_table/ob_all_virtual_shared_gc_diagnose.h"
#include "observer/ob_server_struct.h"
#include "lib/charset/ob_charset.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/config/ob_server_config.h"
#include "src/storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace observer
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::storage;

static const ObMarkSweepGroupType GROUPS[] = {
  ObMarkSweepGroupType::DEL,
  ObMarkSweepGroupType::KEPT
};
static const ObSSMarkSweepDiagFileType TYPES[] = {
  ObSSMarkSweepDiagFileType::MACROS,
  ObSSMarkSweepDiagFileType::TABLET_META_SCNS,
  ObSSMarkSweepDiagFileType::STUDY_RESULTS
};
static const int64_t GROUP_NUM = ARRAYSIZEOF(GROUPS);
static const int64_t TYPE_NUM = ARRAYSIZEOF(TYPES);

ObAllVirtualSharedGCDiagnose::ObAllVirtualSharedGCDiagnose()
  : ObVirtualTableScannerIterator(),
    batch_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "GcDiagBatch")),
    record_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "GcDiagRec"))
{
  reset_state_();
}

ObAllVirtualSharedGCDiagnose::~ObAllVirtualSharedGCDiagnose()
{
  reset();
}

void ObAllVirtualSharedGCDiagnose::reset()
{
  reset_state_();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualSharedGCDiagnose::reset_state_()
{
  predicates_extracted_ = false;
  round_id_ = -1;
  target_tenant_id_ = OB_INVALID_TENANT_ID;
  has_group_filter_ = false;
  group_filter_ = ObMarkSweepGroupType::DEL;
  has_type_filter_ = false;
  type_filter_ = ObSSMarkSweepDiagFileType::MACROS;
  has_ls_id_filter_ = false;
  ls_id_filter_ = 0;
  has_tablet_id_filter_ = false;
  tablet_id_filter_ = 0;
  has_reorg_scn_filter_ = false;
  reorg_scn_filter_ = 0;
  group_idx_ = 0;
  type_idx_ = 0;
  buckets_.reset();
  bucket_idx_ = 0;
  is_buckets_listed_ = false;
  reader_.reset();
  is_reader_opened_ = false;
  cur_records_.reset();
  record_idx_ = 0;
  is_cur_record_decoded_ = false;
  cur_macros_.reset();
  cur_meta_scns_.reset();
  cur_study_results_.reset();
  item_idx_ = 0;
  batch_allocator_.reset();
  record_allocator_.reset();
  memset(value_buf_, 0, sizeof(value_buf_));
}

int ObAllVirtualSharedGCDiagnose::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
    SERVER_LOG(TRACE, "not in shared storage mode", KR(ret));
  } else if (!predicates_extracted_) {
    if (OB_FAIL(extract_predicates_())) {
      SERVER_LOG(WARN, "fail to extract predicates", KR(ret));
    } else {
      predicates_extracted_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    bool has_row = false;
    if (OB_FAIL(advance_cursor_to_next_row_(has_row))) {
      SERVER_LOG(WARN, "fail to advance cursor", KR(ret));
    } else if (!has_row) {
      ret = OB_ITER_END;
      SERVER_LOG(TRACE, "no more rows", KR(ret));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

const char *ObAllVirtualSharedGCDiagnose::group_to_str_(const ObMarkSweepGroupType group)
{
  const char *str = "unknown";
  if (ObMarkSweepGroupType::DEL == group) {
    str = "del";
  } else if (ObMarkSweepGroupType::KEPT == group) {
    str = "kept";
  } else {
    SERVER_LOG_RET(WARN, OB_INVALID_ARGUMENT, "unknown group", K(group));
  }
  return str;
}

const char *ObAllVirtualSharedGCDiagnose::type_to_str_(const ObSSMarkSweepDiagFileType type)
{
  const char *str = "unknown";
  if (ObSSMarkSweepDiagFileType::MACROS == type) {
    str = "macros";
  } else if (ObSSMarkSweepDiagFileType::TABLET_META_SCNS == type) {
    str = "tablet_meta_scns";
  } else if (ObSSMarkSweepDiagFileType::STUDY_RESULTS == type) {
    str = "study_results";
  } else {
    SERVER_LOG_RET(WARN, OB_INVALID_ARGUMENT, "unknown type", K(type));
  }
  return str;
}

int ObAllVirtualSharedGCDiagnose::extract_predicates_()
{
  int ret = OB_SUCCESS;
  if (key_ranges_.count() != 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "round_id must be specified, multiple range select is");
    SERVER_LOG(WARN, "invalid key ranges", KR(ret), "cnt", key_ranges_.count());
  } else {
    ObNewRange &key_range = key_ranges_.at(0);
    if (OB_UNLIKELY(key_range.get_start_key().get_obj_cnt() != ROWKEY_COL_CNT
                    || key_range.get_end_key().get_obj_cnt() != ROWKEY_COL_CNT)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "unexpected rowkey column count", KR(ret),
                 "start_cnt", key_range.get_start_key().get_obj_cnt(),
                 "end_cnt", key_range.get_end_key().get_obj_cnt());
    } else if (OB_FAIL(handle_key_range_(key_range))) {
      SERVER_LOG(WARN, "fail to handle key range", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualSharedGCDiagnose::handle_key_range_(ObNewRange &key_range)
{
  int ret = OB_SUCCESS;
  const ObObj *low = key_range.get_start_key().get_obj_ptr();
  const ObObj *high = key_range.get_end_key().get_obj_ptr();
  // a rowkey column i is "specified" if it is a point predicate (low == high, neither min/max).
  // column order: 0 tenant_id, 1 round_id, 2 group, 3 type, 4 ls_id, 5 tablet_id, 6 reorg_scn.

  // round_id (mandatory)
  const ObObj &round_id_low = low[1];
  const ObObj &round_id_high = high[1];
  const bool round_specified = (!round_id_low.is_min_value()
                                && !round_id_high.is_max_value()
                                && (round_id_low == round_id_high));
  if (OB_UNLIKELY(!round_specified)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "round_id not specified is");
    SERVER_LOG(WARN, "round_id not specified", KR(ret));
  } else {
    round_id_ = round_id_low.get_int();
  }

  // tenant_id: sys tenant may specify a target tenant; otherwise current tenant.
  if (OB_SUCC(ret)) {
    const ObObj &tenant_id_low = low[0];
    const ObObj &tenant_id_high = high[0];
    const bool tenant_specified = !tenant_id_low.is_min_value()
                                  && !tenant_id_high.is_max_value()
                                  && (tenant_id_low == tenant_id_high);
    if (is_sys_tenant(effective_tenant_id_) && tenant_specified) {
      target_tenant_id_ = static_cast<uint64_t>(tenant_id_low.get_int());
    } else {
      target_tenant_id_ = effective_tenant_id_;
    }
    if (OB_UNLIKELY(!is_valid_tenant_id(target_tenant_id_))) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid tenant_id");
      SERVER_LOG(WARN, "invalid target tenant id", KR(ret), K_(target_tenant_id));
    }
  }

  // group (optional): "del"/"kept"
  if (OB_SUCC(ret)) {
    const ObObj &group_low = low[2];
    const ObObj &group_high = high[2];
    if (!group_low.is_min_value() && !group_high.is_max_value() && (group_low == group_high)) {
      ObString group_str = group_low.get_string();
      if (0 == group_str.case_compare("del")) {
        has_group_filter_ = true;
        group_filter_ = ObMarkSweepGroupType::DEL;
      } else if (0 == group_str.case_compare("kept")) {
        has_group_filter_ = true;
        group_filter_ = ObMarkSweepGroupType::KEPT;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "group must be 'del' or 'kept'");
        SERVER_LOG(WARN, "invalid group filter", KR(ret), K(group_str));
      }
    }
  }

  // Optional type filter includes macros/tablet_meta_scns/study_results.
  if (OB_SUCC(ret)) {
    const ObObj &type_low = low[3];
    const ObObj &type_high = high[3];
    if (!type_low.is_min_value() && !type_high.is_max_value() && (type_low == type_high)) {
      ObString type_str = type_low.get_string();
      if (0 == type_str.case_compare("macros")) {
        has_type_filter_ = true;
        type_filter_ = ObSSMarkSweepDiagFileType::MACROS;
      } else if (0 == type_str.case_compare("tablet_meta_scns")) {
        has_type_filter_ = true;
        type_filter_ = ObSSMarkSweepDiagFileType::TABLET_META_SCNS;
      } else if (0 == type_str.case_compare("study_results")) {
        has_type_filter_ = true;
        type_filter_ = ObSSMarkSweepDiagFileType::STUDY_RESULTS;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
            "type must be 'macros', 'tablet_meta_scns' or 'study_results'");
        SERVER_LOG(WARN, "invalid type filter", KR(ret), K(type_str));
      }
    }
  }

  // ls_id (optional)
  if (OB_SUCC(ret)) {
    const ObObj &ls_id_low = low[4];
    const ObObj &ls_id_high = high[4];
    if (!ls_id_low.is_min_value() && !ls_id_high.is_max_value() && (ls_id_low == ls_id_high)) {
      has_ls_id_filter_ = true;
      ls_id_filter_ = ls_id_low.get_int();
    }
  }

  // tablet_id (optional)
  if (OB_SUCC(ret)) {
    const ObObj &tablet_id_low = low[5];
    const ObObj &tablet_id_high = high[5];
    if (!tablet_id_low.is_min_value() && !tablet_id_high.is_max_value() && (tablet_id_low == tablet_id_high)) {
      has_tablet_id_filter_ = true;
      tablet_id_filter_ = tablet_id_low.get_int();
    }
  }

  // reorganization_scn (optional)
  if (OB_SUCC(ret)) {
    const ObObj &reorganization_scn_low = low[6];
    const ObObj &reorganization_scn_high = high[6];
    if (!reorganization_scn_low.is_min_value()
        && !reorganization_scn_high.is_max_value()
        && (reorganization_scn_low == reorganization_scn_high)) {
      has_reorg_scn_filter_ = true;
      reorg_scn_filter_ = static_cast<uint64_t>(reorganization_scn_low.get_int());
    }
  }
  return ret;
}

int ObAllVirtualSharedGCDiagnose::list_buckets_(
    const ObMarkSweepGroupType group,
    const ObSSMarkSweepDiagFileType type,
    ObIArray<int64_t> &buckets)
{
  int ret = OB_SUCCESS;
  char diag_root[common::MAX_PATH_SIZE] = {0};
  char dir_uri[common::MAX_PATH_SIZE] = {0};
  ObBackupDest storage_dest;
  uint64_t storage_id = OB_INVALID_ID;
  ObArenaAllocator tmp_allocator(ObMemAttr(OB_SERVER_TENANT_ID, "GcDiagList"));
  ObArray<ObString> names;
  ObFileListArrayOp list_op(names, tmp_allocator);
  ObBackupIoAdapter io_adapter;
  buckets.reset();
  if (OB_FAIL(ObSSMarkSweepDiagMgr::build_diag_root_dir(target_tenant_id_, diag_root, sizeof(diag_root)))) {
    SERVER_LOG(WARN, "fail to build diag root dir", KR(ret), K_(target_tenant_id));
  } else if (OB_FAIL(databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s%ld/%s/%s",
                     diag_root, ROUND_DIR_STR, round_id_, group_to_str_(group), type_to_str_(type)))) {
    SERVER_LOG(WARN, "fail to build bucket dir uri", KR(ret), K_(round_id));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::get_storage_dest_and_id(storage_dest, storage_id))) {
    SERVER_LOG(WARN, "fail to get storage dest and id", KR(ret));
  } else if (OB_FAIL(io_adapter.list_files(ObString(dir_uri), storage_dest.get_storage_info(), list_op))) {
    SERVER_LOG(WARN, "fail to list bucket files", KR(ret), K(dir_uri));
  } else {
    const int64_t prefix_len = STRLEN(BKT_KEY_STR);
    for (int64_t i = 0; OB_SUCC(ret) && (i < names.count()); ++i) {
      const ObString &name = names.at(i);
      char num_buf[32] = {0};
      // leaf name format: "bkt<bucket>.T<obj_id>". Strip the "bkt" prefix, parse the bucket
      // digits, then tolerate the trailing ".T<obj_id>" type suffix.
      if (OB_UNLIKELY((name.length() <= 0) || (name.length() >= static_cast<int64_t>(sizeof(num_buf))))) {
        SERVER_LOG(INFO, "skip bucket entry with invalid name length", K(name));
      } else if (!name.prefix_match(BKT_KEY_STR)) {
        SERVER_LOG(INFO, "skip bucket entry without bkt prefix", K(name));
      } else {
        const int64_t num_len = name.length() - prefix_len;
        MEMCPY(num_buf, name.ptr() + prefix_len, num_len);
        num_buf[num_len] = '\0';
        char *endptr = nullptr;
        const int64_t bucket = strtoll(num_buf, &endptr, 10/*base*/);
        if (OB_ISNULL(endptr) || (endptr == num_buf)) {
          // no digits after the prefix.
          SERVER_LOG(ERROR, "skip non-numeric bucket entry", K(name));
        } else if (('\0' != *endptr) && (0 != STRNCMP(endptr, OBJECT_TYPE_STR, STRLEN(OBJECT_TYPE_STR)))) {
          // remainder is neither empty nor the ".T<obj_id>" type suffix.
          SERVER_LOG(ERROR, "skip bucket entry with unexpected suffix", K(name));
        } else if (OB_FAIL(buckets.push_back(bucket))) {
          SERVER_LOG(WARN, "fail to push back bucket", KR(ret), K(bucket));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualSharedGCDiagnose::open_next_file_(bool &opened)
{
  int ret = OB_SUCCESS;
  opened = false;
  while (OB_SUCC(ret) && !opened && (group_idx_ < GROUP_NUM)) {
    const ObMarkSweepGroupType group = GROUPS[group_idx_];
    const ObSSMarkSweepDiagFileType type = TYPES[type_idx_];
    // apply group/type filters: skip whole (group, type) that cannot match.
    const bool is_group_match = (!has_group_filter_ || (group_filter_ == group));
    const bool is_type_match = (!has_type_filter_ || (type_filter_ == type));
    const bool is_valid_type = (ObSSMarkSweepDiagFileType::STUDY_RESULTS != type
        || ObMarkSweepGroupType::KEPT == group);
    if (!is_group_match || !is_type_match || !is_valid_type) {
      // skip this (group, type) entirely.
      is_buckets_listed_ = true;
      bucket_idx_ = 0;
      buckets_.reset();
    } else if (!is_buckets_listed_) {
      if (OB_FAIL(list_buckets_(group, type, buckets_))) {
        SERVER_LOG(WARN, "fail to list buckets", KR(ret), K_(group_idx), K_(type_idx));
      } else {
        is_buckets_listed_ = true;
        bucket_idx_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_group_match || !is_type_match || !is_valid_type
          || (bucket_idx_ >= buckets_.count())) {
        // advance to next (group, type)
        reader_.reset();
        is_reader_opened_ = false;
        is_buckets_listed_ = false;
        buckets_.reset();
        bucket_idx_ = 0;
        ++type_idx_;
        if (type_idx_ >= TYPE_NUM) {
          type_idx_ = 0;
          ++group_idx_;
        }
      } else {
        const int64_t bucket = buckets_.at(bucket_idx_);
        reader_.reset();
        if (OB_FAIL(reader_.init(target_tenant_id_, round_id_, group, type, bucket))) {
          SERVER_LOG(WARN, "fail to init diag file reader", KR(ret), K_(target_tenant_id),
                    K_(round_id), K(bucket));
        } else {
          is_reader_opened_ = true;
          opened = true;
          ++bucket_idx_;
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualSharedGCDiagnose::next_record_batch_(bool &has_batch)
{
  int ret = OB_SUCCESS;
  has_batch = false;
  bool is_complete = false;
  while (OB_SUCC(ret) && !has_batch && !is_complete) {
    if (!is_reader_opened_) {
      bool opened = false;
      if (OB_FAIL(open_next_file_(opened))) {
        SERVER_LOG(WARN, "fail to open next file", KR(ret));
      } else if (!opened) {
        is_complete = true; // no more files
      }
    }
    if (OB_SUCC(ret) && is_reader_opened_) {
      batch_allocator_.reset();
      cur_records_.reset();
      record_idx_ = 0;
      if (OB_FAIL(reader_.next_record_batch(batch_allocator_, cur_records_))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          reader_.reset();
          is_reader_opened_ = false; // move to next file on next loop
        } else {
          SERVER_LOG(WARN, "fail to read next batch", KR(ret));
        }
      } else if (OB_UNLIKELY(cur_records_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "unexpected empty batch", KR(ret), K_(record_idx));
      } else { // !cur_records_.empty()
        has_batch = true;
      }
    }
  }
  return ret;
}

int ObAllVirtualSharedGCDiagnose::advance_cursor_to_next_row_(bool &has_row)
{
  int ret = OB_SUCCESS;
  has_row = false;
  bool is_complete = false;
  while (OB_SUCC(ret) && !has_row && !is_complete) {
    const ObSSMarkSweepDiagFileType type = TYPES[type_idx_];
    // ensure we have a current decoded record with remaining items.
    if (is_cur_record_decoded_) {
      ObSSGCTablet tablet;
      int64_t item_cnt = 0;
      if (ObSSMarkSweepDiagFileType::MACROS == type) {
        tablet = cur_macros_.tablet_;
        item_cnt = cur_macros_.macro_ids_.count();
      } else if (ObSSMarkSweepDiagFileType::TABLET_META_SCNS == type) {
        tablet = cur_meta_scns_.tablet_;
        item_cnt = cur_meta_scns_.meta_scns_.count();
      } else if (ObSSMarkSweepDiagFileType::STUDY_RESULTS == type) {
        tablet = cur_study_results_.tablet_;
        item_cnt = cur_study_results_.study_results_.count();
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected type", KR(ret), K(type));
      }
      if (OB_FAIL(ret)) {
      } else if (item_idx_ < item_cnt) {
        ObString value;
        if (OB_FAIL(construct_cur_value_(value))) {
          SERVER_LOG(WARN, "fail to construct value", KR(ret));
        } else if (OB_FAIL(fill_cells_(tablet, value))) {
          SERVER_LOG(WARN, "fail to fill cells", KR(ret));
        } else {
          ++item_idx_;
          has_row = true;
        }
      } else { // item_idx_ >= item_cnt
        // current record exhausted, move to next record.
        is_cur_record_decoded_ = false;
        ++record_idx_;
      }
    }
    if (OB_SUCC(ret) && !has_row) {
      if (record_idx_ < cur_records_.count()) {
        // decode next record in the current batch.
        record_allocator_.reset();
        cur_macros_.reset();
        cur_meta_scns_.reset();
        cur_study_results_.reset();
        item_idx_ = 0;
        const ObString &record = cur_records_.at(record_idx_);
        const ObSSGCTablet *tablet = nullptr;
        if (ObSSMarkSweepDiagFileType::MACROS == type) {
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_macros_record(record, cur_macros_))) {
            SERVER_LOG(WARN, "fail to decode macros record", KR(ret), K_(record_idx));
          } else {
            tablet = &cur_macros_.tablet_;
          }
        } else if (ObSSMarkSweepDiagFileType::TABLET_META_SCNS == type) {
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_meta_scns_record(record, cur_meta_scns_))) {
            SERVER_LOG(WARN, "fail to decode meta scns record", KR(ret), K_(record_idx));
          } else {
            tablet = &cur_meta_scns_.tablet_;
          }
        } else if (ObSSMarkSweepDiagFileType::STUDY_RESULTS == type) {
          if (OB_FAIL(ObSSMarkSweepDiagMgr::decode_study_record(record, cur_study_results_))) {
            SERVER_LOG(WARN, "fail to decode study record", KR(ret), K_(record_idx));
          } else {
            tablet = &cur_study_results_.tablet_;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected type", KR(ret), K(type));
        }
        if (OB_SUCC(ret)) {
          if (!pass_optional_filters_(*tablet)) {
            // skip this record entirely.
            ++record_idx_;
            SERVER_LOG(TRACE, "skip record due to optional filters", KR(ret), K_(record_idx));
          } else {
            is_cur_record_decoded_ = true;
          }
        }
      } else {
        // current batch drained, fetch next batch.
        bool has_batch = false;
        if (OB_FAIL(next_record_batch_(has_batch))) {
          SERVER_LOG(WARN, "fail to fetch next record batch", KR(ret));
        } else if (!has_batch) {
          is_complete = true; // all groups/types/buckets/batches drained
        }
      }
    }
  }
  return ret;
}

bool ObAllVirtualSharedGCDiagnose::pass_optional_filters_(const ObSSGCTablet &tablet) const
{
  bool pass = true;
  if (has_ls_id_filter_ && (tablet.ls_id_.id() != ls_id_filter_)) {
    pass = false;
  } else if (has_tablet_id_filter_
             && (static_cast<int64_t>(tablet.tablet_id_.id()) != tablet_id_filter_)) {
    pass = false;
  } else if (has_reorg_scn_filter_
             && (tablet.transfer_scn_.get_val_for_inner_table_field() != reorg_scn_filter_)) {
    pass = false;
  }
  return pass;
}

int ObAllVirtualSharedGCDiagnose::construct_cur_value_(ObString &value)
{
  int ret = OB_SUCCESS;
  const ObSSMarkSweepDiagFileType type = TYPES[type_idx_];
  if (ObSSMarkSweepDiagFileType::MACROS == type) {
    const MacroBlockId &macro_id = cur_macros_.macro_ids_.at(item_idx_);
    const int64_t len = macro_id.to_string(value_buf_, MAX_VALUE_LEN);
    if (OB_UNLIKELY(len <= 0 || len >= MAX_VALUE_LEN)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to print macro id", KR(ret), K(macro_id), K(len));
    } else {
      value.assign_ptr(value_buf_, static_cast<int32_t>(len));
    }
  } else if (ObSSMarkSweepDiagFileType::TABLET_META_SCNS == type) {
    const SCN &scn = cur_meta_scns_.meta_scns_.at(item_idx_);
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(value_buf_, MAX_VALUE_LEN, pos, "%lu",
                                scn.get_val_for_inner_table_field()))) {
      SERVER_LOG(WARN, "fail to print meta scn", KR(ret), K(scn));
    } else {
      value.assign_ptr(value_buf_, static_cast<int32_t>(pos));
    }
  } else if (ObSSMarkSweepDiagFileType::STUDY_RESULTS == type) {
    char svr_ip[OB_IP_STR_BUFF] = {0};
    int64_t pos = 0;
    if (OB_UNLIKELY(item_idx_ < 0 || item_idx_ >= cur_study_results_.study_results_.count())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid study node index", KR(ret), K_(item_idx));
    } else {
      const ObSSMarkSweepDiagStudyNode &node = cur_study_results_.study_results_.at(item_idx_);
      if (!node.server_.ip_to_string(svr_ip, sizeof(svr_ip))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to print study server address", KR(ret), K(node.server_));
      } else if (OB_FAIL(databuff_printf(value_buf_, MAX_VALUE_LEN, pos,
          "{\"svr_ip\":\"%s\",\"svr_port\":%d,"
          "\"min_dep_meta_scn\":\"%lu\",\"min_major_snapshot\":\"%ld\"}",
          svr_ip, node.server_.get_port(),
          node.study_info_.min_dep_meta_scn_.get_val_for_inner_table_field(),
          node.study_info_.min_major_snapshot_))) {
        SERVER_LOG(WARN, "fail to print study node value", KR(ret), K_(item_idx));
      } else {
        value.assign_ptr(value_buf_, static_cast<int32_t>(pos));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected type", KR(ret), K(type));
  }
  return ret;
}

int ObAllVirtualSharedGCDiagnose::fill_cells_(const ObSSGCTablet &tablet, const ObString &value)
{
  int ret = OB_SUCCESS;
  const ObMarkSweepGroupType group = GROUPS[group_idx_];
  const ObSSMarkSweepDiagFileType type = TYPES[type_idx_];
  const int64_t col_count = output_column_ids_.count();
  const ObCollationType cs_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  for (int64_t i = 0; OB_SUCC(ret) && (i < col_count); ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID: {
        cur_row_.cells_[i].set_int(static_cast<int64_t>(target_tenant_id_));
        break;
      }
      case ROUND_ID: {
        cur_row_.cells_[i].set_int(round_id_);
        break;
      }
      case GROUP: {
        cur_row_.cells_[i].set_varchar(ObString::make_string(group_to_str_(group)));
        cur_row_.cells_[i].set_collation_type(cs_type);
        break;
      }
      case TYPE: {
        cur_row_.cells_[i].set_varchar(ObString::make_string(type_to_str_(type)));
        cur_row_.cells_[i].set_collation_type(cs_type);
        break;
      }
      case LS_ID: {
        cur_row_.cells_[i].set_int(tablet.ls_id_.id());
        break;
      }
      case TABLET_ID: {
        cur_row_.cells_[i].set_int(static_cast<int64_t>(tablet.tablet_id_.id()));
        break;
      }
      case REORGANIZATION_SCN: {
        cur_row_.cells_[i].set_int(static_cast<int64_t>(tablet.transfer_scn_.get_val_for_inner_table_field()));
        break;
      }
      case VALUE: {
        cur_row_.cells_[i].set_varchar(value);
        cur_row_.cells_[i].set_collation_type(cs_type);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid col_id", KR(ret), K(col_id));
        break;
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
