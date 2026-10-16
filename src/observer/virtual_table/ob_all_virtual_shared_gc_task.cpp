/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "observer/virtual_table/ob_all_virtual_shared_gc_task.h"
#include "lib/charset/ob_charset.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace observer
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;

ObAllVirtualSharedGCTask::ObAllVirtualSharedGCTask()
    : ObVirtualTableScannerIterator(),
      table_name_(nullptr),
      tenant_ids_(),
      tenant_idx_(-1),
      tenant_id_filter_(),
      type_filter_(),
      is_type_filter_always_false_(false),
      round_id_filter_(),
      task_iter_(),
      task_iter_opened_(false),
      cur_task_(),
      cur_macro_id_list_(),
      cur_macro_idx_(0),
      cur_gmt_create_(0),
      cur_gmt_modified_(0),
      has_pending_task_(false),
      task_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "SharedGCTaskTk")),
      row_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "SharedGCTaskVT")),
      value_buf_(nullptr)
{
}

ObAllVirtualSharedGCTask::~ObAllVirtualSharedGCTask()
{
  reset();
}

int ObAllVirtualSharedGCTask::ObSharedGCTaskTenantIdFilter::assign(
    const ObSharedGCTaskTenantIdFilter &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ranges_.assign(other.ranges_))) {
    SERVER_LOG(WARN, "failed to assign ranges", KR(ret));
  } else {
    is_filter_enabled_ = other.is_filter_enabled_;
  }
  return ret;
}

void ObAllVirtualSharedGCTask::reset()
{
  tenant_ids_.reset();
  tenant_idx_ = -1;
  tenant_id_filter_.reset();
  type_filter_.reset();
  is_type_filter_always_false_ = false;
  round_id_filter_.reset();
  task_iter_.reset();
  task_iter_opened_ = false;
  cur_task_.reset();
  cur_macro_id_list_.reset();
  cur_macro_idx_ = 0;
  cur_gmt_create_ = 0;
  cur_gmt_modified_ = 0;
  has_pending_task_ = false;
  task_allocator_.reset();
  row_allocator_.reset();
  value_buf_ = nullptr;
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualSharedGCTask::release_last_tenant_()
{
  task_iter_.reset();
  task_iter_opened_ = false;
  cur_task_.reset();
  cur_macro_id_list_.reset();
  cur_macro_idx_ = 0;
  cur_gmt_create_ = 0;
  cur_gmt_modified_ = 0;
  has_pending_task_ = false;
  task_allocator_.reset();
}

int ObAllVirtualSharedGCTask::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
    SERVER_LOG(TRACE, "not in shared storage mode", KR(ret));
  } else if (-1 == tenant_idx_) {
    // Parse predicates first so build_tenant_ids_() can prune by tenant_id range.
    if (OB_FAIL(extract_key_ranges_())) {
      SERVER_LOG(WARN, "fail to extract key ranges", KR(ret));
    } else if (is_type_filter_always_false_) {
      tenant_ids_.reset();
      tenant_idx_ = 0;
    } else if (OB_FAIL(build_tenant_ids_())) {
      SERVER_LOG(WARN, "fail to build tenant ids", KR(ret));
    } else {
      tenant_idx_ = 0;
    }
  }
  while (OB_SUCC(ret) && OB_ISNULL(row)) {
    if (tenant_idx_ >= tenant_ids_.count()) {
      ret = OB_ITER_END;
      SERVER_LOG(TRACE, "iterate end", KR(ret), K_(tenant_idx), K(tenant_ids_.count()));
    } else {
      const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
      if (OB_FAIL(process_curr_tenant_(row))) {
        if (OB_ITER_END == ret) {
          // current tenant exhausted, ignore ret, and advance to the next one
          ret = OB_SUCCESS;
          release_last_tenant_();
          ++tenant_idx_;
          row = nullptr;
        } else {
          SERVER_LOG(WARN, "fail to read shared gc task", KR(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

ObSharedGCTaskType ObAllVirtualSharedGCTask::parse_type_filter_(const ObString &type_name)
{
  ObSharedGCTaskType type = ObSharedGCTaskType::INVALID_TYPE;
  ObString candidate_name;
  const bool compare_end_space = false;
  bool found = false;
  // TYPE 列使用 utf8mb4_general_ci，下推比较需与其大小写不敏感和
  // PAD SPACE 语义一致。
  for (int64_t type_value = static_cast<int64_t>(ObSharedGCTaskType::INVALID_TYPE) + 1;
       !found && type_value < static_cast<int64_t>(ObSharedGCTaskType::MAX_TYPE);
       ++type_value) {
    const ObSharedGCTaskType candidate_type =
        static_cast<ObSharedGCTaskType>(type_value);
    candidate_name = ObString::make_string(ob_shared_gc_task_type_to_string(candidate_type));
    if (0 == ObCharset::strcmpsp(
        CS_TYPE_UTF8MB4_GENERAL_CI,
        type_name.ptr(),
        type_name.length(),
        candidate_name.ptr(),
        candidate_name.length(),
        compare_end_space)) {
      type = candidate_type;
      found = true;
    }
  }
  return type;
}

int ObAllVirtualSharedGCTask::extract_key_ranges_()
{
  int ret = OB_SUCCESS;
  // rowkey order: [0]=tenant_id, [1]=type, [2]=round_id, [3]=task_id.
  static const int64_t TENANT_IDX = 0;
  static const int64_t TYPE_IDX = 1;
  static const int64_t ROUND_IDX = 2;
  static const int64_t ROWKEY_CNT = 4;
  tenant_id_filter_.reset();
  type_filter_.reset();
  is_type_filter_always_false_ = false;
  round_id_filter_.reset();

  // Aggregate the predicates across all ranges directly into the filter structs.
  // type filter stays enabled only while every range contributes a concrete type
  // point; otherwise we scan all types. round_id is folded into one enclosing
  // [round_id_low, round_id_high].
  if (key_ranges_.count() > 0) {
    tenant_id_filter_.is_filter_enabled_ = true;
    type_filter_.is_filter_enabled_ = true;
    round_id_filter_.is_filter_enabled_ = true;
  }
  bool is_complete = false;
  for (int64_t i = 0; OB_SUCC(ret) && (i < key_ranges_.count()) && !is_complete; ++i) {
    const ObNewRange &range = key_ranges_.at(i);
    const ObRowkey &start = range.get_start_key();
    const ObRowkey &end = range.get_end_key();
    if (OB_UNLIKELY((start.get_obj_cnt() != ROWKEY_CNT) || (end.get_obj_cnt() != ROWKEY_CNT)
        || OB_ISNULL(start.get_obj_ptr()) || OB_ISNULL(end.get_obj_ptr()))) {
      // unexpected rowkey shape -> treat as fully open: disable all filter.
      tenant_id_filter_.reset();
      type_filter_.reset();
      round_id_filter_.reset();
      is_complete = true;
      SERVER_LOG_RET(WARN, OB_ERR_UNEXPECTED, "unexpected rowkey, disable filter", K(i), K(start), K(end));
    } else {
      const ObObj *s = start.get_obj_ptr();
      const ObObj *e = end.get_obj_ptr();
      // 1. tenant filter
      if (tenant_id_filter_.is_filter_enabled_
          && !s[TENANT_IDX].is_min_value() && !s[TENANT_IDX].is_max_value()
          && !e[TENANT_IDX].is_min_value() && !e[TENANT_IDX].is_max_value()
          && s[TENANT_IDX].get_uint64() <= e[TENANT_IDX].get_uint64()) {
        const uint64_t tenant_id_low = s[TENANT_IDX].get_uint64();
        const uint64_t tenant_id_high = e[TENANT_IDX].get_uint64();
        if (OB_FAIL(tenant_id_filter_.ranges_.push_back(ObSharedGCTaskTenantIdRange(tenant_id_low, tenant_id_high)))) {
          SERVER_LOG(WARN, "fail to push back tenant range", KR(ret), K(tenant_id_low), K(tenant_id_high));
        }
      } else {
        tenant_id_filter_.is_filter_enabled_ = false;
        SERVER_LOG(WARN, "tenant id filter is disabled", K(i), K(start), K(end));
      }

      // 2. type filter
      int tmp_ret = OB_SUCCESS;
      ObString type_low;
      ObString type_high;
      if (OB_SUCC(ret) && type_filter_.is_filter_enabled_
          && !s[TYPE_IDX].is_min_value() && !s[TYPE_IDX].is_max_value()
          && !e[TYPE_IDX].is_min_value() && !e[TYPE_IDX].is_max_value()
          && OB_TMP_SUCC(s[TYPE_IDX].get_varchar(type_low))
          && OB_TMP_SUCC(e[TYPE_IDX].get_varchar(type_high))
          && (type_low == type_high)) {
        const storage::ObSharedGCTaskType type = parse_type_filter_(type_low);
        if (storage::is_valid_shared_gc_task_type(type)) {
          if (!has_exist_in_array(type_filter_.types_, type)) {
            if (OB_FAIL(type_filter_.types_.push_back(type))) {
              SERVER_LOG(WARN, "fail to push back type filter", KR(ret), K(type));
            }
          }
        } else {
          // 具体点值无法映射为有效枚举时，该 range 不会命中任何物理行。
          SERVER_LOG(WARN, "invalid type string in key range", K(type_low));
        }
      } else {
        type_filter_.is_filter_enabled_ = false;
        SERVER_LOG(WARN, "type filter is disabled", K(i), K(start), K(end));
      }

      // 3. round_id_filter
      if (OB_SUCC(ret) && round_id_filter_.is_filter_enabled_
          && !s[ROUND_IDX].is_min_value() && !s[ROUND_IDX].is_max_value()
          && !e[ROUND_IDX].is_min_value() && !e[ROUND_IDX].is_max_value()
          && s[ROUND_IDX].get_int() <= e[ROUND_IDX].get_int()) {
        const int64_t round_id_low = s[ROUND_IDX].get_int();
        const int64_t round_id_high = e[ROUND_IDX].get_int();
        if (round_id_low < round_id_filter_.round_id_low_) {
          round_id_filter_.round_id_low_ = round_id_low;
        }
        if (round_id_high > round_id_filter_.round_id_high_) {
          round_id_filter_.round_id_high_ = round_id_high;
        }
      } else {
        round_id_filter_.is_filter_enabled_ = false;
        SERVER_LOG(WARN, "round id filter is disabled", K(i), K(start), K(end));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (type_filter_.is_filter_enabled_ && type_filter_.types_.empty()) {
      is_type_filter_always_false_ = true;
      SERVER_LOG(INFO, "type filter is always false", K_(type_filter),
          K_(is_type_filter_always_false));
    }
    if (OB_FAIL(extract_type_filter_from_op_filters_())) {
      SERVER_LOG(WARN, "fail to extract type filter from op filters", KR(ret));
    }
  }
  SERVER_LOG(INFO, "finish to extract key ranges", KR(ret), K_(tenant_id_filter),
      K_(type_filter), K_(round_id_filter));
  return ret;
}

int ObAllVirtualSharedGCTask::extract_type_filter_from_op_filters_()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 2> type_names;
  ObSEArray<ObSharedGCTaskType, 2> types;
  ObSEArray<ObSharedGCTaskType, 2> intersected_types;
  bool is_found = false;
  if (OB_FAIL(get_string_filter_values_by_column_name("type", type_names, is_found))) {
    SERVER_LOG(WARN, "fail to get type filter by column name", KR(ret));
  } else if (is_found) {
    for (int64_t i = 0; OB_SUCC(ret) && i < type_names.count(); ++i) {
      const ObSharedGCTaskType type = parse_type_filter_(type_names.at(i));
      bool is_exist = false;
      if (!is_valid_shared_gc_task_type(type)) {
        // 无效枚举字面量不会命中 TYPE 列，不加入底层过滤集合。
        SERVER_LOG(WARN, "invalid type filter", K(type_names.at(i)));
      } else {
        for (int64_t j = 0; !is_exist && j < types.count(); ++j) {
          is_exist = (type == types.at(j));
        }
        if (!is_exist && OB_FAIL(types.push_back(type))) {
          SERVER_LOG(WARN, "fail to push back type filter", KR(ret), K(type));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (types.empty()) {
      is_type_filter_always_false_ = true;
    } else if (type_filter_.need_filter()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < type_filter_.types_.count(); ++i) {
        bool is_exist = false;
        for (int64_t j = 0; !is_exist && j < types.count(); ++j) {
          is_exist = (type_filter_.types_.at(i) == types.at(j));
        }
        if (is_exist && OB_FAIL(intersected_types.push_back(type_filter_.types_.at(i)))) {
          SERVER_LOG(WARN, "fail to push back intersected type", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (intersected_types.empty()) {
        is_type_filter_always_false_ = true;
      } else if (OB_FAIL(type_filter_.types_.assign(intersected_types))) {
        SERVER_LOG(WARN, "fail to assign intersected type filters", KR(ret));
      }
    } else {
      type_filter_.reset();
      type_filter_.is_filter_enabled_ = true;
      if (OB_FAIL(type_filter_.types_.assign(types))) {
        SERVER_LOG(WARN, "fail to assign type filters", KR(ret));
      }
    }
  }
  return ret;
}

bool ObAllVirtualSharedGCTask::is_tenant_id_filtered_(const uint64_t tenant_id) const
{
  bool bret = false;
  if (tenant_id_filter_.need_filter()) {
    bret = true; // assume filtered out, unless tenant_id hits some range
    for (int64_t i = 0; bret && (i < tenant_id_filter_.ranges_.count()); ++i) {
      const ObSharedGCTaskTenantIdRange &range = tenant_id_filter_.ranges_.at(i);
      if ((tenant_id >= range.tenant_id_low_) && (tenant_id <= range.tenant_id_high_)) {
        bret = false; // inside a queried range -> keep
      }
    }
  }
  return bret;
}

int ObAllVirtualSharedGCTask::build_tenant_ids_()
{
  int ret = OB_SUCCESS;
  tenant_ids_.reset();
  if (is_sys_tenant(effective_tenant_id_)) {
    // SYS tenant sees every cluster tenant (sys + meta + user).
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "schema service is null", KR(ret));
    } else {
      ObArray<uint64_t> all_tenant_ids;
      // user tenant's records are store in meta tenant, so collect the user tenant ids whose
      // meta tenant's status is not normal and skip these user tenants. The meta tenant itself
      // is kept, as its records are stored in SYS (iterator routes meta->sys).
      ObArray<uint64_t> skip_user_tenant_ids;
      share::schema::ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
        SERVER_LOG(WARN, "fail to get sys schema guard", KR(ret));
      } else if (OB_FAIL(schema_guard.get_tenant_ids(all_tenant_ids))) {
        SERVER_LOG(WARN, "fail to get all tenant ids", KR(ret));
      } else {
        // 1. find user tenants whose meta tenant's status is not normal.
        for (int64_t i = 0; OB_SUCC(ret) && (i < all_tenant_ids.count()); ++i) {
          const uint64_t tenant_id = all_tenant_ids.at(i);
          if (is_meta_tenant(tenant_id)) {
            const share::schema::ObSimpleTenantSchema *tenant_schema = nullptr;
            if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
              SERVER_LOG(WARN, "fail to get tenant info", KR(ret), K(tenant_id));
            } else if (OB_ISNULL(tenant_schema)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "tenant schema is null", KR(ret), K(tenant_id));
            } else if (!tenant_schema->is_normal()) {
              const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
              if (OB_FAIL(skip_user_tenant_ids.push_back(user_tenant_id))) {
                SERVER_LOG(WARN, "fail to push back skip user tenant id", KR(ret), K(user_tenant_id));
              } else {
                SERVER_LOG(INFO, "meta tenant status is not normal, will skip its user tenant",
                    K(tenant_id), K(user_tenant_id));
              }
            }
          }
        }
        // 2。 build the iterate list, skipping virtual tenants and the excluded user tenants.
        for (int64_t i = 0; OB_SUCC(ret) && (i < all_tenant_ids.count()); ++i) {
          const uint64_t tenant_id = all_tenant_ids.at(i);
          if (is_virtual_tenant_id(tenant_id)) {
            // skip virtual tenants
          } else if (is_user_tenant(tenant_id)
              && has_exist_in_array(skip_user_tenant_ids, tenant_id)) {
            SERVER_LOG(INFO, "skip user tenant whose meta tenant's status is not normal", K(tenant_id));
          } else if (is_tenant_id_filtered_(tenant_id)) {
            // filter: tenant_id is outside the queried range(s), skip
            SERVER_LOG(INFO, "skip tenant out of queried range", K(tenant_id), K_(tenant_id_filter));
          } else if (OB_FAIL(tenant_ids_.push_back(tenant_id))) {
            SERVER_LOG(WARN, "fail to push back tenant id", KR(ret), K(tenant_id));
          }
        }
      }
    }
  } else { // user/meta tenant
    if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "fail to push back tenant id", KR(ret), K_(effective_tenant_id));
    }
  }
  return ret;
}

int ObAllVirtualSharedGCTask::convert_macro_id_(
    const MacroBlockId &macro_id,
    ObString &readable_value)
{
  int ret = OB_SUCCESS;
  int64_t macro_id_str_len = 0;
  if (OB_ISNULL(value_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "value buf is null", KR(ret));
  } else if (FALSE_IT(macro_id_str_len = macro_id.to_string(value_buf_, MAX_VALUE_LEN))) {
  } else if (OB_UNLIKELY(macro_id_str_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to print macro id", KR(ret), K(macro_id), K(macro_id_str_len));
  } else if (OB_UNLIKELY(macro_id_str_len >= (MAX_VALUE_LEN - 1))) {
    ret = OB_SIZE_OVERFLOW;
    SERVER_LOG(WARN, "macro id str size overflow", KR(ret), K(macro_id),
        K(macro_id_str_len), K(MAX_VALUE_LEN));
  } else {
    readable_value.assign_ptr(value_buf_, static_cast<int32_t>(macro_id_str_len));
  }
  return ret;
}

int ObAllVirtualSharedGCTask::convert_fail_info_(
    const ObString &raw_value,
    ObString &readable_value)
{
  int ret = OB_SUCCESS;
  int64_t print_pos = 0;
  ObSharedGCFailInfo fail_info;
  if (OB_ISNULL(value_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "value buf is null", KR(ret));
  } else if (OB_UNLIKELY(raw_value.empty())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "shared gc task value is empty", KR(ret));
  } else if (OB_FAIL(fail_info.from_value(raw_value))) {
    SERVER_LOG(WARN, "fail to deserialize fail info", KR(ret));
  } else if (OB_FAIL(databuff_printf(value_buf_, MAX_VALUE_LEN, print_pos, fail_info))) {
    SERVER_LOG(WARN, "fail to print fail info", KR(ret));
  } else {
    readable_value.assign_ptr(value_buf_, static_cast<int32_t>(print_pos));
  }
  return ret;
}

int ObAllVirtualSharedGCTask::convert_ls_meta_info_(
    const ObString &raw_value,
    ObString &readable_value)
{
  int ret = OB_SUCCESS;
  int64_t print_pos = 0;
  ObSharedGCLSMetaInfo ls_meta_info;
  if (OB_ISNULL(value_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "value buf is null", KR(ret));
  } else if (OB_UNLIKELY(raw_value.empty())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "shared gc task value is empty", KR(ret));
  } else if (OB_FAIL(ls_meta_info.from_value(raw_value))) {
    SERVER_LOG(WARN, "fail to deserialize ls meta info", KR(ret));
  } else if (OB_FAIL(databuff_printf(value_buf_, MAX_VALUE_LEN, print_pos, ls_meta_info))) {
    SERVER_LOG(WARN, "fail to print ls meta info", KR(ret));
  } else {
    readable_value.assign_ptr(value_buf_, static_cast<int32_t>(print_pos));
  }
  return ret;
}

int ObAllVirtualSharedGCTask::convert_delete_tenant_value_(
    const ObString &raw_value,
    ObString &readable_value)
{
  int ret = OB_SUCCESS;
  int64_t print_pos = 0;
  ObDeleteTenantTaskValue task_value;
  if (OB_ISNULL(value_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "value buf is null", KR(ret));
  } else if (OB_UNLIKELY(raw_value.empty())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "delete tenant task value is empty", KR(ret));
  } else if (OB_FAIL(task_value.from_value(raw_value))) {
    SERVER_LOG(WARN, "fail to deserialize delete tenant task value", KR(ret));
  } else if (OB_UNLIKELY(!task_value.is_valid())) {
    ret = OB_INVALID_DATA;
    SERVER_LOG(WARN, "invalid delete tenant task value", KR(ret), K(task_value));
  } else if (OB_FAIL(databuff_printf(value_buf_, MAX_VALUE_LEN, print_pos, task_value))) {
    SERVER_LOG(WARN, "fail to print delete tenant task value", KR(ret));
  } else {
    readable_value.assign_ptr(value_buf_, static_cast<int32_t>(print_pos));
  }
  return ret;
}

int ObAllVirtualSharedGCTask::convert_delete_server_value_(
    const ObString &raw_value,
    ObString &readable_value)
{
  int ret = OB_SUCCESS;
  int64_t print_pos = 0;
  ObDeleteServerTaskValue task_value;
  if (OB_ISNULL(value_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "value buf is null", KR(ret));
  } else if (OB_UNLIKELY(raw_value.empty())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "delete server task value is empty", KR(ret));
  } else if (OB_FAIL(task_value.from_value(raw_value))) {
    SERVER_LOG(WARN, "fail to deserialize delete server task value", KR(ret));
  } else if (OB_UNLIKELY(!task_value.is_valid())) {
    ret = OB_INVALID_DATA;
    SERVER_LOG(WARN, "invalid delete server task value", KR(ret), K(task_value));
  } else if (OB_FAIL(databuff_printf(value_buf_, MAX_VALUE_LEN, print_pos, task_value))) {
    SERVER_LOG(WARN, "fail to print delete server task value", KR(ret));
  } else {
    readable_value.assign_ptr(value_buf_, static_cast<int32_t>(print_pos));
  }
  return ret;
}

int ObAllVirtualSharedGCTask::fetch_next_task_()
{
  int ret = OB_SUCCESS;
  task_allocator_.reset(); // clear for the previous task
  cur_task_.reset();
  cur_macro_id_list_.reset();
  cur_macro_idx_ = 0;
  cur_gmt_create_ = 0;
  cur_gmt_modified_ = 0;
  if (OB_FAIL(task_iter_.get_next(task_allocator_, cur_task_, cur_gmt_create_, cur_gmt_modified_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next shared gc task row", KR(ret));
    }
  } else if (is_mark_sweep_gc_type(cur_task_.key_.type_)
             || is_fail_gc_macro_id_type(cur_task_.key_.type_)) {
    // Object-list types share per-object expansion.
    if (OB_UNLIKELY(cur_task_.value_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "shared gc task value is empty", KR(ret), K(cur_task_.key_));
    } else if (OB_FAIL(cur_macro_id_list_.from_value(cur_task_.value_))) {
      SERVER_LOG(WARN, "fail to deserialize macro id list", KR(ret), K(cur_task_.key_));
    }
  } else if (is_fail_gc_type(cur_task_.key_.type_)
             || is_ls_meta_gc_type(cur_task_.key_.type_)
             || is_ls_ckpt_gc_type(cur_task_.key_.type_)
             || is_delete_tenant_type(cur_task_.key_.type_)
             || is_delete_server_type(cur_task_.key_.type_)
             || is_mark_sweep_gc_diag_type(cur_task_.key_.type_)
             || is_mark_sweep_gc_stat_type(cur_task_.key_.type_)) {
    // do nothing, single-row value decoded lazily in process_curr_tenant_.
  } else {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected shared gc task type", KR(ret), K(cur_task_.key_));
  }
  return ret;
}

int ObAllVirtualSharedGCTask::fill_cells_(
    const ObSharedGCTask &task,
    const int64_t gmt_create,
    const int64_t gmt_modified,
    const ObString &value_str)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && (i < col_count); ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID: {
        cur_row_.cells_[i].set_int(task.key_.tenant_id_);
        break;
      }
      case TYPE: {
        cur_row_.cells_[i].set_varchar(ob_shared_gc_task_type_to_string(task.key_.type_));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case ROUND_ID: {
        cur_row_.cells_[i].set_int(task.key_.round_id_);
        break;
      }
      case TASK_ID: {
        cur_row_.cells_[i].set_int(task.key_.task_id_);
        break;
      }
      case GMT_CREATE: {
        cur_row_.cells_[i].set_timestamp(gmt_create);
        break;
      }
      case GMT_MODIFIED: {
        cur_row_.cells_[i].set_timestamp(gmt_modified);
        break;
      }
      case VALUE: {
        cur_row_.cells_[i].set_varchar(value_str);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case EXTRA_INFO: {
        // FAIL_GC_MACRO_ID rows carry "fail_gc_task_id=<id>" here to link back
        // to the source FAIL_GC task; other types leave it null.
        if (task.extra_info_.empty()) {
          cur_row_.cells_[i].set_null();
        } else {
          cur_row_.cells_[i].set_varchar(task.extra_info_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case SERVER_ID: {
        if (ObSharedGCTask::INVALID_SERVER_ID == task.server_id_) {
          cur_row_.cells_[i].set_null();
        } else {
          cur_row_.cells_[i].set_int(task.server_id_);
        }
        break;
      }
      case TRACE_ID: {
        if (task.trace_id_.empty()) {
          cur_row_.cells_[i].set_null();
        } else {
          cur_row_.cells_[i].set_varchar(task.trace_id_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid col_id", KR(ret), K(col_id));
        break;
      }
    } // end switch (col_id)
  }
  return ret;
}

int ObAllVirtualSharedGCTask::process_curr_tenant_(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  // The iterator routes its inner SQL by this explicit tenant id, so it works
  // even when the tenant has no unit on this server (no tenant context switch).
  const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
  if (OB_ISNULL(table_name_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "table_name should not be NULL", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "sql proxy is null", KR(ret));
  } else if (!task_iter_opened_) {
    // Push down the type / round_id predicates parsed from key_ranges_. Each filter
    // carries its own enable flag (a disabled one means "scan that dimension fully"),
    // and the upper SQL operator still applies the exact predicate, so this is safe.
    if (OB_FAIL(task_iter_.init(tenant_id, table_name_, type_filter_, round_id_filter_,
        *GCTX.sql_proxy_))) {
      SERVER_LOG(WARN, "fail to init shared gc task full iterator", KR(ret),
          K(tenant_id), K(table_name_));
    } else {
      task_iter_opened_ = true;
    }
  }
  // Lazily allocate the readable value buffer once and reuse it for every row.
  if (OB_SUCC(ret) && OB_ISNULL(value_buf_)) {
    if (OB_ISNULL(value_buf_ = static_cast<char *>(row_allocator_.alloc(MAX_VALUE_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc value buf", KR(ret), K(MAX_VALUE_LEN));
    }
  }
  // Macro-list tasks expand per object until the current list ends.
  while (OB_SUCC(ret) && OB_ISNULL(row)) {
    if (!has_pending_task_) {
      if (OB_FAIL(fetch_next_task_())) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to fetch next shared gc task", KR(ret));
        }
      } else if (is_mark_sweep_gc_type(cur_task_.key_.type_)
                 || is_fail_gc_macro_id_type(cur_task_.key_.type_)) {
        if (OB_UNLIKELY(cur_macro_id_list_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "macro id list is empty", KR(ret), K(cur_task_.key_));
        } else {
          has_pending_task_ = true;
        }
      } else if (is_fail_gc_type(cur_task_.key_.type_)
                 || is_ls_meta_gc_type(cur_task_.key_.type_)
                 || is_ls_ckpt_gc_type(cur_task_.key_.type_)
                 || is_delete_tenant_type(cur_task_.key_.type_)
                 || is_delete_server_type(cur_task_.key_.type_)
                 || is_mark_sweep_gc_diag_type(cur_task_.key_.type_)
                 || is_mark_sweep_gc_stat_type(cur_task_.key_.type_)) {
        has_pending_task_ = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected shared gc task type", KR(ret), K(cur_task_.key_));
      }
    }
    if (OB_SUCC(ret) && has_pending_task_) {
      ObString readable_value;
      if (is_mark_sweep_gc_type(cur_task_.key_.type_)
          || is_fail_gc_macro_id_type(cur_task_.key_.type_)) {
        if (OB_UNLIKELY(cur_macro_idx_ >= cur_macro_id_list_.count())) { // defence
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "macro idx out of range", KR(ret), K(cur_task_.key_), K_(cur_macro_idx),
              "macro_id_list_count", cur_macro_id_list_.count());
        } else if (OB_FAIL(convert_macro_id_(cur_macro_id_list_.at(cur_macro_idx_), readable_value))) {
          SERVER_LOG(WARN, "fail to convert macro id", KR(ret), K(cur_task_.key_), K_(cur_macro_idx));
        } else if (OB_FAIL(fill_cells_(cur_task_, cur_gmt_create_, cur_gmt_modified_, readable_value))) {
          SERVER_LOG(WARN, "fail to fill cells", KR(ret), K(cur_task_.key_));
        } else {
          ++cur_macro_idx_;
          if (cur_macro_idx_ >= cur_macro_id_list_.count()) {
            has_pending_task_ = false;
          }
          row = &cur_row_;
        }
      } else if (is_mark_sweep_gc_stat_type(cur_task_.key_.type_)
                 || is_mark_sweep_gc_diag_type(cur_task_.key_.type_)) {
        if (OB_FAIL(fill_cells_(cur_task_, cur_gmt_create_, cur_gmt_modified_, cur_task_.value_))) {
          SERVER_LOG(WARN, "fail to fill mark sweep gc task cells", KR(ret), K(cur_task_.key_));
        } else {
          has_pending_task_ = false;
          row = &cur_row_;
        }
      } else if (is_fail_gc_type(cur_task_.key_.type_)) {
        if (OB_FAIL(convert_fail_info_(cur_task_.value_, readable_value))) {
          SERVER_LOG(WARN, "fail to convert fail info", KR(ret), K(cur_task_.key_));
        } else if (OB_FAIL(fill_cells_(cur_task_, cur_gmt_create_, cur_gmt_modified_, readable_value))) {
          SERVER_LOG(WARN, "fail to fill cells", KR(ret), K(cur_task_.key_));
        } else {
          has_pending_task_ = false;
          row = &cur_row_;
        }
      } else if (is_ls_meta_gc_type(cur_task_.key_.type_)
                 || is_ls_ckpt_gc_type(cur_task_.key_.type_)) {
        if (OB_FAIL(convert_ls_meta_info_(cur_task_.value_, readable_value))) {
          SERVER_LOG(WARN, "fail to convert ls meta info", KR(ret), K(cur_task_.key_));
        } else if (OB_FAIL(fill_cells_(cur_task_, cur_gmt_create_, cur_gmt_modified_, readable_value))) {
          SERVER_LOG(WARN, "fail to fill cells", KR(ret), K(cur_task_.key_));
        } else {
          has_pending_task_ = false;
          row = &cur_row_;
        }
      } else if (is_delete_tenant_type(cur_task_.key_.type_)) {
        if (OB_FAIL(convert_delete_tenant_value_(cur_task_.value_, readable_value))) {
          SERVER_LOG(WARN, "fail to convert delete tenant task value", KR(ret), K(cur_task_.key_));
        } else if (OB_FAIL(fill_cells_(cur_task_, cur_gmt_create_, cur_gmt_modified_, readable_value))) {
          SERVER_LOG(WARN, "fail to fill cells", KR(ret), K(cur_task_.key_));
        } else {
          has_pending_task_ = false;
          row = &cur_row_;
        }
      } else if (is_delete_server_type(cur_task_.key_.type_)) {
        if (OB_FAIL(convert_delete_server_value_(cur_task_.value_, readable_value))) {
          SERVER_LOG(WARN, "fail to convert delete server task value", KR(ret), K(cur_task_.key_));
        } else if (OB_FAIL(fill_cells_(cur_task_, cur_gmt_create_, cur_gmt_modified_, readable_value))) {
          SERVER_LOG(WARN, "fail to fill cells", KR(ret), K(cur_task_.key_));
        } else {
          has_pending_task_ = false;
          row = &cur_row_;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected shared gc task type", KR(ret), K(cur_task_.key_));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
