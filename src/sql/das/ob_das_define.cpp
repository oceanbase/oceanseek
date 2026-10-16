/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_define.h"
#include "pl/ob_pl.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_bit_vector.h"
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"
#include "storage/blocksstable/ob_datum_row.h"


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

//not assign array member
void ObDASTableLocMeta::light_assign(const ObDASTableLocMeta &other)
{
  table_loc_id_ = other.table_loc_id_;
  ref_table_id_ = other.ref_table_id_;
  flags_ = other.flags_;
  route_policy_ = other.route_policy_;
}

int ObDASTableLocMeta::assign(const ObDASTableLocMeta &other)
{
  int ret = OB_SUCCESS;
  light_assign(other);
  ret = related_table_ids_.assign(other.related_table_ids_);
  return ret;
}

int ObDASTableLocMeta::init_related_meta(uint64_t related_table_id,
                                         ObDASTableLocMeta &related_meta) const
{
  int ret = OB_SUCCESS;
  related_meta.light_assign(*this);
  related_meta.ref_table_id_ = related_table_id;
  related_meta.related_table_ids_.set_capacity(related_table_ids_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < related_table_ids_.count(); ++i) {
    //the related table ids of other table meta are the other related table id and source table id
    uint64_t tmp_related_id = (related_table_ids_.at(i) == related_table_id ?
                              ref_table_id_ : related_table_ids_.at(i));
    ret = related_meta.related_table_ids_.push_back(tmp_related_id);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDASTableLocMeta,
                    table_loc_id_,
                    ref_table_id_,
                    related_table_ids_,
                    flags_,
                    route_policy_);

OB_SERIALIZE_MEMBER(ObDASTabletLoc,
                    tablet_id_,
                    ls_id_,
                    server_,
                    flags_,
                    partition_id_,
                    first_level_part_id_);

OB_DEF_SERIALIZE(ObDASTableLoc)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(*loc_meta_);
  OB_UNIS_ENCODE(flags_);
  OB_UNIS_ENCODE(tablet_locs_.size());
  FOREACH_X(tmp_node, tablet_locs_, OB_SUCC(ret)) {
    ObDASTabletLoc *tablet_loc = *tmp_node;
    OB_UNIS_ENCODE(*tablet_loc);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASTableLoc)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  void *meta_buf = allocator_.alloc(sizeof(ObDASTableLocMeta));
  ObDASTableLocMeta *loc_meta = nullptr;
  if (OB_ISNULL(meta_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate table loc meta failed", K(ret));
  } else {
    loc_meta = new(meta_buf) ObDASTableLocMeta(allocator_);
    loc_meta_ = loc_meta;
  }
  OB_UNIS_DECODE(*loc_meta);
  OB_UNIS_DECODE(flags_);
  OB_UNIS_DECODE(size);
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    ObDASTabletLoc *tablet_loc = nullptr;
    void *tablet_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(tablet_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc buf failed", K(ret));
    } else {
      tablet_loc = new(tablet_buf) ObDASTabletLoc();
      tablet_loc->loc_meta_ = loc_meta_;
      if (OB_FAIL(tablet_locs_.push_back(tablet_loc))) {
        LOG_WARN("store tablet locs failed", K(ret));
      }
    }
    OB_UNIS_DECODE(*tablet_loc);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASTableLoc)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(*loc_meta_);
  OB_UNIS_ADD_LEN(flags_);
  OB_UNIS_ADD_LEN(tablet_locs_.size());
  FOREACH(tmp_node, tablet_locs_) {
    ObDASTabletLoc *tablet_loc = *tmp_node;
    OB_UNIS_ADD_LEN(*tablet_loc);
  }
  return len;
}

int ObDASTableLoc::assign(const ObCandiTableLoc &candi_table_loc)
{
  int ret = OB_SUCCESS;
  const ObCandiTabletLocIArray &candi_tablet_locs = candi_table_loc.get_phy_part_loc_info_list();
  ObLSReplicaLocation replica_loc;
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_tablet_locs.count(); ++i) {
    replica_loc.reset();
    const ObCandiTabletLoc &candi_tablet_loc = candi_tablet_locs.at(i);
    const ObOptTabletLoc &opt_tablet_loc = candi_tablet_loc.get_partition_location();
    ObDASTabletLoc *tablet_loc = nullptr;
    void *tablet_buf = allocator_.alloc(sizeof(ObDASTabletLoc));
    if (OB_ISNULL(tablet_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate tablet loc buf failed", K(ret));
    } else if (OB_ISNULL(tablet_loc = new(tablet_buf) ObDASTabletLoc())) {
      //do nothing
    } else if (OB_FAIL(candi_tablet_loc.get_selected_replica(replica_loc))) {
      LOG_WARN("fail to get selected replica", K(ret), K(candi_tablet_loc));
    } else {
      tablet_loc->server_ = replica_loc.get_server();
      tablet_loc->tablet_id_ = opt_tablet_loc.get_tablet_id();
      tablet_loc->ls_id_ = opt_tablet_loc.get_ls_id();
      tablet_loc->loc_meta_ = loc_meta_;
      tablet_loc->partition_id_ = opt_tablet_loc.get_partition_id();
      tablet_loc->first_level_part_id_ = opt_tablet_loc.get_first_level_part_id();
      if (OB_FAIL(tablet_locs_.push_back(tablet_loc))) {
        LOG_WARN("store tablet loc failed", K(ret), K(tablet_loc));
      }
    }
  }
  LOG_DEBUG("das table loc assign", K(candi_table_loc), KPC_(loc_meta), K(tablet_locs_));
  return ret;
}

int ObDASTableLoc::get_tablet_loc_by_id(const ObTabletID &tablet_id,
                                        ObDASTabletLoc *&tablet_loc)
{
  int ret = OB_SUCCESS;
  tablet_loc = NULL;
  lookup_cnt_++;
  if (tablet_locs_map_.created()) {
    if (OB_FAIL(tablet_locs_map_.get(tablet_id, tablet_loc))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("look up from hash map failed", KR(ret), K(tablet_id));
      }
    }
  }
  if (OB_SUCC(ret) && NULL != tablet_loc) {
    // found in hash map
  } else if (OB_HASH_NOT_EXIST == ret) {
    // key not found
    ret = OB_SUCCESS;
  } else {
    FOREACH(tmp_node, tablet_locs_) {
      ObDASTabletLoc *tmp_tablet_loc = *tmp_node;
      if (tmp_tablet_loc->tablet_id_ == tablet_id) {
        tablet_loc = tmp_tablet_loc;
        break;
      }
    }
  }
  if (OB_FAIL(ret) || tablet_locs_map_.created()) {
    // do nothing
  } else if (lookup_cnt_ > DAS_TABLET_LOC_LOOKUP_THRESHOLD
             && tablet_locs_.size() > DAS_TABLET_LOC_SIZE_THRESHOLD
             && OB_FAIL(create_tablet_locs_map())) {
    LOG_WARN("create tablet locs hash map failed", KR(ret));
  }
  return ret;
}

int ObDASTableLoc::add_tablet_loc(ObDASTabletLoc *tablet_loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_loc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet loc is null", KR(ret), KP(tablet_loc));
  } else if (OB_FAIL(tablet_locs_.push_back(tablet_loc))) {
    LOG_WARN("push into tablet locs list failed", KR(ret), KPC(tablet_loc));
  } else if (tablet_locs_map_.created()) {
    if (OB_FAIL(tablet_locs_map_.set(tablet_loc->tablet_id_, tablet_loc))) {
      LOG_WARN("insert into tablet locs map failed", KR(ret), KPC(tablet_loc));
    }
  }
  return ret;
}

int TabletHashMap::create(int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= bucket_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket number", KR(ret), K(bucket_num));
  } else if (created()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("hash map was already created", KR(ret));
  } else if (FALSE_IT(bucket_num = hash::cal_next_prime(bucket_num))) {
  } else if (OB_ISNULL(buckets_ = static_cast<TabletHashNode **>(
          allocator_.alloc(bucket_num * sizeof(TabletHashNode *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buckets failed", KR(ret));
  } else {
    MEMSET(buckets_, 0, bucket_num * sizeof(TabletHashNode *));
    bucket_num_ = bucket_num;
    is_inited_ = true;
  }
  return ret;
}

int TabletHashMap::find_node(const ObTabletID key,
                             TabletHashNode *head,
                             TabletHashNode *&node) const
{
  int ret = OB_SUCCESS;
  node = NULL;
  if (!created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash map was not created", KR(ret));
  } else if (NULL == head) {
    // do nothing
  } else {
    TabletHashNode *cur = head;
    while (NULL != cur && NULL == node) {
      if (cur->key_ == key) {
        node = cur;
      } else {
        cur = cur->next_;
      }
    }
  }
  return ret;
}

int TabletHashMap::set(const ObTabletID key, ObDASTabletLoc *value)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (!created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash map was not created", KR(ret));
  } else if (OB_FAIL(key.hash(hash_val))) {
    LOG_WARN("hash failed", KR(ret));
  } else {
    TabletHashNode *&bucket = buckets_[hash_val % bucket_num_];
    TabletHashNode *dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", KR(ret));
    } else if (NULL != dst_node) {
      ret = OB_HASH_EXIST;
      LOG_WARN("key already exists", KR(ret), K(key), KP(value));
    } else {
      TabletHashNode *new_node = static_cast<TabletHashNode *>(
              allocator_.alloc(sizeof(TabletHashNode)));
      if (OB_ISNULL(new_node)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc new node failed", KR(ret));
      } else {
        new_node->key_ = key;
        new_node->value_ = value;
        new_node->next_ = bucket;
        bucket = new_node;
      }
    }
  }
  return ret;
}

int TabletHashMap::get(const ObTabletID key, ObDASTabletLoc *&value)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (!created()) {
    ret = OB_NOT_INIT;
    LOG_WARN("hash map was not created", KR(ret));
  } else if (OB_FAIL(key.hash(hash_val))) {
    LOG_WARN("hash failed", KR(ret));
  } else {
    TabletHashNode *&bucket = buckets_[hash_val % bucket_num_];
    TabletHashNode *dst_node = NULL;
    if (OB_FAIL(find_node(key, bucket, dst_node))) {
      LOG_WARN("find node failed", KR(ret));
    } else if (NULL == dst_node) {
      ret = OB_HASH_NOT_EXIST;
    } else {
      value = dst_node->value_;
    }
  }
  return ret;
}

int ObDASTableLoc::create_tablet_locs_map()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_locs_map_.created())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet locs map was already created", KR(ret), K(tablet_locs_map_.created()));
  } else if (OB_FAIL(tablet_locs_map_.create(DAS_TABLET_LOC_MAP_BUCKET_SIZE))) {
    LOG_WARN("create tablet locs map failed", KR(ret));
  } else {
    for (DASTabletLocListIter tablet_node = tablet_locs_begin();
         OB_SUCC(ret) && tablet_node != tablet_locs_end(); ++tablet_node) {
      ObDASTabletLoc *tablet_loc = *tablet_node;
      if (OB_FAIL(tablet_locs_map_.set(tablet_loc->tablet_id_, tablet_loc))) {
        LOG_WARN("insert into tablet locs map failed", KR(ret), KPC(tablet_loc));
      }
    }
  }
  return ret;
}

#define OB_LOGICAL_COLUMN_MAPPING_ENTRY_DO_CODE(CODE, src) \
  LST_DO_CODE(CODE, \
    src, \
    source_idx_, \
    column_name_, \
    key_suffix_ordinal_, \
    json_path_, \
    logical_column_id_, \
    is_vector_col_, \
    is_rowid_, \
    is_query_range_ns_)

OB_DEF_SERIALIZE(ObLogicalColumnMappingEntry)
{
  int ret = OB_SUCCESS;
  int8_t src = static_cast<int8_t>(source_);
  OB_LOGICAL_COLUMN_MAPPING_ENTRY_DO_CODE(OB_UNIS_ENCODE, src);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(obj_meta_.serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize obj_meta failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLogicalColumnMappingEntry)
{
  int ret = OB_SUCCESS;
  int8_t src = 0;
  OB_LOGICAL_COLUMN_MAPPING_ENTRY_DO_CODE(OB_UNIS_DECODE, src);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(src < 0 || src >= static_cast<int8_t>(FROM_SOURCE_MAX))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid logical column source", K(ret), K(src));
    } else {
      source_ = static_cast<ObLogicalColumnSource>(src);
      if (OB_FAIL(obj_meta_.deserialize(buf, data_len, pos))) {
        LOG_WARN("deserialize obj_meta failed", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogicalColumnMappingEntry)
{
  int64_t len = 0;
  int8_t src = static_cast<int8_t>(source_);
  OB_LOGICAL_COLUMN_MAPPING_ENTRY_DO_CODE(OB_UNIS_ADD_LEN, src);
  len += obj_meta_.get_serialize_size();
  return len;
}

#undef OB_LOGICAL_COLUMN_MAPPING_ENTRY_DO_CODE

OB_DEF_SERIALIZE(ObLogicalTableColumnMapping)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, entries_, index_seq_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLogicalTableColumnMapping)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, entries_, index_seq_);
  if (OB_SUCC(ret) && OB_FAIL(deep_copy_strings())) {
    LOG_WARN("deep copy logical column mapping strings failed", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogicalTableColumnMapping)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, entries_, index_seq_);
  return len;
}

int ObLogicalTableColumnMapping::deep_copy_strings()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    // Allocator is only required when there is content to copy. A null allocator
    // with non-empty entries means a serialized path forgot to bind one --
    // treat it as a bug instead of leaving dangling pointers.
    if (OB_UNLIKELY(!entries_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing allocator for logical column mapping deep copy",
               K(ret), K(entries_.count()));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < entries_.count(); ++i) {
      ObLogicalColumnMappingEntry &entry = entries_.at(i);
      if (!entry.column_name_.empty()) {
        common::ObString copied;
        if (OB_FAIL(ob_write_string(*allocator_, entry.column_name_, copied))) {
          LOG_WARN("deep copy column_name failed", K(ret), K(i));
        } else {
          entry.column_name_ = copied;
        }
      }
      if (OB_SUCC(ret) && !entry.json_path_.empty()) {
        common::ObString copied;
        if (OB_FAIL(ob_write_string(*allocator_, entry.json_path_, copied))) {
          LOG_WARN("deep copy json_path failed", K(ret), K(i));
        } else {
          entry.json_path_ = copied;
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLogicalTableScanCtDef,
                    ns_id_expr_,
                    lt_id_start_expr_,
                    lt_id_end_expr_,
                    rowid_start_expr_,
                    rowid_end_expr_,
                    kv_key_expr_,
                    kv_value_expr_,
                    lt_start_inclusive_,
                    lt_end_inclusive_,
                    has_ns_pred_,
                    has_lt_pred_,
                    start_inclusive_,
                    end_inclusive_,
                    physical_data_table_id_,
                    physical_namespace_col_id_,
                    physical_key_col_id_,
                    physical_value_col_id_,
                    logical_ref_table_id_,
                    ivf_vec_project_dim_,
                    analyzed_);

OB_SERIALIZE_MEMBER(ObLogicalTableRtDef,
                    namespace_id_,
                    logical_table_id_);

int ObLogicalTableRtDef::require_resolved_ns_lt(const ObLogicalTableRtDef &lt_rt,
                                                  const char *caller)
{
  int ret = OB_SUCCESS;
  if (!lt_rt.has_resolved_ns_lt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical table rtdef missing resolved ns/lt", K(ret), K(caller), K(lt_rt));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDASPushDownTopN,
                    order_type_,
                    sort_key_,
                    limit_expr_,
                    offset_expr_,
                    with_ties_,
                    is_push_into_index_);

int ObDASPushDownTopN::prepare_limit_param(ObEvalCtx &eval_ctx,
                                           const ObDASPushDownTopN &push_down_topn,
                                           ObLimitParam &limit_param)
{
  int ret = OB_SUCCESS;
  if (nullptr != push_down_topn.limit_expr_) {
    ObDatum *limit_datum = nullptr;
    if (OB_FAIL(push_down_topn.limit_expr_->eval(eval_ctx, limit_datum))) {
      LOG_WARN("failed to eval limit expr", K(ret));
    } else {
      limit_param.limit_ = (limit_datum->is_null() || limit_datum->get_int() < 0) ? 0 : limit_datum->get_int();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (nullptr != push_down_topn.offset_expr_) {
    ObDatum *offset_datum = nullptr;
    if (OB_FAIL(push_down_topn.offset_expr_->eval(eval_ctx, offset_datum))) {
      LOG_WARN("failed to eval limit expr", K(ret));
    } else if (offset_datum->is_null()) {
      limit_param.limit_ = 0;
      limit_param.offset_ = 0;
    } else {
      limit_param.offset_ = offset_datum->get_int() < 0 ? 0 : offset_datum->get_int();
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLogicalTableTabletMapping,
                    logical_tablet_id_,
                    logical_part_id_,
                    logical_first_level_part_id_,
                    physical_tablet_id_,
                    physical_part_id_,
                    physical_first_level_part_id_);

// ========== ObLogicalTableHelper encode_physical_row ==========

int ObLogicalTableHelper::build_ordered_key_datums_(
    const ObSEArray<ObDatum, 1> &rowid_datums,
    const ObSEArray<int64_t, 4> &suffix_ordinals,
    const ObSEArray<ObDatum, 4> &suffix_datums,
    ObSEArray<ObDatum, 4> &key_datums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rowid_datums.count() > 1
      || suffix_ordinals.count() != suffix_datums.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical table key datum layout",
             K(ret), K(rowid_datums.count()), K(suffix_ordinals.count()), K(suffix_datums.count()));
  } else {
    if (1 == rowid_datums.count() && OB_FAIL(key_datums.push_back(rowid_datums.at(0)))) {
      LOG_WARN("push rowid datum failed", K(ret));
    }
    for (int64_t ordinal = 0; OB_SUCC(ret) && ordinal < suffix_datums.count(); ++ordinal) {
      int64_t found_idx = -1;
      for (int64_t i = 0; found_idx < 0 && i < suffix_ordinals.count(); ++i) {
        if (suffix_ordinals.at(i) == ordinal) {
          found_idx = i;
        }
      }
      if (found_idx < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("missing key suffix datum", K(ret), K(ordinal), K(suffix_ordinals));
      } else if (OB_FAIL(key_datums.push_back(suffix_datums.at(found_idx)))) {
        LOG_WARN("push key suffix datum failed", K(ret), K(ordinal), K(found_idx));
      }
    }
  }
  return ret;
}

int ObLogicalTableHelper::collect_datums_from_mapping_(
    const ObLogicalTableColumnMapping &mapping,
    const ObIArray<ObExpr *> &logical_exprs,
    ObEvalCtx &eval_ctx,
    ObIAllocator &row_alloc,
    ObSEArray<ObDatum, 4> &key_datums,
    ObSEArray<ObString, 8> &value_col_names,
    ObSEArray<ObString, 8> &value_json_paths,
    ObSEArray<ObObjMeta, 8> &value_metas,
    ObSEArray<ObDatum, 8> &value_datums,
    bool &has_whole_value,
    ObDatum &whole_value_datum)
{
  int ret = OB_SUCCESS;
  has_whole_value = false;
  ObSEArray<ObDatum, 1> rowid_datums;
  ObSEArray<int64_t, 4> suffix_ordinals;
  ObSEArray<ObDatum, 4> suffix_datums;
  for (int64_t i = 0; OB_SUCC(ret) && i < mapping.entries_.count(); ++i) {
    const ObLogicalColumnMappingEntry &entry = mapping.entries_.at(i);
    if (OB_UNLIKELY(entry.source_idx_ < 0
        || entry.source_idx_ >= logical_exprs.count())) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("source_idx out of range", K(ret), K(entry), K(logical_exprs.count()));
    } else {
      ObExpr *expr = logical_exprs.at(entry.source_idx_);
      ObDatum *datum = nullptr;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(i), K(entry));
      } else if (OB_FAIL(expr->eval(eval_ctx, datum))) {
        LOG_WARN("eval expr failed", K(ret), K(i), K(entry));
      } else {
        switch (entry.source_) {
          case FROM_ROWID:
            if (OB_FAIL(rowid_datums.push_back(*datum))) {
              LOG_WARN("push rowid datum failed", K(ret));
            }
            break;
          case FROM_KEY_SUFFIX:
            if (OB_UNLIKELY(entry.key_suffix_ordinal_ < 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid key suffix ordinal", K(ret), K(entry));
            } else if (OB_FAIL(suffix_ordinals.push_back(entry.key_suffix_ordinal_))) {
              LOG_WARN("push key suffix ordinal failed", K(ret));
            } else if (OB_FAIL(suffix_datums.push_back(*datum))) {
              LOG_WARN("push key suffix datum failed", K(ret));
            }
            break;
          case FROM_VALUE_WHOLE:
            if (has_whole_value) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("duplicated whole value mapping entry", K(ret), K(entry));
            } else {
            has_whole_value = true;
            whole_value_datum = *datum;
            }
            break;
          case FROM_VALUE_PATH:
            if (OB_FAIL(value_col_names.push_back(
                    entry.column_name_))) {
              LOG_WARN("push value column name failed", K(ret));
            } else if (OB_FAIL(value_json_paths.push_back(entry.json_path_))) {
              LOG_WARN("push value json path failed", K(ret));
            } else if (OB_FAIL(value_metas.push_back(entry.obj_meta_))) {
              LOG_WARN("push value meta failed", K(ret));
            } else if (OB_FAIL(value_datums.push_back(*datum))) {
              LOG_WARN("push value datum failed", K(ret));
            }
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported column source", K(ret), K(entry));
            break;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (has_whole_value && !value_datums.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("whole value cannot be mixed with value path entries", K(ret), K(mapping));
  } else if (OB_FAIL(build_ordered_key_datums_(
                 rowid_datums, suffix_ordinals, suffix_datums, key_datums))) {
    LOG_WARN("build ordered key datums failed", K(ret));
  }
  return ret;
}

int ObLogicalTableHelper::collect_datums_from_datum_row_(
    const ObLogicalTableColumnMapping &mapping,
    const blocksstable::ObDatumRow &domain_row,
    ObSEArray<ObDatum, 4> &key_datums,
    ObSEArray<ObString, 8> &value_col_names,
    ObSEArray<ObString, 8> &value_json_paths,
    ObSEArray<ObObjMeta, 8> &value_metas,
    ObSEArray<ObDatum, 8> &value_datums,
    bool &has_whole_value,
    ObDatum &whole_value_datum)
{
  int ret = OB_SUCCESS;
  has_whole_value = false;
  ObSEArray<ObDatum, 1> rowid_datums;
  ObSEArray<int64_t, 4> suffix_ordinals;
  ObSEArray<ObDatum, 4> suffix_datums;
  for (int64_t i = 0; OB_SUCC(ret) && i < mapping.entries_.count(); ++i) {
    const ObLogicalColumnMappingEntry &entry = mapping.entries_.at(i);
    if (OB_UNLIKELY(entry.source_idx_ < 0
        || entry.source_idx_ >= domain_row.get_column_count())) {
      ret = OB_INDEX_OUT_OF_RANGE;
      LOG_WARN("source_idx out of range", K(ret), K(entry),
               K(domain_row.get_column_count()));
    } else {
      const ObDatum &datum = domain_row.storage_datums_[entry.source_idx_];
      switch (entry.source_) {
        case FROM_ROWID:
          if (OB_FAIL(rowid_datums.push_back(datum))) {
            LOG_WARN("push rowid datum failed", K(ret));
          }
          break;
        case FROM_KEY_SUFFIX:
          if (OB_UNLIKELY(entry.key_suffix_ordinal_ < 0)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid key suffix ordinal", K(ret), K(entry));
          } else if (OB_FAIL(suffix_ordinals.push_back(entry.key_suffix_ordinal_))) {
            LOG_WARN("push key suffix ordinal failed", K(ret));
          } else if (OB_FAIL(suffix_datums.push_back(datum))) {
            LOG_WARN("push key suffix datum failed", K(ret));
          }
          break;
        case FROM_VALUE_WHOLE:
          if (has_whole_value) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("duplicated whole value mapping entry", K(ret), K(entry));
          } else {
          has_whole_value = true;
          whole_value_datum = datum;
          }
          break;
        case FROM_VALUE_PATH:
          if (OB_FAIL(value_col_names.push_back(
                  entry.column_name_))) {
            LOG_WARN("push value column name failed", K(ret));
          } else if (OB_FAIL(value_json_paths.push_back(entry.json_path_))) {
            LOG_WARN("push value json path failed", K(ret));
          } else if (OB_FAIL(value_metas.push_back(entry.obj_meta_))) {
            LOG_WARN("push value meta failed", K(ret));
          } else if (OB_FAIL(value_datums.push_back(datum))) {
            LOG_WARN("push value datum failed", K(ret));
          }
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported column source", K(ret), K(entry));
          break;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (has_whole_value && !value_datums.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("whole value cannot be mixed with value path entries", K(ret), K(mapping));
  } else if (OB_FAIL(build_ordered_key_datums_(
                 rowid_datums, suffix_ordinals, suffix_datums, key_datums))) {
    LOG_WARN("build ordered key datums failed", K(ret));
  }
  return ret;
}

int ObLogicalTableHelper::encode_key_value_(
    uint64_t namespace_id,
    uint64_t logical_table_id,
    share::ObLogicalTableAuxType aux_type,
    uint64_t index_seq,
    const ObSEArray<ObDatum, 4> &key_datums,
    const ObSEArray<ObString, 8> &value_col_names,
    const ObSEArray<ObString, 8> &value_json_paths,
    const ObSEArray<ObObjMeta, 8> &value_metas,
    const ObSEArray<ObDatum, 8> &value_datums,
    bool has_whole_value,
    const ObDatum &whole_value_datum,
    ObIAllocator &row_alloc,
    ObString &out_key,
    ObString &out_value)
{
  int ret = OB_SUCCESS;
  share::ObLogicalTableKeyPrefix prefix(
      namespace_id,
      logical_table_id,
      index_seq,
      aux_type);
  if (OB_FAIL(share::ObLogicalTableEncoder::encode_key(
          prefix, key_datums, row_alloc, out_key))) {
    LOG_WARN("encode logical table key failed", K(ret), K(prefix));
  } else {
    LOG_TRACE("[LT hybrid DAS][DML_ENCODE_KEY]",
              K(prefix), K(key_datums),
              "out_key_len", out_key.length(),
              "out_key_hex", common::ObHexStringWrap(out_key));
  }
  if (OB_FAIL(ret)) {
  } else if (has_whole_value) {
    // FROM_VALUE_WHOLE: store raw datum bytes directly as value binary
    if (whole_value_datum.is_null()) {
      out_value.reset();
    } else {
      int64_t len = whole_value_datum.len_;
      char *buf = static_cast<char *>(row_alloc.alloc(len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc whole value buf failed", K(ret), K(len));
      } else {
        MEMCPY(buf, whole_value_datum.ptr_, len);
        out_value.assign_ptr(buf, static_cast<int32_t>(len));
      }
    }
  } else if (share::LT_AUX_IVF_CID_ROWID == aux_type) {
    int64_t vector_idx = -1;
    int64_t vector_cnt = 0;
    for (int64_t i = 0; i < value_metas.count(); ++i) {
      if (ob_is_collection_sql_type(value_metas.at(i).get_type())) {
        vector_idx = i;
        ++vector_cnt;
      }
    }
    if (OB_UNLIKELY(value_datums.count() != value_metas.count()
                    || 1 != vector_cnt
                    || vector_idx < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("cid rowid value must contain exactly one vector mapping", K(ret),
               K(value_datums.count()), K(value_metas.count()), K(vector_cnt), K(vector_idx));
    } else if (value_datums.at(vector_idx).is_null()) {
      // A zero-length raw payload is reserved as the NULL-vector sentinel for aux5.
      // A non-NULL vector cannot be empty and is validated in the branch below.
      out_value.reset();
    } else {
      const ObDatum &vector_datum = value_datums.at(vector_idx);
      const ObObjMeta &vector_meta = value_metas.at(vector_idx);
      ObDatumMeta datum_meta;
      ObString raw_vector = vector_datum.get_string();
      datum_meta.type_ = vector_meta.get_type();
      datum_meta.cs_type_ = vector_meta.get_collation_type();
      datum_meta.scale_ = vector_meta.get_scale();
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(
              row_alloc, vector_datum, datum_meta, raw_vector))) {
        LOG_WARN("read raw cid rowid vector failed", K(ret), K(vector_meta));
      } else if (OB_UNLIKELY(raw_vector.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("raw cid rowid vector is empty", K(ret));
      } else {
        char *buf = static_cast<char *>(row_alloc.alloc(raw_vector.length()));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc raw cid rowid vector failed", K(ret), K(raw_vector.length()));
        } else {
          MEMCPY(buf, raw_vector.ptr(), raw_vector.length());
          out_value.assign_ptr(buf, raw_vector.length());
        }
      }
    }
  } else if (value_col_names.empty()) {
    ObSEArray<ObString, 1> empty_names;
    ObSEArray<ObString, 1> empty_paths;
    ObSEArray<ObObjMeta, 1> empty_metas;
    ObSEArray<ObDatum, 1> empty_datums;
    if (OB_FAIL(share::ObLogicalTableEncoder::encode_value(
            empty_names, empty_paths, empty_metas, empty_datums, row_alloc, out_value,
            namespace_id))) {
      LOG_WARN("encode empty value failed", K(ret));
    }
  } else if (OB_FAIL(share::ObLogicalTableEncoder::encode_value(
          value_col_names, value_json_paths, value_metas, value_datums, row_alloc, out_value,
          namespace_id))) {
    LOG_WARN("encode logical table value failed", K(ret));
  }
  return ret;
}

// ========== ObLogicalTableEncoder static methods ==========

int ObLogicalTableEncoder::encode_physical_row(
    uint64_t namespace_id,
    uint64_t logical_table_id,
    uint64_t physical_key_col_id,
    uint64_t physical_value_col_id,
    share::ObLogicalTableAuxType aux_type,
    const ObLogicalTableColumnMapping &mapping,
    const ObIArray<ObExpr *> &logical_exprs,
    ObEvalCtx &eval_ctx,
    ObIAllocator &row_alloc,
    ObString &out_key,
    ObString &out_value)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDatum, 4> key_datums;
  ObSEArray<ObString, 8> value_col_names;
  ObSEArray<ObString, 8> value_json_paths;
  ObSEArray<ObObjMeta, 8> value_metas;
  ObSEArray<ObDatum, 8> value_datums;
  bool has_whole_value = false;
  ObDatum whole_value_datum;
  if (OB_UNLIKELY(common::OB_INVALID_ID == physical_key_col_id
      || common::OB_INVALID_ID == physical_value_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid physical col ids for logical table encoder", K(ret),
             K(physical_key_col_id), K(physical_value_col_id));
  } else if (OB_FAIL(ObLogicalTableHelper::collect_datums_from_mapping_(
          mapping, logical_exprs, eval_ctx, row_alloc,
          key_datums, value_col_names, value_json_paths, value_metas, value_datums,
          has_whole_value, whole_value_datum))) {
    LOG_WARN("collect datums from mapping failed", K(ret));
  } else if (OB_FAIL(ObLogicalTableHelper::encode_key_value_(
          namespace_id, logical_table_id,
          aux_type, mapping.index_seq_, key_datums,
          value_col_names, value_json_paths, value_metas, value_datums,
          has_whole_value, whole_value_datum,
          row_alloc, out_key, out_value))) {
    LOG_WARN("encode key value failed", K(ret));
  }
  return ret;
}

int ObLogicalTableEncoder::encode_physical_row(
    uint64_t namespace_id,
    uint64_t logical_table_id,
    uint64_t physical_key_col_id,
    uint64_t physical_value_col_id,
    share::ObLogicalTableAuxType aux_type,
    const ObLogicalTableColumnMapping &mapping,
    const blocksstable::ObDatumRow &domain_row,
    ObIAllocator &row_alloc,
    ObString &out_key,
    ObString &out_value)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDatum, 4> key_datums;
  ObSEArray<ObString, 8> value_col_names;
  ObSEArray<ObString, 8> value_json_paths;
  ObSEArray<ObObjMeta, 8> value_metas;
  ObSEArray<ObDatum, 8> value_datums;
  bool has_whole_value = false;
  ObDatum whole_value_datum;
  if (OB_UNLIKELY(common::OB_INVALID_ID == physical_key_col_id
      || common::OB_INVALID_ID == physical_value_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid physical col ids for logical table encoder", K(ret),
             K(physical_key_col_id), K(physical_value_col_id));
  } else if (OB_FAIL(ObLogicalTableHelper::collect_datums_from_datum_row_(
          mapping, domain_row,
          key_datums, value_col_names, value_json_paths, value_metas, value_datums,
          has_whole_value, whole_value_datum))) {
    LOG_WARN("collect datums from datum row failed", K(ret));
  } else if (OB_FAIL(ObLogicalTableHelper::encode_key_value_(
          namespace_id, logical_table_id,
          aux_type, mapping.index_seq_, key_datums,
          value_col_names, value_json_paths, value_metas, value_datums,
          has_whole_value, whole_value_datum,
          row_alloc, out_key, out_value))) {
    LOG_WARN("encode key value failed", K(ret));
  }
  return ret;
}

// ========== ObLogicalTableHelper::encode_physical_row thin wrappers ==========
// (Deleted: instance methods removed when ObLogicalTableHelper became static-only.
//  Use ObLogicalTableEncoder::encode_physical_row directly with explicit ns/lt/key/value args.)

common::ObIVector *ObLogicalTableHelper::get_logical_output_vector_(ObExpr &expr, ObEvalCtx &eval_ctx)
{
  return expr.is_batch_result() && expr.enable_rich_format()
      ? expr.get_vector(eval_ctx) : nullptr;
}

template <typename T>
struct ObLogicalOutputSetter
{
};

template <>
struct ObLogicalOutputSetter<uint64_t>
{
  static void set_vector(common::ObIVector &vec, const int64_t row_idx, const uint64_t v)
  {
    vec.set_uint(row_idx, v);
  }
  static void set_datum(ObDatum &datum, const uint64_t v)
  {
    datum.set_uint(v);
  }
};

template <>
struct ObLogicalOutputSetter<int64_t>
{
  static void set_vector(common::ObIVector &vec, const int64_t row_idx, const int64_t v)
  {
    vec.set_int(row_idx, v);
  }
  static void set_datum(ObDatum &datum, const int64_t v)
  {
    datum.set_int(v);
  }
};

template <>
struct ObLogicalOutputSetter<float>
{
  static void set_vector(common::ObIVector &vec, const int64_t row_idx, const float v)
  {
    vec.set_float(row_idx, v);
  }
  static void set_datum(ObDatum &datum, const float v)
  {
    datum.set_float(v);
  }
};

template <>
struct ObLogicalOutputSetter<double>
{
  static void set_vector(common::ObIVector &vec, const int64_t row_idx, const double v)
  {
    vec.set_double(row_idx, v);
  }
  static void set_datum(ObDatum &datum, const double v)
  {
    datum.set_double(v);
  }
};

template <>
struct ObLogicalOutputSetter<ObString>
{
  static void set_vector(common::ObIVector &vec, const int64_t row_idx, const ObString &v)
  {
    vec.set_string(row_idx, v);
  }
  static void set_datum(ObDatum &datum, const ObString &v)
  {
    datum.set_string(v);
  }
};

template <typename T>
static void set_logical_output_value(ObExpr &expr,
                                     const int64_t row_idx,
                                     ObEvalCtx &eval_ctx,
                                     const T &v)
{
  if (expr.is_batch_result() && expr.enable_rich_format()) {
    ObLogicalOutputSetter<T>::set_vector(*expr.get_vector(eval_ctx), row_idx, v);
    expr.set_evaluated_flag(eval_ctx);
  } else {
    ObLogicalOutputSetter<T>::set_datum(expr.locate_expr_datum(eval_ctx, row_idx), v);
  }
}

void ObLogicalTableHelper::set_logical_output_null(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx)
{
  if (expr.is_batch_result() && expr.enable_rich_format()) {
    get_logical_output_vector_(expr, eval_ctx)->set_null(row_idx);
    expr.set_evaluated_flag(eval_ctx);
  } else {
    expr.locate_expr_datum(eval_ctx, row_idx).set_null();
  }
}

void ObLogicalTableHelper::set_logical_output_uint(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                                   const uint64_t v)
{
  set_logical_output_value<uint64_t>(expr, row_idx, eval_ctx, v);
}

void ObLogicalTableHelper::set_logical_output_int(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                                  const int64_t v)
{
  set_logical_output_value<int64_t>(expr, row_idx, eval_ctx, v);
}

void ObLogicalTableHelper::set_logical_output_float(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                                    const float v)
{
  set_logical_output_value<float>(expr, row_idx, eval_ctx, v);
}

void ObLogicalTableHelper::set_logical_output_double(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                                     const double v)
{
  set_logical_output_value<double>(expr, row_idx, eval_ctx, v);
}

int ObLogicalTableHelper::set_logical_output_string(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                                    const ObString &str)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (!str.empty()) {
    // 逻辑列的字符串结果必须归表达式当前 batch 槽所有。该缓冲按 row_idx
    // 复用；若使用 exec_ctx allocator，扫描的每一行都会保留到 SQL 结束。
    if (OB_ISNULL(buf = expr.get_str_res_mem(eval_ctx, str.length(), row_idx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate logical string result memory failed", K(ret), K(row_idx), K(str.length()));
    } else {
      MEMCPY(buf, str.ptr(), str.length());
    }
  }
  if (OB_SUCC(ret)) {
    set_logical_output_value<ObString>(
        expr, row_idx, eval_ctx, ObString(static_cast<int32_t>(str.length()), buf));
  }
  return ret;
}

static void clear_expr_tree_evaluated_flags_for_batch(
    ObExpr &expr,
    ObEvalCtx &eval_ctx,
    const int64_t count)
{
  for (int64_t i = 0; i < expr.arg_cnt_; ++i) {
    ObExpr *arg = expr.args_[i];
    if (OB_NOT_NULL(arg)) {
      clear_expr_tree_evaluated_flags_for_batch(*arg, eval_ctx, count);
    }
  }
  if (expr.is_batch_result() && count > 0) {
    ObBitVector &eval_flags = expr.get_evaluated_flags(eval_ctx);
    for (int64_t r = 0; r < count; ++r) {
      eval_flags.unset(r);
    }
  }
  expr.get_eval_info(eval_ctx).clear_evaluated_flag();
}

void ObLogicalTableHelper::clear_consumer_output_expr_evaluated_flags_for_batch(
    const common::ObIArray<ObExpr *> &output_exprs,
    ObEvalCtx &eval_ctx,
    const int64_t count)
{
  for (int64_t i = 0; i < output_exprs.count(); ++i) {
    ObExpr *expr = output_exprs.at(i);
    if (OB_NOT_NULL(expr)) {
      clear_expr_tree_evaluated_flags_for_batch(*expr, eval_ctx, count);
    }
  }
}

int ObLogicalTableHelper::compact_logical_batch_exprs(
    ObEvalCtx &eval_ctx,
    const common::ObIArray<ObExpr *> &exprs,
    const sql::ObBitVector &skip,
    int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int64_t write_pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
    if (!skip.at(i)) {
      if (write_pos != i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < exprs.count(); ++j) {
          ObExpr *expr = exprs.at(j);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null expr in compact batch", K(ret), K(j));
          } else if (expr->is_batch_result()) {
            ObDatum *datums = expr->locate_batch_datums(eval_ctx);
            datums[write_pos] = datums[i];
          }
        }
      }
      ++write_pos;
    }
  }
  if (OB_SUCC(ret)) {
    row_count = write_pos;
  }
  return ret;
}

void ObLogicalTableHelper::append_unique_expr(
    const ObExpr *expr,
    common::ObIArray<ObExpr *> &exprs)
{
  if (OB_NOT_NULL(expr)) {
    bool found = false;
    for (int64_t i = 0; !found && i < exprs.count(); ++i) {
      found = (exprs.at(i) == expr);
    }
    if (!found) {
      (void)exprs.push_back(const_cast<ObExpr *>(expr));
    }
  }
}

}  // namespace sql
}  // namespace oceanbase
