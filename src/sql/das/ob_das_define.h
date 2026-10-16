/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_DEFINE_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_DEFINE_H_
#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "share/location_cache/ob_location_struct.h"
#include "share/ob_i_tablet_scan.h"
#include "share/logical_table/ob_logical_table_define.h"
#include "share/schema/ob_table_dml_param.h"
#include "lib/json_type/ob_json_path.h"
#include "common/ob_tablet_id.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/logical_table/ob_logical_table_rewrite_ctx.h"
#include "sql/engine/ob_bit_vector.h"

#define DAS_SCAN_OP(_task_op) \
    (::oceanbase::sql::DAS_OP_TABLE_SCAN != (_task_op)->get_type() && \
        ::oceanbase::sql::DAS_OP_TABLE_BATCH_SCAN != (_task_op)->get_type() ? \
        nullptr : static_cast<::oceanbase::sql::ObDASScanOp*>(_task_op))
#define DAS_GROUP_SCAN_OP(_task_op) \
    (::oceanbase::sql::DAS_OP_TABLE_BATCH_SCAN != (_task_op)->get_type() ? \
        nullptr : static_cast<::oceanbase::sql::ObDASGroupScanOp*>(_task_op))

#define IS_DAS_DML_OP(_task_op)                         \
  ({                                                    \
    DAS_OP_TABLE_INSERT == (_task_op).get_type() ||     \
        DAS_OP_TABLE_UPDATE == (_task_op).get_type() || \
        DAS_OP_TABLE_LOCK == (_task_op).get_type() ||   \
        DAS_OP_TABLE_DELETE == (_task_op).get_type();   \
  })

namespace oceanbase
{
namespace common
{
class ObIVector;
}
namespace blocksstable
{
class ObDatumRow;
}
namespace sql
{
class ObDASTaskArg;
class ObIDASTaskOp;
class ObExecContext;
class ObPhysicalPlan;
class ObChunkDatumStore;
class ObEvalCtx;
class ObExpr;
struct ObDASScanCtDef;
struct ObDASScalarScanCtDef;

namespace das
{
//reserve 8K for das write buffer's another structure
const int64_t OB_DAS_MAX_PACKET_SIZE = 2 * 1024 * 1024l - 8 * 1024;
/**
 * Generally, the most common configuration of a cluster is 3 zones.
 * When the leader is randomly distributed,
 * it can be considered that the DAS request will send at most one RPC request to each zone.
 * so OB_DAS_MAX_TOTAL_PACKET_SIZE was defined as:
 */
const int64_t OB_DAS_MAX_TOTAL_PACKET_SIZE = 1 * OB_DAS_MAX_PACKET_SIZE;
const int64_t OB_DAS_MAX_META_TENANT_PACKET_SIZE = 1 * 1024 * 1024l - 8 * 1024;
// offset of das parallel thread_pool group_id
static const int32_t OB_DAS_PARALLEL_POOL_MARK = 1 << 30;
}  // namespace das

enum class ObDasTaskStatus: uint8_t
{
  UNSTART = 0,
  FAILED,
  FINISHED
};

enum ObDASOpType
{
  //can not adjust the order of DASOpType, append OpType at the last
  DAS_OP_INVALID = 0,
  DAS_OP_TABLE_SCAN,
  DAS_OP_TABLE_INSERT,
  DAS_OP_TABLE_UPDATE,
  DAS_OP_TABLE_DELETE,
  DAS_OP_TABLE_LOCK,
  DAS_OP_TABLE_BATCH_SCAN,
  DAS_OP_SPLIT_MULTI_RANGES,
  DAS_OP_GET_RANGES_COST,
  DAS_OP_TABLE_LOOKUP,
  DAS_OP_IR_SCAN,
  DAS_OP_IR_AUX_LOOKUP,
  DAS_OP_SORT,
  DAS_OP_VEC_SCAN,
  DAS_OP_VID_MERGE,
  DAS_OP_INDEX_MERGE,
  DAS_OP_DOC_ID_MERGE,
  DAS_OP_FUNC_LOOKUP,
  DAS_OP_INDEX_PROJ_LOOKUP,
  DAS_OP_DOMAIN_ID_MERGE,
  DAS_OP_IR_ES_MATCH,
  DAS_OP_IR_ES_SCORE,
  DAS_OP_SCALAR_QUERY,
  DAS_OP_SCALAR_SCAN_QUERY,
  DAS_OP_FUSION_QUERY,
  DAS_OP_BOOLEAN_QUERY,
  DAS_OP_MATCH_QUERY,
  DAS_OP_MATCH_PHRASE_QUERY,
  DAS_OP_MULTI_MATCH_QUERY,
  DAS_OP_QUERY_STRING_QUERY,
  DAS_OP_VEC_INDEX_DRIVER,
  DAS_OP_VEC_INDEX_SCAN,
  DAS_OP_VEC_INDEX_HNSW_SCAN,
  DAS_OP_VEC_INDEX_IVF_SCAN,
  DAS_OP_TOPK_COLLECT,
  DAS_OP_NESTED_QUERY,
  DAS_OP_VEC_INDEX_BRUTE_SCAN,
  DAS_OP_VEC_INDEX_ASYNC_IVF_SCAN,
  //append OpType before me
  DAS_OP_MAX
};

typedef common::ObFixedArray<common::ObTableID, common::ObIAllocator> DASTableIDArray;

typedef common::ObIArrayWrap<common::ObTableID> DASTableIDArrayWrap;

struct ObDASTableLocMeta
{
  OB_UNIS_VERSION(1);
public:
  ObDASTableLocMeta(common::ObIAllocator &alloc)
    : table_loc_id_(common::OB_INVALID_ID),
      ref_table_id_(common::OB_INVALID_ID),
      related_table_ids_(alloc),
      flags_(0),
      route_policy_(0)
  { }
  ~ObDASTableLocMeta() = default;
  int assign(const ObDASTableLocMeta &other);
  int init_related_meta(uint64_t related_table_id, ObDASTableLocMeta &related_meta) const;
  void reset()
  {
    table_loc_id_ = common::OB_INVALID_ID;
    ref_table_id_ = common::OB_INVALID_ID;
    flags_ = 0;
    related_table_ids_.reset();
  }

  TO_STRING_KV(K_(table_loc_id),
               K_(ref_table_id),
               K_(related_table_ids),
               K_(use_dist_das),
               K_(select_leader),
               K_(is_dup_table),
               K_(is_weak_read),
               K_(unuse_related_pruning),
               K_(is_external_table),
               K_(route_policy));

  uint64_t table_loc_id_; //location object id
  uint64_t ref_table_id_; //table object id
  DASTableIDArray related_table_ids_;
  union {
    uint64_t flags_;
    struct {
      uint64_t use_dist_das_                    : 1; //mark whether this table touch data through distributed DAS
      uint64_t select_leader_                   : 1; //mark whether this table use leader replica
      uint64_t is_dup_table_                    : 1; //mark if this table is a duplicated table
      uint64_t is_weak_read_                    : 1; //mark if this tale can use weak read consistency
      uint64_t unuse_related_pruning_           : 1; //mark if this table use the related pruning to prune local index tablet_id
      uint64_t is_external_table_               : 1; //mark if this table is an external table
      uint64_t is_external_files_on_disk_       : 1; //mark if files in external table are located at local disk
      uint64_t das_empty_part_                  : 1; //mark there is false startup filter on DAS access table
      uint64_t is_lake_table_                   : 1; //mark if this table is an lake table
      uint64_t is_shared_external_files_on_disk_: 1; //mark if external files using sfile:// (need de-dup across observers)
      uint64_t reserved_                        : 54;
    };
  };
  int64_t route_policy_;

private:
  void light_assign(const ObDASTableLocMeta &other); //without array
};
typedef common::ObFixedArray<ObDASTableLocMeta, common::ObIAllocator> DASTableLocMetaArray;
/**
 * ObDASTabletLoc is generated by PlanCache or Optimizer,
 * and used in the execution engine
 * or dynamically calculated by the operator during execution, for example:
 *               DISTRIBUTED UPDATE
 *                       |
 *                NESTED LOOP JOIN
 *                /            \
 *             TSC(t1)        TSC(t2)
 **/
struct ObDASTabletLoc
{
  OB_UNIS_VERSION(1);
public:
  ObDASTabletLoc()
    : tablet_id_(),
      ls_id_(),
      server_(),
      loc_meta_(nullptr),
      next_(this),
      flags_(0),
      partition_id_(OB_INVALID_ID),
      first_level_part_id_(OB_INVALID_ID)
  { }
  ~ObDASTabletLoc() = default;

  TO_STRING_KV(K_(tablet_id),
               K_(ls_id),
               K_(server),
               K_(loc_meta),
               K_(in_retry),
               K_(partition_id),
               K_(first_level_part_id));
  /**
   * BE CAREFUL!!! can't declare implicit allocator or
   * data structure holding implicit allocator here,
   * such as ObSEArray, ObArray, ObRowStore, etc.
   * Otherwise it will cause memory leak,
   * because ObDASTabletLoc will not be released
   */
  common::ObTabletID tablet_id_; //the data_object_id corresponding to partition_id
  //To reduce the conversion between tablet_id to ls_id,
  //DAS caches ls_id_, SQL should not actually touch ls_id_
  share::ObLSID ls_id_;
  common::ObAddr server_;
  const ObDASTableLocMeta *loc_meta_; //reference the table location meta, not serialize it
  ObDASTabletLoc *next_; //to bind all data table and local index tablet location
  union {
    uint64_t flags_;
    struct {
      uint64_t in_retry_                        : 1; //need to refresh tablet location cache
      uint64_t reserved_                        : 63;
    };
  };
  // partition id of this tablet
  uint64_t partition_id_;
  // first level part id of this tablet, only valid for subpartitioned table.
  uint64_t first_level_part_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASTabletLoc);
  int assign(const ObDASTabletLoc &other);
};

static const int64_t DAS_TABLET_LOC_LOOKUP_THRESHOLD = 1000;
static const int64_t DAS_TABLET_LOC_SIZE_THRESHOLD = 10;
static const int64_t DAS_TABLET_LOC_MAP_BUCKET_SIZE = 5000;

typedef common::ObList<ObDASTabletLoc*, common::ObIAllocator> DASTabletLocList;
typedef common::ObList<ObDASTabletLoc*, common::ObIAllocator>::iterator DASTabletLocListIter;
typedef common::ObList<ObDASTabletLoc*, common::ObIAllocator>::const_iterator ConstDASTabletLocListIter;
typedef common::ObIArray<ObDASTabletLoc*> DASTabletLocIArray;
typedef common::ObSEArray<ObDASTabletLoc*, 1> DASTabletLocSEArray;
typedef common::ObArray<ObDASTabletLoc*> DASTabletLocArray;

class TabletHashMap
{
  struct TabletHashNode
  {
    ObTabletID key_;
    ObDASTabletLoc *value_;
    struct TabletHashNode *next_;
  };
public:
  TabletHashMap(common::ObIAllocator &allocator)
    : allocator_(allocator), bucket_num_(0), buckets_(NULL), is_inited_(false)
  {}
  virtual ~TabletHashMap() {} // does not free memory
  int create(int64_t bucket_num);
  bool created() const { return is_inited_; }
  int set(const ObTabletID key, ObDASTabletLoc *value);
  int get(const ObTabletID key, ObDASTabletLoc *&value);
private:
  int find_node(const ObTabletID key, TabletHashNode *head, TabletHashNode *&node) const;

  common::ObIAllocator &allocator_;
  int64_t bucket_num_;
  TabletHashNode **buckets_;
  bool is_inited_;
};

/**
 * store the location information of which tables are accessed in this plan
 * generate this table location when this plan is chosen in the plan cache or generated this plan by CG
 * for multiple part dml operator, maybe generated when the operator is opened
 */
struct ObDASTableLoc
{
  OB_UNIS_VERSION(1);
public:
  ObDASTableLoc(common::ObIAllocator &allocator)
    : allocator_(allocator),
      loc_meta_(nullptr),
      flags_(0),
      tablet_locs_(allocator),
      tablet_locs_map_(allocator),
      lookup_cnt_(0)
  { }
  ~ObDASTableLoc() = default;

  int assign(const ObCandiTableLoc &candi_table_loc);
  int64_t get_table_location_key() const { return loc_meta_->table_loc_id_; }
  int64_t get_ref_table_id() const { return loc_meta_->ref_table_id_; }
  bool empty() const { return tablet_locs_.size() == 0; }
  const DASTabletLocList &get_tablet_locs() const { return tablet_locs_; }
  ObDASTabletLoc *get_first_tablet_loc() { return tablet_locs_.get_first(); }
  DASTabletLocListIter tablet_locs_begin() { return tablet_locs_.begin(); }
  DASTabletLocListIter tablet_locs_end() { return tablet_locs_.end(); }
  int get_tablet_loc_by_id(const ObTabletID &tablet_id, ObDASTabletLoc *&tablet_loc);
  int add_tablet_loc(ObDASTabletLoc *table_loc);

  TO_STRING_KV(KPC_(loc_meta),
               K_(tablet_locs),
               K_(is_writing),
               K_(is_reading),
               K_(need_refresh),
               K_(is_fk_check));

  /**
   * BE CAREFUL!!! can't declare implicit allocator or
   * data structure holding implicit allocator here,
   * such as ObSEArray, ObArray, ObRowStore, etc.
   * Otherwise it will cause memory leak,
   * because ObDASTableLoc will not be released
   */
  common::ObIAllocator &allocator_;
  const ObDASTableLocMeta *loc_meta_;
  union {
    /**
     * used to mark some status related to table access,
     * and reserve some expansion bits for subsequent needs
     */
    uint64_t flags_;
    struct {
      uint64_t is_writing_                      : 1; //mark this table is writing
      uint64_t is_reading_                      : 1; //mark this table is reading
      uint64_t rebuild_reference_               : 1; //mark whether rebuild the related reference
      uint64_t need_refresh_                    : 1;
      uint64_t is_fk_check_                     : 1; //mark this table is used for foreign key checking
      uint64_t reserved_                        : 59;
    };
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObDASTableLoc);
  int assign(const ObDASTableLoc &other);
  int create_tablet_locs_map();

  /**
   * The reason for using ObList to store ObTabletLoc objects is that
   * during the execution process,
   * some operators will dynamically extend some new partitions,
   * so tablet_locs_ needs to be able to expand dynamically.
   * ObArray/ObSEArray will cause dynamic extension or release of memory when expanding members.
   * At the same time, in partition task retry,
   * it is very convenient to clear the invalid TabletLoc object,
   * so ObList is more suitable for its needs.
   * Why not use ObDList?
   * Considering that ObTabletLoc may be referenced by list of other modules, such as PX,
   * and elements in ObDList can only be referenced by one list at the same time,
   * ObDList has more restrictions and is not suitable as a container for ObTabletLoc
   **/
  DASTabletLocList tablet_locs_;
  TabletHashMap tablet_locs_map_;
  int64_t lookup_cnt_;
};
typedef common::ObList<ObDASTableLoc*, common::ObIAllocator> DASTableLocList;
typedef common::ObList<uint64_t, common::ObIAllocator> DASTableIdList;
typedef common::ObFixedArray<uint64_t, common::ObIAllocator> UIntFixedArray;
typedef common::ObFixedArray<int64_t, common::ObIAllocator> IntFixedArray;
typedef common::ObFixedArray<ObObjectID, common::ObIAllocator> ObjectIDFixedArray;
typedef common::ObFixedArray<ObDASTableLoc*, common::ObIAllocator> DASTableLocFixedArray;
typedef common::ObFixedArray<common::ObString, common::ObIAllocator> ExternalFileNameArray;

// ========== Logical Table Structures (M5/M6 shared) ==========

enum ObLogicalColumnSource : uint8_t
{
  FROM_ROWID        = 0,
  FROM_VALUE_PATH   = 1,
  FROM_KEY_SUFFIX   = 2,
  FROM_VALUE_WHOLE  = 3,
  FROM_QUERY_RANGE  = 4,
  FROM_KEY_CID      = 5,
  FROM_DOMAIN_FILL  = 6,
  FROM_SOURCE_MAX
};

struct ObLogicalColumnMappingEntry
{
  OB_UNIS_VERSION(1);
public:
  ObLogicalColumnSource source_;
  int64_t source_idx_;
  common::ObString column_name_;
  common::ObObjMeta obj_meta_;
  int64_t key_suffix_ordinal_;
  common::ObString json_path_;
  uint64_t logical_column_id_;
  bool is_rowid_;
  bool is_query_range_ns_;
  bool is_vector_col_;

  ObLogicalColumnMappingEntry()
    : source_(FROM_SOURCE_MAX),
      source_idx_(-1),
      column_name_(),
      obj_meta_(),
      key_suffix_ordinal_(-1),
      json_path_(),
      logical_column_id_(common::OB_INVALID_ID),
      is_rowid_(false),
      is_query_range_ns_(false),
      is_vector_col_(false)
  {}

  TO_STRING_KV(K_(source), K_(source_idx), K_(column_name), K_(obj_meta),
               K_(key_suffix_ordinal), K_(json_path), K_(logical_column_id),
               K_(is_rowid), K_(is_query_range_ns), K_(is_vector_col));
};

struct ObLogicalTableColumnMapping
{
  OB_UNIS_VERSION(1);
public:
  common::ObSEArray<ObLogicalColumnMappingEntry, 8, common::ObIAllocator &> entries_;
  uint64_t index_seq_;
  // Not serialized. The owning CtDef allocator owns both entries_ expansion
  // storage and deep-copied strings after remote deserialization.
  common::ObIAllocator *allocator_;

  explicit ObLogicalTableColumnMapping(common::ObIAllocator &alloc)
    : entries_(OB_MALLOC_NORMAL_BLOCK_SIZE, alloc),
      index_seq_(0),
      allocator_(&alloc)
  {}
  bool empty() const { return entries_.empty(); }
  int64_t count() const { return entries_.count(); }
  // Deep-copy entries_ string members (column_name_/json_path_) onto allocator_
  // after deserialization to avoid dangling into the RPC packet buffer.
  int deep_copy_strings();

  TO_STRING_KV(K_(entries), K_(index_seq));
};

struct ObLogicalTableTabletMapping
{
  OB_UNIS_VERSION(1);
public:
  common::ObTabletID logical_tablet_id_;
  common::ObObjectID logical_part_id_;
  common::ObObjectID logical_first_level_part_id_;
  common::ObTabletID physical_tablet_id_;
  common::ObObjectID physical_part_id_;
  common::ObObjectID physical_first_level_part_id_;

  ObLogicalTableTabletMapping()
    : logical_tablet_id_(),
      logical_part_id_(common::OB_INVALID_ID),
      logical_first_level_part_id_(common::OB_INVALID_ID),
      physical_tablet_id_(),
      physical_part_id_(common::OB_INVALID_ID),
      physical_first_level_part_id_(common::OB_INVALID_ID)
  {}

  TO_STRING_KV(K_(logical_tablet_id), K_(logical_part_id), K_(logical_first_level_part_id),
               K_(physical_tablet_id), K_(physical_part_id), K_(physical_first_level_part_id));
};

typedef common::ObFixedArray<ObLogicalTableTabletMapping, common::ObIAllocator> ObLogicalTableTabletMappingArray;

// Moved before ObLogicalTableHelper because project_logical_columns uses it.
enum ObTSCIRScanType : uint16_t
{
  OB_NOT_A_SPEC_SCAN = 0,
  OB_IR_DOC_ID_IDX_AGG,
  OB_IR_INV_IDX_AGG,
  OB_IR_INV_IDX_SCAN,
  OB_IR_FWD_IDX_AGG,
  OB_VEC_DELTA_BUF_SCAN,
  OB_VEC_IDX_ID_SCAN,
  OB_VEC_SNAPSHOT_SCAN,
  OB_VEC_COM_AUX_SCAN,
  OB_VEC_ROWKEY_VID_SCAN,
  OB_VEC_VID_ROWKEY_SCAN,
  OB_VEC_FILTER_SCAN,
  OB_VEC_IVF_CENTROID_SCAN,
  OB_VEC_IVF_CID_VEC_SCAN,    // for pq is pq code table
  OB_VEC_IVF_ROWKEY_CID_SCAN, // for pq is pq ROWKEY_CID_TABLE
  OB_VEC_IVF_SPECIAL_AUX_SCAN,// for pq is pq id, for sq is sq meta
  OB_VEC_SPIV_INDEX_SCAN,
  OB_VEC_EMBEDDED_SCAN,
  OB_IR_BLOCK_MAX_SCAN,
  OB_VEC_SPIV_BLOCK_MAX_SCAN,
  // OB_VEC_SPIV_INDEX_AGG,
  OB_IR_MULTIVALUE_IDX_SCAN,
  OB_VEC_BRUTE_MAIN_SCAN,
  OB_VEC_ASYNC_IVF_INDEX_SCAN,
  OB_ASYNC_INDEX_DELETE_SCAN
};

// Compile-time logical table scan metadata (plan-cache safe; no runtime ns/lt).
struct ObLogicalTableScanCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObExpr *ns_id_expr_;
  ObExpr *lt_id_start_expr_;
  ObExpr *lt_id_end_expr_;
  ObExpr *rowid_start_expr_;
  ObExpr *rowid_end_expr_;
  ObExpr *kv_key_expr_;
  ObExpr *kv_value_expr_;
  bool lt_start_inclusive_;
  bool lt_end_inclusive_;
  bool has_ns_pred_;
  bool has_lt_pred_;
  bool start_inclusive_;
  bool end_inclusive_;
  uint64_t physical_data_table_id_;
  uint64_t physical_namespace_col_id_;
  uint64_t physical_key_col_id_;
  uint64_t physical_value_col_id_;
  uint64_t logical_ref_table_id_;
  int64_t ivf_vec_project_dim_;
  bool analyzed_;

  ObLogicalTableScanCtDef()
    : ns_id_expr_(nullptr), lt_id_start_expr_(nullptr), lt_id_end_expr_(nullptr),
      rowid_start_expr_(nullptr), rowid_end_expr_(nullptr),
      kv_key_expr_(nullptr), kv_value_expr_(nullptr),
      lt_start_inclusive_(true), lt_end_inclusive_(true),
      has_ns_pred_(false), has_lt_pred_(false),
      start_inclusive_(true), end_inclusive_(true),
      physical_data_table_id_(common::OB_INVALID_ID),
      physical_namespace_col_id_(common::OB_INVALID_ID),
      physical_key_col_id_(common::OB_INVALID_ID),
      physical_value_col_id_(common::OB_INVALID_ID),
      logical_ref_table_id_(common::OB_INVALID_ID),
      ivf_vec_project_dim_(-1),
      analyzed_(false)
  {}

  void reset()
  {
    ns_id_expr_ = nullptr;
    lt_id_start_expr_ = nullptr;
    lt_id_end_expr_ = nullptr;
    rowid_start_expr_ = nullptr;
    rowid_end_expr_ = nullptr;
    kv_key_expr_ = nullptr;
    kv_value_expr_ = nullptr;
    lt_start_inclusive_ = true;
    lt_end_inclusive_ = true;
    has_ns_pred_ = false;
    has_lt_pred_ = false;
    start_inclusive_ = true;
    end_inclusive_ = true;
    physical_data_table_id_ = common::OB_INVALID_ID;
    physical_namespace_col_id_ = common::OB_INVALID_ID;
    physical_key_col_id_ = common::OB_INVALID_ID;
    physical_value_col_id_ = common::OB_INVALID_ID;
    logical_ref_table_id_ = common::OB_INVALID_ID;
    ivf_vec_project_dim_ = -1;
    analyzed_ = false;
  }

  TO_STRING_KV(K_(analyzed), K_(has_ns_pred), K_(has_lt_pred),
               K_(physical_data_table_id), K_(ivf_vec_project_dim));
};

struct ObLogicalTableScanRtDef
{
  bool direct_phys_ranges_built_;
  common::ObJsonPathCache path_cache_;
  common::ObTabletID physical_kv_tablet_id_;

  explicit ObLogicalTableScanRtDef(common::ObIAllocator *alloc)
    : direct_phys_ranges_built_(false),
      path_cache_(alloc),
      physical_kv_tablet_id_()
  {}

  void reset_scan_state() { direct_phys_ranges_built_ = false; }

  TO_STRING_KV(K_(direct_phys_ranges_built), K_(physical_kv_tablet_id));
};

struct ObLogicalTableRtDef
{
  OB_UNIS_VERSION(1);
public:
  uint64_t namespace_id_;
  uint64_t logical_table_id_;
  bool enabled_;

  ObLogicalTableRtDef()
    : namespace_id_(share::OB_INVALID_NAMESPACE_ID),
      logical_table_id_(share::OB_INVALID_LOGICAL_TABLE_ID),
      enabled_(false)
  {}

  void reset_runtime_identity()
  {
    namespace_id_ = share::OB_INVALID_NAMESPACE_ID;
    logical_table_id_ = share::OB_INVALID_LOGICAL_TABLE_ID;
    enabled_ = false;
  }

  OB_INLINE bool has_resolved_ns_lt() const
  {
    return share::OB_INVALID_NAMESPACE_ID != namespace_id_
        && share::OB_INVALID_LOGICAL_TABLE_ID != logical_table_id_;
  }

  // Resolve runtime ns/lt from expr-evaluated namespace_id_/logical_table_id_.
  OB_INLINE bool get_resolved_identity(uint64_t &out_ns_id, uint64_t &out_lt_id) const
  {
    if (has_resolved_ns_lt()) {
      out_ns_id = namespace_id_;
      out_lt_id = logical_table_id_;
    } else {
      out_ns_id = share::OB_INVALID_NAMESPACE_ID;
      out_lt_id = share::OB_INVALID_LOGICAL_TABLE_ID;
    }
    return share::OB_INVALID_NAMESPACE_ID != out_ns_id
        && share::OB_INVALID_LOGICAL_TABLE_ID != out_lt_id;
  }

  OB_INLINE bool is_valid() const { return enabled_ && has_resolved_ns_lt(); }

  static int require_resolved_ns_lt(const ObLogicalTableRtDef &lt_rt,
                                    const char *caller);

  TO_STRING_KV(K_(namespace_id), K_(logical_table_id), K_(enabled));
};

// Static encoder: pure encoding logic with no runtime identity state.
// Callers pass namespace_id / logical_table_id / physical col_ids explicitly.
struct ObLogicalTableEncoder
{
  static int encode_physical_row(
      uint64_t namespace_id,
      uint64_t logical_table_id,
      uint64_t physical_key_col_id,
      uint64_t physical_value_col_id,
      share::ObLogicalTableAuxType aux_type,
      const ObLogicalTableColumnMapping &mapping,
      const common::ObIArray<ObExpr *> &logical_exprs,
      ObEvalCtx &eval_ctx,
      common::ObIAllocator &row_alloc,
      common::ObString &out_key,
      common::ObString &out_value);

  static int encode_physical_row(
      uint64_t namespace_id,
      uint64_t logical_table_id,
      uint64_t physical_key_col_id,
      uint64_t physical_value_col_id,
      share::ObLogicalTableAuxType aux_type,
      const ObLogicalTableColumnMapping &mapping,
      const blocksstable::ObDatumRow &domain_row,
      common::ObIAllocator &row_alloc,
      common::ObString &out_key,
      common::ObString &out_value);
};

// Shared by ctdef (compile-time expr/meta), op (runtime identity), and iter-owned ctx.
// Field-level layer boundaries: issue/logical_table_das_lt_ctx_layers.md
struct ObLogicalTableHelper
{
  friend struct ObLogicalTableEncoder;
public:
  // Static-only utility class: not instantiable. All state lives in ctdef/rtdef/rewrite_ctx.
  ObLogicalTableHelper() = delete;

  // Read an ObExpr's payload at row_idx as ObString, robust against rich-format
  // vectors, batched datums, and single datums. Optionally validates and unwraps
  // LOB header per expr->obj_meta_.is_lob_storage(). out_is_null indicates the
  // per-row null flag (caller may need null-branch logic).
  static int read_kv_string_at_row(ObExpr &kv_expr,
                                   ObEvalCtx &eval_ctx,
                                   int64_t row_idx,
                                   common::ObIAllocator &alloc,
                                   bool &out_is_null,
                                   common::ObString &out_str);

  // Invalidate spec output expr trees (root -> args) for [0, count) before re-eval
  // (logical-table hybrid fusion output batch + rowsets).
  static void clear_consumer_output_expr_evaluated_flags_for_batch(
      const common::ObIArray<ObExpr *> &output_exprs,
      ObEvalCtx &eval_ctx,
      const int64_t count);

  // Core projection: compile-time scan metadata + runtime identity (+ optional heavy state).
  static int project_logical_columns(
      const ObLogicalTableScanCtDef &lt_ctdef,
      const ObLogicalTableRtDef &lt_rt,
      const ObLogicalTableColumnMapping &mapping,
      const ObTSCIRScanType ir_scan_type,
      const share::ObLogicalTableAuxType aux_type,
      common::ObIArray<ObExpr *> &logical_output_exprs,
      int64_t row_count,
      ObEvalCtx &eval_ctx,
      ObLogicalTableScanRtDef *lt_scan_rt,
      const sql::ExprFixedArray *result_output_ptr = nullptr,
      const bool storage_agg_pushdown = false);

  // Convenience wrapper for ObDASScanCtDef + rtdef split path.
  static int project_logical_columns(
      const ObLogicalTableScanCtDef &lt_ctdef,
      const ObLogicalTableRtDef &lt_rt,
      const ObDASScanCtDef &scan_ctdef,
      common::ObIArray<ObExpr *> &logical_output_exprs,
      int64_t row_count,
      ObEvalCtx &eval_ctx,
      ObLogicalTableScanRtDef *lt_scan_rt);

  // Validate JsonString raw float[] payload (dim*sizeof(float) bytes, no LOB header).
  static int validate_json_vec_raw_bytes(const common::ObString &vec_str, int64_t dim_hint);
  // Decode JsonString raw float[] into inrow LOB bytes (ObLobCommon + float payload).
  // out_lob points into alloc; valid until alloc is reset.
  static int decode_json_vec_str_to_lob(const common::ObString &vec_str, int64_t dim_hint,
                                        common::ObIAllocator &alloc, common::ObString &out_lob);
  // Evaluate logical_residual_filters_ after project_logical_columns; compacts batch row count.
  static int eval_logical_residual_filters(
      const sql::ExprFixedArray &residual_filters,
      const sql::ExprFixedArray &access_exprs,
      const sql::ExprFixedArray &logical_output_exprs,
      int64_t &row_count,
      ObEvalCtx &eval_ctx);

  static int eval_logical_residual_filters(
      const ObDASScanCtDef &scan_ctdef,
      int64_t &row_count,
      ObEvalCtx &eval_ctx);

  static int eval_logical_residual_filters(
      const ObDASScalarScanCtDef &scalar_ctdef,
      int64_t &row_count,
      ObEvalCtx &eval_ctx,
      const common::ObIArray<ObExpr *> *rowid_exprs_for_compact = nullptr);

  // Map logic-main partition tablet -> KV physical tablet (same rule as init_scan_param redirect).
  static int resolve_kv_tablet_via_logical_anchor(
      const uint64_t tenant_id,
      const uint64_t logical_ref_table_id,
      const uint64_t kv_table_id,
      const common::ObTabletID &logical_anchor_tablet,
      common::ObTabletID &kv_tablet_id,
      common::ObObjectID &kv_object_id);

  // Logical rowkey ranges -> physical KV ranges.
  // Caller pre-populates rewrite_ctx.namespace_id_/logical_table_id_ from expr-evaluated lt_rt.
  static int rewrite_table_scan_param_key_ranges(storage::ObTableScanParam &scan_param,
                                                 const ObDASScanCtDef &scan_ctdef, ObEvalCtx &eval_ctx,
                                                 common::ObIAllocator &alloc, ObLogicalTableRewriteCtx &rewrite_ctx);

  // ScalarScan variant: same hybrid rewrite as ObDASScanCtDef; ns/lt from rewrite_ctx.
  static int rewrite_table_scan_param_key_ranges(storage::ObTableScanParam &scan_param,
                                                 const ObDASScalarScanCtDef &scalar_ctdef,
                                                 ObEvalCtx &eval_ctx,
                                                 common::ObIAllocator &alloc,
                                                 ObLogicalTableRewriteCtx &rewrite_ctx);

private:
  static int project_logical_columns_with_path_cache(
      const ObLogicalTableScanCtDef &lt_ctdef,
      const ObLogicalTableRtDef &lt_rt,
      common::ObJsonPathCache &path_cache,
      const ObLogicalTableColumnMapping &mapping,
      const ObTSCIRScanType ir_scan_type,
      const share::ObLogicalTableAuxType aux_type,
      common::ObIArray<ObExpr *> &logical_output_exprs,
      int64_t row_count,
      ObEvalCtx &eval_ctx,
      const sql::ExprFixedArray *result_output_ptr,
      const bool storage_agg_pushdown);

  static common::ObIVector *get_logical_output_vector_(ObExpr &expr, ObEvalCtx &eval_ctx);
  static void set_logical_output_null(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx);
  static void set_logical_output_uint(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                      const uint64_t v);
  static void set_logical_output_int(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                     const int64_t v);
  static void set_logical_output_float(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                       const float v);
  static void set_logical_output_double(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                        const double v);
  static int set_logical_output_string(ObExpr &expr, const int64_t row_idx, ObEvalCtx &eval_ctx,
                                       const common::ObString &str);

  // Rewrites scan_param.key_ranges_ from logical rowkey form into physical KV-prefixed form.
  // Reads runtime @namespace_id / @ltable_id from rewrite_ctx (caller must pre-populate either
  // via session capture on local QC or via DAS task deserialization on PX worker). No longer
  // reads any baked ns/lt from ctdef.
  static int rewrite_hybrid_logical_table_scan_param_key_ranges_(
      storage::ObTableScanParam &scan_param,
      uint64_t physical_data_table_id,
      share::ObLogicalTableAuxType aux_type,
      uint64_t index_seq,
      uint64_t ref_table_id,
      const ObPushdownExprSpec &pd_expr_spec,
      ObEvalCtx &eval_ctx,
      common::ObIAllocator &alloc,
      ObLogicalTableRewriteCtx &rewrite_ctx);

  static int build_ordered_key_datums_(
      const common::ObSEArray<common::ObDatum, 1> &rowid_datums,
      const common::ObSEArray<int64_t, 4> &suffix_ordinals,
      const common::ObSEArray<common::ObDatum, 4> &suffix_datums,
      common::ObSEArray<common::ObDatum, 4> &key_datums);

  static int collect_datums_from_mapping_(
      const ObLogicalTableColumnMapping &mapping,
      const common::ObIArray<ObExpr *> &logical_exprs,
      ObEvalCtx &eval_ctx,
      common::ObIAllocator &row_alloc,
      common::ObSEArray<common::ObDatum, 4> &key_datums,
      common::ObSEArray<common::ObString, 8> &value_col_names,
      common::ObSEArray<common::ObString, 8> &value_json_paths,
      common::ObSEArray<common::ObObjMeta, 8> &value_metas,
      common::ObSEArray<common::ObDatum, 8> &value_datums,
      bool &has_whole_value,
      common::ObDatum &whole_value_datum);

  static int collect_datums_from_datum_row_(
      const ObLogicalTableColumnMapping &mapping,
      const blocksstable::ObDatumRow &domain_row,
      common::ObSEArray<common::ObDatum, 4> &key_datums,
      common::ObSEArray<common::ObString, 8> &value_col_names,
      common::ObSEArray<common::ObString, 8> &value_json_paths,
      common::ObSEArray<common::ObObjMeta, 8> &value_metas,
      common::ObSEArray<common::ObDatum, 8> &value_datums,
      bool &has_whole_value,
      common::ObDatum &whole_value_datum);

  static int encode_key_value_(
      uint64_t namespace_id,
      uint64_t logical_table_id,
      share::ObLogicalTableAuxType aux_type,
      uint64_t index_seq,
      const common::ObSEArray<common::ObDatum, 4> &key_datums,
      const common::ObSEArray<common::ObString, 8> &value_col_names,
      const common::ObSEArray<common::ObString, 8> &value_json_paths,
      const common::ObSEArray<common::ObObjMeta, 8> &value_metas,
      const common::ObSEArray<common::ObDatum, 8> &value_datums,
      bool has_whole_value,
      const common::ObDatum &whole_value_datum,
      common::ObIAllocator &row_alloc,
      common::ObString &out_key,
      common::ObString &out_value);

  static int project_ivf_cid_rowid_posting_(
      const ObLogicalTableScanCtDef &lt_ctdef,
      const ObLogicalTableColumnMapping &mapping,
      common::ObIArray<ObExpr *> &logical_output_exprs,
      const int64_t row_count,
      ObEvalCtx &eval_ctx);

  static int project_ivf_cid_rowid_posting_rows_(
      const ObLogicalTableScanCtDef &lt_ctdef,
      ObExpr &vec_out_expr,
      ObExpr &rowid_out_expr,
      ObExpr *cid_out_expr,
      const int64_t row_count,
      ObEvalCtx &eval_ctx);

  static int decode_ivf_cid_rowid_key_(
      const common::ObString &key,
      uint64_t &rowid,
      common::ObString &cid);

  static int set_ivf_raw_vector_output_(
      ObExpr &out_expr,
      const common::ObString &raw_vec,
      const int64_t row_idx,
      ObEvalCtx &eval_ctx);

  static int compact_logical_batch_exprs(
      ObEvalCtx &eval_ctx,
      const common::ObIArray<ObExpr *> &exprs,
      const sql::ObBitVector &skip,
      int64_t &row_count);

  static void append_unique_expr(
      const ObExpr *expr,
      common::ObIArray<ObExpr *> &exprs);

  static int eval_logical_residual_filters_with_extra_compact_(
      const sql::ExprFixedArray &residual_filters,
      const sql::ExprFixedArray &access_exprs,
      const sql::ExprFixedArray &logical_output_exprs,
      int64_t &row_count,
      ObEvalCtx &eval_ctx,
      const common::ObIArray<ObExpr *> *extra_compact_exprs);
};

struct ObLogicalTableAuxMeta
{
  share::ObLogicalTableAuxType aux_type_;
  uint64_t aux_table_id_;
  uint64_t index_seq_;
  // Schema column_ids for the logical KEY, in col_descs order.
  // key_col_ids_count_ must equal get_aux_type_col_descs(aux_type_).col_cnt.
  // Populated in build_logical_table_meta_ using schema flags
  // (is_doc_id_column / is_word_segment_column / is_vec_ivf_center_id_column / rowkey).
  uint64_t *key_col_ids_;
  int64_t key_col_ids_count_;

  ObLogicalTableAuxMeta()
    : aux_type_(share::LT_AUX_MAX_TYPE),
      aux_table_id_(common::OB_INVALID_ID),
      index_seq_(0),
      key_col_ids_(nullptr),
      key_col_ids_count_(0)
  {}

  TO_STRING_KV(K_(aux_type), K_(aux_table_id), K_(index_seq),
               "key_col_ids", common::ObArrayWrap<uint64_t>(key_col_ids_, key_col_ids_count_));
};

struct ObLogicalTableMeta
{
  uint64_t namespace_id_;
  uint64_t logical_table_id_;
  uint64_t physical_data_table_id_;
  uint64_t physical_namespace_col_id_;
  uint64_t physical_key_col_id_;
  uint64_t physical_value_col_id_;
  common::ObFixedArray<ObLogicalTableAuxMeta, common::ObIAllocator> aux_metas_;
  ObLogicalTableTabletMappingArray tablet_mappings_;

  explicit ObLogicalTableMeta(common::ObIAllocator &alloc)
    : namespace_id_(share::OB_INVALID_NAMESPACE_ID),
      logical_table_id_(share::OB_INVALID_LOGICAL_TABLE_ID),
      physical_data_table_id_(common::OB_INVALID_ID),
      physical_namespace_col_id_(common::OB_INVALID_ID),
      physical_key_col_id_(common::OB_INVALID_ID),
      physical_value_col_id_(common::OB_INVALID_ID),
      aux_metas_(alloc),
      tablet_mappings_(alloc)
  {}

  OB_INLINE bool is_valid() const
  {
    return share::OB_INVALID_NAMESPACE_ID != namespace_id_
      && share::OB_INVALID_LOGICAL_TABLE_ID != logical_table_id_
      && common::OB_INVALID_ID != physical_data_table_id_
      && common::OB_INVALID_ID != physical_key_col_id_
      && common::OB_INVALID_ID != physical_value_col_id_;
  }

  const ObLogicalTableAuxMeta *find_aux_meta(uint64_t table_id) const
  {
    for (int64_t i = 0; i < aux_metas_.count(); ++i) {
      if (aux_metas_.at(i).aux_table_id_ == table_id) {
        return &aux_metas_.at(i);
      }
    }
    return nullptr;
  }

  const ObLogicalTableAuxMeta *find_primary_aux_meta() const
  {
    for (int64_t i = 0; i < aux_metas_.count(); ++i) {
      if (share::LT_AUX_PRIMARY == aux_metas_.at(i).aux_type_) {
        return &aux_metas_.at(i);
      }
    }
    return nullptr;
  }

  TO_STRING_KV(K_(namespace_id), K_(logical_table_id),
               K_(physical_data_table_id), K_(physical_namespace_col_id),
               K_(physical_key_col_id), K_(physical_value_col_id),
               K_(aux_metas), K_(tablet_mappings));
};

// ========== End Logical Table Structures ==========


//DAS: data access service
//CtDef: Compile time Definition
struct ObDASBaseCtDef
{
  OB_UNIS_VERSION_PV();
public:
  ObDASOpType op_type_;
  ObDASBaseCtDef **children_;
  uint32_t children_cnt_;
  share::ObLogicalTableAuxType logical_table_aux_type_;
  uint64_t physical_data_table_id_;
  ObLogicalTableColumnMapping logical_column_mapping_;
  uint64_t logical_index_seq_;
  const share::schema::ObTableDMLParam *lt_physical_table_param_;

  virtual ~ObDASBaseCtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(op_type), K_(children_cnt), K_(logical_table_aux_type),
                        K_(physical_data_table_id), K_(logical_index_seq));

  virtual bool has_expr() const { return false; }
  virtual bool has_pdfilter_or_calc_expr() const { return false; }
  virtual bool has_pl_udf() const { return false; }
  virtual bool is_search_ctdef() const { return false; }

  bool is_logical_table() const
  {
    return share::LT_AUX_MAX_TYPE != logical_table_aux_type_;
  }

protected:
  ObDASBaseCtDef(common::ObIAllocator &alloc, ObDASOpType op_type)
    : op_type_(op_type),
      children_(nullptr),
      children_cnt_(0),
      logical_table_aux_type_(share::LT_AUX_MAX_TYPE),
      physical_data_table_id_(OB_INVALID_ID),
      logical_column_mapping_(alloc),
      logical_index_seq_(common::OB_INVALID_ID),
      lt_physical_table_param_(nullptr)
  { }
};

//DAS: data access service
//RtDef: Runtime Definition
struct ObDASBaseRtDef
{
  OB_UNIS_VERSION_PV();
public:
  ObDASOpType op_type_;
  const ObDASBaseCtDef *ctdef_;
  ObEvalCtx *eval_ctx_; //nullptr in DML DAS Op
  ObDASTableLoc *table_loc_;
  ObDASBaseRtDef **children_;
  uint32_t children_cnt_;

  virtual ~ObDASBaseRtDef() = default;
  VIRTUAL_TO_STRING_KV(K_(op_type), K_(children_cnt));
protected:
  ObDASBaseRtDef(ObDASOpType op_type)
    : op_type_(op_type),
      ctdef_(nullptr),
      eval_ctx_(NULL),
      table_loc_(nullptr),
      children_(nullptr),
      children_cnt_(0)
  { }
};
typedef common::ObFixedArray<const ObDASBaseCtDef*, common::ObIAllocator> DASCtDefFixedArray;
typedef common::ObFixedArray<ObDASBaseRtDef*, common::ObIAllocator> DASRtDefFixedArray;

OB_INLINE void duplicate_type_to_loc_meta(ObDuplicateType v, ObDASTableLocMeta &loc_meta)
{
  switch (v) {
    case ObDuplicateType::NOT_DUPLICATE:
      loc_meta.is_dup_table_ = 0;
      break;
    case ObDuplicateType::DUPLICATE:
      loc_meta.is_dup_table_ = 1;
      loc_meta.select_leader_ = 0;
      break;
    case ObDuplicateType::DUPLICATE_IN_DML:
      loc_meta.is_dup_table_ = 1;
      loc_meta.select_leader_ = 1;
      break;
    default:
      break;
  }
}

OB_INLINE ObDuplicateType loc_meta_to_duplicate_type(const ObDASTableLocMeta &loc_meta)
{
  ObDuplicateType dup_type = ObDuplicateType::NOT_DUPLICATE;
  if (loc_meta.is_dup_table_) {
    dup_type = loc_meta.select_leader_ ?
        ObDuplicateType::DUPLICATE_IN_DML : ObDuplicateType::DUPLICATE;
  }
  return dup_type;
}

// ObTSCIRScanType is declared before ObLogicalTableHelper.

struct ObDASPushDownTopN
{
  OB_UNIS_VERSION(1);
public:
  ObDASPushDownTopN()
  : order_type_(NULLS_FIRST_ASC),
    sort_key_(nullptr),
    limit_expr_(nullptr),
    offset_expr_(nullptr),
    with_ties_(false),
    is_push_into_index_(false)
    {}
  ~ObDASPushDownTopN() {}

  TO_STRING_KV(K_(order_type), K_(sort_key), K_(limit_expr), K_(offset_expr), K_(with_ties), K_(is_push_into_index));

  OB_INLINE bool is_valid() const
  {
    return nullptr != limit_expr_;
  }

  OB_INLINE ObDASPushDownTopN& operator=(const ObDASPushDownTopN &other)
  {
    if (OB_LIKELY(this != &other)) {
      order_type_ = other.order_type_;
      sort_key_ = other.sort_key_;
      limit_expr_ = other.limit_expr_;
      offset_expr_ = other.offset_expr_;
      with_ties_ = other.with_ties_;
      is_push_into_index_ = other.is_push_into_index_;
    }
    return *this;
  }

  static int prepare_limit_param(ObEvalCtx &eval_ctx,
                                 const ObDASPushDownTopN &push_down_topn,
                                 ObLimitParam &limit_param);

  ObOrderDirection order_type_;
  ObExpr *sort_key_;
  ObExpr *limit_expr_;
  ObExpr *offset_expr_;
  bool with_ties_;
  bool is_push_into_index_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_DEFINE_H_ */
