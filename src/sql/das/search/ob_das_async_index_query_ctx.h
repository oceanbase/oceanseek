/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_INDEX_QUERY_CTX_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_INDEX_QUERY_CTX_H_

#include "common/ob_range.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/async_index/ob_async_index_segment_group.h"

namespace oceanbase
{
namespace storage
{
class ObAccessService;
}
namespace sql
{
class ObSQLSessionInfo;
struct ObDASBaseCtDef;
struct ObDASTabletLoc;

enum ObDASAsyncIndexDomainType
{
  ASYNC_INDEX_DOMAIN_NONE = 0,
  ASYNC_INDEX_DOMAIN_SINGLE,
  ASYNC_INDEX_DOMAIN_MULTI,
  ASYNC_INDEX_DOMAIN_MIXED_SYNC,
  ASYNC_INDEX_DOMAIN_NON_ALIGNABLE
};

enum ObDASAsyncIndexExecMode
{
  ASYNC_INDEX_EXEC_LEGACY = 0,
  ASYNC_INDEX_EXEC_GROUP
};

struct ObDASAsyncIndexSemanticBranchCount
{
  ObDASAsyncIndexSemanticBranchCount()
    : index_table_id_(common::OB_INVALID_ID), branch_count_(0)
  {}
  ObDASAsyncIndexSemanticBranchCount(const uint64_t index_table_id,
                                     const int64_t branch_count)
    : index_table_id_(index_table_id), branch_count_(branch_count)
  {}
  TO_STRING_KV(K_(index_table_id), K_(branch_count));

  uint64_t index_table_id_;
  int64_t branch_count_;
};

struct ObDASAsyncIndexDomainAnalysis
{
  ObDASAsyncIndexDomainAnalysis()
    : type_(ASYNC_INDEX_DOMAIN_NONE),
      index_table_id_(common::OB_INVALID_ID),
      has_async_fts_wrapper_(false),
      is_pure_range_subtree_(false),
      has_group_compatible_sync_filter_(false),
      semantic_branch_counts_()
  {}
  bool is_single() const { return ASYNC_INDEX_DOMAIN_SINGLE == type_; }
  bool has_multi_semantic_branch() const
  {
    bool has_multi = false;
    for (int64_t i = 0; !has_multi && i < semantic_branch_counts_.count(); ++i) {
      has_multi = semantic_branch_counts_.at(i).branch_count_ > 1;
    }
    return has_multi;
  }
  ObDASAsyncIndexExecMode get_exec_mode() const
  {
    ObDASAsyncIndexExecMode exec_mode;
    if (ASYNC_INDEX_DOMAIN_MULTI == type_) {
      exec_mode = ASYNC_INDEX_EXEC_GROUP;
    } else if (ASYNC_INDEX_DOMAIN_SINGLE == type_
               && (has_multi_semantic_branch()
                   || has_group_compatible_sync_filter_)) {
      exec_mode = ASYNC_INDEX_EXEC_GROUP;
    } else {
      exec_mode = ASYNC_INDEX_EXEC_LEGACY;
    }
    return exec_mode;
  }
  TO_STRING_KV(K_(type), K_(index_table_id), K_(has_async_fts_wrapper),
               K_(is_pure_range_subtree),
               K_(has_group_compatible_sync_filter),
               K_(semantic_branch_counts));

  ObDASAsyncIndexDomainType type_;
  uint64_t index_table_id_;
  bool has_async_fts_wrapper_;
  bool is_pure_range_subtree_;
  bool has_group_compatible_sync_filter_;
  common::ObSEArray<ObDASAsyncIndexSemanticBranchCount, 2> semantic_branch_counts_;
};

struct ObDASGroupFilterDomainAnalysis
{
  ObDASGroupFilterDomainAnalysis()
    : is_compatible_(false), has_sync_filter_(false), has_async_filter_(false)
  {}
  TO_STRING_KV(K_(is_compatible), K_(has_sync_filter), K_(has_async_filter));

  bool is_compatible_;
  bool has_sync_filter_;
  bool has_async_filter_;
};

struct ObDASAsyncIndexCandidate
{
  ObDASAsyncIndexCandidate() : ls_id_(), main_tablet_id_(), delete_tablet_id_() {}
  ObDASAsyncIndexCandidate(const share::ObLSID &ls_id,
                           const common::ObTabletID &main_tablet_id,
                           const common::ObTabletID &delete_tablet_id)
    : ls_id_(ls_id),
      main_tablet_id_(main_tablet_id),
      delete_tablet_id_(delete_tablet_id)
  {}
  bool has_same_key(const ObDASAsyncIndexCandidate &other) const
  { return ls_id_ == other.ls_id_ && main_tablet_id_ == other.main_tablet_id_; }
  bool key_less(const ObDASAsyncIndexCandidate &other) const
  {
    return ls_id_.id() < other.ls_id_.id()
        || (ls_id_ == other.ls_id_
            && main_tablet_id_.id() < other.main_tablet_id_.id());
  }
  bool operator==(const ObDASAsyncIndexCandidate &other) const
  {
    return has_same_key(other) && delete_tablet_id_ == other.delete_tablet_id_;
  }
  TO_STRING_KV(K_(ls_id), K_(main_tablet_id), K_(delete_tablet_id));

  share::ObLSID ls_id_;
  common::ObTabletID main_tablet_id_;
  common::ObTabletID delete_tablet_id_;
};

struct ObDASAsyncIndexBinding
{
  ObDASAsyncIndexBinding()
    : ls_id_(), main_tablet_id_(), index_table_id_(common::OB_INVALID_ID), index_tablet_id_()
  {}
  ObDASAsyncIndexBinding(const share::ObLSID &ls_id,
                         const common::ObTabletID &main_tablet_id,
                         const uint64_t index_table_id,
                         const common::ObTabletID &index_tablet_id)
    : ls_id_(ls_id),
      main_tablet_id_(main_tablet_id),
      index_table_id_(index_table_id),
      index_tablet_id_(index_tablet_id)
  {}
  bool has_same_key(const ObDASAsyncIndexBinding &other) const
  {
    return ls_id_ == other.ls_id_
        && main_tablet_id_ == other.main_tablet_id_
        && index_table_id_ == other.index_table_id_;
  }
  bool key_less(const ObDASAsyncIndexBinding &other) const
  {
    return ls_id_.id() < other.ls_id_.id()
        || (ls_id_ == other.ls_id_
            && (main_tablet_id_.id() < other.main_tablet_id_.id()
                || (main_tablet_id_ == other.main_tablet_id_
                    && index_table_id_ < other.index_table_id_)));
  }
  bool operator==(const ObDASAsyncIndexBinding &other) const
  { return has_same_key(other) && index_tablet_id_ == other.index_tablet_id_; }
  TO_STRING_KV(K_(ls_id), K_(main_tablet_id), K_(index_table_id), K_(index_tablet_id));

  share::ObLSID ls_id_;
  common::ObTabletID main_tablet_id_;
  uint64_t index_table_id_;
  common::ObTabletID index_tablet_id_;
};

// Owns the storage snapshot and segment groups for one data partition. The logical/physical
// index arrays preserve the same ordinal used by ObAsyncIndexSegmentGroupCtx.
struct ObAsyncIndexPartitionSegCtx
{
  ObAsyncIndexPartitionSegCtx()
    : ls_id_(), main_tablet_id_(), index_table_ids_(), index_tablet_ids_(),
      sql_read_snapshot_(), storage_group_ctx_()
  {}
  void reset()
  {
    storage_group_ctx_.reset();
    ls_id_.reset();
    main_tablet_id_.reset();
    index_table_ids_.reset();
    index_tablet_ids_.reset();
    sql_read_snapshot_.reset();
  }
  share::SCN get_read_snapshot() const
  { return storage_group_ctx_.get_async_index_read_snapshot(); }
  const storage::ObAsyncIndexSegmentGroupCtx &get_storage_group_ctx() const
  { return storage_group_ctx_; }
  int find_index_tablet_idx(const uint64_t index_table_id, int64_t &tablet_idx) const;
  int find_index_tablet_idx(
      const common::ObTabletID &index_tablet_id,
      int64_t &tablet_idx) const;
  int get_index_segment_cnt(const uint64_t index_table_id, int64_t &segment_cnt) const;
  int get_pinned_index_tablet_handle(const common::ObTabletID &tablet_id,
                                     const storage::ObTabletHandle *&tablet_handle) const;

  // Extracts and validates UINT64 DocID bounds from the pinned Segment's INV metadata.
  // Unsupported/unproven bounds leave available false; invalid metadata/handles return errors.
  // [UINT64_MAX, 0] denotes known emptiness only after independent SSTable emptiness validation.
  int get_segment_rowid_range(const uint64_t index_table_id,
                             const common::ObNewVersionRange &version_range,
                             bool &available,
                             uint64_t &min_rowid,
                             uint64_t &max_rowid) const;

  TO_STRING_KV(K_(ls_id), K_(main_tablet_id), K_(index_table_ids),
               K_(index_tablet_ids), K_(sql_read_snapshot), K_(storage_group_ctx));

  share::ObLSID ls_id_;
  common::ObTabletID main_tablet_id_;
  common::ObSEArray<uint64_t, 1> index_table_ids_;
  common::ObSEArray<common::ObTabletID, 1> index_tablet_ids_;
  share::SCN sql_read_snapshot_;
  storage::ObAsyncIndexSegmentGroupCtx storage_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObAsyncIndexPartitionSegCtx);
};

/**
 * @brief Query-level state for async-index DAS execution.
 *
 * It is owned by ObSqcCtx for PX, ObTableScanOp for local non-PX execution, or ObDASScanOp for a
 * remote DAS task. Prepare pins the complete immutable partition catalog and must be called exactly
 * once in the owner lifecycle. The finalized object is read-only while tasks run and can be shared
 * by all DAS scan tasks in its owner scope; partition selection remains worker-local.
 *
 * MOCK SEAM: set_mock_fetcher() injects a stub for get_async_index_segment_group_ctx so the flow can
 * be exercised in mittest without the storage-side iterator.
 */
class ObDASAsyncIndexQueryCtx
{
public:
  // Fetch one partition's segment ctx. Default impl calls ObAccessService; tests inject a stub.
  typedef int (*MockFetcher)(void *arg,
                             const share::ObLSID &ls_id,
                             const common::ObIArray<uint64_t> &index_table_ids,
                             const common::ObIArray<common::ObTabletID> &index_tablet_ids,
                             const common::ObTabletID &delete_tablet_id,
                             const share::SCN &sql_read_snapshot,
                             ObAsyncIndexPartitionSegCtx &out_seg_ctx);

  ObDASAsyncIndexQueryCtx()
    : allocator_(),
      partitions_(),
      group_alignment_enabled_(false),
      mock_fetcher_(nullptr),
      mock_fetcher_arg_(nullptr),
      is_inited_(false)
  {}
  ~ObDASAsyncIndexQueryCtx() { reset(); }
  void reset();

  void set_mock_fetcher(MockFetcher fetcher, void *arg)
  { mock_fetcher_ = fetcher; mock_fetcher_arg_ = arg; }

  static int check_tx_isolation(const ObSQLSessionInfo *session);
  // Recursively detect async fulltext or scalar index queries in an attach ctdef tree.
  static bool contains_async_index_query(const ObDASBaseCtDef *ctdef);
  // Validate async wrapper descriptors, analyze the user query domain, and count independent
  // semantic roots per logical async index.
  static int analyze_async_index_domain(
      const ObDASBaseCtDef *ctdef,
      ObDASAsyncIndexDomainAnalysis &analysis);
  // Check whether a subtree that is already known to be in Boolean FILTER
  // context can participate in Group execution. This deliberately shares the
  // recursive domain rules with analyze_async_index_domain(), so CodeGen does
  // not accept a narrower set of nested/lookup-wrapped synchronous filters.
  static int is_group_compatible_filter_domain(
      const ObDASBaseCtDef *ctdef,
      bool &is_compatible);
  // Classify a Boolean FILTER subtree by visibility scope. Pure synchronous
  // clauses can be materialized once for the query, while async clauses retain
  // their Group binding. A subtree containing both kinds is compatible but is
  // not necessarily splittable at the caller's clause boundary.
  static int analyze_group_filter_domain(
      const ObDASBaseCtDef *ctdef,
      ObDASGroupFilterDomainAnalysis &analysis);
  // Validate supported async query shapes and collect logical index table IDs.
  static int collect_async_index_table_ids(
      const ObDASBaseCtDef *ctdef,
      common::ObIArray<uint64_t> &index_table_ids);
  // Collect the logical delete-index table id shared by all async index wrappers.
  static int collect_async_delete_table_id(
      const ObDASBaseCtDef *ctdef,
      uint64_t &delete_table_id);

  // Build and finalize an immutable catalog from the complete candidate set in one owner scope.
  // Repeated calls violate the owner lifecycle and return OB_INIT_TWICE.
  int prepare(const ObDASBaseCtDef *ctdef,
              const common::ObIArray<const ObDASTabletLoc *> &tablet_locs,
              const share::SCN &sql_read_snapshot,
              const ObDASAsyncIndexExecMode exec_mode);
  // Prepare one already-resolved remote partition when no tablet-location graph is retained.
  int prepare_resolved_partition(
      const ObDASBaseCtDef *ctdef,
      const share::ObLSID &ls_id,
      const common::ObTabletID &main_tablet_id,
      const common::ObIArray<uint64_t> &index_table_ids,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      const common::ObTabletID &delete_tablet_id,
      const share::SCN &sql_read_snapshot,
      const ObDASAsyncIndexExecMode exec_mode);
  // Add one partition by calling the segment-group ctx interface (or the mock seam).
  int add_partition(const share::ObLSID &ls_id,
                    const common::ObTabletID &main_tablet_id,
                    const common::ObIArray<uint64_t> &index_table_ids,
                    const common::ObIArray<common::ObTabletID> &index_tablet_ids,
                    const common::ObTabletID &delete_tablet_id,
                    const share::SCN &sql_read_snapshot);
  // After all partitions are added, freeze the catalog.
  int finalize();

  // ----- accessors used during build / rescan / snapshot injection -----
  bool is_inited() const { return is_inited_; }
  bool is_group_alignment_enabled() const { return group_alignment_enabled_; }
  int64_t get_partition_cnt() const { return partitions_.count(); }
  int64_t get_max_group_cnt() const;
  int64_t get_max_segment_cnt(const uint64_t index_table_id) const;
  int64_t get_max_segment_cnt(
      const int64_t group_slot,
      const uint64_t index_table_id) const;

  // The catalog is immutable after finalize. Each worker keeps its own current-partition pointer.
  int find_partition(const common::ObTabletID &main_tablet_id,
                     const ObAsyncIndexPartitionSegCtx *&partition) const;

  TO_STRING_KV(K_(partitions), K_(group_alignment_enabled), K_(is_inited));

private:
  int default_fetch_(const share::ObLSID &ls_id,
                     const common::ObIArray<common::ObTabletID> &index_tablet_ids,
                     const common::ObTabletID &delete_tablet_id,
                     const share::SCN &sql_read_snapshot,
                     ObAsyncIndexPartitionSegCtx &out_seg_ctx);
  static int build_normalized_catalog_input_(
      const common::ObIArray<const ObDASTabletLoc *> &tablet_locs,
      const common::ObIArray<uint64_t> &index_table_ids,
      const uint64_t delete_table_id,
      common::ObIArray<ObDASAsyncIndexCandidate> &candidates,
      common::ObIArray<ObDASAsyncIndexBinding> &bindings);
  static int build_normalized_resolved_input_(
      const share::ObLSID &ls_id,
      const common::ObTabletID &main_tablet_id,
      const common::ObIArray<uint64_t> &index_table_ids,
      const common::ObIArray<common::ObTabletID> &index_tablet_ids,
      const common::ObTabletID &delete_tablet_id,
      common::ObIArray<ObDASAsyncIndexCandidate> &candidates,
      common::ObIArray<ObDASAsyncIndexBinding> &bindings);
  static int build_canonical_index_table_ids_(
      const common::ObIArray<uint64_t> &index_table_ids,
      common::ObIArray<uint64_t> &canonical_ids);
  static int restore_candidate_bindings_(
      const ObDASAsyncIndexCandidate &candidate,
      const common::ObIArray<ObDASAsyncIndexBinding> &bindings,
      common::ObIArray<uint64_t> &index_table_ids,
      common::ObIArray<common::ObTabletID> &index_tablet_ids);

private:
  common::ObArenaAllocator allocator_;
  common::ObSEArray<ObAsyncIndexPartitionSegCtx *, 4> partitions_;
  bool group_alignment_enabled_;
  MockFetcher mock_fetcher_;
  void *mock_fetcher_arg_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIndexQueryCtx);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_INDEX_QUERY_CTX_H_
