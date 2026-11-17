/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX TRANS
#include "ob_mvcc_engine.h"
#include "storage/memtable/ob_row_conflict_handler.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;
namespace memtable
{

ObMvccEngine::ObMvccEngine()
    : is_inited_(false),
      kv_builder_(NULL),
      query_engine_(NULL),
      engine_allocator_(NULL),
      memtable_(NULL)
{
}

ObMvccEngine::~ObMvccEngine()
{
  destroy();
}

int ObMvccEngine::init(
    common::ObIAllocator *allocator,
    ObMTKVBuilder *kv_builder,
    ObQueryEngine *query_engine,
    ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    TRANS_LOG(WARN, "init twice", KP(this));
    ret = OB_INIT_TWICE;
  } else if (NULL == allocator
             || NULL == kv_builder
             || NULL == query_engine
             || NULL == memtable) {
    TRANS_LOG(WARN, "invalid param", K(allocator), K(kv_builder), K(query_engine), K(memtable));
    ret = OB_INVALID_ARGUMENT;
  } else {
    engine_allocator_ = allocator;
    kv_builder_ = kv_builder;
    query_engine_ = query_engine;
    memtable_ = memtable;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObMvccEngine::destroy()
{
  is_inited_ = false;
  kv_builder_ = NULL;
  query_engine_ = NULL;
  engine_allocator_ = NULL;
  memtable_ = NULL;
}
int ObMvccEngine::try_compact_row_when_mvcc_read_(const SCN &snapshot_version,
                                                  ObMvccRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t latest_compact_ts = row.latest_compact_ts_;
  const int64_t WEAK_READ_COMPACT_THRESHOLD = 3 * 1000 * 1000;
  if (SCN::min_scn() >= snapshot_version) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid snapshot version", K(ret), K(snapshot_version));
  } else if (SCN::max_scn() == snapshot_version
      || ObTimeUtility::current_time() < latest_compact_ts + WEAK_READ_COMPACT_THRESHOLD) {
    // do not compact row when merging
  } else {
    ObRowLatchGuard guard(row.latch_);
    if (OB_FAIL(row.row_compact(memtable_,
                                snapshot_version,
                                engine_allocator_))) {
      TRANS_LOG(WARN, "row compact error", K(ret), K(snapshot_version));
    }
  }
  return ret;
}

int ObMvccEngine::get(ObMvccAccessCtx &ctx,
                      const ObQueryFlag &query_flag,
                      const ObMemtableKey *parameter_key,
                      const share::ObLSID memtable_ls_id,
                      ObMemtableKey *returned_key,
                      ObMvccValueIterator &value_iter,
                      ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  ObMvccRow *value = NULL;
  const bool for_read = true;
  const bool for_replay = false;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(parameter_key)) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_get_(parameter_key, value, returned_key))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else if (!query_flag.is_prewarm() && value->need_compact(for_read, for_replay, memtable_->is_delete_insert_table())) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_compact_row_when_mvcc_read_(ctx.get_snapshot_version(), *value))) {
      TRANS_LOG(WARN, "fail to try to compact row", K(tmp_ret));
    }
  } else if (query_flag.is_for_foreign_key_check() || query_flag.is_plain_insert_gts_opt()) {
    ret = ObRowConflictHandler::check_foreign_key_constraint_for_memtable(ctx, value, lock_state);
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(value_iter.init(ctx,
                                returned_key,
                                value,
                                memtable_ls_id,
                                query_flag))) {
      TRANS_LOG(WARN, "ObMvccValueIterator init fail", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", KR(ret), K(ctx), KP(parameter_key), KP(&value_iter));
  }
  return ret;
}

int ObMvccEngine::scan(
    ObMvccAccessCtx &ctx,
    const ObQueryFlag &query_flag,
    const ObMvccScanRange &range,
    const share::ObLSID memtable_ls_id,
    ObMvccRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_iter.init(*query_engine_,
                                   ctx,
                                   range,
                                   memtable_ls_id,
                                   query_flag))) {
    TRANS_LOG(WARN, "row_iter init fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), K(range));
  }
  return ret;
}

int ObMvccEngine::scan(
    ObMvccAccessCtx &ctx,
    const ObMvccScanRange &range,
    const common::ObVersionRange &version_range,
    ObMultiVersionRowIterator &row_iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(row_iter.init(*query_engine_,
                                   ctx,
                                   version_range,
                                   range))) {
    TRANS_LOG(WARN, "row_iter init fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get fail", K(ret), K(ctx), K(range));
  }
  return ret;
}

int ObMvccEngine::estimate_scan_row_count(
    const transaction::ObTransID &tx_id,
    const ObMvccScanRange &range,
    ObPartitionEst &part_est) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "not init", KP(this));
    ret = OB_NOT_INIT;
  } else if (!range.is_valid()) {
    TRANS_LOG(WARN, "invalid param");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(query_engine_->estimate_row_count(
              tx_id,
              range.start_key_,  !range.border_flag_.inclusive_start(),
              range.end_key_,    !range.border_flag_.inclusive_end(),
              part_est.logical_row_count_, part_est.physical_row_count_))) {
    TRANS_LOG(WARN, "query engine estimate row count fail", K(ret));
  }

  return ret;
}

int ObMvccEngine::check_row_locked(ObMvccAccessCtx &ctx,
                                   const ObMemtableKey *key,
                                   ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  ObMemtableKey stored_key;
  ObMvccRow *value = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mvcc_engine not init", K(this));
  } else if (OB_FAIL(query_engine_get_(key, value, &stored_key))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      // rewrite ret
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(value->check_row_locked(ctx, lock_state))) {
    TRANS_LOG(WARN, "check row locked fail", K(ret), KPC(value), K(ctx), K(lock_state));
  }

  return ret;
}
int ObMvccEngine::mvcc_write(storage::ObStoreCtx &ctx,
                              ObMvccRow &value,
                              const ObTxNodeArg &arg,
                              const bool check_exist,
                              void *buf, // preallocted buffer for ObMvccTransNode
                              ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *node = (ObMvccTransNode *)buf;

  if (OB_FAIL(init_tx_node_for_write_(ctx, arg, node))) {
    TRANS_LOG(WARN, "build tx node failed", K(ret), K(ctx), K(arg));
    node = nullptr; // Do not inspect an incomplete data header during cleanup.
  } else if (OB_FAIL(value.mvcc_write(ctx,
                                      *node,
                                      check_exist,
                                      res))) {
    if (!is_mvcc_write_related_error_(ret)) {
      TRANS_LOG(WARN, "mvcc write failed", K(ret), K(ctx), K(arg),
                KPC(node), K(value));
    }
  } else {
    TRANS_LOG(DEBUG, "mvcc write succeed", K(ret), K(ctx), K(arg),
              KPC(node), K(value));
  }
  if (nullptr != node && (OB_SUCCESS != ret || !res.has_insert())) {
    node->release_lob_redo();
  }

  return ret;
}

int ObMvccEngine::mvcc_write(storage::ObStoreCtx &ctx,
                             ObMvccRow &value,
                             const ObTxNodeArg &arg,
                             const bool check_exist,
                             ObMvccWriteResult &res)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *node = nullptr;

  if (OB_FAIL(build_tx_node_for_write_(ctx, arg, node))) {
    TRANS_LOG(WARN, "build tx node failed", K(ret), K(ctx), K(arg));
    node = nullptr; // Do not inspect an incomplete data header during cleanup.
  } else if (OB_FAIL(value.mvcc_write(ctx,
                                      *node,
                                      check_exist,
                                      res))) {
    if (!is_mvcc_write_related_error_(ret)) {
      TRANS_LOG(WARN, "mvcc write failed", K(ret), K(ctx), K(arg),
                KPC(node), K(value));
    }
  } else {
    TRANS_LOG(DEBUG, "mvcc write succeed", K(ret), K(ctx), K(arg),
              KPC(node), K(value));
  }
  if (nullptr != node && (OB_SUCCESS != ret || !res.has_insert())) {
    node->release_lob_redo();
  }

  return ret;
}

int ObMvccEngine::mvcc_replay(const ObTxNodeArg &arg, ObMvccReplayResult &res)
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *node = NULL;
  if (OB_FAIL(build_tx_node_for_replay_(arg, node))) {
    TRANS_LOG(WARN, "build tx node failed", K(ret), K(arg));
  } else {
    res.tx_node_ = node;
    TRANS_LOG(DEBUG, "mvcc replay succeed", K(ret), K(arg));
  }
  return ret;
}

// MDS write path: buf_ is [ ObMemtableDataHeader ][ ObMdsValueMeta ][ V ]
// (buf_ is FAM tail). meta carries mds_type_; live V is at mds_inner_value_ptr().
// Total allocation matches dup_data-style sizing so checksum / dup_size stay consistent.
int ObMvccEngine::build_mds_tx_node_(const ObTxNodeArg &arg,
                                     ObMvccTransNode *&node)
{
  int ret = OB_SUCCESS;
  node = nullptr;
  const storage::ObMdsTypeRegistry &registry = storage::ObMdsTypeRegistry::get_instance();
  const storage::ObMdsTypeRegistry::TypeOps *ops = registry.get_ops(static_cast<uint16_t>(arg.mds_type_));
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "mds type not registered", K(ret), K(arg.mds_type_));
  } else {
    const int64_t hdr_buf_len = sizeof(ObMdsValueMeta) + ops->value_size;
    const int64_t alloc_size = sizeof(ObMvccTransNode) + sizeof(ObMemtableDataHeader) + hdr_buf_len;
    if (OB_ISNULL(node = (ObMvccTransNode *)engine_allocator_->alloc(alloc_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc ObMvccTransNode for mds failed", K(ret), K(alloc_size));
    } else {
      new(node) ObMvccTransNode();
      ObMemtableDataHeader *hdr = reinterpret_cast<ObMemtableDataHeader *>(node->buf_);
      new (hdr) ObMemtableDataHeader(arg.mds_dml_flag_,
                                     hdr_buf_len,
                                     ObMemtablePayloadKind::MDS_LIVE_VALUE);
      MEMSET(hdr->buf_, 0, hdr_buf_len);
      storage::ObMdsValueMeta *meta = hdr->mds_meta_ptr();
      if (OB_NOT_NULL(arg.mds_meta_)) {
        // assign() copies payload fields only and never adopts the source's
        // patch_seq_: this fresh record must start with a stable (even, 0)
        // seq -- a copied odd seq would make every reader of this node spin
        // on a patch that no writer will ever finish. (seq is already 0 here
        // from the MEMSET above; assign() keeps it that way by contract.)
        meta->assign(*arg.mds_meta_);
      } else {
        meta->init(arg.tx_id_);
      }
      meta->mds_type_ = static_cast<uint16_t>(arg.mds_type_);
      void *user_data = hdr->mds_inner_value_ptr();
      ops->construct(user_data);
      if (OB_ISNULL(arg.mds_value_)) {
        // no source value to copy, keep the constructed default value
      } else if (OB_ISNULL(ops->copy_from)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "mds type has no copy_from op", K(ret), K(arg.mds_type_));
      } else if (OB_FAIL(ops->copy_from(user_data, arg.mds_value_, engine_allocator_))) {
        TRANS_LOG(WARN, "copy mds value into tx node failed", K(ret), K(arg.mds_type_));
      }
      if (OB_UNLIKELY(OB_SUCCESS != ret)) {
        ops->destruct(user_data);
        node->~ObMvccTransNode();
        engine_allocator_->free(node);
        node = nullptr;
      }
    }
  }
  TRANS_LOG(TRACE, "completed build_mds_tx_node", K(ret), K(arg.mds_type_));
  return ret;
}

static inline void apply_common_tx_node_fields_(const ObTxNodeArg &arg,
                                                ObMvccTransNode *node)
{
  node->tx_id_ = arg.tx_id_;
  node->trans_version_ = SCN::max_scn();
  node->modify_count_ = arg.modify_count_;
  node->acc_checksum_ = arg.acc_checksum_;
  node->version_ = arg.memstore_version_;
  node->scn_ = arg.scn_;
  node->seq_no_ = arg.seq_no_;
  node->write_epoch_ = arg.write_epoch_;
  node->prev_ = NULL;
  node->next_ = NULL;
  if (node->scn_.is_max()) {
    // leader write: subsequent sstable check can still fail, so hide the
    // node behind INCOMPLETE until finish_kv promotes it.
    node->set_incomplete();
  }
}

// Alloc the memory and build the ObMvccTransNode base on the memory
int ObMvccEngine::build_tx_node_(const ObTxNodeArg &arg,
                                 ObMvccTransNode *&node)
{
  int ret = OB_SUCCESS;
  if (arg.is_mds_write_) {
    if (OB_FAIL(build_mds_tx_node_(arg, node))) {
      TRANS_LOG(WARN, "build mds tx node failed", K(ret), K(arg));
    }
  } else if (OB_FAIL(kv_builder_->dup_data(node, *engine_allocator_, arg.data_))) {
    TRANS_LOG(WARN, "MvccTranNode dup fail", K(ret), "node", node);
  }
  if (OB_SUCC(ret)) {
    apply_common_tx_node_fields_(arg, node);
  }
  return ret;
}

// Build the ObMvccTransNode with preallocted memory
int ObMvccEngine::init_tx_node_(const ObTxNodeArg &arg,
                                ObMvccTransNode *node)
{
  int ret = OB_SUCCESS;

  new(node) ObMvccTransNode();
  if (arg.is_mds_write_) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "mds write is not supported", K(ret), K(arg));
  } else if (OB_FAIL(ObMemtableDataHeader::build(
                       reinterpret_cast<ObMemtableDataHeader *>(node->buf_),
                       arg.data_))) {
    TRANS_LOG(WARN, "MemtableData dup fail", K(ret));
  } else {
    apply_common_tx_node_fields_(arg, node);
  }

  return ret;
}

void ObMvccEngine::finish_kv(ObMvccWriteResult& res)
{
  // The trans_node after ObMvccRow::mvcc_write_ is incomplete, then we need use
  // finish_kv as the final step of ObMemtable::set. Therefore, it is safe to
  // make the data visible.
  if (nullptr != res.tx_node_) {
    res.tx_node_->set_complete();
  }
}

void ObMvccEngine::finish_kvs(ObMvccWriteResults& results)
{
  // The trans_node after ObMvccRow::mvcc_write_ is incomplete, then we need use
  // finish_kv as the final step of ObMemtable::multi_set. Therefore, it is safe
  // to make the data visible.
  for (int64_t i = 0; i < results.count(); ++i) {
    ObMvccWriteResult &res = results[i];
    if (nullptr != res.tx_node_) {
      res.tx_node_->set_complete();
    }
  }
}
void ObMvccEngine::mvcc_undo(ObMvccRow *value)
{
  value->mvcc_undo();
}

int ObMvccEngine::create_kv(const ObMemtableKey *key,
                            const bool no_get_before_set,
                            ObMemtableKey *stored_key,
                            ObMvccRow *&value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = create_kv_(key, no_get_before_set, stored_key, value);
  }
  return ret;
}

int ObMvccEngine::create_kvs(const ObMemtableSetArg &memtable_set_arg,
                             ObMemtableKeyGenerator &memtable_key_generator,
                             const bool no_get_before_set,
                             ObStoredKVs &kvs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    const blocksstable::ObDatumRow *new_rows = memtable_set_arg.new_row_;
    const int64_t row_count = memtable_set_arg.row_count_;
    ObMemtableKeyGenerator::ObMemtableKeyBuffer *memtable_key_buffer =
        memtable_key_generator.get_key_buffer();
  
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_FAIL(memtable_key_generator.generate_memtable_key(new_rows[i]))) {
        TRANS_LOG(WARN, "generate memtable key fail", K(ret), K(memtable_set_arg));
      } else if (OB_FAIL(create_kv_(&memtable_key_generator.get_memtable_key(),
                                    no_get_before_set,
                                    &kvs.at(i).key_,
                                    kvs.at(i).value_))) {
        TRANS_LOG(WARN, "create kv fail", K(ret), K(memtable_set_arg));
      } else if (nullptr != memtable_key_buffer &&
                 OB_FAIL(memtable_key_buffer->push_back(kvs[i].key_))) {
        TRANS_LOG(WARN, "push back stored memtable key into buffer failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMvccEngine::ensure_kv(const ObMemtableKey *stored_key, ObMvccRow *value)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ObRowLatchGuard guard(value->latch_);
    if (OB_FAIL(query_engine_->ensure(stored_key, value))) {
      TRANS_LOG(WARN, "ensure_row fail", K(ret));
    }
  }
  return ret;
}

int ObMvccEngine::query_engine_get_(const ObMemtableKey *parameter_key,
                                    ObMvccRow *&row,
                                    ObMemtableKey *returned_key)
{
  return query_engine_->get(parameter_key, row, returned_key);
}

int ObMvccEngine::create_kv_(const ObMemtableKey *key,
                             const bool no_get_before_set,
                             ObMemtableKey *stored_key,
                             ObMvccRow *&value)
{
  int64_t loop_cnt = 0;
  int ret = OB_SUCCESS;
  value = nullptr;
  while (OB_SUCCESS == ret && NULL == value) {
    ObStoreRowkey *tmp_key = nullptr;
    // We optimize the create_kv operation by skipping the first hash table
    // get for insert operation because it is unnecessary at most cases. Under
    // the concurrent inserts, we rely on the conflict on the hash table set
    // and the while loops for the next hash table get to maintain the origin
    // create_kv semantic
    if (!(0 == loop_cnt // is the first try in the loop
          && no_get_before_set)
        && OB_SUCC(query_engine_->get(key, value, stored_key))) {
      if (NULL == value) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "get NULL value");
      }
    } else if (OB_FAIL(kv_builder_->dup_key(tmp_key,
                                            *engine_allocator_,
                                            key->get_rowkey()))) {
      TRANS_LOG(WARN, "key dup fail", K(ret));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(stored_key->encode(tmp_key))) {
      TRANS_LOG(WARN, "key encode fail", K(ret));
    } else if (NULL == (value = (ObMvccRow *)engine_allocator_->alloc(sizeof(*value)))) {
      TRANS_LOG(WARN, "alloc ObMvccRow fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      value = new(value) ObMvccRow();
      value->set_host_tablet_id(memtable_->get_tablet_id());
      if (OB_SUCCESS == (ret = query_engine_->set(stored_key, value))) {
        TRANS_LOG(DEBUG, "create kv succeed", K(ret), K(*key), K(*stored_key));
      } else if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
        value = NULL;
      } else {
        value = NULL;
      }
    }
    loop_cnt++;
  }

  if (loop_cnt > 2) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000) || 3 == loop_cnt) {
      TRANS_LOG(ERROR, "unexpected loop cnt when preparing kv", K(ret), K(loop_cnt), K(*key), K(*stored_key));
    }
  }
  return ret;
}


//========================== ObMvccEngineWithoutHashIndex ==========================//

ObMvccEngineWithoutHashIndex::ObMvccEngineWithoutHashIndex()
  : ObMvccEngine()
{
}

ObMvccEngineWithoutHashIndex::~ObMvccEngineWithoutHashIndex()
{
}
int ObMvccEngineWithoutHashIndex::create_kv(const ObMemtableKey *key,
                                            const bool no_get_before_set,
                                            ObMemtableKey *stored_key,
                                            ObMvccRow *&value)
{
  UNUSED(no_get_before_set);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    ret = create_btree_kv_(key, stored_key, value);
  }
  return ret;
}

int ObMvccEngineWithoutHashIndex::create_kvs(const ObMemtableSetArg &memtable_set_arg,
                                             ObMemtableKeyGenerator &memtable_key_generator,
                                             const bool no_get_before_set,
                                             ObStoredKVs &kvs)
{
  UNUSED(no_get_before_set);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    const blocksstable::ObDatumRow *new_rows = memtable_set_arg.new_row_;
    const int64_t row_count = memtable_set_arg.row_count_;
    ObMemtableKeyGenerator::ObMemtableKeyBuffer *memtable_key_buffer = memtable_key_generator.get_key_buffer();

    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_FAIL(memtable_key_generator.generate_memtable_key(new_rows[i]))) {
        TRANS_LOG(WARN, "generate memtable key fail", K(ret), K(memtable_set_arg));
      } else if (OB_FAIL(create_btree_kv_(&memtable_key_generator.get_memtable_key(),
                                          &kvs.at(i).key_,
                                          kvs.at(i).value_))) {
        TRANS_LOG(WARN, "create kv fail", K(ret), K(memtable_set_arg));
      } else if (nullptr != memtable_key_buffer &&
                 OB_FAIL(memtable_key_buffer->push_back(kvs[i].key_))) {
        TRANS_LOG(WARN, "push back stored memtable key into buffer failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMvccEngineWithoutHashIndex::ensure_kv(const ObMemtableKey *stored_key, ObMvccRow *value)
{
  // the kv has been inserted into btree when create_kv_ without hash index is called, no need to ensure kv
  return OB_SUCCESS;
}

int ObMvccEngineWithoutHashIndex::query_engine_get_(const ObMemtableKey *parameter_key,
                                                   ObMvccRow *&row,
                                                   ObMemtableKey *returned_key)
{
  return query_engine_->get_from_btree(parameter_key, row, returned_key);
}

int ObMvccEngineWithoutHashIndex::create_btree_kv_(const ObMemtableKey *key,
                                                   ObMemtableKey *stored_key,
                                                   ObMvccRow *&value)
{
  int ret = OB_SUCCESS;
  ObQueryEngine::ObMvccRowCreator row_creator = [this, key, stored_key](const bool is_exist_key,
                                                                        ObStoreRowkeyWrapper &new_or_exist_key,
                                                                        ObMvccRow *&new_row) {
    int ret = OB_SUCCESS;
    if (is_exist_key) { // the key already exists in the btree
      stored_key->encode(new_or_exist_key.get_rowkey());
    } else {
      ObStoreRowkey *tmp_key = nullptr;
      if (OB_FAIL(kv_builder_->dup_key(tmp_key, *engine_allocator_, key->get_rowkey()))) {
        TRANS_LOG(WARN, "key dup fail", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_ISNULL(new_row = (ObMvccRow *)engine_allocator_->alloc(sizeof(*new_row)))) {
        TRANS_LOG(WARN, "alloc ObMvccRow fail", K(ret));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        stored_key->encode(tmp_key);
        new_or_exist_key = ObStoreRowkeyWrapper(tmp_key);
        new_row = new(new_row) ObMvccRow();
        new_row->set_host_tablet_id(memtable_->get_tablet_id());
      }
    }
    return ret;
  };

  value = nullptr;
  if (OB_FAIL(query_engine_->create_btree_kv(key, row_creator, value))) {
    TRANS_LOG(WARN, "create btree kv fail", K(ret), K(key));
  } else if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get NULL value", K(ret), K(key));
  }

  return ret;
}

int ObMvccEngine::build_tx_node_for_write_(ObStoreCtx &ctx, const ObTxNodeArg &arg,
    ObMvccTransNode *&node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_tx_node_(arg, node))) {
    TRANS_LOG(WARN, "failed to build write node", K(ret), K(arg));
  } else if (OB_FAIL(prepare_lob_redo_(ctx, arg, *node))) {
    TRANS_LOG(WARN, "failed to prepare LOB redo for write node", K(ret), K(arg));
  }
  return ret;
}

int ObMvccEngine::init_tx_node_for_write_(ObStoreCtx &ctx, const ObTxNodeArg &arg,
    ObMvccTransNode *node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_tx_node_(arg, node))) {
    TRANS_LOG(WARN, "failed to initialize write node", K(ret), K(arg));
  } else if (OB_FAIL(prepare_lob_redo_(ctx, arg, *node))) {
    TRANS_LOG(WARN, "failed to prepare LOB redo for preallocated node", K(ret), K(arg));
  }
  return ret;
}

int ObMvccEngine::build_tx_node_for_replay_(const ObTxNodeArg &arg, ObMvccTransNode *&node)
{
  int ret = OB_SUCCESS;
  if (nullptr != arg.lob_redo_payload_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "replay node must carry a digest, not a write payload", K(ret), K(arg));
  } else if (OB_FAIL(build_tx_node_(arg, node))) {
    TRANS_LOG(WARN, "failed to build replay node", K(ret), K(arg));
  }
  return ret;
}

int ObMvccEngine::prepare_lob_redo_(ObStoreCtx &ctx, const ObTxNodeArg &arg, ObMvccTransNode &node)
{
  int ret = OB_SUCCESS;
  ObMemtableDataHeader *header = reinterpret_cast<ObMemtableDataHeader *>(node.buf_);
  const bool with_lob_payload = header->with_lob_payload();
  if (nullptr == arg.lob_redo_payload_ && !with_lob_payload) {
    // Ordinary rows have no LOB attachment.
  } else if (nullptr == arg.lob_redo_payload_ || !with_lob_payload
      || nullptr == ctx.mvcc_acc_ctx_.get_mem_ctx()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid LOB write node payload", K(ret), K(arg));
  } else if (blocksstable::ObDmlFlag::DF_INSERT != header->dml_flag_
        && blocksstable::ObDmlFlag::DF_UPDATE != header->dml_flag_
        && blocksstable::ObDmlFlag::DF_PUT != header->dml_flag_) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(header->lob_redo_info()->init(
      ctx.mvcc_acc_ctx_.get_mem_ctx()->get_tenant_id(), *arg.lob_redo_payload_))) {
    TRANS_LOG(WARN, "failed to build LOB redo in trans node", K(ret), K(arg));
  }
  return ret;
}

} // end namespace memtable
} // end namespace oceanbase
