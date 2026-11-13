// Included from ob_i_tablet_mds_interface.h (outer guard). Do not wrap these includes in
// INCLUDE_OB_TABLET_MDS_PART_IPP — the .h defines that macro before #include of this file.
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_fork_src_mds_data.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_node_info_for_virtual_table.h"
#include "storage/mds_table/ob_mds_table_datum_row_converter.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include <cstdint>
#include <type_traits>
namespace oceanbase
{
namespace storage
{

/********************************READ-PATH RETRY FRAMEWORK*****************************************/
//
// retry_with_ls_switch_check_ owns the shared LS-switch / double-check /
// retry loop. Op is one of the *Op nested structs below;
// it must expose int operator()() that performs the actual mds_*_row_ call
// plus error-code translation and self-logging.

template <typename Op>
inline int ObITabletMdsInterface::retry_with_ls_switch_check_(ObLS *ls, Op &op) const
{
  int ret = OB_SUCCESS;
  bool is_online = false;
  if (OB_ISNULL(ls)) {
    ret = OB_BAD_NULL_ERROR;
  } else {
    bool is_epoch_match = true;
    do {
      is_epoch_match = true;
      ObLSSwitchChecker ls_switch_checker(*ls);
      is_online = ls_switch_checker.check_online();
      if (!is_online) {
        ret = OB_LS_OFFLINE;
      } else if (OB_FAIL(op())) {
        // op already logged + translated error codes
      } else {
        is_online = ls_switch_checker.double_check_online(is_epoch_match);
        if (!is_online) {
          ret = OB_LS_OFFLINE;
        }
        if (!is_epoch_match) {
          MDS_LOG(WARN, "ls switch epoch changed", K(ret), KPC(this));
        }
      }
    } while (!is_epoch_match && is_online);
  }
  return ret;
}

// ---- IsLockedByOthersOp -----------------------------------------------------
template <typename T>
struct ObITabletMdsInterface::IsLockedByOthersOp
{
  IsLockedByOthersOp(const ObITabletMdsInterface &self_iface,
                     const mds::MdsWriter &self_writer,
                     const int64_t timeout_us,
                     bool &is_locked)
      : self_iface_(self_iface),
        self_writer_(self_writer),
        timeout_us_(timeout_us),
        is_locked_(is_locked),
        row_info_()
  {}

  int operator()()
  {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObMdsTable *mds_table = nullptr;
    if (OB_FAIL(self_iface_.get_mds_table_for_write_(ls_handle, mds_table))) {
      MDS_LOG(WARN, "get ls aggregate mds table failed", K(ret), KPC(&self_iface_));
    } else if (OB_ISNULL(mds_table)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "mds table is null", K(ret), KPC(&self_iface_));
    } else {
      mds::DummyKey dummy_key;
      if (OB_FAIL((mds_table->get_mds_row_read_info_<mds::DummyKey, T>(
                     nullptr,
                     self_iface_.get_tablet_id_(),
                     self_iface_.get_reorganization_scn(),
                     dummy_key,
                     share::SCN::max_scn(),
                     timeout_us_,
                     row_info_)))) {
        if (OB_ITER_END == ret || OB_EMPTY_RESULT == ret || OB_SNAPSHOT_DISCARDED == ret) {
          ret = OB_SUCCESS;
          is_locked_ = false;
        } else {
          MDS_LOG(WARN, "failed to check lock unit data", K(ret), KPC(&self_iface_));
        }
      } else {
        const bool is_self_tx = self_writer_.writer_type_ == mds::WriterType::TRANSACTION
                             && row_info_.writer_tx_id_.is_valid()
                             && row_info_.writer_tx_id_.get_id() == self_writer_.writer_id_;
        is_locked_ = row_info_.is_uncommitted_ && !is_self_tx;
        MDS_LOG(TRACE, "success to get is locked by others state", K(ret), KPC(&self_iface_), K(is_locked_));
      }
    }
    return ret;
  }

  const ObITabletMdsInterface &self_iface_;
  const mds::MdsWriter &self_writer_;
  const int64_t timeout_us_;
  bool &is_locked_;
  ObMdsTable::ObMdsRowReadInfo row_info_;
};

// ---- GetLatestOp ------------------------------------------------------------
template <typename T>
struct ObITabletMdsInterface::GetLatestOp
{
  GetLatestOp(const ObITabletMdsInterface &self_iface,
              T &value,
              mds::MdsWriter &writer,
              mds::TwoPhaseCommitState &trans_stat,
              share::SCN &trans_version,
              ObIAllocator *alloc)
      : self_iface_(self_iface),
        value_(value),
        writer_(writer),
        trans_stat_(trans_stat),
        trans_version_(trans_version),
        alloc_(alloc)
  {}

  int operator()()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL((self_iface_.template mds_get_row_<mds::DummyKey, T>(
            mds::DummyKey(),
            share::SCN::max_scn()/*snapshot*/,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/,
            true/*read_uncommitted*/,
            value_,
            alloc_,
            writer_,
            trans_stat_,
            trans_version_)))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        ret = OB_EMPTY_RESULT;
        MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret), KPC(&self_iface_));
      } else {
        MDS_LOG(WARN, "failed to get latest data from tablet", K(ret), KPC(&self_iface_));
      }
    } else {
      MDS_LOG(TRACE, "success to get_latest", K(ret), KPC(&self_iface_));
    }
    return ret;
  }

  const ObITabletMdsInterface &self_iface_;
  T &value_;
  mds::MdsWriter &writer_;
  mds::TwoPhaseCommitState &trans_stat_;
  share::SCN &trans_version_;
  ObIAllocator *alloc_;
};

// ---- GetLatestCommittedOp ---------------------------------------------------
template <typename T>
struct ObITabletMdsInterface::GetLatestCommittedOp
{
  GetLatestCommittedOp(const ObITabletMdsInterface &self_iface, T &value, ObIAllocator *alloc)
      : self_iface_(self_iface), value_(value), alloc_(alloc)
  {}

  int operator()()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL((self_iface_.template mds_get_committed_row_<mds::DummyKey, T>(
            mds::DummyKey(),
            share::SCN::max_scn()/*snapshot*/,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US/*timeout_us*/,
            false/*read_uncommitted*/,
            value_,
            alloc_)))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        ret = OB_EMPTY_RESULT;
        MDS_LOG(WARN, "read nothing from mds sstable", K(ret), KPC(&self_iface_));
      } else {
        MDS_LOG(WARN, "failed to get latest committed data from tablet", K(ret), "type", typeid(T).name(), KPC(&self_iface_));
      }
    } else {
      MDS_LOG(TRACE, "success to get_latest_committed", K(ret), KPC(&self_iface_));
    }
    return ret;
  }

  const ObITabletMdsInterface &self_iface_;
  T &value_;
  ObIAllocator *alloc_;
};

// ---- GetSnapshotOp ----------------------------------------------------------
template <typename Key, typename Value>
struct ObITabletMdsInterface::GetSnapshotOp
{
  GetSnapshotOp(const ObITabletMdsInterface &self_iface,
                const Key &key, Value &value,
                const share::SCN snapshot, const int64_t timeout_us,
                ObIAllocator *alloc)
      : self_iface_(self_iface), key_(key), value_(value),
        snapshot_(snapshot), timeout_us_(timeout_us), alloc_(alloc)
  {}

  int operator()()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL((self_iface_.template mds_get_committed_row_<Key, Value>(
            key_, snapshot_, timeout_us_, false/*read_uncommitted*/, value_, alloc_)))) {
      if (OB_SNAPSHOT_DISCARDED == ret) {
        ret = OB_EMPTY_RESULT;
        MDS_LOG(DEBUG, "read nothing from mds sstable", K(ret), KPC(&self_iface_));
      } else {
        MDS_LOG(WARN, "failed to get snapshot data from tablet", K(ret), KPC(&self_iface_));
      }
    } else {
      MDS_LOG(TRACE, "success to get snapshot", K(ret), KPC(&self_iface_));
    }
    return ret;
  }

  const ObITabletMdsInterface &self_iface_;
  const Key &key_;
  Value &value_;
  const share::SCN snapshot_;
  const int64_t timeout_us_;
  ObIAllocator *alloc_;
};

/********************************IMPLEMENTATION WITHOUT TEMPLATE***********************************/

inline common::ObTabletID ObITabletMdsInterface::get_tablet_id_() const
{
  return get_tablet_meta_().tablet_id_;
}

inline int ObITabletMdsInterface::check_tablet_status_written(bool &written) const
{
  int ret = OB_SUCCESS;
  written = false;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), KPC(this));
  } else {
    written = get_tablet_pointer_()->is_tablet_status_written();
  }
  return ret;
}

/**********************************IMPLEMENTATION WITH TEMPLATE************************************/

template <typename K, typename V>
int ObITabletMdsInterface::mds_get_committed_row_(
    const K &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const bool read_uncommitted,
    V &out,
    common::ObIAllocator *alloc) const
{
  mds::MdsWriter unused_writer;
  mds::TwoPhaseCommitState unused_trans_stat;
  share::SCN unused_trans_version;
  return mds_get_row_(key, snapshot, timeout_us, read_uncommitted, out, alloc,
      unused_writer, unused_trans_stat, unused_trans_version);
}

template <typename K, typename V>
int ObITabletMdsInterface::mds_get_row_(
    const K &key,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    const bool read_uncommitted,
    V &out,
    common::ObIAllocator *alloc,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version) const
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObMdsTable *mds_table = nullptr;
  ObMdsValueMeta meta;
  bool is_committed = false;

  if (OB_FAIL(get_mds_table_for_write_(ls_handle, mds_table))) {
    MDS_LOG(WARN, "fail to get ls aggregate mds table", K(ret));
  } else if (OB_ISNULL(mds_table)) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "mds table is null", K(ret));
  } else {
    const common::ObTabletID tablet_id = get_tablet_id_();
    const share::SCN transfer_scn = get_reorganization_scn();
    ret = mds_table->get(
        alloc, tablet_id, transfer_scn, key, snapshot, timeout_us, read_uncommitted, out, meta);
    if (OB_FAIL(ret)) {
      if (OB_ITER_END == ret) {
        ret = OB_EMPTY_RESULT;
        MDS_LOG(WARN, "read nothing from mds tablet", K(ret), K(tablet_id), K(snapshot));
      } else if (OB_EMPTY_RESULT == ret) {
        // skip warn
      } else if (OB_SNAPSHOT_DISCARDED != ret) {
        MDS_LOG(WARN, "fail to read mds kv", K(ret));
      }
    } else {
      writer.writer_type_ = mds::WriterType::TRANSACTION;
      writer.writer_id_ = meta.get_writer_trans_id().get_id();
      if (meta.is_committed()) {
        trans_stat = mds::TwoPhaseCommitState::ON_COMMIT;
        trans_version = meta.get_commit_version();
      } else {
        trans_stat = meta.get_mds_2pc_state();
        trans_version = meta.get_trans_version();
      }
      MDS_LOG(DEBUG, "succeed to get data from mds tablet", K(ret), K(out));
    }
  }
  return ret;
}

template <typename T>// general set for dummy key unit
int ObITabletMdsInterface::set(T &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(data), K(ctx), K(lock_timeout_us)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  storage::ObStoreCtx *store_ctx = ctx.get_store_ctx();
  const share::SCN transfer_scn = get_reorganization_scn();
  ObLSHandle ls_handle;
  ObMdsTable *mds_table = nullptr;
  if (OB_ISNULL(store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_SET(WARN, "mds set requires non-null ObStoreCtx on MdsCtx (LS aggregate path)");
  } else if (FALSE_IT(store_ctx->mvcc_acc_ctx_.tx_lock_timeout_us_ = lock_timeout_us)) {
  } else if (OB_FAIL(get_mds_table_for_write_(ls_handle, mds_table))) {
    MDS_LOG_SET(WARN, "get ls aggregate mds table failed");
  } else if (MDS_FAIL((mds_table->write(
                 get_tablet_id_(), transfer_scn, std::forward<T>(data), *store_ctx, ctx)))) {
    MDS_LOG_SET(WARN, "failed to write mds node to mds tablet");
  } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
    if (OB_NOT_NULL(get_tablet_pointer_())) {
      get_tablet_pointer_()->set_tablet_status_written();
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>// general replay for dummy key unit
int ObITabletMdsInterface::replay(T &&data, mds::MdsCtx &ctx, const share::SCN &scn)// called by ObTabletReplayExecutor
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(data), K(ctx), K(scn)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    ObLSHandle ls_handle;
    ObMdsTable *mds_table = nullptr;
    if (CLICK_FAIL(get_mds_table_for_write_(ls_handle, mds_table))) {
      MDS_LOG_SET(WARN, "get ls aggregate mds table failed");
    } else if (OB_ISNULL(mds_table)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds table is null");
    } else if (CLICK_FAIL(mds_table->replay(get_tablet_id_(),
              get_reorganization_scn(),
              std::forward<T>(data),
              ctx,
              scn))) {
      if (OB_NO_NEED_UPDATE != ret) {
        MDS_LOG_SET(WARN, "failed to replay dummy key unit data via ls mds table");
      }
    } else if (std::is_same<ObTabletCreateDeleteMdsUserData, typename std::decay<T>::type>::value) {
      get_tablet_pointer_()->set_tablet_status_written();
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>// general set for multi key unit
int ObITabletMdsInterface::set(const Key &key, Value &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us, const bool check_dup)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(data), K(ctx), K(lock_timeout_us)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  storage::ObStoreCtx *store_ctx = ctx.get_store_ctx();
  ObMdsTable *mds_table = NULL;
  const share::SCN transfer_scn = get_reorganization_scn();
  ObLSHandle ls_handle;
  if (OB_ISNULL(store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_SET(WARN, "mds set requires non-null ObStoreCtx on MdsCtx (LS aggregate path)");
  } else if (FALSE_IT(store_ctx->mvcc_acc_ctx_.tx_lock_timeout_us_ = lock_timeout_us)) {
  } else if (OB_FAIL(get_mds_table_for_write_(ls_handle, mds_table))) {
    MDS_LOG_SET(WARN, "get ls aggregate mds table failed");
  } else if (OB_FAIL((mds_table->write(
      get_tablet_id_(), transfer_scn, key, std::forward<Value>(data), *store_ctx, ctx, check_dup)))) {
    MDS_LOG_SET(WARN, "failed to write mds node to mds tablet");
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>
int ObITabletMdsInterface::replay(const Key &key,
                                  Value &&mds,
                                  mds::MdsCtx &ctx,
                                  const share::SCN &scn)// called only by ObTabletReplayExecutor
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(key), K(mds), K(ctx), K(scn)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    ObLSHandle ls_handle;
    ObMdsTable *mds_table = nullptr;
    if (CLICK_FAIL(get_mds_table_for_write_(ls_handle, mds_table))) {
      MDS_LOG_SET(WARN, "get ls aggregate mds table failed");
    } else if (OB_ISNULL(mds_table)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_SET(WARN, "mds table is null");
    } else if (CLICK_FAIL(mds_table->replay(get_tablet_id_(),
            get_reorganization_scn(),
            key,
            std::forward<Value>(mds),
            ctx,
            scn))) {
      if (OB_NO_NEED_UPDATE != ret) {
        MDS_LOG_SET(WARN, "failed to replay multi key unit data via ls mds table");
      }
    } else {
      get_tablet_pointer_()->set_tablet_status_written();
      MDS_LOG_SET(TRACE, "success to replay multi key unit data");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::is_locked_by_others(bool &is_locked, const mds::MdsWriter &self) const
{// FIXME: need concern about dumped uncommitted data on tablet
  #define PRINT_WRAPPER KR(ret), K(*this), K(is_locked), K(self)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US;
  if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    IsLockedByOthersOp<T> op(*this, self, timeout_us, is_locked);
    ret = retry_with_ls_switch_check_(get_tablet_pointer_()->get_ls(), op);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::get_latest(T &value,
                                      mds::MdsWriter &writer,
                                      mds::TwoPhaseCommitState &trans_stat,
                                      share::SCN &trans_version,
                                      ObIAllocator *alloc,
                                      const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(T).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_data_complete = false;
  UNUSED(read_seq);
  if (OB_FAIL(check_mds_data_complete_<T>(is_data_complete))) {
    MDS_LOG(WARN, "failed to check data completion");
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "mds_data is not complete, try again later", K(ret), K(get_tablet_meta_().ha_status_));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    GetLatestOp<T> op(*this, value, writer, trans_stat, trans_version, alloc);
    ret = retry_with_ls_switch_check_(get_tablet_pointer_()->get_ls(), op);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP, typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int ObITabletMdsInterface::get_latest(OP &&read_op,
                                      mds::MdsWriter &writer,
                                      mds::TwoPhaseCommitState &trans_stat,
                                      share::SCN &trans_version,
                                      const int64_t read_seq) const
{
  T value;
  int ret = get_latest(value, writer, trans_stat, trans_version, nullptr, read_seq);
  if (OB_SUCC(ret)) {
    ret = read_op(value);
  }
  return ret;
}

template <typename T>
int ObITabletMdsInterface::get_latest_committed(T &value, ObIAllocator *alloc) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(typeid(T).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_data_complete = false;
  if (OB_FAIL(check_mds_data_complete_<T>(is_data_complete))) {
    MDS_LOG(WARN, "failed to check data completion");
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "mds_data is not complete, try again later", K(ret), K(get_tablet_meta_().ha_status_));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    GetLatestCommittedOp<T> op(*this, value, alloc);
    ret = retry_with_ls_switch_check_(get_tablet_pointer_()->get_ls(), op);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP, typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int ObITabletMdsInterface::get_latest_committed(OP &&read_op) const
{
  T value;
  int ret = get_latest_committed(value, nullptr);
  if (OB_SUCC(ret)) {
    ret = read_op(value);
  }
  return ret;
}

template <typename T>
int ObITabletMdsInterface::get_snapshot(T &value,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us,
                                        ObIAllocator *alloc,
                                        const int64_t read_seq) const
{
  UNUSED(read_seq);
  return get_snapshot(mds::DummyKey(), value, snapshot, timeout_us, alloc);
}

template <typename Key, typename Value>
int ObITabletMdsInterface::get_snapshot(const Key &key,
                                        Value &value,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us,
                                        ObIAllocator *alloc) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(snapshot), K(typeid(Value).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool is_data_complete = false;
  if (OB_FAIL(check_mds_data_complete_<Value>(is_data_complete))) {
    MDS_LOG(WARN, "failed to check data completion");
  } else if (!is_data_complete) {
    ret = OB_EAGAIN;
    MDS_LOG(INFO, "mds_data is not complete, try again later", K(ret), K(get_tablet_meta_().ha_status_));
  } else if (OB_ISNULL(get_tablet_pointer_())) {
    ret = OB_BAD_NULL_ERROR;
    MDS_LOG(ERROR, "pointer on tablet should not be null");
  } else {
    GetSnapshotOp<Key, Value> op(*this, key, value, snapshot, timeout_us, alloc);
    ret = retry_with_ls_switch_check_(get_tablet_pointer_()->get_ls(), op);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename T, typename OP, typename std::enable_if<OB_TRAIT_IS_FUNCTION_LIKE(OP, int(const T&)), bool>::type>
int ObITabletMdsInterface::get_snapshot(OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us) const
{
  T value;
  int ret = get_snapshot(value, snapshot, timeout_us, nullptr);
  if (OB_SUCC(ret)) {
    ret = read_op(value);
  }
  return ret;
}

template <typename Key, typename Value, typename OP>
int ObITabletMdsInterface::get_snapshot(const Key &key,
                                        OP &&read_op,
                                        const share::SCN snapshot,
                                        const int64_t timeout_us) const
{
  Value value;
  int ret = get_snapshot(key, value, snapshot, timeout_us, nullptr);
  if (OB_SUCC(ret)) {
    ret = read_op(value);
  }
  return ret;
}

template <typename T>
int ObITabletMdsInterface::obj_to_string_holder_(const T &obj, ObStringHolder &holder) const
{
  int ret = OB_SUCCESS;
  constexpr int64_t buffer_size = 1_KB;
  char stack_buffer[buffer_size] = { 0 };
  int64_t pos = 0;
  if (FALSE_IT(databuff_printf(stack_buffer, buffer_size, pos, obj))) {// try hard to fill buffer, it's ok if buffer not enough
  } else if (OB_FAIL(holder.assign(ObString(pos, stack_buffer)))) {
    MDS_LOG(WARN, "fail to assign to holder");
  }
  return ret;
}

template <typename K, typename T>
int ObITabletMdsInterface::fill_virtual_info_from_mds_table(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array), K(typeid(T).name())
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("vir_mds_ss_info");
  mds::MdsNodeInfoForVirtualTable *cur_virtual_info = nullptr;
  constexpr uint8_t mds_type_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, T>::value);
  const common::ObTabletID &tablet_id = get_tablet_meta_().tablet_id_;
  const share::SCN transfer_scn = get_reorganization_scn();
  const share::SCN read_snapshot = share::SCN::max_scn();
  ObLSHandle ls_handle;
  ObMdsTable *mds_table = nullptr;
  ObMdsTableScanIterator scan_iter;

  if (OB_FAIL(get_mds_table_for_write_(ls_handle, mds_table))) {
    MDS_LOG(WARN, "fail to get ls aggregate mds table", K(ret));
  } else if (OB_ISNULL(mds_table)) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "mds table is null", K(ret));
  } else if (MDS_FAIL((mds_table->table_scan<K, T>(
                 allocator, tablet_id, transfer_scn, read_snapshot, scan_iter)))) {
    MDS_LOG(WARN, "fail to open mds table scan for virtual info", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    ObMdsValueMeta meta;
    while (OB_SUCC(ret) && OB_SUCC(scan_iter.get_next_row(datum_row))) {
      K user_key_obj;
      T user_data_obj;
      if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG_GET(WARN, "datum row is null", K(ret), KP(datum_row));
      } else if (OB_FAIL(ObMdsTableDatumRowConverter::convert_row_to_kv(&allocator, *datum_row, user_key_obj, user_data_obj, meta))) {
        MDS_LOG_GET(WARN, "fail to convert datum row", K(ret));
      } else if (CLICK_FAIL(mds_node_info_array.push_back(mds::MdsNodeInfoForVirtualTable()))) {
        MDS_LOG_GET(WARN, "fail to push_back");
      } else {
        cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1);
        cur_virtual_info->ls_id_ = get_tablet_meta_().ls_id_;
        cur_virtual_info->tablet_id_ = get_tablet_meta_().tablet_id_;
        cur_virtual_info->position_ = mds::NodePosition::DISK;
        cur_virtual_info->unit_id_ = mds_type_id;
        if (CLICK_FAIL(obj_to_string_holder_(user_key_obj, cur_virtual_info->user_key_))) {
          MDS_LOG_GET(WARN, "fail to fill user key holder");
        } else if (CLICK_FAIL(obj_to_string_holder_(user_data_obj, cur_virtual_info->user_data_))) {
          MDS_LOG_GET(WARN, "fail to fill user data holder");
        } else {
          MDS_LOG_GET(TRACE, "success to fill virtual info");
        }
      }

      if (OB_FAIL(ret)) {
        mds_node_info_array.pop_back();
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      MDS_LOG(INFO, "tablet mds scan reach the end ", K(ret));
    } else {
      MDS_LOG(WARN, "fail to read raw data", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename T>
int ObITabletMdsInterface::fill_virtual_info_by_obj_(const T &obj,
                                                     const mds::NodePosition position,
                                                     ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array)
  MDS_TG(10_ms);
  mds::MdsNodeInfoForVirtualTable *cur_virtual_info = nullptr;
  int ret = OB_SUCCESS;
  if (!obj.is_valid()) {
    MDS_LOG_GET(INFO, "obj is not valid");
  } else if (CLICK_FAIL(mds_node_info_array.push_back(mds::MdsNodeInfoForVirtualTable()))) {
    MDS_LOG_GET(WARN, "fail to push_back");
  } else {
    cur_virtual_info = &mds_node_info_array.at(mds_node_info_array.count() - 1);
    if (CLICK_FAIL(obj_to_string_holder_(obj, cur_virtual_info->user_data_))) {
      MDS_LOG_GET(WARN, "fail to fill string holder");
    } else {
      cur_virtual_info->ls_id_ = get_tablet_meta_().ls_id_;
      cur_virtual_info->tablet_id_ = get_tablet_meta_().tablet_id_;
      cur_virtual_info->position_ = position;
      cur_virtual_info->unit_id_ = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, T>::value);
    }
    if (CLICK_FAIL(ret)) {
      mds_node_info_array.pop_back();
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

inline int ObITabletMdsInterface::fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(mds_node_info_array)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;

  if (CLICK_FAIL((fill_virtual_info_from_mds_table<mds::DummyKey, ObTabletCreateDeleteMdsUserData>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_status_");
  } else if (CLICK_FAIL((fill_virtual_info_from_mds_table<mds::DummyKey, share::ObTabletAutoincSeq>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill seq from disk");
  } else if (CLICK_FAIL((fill_virtual_info_from_mds_table<mds::DummyKey, ObTabletBindingMdsUserData>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill aux_tablet_info_");
  } else if (CLICK_FAIL((fill_virtual_info_from_mds_table<mds::DummyKey, ObTabletSplitMdsUserData>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill tablet_split_data_");
  } else if (CLICK_FAIL((fill_virtual_info_from_mds_table<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill medium compaction info");
  } else if (CLICK_FAIL((fill_virtual_info_from_mds_table<ObTruncateInfoKey, ObTruncateInfo>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill truncate info");
  } else if (CLICK_FAIL((fill_virtual_info_from_mds_table<ObTTLFilterInfoKey, ObTTLFilterInfo>(mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill ttl filter info");
  } else if (CLICK_FAIL(
      (fill_virtual_info_from_mds_table<ObTabletForkSrcMdsKey, ObTabletForkSrcMdsData>(
          mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill fork src info");
  } else if (CLICK_FAIL(
      (fill_virtual_info_from_mds_table<mds::DummyKey, ObTabletMdsRelocStatus>(
          mds_node_info_array)))) {
    MDS_LOG_GET(WARN, "fail to fill mds reloc done marker");
  } else {
    MDS_LOG_GET(TRACE, "sucess to fill virtual info");
  }

  return ret;
  #undef PRINT_WRAPPER
}

template<typename K, typename T>
int ObITabletMdsInterface::mds_range_query(
    const share::SCN &read_snapshot,
    ObMdsRangeQueryIterator<K, T> &iter) const
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  ObITabletMdsInterface *src = nullptr;
  ObTabletHandle src_tablet_handle;
  if (get_tablet_meta_().has_transfer_table()) {
    if (OB_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
      MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_handle_from_this(tablet_handle))) {
    MDS_LOG(WARN, "fail to get tablet_handle", K(ret));
  } else if (OB_FAIL(iter.init(read_snapshot, tablet_handle, src_tablet_handle))) {
    MDS_LOG(WARN, "fail to init range scan iter", K(ret), K(read_snapshot));
  }

  return ret;
}

template <typename Key, typename Value>
int ObITabletMdsInterface::get_mds_snapshot_data(
    const Key &key,
    Value &value,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    ObIAllocator *alloc) const
{
  int ret = OB_SUCCESS;
  ret = get_snapshot_data_consider_transfer_(key, value, snapshot, timeout_us, alloc);
  return ret;
}

template<typename T, typename = void>
struct ObMdsUserDataHasIsValid : std::false_type {};

template<typename T>
struct ObMdsUserDataHasIsValid<T, decltype(std::declval<const T &>().is_valid(), void())>
    : std::true_type {};

template <typename T>
int ObITabletMdsInterface::get_latest_data_consider_transfer_(
    T &value,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    ObIAllocator *alloc,
    const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(value)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(cross_ls_get_latest(
        src,
        value,
        alloc,
        writer,
        trans_stat,
        trans_version,
        read_seq))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    } else if (ObMdsUserDataHasIsValid<T>::value && !value.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "invalid user data", K(lbt()));
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

template <typename T>
int ObITabletMdsInterface::get_latest_committed_data_consider_transfer_(T &value, ObIAllocator *alloc) const
{
  #define PRINT_WRAPPER KR(ret), K(value)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(cross_ls_get_latest_committed(
        src,
        value,
        alloc))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    } else if (ObMdsUserDataHasIsValid<T>::value && !value.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG_GET(WARN, "invalid user data", K(lbt()));
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

template <typename Key, typename Value>
int ObITabletMdsInterface::get_snapshot_data_consider_transfer_(
    const Key &key,
    Value &value,
    const share::SCN &snapshot,
    const int64_t timeout_us,
    ObIAllocator *alloc) const
{
  #define PRINT_WRAPPER KR(ret), K(value), K(snapshot), K(timeout_us)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(cross_ls_get_snapshot(
        src, key, value, snapshot, timeout_us, alloc))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(lbt()));
      }
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}


// Shared "try local; if OB_EMPTY_RESULT and another is non-null, retry against
// another" pattern for cross_ls_get_latest / cross_ls_get_latest_committed /
// cross_ls_get_snapshot. ReadFn is a const member-function pointer of
// ObITabletMdsInterface; @p args... are forwarded to both invocations.
template <typename ReadFn, typename... Args>
inline int try_cross_ls_read_(const ObITabletMdsInterface *self,
                              const ObITabletMdsInterface *another,
                              ReadFn read_fn,
                              Args&... args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((self->*read_fn)(args...))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to read mds from local tablet", K(ret));
    }
  }
  if (OB_EMPTY_RESULT == ret && nullptr != another) {
    if (OB_FAIL((another->*read_fn)(args...))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG(WARN, "fail to read mds from src tablet", K(ret));
      }
    }
  }
  return ret;
}

template <typename T>
int ObITabletMdsInterface::cross_ls_get_latest(
    const ObITabletMdsInterface *another,
    T &value,
    common::ObIAllocator *alloc,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    const int64_t read_seq) const
{
  using Fn = int (ObITabletMdsInterface::*)(T &,
                                            mds::MdsWriter &,
                                            mds::TwoPhaseCommitState &,
                                            share::SCN &,
                                            ObIAllocator *,
                                            const int64_t) const;
  Fn fn = &ObITabletMdsInterface::get_latest<T>;
  return try_cross_ls_read_(this, another, fn, value, writer, trans_stat, trans_version, alloc, read_seq);
}

template <typename T>
int ObITabletMdsInterface::cross_ls_get_latest_committed(
    const ObITabletMdsInterface *another,
    T &value,
    ObIAllocator *alloc) const
{
  using Fn = int (ObITabletMdsInterface::*)(T &, ObIAllocator *) const;
  Fn fn = &ObITabletMdsInterface::get_latest_committed<T>;
  return try_cross_ls_read_(this, another, fn, value, alloc);
}

template <typename Key, typename Value>
int ObITabletMdsInterface::cross_ls_get_snapshot(
    const ObITabletMdsInterface *another,
    const Key &key,
    Value &value,
    const share::SCN snapshot,
    const int64_t timeout_us,
    common::ObIAllocator *alloc) const
{
  using Fn = int (ObITabletMdsInterface::*)(const Key &,
                                            Value &,
                                            const share::SCN,
                                            const int64_t,
                                            ObIAllocator *) const;
  Fn fn = &ObITabletMdsInterface::get_snapshot<Key, Value>;
  return try_cross_ls_read_(this, another, fn, key, value, snapshot, timeout_us, alloc);
}

template <class T, typename std::enable_if<!OB_TRAIT_IS_SAME_CLASS(T, ObTabletCreateDeleteMdsUserData), bool>::type>
int ObITabletMdsInterface::check_mds_data_complete_(bool &is_complete) const 
{
  int ret = OB_SUCCESS;
  // For multi-source data (excluding tablet_status), during the migration of LS, 
  // reading is only permitted once the data is complete; 
  // otherwise, the data read will be incomplete.
  is_complete = get_tablet_meta_().ha_status_.is_data_status_complete();
  return ret;
}

} // namespace storage
} // namespace oceanbase
