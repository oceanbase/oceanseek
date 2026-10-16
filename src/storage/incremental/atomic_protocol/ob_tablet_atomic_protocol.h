/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TABLET_ATOMIC_PROTOCOL_DEFINE_
#define OCEANBASE_TABLET_ATOMIC_PROTOCOL_DEFINE_


#include "ob_atomic_protocol_define.h"
#include "ob_atomic_protocol.h"
#include "ob_lock_atomic_protocol.h"


namespace oceanbase
{
namespace storage
{

// Parameter object for ObTabletAtomicProtocol::init().
//
// All fields except `key_infos_` MUST be set by the caller; `is_valid()`
// rejects any combination that ObTabletAtomicProtocol cannot operate on.
struct ObTabletAtomicProtocolParam final
{
  ObTabletAtomicProtocolParam();
  ~ObTabletAtomicProtocolParam() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObTabletAtomicProtocolParam &param);
  // tenant id the tablet belongs to.
  uint64_t tenant_id_;
  // log stream id the tablet belongs to.
  share::ObLSID ls_id_;
  // the target tablet id this protocol acts on.
  common::ObTabletID tablet_id_;
  // reorganization scn of the tablet, used to distinguish different
  // incarnations of the same tablet id after reorganization.
  share::SCN reorg_scn_;
  // ctrl-key bookkeeping info of the sstables that compose this tablet at
  // this protocol round. May be empty (e.g. when the tablet currently has no
  // sstable to track), in which case no sstable-level ctrl key will be
  // touched by this protocol.
  ObArray<ObSSMetaCtrlKeyInfo> key_infos_;
  // set to true if the tablet is being deleted; the deleted_scn_ below will
  // then be persisted into the lock row alongside the is_mark_deleted_ flag.
  bool is_deleted_;
  // deleted scn  and last major snapshot scn carried into the lock row when is_deleted_ is true.
  // Required to be valid if is_deleted_ is true.
  share::SCN deleted_scn_;
  int64_t last_major_snapshot_;
  // TODO(muwei.ym): using tablet_meta_.transfer_info_.transfer_seq_
  int64_t transfer_seq_;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn),
    K_(key_infos), K_(is_deleted), K_(deleted_scn), K_(last_major_snapshot), K_(transfer_seq));
  DISALLOW_COPY_AND_ASSIGN(ObTabletAtomicProtocolParam);
};

class ObTabletAtomicProtocol : public ObAtomicProtocol
{
public:
  ObTabletAtomicProtocol();
  virtual ~ObTabletAtomicProtocol();
  // Initialize the tablet atomic protocol instance from `param`. The
  // protocol type is hardcoded to ObAtomicProtocolTypeEnum::TABLET_META.
  // Returns OB_SUCCESS on success, otherwise the corresponding error code.
  int init(
      const ObTabletAtomicProtocolParam &param);

  virtual int create(
      const bool need_check_sswriter);
  virtual int finish(ObIAtomicProtocolOP &op);
  virtual int abort();
  // Abort only the tablet atomic operation itself. The referenced SSTable
  // control rows are kept intact so that an outer retry can reuse them.
  int abort_self();

  INHERIT_TO_STRING_KV("ObAtomicProtocol", ObAtomicProtocol,
      K_(is_inited), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn));

protected:
  virtual int prepare_ss_meta_ctrl_key_(
      ObSSMetaCtrlKey &ctrl_key);
  virtual int prepare_atomic_ctrl_info_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObSSMetaCtrlValue &ctrl_value);
  virtual int update_lock_row_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObMySQLTransaction &trans);
  virtual int prepare_lock_row_(
      ObLockAtomicProtocol &lock_row);
private:
  // Composite op used by `finish()` so that the base `finish_()` runs both the
  // caller-supplied op and tablet-private `delete_key_infos_` inside the same
  // transaction. This lets ObTabletAtomicProtocol::finish() delegate fully to
  // the base implementation instead of duplicating its trans flow (and the
  // accompanying mark-lock-row-deleted bookkeeping).
  class DeleteKeyInfosOP final : public ObIAtomicProtocolOP
  {
  public:
    DeleteKeyInfosOP(ObTabletAtomicProtocol &owner, ObIAtomicProtocolOP &user_op)
      : owner_(owner), user_op_(user_op) {}
    virtual ~DeleteKeyInfosOP() {}
    virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override;
  private:
    ObTabletAtomicProtocol &owner_;
    ObIAtomicProtocolOP &user_op_;
    DISALLOW_COPY_AND_ASSIGN(DeleteKeyInfosOP);
  };
  // Composite op used by `abort()` so that the base `abort_()` runs the
  // tablet-private `abandon_key_infos_` inside the same transaction as the
  // tablet ctrl row abandon. This is the abort-time counterpart of
  // DeleteKeyInfosOP, letting ObTabletAtomicProtocol::abort() delegate fully to
  // the base implementation instead of duplicating its trans flow.
  class AbandonKeyInfosOP final : public ObIAtomicProtocolOP
  {
  public:
    explicit AbandonKeyInfosOP(ObTabletAtomicProtocol &owner)
      : owner_(owner) {}
    virtual ~AbandonKeyInfosOP() {}
    virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override;
  private:
    ObTabletAtomicProtocol &owner_;
    DISALLOW_COPY_AND_ASSIGN(AbandonKeyInfosOP);
  };
  // How handle_key_infos_ resolves each sstable ctrl row tracked by key_infos_.
  enum class KeyInfoAction
  {
    // finish(): the sstable ctrl row must be COMPLETE, then it is deleted.
    DELETE_ROW = 0,
    // abort(): the sstable ctrl row (START/COMPLETE, or already ABANDON) is
    // marked ABANDON so a later GC can reclaim it.
    ABANDON_ROW = 1,
  };
  int delete_key_infos_(ObMySQLTransaction &trans);
  int abandon_key_infos_(ObMySQLTransaction &trans);
  // Shared driver behind delete_key_infos_/abandon_key_infos_: iterates
  // key_infos_, locks each distinct sstable lock-row type once (tablet -> sstable
  // order), then applies `action` to that sstable's ctrl row.
  int handle_key_infos_(const KeyInfoAction action, ObMySQLTransaction &trans);
  // Lock the sstable lock row for `key_info` exactly once per lock type, tracking
  // the already-locked types in `lock_set`.
  int lock_key_info_lock_row_(
      const ObSSMetaCtrlKeyInfo &key_info,
      hash::ObHashSet<ObSSMetaCtrlType> &lock_set,
      ObMySQLTransaction &trans);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorg_scn_;
  ObArray<ObSSMetaCtrlKeyInfo> key_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletAtomicProtocol);
};


} //storage
} //oceanbase


#endif /* OCEANBASE_TABLET_ATOMIC_PROTOCOL_DEFINE_ */
