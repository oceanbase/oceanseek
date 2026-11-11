/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SSTABLE_ATOMIC_PROTOCOL_DEFINE_
#define OCEANBASE_SSTABLE_ATOMIC_PROTOCOL_DEFINE_


#include "ob_atomic_protocol_define.h"
#include "ob_atomic_protocol.h"
#include "ob_lock_atomic_protocol.h"


namespace oceanbase
{
namespace storage
{

class ObSSTableAtomicProtocol : public ObAtomicProtocol
{
public:
  ObSSTableAtomicProtocol();
  virtual ~ObSSTableAtomicProtocol();
  int init(
      const ObAtomicProtocolType &type,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorg_scn,
      const ObSSMetaCtrlObjectInfo &object_info);
  int init_for_load(
      const ObAtomicProtocolType &type,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorg_scn);

  virtual int create(
      const bool need_check_sswriter);
  virtual int finish(ObIAtomicProtocolOP &op);
  virtual int abort();
  int finish();
  int get_ctrl_key_info(ObSSMetaCtrlKeyInfo &key_info);
  // 查找 tablet 上是否已存在 snapshot_version 的 major 任务, 如果存在则用表中信息重构本对象
  // 如果不存在则置 need_create=true，外部调用决定是否create/finish
  int load_if_exist(const int64_t snapshot_version, bool &need_create);
  bool is_inited() const { return is_inited_; }
  bool is_non_idem_major() const
  {
    return ObAtomicProtocolTypeEnum::NON_IDEM_MAJOR_SSTABLE == type_.type_;
  }

  INHERIT_TO_STRING_KV("ObAtomicProtocol", ObAtomicProtocol,
      K_(is_inited), K_(tenant_id), K_(ls_id), K_(tablet_id), K_(reorg_scn), K_(object_info));

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
  virtual int check_before_create_under_lock_(
      common::ObISQLClient &sql_client,
      bool &need_create);
  // Lock the owning tablet's TABLET_LOCK_ROW first (before this sstable's own
  // lock row) and reject if the tablet is already mark-deleted. Locking the
  // tablet before the sstable keeps the global lock order tablet -> sstable.
  virtual int lock_parent_lock_row_(ObMySQLTransaction &trans);
private:
  // Internal helper struct that aggregates the running "latest major row" we've
  // seen during the ctrl-table scan. Avoids 7+ parameter helper signatures.
  struct LatestMajorRowState_
  {
    LatestMajorRowState_() : has(false), key_info(), value() {}
    void reset() { has = false; key_info.reset(); value.reset(); }
    bool has;
    ObSSMetaCtrlKeyInfo key_info;
    ObSSMetaCtrlValue value;
  };

  // query_latest_major_row_ 对目标 snapshot_version 的查询结论
  enum class MajorRowStatus_
  {
    NOT_EXIST = 0,       // 不存在 snapshot >= 目标值的 major 行
    SAME_SNAPSHOT = 1,   // 存在同 snapshot 的 major 行（输出其中 meta_scn 最大的一行）
    NEWER_SNAPSHOT = 2,  // 存在更大 snapshot 的 major 行，目标 snapshot 已过期
  };

  int query_latest_major_row_(
      common::ObISQLClient &sql_client,
      const int64_t snapshot_version,
      MajorRowStatus_ &row_status,
      ObSSMetaCtrlKeyInfo &ctrl_key_info,
      ObSSMetaCtrlValue &ctrl_value);
  int build_query_prefix_(
      common::ObArenaAllocator &allocator,
      ObString &key_prefix);
  // 单行处理：跳过非 major 行；同 snapshot 的行在 state 中追踪 meta_scn 最大者，
  // 发现更大 snapshot 的行时置 found_newer=true。
  int process_latest_major_row_(
      const ObSSMetaCtrlKeyInfo &row_key_info,
      const ObSSMetaCtrlValue &row_value,
      const int64_t snapshot_version,
      LatestMajorRowState_ &state,
      bool &found_newer);
  int iterate_for_latest_major_row_(
      common::ObISQLClient &sql_client,
      const ObString &key_prefix,
      const int64_t snapshot_version,
      LatestMajorRowState_ &state,
      bool &found_newer);
  int load_from_row_(
      const ObSSMetaCtrlKeyInfo &key_info,
      const ObSSMetaCtrlValue &ctrl_value);
  // Lock the owning tablet's TABLET_LOCK_ROW (SELECT ... FOR UPDATE) inside the
  // given transaction and return its value. Used by lock_parent_lock_row_ to
  // read the tablet's deletion state while serialized against the tablet delete.
  int lock_tablet_lock_row_(
      ObMySQLTransaction &trans,
      ObSSMetaCtrlLockRowValue &tablet_lock_row_value);
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorg_scn_;
  ObSSMetaCtrlObjectInfo object_info_;
  DISALLOW_COPY_AND_ASSIGN(ObSSTableAtomicProtocol);
};


} //storage
} //oceanbase


#endif /* OCEANBASE_SSTABLE_ATOMIC_PROTOCOL_DEFINE_ */
