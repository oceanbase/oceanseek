/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_FORK_MDS_HELPER_H_
#define OCEANBASE_STORAGE_OB_TABLET_FORK_MDS_HELPER_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_fork_table_util.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_status.h"

namespace oceanbase
{
namespace transaction
{
enum class ObTxDataSourceType : int64_t;
}

namespace storage
{
class ObLS;
class ObTablet;
namespace mds
{
struct BufferCtx;
}
class ObTabletForkDestMdsArg final
{
public:
  ObTabletForkDestMdsArg();
  ~ObTabletForkDestMdsArg() = default;

  int set_autoinc_seq_arg(const obrpc::ObBatchSetTabletAutoincSeqArg &arg);

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(autoinc_seq_arg), K_(data_version));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  obrpc::ObBatchSetTabletAutoincSeqArg autoinc_seq_arg_;
  uint64_t data_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletForkDestMdsArg);
};

class ObTabletForkStatusMdsArg final
{
  OB_UNIS_VERSION_V(1);
public:
  ObTabletForkStatusMdsArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      ls_id_(),
      tablet_ids_(),
      fork_dst_tablet_ids_(),
      tablet_status_(ObTabletStatus::NONE),
      data_type_(ObTabletMdsUserDataType::NONE),
      fork_snapshot_version_(0)
  {}
  ~ObTabletForkStatusMdsArg() = default;

  bool is_valid() const;
  void reset();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      const common::ObIArray<common::ObTabletID> &fork_dst_tablet_ids,
      const ObTabletStatus tablet_status,
      const ObTabletMdsUserDataType data_type);
  int set_fork_src_mds_data(const int64_t fork_snapshot_version);
  bool need_write_src_fork_mds() const
  {
    return ObTabletMdsUserDataType::START_FORK_SRC == data_type_
        || ObTabletMdsUserDataType::FINISH_FORK_SRC == data_type_
        || ObTabletMdsUserDataType::ABORT_FORK_SRC == data_type_;
  }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_ids), K_(fork_dst_tablet_ids),
               K_(tablet_status), K_(data_type), K_(fork_snapshot_version));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> tablet_ids_;
  common::ObSArray<common::ObTabletID> fork_dst_tablet_ids_;
  ObTabletStatus tablet_status_;
  ObTabletMdsUserDataType data_type_;
  // src fork MDS 的逻辑 MVCC 点位，仅 START_FORK_SRC 有效；FINISH/ABORT 写无效值。
  int64_t fork_snapshot_version_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletForkStatusMdsArg);
};

// fork status MDS 共用实现：src 保持 NORMAL，active 状态由 fork-src MDS 表示；
// dst 完成时从 FORK_DST 迁移到 NORMAL；PREPARE 补偿由后续标准删除 MDS 清理。
class ObTabletForkStatusMdsHelper final
{
public:
  static int register_mds(
      const ObTabletForkStatusMdsArg &arg,
      common::ObMySQLTransaction &trans,
      const transaction::ObTxDataSourceType type);
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
private:
  static int modify(const ObTabletForkStatusMdsArg &arg, const share::SCN &scn, mds::BufferCtx &ctx);
  static int set_tablet_status(
      ObLS &ls,
      const common::ObTabletID &tablet_id,
      const ObTabletStatus tablet_status,
      const ObTabletMdsUserDataType data_type,
      const share::SCN &replay_scn,
      mds::BufferCtx &ctx);
  static int check_fork_dst_finish_ready_(ObTablet &tablet);
  static int write_src_fork_mds(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const common::ObTabletID &fork_dst_tablet_id,
      const ObTabletMdsUserDataType data_type,
      const int64_t fork_snapshot_version,
      const share::SCN &replay_scn,
      mds::BufferCtx &ctx);
};

class ObTabletForkSrcMdsHelper final
{
public:
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
};

class ObTabletForkDestMdsHelper final
{
public:
  // payload 先记录 data version，再序列化对应版本携带的数据。
  static int register_autoinc_mds(
      const obrpc::ObBatchSetTabletAutoincSeqArg &autoinc_arg,
      const uint64_t data_version,
      common::ObMySQLTransaction &trans);
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
private:
  static int register_fork_dest_mds_(const ObTabletForkDestMdsArg &arg, common::ObMySQLTransaction &trans);
  // on_register/on_replay 共用：按 data version 反序列化并处理 fork dst MDS。
  static int process_fork_dest_mds_(
      const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
  // 把已反序列化的 dst autoinc_seq 写到 dst tablet。
  // replay 时 tablet 未提交可见返 EAGAIN，不 skip。
  static int write_dst_autoinc_seq_(
      const obrpc::ObBatchSetTabletAutoincSeqArg &autoinc_arg,
      const share::SCN &scn,
      mds::BufferCtx &ctx);
};

class ObTabletForkFinishMdsHelper final
{
public:
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_FORK_MDS_HELPER_H_
