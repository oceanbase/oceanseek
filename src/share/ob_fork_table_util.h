/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_OB_FORK_TABLE_UTIL_H_
#define OCEANBASE_SHARE_OB_FORK_TABLE_UTIL_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{

// RS 表级 fork 配对元信息:src/dest 逻辑表 id + fork 快照版本。
// 一条记录描述一张参与 fork 的表(src<->dest),供 ObForkTableInfoBuilder 按 dest 展开 per-tablet 信息。
class ObForkTableInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObForkTableInfo()
    : fork_src_table_id_(OB_INVALID_ID), fork_dest_table_id_(OB_INVALID_ID), fork_snapshot_version_(0)
  {}
  ~ObForkTableInfo() = default;
  void reset()
  {
    fork_src_table_id_ = OB_INVALID_ID;
    fork_dest_table_id_ = OB_INVALID_ID;
    fork_snapshot_version_ = 0;
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != fork_src_table_id_
           && OB_INVALID_ID != fork_dest_table_id_
           && fork_snapshot_version_ > 0;
  }
  uint64_t get_fork_src_table_id() const { return fork_src_table_id_; }
  void set_fork_src_table_id(const uint64_t table_id) { fork_src_table_id_ = table_id; }
  uint64_t get_fork_dest_table_id() const { return fork_dest_table_id_; }
  void set_fork_dest_table_id(const uint64_t table_id) { fork_dest_table_id_ = table_id; }
  int64_t get_fork_snapshot_version() const { return fork_snapshot_version_; }
  void set_fork_snapshot_version(const int64_t version) { fork_snapshot_version_ = version; }
  TO_STRING_KV(K_(fork_src_table_id), K_(fork_dest_table_id), K_(fork_snapshot_version));
private:
  uint64_t fork_src_table_id_;
  uint64_t fork_dest_table_id_;
  int64_t fork_snapshot_version_;
};

// dst tablet_meta_.fork_info_ / 读路径 / DAG 守门专用。
// 不存 task_id、dst_tablet_id、ls 等(消费者从自身上下文取)。
class ObForkTabletInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObForkTabletInfo()
    : fork_snapshot_version_(0), src_tablet_id_(), data_complete_(false), all_replicas_land_complete_(false)
  {}
  ~ObForkTabletInfo() = default;

  void reset()
  {
    fork_snapshot_version_ = 0;
    src_tablet_id_.reset();
    data_complete_ = false;
    all_replicas_land_complete_ = false;
  }
  bool is_valid() const
  {
    return fork_snapshot_version_ > 0
           && src_tablet_id_.is_valid();
  }
  bool is_active() const { return is_valid() && !is_all_replicas_land_complete(); }
  int assign(const ObForkTabletInfo &other)
  {
    fork_snapshot_version_ = other.fork_snapshot_version_;
    src_tablet_id_ = other.src_tablet_id_;
    data_complete_ = other.data_complete_;
    all_replicas_land_complete_ = other.all_replicas_land_complete_;
    return common::OB_SUCCESS;
  }
  // 身份字段(fork_snapshot_version/src_tablet_id):RS 侧 build 时经 setter 写入,落 meta 后只读,读走 getter。
  int64_t get_fork_snapshot_version() const { return fork_snapshot_version_; }
  void set_fork_snapshot_version(const int64_t version) { fork_snapshot_version_ = version; }
  share::SCN get_fork_snapshot_scn() const
  {
    share::SCN scn;
    (void)scn.convert_for_tx(fork_snapshot_version_);  // version 恒为正常 GTS,convert 不会失败
    return scn;
  }
  const common::ObTabletID &get_src_tablet_id() const { return src_tablet_id_; }
  void set_src_tablet_id(const common::ObTabletID &tablet_id) { src_tablet_id_ = tablet_id; }
  // 补数据完成标记的语义随所在 meta 解释：SS/shared meta 上表示 FORK_PUBLISH 已完成；
  // 本地 meta 上表示本副本已完成 FORK_LOCAL_LAND，读链路仅在 fork info 有效
  // 且该标记为 false 时执行 overlay。
  // 本地落盘替换在表存储与元数据的同一次 CAS 中设置本地完成位；
  // V2 共享发布必须先等待 LS_MDS_TABLET checkpoint 覆盖 final reloc redo；
  // HA 按同一快照导入内表和用户 tablet，继承完成位时也具备本地 MDS 持久化条件。
  void set_data_complete(const bool complete) { data_complete_ = complete; }
  bool is_data_complete() const { return data_complete_; }
  void set_all_replicas_land_complete(const bool complete) { all_replicas_land_complete_ = complete; }
  bool is_all_replicas_land_complete() const { return all_replicas_land_complete_; }

  TO_STRING_KV(K_(fork_snapshot_version), K_(src_tablet_id), K_(data_complete), K_(all_replicas_land_complete));
private:
  // 身份字段 RS 侧 build 时经 set_fork_snapshot_version / set_src_tablet_id 写入,落 meta 后只读;
  int64_t fork_snapshot_version_;
  common::ObTabletID src_tablet_id_;
  bool data_complete_;
  // shared tablet meta 为权威值；true 表示稳定数据副本均已完成 local land，FINISH 前同步到各 local meta。
  bool all_replicas_land_complete_;
};

// fork prepare 阶段每张 src/dst 表只保存一份的构建信息。
struct ObForkTableBuildInfo
{
  ObForkTableBuildInfo()
    : fork_table_info_(),
      src_schema_version_(OB_INVALID_VERSION),
      dst_schema_version_(OB_INVALID_VERSION)
  {}

  // dst schema version 在 create_table 后回填，因此完整有效性仅在 resolve 阶段后成立。
  bool is_valid() const
  {
    return fork_table_info_.is_valid()
        && src_schema_version_ > 0
        && dst_schema_version_ > 0;
  }
  TO_STRING_KV(K_(fork_table_info), K_(src_schema_version), K_(dst_schema_version));

  ObForkTableInfo fork_table_info_;
  int64_t src_schema_version_;
  int64_t dst_schema_version_;
};

// fork prepare 阶段每个 src/dst tablet 的配对信息，通过下标关联所属表级信息。
struct ObForkTabletBuildInfo
{
  ObForkTabletBuildInfo()
    : src_tablet_id_(),
      dst_tablet_id_(),
      table_info_idx_(-1)
  {}

  bool is_valid() const
  {
    return src_tablet_id_.is_valid()
        && dst_tablet_id_.is_valid()
        && table_info_idx_ >= 0;
  }
  TO_STRING_KV(K_(src_tablet_id), K_(dst_tablet_id), K_(table_info_idx));

  common::ObTabletID src_tablet_id_;
  common::ObTabletID dst_tablet_id_;
  int64_t table_info_idx_;
};

// fork_table_ prepare 阶段产出，供 create tablet / MDS / reorg 等后续步骤消费。
struct ObForkTablePrepareResult
{
  ObForkTablePrepareResult()
    : table_build_infos_(),
      tablet_build_infos_(),
      data_format_version_(0)
  {}

  void reset()
  {
    table_build_infos_.reset();
    tablet_build_infos_.reset();
    data_format_version_ = 0;
  }

  common::ObArray<ObForkTableBuildInfo> table_build_infos_;
  common::ObArray<ObForkTabletBuildInfo> tablet_build_infos_;
  uint64_t data_format_version_;
};

class ObForkTableUtil final
{
public:
  // 收集一张表 fork 参与的表 id：数据表 + 普通 row-store index + 完整 domain/aux 索引组 + lob meta/piece。
  // domain/aux 索引（FTS/multivalue/vector）必须收齐完整组才纳入，残缺组跳过；与 create-like 同一套算法保持一致。
  static int collect_table_ids_from_table(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const share::schema::ObTableSchema &table_schema,
      common::ObIArray<uint64_t> &table_ids);

  // 把表上 FTS/multivalue/vector 等 domain/aux 索引组装成完整索引组，只有完整组进入 map。
  static int collect_complete_domain_index_schemas(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const uint64_t tenant_id,
      const share::schema::ObTableSchema &table_schema,
      common::hash::ObHashMap<uint64_t, share::schema::ObTableSchema> &complete_index_schema_map);

  // 判断 index 是否属于 domain/aux 索引（FTS/multivalue/vector 的共享、domain、aux 子表）。
  static bool is_domain_or_aux_index(const share::schema::ObTableSchema &index_schema);

  // 在 fork_table_infos 中按 dest table id 查找对应 src table id。
  static int find_fork_src_table_id(
      const common::ObIArray<ObForkTableInfo> &fork_table_infos,
      const uint64_t fork_dest_table_id,
      uint64_t &fork_src_table_id);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_FORK_TABLE_UTIL_H_
