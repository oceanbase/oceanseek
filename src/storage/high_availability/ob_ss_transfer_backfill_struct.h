/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_STRUCT_
#define OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_STRUCT_

#include "storage/incremental/ob_inc_ss_define.h"
#include "share/scn.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/high_availability/ob_transfer_backfill_tx.h"

namespace oceanbase {
namespace storage
{

class ObSSTransferBackfillType final
{
public:
  enum TYPE
  {
    BACKFILL_UPLOAD = 0,
    BACKFILL_TX = 1,
    BACKFILL_REPLACE_TABLE = 2,
    BACKFILL_REFRESH = 3,
    BACKFILL_MAX
  };
public:
  ObSSTransferBackfillType();
  ~ObSSTransferBackfillType() = default;
  explicit ObSSTransferBackfillType(const TYPE &type);
  operator TYPE() const;
  ObSSTransferBackfillType &operator=(const TYPE &backfill_type);
  bool operator==(const TYPE &backfill_type);
  bool operator!=(const TYPE &backfill_type);

  uint64_t hash() const;
  void reset();
  bool is_valid() const;
  static const char *get_str(const ObSSTransferBackfillType &type);

  TO_STRING_KV("val", static_cast<uint8_t>(type_),
               "str", get_str(*this));
private:
  TYPE type_;
  static constexpr const char *type_str_array_[] = {
    "BACKFILL_UPLOAD",
    "BACKFILL_TX",
    "BACKFILL_REPLACE_TABLE",
    "BACKFILL_REFRESH",
  };
};

struct ObSSTransferBackfillTXParam: public share::ObIDagInitParam
{
public:
  ObSSTransferBackfillTXParam();
virtual ~ObSSTransferBackfillTXParam() {}
  virtual bool is_valid() const override;
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(src_ls_id), K_(dest_ls_id), K_(tablet_infos), K_(backfill_type), K_(dag_net_id));
public:
  uint64_t tenant_id_;
  share::ObTaskId task_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  common::ObArray<ObTabletBackfillInfo> tablet_infos_;
  ObSSTransferBackfillType backfill_type_;
  share::ObTaskId dag_net_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferBackfillTXParam);
};

struct ObBackfillScnCompare {
public:
  explicit ObBackfillScnCompare(int &sort_ret)
    : result_code_(sort_ret) {}
  bool operator()(const ObTabletBackfillInfo &linfo, const ObTabletBackfillInfo &rinfo) const;
  int &result_code_;
private:
  int compare_by_backfill_scn_(
      const ObTabletBackfillInfo &linfo, const ObTabletBackfillInfo &rinfo, bool &bret) const;
};

struct ObSSTransferBackfillTXCtx: public ObTransferBackfillTXBaseCtx
{
public:
  ObSSTransferBackfillTXCtx();
  virtual ~ObSSTransferBackfillTXCtx();
  void reuse();
  void reset();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::SS_TRANSFER_BACKFILL_TX; }
  virtual bool is_valid() const override;
  virtual int init_ls_handle() override;
  virtual int get_src_ls(ObLS *&src_ls) override;
  virtual int get_dest_ls(ObLS *&dest_ls) override;
  virtual int get_src_ls_handle(ObLSHandle &src_ls_handle) override;
  virtual int get_dest_ls_handle(ObLSHandle &dest_ls_handle) override;
  share::ObLSID get_ls_id() const;
  int get_ls(ObLS *&ls);  
  INHERIT_TO_STRING_KV(
      "ObTransferBackfillTXBaseCtx", ObTransferBackfillTXBaseCtx,
      K_(backfill_type),
      K_(start_time),
      K_(finish_time),
      K_(dag_net_id),
      K_(replace_scn_range),
      K_(replace_ss_clog_ckpt_scn));
public:
  ObSSTransferBackfillType backfill_type_;
  int64_t start_time_;
  int64_t finish_time_;
  share::ObTaskId dag_net_id_;
  share::ObScnRange replace_scn_range_;
  share::SCN replace_ss_clog_ckpt_scn_;
private:
  bool allow_to_get_src_ls() const;
  bool allow_to_get_dest_ls() const;
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferBackfillTXCtx);
};


} //storage
} //oceanbase
#endif
