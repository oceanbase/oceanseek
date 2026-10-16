/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INC_UPLOAD_DAG_NET_H_
#define OCEANBASE_STORAGE_INC_UPLOAD_DAG_NET_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"

namespace oceanbase {
namespace storage {

enum class ObIncUploadType : char
{
  MDS = 0,
  DDL = 1,
  MINOR = 2,
};

static ObIncUploadType get_upload_type_from_table_key(const ObITable::TableKey &table_key)
{
  if (table_key.is_ddl_dump_sstable() || table_key.is_inc_major_ddl_dump_sstable()) { return ObIncUploadType::DDL; }
  if (table_key.is_minor_sstable()) { return ObIncUploadType::MINOR; }
  return ObIncUploadType::MINOR;
}

struct ObIncUploadDagNetParam : public share::ObIDagInitParam
{
  ObIncUploadDagNetParam()
      : ls_id_(),
        tablet_id_(),
        transfer_scn_(),
        upload_type_(ObIncUploadType::MINOR),
        is_replace_(false),
        inc_upload_param_() {}

  bool is_valid() const override;

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  ObIncUploadType upload_type_;
  bool is_replace_;
  ObIncSSTableUploadParam inc_upload_param_;

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(transfer_scn), K_(upload_type), K_(is_replace));
};

struct ObIncUploadDagNetCtx : public ObIHADagNetCtx
{
  ObIncUploadDagNetCtx();
  virtual ~ObIncUploadDagNetCtx() = default;

  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() override { return ObIHADagNetCtx::INC_UPLOAD; }
  virtual bool is_valid() const override;

  bool is_replace_upload() const { return is_replace_; }

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  share::SCN transfer_scn_;
  ObIncUploadType upload_type_;
  bool is_replace_;
  ObIncSSTableUploadParam inc_upload_param_;
  // Replace-upload diagnostic info, written as early as each value becomes known
  // so the terminal finish dag can record them even on partial failure.
  share::ObScnRange replace_scn_range_;
  share::SCN replace_ss_clog_ckpt_scn_;

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(transfer_scn), K_(upload_type), K_(is_replace),
               K_(replace_scn_range), K_(replace_ss_clog_ckpt_scn));
};

class ObIncUploadDagNet : public share::ObIDagNet
{
public:
  ObIncUploadDagNet();
  virtual ~ObIncUploadDagNet() = default;

  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;

  virtual bool is_ha_dag_net() const override { return false; }
  virtual storage::ObIHADagNetCtx *get_dag_net_ctx() override { return &ctx_; }

  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));

private:
  int start_running_for_replace_upload_();
  int start_running_for_inc_upload_();

  bool is_inited_;
  ObIncUploadDagNetCtx ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObIncUploadDagNet);
};

}  // namespace storage
}  // namespace oceanbase

#endif
