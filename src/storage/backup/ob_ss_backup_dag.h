/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BACKUP_SS_BACKUP_DAG_H_
#define OCEANBASE_STORAGE_BACKUP_SS_BACKUP_DAG_H_

#include "storage/backup/ob_ss_backup_ctx.h"

namespace oceanbase
{
namespace backup
{

// TODO(yanfeng): add SS prefix to all dag and task
class ObSSBackupDagNet : public share::ObIDagNet
{
public:
  ObSSBackupDagNet();
  virtual ~ObSSBackupDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;

  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(backup_ctx));

public:
  ObSSBackupCtx *get_backup_ctx() { return backup_ctx_; }

private:
  int start_running_for_backup_meta_();
  int start_running_for_backup_data_();
  int alloc_ctx_();
  void free_ctx_();

private:
  bool is_inited_;
  bool is_backup_data_;
  ObSSBackupCtx *backup_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSBackupDagNet);
};

class ObSSBackupDag : public share::ObIDag 
{
public:
  explicit ObSSBackupDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObSSBackupDag();
  virtual int inner_reset_status_for_retry();
  virtual bool inner_check_can_retry();
  virtual lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }  
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  ObSSBackupCtx *get_backup_ctx() const { return backup_ctx_; }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KP(this));

protected:
  ObSSBackupCtx *backup_ctx_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObSSBackupDag);
};

class ObLSBackupSSMetaDag : public ObSSBackupDag
{
public:
  ObLSBackupSSMetaDag();
  virtual ~ObLSBackupSSMetaDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupSSMetaDag);
};

class ObInitialLSBackupDag : public ObSSBackupDag
{
public:
  ObInitialLSBackupDag();
  virtual ~ObInitialLSBackupDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialLSBackupDag);
};

class ObStartLSBackupDag : public ObSSBackupDag
{
public:
  ObStartLSBackupDag();
  virtual ~ObStartLSBackupDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStartLSBackupDag);
};

class ObBackupSysTabletsDag : public ObSSBackupDag
{
public:
  ObBackupSysTabletsDag();
  virtual ~ObBackupSysTabletsDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupSysTabletsDag);
};

class ObBackupDataTabletsDag : public ObSSBackupDag
{
public:
  ObBackupDataTabletsDag();
  virtual ~ObBackupDataTabletsDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDataTabletsDag);
};

class ObTabletGroupBackupDag : public ObSSBackupDag
{
public:
  ObTabletGroupBackupDag();
  virtual ~ObTabletGroupBackupDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;

  int init(
      const common::ObIArray<ObSSBackupTabletPair> &tablet_array,
      share::ObIDagNet *dag_net,
      share::ObIDag *finish_dag,
      ObBackupTabletGroupCtx *tablet_group_ctx);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));
protected:
  bool is_inited_;
  ObArray<ObSSBackupTabletPair> tablet_array_;
  share::ObIDag *finish_dag_;
  ObBackupTabletGroupCtx *tablet_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupBackupDag);
};

class ObTabletBackupDag : public ObSSBackupDag
{
public:
  ObTabletBackupDag();
  virtual ~ObTabletBackupDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      share::ObIDagNet *dag_net,
      ObBackupTabletGroupCtx *tablet_group_ctx = nullptr);
  int get_tablet_group_ctx(ObBackupTabletGroupCtx *&tablet_group_ctx);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));

protected:
  bool is_inited_;
  ObSSBackupTabletCtx tablet_ctx_;
  ObBackupTabletGroupCtx *tablet_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackupDag);
};

class ObFinishLSBackupDag : public ObSSBackupDag
{
public:
  ObFinishLSBackupDag();
  virtual ~ObFinishLSBackupDag();
  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool check_can_schedule() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObSSBackupDag", ObSSBackupDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishLSBackupDag);
};

}
}
#endif
