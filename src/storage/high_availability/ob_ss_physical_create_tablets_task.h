/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#ifndef OCEANBASE_SHARED_STORAGE_PHYSICAL_CREATE_TABLETS_TASK_H_
#define OCEANBASE_SHARED_STORAGE_PHYSICAL_CREATE_TABLETS_TASK_H_

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "storage/high_availability/ob_ss_storage_ha_reader.h"

namespace oceanbase
{
namespace storage
{
class ObSSMigrationCtx;
struct ObSSMetaCtrlKey;
// ObPhysicalCreateTabletsTaskParam
struct ObPhysicalCreateTabletsTaskParam final
{
  ObPhysicalCreateTabletsTaskParam();
  ~ObPhysicalCreateTabletsTaskParam();
  void reset();
  bool is_valid() const;
  int assign(const ObPhysicalCreateTabletsTaskParam &other);

  TO_STRING_KV(K_(tenant_id), 
               K_(ls_id), 
               "type", ObMigrationOpType::get_str(type_),
               K_(task_id), 
               K_(snapshot), 
               KP_(ls), 
               KP_(ha_dag),
               K_(ss_checkpoint_scn),
               K_(filled_tx_scn),
               K_(use_ls_ckpt),
               K_(ls_ckpt_macro_id),
               KP_(ctx));


  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObMigrationOpType::TYPE type_;
  share::ObTaskId task_id_;
  share::SCN snapshot_;
  ObLS *ls_;
  ObStorageHADag *ha_dag_;
  share::SCN ss_checkpoint_scn_;
  share::SCN filled_tx_scn_;
  bool use_ls_ckpt_;
  blocksstable::MacroBlockId ls_ckpt_macro_id_;
  ObSSMigrationCtx *ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalCreateTabletsTaskParam);
};


class ObSSPhysicalCreateTabletsProducerTask;
class ObSSPhysicalCreateTabletsConsumerTask;
class ObSSPhysicalCreateTabletsFinishTask;

// ObSSPhysicalCreateTabletsProducerTask
class ObSSPhysicalCreateTabletsProducerTask : public share::ObITask
{
public:
  ObSSPhysicalCreateTabletsProducerTask();
  virtual ~ObSSPhysicalCreateTabletsProducerTask();
  int init(
      const ObPhysicalCreateTabletsTaskParam &param,
      ObSSPhysicalCreateTabletsFinishTask *finish_task);
  virtual int process() override;

  void notify_create_next_consumer();
  void inc_consumer_cnt();
  void dec_consumer_cnt();

  VIRTUAL_TO_STRING_KV(K("ObSSPhysicalCreateTabletsProducerTask"), 
                       KP(this), 
                       K_(param),
                       "create_consumer_signal_cnt", ATOMIC_LOAD(&create_consumer_signal_cnt_),
                       "consumer_cnt", ATOMIC_LOAD(&consumer_cnt_),
                       K_(generated_consumer_id));

private:
  int create_ls_inner_tablets_first_();
  int alloc_and_init_tablets_reader_(ObICopySSTabletsReader *&tablets_reader);
  void free_tablets_reader_(ObICopySSTabletsReader *&tablets_reader);
  int64_t generate_consumer_id_();
  int generate_consumer_(
      ObIDagNet *dag_net, 
      ObIDag *dag, 
      ObICopySSTabletsReader *tablets_reader);
  void wait_until_consumers_complete_();


private:
  static const int64_t CHECK_CONDITION_INTERVAL_MS = 200;
  static const int64_t CHECK_CONDITION_INTERVAL = 200_ms;

  bool is_inited_;
  ObPhysicalCreateTabletsTaskParam param_;
  ObSSPhysicalCreateTabletsFinishTask *finish_task_;

  common::ObThreadCond create_consumer_cond_;
  int64_t create_consumer_signal_cnt_;
  int64_t consumer_cnt_;
  int64_t generated_consumer_id_;

  DISALLOW_COPY_AND_ASSIGN(ObSSPhysicalCreateTabletsProducerTask);
};


// ObSSPhysicalCreateTabletsConsumerDag
class ObSSPhysicalCreateTabletsConsumerDag : public ObStorageHADag
{
public:
  ObSSPhysicalCreateTabletsConsumerDag();
  virtual ~ObSSPhysicalCreateTabletsConsumerDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param, 
      ObIAllocator &allocator) const override;
  virtual bool inner_check_can_retry() override;
  virtual int create_first_task() override;

  int init(
      int64_t consumer_id,
      const ObPhysicalCreateTabletsTaskParam &param, 
      ObSSPhysicalCreateTabletsFinishTask *finish_task,
      ObSSPhysicalCreateTabletsProducerTask *producer_task,
      ObICopySSTabletsReader *tablets_reader);

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, 
                       KP(this), 
                       K_(consumer_id), 
                       K_(param),
                       KP_(finish_task),
                       KP_(producer_task),
                       KP_(tablets_reader));

private:
  bool is_inited_;
  int64_t consumer_id_;
  ObPhysicalCreateTabletsTaskParam param_;
  ObSSPhysicalCreateTabletsFinishTask *finish_task_;
  ObSSPhysicalCreateTabletsProducerTask *producer_task_;
  ObICopySSTabletsReader *tablets_reader_;

  DISALLOW_COPY_AND_ASSIGN(ObSSPhysicalCreateTabletsConsumerDag);
};


// ObSSPhysicalCreateTabletsConsumerTask
class ObSSPhysicalCreateTabletsConsumerTask : public share::ObITask
{
public:
  ObSSPhysicalCreateTabletsConsumerTask();
  virtual ~ObSSPhysicalCreateTabletsConsumerTask();
  int init(
      int64_t consumer_id,
      const ObPhysicalCreateTabletsTaskParam &param, 
      ObSSPhysicalCreateTabletsFinishTask *finish_task,
      ObSSPhysicalCreateTabletsProducerTask *producer_task,
      ObICopySSTabletsReader *tablets_reader);
  virtual int process() override;

  int64_t consumer_id() const { return consumer_id_; }
  int64_t get_read_tablets_cnt() const { return sstablet_cnt_; }
  int64_t get_read_tablets_cost() const { return read_tablets_cost_us_; }
  int64_t get_create_tablets_cost() const { return create_tablets_cost_us_; }
  int64_t get_create_tablets_cnt() const { return created_tablet_array_.size(); }
  const common::ObIArray<common::ObTabletID> &get_created_tablet_array() const { return created_tablet_array_; }
  share::SCN get_max_transfer_scn() const { return max_tablet_transfer_scn_; }

  VIRTUAL_TO_STRING_KV(K("ObSSPhysicalCreateTabletsConsumerTask"), 
                       KP(this), 
                       K_(consumer_id), 
                       K_(param),
                       KP_(finish_task),
                       KP_(producer_task),
                       K_(sstablet_cnt),
                       K_(read_tablets_cost_us),
                       K_(create_tablets_cost_us),
                       K_(created_tablet_array),
                       K_(max_tablet_transfer_scn));

private:
  struct ObRestoredTabletLockRowArg;
  int fetch_tablets_(
      int64_t consumer_id,
      const ObPhysicalCreateTabletsTaskParam &param,
      ObICopySSTabletsReader *tablets_reader);
  int rebuild_restored_tablet_lock_rows_();
  int create_or_verify_restored_tablet_lock_rows_(
      const common::ObIArray<ObRestoredTabletLockRowArg> &args);
  int create_or_verify_restored_lock_row_(
      const ObSSMetaCtrlKey &lock_row_key,
      const int64_t next_op_id,
      common::ObMySQLTransaction &trans);
  int create_tablets_();
  int check_is_deleted_tablet_(
      const ObTablet &ss_tablet,
      bool &is_deleted, 
      share::SCN &deleted_scn,
      share::SCN &transfer_scn) const;
  int report_tablet_(
      const share::ObLSID &ls_id, 
      const common::ObTabletID &tablet_id);

private:
  static const int64_t CREATE_TABLETS_BATCH_CNT_PER_TASK = 8;

  bool is_inited_;
  int64_t consumer_id_;
  ObPhysicalCreateTabletsTaskParam param_;
  ObSSPhysicalCreateTabletsFinishTask *finish_task_;
  ObSSPhysicalCreateTabletsProducerTask *producer_task_;

  common::ObArenaAllocator allocator_;
  ObTabletHandle sstablet_hdls_[CREATE_TABLETS_BATCH_CNT_PER_TASK];
  ObCopySSTabletMeta sstablet_metas_[CREATE_TABLETS_BATCH_CNT_PER_TASK];
  int64_t sstablet_cnt_;

  int64_t read_tablets_cost_us_;
  int64_t create_tablets_cost_us_;
  common::ObSArray<common::ObTabletID> created_tablet_array_;
  share::SCN max_tablet_transfer_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObSSPhysicalCreateTabletsConsumerTask);
};


// ObSSPhysicalCreateTabletsFinishTask
class ObSSPhysicalCreateTabletsFinishTask : public share::ObITask
{
public:
  ObSSPhysicalCreateTabletsFinishTask();
  virtual ~ObSSPhysicalCreateTabletsFinishTask();
  int init(const ObPhysicalCreateTabletsTaskParam &param);
  virtual int process() override;

  VIRTUAL_TO_STRING_KV(K("ObSSPhysicalCreateTabletsFinishTask"), 
                       KP(this),
                       K_(param),
                       K_(consumer_cnt),
                       K_(read_tablets_cnt),
                       K_(create_tablets_cnt),
                       K_(total_cost_us),
                       K_(read_tablets_cost_us),
                       K_(create_tablets_cost_us),
                       K_(max_transfer_scn));

  int one_consumer_task_finish(const ObSSPhysicalCreateTabletsConsumerTask* const consumer_task);

private:
  int prepare_for_remove_redundant_tablets_if_need_(ObLS *ls);
  bool check_need_remove_redundant_tablets_(ObLS *ls);
  int remove_redundant_tablets_if_need_();
  int try_update_ls_transfer_scn_();

private:
  static const int64_t CREATED_TABLET_SET_BUCKET_NUM = 1024;


  bool is_inited_;
  ObPhysicalCreateTabletsTaskParam param_;
  common::SpinRWLock lock_;

  int64_t consumer_cnt_;
  int64_t read_tablets_cnt_;
  int64_t create_tablets_cnt_;

  int64_t start_ts_;
  int64_t total_cost_us_;
  int64_t read_tablets_cost_us_;
  int64_t create_tablets_cost_us_;
  common::hash::ObHashSet<common::ObTabletID> created_tablet_set_;
  bool need_remove_redundant_tablets_;
  share::SCN max_transfer_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObSSPhysicalCreateTabletsFinishTask);
};


}
}

#endif
