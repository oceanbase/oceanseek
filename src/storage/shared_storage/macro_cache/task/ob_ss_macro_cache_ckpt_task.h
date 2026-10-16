/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_CKPT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_CKPT_TASK_H_

#include "lib/task/ob_timer.h"
#include "lib/container/ob_se_array.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"

namespace oceanbase
{
namespace storage
{
class ObSSMacroCacheMgr;
class ObSSMacroCache;

// When writing the macro cache ckpt, first serialize all ObSSMacroCacheMeta into the ckpt data blocks.
// After the ckpt data blocks, serialize ObSSMacroCacheCkptMeta and write it into the meta block.
// If the meta block exists, it indicates the corresponding ckpt was successfully written.
// If there's a failure during the checkpoint,
// some data blocks might exist, but the meta block will not be written.
//
// Data block path: cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/data/version_id/seq_id
// Meta block path: cluster_id/server_id/tenant_id_epoch_id/macro_cache_ckpt/meta/version_id
class ObSSMacroCacheCkptMeta final
{
public:
  using CkptDataBlockSeqArr = common::ObSEArray<uint64_t, 64>;

public:
  ObSSMacroCacheCkptMeta(const uint64_t tenant_id);
  ~ObSSMacroCacheCkptMeta();
  void reset();

  int record_ckpt_data_seq(const uint64_t seq_id);
  const CkptDataBlockSeqArr &get_ckpt_data_seq_arr() const;

  void record_ckpt_time();
  int64_t get_ckpt_time() const;

  TO_STRING_KV(K(ckpt_time_us_), K(data_seq_arr_));

public:
  static const int64_t OB_SS_MACRO_CACHE_CKPT_META_VERSION = 1;
  OB_UNIS_VERSION(OB_SS_MACRO_CACHE_CKPT_META_VERSION);

private:
  int64_t ckpt_time_us_;
  // Stores sequence IDs of ckpt data macro blocks in this checkpoint.
  // Combined with version ID, it uniquely identifies a physical checkpoint data block location.
  CkptDataBlockSeqArr data_seq_arr_;
};

class ObMCParseCkptVersionsOp : public ObBaseDirEntryOperator
{
public:
  ObMCParseCkptVersionsOp(const uint64_t tenant_id);

  virtual int func(const dirent *entry) override;
  virtual bool need_get_file_meta() const override { return true; }
  virtual int get_path_format(char *format, const int64_t format_buf_size) const = 0;

  bool is_max_ckpt_version_valid() const;
  bool is_empty() const;

  TO_STRING_KV(K(max_ckpt_version_), K(meta_file_size_),
      K(version_id_arr_.count()), K(version_id_arr_));

public:
  uint64_t max_ckpt_version_;
  int64_t meta_file_size_;
  ObSEArray<uint64_t, 64> version_id_arr_;
};

// During replay, only ckpts that have been successfully written can be used.
// Therefore, it is necessary to traverse the ckpt meta files to obtain max ckpt version.
class ObMCGetCkptMetaVersionsOp : public ObMCParseCkptVersionsOp
{
public:
  ObMCGetCkptMetaVersionsOp(const uint64_t tenant_id);
  virtual int get_path_format(char *format, const int64_t format_buf_size) const override;
};

// If there's a failure during the checkpoint,
// some data blocks might exist, but the meta block will not be written.
// when cleaning up old checkpoints,
// we need to ensure that all "versions" for which data write attempts were made are deleted.
class ObMCGetCkptDataVersionsOp : public ObMCParseCkptVersionsOp
{
public:
  ObMCGetCkptDataVersionsOp(const uint64_t tenant_id);
  virtual int get_path_format(char *format, const int64_t format_buf_size) const override;
};

class ObSSMacroCacheCkptTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSMacroCacheCkptTask(volatile bool &is_stopped);
  virtual ~ObSSMacroCacheCkptTask();
  int init(ObSSMacroCacheMgr *macro_cache_mgr, const int64_t parallelism);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

  static int read_ckpt_meta(
      const int64_t version_id,
      const int64_t meta_file_size,
      ObSSMacroCacheCkptMeta &ckpt_meta);
  static int async_read_ckpt_data(
      char *buf, const int64_t buf_size,
      const int64_t version_id,
      const int64_t seq_id,
      blocksstable::ObStorageObjectHandle &read_handle);
  // `next_ckpt_version` is version for next checkpoint write,
  // and equals the num of completed checkpoints
  void set_next_ckpt_version(const uint64_t next_ckpt_version) { next_version_ = next_ckpt_version; };
  int64_t get_data_blk_size() const;

private:
  int write_ckpt_();
  int write_ckpt_data_(
      const ObSSMacroCacheType &cache_type,
      ObSSMacroCache &macro_cache,
      ObSSMacroCacheCkptMeta &ckpt_meta);
  int async_write_ckpt_data_block_(
      const char *buf, const int64_t buf_size,
      const uint64_t seq_id,
      blocksstable::ObStorageObjectHandle &write_handle);
  int write_ckpt_meta_(const ObSSMacroCacheCkptMeta &ckpt_meta);

  int clean_up_old_ckpts_(const uint64_t min_verison_to_keep);
  int try_restore_next_ckpt_version_();

private:
  static const int64_t MAX_PARALLELISM = 64;
  static const int64_t MIN_PARALLELISM = 1;
  static const int64_t CKPT_SCHEDULE_INTERVAL_US = 600LL * 1000LL * 1000LL; // 10m
  
  int64_t parallelism_;
  int64_t ckpt_data_blk_size_;
  ObSSMacroCacheMgr *macro_cache_mgr_;
  volatile uint64_t next_version_;
  int64_t ckpt_item_cnt_; // just for print log
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_CKPT_TASK_H_ */