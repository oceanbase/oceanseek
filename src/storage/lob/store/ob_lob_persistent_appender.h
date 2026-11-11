/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information and may not be used or disclosed without authorization.
 */

#ifndef OCEANBASE_STORAGE_LOB_PERSISTENT_APPENDER_H_
#define OCEANBASE_STORAGE_LOB_PERSISTENT_APPENDER_H_

#include "lib/container/ob_array.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/lob/store/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaManager;
class ObLobPieceManager;

class ObLobPersistentAppender
{
public:
  ObLobPersistentAppender();
  virtual ~ObLobPersistentAppender();
  void reset();
  int init(ObLobAppendContext *ctx,
           ObLobMetaManager *meta_mgr,
           ObLobPieceManager *piece_mgr,
           const ObLobMetaInfo &old_summary_info,
           const ObLobMetaInfo &last_meta_info);
  int append();

  TO_STRING_KV(K_(is_inited), KPC_(ctx), K_(old_summary_info), K_(last_meta_info), K_(write_stats));

protected:
  virtual bool is_outtable() = 0;
  virtual int read_last_chunk_() = 0;
  virtual int write_data() = 0;
  virtual int after_write_summary();

  int detect_no_logging_(const ObLobAppendContext &ctx);
  // Split the append source into two parts:
  //   1. In-row data, or the tail chunk for OutRow/OutTable data.
  //   2. Appended data.
  // This function may be called repeatedly because the OutTable append path rereads the data when writing CLOG.
  int prepare_source_();
  int validate_write_stats_(uint64_t &total_chunk_count);
  int write_meta_summary_(const uint64_t total_chunk_count);

protected:
  bool is_inited_;
  ObLobAppendContext *ctx_;
  ObLobReadContext read_ctx_;
  ObLobMetaManager *meta_mgr_;
  ObLobPieceManager *piece_mgr_;
  common::ObString first_buffer_;
  common::ObString append_data_;
  ObLobMetaInfo old_summary_info_;
  ObLobMetaInfo last_meta_info_;
  ObLobPieceInfo old_piece_;
  ObLobAppendSourceIter source_iter_;
  ObLobWriteStats write_stats_;
  bool is_no_logging_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistentAppender);
};

class ObLobInTablePersistentAppender final : public ObLobPersistentAppender
{
public:
  ObLobInTablePersistentAppender();
  virtual ~ObLobInTablePersistentAppender();
  void reset();

protected:
  virtual bool is_outtable() override { return false; }
  virtual int write_data() override;
  virtual int read_last_chunk_() override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobInTablePersistentAppender);
};

class ObLobOutTablePersistentAppender final : public ObLobPersistentAppender
{
public:
  ObLobOutTablePersistentAppender();
  virtual ~ObLobOutTablePersistentAppender();
  void reset();

protected:
  virtual bool is_outtable() override { return true; }
  virtual int write_data() override;
  virtual int after_write_summary() override;
  virtual int read_last_chunk_() override;

private:
  common::ObArray<ObLobMetaInfo> clog_metas_;

  DISALLOW_COPY_AND_ASSIGN(ObLobOutTablePersistentAppender);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_PERSISTENT_APPENDER_H_
