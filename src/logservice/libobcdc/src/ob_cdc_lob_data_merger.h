/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_DATA_MERGER_H_
#define OCEANBASE_LIBOBCDC_LOB_DATA_MERGER_H_

#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "ob_cdc_lob_ctx.h"    // ObLobDataOutRowCtxList

namespace oceanbase
{
namespace libobcdc
{
class IObCDCLobDataMerger
{
public:
  enum
  {
    MAX_PARSER_NUM = 32
  };

public:
  virtual ~IObCDCLobDataMerger() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;

  // 异步提交 DML LOB 任务，DDL 使用 merge_ddl_lob 同步拼接接口。
  virtual int push(ObLobDataOutRowCtxList &task, volatile bool &stop_flag) = 0;
  // 在调用线程中完成 DDL LOB 拼接，返回时不会遗留引用该 ObLobDataOutRowCtxList 的异步任务。
  virtual int merge_ddl_lob(ObLobDataOutRowCtxList &task, volatile bool &stop_flag) = 0;
  virtual void get_task_count(int64_t &lob_data_list_task_count) const = 0;
  virtual int get_frag_queue_info(int64_t &total_task_count, int64_t &total_queue_capacity) = 0;
  virtual void print_stat_info() = 0;
};

/////////////////////////////////////////////////////////////////////////////////////////

class IObLogErrHandler;

typedef common::ObMQThread<IObCDCLobDataMerger::MAX_PARSER_NUM> ObLobDataMergerThread;

class ObCDCLobDataMerger : public IObCDCLobDataMerger, public ObLobDataMergerThread
{
public:
  ObCDCLobDataMerger();
  virtual ~ObCDCLobDataMerger();

public:
  int start();
  void stop();
  void mark_stop_flag() { ObLobDataMergerThread::mark_stop_flag(); }
  int push(ObLobDataOutRowCtxList &task, volatile bool &stop_flag);
  int merge_ddl_lob(ObLobDataOutRowCtxList &task, volatile bool &stop_flag);
  void get_task_count(int64_t &lob_data_list_task_count) const
  {
    lob_data_list_task_count = ATOMIC_LOAD(&lob_data_list_task_count_);
  }
  int get_frag_queue_info(int64_t &total_task_count, int64_t &total_queue_capacity);
  void print_stat_info() {}
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  int init(
      const int64_t thread_num,
      const int64_t queue_size,
      IObLogErrHandler &err_handler);
  void destroy();

private:
  int check_image_desc_(
      ObLobDataOutRowCtxList &task);
  int push_task_(
      ObLobDataOutRowCtxList &task,
      volatile bool &stop_flag);
  int push_lob_column_(
      ObIAllocator &allocator,
      ObLobDataGetCtx &lob_data_get_ctx,
      volatile bool &stop_flag);
  int push_prepare_task_(
      ObIAllocator &allocator,
      ObLobDataGetCtx &lob_data_get_ctx,
      const bool is_new_col,
      volatile bool &stop_flag);
  int handle_prepare_task_(
      ObLobImagePrepareTask &task,
      const bool is_sync,
      volatile bool &stop_flag);
  int handle_fragment_task_(
      ObLobColFragTask &task,
      volatile bool &stop_flag);
  int build_and_dispatch_fragment_tasks_(
      ObLobImagePrepareTask &task,
      const uint64_t chunk_count,
      const uint64_t expected_byte_len,
      const bool is_sync,
      volatile bool &stop_flag);
  int finish_lob_image_(
      ObLobDataGetCtx &lob_data_get_ctx,
      const bool is_new_col,
      volatile bool &stop_flag);
  int handle_when_outrow_log_fragment_progress_done_(
      ObLobDataGetCtx &lob_data_get_ctx,
      const bool is_new_col,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  int try_to_push_task_into_formatter_(
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  int merge_fragments_(
      ObLobDataGetCtx &lob_data_get_ctx,
      const bool is_new_col,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      ObString &data);
  int after_fragment_progress_done_(
      ObLobDataGetCtx &lob_data_get_ctx,
      ObLobDataOutRowCtxList &lob_data_out_row_ctx_list,
      volatile bool &stop_flag);
  bool is_in_stop_status(volatile bool stop_flag) const { return stop_flag || ObLobDataMergerThread::is_stoped(); }
  // TODO
  void print_task_count_();

private:
  bool                      is_inited_;
  // Used to ensure that tasks are evenly distributed to threads
  uint64_t                  round_value_;
  // ObLobDataOutRowCtxList
  int64_t                   lob_data_list_task_count_ CACHE_ALIGNED;
  // Per-thread queue capacity, used to compute total queue capacity:
  //   total_capacity = thread_num * queue_size_
  int64_t                   queue_size_ CACHE_ALIGNED;

  IObLogErrHandler          *err_handler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCLobDataMerger);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
