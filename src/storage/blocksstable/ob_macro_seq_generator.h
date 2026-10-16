/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_SEQ_GENERATOR_H
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_SEQ_GENERATOR_H

#include "storage/ddl/ob_ddl_seq_generator.h"

namespace oceanbase
{

namespace blocksstable
{

struct ObMacroSeqParam
{
public:
  enum SeqType
  {
    SEQ_TYPE_INC = 0,
    SEQ_TYPE_SKIP = 1,
    SEQ_TYPE_MAX,
  };
  ObMacroSeqParam()
    : seq_type_(SEQ_TYPE_MAX), start_(0), end_(-1), interval_(0), step_(0)
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(seq_type), K_(start), K_(end), K_(interval), K_(step));

  SeqType seq_type_;
  int64_t start_;
  int64_t end_;
  int64_t interval_;
  int64_t step_;
};

struct ObMacroSeqWindow
{
  ObMacroSeqWindow() : start_(-1), end_(-1) {}
  void reset()
  {
    start_ = -1;
    end_ = -1;
  }
  TO_STRING_KV(K_(start), K_(end));

  int64_t start_;
  int64_t end_;
};

/**
 * 在同一 object version 内按固定 slot 划分 macro sequence 窗口。
 *
 * slot_count = VERSION_LOCAL_SEQ_LIMIT / MACRO_STEP_SIZE，当前为 2048。
 * 每个 slot 对应一个左闭右开窗口：
 * [seq_slot * MACRO_STEP_SIZE, (seq_slot + 1) * MACRO_STEP_SIZE)。
 *
 * INC Major 的 slot 分配如下：
 *
 * [0, slot_count - 3) : data/CG/LOB producer，由稳定的 slice_idx 选择 slot。
 * slot_count - 3      : final index/meta tree 固定 slot，当前为 2045。
 * slot_count - 2      : final persister/linked 固定 slot，当前为 2046。
 * slot_count - 1      : guard slot，当前为 2047，正常路径不分配。
 *
 * data/CG/LOB 使用低位动态 slot，final producer 使用尾部固定 slot，
 * 因此 DAG 重建时可以直接重算窗口，不需要恢复 data high-water。
 * guard slot 只用于隔离 persister 极端越界失败可能留下的对象。
 *
 * 本类只负责无状态的选窗和边界转换，不在窗口内逐块发号。
 * 窗口内递增仍由 ObMacroIncSeqGenerator 或 persister 既有计数逻辑完成。
 */
struct ObVersionLocalMacroSeq final
{
  static int build_window(
      const int64_t seq_slot,
      ObMacroSeqWindow &window);
  static int build_inc_major_data_window(
      const int64_t seq_slot,
      ObMacroSeqWindow &window);
  static int calc_inc_major_data_seq_end(
      const int64_t seq_slot,
      const int64_t start,
      int64_t &end);
  static int build_inc_major_index_window(
      ObMacroSeqWindow &window);
  static int build_inc_major_persister_window(
      ObMacroSeqWindow &window);
  static int get_raw_seq(
      const int64_t packed_seq,
      int64_t &raw_seq);
  static int64_t get_inc_major_index_slot();
  static int64_t get_inc_major_persister_slot();
};

class ObMacroSeqGenerator
{
public:
  ObMacroSeqGenerator() {}
  virtual ~ObMacroSeqGenerator() {}
  virtual void reset() = 0;
  virtual int init(const ObMacroSeqParam &seq_param) = 0;
  virtual int get_next(int64_t &seq_val) = 0;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const = 0;
  virtual int64_t get_current() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObMacroIncSeqGenerator: public ObMacroSeqGenerator
{
public:
  ObMacroIncSeqGenerator() : is_inited_(false), start_(0), current_(-1), seq_threshold_(0) {}
  virtual ~ObMacroIncSeqGenerator() {}
  virtual void reset() override;
  virtual int init(const ObMacroSeqParam &seq_param) override;
  virtual int get_next(int64_t &seq_val) override;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const override;
  virtual int64_t get_current() override { return current_; }
  TO_STRING_KV(K_(is_inited), K_(start), K_(current), K_(seq_threshold));
private:
  bool is_inited_;
  int64_t start_;
  int64_t current_;
  int64_t seq_threshold_;
};

class ObMacroSkipSeqGenerator: public ObMacroSeqGenerator
{
public:
  ObMacroSkipSeqGenerator() : ddl_seq_generator_() {}
  virtual ~ObMacroSkipSeqGenerator() {}
  virtual void reset() override;
  virtual int init(const ObMacroSeqParam &seq_param) override;
  virtual int get_next(int64_t &seq_val) override;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const override;
  virtual int64_t get_current() override { return ddl_seq_generator_.get_current(); }
  TO_STRING_KV(K_(ddl_seq_generator));
private:
  storage::ObDDLSeqGenerator ddl_seq_generator_;
};

}// namespace blocksstable
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_SEQ_GENERATOR_H
