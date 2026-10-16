/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_SEQ_
#define OCEABASE_STORAGE_OB_LOB_SEQ_

#include <string>

#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_ctype.h"

namespace oceanbase
{
namespace storage
{

// ObLobSeqId 是一个轻量值类型 / 包装器：非拥有式持有一个 ObString 视图，
// 仅用于方便打印与比较一个 lob seq id，不分配内存、不需要 allocator。
// 有状态的递增生成逻辑见 ObLobSeqIdGenerator。
class ObLobSeqId
{
public:
  ObLobSeqId() : seq_id_() {}
  explicit ObLobSeqId(const ObString &seq_id) : seq_id_(seq_id) {}
  ~ObLobSeqId() {}
  void reset() { seq_id_.reset(); }
  const ObString &get_obstring() const { return seq_id_; }
  bool is_valid() const
  {
    return nullptr != seq_id_.ptr()
           && seq_id_.length() >= static_cast<ObString::obstr_size_t>(LOB_SEQ_ID_MIN_LEN)
           && static_cast<uint8_t>(seq_id_.ptr()[0]) == LOB_SEQ_ID_VERSION
           && ((seq_id_.length() - LOB_SEQ_ID_VERSION_LEN) % LOB_SEQ_ID_UNIT_LEN) == 0;
  }
  bool empty() const { return 0 == seq_id_.length(); }
  // this > other => return 1
  // this == other => return 0
  // this < other => return -1
  int compare(const ObLobSeqId &other) const;
  int compare(const ObString &other) const { return compare(ObLobSeqId(other)); }
  std::string get_hex_str() const;
  int64_t needed_to_string_buffer_size() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

public:
  static char *store32be(char *ptr, uint32_t val);
  static uint32_t load32be(const char *ptr);
  // 生成 summary 使用的固定 seq id（idx 为 0）
  static int get_summary_seq_id(ObString &summary_seq_id);
  // 把第 idx 个 step 的 seq num 以 [version][大端 uint32] 写入 seq_id 缓冲区
  static int get_seq_id(int64_t idx, ObString &seq_id);
  // Build the idx-th seq id with allocator-owned storage, expanding to multiple units as needed.
  static int get_seq_id(ObIAllocator &allocator, const int64_t idx, ObString &seq_id);

public:
  static constexpr uint8_t LOB_SEQ_ID_VERSION = 1;
  static constexpr uint32_t LOB_SEQ_ID_VERSION_LEN = sizeof(uint8_t);
  static constexpr uint32_t LOB_SEQ_ID_UNIT_LEN = sizeof(uint32_t);
  static constexpr uint32_t LOB_SEQ_ID_MIN_LEN = LOB_SEQ_ID_VERSION_LEN + LOB_SEQ_ID_UNIT_LEN;
  static constexpr uint32_t LOB_SEQ_ID_MIN = 256;
  static constexpr uint32_t LOB_SEQ_STEP_LEN = 256;
  static constexpr uint32_t LOB_SEQ_STEP_MAX = UINT32_MAX;//65535;
  static constexpr uint32_t LOB_SEQ_UNIT_MAX = LOB_SEQ_STEP_MAX / LOB_SEQ_STEP_LEN;

private:
  ObString seq_id_;  // 非拥有式视图
};

// ObLobSeqIdGenerator 是有状态的 seq id 生成器：解析当前 seq id、按 step 自增，
// 并把下一个 seq id 写回内部缓冲区。需要 allocator 持有 digits/seq buf。
class ObLobSeqIdGenerator
{
public:
  ObLobSeqIdGenerator(const ObString &seq_id, ObIAllocator *allocator);
  ObLobSeqIdGenerator(ObIAllocator *allocator);
  ObLobSeqIdGenerator();
  ~ObLobSeqIdGenerator();
  void set_seq_id(ObString &seq_id);
  void set_allocator(ObIAllocator *allocator);
  int get_next_seq_id(ObString &seq_id);
  int get_next_seq_id(ObString &seq_id, ObLobSeqIdGenerator &end);
  void get_seq_id(ObString &seq_id);
  int compare(ObLobSeqIdGenerator &other);
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int init_digits();
  int init_seq_buf();
  void reset_digits();
  void reset_seq_buf();
  int parse();
  int add_digits(uint32_t val);
  int append_seq_buf(uint32_t val);
  int replace_seq_buf_last_node(uint32_t val);
  int extend_digits();
  int extend_seq_buf();
  int empty();
private:

  ObIAllocator *allocator_;
  ObString seq_id_;

  bool read_only_;
  bool parsed_;

  uint32_t last_seq_buf_pos_;
  char *buf_;
  uint32_t len_;
  uint32_t cap_;

  uint32_t *digits_;
  uint32_t dig_len_;
  uint32_t dig_cap_;

private:
  static const uint32_t SEQ_PER_UNIT_BUF_SIZE = 16;

public:
  static const uint32_t LOB_SEQ_DIGIT_DEFAULT_LEN = 1024;
  static const uint32_t LOB_SEQ_DEFAULT_SEQ_BUF_LEN = 1024 * 8;
};

} // storage
} // oceanbase

#endif
