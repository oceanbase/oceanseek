/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_seq.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

namespace
{

// TO_STRING format: [version:<uint8>;<uint32>,...], len:<int32>
static constexpr int64_t LOB_SEQ_ID_TO_STRING_FIXED_BUF_SIZE = 64;
static constexpr int64_t LOB_SEQ_ID_TO_STRING_UNIT_BUF_SIZE = 16;

inline bool lob_seq_id_has_version(const ObString &seq_id)
{
  return NULL != seq_id.ptr() && seq_id.length() >= static_cast<int32_t>(ObLobSeqId::LOB_SEQ_ID_VERSION_LEN);
}

inline const char *lob_seq_id_payload_ptr(const ObString &seq_id)
{
  return lob_seq_id_has_version(seq_id) ? seq_id.ptr() + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN : seq_id.ptr();
}

inline int64_t lob_seq_id_payload_len(const ObString &seq_id)
{
  return lob_seq_id_has_version(seq_id) ? (seq_id.length() - ObLobSeqId::LOB_SEQ_ID_VERSION_LEN) : 0;
}

inline uint8_t lob_seq_id_version(const ObString &seq_id)
{
  return lob_seq_id_has_version(seq_id) ? static_cast<uint8_t>(seq_id.ptr()[0]) : 0;
}

} // namespace


//==================== ObLobSeqId (wrapper) ====================
char *ObLobSeqId::store32be(char *ptr, uint32_t val) {
  val = htonl(val);
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}

uint32_t ObLobSeqId::load32be(const char *ptr) {
  uint32_t val;
  memcpy(&val, ptr, sizeof(val));
  return ntohl(val);
}

int ObLobSeqId::get_summary_seq_id(ObString &summary_seq_id)
{
  return get_seq_id(0/*idx*/, summary_seq_id);
}

int ObLobSeqId::get_seq_id(int64_t idx, ObString &seq_id)
{
  INIT_SUCC(ret);
  const int64_t max_idx = static_cast<int64_t>(LOB_SEQ_STEP_MAX / LOB_SEQ_STEP_LEN);
  char *const seq_buf = seq_id.ptr();
  if (OB_ISNULL(seq_buf) || seq_id.length() < LOB_SEQ_ID_MIN_LEN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seq id buffer", K(ret), K(idx), K(seq_id));
  } else if (OB_UNLIKELY(idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("seq id index is negative", K(ret), K(idx));
  } else if (OB_UNLIKELY(idx > max_idx)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("seq id index is too large", K(ret), K(idx), K(max_idx));
  } else {
    const uint32_t seq_num = static_cast<uint32_t>(idx) * LOB_SEQ_STEP_LEN;
    seq_buf[0] = static_cast<char>(LOB_SEQ_ID_VERSION);
    ObLobSeqId::store32be(seq_buf + LOB_SEQ_ID_VERSION_LEN, seq_num);
    // get_seq_id() 目前生成仅含一个单元的定长表示。
    seq_id.assign_ptr(seq_buf, LOB_SEQ_ID_MIN_LEN);
  }
  return ret;
}

int ObLobSeqId::get_seq_id(ObIAllocator &allocator, const int64_t idx, ObString &seq_id)
{
  int ret = OB_SUCCESS;
  seq_id.reset();
  if (OB_UNLIKELY(idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("seq id index is negative", K(ret), K(idx));
  } else {
    const int64_t unit_count = (idx == 0) ? 1 : ((idx - 1) / LOB_SEQ_UNIT_MAX + 1);
    const int64_t seq_id_len = LOB_SEQ_ID_VERSION_LEN + unit_count * LOB_SEQ_ID_UNIT_LEN;
    if (OB_UNLIKELY(seq_id_len > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("seq id is too long", K(ret), K(idx), K(unit_count), K(seq_id_len));
    } else {
      char *seq_buf = static_cast<char *>(allocator.alloc(seq_id_len));
      if (OB_ISNULL(seq_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate seq id", K(ret), K(idx), K(seq_id_len));
      } else {
        char *write_pos = seq_buf;
        // Full units keep the largest step-aligned value, and the last unit stores the remaining steps.
        const uint32_t max_seq_num = static_cast<uint32_t>(LOB_SEQ_UNIT_MAX * LOB_SEQ_STEP_LEN);
        write_pos[0] = LOB_SEQ_ID_VERSION;
        write_pos++;
        for (int64_t i = 1; i < unit_count; ++i) {
          write_pos = store32be(write_pos, max_seq_num);
        }
        const int64_t last_unit_idx = idx - (unit_count - 1) * LOB_SEQ_UNIT_MAX;
        (void)store32be(write_pos, static_cast<uint32_t>(last_unit_idx) * LOB_SEQ_STEP_LEN);
        seq_id.assign_ptr(seq_buf, static_cast<ObString::obstr_size_t>(seq_id_len));
      }
    }
  }
  return ret;
}

std::string ObLobSeqId::get_hex_str() const
{
  // seq_id_ 可能包含 '\0' 和不可打印字符，使用小写十六进制编码，可以保证转换结果无损、稳定且可打印。
  static const char HEX[] = "0123456789abcdef";
  std::string hex_str;
  const int64_t seq_id_len = seq_id_.length();
  if (seq_id_len > 0) {
    hex_str.resize(static_cast<std::string::size_type>(seq_id_len) * 2);
    for (int64_t idx = 0; idx < seq_id_len; ++idx) {
      const unsigned char ch = static_cast<unsigned char>(seq_id_.ptr()[idx]);
      const std::string::size_type pos = static_cast<std::string::size_type>(idx) * 2;
      hex_str[pos] = HEX[ch >> 4];
      hex_str[pos + 1] = HEX[ch & 0x0F];
    }
  }
  return hex_str;
}

// this > other => return 1
// this == other => return 0
// this < other => return -1
int ObLobSeqId::compare(const ObLobSeqId &other) const
{
  int cmp_ret = 0;
  const int64_t unit = static_cast<int64_t>(sizeof(uint32_t));
  const int64_t self_len = lob_seq_id_payload_len(seq_id_);
  const int64_t other_len = lob_seq_id_payload_len(other.seq_id_);
  const char *self_ptr = lob_seq_id_payload_ptr(seq_id_);
  const char *other_ptr = lob_seq_id_payload_ptr(other.seq_id_);
  const int64_t common_units = ((self_len < other_len ? self_len : other_len)) / unit;
  for (int64_t i = 0; i < common_units && cmp_ret == 0; ++i) {
    uint32_t l = ObLobSeqId::load32be(self_ptr + unit * i);
    uint32_t r = ObLobSeqId::load32be(other_ptr + unit * i);
    if (l > r) {
      cmp_ret = 1;
    } else if (l < r) {
      cmp_ret = -1;
    }
  }
  if (cmp_ret == 0) {
    if (self_len > other_len) {
      cmp_ret = 1;
    } else if (self_len < other_len) {
      cmp_ret = -1;
    }
  }
  return cmp_ret;
}

int64_t ObLobSeqId::needed_to_string_buffer_size() const
{
  const int64_t payload_len = lob_seq_id_payload_len(seq_id_);
  const int64_t unit_cnt = payload_len > 0 ? payload_len / LOB_SEQ_ID_UNIT_LEN : 0;
  return LOB_SEQ_ID_TO_STRING_FIXED_BUF_SIZE
         + unit_cnt * LOB_SEQ_ID_TO_STRING_UNIT_BUF_SIZE;
}

int64_t ObLobSeqId::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t unit = static_cast<int64_t>(sizeof(uint32_t));
  const uint8_t version = lob_seq_id_version(seq_id_);
  const int64_t seq_len = seq_id_.length();
  const int64_t payload_len = lob_seq_id_payload_len(seq_id_);
  const int64_t unit_cnt = (OB_NOT_NULL(seq_id_.ptr()) && payload_len > 0) ? (payload_len / unit) : 0;
  common::databuff_printf(buf, buf_len, pos, "[version:%u;", version);
  for (int64_t i = 0; i < unit_cnt; ++i) {
    uint32_t val = ObLobSeqId::load32be(lob_seq_id_payload_ptr(seq_id_) + unit * i);
    common::databuff_printf(buf, buf_len, pos, "%u", val);
    if (i + 1 < unit_cnt) {
      common::databuff_printf(buf, buf_len, pos, ",");
    }
  }
  common::databuff_printf(buf, buf_len, pos, "], len:%ld", seq_len);
  return pos;
}

//==================== ObLobSeqIdGenerator ====================
ObLobSeqIdGenerator::ObLobSeqIdGenerator(const ObString &seq_id, ObIAllocator *allocator)
  : allocator_(allocator),
    seq_id_(seq_id),
    read_only_(false),
    parsed_(false),
    last_seq_buf_pos_(0),
    buf_(NULL),
    len_(0),
    cap_(0),
    digits_(NULL),
    dig_len_(0),
    dig_cap_(0)
{
}

ObLobSeqIdGenerator::ObLobSeqIdGenerator(ObIAllocator *allocator)
  : ObLobSeqIdGenerator()
{
  allocator_ = allocator;
}

ObLobSeqIdGenerator::ObLobSeqIdGenerator()
  : allocator_(NULL),
    seq_id_(),
    read_only_(false),
    parsed_(false),
    last_seq_buf_pos_(0),
    buf_(NULL),
    len_(0),
    cap_(0),
    digits_(NULL),
    dig_len_(0),
    dig_cap_(0)
{
}

void ObLobSeqIdGenerator::reset()
{
  reset_digits();
  reset_seq_buf();
  seq_id_ = ObString();
  parsed_ = false;
}

void ObLobSeqIdGenerator::reset_digits()
{
  if (digits_) {
    allocator_->free(digits_);
    digits_ = NULL;
  }
  dig_cap_ = 0;
  dig_len_ = 0;
}

void ObLobSeqIdGenerator::reset_seq_buf()
{
  if (buf_) {
    allocator_->free(buf_);
    buf_ = NULL;
  }
  cap_ = 0;
  len_ = 0;
}

void ObLobSeqIdGenerator::set_seq_id(ObString &seq_id)
{
  seq_id_ = seq_id;
}

void ObLobSeqIdGenerator::set_allocator(ObIAllocator *allocator)
{
  allocator_ = allocator;
}

ObLobSeqIdGenerator::~ObLobSeqIdGenerator()
{
  if (allocator_) {
    if (buf_) {
      allocator_->free(buf_);
    }

    if (digits_) {
      allocator_->free(digits_);
    }
  }
}

// this > other => return 1
// this == other => return 0
// this < other => return -1
int ObLobSeqIdGenerator::compare(ObLobSeqIdGenerator &other)
{
  int cmp_ret = 0;
  uint32_t common_len = (dig_len_ > other.dig_len_) ? other.dig_len_ : dig_len_;
  for (uint32_t i = 0; i < common_len && cmp_ret == 0; ++i) {
    if (digits_[i] > other.digits_[i]) {
      cmp_ret = 1;
    } else if (digits_[i] < other.digits_[i]) {
      cmp_ret = -1;
    }
  }
  if (cmp_ret == 0) {
    if (dig_len_ > other.dig_len_) {
      cmp_ret = 1;
    } else if (dig_len_ < other.dig_len_) {
      cmp_ret = -1;
    }
  }
  return cmp_ret;
}

int ObLobSeqIdGenerator::get_next_seq_id(ObString &seq_id, ObLobSeqIdGenerator &end)
{
  int ret = OB_SUCCESS;
  if (!parsed_ && OB_FAIL(parse())) {
    LOG_WARN("failed get next seq id, parse failed.", K(ret), K(seq_id_));
  } else if (!end.parsed_ && OB_FAIL(end.parse())) {
     LOG_WARN("failed get next seq id, parse end seq id failed.", K(ret), K(end.seq_id_));
  } else if (end.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end should not be empty", K(ret));
  } else if (compare(end) != -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("end is not bigger than this.", K(ret));
  } else if (empty()) {
    uint32_t val = (ObLobSeqId::LOB_SEQ_ID_MIN > end.digits_[0] / 2) ? (end.digits_[0] / 2) : ObLobSeqId::LOB_SEQ_ID_MIN;
    if (OB_FAIL(add_digits(ObLobSeqId::LOB_SEQ_ID_MIN)) || OB_FAIL(append_seq_buf(ObLobSeqId::LOB_SEQ_ID_MIN))) {
      LOG_WARN("failed add seq node.", K(ret));
    } else {
      seq_id.assign_ptr(buf_, len_);
    }
  } else {
    bool need_expend = false;
    uint64_t cur = digits_[dig_len_ - 1];
    // TODO @luohongdi.lhd. try smaller step rather than add digits
    cur += ObLobSeqId::LOB_SEQ_STEP_LEN;
    if (cur > ObLobSeqId::LOB_SEQ_STEP_MAX) {
      need_expend = true;
    } else {
      digits_[dig_len_ - 1] = cur;
      if (compare(end) != -1) {
        need_expend = true;
      }
      digits_[dig_len_ - 1] = (cur - ObLobSeqId::LOB_SEQ_STEP_LEN);
    }
    if (need_expend) {
      if (OB_FAIL(add_digits(ObLobSeqId::LOB_SEQ_ID_MIN)) || OB_FAIL(append_seq_buf(ObLobSeqId::LOB_SEQ_ID_MIN))) {
        LOG_WARN("failed add seq node.", K(ret), K(len_), K(cap_), K(dig_len_), K(dig_cap_));
      } else {
        seq_id.assign_ptr(buf_, len_);
      }
    } else if (OB_FAIL(replace_seq_buf_last_node(static_cast<uint32_t>(cur)))) {
      LOG_WARN("failed replace seq node.", K(ret), K(len_), K(cap_), K(dig_len_), K(dig_cap_));
    } else {
      seq_id.assign_ptr(buf_, len_);
    }
  }
  return ret;
}

int ObLobSeqIdGenerator::get_next_seq_id(ObString &seq_id)
{
  int ret = OB_SUCCESS;
  if (!parsed_ &&
      OB_FAIL(parse())) {
    LOG_WARN("failed get next seq id, parse failed.", K(ret), K(seq_id_));
  } else if (empty()) {
    if (OB_FAIL(add_digits(ObLobSeqId::LOB_SEQ_ID_MIN)) || OB_FAIL(append_seq_buf(ObLobSeqId::LOB_SEQ_ID_MIN))) {
      LOG_WARN("failed add seq node.", K(ret));
    } else {
      seq_id.assign_ptr(buf_, len_);
    }
  } else {
    uint64_t cur = digits_[dig_len_ - 1];
    cur += ObLobSeqId::LOB_SEQ_STEP_LEN;
    if (cur > ObLobSeqId::LOB_SEQ_STEP_MAX) {
      if (OB_FAIL(add_digits(ObLobSeqId::LOB_SEQ_ID_MIN)) || OB_FAIL(append_seq_buf(ObLobSeqId::LOB_SEQ_ID_MIN))) {
        LOG_WARN("failed add seq node.", K(ret), K(len_), K(cap_), K(dig_len_), K(dig_cap_));
      } else {
        seq_id.assign_ptr(buf_, len_);
      }
    } else if (OB_FAIL(replace_seq_buf_last_node(static_cast<uint32_t>(cur)))) {
      LOG_WARN("failed replace seq node.", K(ret), K(len_), K(cap_), K(dig_len_), K(dig_cap_));
    } else {
      seq_id.assign_ptr(buf_, len_);
    }
  }
  return ret;
}

// just return origin seq id
void ObLobSeqIdGenerator::get_seq_id(ObString &seq_id)
{
  seq_id.assign_ptr(seq_id_.ptr(), seq_id_.length());
}

int ObLobSeqIdGenerator::init_digits()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed init digits, allocator is null.", K(ret));
  } else if (OB_ISNULL(digits_ = static_cast<uint32_t*>(allocator_->alloc(LOB_SEQ_DIGIT_DEFAULT_LEN * sizeof(uint32_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed init digits, allocator digits buf null.", K(ret));
  } else {
    MEMSET(digits_, 0, LOB_SEQ_DIGIT_DEFAULT_LEN * sizeof(uint32_t));
    dig_len_ = 0;
    dig_cap_ = LOB_SEQ_DIGIT_DEFAULT_LEN;
  }
  return ret;
}

int ObLobSeqIdGenerator::init_seq_buf()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed init seq buf, allocator is null.", K(ret));
  } else if (OB_ISNULL(buf_ = static_cast<char*>(allocator_->alloc(LOB_SEQ_DEFAULT_SEQ_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed init seq buf, allocator buf null.", K(ret));
  } else {
    MEMSET(buf_, 0, LOB_SEQ_DEFAULT_SEQ_BUF_LEN);
    buf_[0] = static_cast<char>(ObLobSeqId::LOB_SEQ_ID_VERSION);
    len_ = ObLobSeqId::LOB_SEQ_ID_VERSION_LEN;
    cap_ = LOB_SEQ_DEFAULT_SEQ_BUF_LEN;
  }
  return ret;
}

int ObLobSeqIdGenerator::add_digits(uint32_t val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(digits_) && OB_FAIL(init_digits())) {
    LOG_WARN("failed init digits, init digits failed.", K(ret));
  } else if (dig_len_ < dig_cap_) {
    digits_[dig_len_] = val;
    ++dig_len_;
  } else if (OB_FAIL(extend_digits())) {
    LOG_WARN("failed add digits, extend buf null.", K(ret));
  } else {
    ret = add_digits(val);
  }

  return ret;
}

int ObLobSeqIdGenerator::append_seq_buf(uint32_t val)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_) && OB_FAIL(init_seq_buf())) {
    LOG_WARN("failed add seq buf, init seq buf failed.", K(ret));
  } else if (len_ + sizeof(val) < cap_) {
    last_seq_buf_pos_ = len_;
    (void)ObLobSeqId::store32be(buf_ + len_, val);
    len_ += sizeof(val);
  } else if (OB_FAIL(extend_seq_buf())) {
    LOG_WARN("failed extend seq buf, extend buf null.", K(ret), K(len_), K(cap_));
  } else {
    ret = append_seq_buf(val);
  }

  return ret;
}

int ObLobSeqIdGenerator::replace_seq_buf_last_node(uint32_t val)
{
  int ret = OB_SUCCESS;
  if (last_seq_buf_pos_ + sizeof(val) < cap_) {
    if (dig_len_ == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed replace seq digits array, digits is empty.", K(ret), K(dig_len_), K(dig_cap_));
    } else {
      digits_[dig_len_ - 1] = val;
      (void)ObLobSeqId::store32be(buf_ + last_seq_buf_pos_, val);
      len_ = last_seq_buf_pos_ + sizeof(val);
    }
  } else if (OB_FAIL(extend_seq_buf())) {
    LOG_WARN("failed extend seq buf, extend buf null.", K(ret), K(len_), K(cap_));
  } else {
    ret = replace_seq_buf_last_node(val);
  }
  return ret;
}

int ObLobSeqIdGenerator::extend_digits()
{
  int ret = OB_SUCCESS;
  uint32_t extend_len = dig_cap_ * 2 * sizeof(uint32_t);
  uint32_t *extend_dig = NULL;
  if (OB_ISNULL(extend_dig = static_cast<uint32_t*>(allocator_->alloc(extend_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed extend dig buf, allocator buf null.", K(ret));
  } else {
    MEMCPY(extend_dig, digits_, dig_cap_ * sizeof(uint32_t));
    MEMSET(reinterpret_cast<char*>(extend_dig) + dig_cap_ * sizeof(uint32_t),
           0, dig_cap_ * sizeof(uint32_t));
    dig_cap_ = dig_cap_ * 2;
    allocator_->free(digits_);
    digits_ = extend_dig;
  }
  return ret;
}

int ObLobSeqIdGenerator::extend_seq_buf()
{
  int ret = OB_SUCCESS;
  uint32_t extend_len = cap_ * 2;
  char *extend_buf = NULL;
  if (OB_ISNULL(extend_buf = static_cast<char*>(allocator_->alloc(extend_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed init seq buf, allocator buf null.", K(ret));
  } else {
    MEMCPY(extend_buf, buf_, cap_);
    MEMSET(extend_buf + cap_, 0, cap_);
    cap_ = extend_len;
    allocator_->free(buf_);
    buf_ = extend_buf;
  }
  return ret;
}

int ObLobSeqIdGenerator::empty()
{
  int ret = 0;
  if (dig_len_ == 0) {
    ret = 1;
  }
  return ret;
}

int ObLobSeqIdGenerator::parse()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed parse seq_id, allocator is null.", K(ret));
  } else {
    ObString tmp_seq = seq_id_;
    size_t len = tmp_seq.length();

    if (len == 0) {
    } else if (len < ObLobSeqId::LOB_SEQ_ID_MIN_LEN
        || lob_seq_id_version(tmp_seq) != ObLobSeqId::LOB_SEQ_ID_VERSION
        || ((len - ObLobSeqId::LOB_SEQ_ID_VERSION_LEN) % sizeof(uint32_t)) > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail parse seq, internel unexpected.", K(ret), K(len));
    } else {
      const char *payload_ptr = lob_seq_id_payload_ptr(tmp_seq);
      const uint32_t ori_len = static_cast<uint32_t>(lob_seq_id_payload_len(tmp_seq) / sizeof(uint32_t));
      uint32_t cur_pos = 0;
      while (OB_SUCC(ret) && cur_pos < ori_len) {
        uint32_t val = ObLobSeqId::load32be(payload_ptr + sizeof(uint32_t) * cur_pos);
        if (OB_FAIL(add_digits(val))) {
          LOG_WARN("add_digits failed.", K(ret), K(val));
        } else if (OB_FAIL(append_seq_buf(val))) {
          LOG_WARN("append_seq_buf failed.", K(ret), K(val));
        } else {
          cur_pos++;
        }
      } // end while
    }
  }

  if (OB_SUCC(ret)) {
    parsed_ = true;
  }

  return ret;
}

int64_t ObLobSeqIdGenerator::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  ObString tmp_seq = seq_id_;
  const uint8_t seq_version = lob_seq_id_version(tmp_seq);
  uint32_t ori_len = static_cast<uint32_t>(lob_seq_id_payload_len(tmp_seq) / sizeof(uint32_t)); //TODO(yuanzhi.zy): check is len int32 enough
  uint32_t cur_pos = 0;
  common::databuff_printf(buf, buf_len, pos, "seq_id(version:%u):[", seq_version);
  while (cur_pos < ori_len && tmp_seq.ptr() != nullptr) {
    uint32_t val = ObLobSeqId::load32be(lob_seq_id_payload_ptr(tmp_seq) + sizeof(uint32_t) * cur_pos);
    common::databuff_printf(buf, buf_len, pos, "%u.", val);
    cur_pos++;
  } // end while
  common::databuff_printf(buf, buf_len, pos, "], ");
  oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(read_only), K_(parsed));

  common::databuff_printf(buf, buf_len, pos, ", seq_id_in_buff:[");
  ori_len = static_cast<uint32_t>((len_ > ObLobSeqId::LOB_SEQ_ID_VERSION_LEN)
      ? ((len_ - ObLobSeqId::LOB_SEQ_ID_VERSION_LEN) / sizeof(uint32_t)) : 0);
  cur_pos = 0;
  while (cur_pos < ori_len && buf_ != nullptr) {
    uint32_t val = ObLobSeqId::load32be(buf_ + ObLobSeqId::LOB_SEQ_ID_VERSION_LEN + sizeof(uint32_t) * cur_pos);
    common::databuff_printf(buf, buf_len, pos, "%u.", val);
    cur_pos++;
  } // end while
  common::databuff_printf(buf, buf_len, pos, "], ");
  oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(len), K_(cap));

  common::databuff_printf(buf, buf_len, pos, ", seq_id_in_digits:[");
  ori_len = dig_len_;
  cur_pos = 0;
  while (cur_pos < ori_len && digits_ != nullptr) {
    common::databuff_printf(buf, buf_len, pos, "%u.", digits_[cur_pos]);
    cur_pos++;
  } // end while
  common::databuff_printf(buf, buf_len, pos, "], ");
  oceanbase::common::databuff_print_kv(buf, buf_len, pos, K_(dig_len), K_(dig_cap));

  J_OBJ_END();
  return pos;
}

} // storage
} // oceanbase
