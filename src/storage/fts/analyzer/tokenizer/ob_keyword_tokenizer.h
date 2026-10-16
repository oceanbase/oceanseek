/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_KEYWORD_TOKENIZER_H_
#define OB_KEYWORD_TOKENIZER_H_

#include "storage/fts/analyzer/ob_analyzer.h"

namespace oceanbase
{
namespace storage
{

struct ObKeywordTokenizerSpec : public ObTokenizerSpec
{
  ObKeywordTokenizerSpec()
    : ObTokenizerSpec(ObTokenizerType::TOKENIZER_TYPE_KEYWORD)
  {}
};

class ObKeywordTokenizer : public ObITokenizer
{
public:
  ObKeywordTokenizer();
  ~ObKeywordTokenizer();
  int init(const ObTokenizerSpec &spec, common::ObIAllocator &alloc) override;
  int set_input(const char *text, int64_t text_len, ObCollationType coll_type) override;
  int get_next_token(ObTokenAttr &token) override;
  void reset() override;
protected:
  common::ObString text_;
  bool emitted_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObKeywordTokenizer);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_KEYWORD_TOKENIZER_H_
