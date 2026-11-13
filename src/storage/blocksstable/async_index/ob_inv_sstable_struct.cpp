/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObInvIdxStatsMeta::init_empty_for_write(const ObInvIdxStatsMeta &source)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!source.is_valid()
      || (source.is_search_text_index()
          && !is_supported_search_text_write_layout(
              source.has_relevance_params_, source.has_nested_key_)))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "inverted index meta does not support empty write", K(ret), K(source));
  } else {
    *this = source;
    reuse();
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObInvIdxStatsMeta,
    index_type_,
    has_relevance_params_,
    has_position_list_,
    has_nested_key_,
    doc_id_obj_meta_,
    min_doc_id_,
    max_doc_id_,
    total_doc_length_);

OB_DEF_SERIALIZE(ObInvSSTableMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(stats_);
  return ret;
}

OB_DEF_DESERIALIZE(ObInvSSTableMeta)
{
  int ret = OB_SUCCESS;
  reset();
  OB_UNIS_DECODE(stats_);
  is_inited_ = OB_SUCC(ret);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObInvSSTableMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(stats_);
  return len;
}

OB_DEF_SERIALIZE(ObInvMacroMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(stats_);
  return ret;
}

OB_DEF_DESERIALIZE(ObInvMacroMeta)
{
  int ret = OB_SUCCESS;
  reset();
  OB_UNIS_DECODE(stats_);
  is_inited_ = OB_SUCC(ret);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObInvMacroMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(stats_);
  return len;
}


int ObInvIdxStatsMeta::assign(const ObInvIdxStatsMeta &other)
{
  *this = other;
  return OB_SUCCESS;
}

bool ObInvIdxStatsMeta::is_reset() const
{
  return INV_IDX_TYPE_INVALID == index_type_ && !has_relevance_params_
      && !has_position_list_ && !has_nested_key_ && doc_id_obj_meta_ == ObObjMeta()
      && UINT64_MAX == min_doc_id_ && 0 == max_doc_id_ && 0 == total_doc_length_;
}

bool ObInvIdxStatsMeta::has_same_layout(const ObInvIdxStatsMeta &other) const
{
  return index_type_ == other.index_type_
      && has_relevance_params_ == other.has_relevance_params_
      && has_position_list_ == other.has_position_list_
      && has_nested_key_ == other.has_nested_key_
      && doc_id_obj_meta_ == other.doc_id_obj_meta_;
}

int ObInvIdxStatsMeta::merge(const ObInvIdxStatsMeta &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted stats to merge", K(ret), K(other));
  } else if (is_reset()) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("failed to assign inverted stats", K(ret), K(other));
    }
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target inverted stats", K(ret), KPC(this));
  } else if (!has_same_layout(other)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inconsistent inverted stats layouts", K(ret), KPC(this), K(other));
  } else if (!other.is_empty_range()) {
    if (OB_FAIL(update(other.min_doc_id_, other.max_doc_id_, other.total_doc_length_))) {
      LOG_WARN("failed to merge inverted stats", K(ret));
    }
  }
  return ret;
}

int ObInvSSTableMeta::init(const ObInvIdxStatsMeta &stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inverted sstable metadata already initialized", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted stats for initialization", K(ret), K(stats));
  } else if (OB_FAIL(stats_.assign(stats))) {
    LOG_WARN("failed to initialize inverted sstable metadata", K(ret), K(stats));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInvMacroMeta::init(const ObInvIdxStatsMeta &stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inverted macro metadata already initialized", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted stats for initialization", K(ret), K(stats));
  } else if (OB_FAIL(stats_.assign(stats))) {
    LOG_WARN("failed to initialize inverted macro metadata", K(ret), K(stats));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInvSSTableMeta::merge(const ObInvSSTableMeta &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted sstable meta to merge", K(ret), K(other));
  } else if (!is_inited_) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("failed to initialize inverted sstable meta from partial", K(ret));
    }
  } else if (OB_FAIL(stats_.merge(other.stats_))) {
    LOG_WARN("failed to merge inverted sstable metadata", K(ret), KPC(this), K(other));
  }
  return ret;
}

int ObInvSSTableMeta::check_merge_macro(const ObInvMacroMeta &macro_meta) const
{
  int ret = OB_SUCCESS;
  ObInvSSTableMeta tmp;
  // The writer may flush pending rows after this check. Validate without retaining
  // the prospective result, and merge into the current SSTable value when committed.
  if (OB_FAIL(tmp.assign(*this))) {
    LOG_WARN("failed to copy inverted sstable meta for validation", K(ret));
  } else if (OB_FAIL(tmp.merge_macro(macro_meta))) {
    LOG_WARN("incompatible inverted macro metadata", K(ret), KPC(this), K(macro_meta));
  }
  return ret;
}

int ObInvSSTableMeta::merge_macro(const ObInvMacroMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted macro metadata to aggregate", K(ret), K(macro_meta));
  } else if (!is_inited_) {
    if (OB_FAIL(init(macro_meta.get_stats()))) {
      LOG_WARN("failed to initialize sstable metadata from macro", K(ret), K(macro_meta));
    }
  } else if (OB_FAIL(stats_.merge(macro_meta.get_stats()))) {
    LOG_WARN("failed to aggregate macro metadata into sstable", K(ret), KPC(this), K(macro_meta));
  }
  return ret;
}

int ObInvMacroMeta::merge(const ObInvMacroMeta &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted macro metadata to merge", K(ret), K(other));
  } else if (!is_inited_) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("failed to initialize inverted macro metadata from partial", K(ret));
    }
  } else if (OB_FAIL(stats_.merge(other.stats_))) {
    LOG_WARN("failed to merge inverted macro metadata", K(ret), KPC(this), K(other));
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
