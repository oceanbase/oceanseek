/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "object/ob_object.h"
#include "storage/fts/ob_fts_struct.h"
#define USING_LOG_PREFIX STORAGE_FTS

#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/fts/ob_ft_token_processor.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace storage
{
/**
* -----------------------------------ObFTTokenProcessor-----------------------------------
*/
int ObFTTokenProcessor::init(const ObFTParserProperty &property,
                             const ObObjMeta &meta,
                             const ObProcessTokenFlag &flag,
                             ObFTTokenMap *token_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObFTTokenProcessor has been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == token_map)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid arguments", K(ret), KP(token_map));
  } else {
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(meta.get_type(), meta.get_collation_type());
    ObDatumCmpFuncType cmp_func = get_datum_cmp_func(meta, meta);
    token_meta_ = meta;
    token_map_ = token_map;
    non_stop_token_cnt_ = 0;
    flag_ = flag;

    if (OB_UNLIKELY(nullptr == basic_funcs || nullptr == cmp_func)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get basic funcs or cmp func",
          K(ret), K(token_meta_), KP(basic_funcs), KP(cmp_func));
    } else if (OB_UNLIKELY(nullptr == basic_funcs->default_hash_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the default hash is null", K(ret));
    } else {
      hash_func_ = basic_funcs->default_hash_;
      cmp_func_ = cmp_func;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObFTTokenProcessor::reset()
{
  if (OB_LIKELY(is_inited_)) {
    token_map_->destroy();
    token_meta_.reset();
    non_stop_token_cnt_ = 0;
    flag_.reset();
    hash_func_ = nullptr;
    cmp_func_ = nullptr;
    is_inited_ = false;
  }
}

void ObFTTokenProcessor::reuse()
{
  non_stop_token_cnt_ = 0;
}

int ObFTTokenProcessor::process_token(
    const bool need_pos_list,
    const char *token,
    const int64_t token_len,
    const int64_t position)
{
  int ret = OB_SUCCESS;
  ObFTToken src_token;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObFTTokenProcessor hasn't been initialized", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == token || 0 >= token_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the are invalid arguments", K(ret), KP(token), K(token_len));
  } else if (OB_FAIL(src_token.init(token, token_len, token_meta_, hash_func_, cmp_func_))) {
    LOG_WARN("fail to initialize src token", K(ret), K(token), K(token_len), K(token_meta_));
  } else if (OB_FAIL(groupby_token(need_pos_list, src_token, position))) {
    LOG_WARN("fail to groupby token", K(ret), K(src_token), K(position));
  } else {
    non_stop_token_cnt_ += 1;
    LOG_DEBUG("process non stop token", K(ret), KP(token), K(token_len), K(position), K(src_token));
  }
  return ret;
}

int ObFTTokenProcessor::groupby_token(const bool need_pos_list, const ObFTToken &token, const int64_t position)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(flag_.groupby_token())) {
    const int64_t token_len = token.get_token().get_string().length();
    ObFTTokenPair new_entry;
    // Keep the lookup result separate: the callback writes its update result to ret.
    int hash_ret = OB_SUCCESS;
    if (need_pos_list) {
      UpdateTokenCallBack update_token_func(scratch_allocator_, position, ret);
      hash_ret = token_map_->atomic_refactored(token, update_token_func);
      if (OB_HASH_NOT_EXIST == hash_ret && OB_FAIL(update_token_func(new_entry))) {
        LOG_WARN("failed to initialize fulltext token with position", K(ret), K(token_len), K(position));
      }
    } else {
      UpdateTokenWithoutPosListCallBack update_token_func(ret);
      hash_ret = token_map_->atomic_refactored(token, update_token_func);
      if (OB_HASH_NOT_EXIST == hash_ret && OB_FAIL(update_token_func(new_entry))) {
        LOG_WARN("failed to initialize fulltext token", K(ret), K(token_len));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      ObString owned_text;
      if (OB_FAIL(ob_write_string(scratch_allocator_, token.get_token().get_string(), owned_text))) {
        LOG_WARN("failed to copy new fulltext token", K(ret), K(token_len));
      } else if (OB_FAIL(new_entry.first.init(owned_text.ptr(), owned_text.length(), token_meta_, hash_func_, cmp_func_))) {
        LOG_WARN("failed to initialize owned fulltext token", K(ret));
      } else if (OB_FAIL(token_map_->set_refactored(new_entry.first, new_entry.second))) {
        LOG_WARN("failed to insert fulltext token", K(ret), K(token_len));
      }
      if (OB_FAIL(ret) && nullptr != owned_text.ptr()) {
        scratch_allocator_.free(owned_text.ptr());
      }
    } else if (OB_SUCCESS != hash_ret) {
      ret = hash_ret;
      LOG_WARN("failed to find fulltext token", K(hash_ret), K(token_len));
    }
  } else {
    // do nothing now
  }
  return ret;
}

int ObFTTokenProcessor::UpdateTokenCallBack::operator()(common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> &pair)
{
  int &ret = ret_;
  if (OB_FAIL(pair.second.update_one_position(allocator_, position_))) {
    LOG_WARN("Fail to update one position", K(ret), K(position_), K(pair.first), K(pair.second));
  }
  return ret;
}

int ObFTTokenProcessor::UpdateTokenWithoutPosListCallBack::operator()(common::hash::HashMapPair<ObFTToken, ObFTTokenInfo> &pair)
{
  int &ret = ret_;
  if (OB_FAIL(pair.second.update_without_pos_list())) {
    LOG_WARN("Fail to update without pos list", K(ret), K(pair.first), K(pair.second));
  }
  return ret;
}
} // end namespace storage
} // end namespace oceanbase
