/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ik/ob_ik_arbitrator.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ik/ob_ik_processor.h"
#include "storage/fts/ik/ob_ik_token.h"
#include "storage/fts/ob_ik_ft_parser.h"

namespace oceanbase
{
namespace storage
{
int ObIKArbitrator::process(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  ObFastList<ObIKToken, HANDLE_SIZE_LIMIT> &tokens = ctx.token_list().tokens();
  ObIKTokenChain *chain_need_arbitrate = nullptr;
  bool use_smart = ctx.is_smart();
  if (OB_ISNULL(chain_need_arbitrate = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed");
  } else {
    while (OB_SUCC(ret) && !tokens.empty()) {
      ObIKToken token = tokens.get_first();
      bool is_add = false;
      if (OB_FAIL(ctx.check_status())) {
        LOG_WARN("Worker interrupted while building IK conflict chains", K(ret));
      } else if (OB_FAIL(tokens.pop_front())) {
        LOG_WARN("pop_front failed", K(ret));
      } else if (OB_FAIL(chain_need_arbitrate->add_token_if_conflict(token, is_add))) {
        LOG_WARN("add token if conflict failed", K(ret));
      } else if (!is_add) {
        ObFTLightSortList::CellIter iter = chain_need_arbitrate->list().tokens().begin();
        ObIKTokenChain *judge_result = nullptr;
        if (chain_need_arbitrate->list().tokens().size() == 1 || !use_smart) {
          if (OB_FAIL(add_chain(chain_need_arbitrate))) {
            LOG_WARN("Add a chain failed", K(ret));
          } else {
            // add ok
          }
        } else if (OB_FAIL(optimize(ctx,
                                    chain_need_arbitrate,
                                    iter,
                                    chain_need_arbitrate->offset_len(),
                                    judge_result))) {
          LOG_WARN("Failed to optimize chain.", K(ret));
        } else if (OB_FAIL(add_chain(judge_result))) {
          LOG_WARN("Failed to add chain", K(ret));
        } else {
          // add best chain and delete origin chain
          OB_DELETEx(ObIKTokenChain, &alloc_, chain_need_arbitrate);
        }

        // start a new chain
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(chain_need_arbitrate = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc memory failed");
        } else if (OB_FAIL(chain_need_arbitrate->add_token_if_conflict(token, is_add))) {
          LOG_WARN("add token if conflict failed", K(ret));
        } else {
          // Add first, it should be added.
        }
      } else {
        // still use same cross chain
      }
    }

    // handle last chain
    ObIKTokenChain *judge_result = nullptr;
    if (OB_FAIL(ret)) {
    } else if (chain_need_arbitrate->list().tokens().size() == 1 || !use_smart) {
      if (OB_FAIL(add_chain(chain_need_arbitrate))) {
        LOG_WARN("Failed to add last chain", K(ret));
      }
    } else if (OB_FAIL(optimize(ctx,
                                chain_need_arbitrate,
                                chain_need_arbitrate->list().tokens().begin(),
                                chain_need_arbitrate->offset_len(),
                                judge_result))) {
    } else if (OB_FAIL(add_chain(judge_result))) {
      OB_DELETEx(ObIKTokenChain, &alloc_, judge_result);
      LOG_WARN("Failed to add last chain", K(ret));
    } else {
    }
  }

  // if failed, free chain_need_arbitrate
  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKTokenChain, &alloc_, chain_need_arbitrate);
  }

  return ret;
}

int ObIKArbitrator::output_result(TokenizeContext &ctx)
{
  int ret = OB_SUCCESS;

  int64_t char_len = 0;
  ObIKTokenChain *chain = nullptr;
  const int64_t buffer_start_cursor = ctx.get_buffer_start_cursor();
  const int64_t buffer_end_cursor = ctx.get_buffer_end_cursor();
  for (int64_t current = buffer_start_cursor; OB_SUCC(ret) && current < buffer_end_cursor;) {
    ObFTCharUtil::CharType type = ObFTCharUtil::CharType::USELESS;
    if (OB_FAIL(ObFTCharUtil::classify_first_valid_char(ctx.get_cs_type(),
                                                        ctx.fulltext() + current,
                                                        ctx.fulltext_len() - current,
                                                        ctx.well_formed_len_fn(),
                                                        ctx.get_cs(),
                                                        char_len,
                                                        type))) {
      LOG_WARN("Failed to classify first valid char", K(ret));
    } else if (ObFTCharUtil::CharType::USELESS == type) {
      if (OB_FAIL(ctx.check_status())) {
        LOG_WARN("Worker interrupted during IK result output", K(ret));
      } else {
        current += char_len; // ignore useless char
      }
    } else if (OB_FAIL(chains_.get_refactored(current, chain)) && OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("Failed to find in chain map", K(ret));
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(ctx.check_status())) {
        LOG_WARN("Worker interrupted during IK result output", K(ret));
      } else if (!keep_single()) {
        // ignore single token
      } else {
        // output single token
        ObIKToken token;
        token.offset_ = current;
        token.length_ = char_len;
        token.ptr_ = ctx.fulltext();
        token.char_cnt_ = 1;
        bool is_ignore = false;
        if (ObFTCharUtil::CharType::CHINESE == type) {
          token.type_ = ObIKTokenType::IK_CHINESE_TOKEN;
          if (OB_FAIL(ctx.get_results().push_back(token))) {
            LOG_WARN("Failed to output result to ctx", K(ret));
          }
        } else if (ObFTCharUtil::CharType::OTHER_CJK == type) {
          if (OB_FAIL(ObFTCharUtil::is_ignore_single_cjk(ctx.collation(),
                                                         ctx.fulltext() + current,
                                                         char_len,
                                                         is_ignore))) {
            LOG_WARN("Failed to check ignore", K(ret));
          } else if (!is_ignore && !FALSE_IT(token.type_ = ObIKTokenType::IK_OTHER_CJK_TOKEN)
                     && OB_FAIL(ctx.get_results().push_back(token))) {
            LOG_WARN("Failed to add token to ctx result", K(ret));
          } else {
            // ignore
          }
        }
      }
      if (OB_SUCC(ret)) {
        current += char_len;
      }
    } else {
      for (ObList<ObIKToken, ObIAllocator>::const_iterator iter = chain->list().tokens().begin();
           OB_SUCC(ret) && iter != chain->list().tokens().end();
           iter++) {
        const ObIKToken &token = *iter;
        if (OB_FAIL(ctx.check_status())) {
          LOG_WARN("Worker interrupted while outputting IK chain tokens", K(ret));
        } else if (!keep_single()) {
          current = token.offset_;
        } else {
          // output single-token between two token
          while (OB_SUCC(ret) && current < token.offset_) {
            if (OB_FAIL(ctx.check_status())) {
              LOG_WARN("Worker interrupted while outputting IK single characters", K(ret));
            } else if (OB_FAIL(ObFTCharUtil::classify_first_valid_char(ctx.get_cs_type(),
                                                                ctx.fulltext() + current,
                                                                ctx.fulltext_len() - current,
                                                                ctx.well_formed_len_fn(),
                                                                ctx.get_cs(),
                                                                char_len,
                                                                type))) {
              LOG_WARN("Failed to classify first valid char", K(ret));
            } else if (ObFTCharUtil::CharType::USELESS == type) {
              current += char_len; // ignore useless char
            } else {
              ObIKToken token;
              token.offset_ = current;
              token.length_ = char_len;
              token.ptr_ = ctx.fulltext();
              token.char_cnt_ = 1;
              bool is_ignore = false;
              if (ObFTCharUtil::CharType::CHINESE == type) {
                token.type_ = ObIKTokenType::IK_CHINESE_TOKEN;
                if (OB_FAIL(ctx.get_results().push_back(token))) {
                  LOG_WARN("Failed to add token to ctx result", K(ret));
                }
              } else if (ObFTCharUtil::CharType::OTHER_CJK == type) {
                if (OB_FAIL(ObFTCharUtil::is_ignore_single_cjk(ctx.collation(),
                                                               ctx.fulltext() + current,
                                                               char_len,
                                                               is_ignore))) {
                  LOG_WARN("Failed to check ignore", K(ret));
                } else if (!is_ignore) {
                  token.type_ = ObIKTokenType::IK_OTHER_CJK_TOKEN;
                  if (OB_FAIL(ctx.get_results().push_back(token))) {
                    LOG_WARN("Failed to add token to ctx result", K(ret));
                  }
                }
              }
              current += char_len;
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ctx.get_results().push_back(token))) {
          LOG_WARN("Failed to add token to ctx result", K(ret));
        } else {
          current = token.offset_ + token.length_;
        }
      }
    }
  }

  return ret;
}

int ObIKArbitrator::optimize(TokenizeContext &ctx,
                             ObIKTokenChain *chain,
                             ObFTLightSortList::CellIter iter,
                             int64_t chain_span,
                             ObIKTokenChain *&best)
{
  int ret = OB_SUCCESS;

  ObIKTokenChain *option = nullptr;
  ObList<ObFTLightSortList::CellIter, ObIAllocator> conflict_stack(alloc_);
  const bool need_backtrack = nullptr != chain
      && chain->list().tokens().size() <= MAX_BACKTRACK_TOKEN_COUNT;

  if (OB_ISNULL(option = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed");
  } else if (OB_FAIL(try_add_next_words(ctx, chain, iter, option, need_backtrack, conflict_stack))) {
    LOG_WARN("Failed to add chain and record conflict token.", K(ret));
  } else if (!need_backtrack) {
    best = option;
    option = nullptr;
  } else if (OB_ISNULL(best = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Alloc memory failed");
  } else if (OB_FAIL(best->copy(option))) {
    LOG_WARN("Copy best option failed", K(ret));
  } else {
    // A single token covering the chain is optimal by coverage and token count.
    while (OB_SUCC(ret) && !conflict_stack.empty()
        && (best->payload() != chain_span || best->list().tokens().size() != 1)) {
      ObFTLightSortList::CellIter iter = conflict_stack.get_last();
      if (OB_FAIL(conflict_stack.pop_back())) {
        LOG_WARN("Failed to pop IK conflict candidate", K(ret));
      } else if (OB_FAIL(remove_conflict(*iter, option))) {
        LOG_WARN("Failed to remove conflict", K(ret));
      } else if (OB_FAIL(try_add_next_words(ctx, chain, iter, option, false, conflict_stack))) {
        LOG_WARN("Failed to add next words", K(ret));
      } else if (option->better_than(*best)) {
        OB_DELETEx(ObIKTokenChain, &alloc_, best);
        if (OB_ISNULL(best = OB_NEWx(ObIKTokenChain, &alloc_, alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("Alloc memory failed");
        } else if (OB_FAIL(best->copy(option))) {
          LOG_WARN("Copy best option failed", K(ret));
        } else {
          // best option is copied
        }
      } else {
        // ignore worse chain
      }
    }
  }

  // any way, free option
  OB_DELETEx(ObIKTokenChain, &alloc_, option);

  if (OB_FAIL(ret)) {
    OB_DELETEx(ObIKTokenChain, &alloc_, best);
  }
  return ret;
}

int ObIKArbitrator::try_add_next_words(TokenizeContext &ctx,
                                       ObIKTokenChain *chain,
                                       ObFTLightSortList::CellIter iter,
                                       ObIKTokenChain *option,
                                       bool need_conflict,
                                       ObList<ObFTLightSortList::CellIter, ObIAllocator> &conflict_stack)
{
  int ret = OB_SUCCESS;
  int64_t last_token_start = 0;
  if (OB_ISNULL(chain) || OB_ISNULL(option)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("iter is null", K(ret));
  } else if (!need_conflict && !chain->list().is_empty()) {
    last_token_start = chain->list().tokens().get_last().offset_;
  }

  while (OB_SUCC(ret) && iter != chain->list().tokens().end()) {
    bool is_add = false;
    if (OB_FAIL(ctx.check_status())) {
      LOG_WARN("Worker interrupted during IK candidate scanning", K(ret));
    } else if (OB_FAIL(option->add_token_if_no_conflict(*iter, is_add))) {
      LOG_WARN("Failed to add token", K(ret));
    } else if (!is_add && need_conflict && OB_FAIL(conflict_stack.push_back(iter))) {
      LOG_WARN("push_back failed", K(ret));
    } else if (is_add && !need_conflict && option->max_offset() > last_token_start) {
      // All remaining candidates start inside the selected path.
      iter = chain->list().tokens().end();
    } else {
      iter++;
    }
  }

  return ret;
}

int ObIKArbitrator::remove_conflict(const ObIKToken &token, ObIKTokenChain *option)
{
  int ret = OB_SUCCESS;
  ObIKToken tmp;
  while (OB_SUCC(ret) && option->check_conflict(token)) {
    if (OB_FAIL(option->pop_back(tmp))) {
      LOG_WARN("Failed to pop back token", K(ret));
    }
  }
  return ret;
}

int ObIKArbitrator::prepare()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chains_.create(HANDLE_SIZE_LIMIT * 2L, ObMemAttr(MTL_ID(), "ik_arb_chains")))) {
    LOG_WARN("create chain map failed", K(ret));
  }
  return ret;
}

ObIKArbitrator::ObIKArbitrator() : alloc_(lib::ObMemAttr(MTL_ID(), "ik_arbitrator")) {}

int ObIKArbitrator::add_chain(ObIKTokenChain *chain)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(chain)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid chain argument", K(ret));
  } else if (chain->list().is_empty()) {
    // no need to add empty chain
  } else if (OB_FAIL(chains_.set_refactored(chain->min_offset(), chain))) {
    LOG_WARN("Failed to add chain to map", K(ret));
  }
  return ret;
}

ObIKArbitrator::~ObIKArbitrator()
{
  chains_.destroy();
  alloc_.reset();
}

void ObIKArbitrator::reuse()
{
  chains_.reuse();
  alloc_.reset_remain_one_page();
}
} // namespace storage
} // namespace oceanbase