/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/micro_cache/ob_ss_la_micro_key_manager.h"
#include "lib/ob_define.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common::hash;

ObSSLAMicroKeyManager::ObSSLAMicroKeyManager()
  : is_inited_(false), la_micro_key_set_(), is_stop_record_la_micro_key_(false)
{}

ObSSLAMicroKeyManager::~ObSSLAMicroKeyManager()
{
  destroy();
}

int ObSSLAMicroKeyManager::init()
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_BUCKET_NUM = 10001;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already init", KR(ret));
  } else if (OB_FAIL(la_micro_key_set_.create(DEFAULT_BUCKET_NUM, ObMemAttr(MTL_ID(), "LSPrewarmSet")))) {
    LOG_WARN("fail to create set", KR(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succ to init ss latest access micro key manager", K_(is_inited));
  }
  return ret;
}

void ObSSLAMicroKeyManager::destroy()
{
  is_inited_ = false;
  la_micro_key_set_.destroy();
  is_stop_record_la_micro_key_ = true;
}

void ObSSLAMicroKeyManager::clear()
{
  la_micro_key_set_.clear();
}

int ObSSLAMicroKeyManager::push_la_micro_key_to_hashset(const ObSSMicroPrewarmMeta &micro_prewarm_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_filtered = false;
  uint64_t tablet_id = 0;
  const uint64_t cur_us = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_prewarm_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_prewarm_meta));
  } else if (OB_UNLIKELY(is_stop_record_la_micro_key_)) {
    /// do nothing, stop record latest access micro key
  } else if (OB_FAIL(filter_micro_key_by_object_type(micro_prewarm_meta, is_filtered))) {
    LOG_WARN("fail to filter ss micro block cache key by object type", KR(ret), K(micro_prewarm_meta));
  } else if (is_filtered) {
    // do nothing, PRIVATE_MACRO need filter, only prewarm SHARED_MACRO
  } else if (la_micro_key_set_.size() > MAX_MICRO_BLOCK_KEY_SET_CAPACITY) {
    // do nothing, latest_access_micro_key_set can noly hold 10w elements, others throw away
    // TODO 改成队列式的丢弃，新来的key存入，丢弃旧的key @xiaotao.ht
  } else if (OB_FAIL(la_micro_key_set_.set_refactored(micro_prewarm_meta, false/*overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_WARN("fail to set refactored", KR(ret), K(micro_prewarm_meta));
    }
  }
  LOG_TRACE("latest access micro key set size", KR(ret), "micro_keys_size", la_micro_key_set_.size());
  return ret;
}

int ObSSLAMicroKeyManager::get_batch_micro_keys(ObLS *ls, ObIArray<ObSSMicroPrewarmMeta> &infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls));
  } else {
    ObGetMicroKeyOp get_micro_key_op(infos, ls, *this);
    if (OB_FAIL(la_micro_key_set_.foreach_refactored(get_micro_key_op))) {
      LOG_WARN("fail to get micro keys", KR(ret));
    } else if (OB_FAIL(batch_erase_micro_keys(get_micro_key_op.keys_))) {
      LOG_WARN("fail to batch erase micro keys", KR(ret));
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::ObGetMicroKeyOp::operator()(
    HashSetTypes<ObSSMicroPrewarmMeta>::pair_type &pair)
{
  int ret = OB_SUCCESS;
  const ObSSMicroPrewarmMeta &micro_prewarm_meta = pair.first;
  bool is_filtered = true;
  if (OB_FAIL(la_micro_key_manager_.filter_micro_key_by_tablet_id(ls_, micro_prewarm_meta, is_filtered))) {
    LOG_WARN("fail to filter", KR(ret), K(micro_prewarm_meta));
  } else if (is_filtered) {
    // do nothing, this ObSSMicroPrewarmMeta do not belong to this ls
  } else if (OB_FAIL(keys_.push_back(micro_prewarm_meta))) {
    LOG_WARN("fail to push back", KR(ret), K(micro_prewarm_meta));
  }
  return ret;
}

int ObSSLAMicroKeyManager::batch_erase_micro_keys(const ObIArray<ObSSMicroPrewarmMeta> &infos)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const int64_t micro_cnt = infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); i++) {
      if (OB_FAIL(la_micro_key_set_.erase_refactored(infos.at(i)))) {
        LOG_WARN("fail to erase", KR(ret), K(i), K(micro_cnt), "micro_prewarm_meta", infos.at(i));
      }
      ret = OB_SUCCESS; // ignore ret, and go on erasing next key
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::filter_micro_key_by_object_type(
    const ObSSMicroPrewarmMeta &micro_prewarm_meta,
    bool &is_filtered)
{
  int ret = OB_SUCCESS;
  is_filtered = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!micro_prewarm_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_prewarm_meta));
  } else if (ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE == micro_prewarm_meta.micro_key_.mode_) {
    const MacroBlockId &macro_id = micro_prewarm_meta.micro_key_.get_micro_id().macro_id_;
    if (macro_id.is_shared_data_or_meta()) { // only prewarm shared macro
      is_filtered = false;
    } else {
      is_filtered = true;
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::filter_micro_key_by_tablet_id(
    ObLS *ls,
    const ObSSMicroPrewarmMeta &micro_prewarm_meta,
    bool &is_filter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((nullptr == ls) || !micro_prewarm_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(micro_prewarm_meta));
  } else if (OB_FAIL(is_tablet_id_need_filter(ls, micro_prewarm_meta.effective_tablet_id_, is_filter))) {
    LOG_WARN("fail to judge is tablet id need filter", KR(ret), K(micro_prewarm_meta));
  }
  return ret;
}

int ObSSLAMicroKeyManager::is_tablet_id_need_filter(
    ObLS *ls, 
    const uint64_t tablet_id,
    bool &is_filter)
{
  int ret = OB_SUCCESS;
  is_filter = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((nullptr == ls) || (ObTabletID::INVALID_TABLET_ID == tablet_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(tablet_id));
  } else {
    bool is_exist = false;
    ObLSTabletService *ls_tablet_svr = nullptr;
    if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_tablet_svr is nullptr", KR(ret), KP(ls_tablet_svr));
    } else if (OB_FAIL(ls_tablet_svr->is_tablet_exist(ObTabletID(tablet_id), is_exist))) {
      LOG_WARN("fail to judge is tablet exist", KR(ret), K(tablet_id));
    } else if (is_exist) {
      is_filter = false;
    }
  }
  return ret;
}

int ObSSLAMicroKeyManager::check_stop_record_la_micro_key()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else {
    const bool ori_stop_flag = is_stop_record_la_micro_key_;
    const bool enable_ss_replica_prewarm = false;
    is_stop_record_la_micro_key_ = !enable_ss_replica_prewarm;
    if (is_stop_record_la_micro_key_) {
      la_micro_key_set_.clear();
    }
    if (ori_stop_flag != is_stop_record_la_micro_key_) {
      FLOG_INFO("ss replica prewarm is disabled", K(enable_ss_replica_prewarm), K_(is_stop_record_la_micro_key));
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
