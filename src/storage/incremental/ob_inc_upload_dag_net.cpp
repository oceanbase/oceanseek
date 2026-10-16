/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_inc_upload_dag_net.h"
#include "ob_inc_ss_dag_util.h"
#include "ob_inc_replace_upload_task.h"
#include "share/config/ob_server_config.h"
#include "share/scheduler/ob_dag_scheduler_config.h"

namespace oceanbase
{
namespace storage
{

/****************** ObIncUploadDagNetParam ******************/
bool ObIncUploadDagNetParam::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && transfer_scn_.is_valid();
}

/****************** ObIncUploadDagNetCtx ******************/
ObIncUploadDagNetCtx::ObIncUploadDagNetCtx()
    : ObIHADagNetCtx(),
      ls_id_(),
      tablet_id_(),
      transfer_scn_(),
      upload_type_(ObIncUploadType::MINOR),
      is_replace_(false),
      inc_upload_param_(),
      replace_scn_range_(),
      replace_ss_clog_ckpt_scn_(share::SCN::min_scn())
{
  replace_scn_range_.reset();
}

bool ObIncUploadDagNetCtx::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && transfer_scn_.is_valid();
}

int ObIncUploadDagNetCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ret = databuff_printf(buf, buf_len, pos, "ObIncUploadDagNetCtx : ls_id = ");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ls_id_.id());
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tablet_id = ");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, tablet_id_.id());
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", transfer_scn = ");
  OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, transfer_scn_);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to fill comment", K(ret), K_(ls_id), K_(tablet_id), K_(transfer_scn));
  }
  return ret;
}

/****************** ObIncUploadDagNet ******************/
ObIncUploadDagNet::ObIncUploadDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_SS_INC_UPLOAD),
      is_inited_(false),
      ctx_()
{
}

int ObIncUploadDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObIncUploadDagNetParam *init_param = static_cast<const ObIncUploadDagNetParam *>(param);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inc upload dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else {
    ctx_.ls_id_ = init_param->ls_id_;
    ctx_.tablet_id_ = init_param->tablet_id_;
    ctx_.transfer_scn_ = init_param->transfer_scn_;
    ctx_.upload_type_ = init_param->upload_type_;
    ctx_.is_replace_ = init_param->is_replace_;
    ctx_.inc_upload_param_ = init_param->inc_upload_param_;
    is_inited_ = true;
  }
  return ret;
}

bool ObIncUploadDagNet::is_valid() const
{
  return ctx_.is_valid();
}

uint64_t ObIncUploadDagNet::hash() const
{
  uint64_t hash_value = ctx_.tablet_id_.hash();
  hash_value = common::murmurhash(&ctx_.ls_id_, sizeof(ctx_.ls_id_), hash_value);
  hash_value = common::murmurhash(&ctx_.transfer_scn_, sizeof(ctx_.transfer_scn_), hash_value);
  hash_value = common::murmurhash(&ctx_.upload_type_, sizeof(ctx_.upload_type_), hash_value);
  return hash_value;
}

bool ObIncUploadDagNet::operator==(const share::ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObIncUploadDagNet &other_dag_net = static_cast<const ObIncUploadDagNet &>(other);
    is_same = ctx_.ls_id_ == other_dag_net.ctx_.ls_id_
              && ctx_.tablet_id_ == other_dag_net.ctx_.tablet_id_
              && ctx_.transfer_scn_ == other_dag_net.ctx_.transfer_scn_
              && ctx_.upload_type_ == other_dag_net.ctx_.upload_type_;
  }
  return is_same;
}

int ObIncUploadDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inc upload dag net do not init", K(ret));
  } else if (ctx_.is_replace_upload()) {
    ret = start_running_for_replace_upload_();
  } else {
    ret = start_running_for_inc_upload_();
  }
  return ret;
}

int ObIncUploadDagNet::start_running_for_replace_upload_()
{
  int ret = OB_SUCCESS;
  ObIncReplaceUploadParam param;

  // prepare_dag -> complete_dag
  if (OB_FAIL(param.init(ctx_.ls_id_, ctx_.tablet_id_, ctx_.transfer_scn_))) {
    LOG_WARN("init replace upload param fail", K(ret), K(ctx_));
  } else if (OB_FAIL(ObIncSSDagUtil::generate_replace_upload_prepare_dags(
      this, nullptr /*parent_dag*/, param, false /*is_ha*/, get_emergency()))) {
    LOG_WARN("generate replace upload prepare dags fail", K(ret), K(ctx_), K(param));
  } else {
    LOG_INFO("[IncUpload] start replace upload dag net", "dag_net_id", get_dag_id(),
        "is_emergency", get_emergency(), K_(ctx));
  }
  return ret;
}

int ObIncUploadDagNet::start_running_for_inc_upload_()
{
  int ret = OB_SUCCESS;
  ObIncSSTableUploadDag *upload_dag = nullptr;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObDagId upload_dag_id;

  if (OB_FAIL(scheduler->create_dag(&ctx_.inc_upload_param_, upload_dag))) {
    LOG_WARN("create inc upload dag fail", K(ret), K(ctx_));
  } else if (FALSE_IT(upload_dag_id.init(GCONF.self_addr_))) {
  } else if (OB_FAIL(upload_dag->set_dag_id(upload_dag_id))) {
    LOG_WARN("set inc upload dag id fail", K(ret), K(upload_dag_id));
  } else if (OB_FAIL(add_dag_into_dag_net(*upload_dag))) {
    LOG_WARN("add inc upload dag into dag net fail", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(upload_dag, get_emergency()))) {
    LOG_WARN("add inc upload dag fail", K(ret));
  } else {
    LOG_INFO("[IncUpload] start inc upload dag net", "dag_net_id", get_dag_id(),
        K(upload_dag_id), K_(ctx));
    upload_dag = nullptr;
  }

  if (OB_NOT_NULL(upload_dag) && OB_NOT_NULL(scheduler)) {
    erase_dag_from_dag_net(*upload_dag);
    scheduler->free_dag(*upload_dag);
    upload_dag = nullptr;
  }
  return ret;
}

int ObIncUploadDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inc upload dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObIncUploadDagNet : ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.ls_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tablet_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.tablet_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", transfer_scn = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.transfer_scn_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), K(ctx_));
    }
  }
  return ret;
}

int ObIncUploadDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("inc upload dag net do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_printf(buf, buf_len, pos, "ObIncUploadDagNet : ls_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.ls_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", tablet_id = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.tablet_id_.id());
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ", transfer_scn = ");
    OB_SUCCESS != ret ? : ret = databuff_printf(buf, buf_len, pos, ctx_.transfer_scn_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill dag net key", K(ret), K(ctx_));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
