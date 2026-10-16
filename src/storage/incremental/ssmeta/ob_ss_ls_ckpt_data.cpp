/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_data.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;

namespace storage
{

int ObSSLsCkptData::add_tablet_meta_key(const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet meta macro id", KR(ret), K(macro_id));
  } else if (OB_FAIL(tablet_meta_keys_.push_back(macro_id))) {
    LOG_WARN("fail to push back tablet meta macro id", KR(ret), K(macro_id));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObSSLsCkptData, // FARM COMPAT WHITELIST
                    ls_meta_,  
                    ckpt_scn_, 
                    ckpt_ts_,
                    tablet_meta_keys_,
                    reserved_meta_scn_);

} // namespace storage
} // namespace oceanbase
