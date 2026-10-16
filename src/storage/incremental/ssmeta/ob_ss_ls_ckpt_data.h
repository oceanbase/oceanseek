/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SS_LS_CKPT_DATA_H_
#define OCEANBASE_STORAGE_OB_SS_LS_CKPT_DATA_H_

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"

namespace oceanbase
{
namespace storage
{

// Self-contained checkpoint payload for sys-tenant LS.
class ObSSLsCkptData
{
  OB_UNIS_VERSION_V(1);
public:
  ObSSLsCkptData()
    : ls_meta_(),
      ckpt_scn_(),
      ckpt_ts_(0),
      tablet_meta_keys_(),
      reserved_meta_scn_()
  {}
  ~ObSSLsCkptData() { reset(); }

  void reset()
  {
    ls_meta_.reset();
    ckpt_scn_.reset();
    ckpt_ts_ = 0;
    tablet_meta_keys_.reset();
    reserved_meta_scn_.reset();
  }

  int add_tablet_meta_key(const blocksstable::MacroBlockId &macro_id);

  bool is_valid() const
  {
    return ls_meta_.is_valid()
           && ckpt_scn_.is_valid()
           && ckpt_ts_ > 0
           && reserved_meta_scn_.is_valid()
           && reserved_meta_scn_ <= ckpt_scn_;
  }
  uint64_t get_reserved_meta_scn_val() const
  {
    return reserved_meta_scn_.get_val_for_inner_table_field();
  }

  TO_STRING_KV(K_(ls_meta), K_(ckpt_scn), K_(ckpt_ts),
    "tablet_meta_keys_count", tablet_meta_keys_.count(), K_(reserved_meta_scn));

public:
  ObSSLSMeta ls_meta_;
  // ls_ckpt protocol meta_scn
  share::SCN ckpt_scn_;
  int64_t ckpt_ts_;
  common::ObSEArray<blocksstable::MacroBlockId, 64> tablet_meta_keys_;
  // reserved_meta_scn which prevent GC
  share::SCN reserved_meta_scn_;

  DISALLOW_COPY_AND_ASSIGN(ObSSLsCkptData);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_LS_CKPT_DATA_H_
