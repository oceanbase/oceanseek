/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_CHECKPOINT_ATOMIC_PROTOCOL_DEFINE_
#define OCEANBASE_CHECKPOINT_ATOMIC_PROTOCOL_DEFINE_


#include "ob_atomic_protocol_define.h"
#include "ob_atomic_protocol.h"
#include "ob_lock_atomic_protocol.h"


namespace oceanbase
{
namespace storage
{

class ObCheckpointAtomicProtocol : public ObAtomicProtocol
{
public:
  ObCheckpointAtomicProtocol();
  virtual ~ObCheckpointAtomicProtocol();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);

  virtual int create(
      const bool need_check_sswriter);
  virtual int finish(ObIAtomicProtocolOP &op);
  virtual int abort();

  INHERIT_TO_STRING_KV("ObAtomicProtocol", ObAtomicProtocol, K_(is_inited), K_(tenant_id), K_(ls_id));

protected:
  virtual int prepare_ss_meta_ctrl_key_(
      ObSSMetaCtrlKey &ctrl_key);
  virtual int prepare_atomic_ctrl_info_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObSSMetaCtrlValue &ctrl_value);
  virtual int update_lock_row_(
      const ObSSMetaCtrlLockRowValue &lock_row_value,
      ObMySQLTransaction &trans);
  virtual int prepare_lock_row_(
      ObLockAtomicProtocol &lock_row);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  DISALLOW_COPY_AND_ASSIGN(ObCheckpointAtomicProtocol);
};

} //storage
} //oceanbase


#endif /* OCEANBASE_CHECKPOINT_ATOMIC_PROTOCOL_DEFINE_ */
