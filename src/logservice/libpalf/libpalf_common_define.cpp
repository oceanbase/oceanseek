/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "palf_ffi.h"
#include "logservice/ipalf/ipalf_define.h"
#include "logservice/ipalf/ipalf_options.h"
#include "logservice/palf/palf_base_info.h"

#include "libpalf_common_define.h"

namespace oceanbase
{
namespace libpalf
{
int transfer_access_mode(
    ipalf::AccessMode const &access_mode,
    LibPalfAccessMode& libpalf_access_mode)
{
  int ret = OB_SUCCESS;
  switch (access_mode) {
    case ipalf::AccessMode::APPEND: libpalf_access_mode = LibPalfAccessMode::Append; break;
    case ipalf::AccessMode::RAW_WRITE: libpalf_access_mode = LibPalfAccessMode::RawWrite; break;
    case ipalf::AccessMode::FLASHBACK: libpalf_access_mode = LibPalfAccessMode::FlashBack; break;
    case ipalf::AccessMode::PREPARE_FLASHBACK: libpalf_access_mode = LibPalfAccessMode::PrepareFlashback; break;
    case ipalf::AccessMode::INVALID_ACCESS_MODE:
    default: {
      ret = OB_INVALID_ARGUMENT;
      break;
    }
  }
  return ret;
}

int transfer_libpalf_access_mode_to_access_mode(const LibPalfAccessMode &libpalf_access_mode,
                                                ipalf::AccessMode &access_mode)
{
  int ret = OB_SUCCESS;
  switch (libpalf_access_mode) {
    case LibPalfAccessMode::Append: access_mode = ipalf::AccessMode::APPEND; break;
    case LibPalfAccessMode::RawWrite: access_mode = ipalf::AccessMode::RAW_WRITE; break;
    case LibPalfAccessMode::FlashBack: access_mode = ipalf::AccessMode::FLASHBACK; break;
    case LibPalfAccessMode::PrepareFlashback: access_mode = ipalf::AccessMode::PREPARE_FLASHBACK; break;
    default: {
      ret = OB_INVALID_ARGUMENT;
      break;
    }
  }
  return ret;
}
  

int transfer_palf_base_info_to_libpalf_base_info(palf::PalfBaseInfo const &palf_base_info,
                                                 LibPalfBaseInfo &libpalf_base_info)
{
    int ret = OB_SUCCESS;
    if (!palf_base_info.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(palf_base_info.prev_log_info_));
    } else if (0 > palf_base_info.prev_log_info_.log_id_) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(palf_base_info.prev_log_info_));
    } else if (0 == palf_base_info.prev_log_info_.log_id_
              && (palf::INVALID_PROPOSAL_ID != palf_base_info.prev_log_info_.log_proposal_id_ 
                 || -1 != palf_base_info.prev_log_info_.accum_checksum_)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(palf_base_info.prev_log_info_));
    } else if (0 < palf_base_info.prev_log_info_.log_id_
              && (0 > palf_base_info.prev_log_info_.log_proposal_id_
                 || 0 > palf_base_info.prev_log_info_.accum_checksum_)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(palf_base_info.prev_log_info_));
    } else {
        libpalf_base_info.lsn = palf_base_info.curr_lsn_.val_;
        libpalf_base_info.prev_log_info = LibPalfLogInfo(
                palf_base_info.prev_log_info_.lsn_.val_,
                palf_base_info.prev_log_info_.scn_.get_val_for_logservice(),
                palf_base_info.prev_log_info_.log_id_,
                palf_base_info.prev_log_info_.log_proposal_id_,
                palf_base_info.prev_log_info_.accum_checksum_);
        if (0 == palf_base_info.prev_log_info_.log_id_) {
            libpalf_base_info.prev_log_info.log_proposal_id = 0;
            libpalf_base_info.prev_log_info.accum_checksum = 0;
        } else {
            // do nothing
        }     
    }
    return ret;
}

int transfer_libpalf_base_info_to_palf_base_info(const LibPalfBaseInfo &libpalf_base_info,
                                                 palf::PalfBaseInfo &palf_base_info)
{
    
    int ret = OB_SUCCESS;
    palf_base_info.curr_lsn_.val_ = libpalf_base_info.lsn;
    palf_base_info.prev_log_info_.log_id_ = libpalf_base_info.prev_log_info.log_id;
    palf_base_info.prev_log_info_.lsn_.val_ = libpalf_base_info.prev_log_info.lsn;
    palf_base_info.prev_log_info_.scn_.convert_for_logservice(libpalf_base_info.prev_log_info.scn);
    palf_base_info.prev_log_info_.log_proposal_id_ = libpalf_base_info.prev_log_info.log_proposal_id;
    palf_base_info.prev_log_info_.accum_checksum_ = libpalf_base_info.prev_log_info.accum_checksum;
    return ret;
}

} // end namespace libpalf
} // end namespace oceanbase
