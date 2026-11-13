/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "storage/shared_storage/prewarm/ob_prewarm_parallelism_controller.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}
namespace storage
{
class ObTabletHandle;

// Prewarm major SSTable macro blocks into local macro cache.
class ObHotTabletInfoReader : public ObPrewarmParController, public ObSSBasePrewarmer
{
public:
  ObHotTabletInfoReader(const int64_t compaction_scn,
                        const share::SCN &reorganization_scn,
                        const ObTabletHandle &tablet_handle,
                        const blocksstable::ObSSTable &sstable);
  virtual ~ObHotTabletInfoReader();
  int load_hot_macro_infos();

  TO_STRING_KV(K(tablet_id_), K(compaction_scn_), K(reorganization_scn_));

private:
  int handle_hot_tablet();
  int prewarm_sstable_(const blocksstable::ObSSTable &sstable);
  int check_if_stop_prewarm(bool &is_stop);

private:
  int64_t tablet_id_;
  int64_t compaction_scn_;
  share::SCN reorganization_scn_;
  const ObTabletHandle &tablet_handle_;
  const blocksstable::ObSSTable &sstable_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_ */
