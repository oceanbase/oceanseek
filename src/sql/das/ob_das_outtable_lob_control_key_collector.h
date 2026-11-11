/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_DAS_OB_DAS_OUTTABLE_LOB_CONTROL_KEY_COLLECTOR_H_
#define OCEANBASE_SQL_DAS_OB_DAS_OUTTABLE_LOB_CONTROL_KEY_COLLECTOR_H_

#include "lib/allocator/page_arena.h"
#include "storage/lob/store/ob_outtable_lob_control_ctx.h"

namespace oceanbase
{
namespace sql
{

struct ObDASDMLBaseCtDef;

class ObDASOuttableLobControlKeyCollector final
{
public:
  ObDASOuttableLobControlKeyCollector();
  ~ObDASOuttableLobControlKeyCollector();

  int prepare_if_needed(const ObDASDMLBaseCtDef &dml_ctdef);
  void reuse();
  void reset();
  int assign(const common::ObIArray<storage::ObOuttableLobControlKey> *control_keys);
  int append_to(common::ObIArray<storage::ObOuttableLobControlKey> *control_keys) const;
  common::ObIArray<storage::ObOuttableLobControlKey> *get_keys();
  const common::ObIArray<storage::ObOuttableLobControlKey> *get_keys() const;
  bool empty() const;
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV("key_count", (empty() ? 0 : get_keys()->count()));

private:
  // Parallel workers copy results into different collectors sharing the same DAS allocator.
  // Allocate keys independently; keep the allocator alive until control_ctx_ is destroyed.
  common::ModulePageAllocator allocator_;
  storage::ObOuttableLobControlCtx control_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObDASOuttableLobControlKeyCollector);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_DAS_OB_DAS_OUTTABLE_LOB_CONTROL_KEY_COLLECTOR_H_
