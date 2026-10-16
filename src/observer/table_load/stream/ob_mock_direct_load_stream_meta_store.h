/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{

class ObMockDirectLoadStreamMetaStore : public ObIDirectLoadStreamMetaStore
{
  typedef common::hash::ObHashMap<int64_t, ObDirectLoadStreamMeta *> StreamMetaMap;

public:
  explicit ObMockDirectLoadStreamMetaStore(const uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  virtual ~ObMockDirectLoadStreamMetaStore();

  int init();
  void reset();

  virtual int create_stream(const int64_t stream_id,
                            const ObDirectLoadStreamCreateParam &param,
                            ObDirectLoadStreamMetaCreateOutcome &outcome) override;
  int create_stream(const int64_t stream_id,
                    const ObDirectLoadStreamCreateParam &param);
  virtual int get_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta) override;
  virtual int scan_streams(const ObDirectLoadStreamScanParam &param,
                           common::ObIArray<ObDirectLoadStreamMeta> &metas) override;
  virtual int scan_gc_streams(const int64_t now,
                              common::ObIArray<ObDirectLoadStreamMeta> &metas) override;
  virtual int finish_expired_unclaimed_stream(const int64_t stream_id,
                                              const int64_t now,
                                              bool &finished) override;
  virtual int seal_stream(const int64_t stream_id) override;
  virtual int claim_stream(const ObDirectLoadStreamClaimParam &param) override;
  virtual int finish_stream(const int64_t stream_id, const int result_code) override;
  virtual int finish_stream_if_open(const int64_t stream_id,
                                    const int result_code,
                                    bool &finished) override;
  virtual int delete_stream(const int64_t stream_id,
                            const int64_t max_modified_ts) override;

  void set_create_result_for_test(const int ret,
                                  const ObDirectLoadStreamMetaCreateOutcome outcome)
  {
    create_ret_for_test_ = ret;
    create_outcome_for_test_ = outcome;
  }

private:
  uint64_t tenant_id_;
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  StreamMetaMap stream_meta_map_;
  lib::ObMutex mutex_;
  int create_ret_for_test_;
  ObDirectLoadStreamMetaCreateOutcome create_outcome_for_test_;
};

} // namespace observer
} // namespace oceanbase
