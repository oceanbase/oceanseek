/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{

class ObDirectLoadStreamService;

class ObDirectLoadStreamGc
{
public:
  explicit ObDirectLoadStreamGc(const uint64_t tenant_id);
  int run(ObDirectLoadStreamService &stream_service,
          ObIDirectLoadStreamMetaStore &meta_store,
          const int64_t now);
  int run_for_test(ObDirectLoadStreamService &stream_service,
                   ObIDirectLoadStreamMetaStore &meta_store,
                   const int64_t now);

private:
  int run_(ObDirectLoadStreamService &stream_service,
           ObIDirectLoadStreamMetaStore &meta_store,
           const int64_t now);
  int process_stream_(ObDirectLoadStreamService &stream_service,
                      ObIDirectLoadStreamMetaStore &meta_store,
                      const ObDirectLoadStreamMeta &meta,
                      const int64_t now);
  int check_sys_ls_leader_(bool &is_leader);

private:
  const uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamGc);
};

} // namespace observer
} // namespace oceanbase
