/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_isql_connection.h"
#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace observer
{

class ObDirectLoadStreamPersistentMetaStore : public ObIDirectLoadStreamMetaStore
{
public:
  explicit ObDirectLoadStreamPersistentMetaStore(const uint64_t tenant_id);
  virtual ~ObDirectLoadStreamPersistentMetaStore();

  int init(common::ObISQLClient &sql_client);
  void reset();

  virtual int create_stream(const int64_t stream_id,
                            const ObDirectLoadStreamCreateParam &param,
                            ObDirectLoadStreamMetaCreateOutcome &outcome) override;
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
  int finish_success_in_trans(
      const int64_t stream_id,
      common::sqlclient::ObISQLConnection &sql_connection);
  virtual int finish_stream_if_open(const int64_t stream_id,
                                    const int result_code,
                                    bool &finished) override;
  virtual int delete_stream(const int64_t stream_id,
                            const int64_t max_modified_ts) override;

private:
  uint64_t get_sql_tenant_id_() const;
  int resolve_create_write_result_(const int64_t stream_id,
                                   const ObDirectLoadStreamCreateParam &param,
                                   const int write_ret,
                                   ObDirectLoadStreamMetaCreateOutcome &outcome);
  int resolve_finish_if_open_write_result_(const int64_t stream_id,
                                           const int result_code,
                                           const int write_ret,
                                           bool &finished);
  int read_one_stream_(const common::ObSqlString &sql, const int64_t stream_id,
                       ObDirectLoadStreamMeta &meta);
  int read_streams_(const common::ObSqlString &sql,
                    common::ObIArray<ObDirectLoadStreamMeta> &metas);
  int resolve_zero_affected_seal_(const int64_t stream_id);
  int resolve_zero_affected_claim_(const int64_t stream_id);
  int build_finish_stream_sql_(const int64_t stream_id,
                               const int result_code,
                               common::ObSqlString &sql);
  int resolve_zero_affected_finish_(const int64_t stream_id);

private:
  const uint64_t tenant_id_;
  common::ObISQLClient *sql_client_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamPersistentMetaStore);
};

} // namespace observer
} // namespace oceanbase
