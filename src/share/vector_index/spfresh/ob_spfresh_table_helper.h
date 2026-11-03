/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TABLE_HELPER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TABLE_HELPER_H_

#include <utility>
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/ob_define.h"
#include "share/scn.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/logical_table/ob_logical_table_define.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"

namespace oceanbase
{
namespace common
{
class ObTimeoutCtx;
}
namespace share
{

// KV table location info for SPFresh operations (replaces ObSpfreshAuxTableIds)
struct ObSpfreshKvTableInfo
{
  static const int64_t MAX_KV_TABLE_NAME_LEN = OB_MAX_TABLE_NAME_BUF_LENGTH;

  ObSpfreshKvTableInfo()
    : namespace_id_(OB_INVALID_NAMESPACE_ID),
      logical_table_id_(OB_INVALID_LOGICAL_TABLE_ID),
      centroid_index_seq_(0),
      cid_vector_index_seq_(0),
      rowid_cid_index_seq_(0),
      kv_data_table_id_(OB_INVALID_ID),
      centroid_tablet_id_(common::OB_INVALID_ID)
  {
    db_name_[0] = '\0';
    kv_table_name_[0] = '\0';
    vector_col_name_[0] = '\0';
    centroid_vector_col_name_[0] = '\0';
    centroid_state_col_name_[0] = '\0';
  }

  void reset()
  {
    namespace_id_ = OB_INVALID_NAMESPACE_ID;
    logical_table_id_ = OB_INVALID_LOGICAL_TABLE_ID;
    centroid_index_seq_ = 0;
    cid_vector_index_seq_ = 0;
    rowid_cid_index_seq_ = 0;
    kv_data_table_id_ = OB_INVALID_ID;
    centroid_tablet_id_ = common::OB_INVALID_ID;
    db_name_[0] = '\0';
    kv_table_name_[0] = '\0';
    vector_col_name_[0] = '\0';
    centroid_vector_col_name_[0] = '\0';
    centroid_state_col_name_[0] = '\0';
  }

  bool is_valid() const
  {
    return OB_INVALID_NAMESPACE_ID != namespace_id_
        && OB_INVALID_LOGICAL_TABLE_ID != logical_table_id_
        && db_name_[0] != '\0'
        && kv_table_name_[0] != '\0';
  }

  TO_STRING_KV(K_(namespace_id), K_(logical_table_id),
               K_(centroid_index_seq), K_(cid_vector_index_seq),
               K_(rowid_cid_index_seq), K_(kv_data_table_id),
               K_(centroid_tablet_id), K_(db_name), K_(kv_table_name),
               K_(vector_col_name), K_(centroid_vector_col_name),
               K_(centroid_state_col_name));

public:
  uint64_t namespace_id_;
  uint64_t logical_table_id_;
  uint64_t centroid_index_seq_;       // aux_type=4 (LT_AUX_IVF_CID_VECTOR)
  uint64_t cid_vector_index_seq_;     // aux_type=5 (LT_AUX_IVF_CID_ROWID)
  uint64_t rowid_cid_index_seq_;      // aux_type=6 (LT_AUX_IVF_ROWID_CID)
  uint64_t kv_data_table_id_;         // physical collection KV table schema id
  uint64_t centroid_tablet_id_;
  char db_name_[OB_MAX_DATABASE_NAME_BUF_LENGTH];
  char kv_table_name_[MAX_KV_TABLE_NAME_LEN];
  char vector_col_name_[OB_MAX_COLUMN_NAME_BUF_LENGTH];
  char centroid_vector_col_name_[OB_MAX_COLUMN_NAME_BUF_LENGTH];
  char centroid_state_col_name_[OB_MAX_COLUMN_NAME_BUF_LENGTH];
};

// Resolves KV table info from IVF index table id
struct ObSpfreshKvTableResolver
{
public:
  ObSpfreshKvTableResolver() {}
  ~ObSpfreshKvTableResolver() {}

  int resolve(uint64_t tenant_id, uint64_t index_table_id,
              ObSpfreshKvTableInfo &kv_info);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSpfreshKvTableResolver);
};

// Centroid row decoded from KV (aux_type=LT_AUX_IVF_CID_VECTOR)
struct ObSpfreshCentroidRow
{
  ObSpfreshCentroidRow()
    : ltable_id_(0),
      center_id_(OB_INVALID_ID),
      vector_(nullptr),
      vector_len_(0),
      state_(ObIvfCentroidState::NORMAL)
  {}

  void reset()
  {
    ltable_id_ = 0;
    center_id_ = OB_INVALID_ID;
    vector_ = nullptr;
    vector_len_ = 0;
    state_ = ObIvfCentroidState::NORMAL;
  }

  TO_STRING_KV(K_(ltable_id), K_(center_id), K_(vector_len), K_(state));

  uint64_t ltable_id_;
  uint64_t center_id_;
  const char *vector_;
  int64_t vector_len_;
  ObIvfCentroidState state_;
};

// Vector row info for SPFresh operations
struct ObSpfreshVectorRow
{
public:
  static const int64_t MAX_ROWKEY_BUF_LEN = 64;  // Enough for most rowkey types

  ObSpfreshVectorRow()
    : rowkey_(nullptr),
      rowkey_len_(0),
      vector_(nullptr),
      vector_len_(0),
      center_id_(OB_INVALID_ID)
  {
    rowkey_buf_[0] = '\0';
  }

  void reset()
  {
    rowkey_ = nullptr;
    rowkey_len_ = 0;
    vector_ = nullptr;
    vector_len_ = 0;
    center_id_ = OB_INVALID_ID;
    rowkey_buf_[0] = '\0';
  }

  void set_rowkey_from_int(int64_t rowkey_int)
  {
    int64_t rowkey_be = htonll(rowkey_int);
    MEMCPY(rowkey_buf_, &rowkey_be, 8);
    rowkey_ = rowkey_buf_;
    rowkey_len_ = 8;
  }

  void set_rowkey_from_string(const ObString &rowkey_str)
  {
    if (rowkey_str.length() <= MAX_ROWKEY_BUF_LEN) {
      MEMCPY(rowkey_buf_, rowkey_str.ptr(), rowkey_str.length());
      rowkey_ = rowkey_buf_;
      rowkey_len_ = rowkey_str.length();
    } else {
      rowkey_ = rowkey_str.ptr();
      rowkey_len_ = rowkey_str.length();
    }
  }

  TO_STRING_KV(K_(rowkey_len), K_(vector_len), K_(center_id));

public:
  const char *rowkey_;
  int64_t rowkey_len_;
  const char *vector_;
  int64_t vector_len_;
  uint64_t center_id_;
  char rowkey_buf_[MAX_ROWKEY_BUF_LEN];
};

// Table helper for SPFresh operations — operates on kv_data_table via encoder/decoder
class ObSpfreshTableHelper
{
public:
  ObSpfreshTableHelper();
  ~ObSpfreshTableHelper();

  int init(uint64_t tenant_id,
           common::ObMySQLProxy *sql_proxy,
           const ObSpfreshKvTableInfo &kv_info);
  void destroy();

  // Release contract: aux_type=5 was never released in a pre-raw format,
  // rolling upgrades never mix different writers, and pre-release data need
  // not be read. Therefore non-NULL values are exactly float32[dimension] and
  // NULL is represented by a zero-length value. If any premise changes, this
  // raw-only contract must be stopped and compatibility reassessed.
  static int get_raw_vector_expected_bytes(int64_t dimension,
                                           int64_t &expected_bytes);

  int scan_cid_vector_by_center(uint64_t center_id,
                                int64_t dimension,
                                common::ObIAllocator &allocator,
                                ObIArray<ObSpfreshVectorRow> &vectors);

  int scan_cid_vector_by_center_limit(uint64_t center_id,
                                      const char *last_rowkey,
                                      int64_t last_rowkey_len,
                                      int64_t limit,
                                      int64_t dimension,
                                      common::ObIAllocator &allocator,
                                      ObIArray<ObSpfreshVectorRow> &vectors);

  int insert_centroid(uint64_t center_id,
                      const char *vector,
                      int64_t vector_len,
                      ObIvfCentroidState state,
                      SCN &commit_scn);

  int delete_from_cid_vector(uint64_t center_id,
                             const ObIArray<ObString> &rowkeys,
                             SCN &commit_scn);

  int insert_to_cid_vector(uint64_t center_id,
                           const ObIArray<ObSpfreshVectorRow> &vectors,
                           SCN &commit_scn);

  int delete_from_rowid_cid(uint64_t center_id,
                            const ObIArray<ObString> &rowkeys,
                            SCN &commit_scn);

  int insert_to_rowid_cid(uint64_t center_id,
                          const ObIArray<ObSpfreshVectorRow> &vectors,
                          SCN &commit_scn);

  int update_rowkey_cid(const char *rowkey,
                        int64_t rowkey_len,
                        uint64_t new_center_id,
                        SCN &commit_scn);

  int update_centroid_state(uint64_t center_id,
                            ObIvfCentroidState new_state,
                            ObIvfCentroidState expected_old_state,
                            SCN &commit_scn);

  int delete_centroid(uint64_t center_id, SCN &commit_scn);

  // CAS update centroid state within an active batch transaction.
  // SELECT kv_value → decode check state → re-encode → UPDATE WHERE kv_value=old.
  // Same-state requests use SELECT FOR UPDATE to confirm the row until transaction end.
  // Returns affected_rows: 1=updated or locked match, 0=state/value mismatch.
  int update_centroid_value_cas(uint64_t center_id,
                                 ObIvfCentroidState expected_state,
                                 ObIvfCentroidState new_state,
                                 int64_t &affected_rows);

  // Restore a lock state without overwriting a concurrently changed value.
  // A zero-row CAS is successful only when a read in the same transaction
  // confirms that the persisted state is already the rollback target.
  int rollback_centroid_state(uint64_t center_id,
                              ObIvfCentroidState locked_state,
                              ObIvfCentroidState target_state,
                              SCN &commit_scn);

  // Register a physical-tablet modification inside an active batch transaction
  // by touching an existing aux_type=05 row. Unlike centroid CAS, this also
  // works for bootstrap center 0, which has no persisted aux_type=04 row.
  int touch_cid_vector(uint64_t center_id, const common::ObString &rowkey);

  // Counts exact-dimension non-NULL vectors. Any non-empty payload with a
  // different length is treated as corruption and fails the whole query.
  int count_vectors_by_center(uint64_t center_id,
                              int64_t dimension,
                              int64_t &count);

  // Batch version: returns posting count grouped by center_id in a single SQL.
  // As with the scalar version, malformed non-empty payloads fail the query.
  // ObArray is required because its assign() builds replacement storage before
  // releasing the old elements, making final publication failure-atomic.
  // The output allocator should be bound to MTL_ID() by the caller.
  int count_vectors_group_by_center(
      int64_t dimension,
      common::ObArray<std::pair<uint64_t, int64_t>> &center_counts);

  // Scan all centroid rows (aux_type=4) under namespace_id in kv_data_table.
  int scan_centroids_by_namespace(uint64_t namespace_id,
                                  common::ObIAllocator &allocator,
                                  ObIArray<ObSpfreshCentroidRow> &centroids);

  // Scan all centroid rows (aux_type=4) under (namespace_id, ltable_id) in kv_data_table.
  int scan_centroids_by_ltable(uint64_t namespace_id,
                               uint64_t ltable_id,
                               common::ObIAllocator &allocator,
                               ObIArray<ObSpfreshCentroidRow> &centroids);

  // Read the max center_id among centroid rows (aux_type=4) in kv_data_table for
  // this helper's (namespace, ltable, centroid_tablet). KV is the authority for
  // child-center-id allocation so a stale cache cannot cause PK collisions (-5024).
  // Returns 0 when no centroid row exists (bootstrap). scoped to the centroid tablet
  // because center_id uniqueness is per-(tablet, ns, ltable).
  int get_max_center_id_from_kv(uint64_t &max_center_id);

  // Batch transaction: allow multiple SQLs to execute in a single transaction.
  // Call start_batch_transaction() before a series of writes, then call
  // end_batch_transaction(true, scn) to commit or end_batch_transaction(false, scn)
  // to rollback. While batch is active, vector scans and writes use the same connection
  // without starting/ending their own transactions.
  int start_batch_transaction();
  // Transaction-scoped physical tablet exclusion; conflicts return OB_EAGAIN.
  int lock_batch_tablet(const common::ObTabletID &tablet_id);
  int end_batch_transaction(const bool commit, SCN &commit_scn);

  static int get_commit_scn_from_trans(class ObMySQLTransaction &trans, SCN &commit_scn);

private:
  // Shared timeout budget for SPFresh inner SQL transactions.
  static constexpr int64_t SPFRESH_TIMEOUT_US = 300L * 1000L * 1000L;
  static int set_spfresh_timeout_ctx_(common::ObTimeoutCtx &timeout_ctx,
                                      const bool set_trx_timeout);
  int build_kv_key_(ObLogicalTableAuxType aux_type,
                    const common::ObIArray<common::ObDatum> &datums,
                    common::ObIAllocator &allocator,
                    common::ObString &key);
  int build_kv_value_(const common::ObIArray<common::ObString> &col_names,
                      const common::ObIArray<common::ObObjMeta> &metas,
                      const common::ObIArray<common::ObDatum> &datums,
                      common::ObIAllocator &allocator,
                      common::ObString &value);
  int build_center_id_datum_(uint64_t center_id,
                             common::ObIAllocator &allocator,
                             common::ObDatum &datum);
  int decode_kv_row_(const char *kv_key, int64_t kv_key_len,
                     const char *kv_value, int64_t kv_value_len,
                     int64_t expected_vector_bytes,
                     common::ObIAllocator &allocator,
                     ObIArray<ObSpfreshVectorRow> &vectors);
  static int append_raw_cid_vector_(const char *kv_value,
                                    int64_t kv_value_len,
                                    int64_t expected_vector_bytes,
                                    common::ObIAllocator &allocator,
                                    const ObSpfreshVectorRow &key_row,
                                    ObIArray<ObSpfreshVectorRow> &vectors);
  static void finalize_vector_scan_result_(
      int scan_ret,
      ObIArray<ObSpfreshVectorRow> &vectors);
  static int validate_raw_vector_count_result_(int64_t valid_count,
                                               int64_t invalid_count);
  static int publish_grouped_counts_(
      const common::ObIArray<std::pair<uint64_t, int64_t>> &local_counts,
      common::ObArray<std::pair<uint64_t, int64_t>> &center_counts);
  int validate_cid_vector_rows_(const ObIArray<ObSpfreshVectorRow> &vectors) const;
  int decode_centroid_kv_row_(const char *kv_key, int64_t kv_key_len,
                              const char *kv_value, int64_t kv_value_len,
                              common::ObIAllocator &allocator,
                              ObSpfreshCentroidRow &centroid);
  int decode_centroid_value_(common::ObIAllocator &allocator,
                             const char *kv_value, int64_t kv_value_len,
                             const char *&vector_data, int64_t &vector_len,
                             ObIvfCentroidState &state);
  int scan_centroids_by_kv_key_range_(uint64_t namespace_id,
                                      const char *start_hex,
                                      const char *end_hex,
                                      const bool end_inclusive,
                                      common::ObIAllocator &allocator,
                                      ObIArray<ObSpfreshCentroidRow> &centroids,
                                      bool is_get_visiable = false);
  int execute_inner_sql_with_centroid_result_(const char *sql,
                                              common::ObIAllocator &allocator,
                                              ObIArray<ObSpfreshCentroidRow> &centroids,
                                              bool is_get_visiable = false);
  int execute_inner_sql_(const char *sql, SCN &commit_scn);
  int execute_inner_sql_with_result_(const char *sql,
                                     int64_t expected_vector_bytes,
                                     ObIArray<ObSpfreshVectorRow> &vectors);
  int execute_inner_sql_with_result_(const char *sql,
                                     int64_t expected_vector_bytes,
                                     common::ObIAllocator &allocator,
                                     ObIArray<ObSpfreshVectorRow> &vectors);
  int execute_sql_no_tx_(const char *sql);
  int execute_sql_no_tx_(const char *sql, int64_t &affected_rows);
  int read_centroid_state_in_tx_(uint64_t center_id,
                                 ObIvfCentroidState &state);
  uint64_t get_index_seq_(ObLogicalTableAuxType aux_type) const;
  int64_t get_scan_parallelism_() const;

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  char db_name_[OB_MAX_DATABASE_NAME_BUF_LENGTH];
  char kv_data_table_[OB_MAX_TABLE_NAME_BUF_LENGTH];
  char vector_col_name_[OB_MAX_COLUMN_NAME_BUF_LENGTH];
  char centroid_vector_col_name_[OB_MAX_COLUMN_NAME_BUF_LENGTH];
  char centroid_state_col_name_[OB_MAX_COLUMN_NAME_BUF_LENGTH];
  ObLogicalTableKeyPrefix key_prefix_;
  uint64_t centroid_index_seq_;
  uint64_t cid_vector_index_seq_;
  uint64_t rowid_cid_index_seq_;
  uint64_t centroid_tablet_id_;
  uint64_t kv_data_table_id_;
  sqlclient::ObISQLConnection *batch_conn_;
  common::ObSingleConnectionProxy *batch_conn_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObSpfreshTableHelper);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_TABLE_HELPER_H_
