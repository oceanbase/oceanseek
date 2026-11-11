/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_CTX_H_
#define OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_CTX_H_

#include "lib/container/ob_array_serialization.h"
#include "storage/lob/common/ob_outtable_lob_control_key.h"

namespace oceanbase
{
namespace observer
{
class ObInnerSQLConnection;
}
namespace sql
{
class ObExecContext;
}
namespace storage
{

class ObOuttableLobControlCtx final
{
public:
  ObOuttableLobControlCtx();
  ~ObOuttableLobControlCtx();

  int init(common::ObIAllocator &allocator);
  void reuse();
  void reset();
  int assign(const common::ObIArray<ObOuttableLobControlKey> *control_keys);
  int append_to(common::ObIArray<ObOuttableLobControlKey> *control_keys) const;
  common::ObIArray<ObOuttableLobControlKey> *get_keys();
  const common::ObIArray<ObOuttableLobControlKey> *get_keys() const;
  bool empty() const;
  int serialize_control_keys(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize_control_keys(const char *buf, int64_t data_len, int64_t &pos);
  int64_t get_control_keys_serialize_size() const;

  int complete(sql::ObExecContext &exec_ctx);

private:
  int complete_in_session_(sql::ObExecContext &exec_ctx,
                           observer::ObInnerSQLConnection &connection);

  typedef common::ObSArray<ObOuttableLobControlKey> ControlKeyArray;
  common::ObIAllocator *allocator_;
  ControlKeyArray *control_keys_;
  DISALLOW_COPY_AND_ASSIGN(ObOuttableLobControlCtx);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_CTX_H_
