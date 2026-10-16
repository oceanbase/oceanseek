/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_DAS_OB_DAS_DML_IVF_ITER_H_
#define OCEANBASE_SQL_DAS_OB_DAS_DML_IVF_ITER_H_

#include "sql/das/ob_das_domain_utils.h"

namespace oceanbase
{
namespace sql
{

// Projects logical IVF user-row auxiliary records from a DAS StoredRow whose
// new CID was already materialized by the Leader preprocessing stage.  This
// iterator deliberately owns no assignment state or cache.
class ObIVFDMLIterator final : public ObDomainDMLIterator
{
public:
  ObIVFDMLIterator(common::ObIAllocator &allocator,
                   const IntFixedArray *row_projector,
                   ObDASWriteBuffer::Iterator &write_iter,
                   const ObDASDMLBaseCtDef *das_ctdef,
                   const ObDASDMLBaseCtDef *main_ctdef)
    : ObDomainDMLIterator(allocator, row_projector, write_iter,
                          das_ctdef, main_ctdef)
  {}
  virtual ~ObIVFDMLIterator() = default;

private:
  virtual int generate_domain_rows(
      const ObChunkDatumStore::StoredRow *store_row) override;
  int get_cid_output_idx_(int64_t &cid_idx) const;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_DAS_OB_DAS_DML_IVF_ITER_H_
