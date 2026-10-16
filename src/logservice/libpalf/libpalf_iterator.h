/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_ITERATOR_
#define OCEANBASE_LOGSERVICE_LIBPALF_ITERATOR_

#include "share/scn.h"
#include "lib/function/ob_function.h"
#include "palf_ffi.h"

namespace oceanbase
{
namespace palf
{
class ILogStorage;
class LSN;
class ILogStorage;
class LogIOContext;
}
namespace libpalf
{
typedef ObFunction<void()> DestroyStorageFunctor;
class LibPalfLogEntry;

class LibPalfLogEntryIterator
{
public:
  LibPalfLogEntryIterator();
  ~LibPalfLogEntryIterator();
  int init(const LibPalfLogEntryIteratorFFI *iterator_ffi);

  void destroy();

  int reuse(const palf::LSN &start_lsn);
  
  int next();
  int next(const share::SCN &replayable_point_scn);
  int next(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point);

  int get_entry(LibPalfLogEntry &entry, palf::LSN &lsn);
  int get_entry(const char *&buffer, LibPalfLogEntry &entry, palf::LSN& lsn);
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, bool &is_raw_write);
  int get_entry(const char *&buffer, int64_t &nbytes, palf::LSN &lsn, int64_t &log_proposal_id);
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, int64_t &log_proposal_id, bool &is_raw_write);
  int get_consumed_info(palf::LSN &end_lsn, share::SCN &end_scn);
  bool is_inited() const;
  bool is_valid() const;
  bool check_is_the_last_entry();
  // @brief cleanup some resource when calling 'destroy'.
  int set_destroy_iterator_storage_functor(const DestroyStorageFunctor &destroy_func);

  TO_STRING_KV(K(is_inited_),
               KP(iterator_ffi_),
               K(destroy_storage_functor_));
private:
    bool is_inited_;

    const LibPalfLogEntryIteratorFFI *iterator_ffi_;
    DestroyStorageFunctor destroy_storage_functor_;
};
} // end namespace libpalf
} // end namespace oceanbase


#endif