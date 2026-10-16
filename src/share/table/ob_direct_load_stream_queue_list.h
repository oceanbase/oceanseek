/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace table
{

struct ObDirectLoadStreamQueueEntry
{
public:
  ObDirectLoadStreamQueueEntry();
  ObDirectLoadStreamQueueEntry(const common::ObAddr &server_addr, const int64_t queue_count);
  ~ObDirectLoadStreamQueueEntry() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(server_addr), K_(queue_count));
public:
  common::ObAddr server_addr_;
  int64_t queue_count_;
};

struct ObDirectLoadStreamQueueDesc
{
public:
  ObDirectLoadStreamQueueDesc();
  ObDirectLoadStreamQueueDesc(const uint64_t queue_id, const common::ObAddr &server_addr);
  ~ObDirectLoadStreamQueueDesc() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(queue_id), K_(server_addr));
public:
  uint64_t queue_id_;
  common::ObAddr server_addr_;
};

class ObDirectLoadStreamQueueListCodec
{
public:
  ObDirectLoadStreamQueueListCodec() = delete;
  ~ObDirectLoadStreamQueueListCodec() = delete;

  static int encode(const common::ObIArray<ObDirectLoadStreamQueueEntry> &entries,
                    common::ObIAllocator &allocator, common::ObString &json);
  static int decode(const common::ObString &json, common::ObIAllocator &allocator,
                    common::ObIArray<ObDirectLoadStreamQueueEntry> &entries);
  static int validate(const common::ObIArray<ObDirectLoadStreamQueueEntry> &entries);
  static int expand(
      const common::ObIArray<ObDirectLoadStreamQueueEntry> &entries,
      common::ObIArray<ObDirectLoadStreamQueueDesc> &queue_descs);
  static int build_topology(
      const common::ObIArray<common::ObAddr> &alive_servers,
      const int64_t stream_id,
      const int64_t resource_budget,
      common::ObIArray<ObDirectLoadStreamQueueEntry> &entries);
};

} // namespace table
} // namespace oceanbase
