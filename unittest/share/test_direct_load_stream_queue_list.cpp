/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <cstring>
#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"
#include "share/rc/ob_tenant_base.h"
#include "share/table/ob_direct_load_stream_queue_list.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::table;

namespace
{

const int64_t MAX_TEST_DIRECT_LOAD_STREAM_QUEUE_COUNT = 65536;
const int64_t MAX_TEST_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH = 65536;

ObAddr make_addr(const char *ip, const int32_t port)
{
  ObAddr addr;
  EXPECT_TRUE(addr.set_ip_addr(ip, port));
  return addr;
}

void expect_string_equal(const ObString &actual, const char *expected)
{
  ASSERT_EQ(static_cast<int64_t>(strlen(expected)), actual.length());
  EXPECT_EQ(0, memcmp(actual.ptr(), expected, actual.length()));
}

ObString make_json(const char *json)
{
  return ObString(static_cast<int32_t>(strlen(json)), json);
}

} // namespace

#ifndef OB_USE_ASAN
// 当前租户无 allocator 时必须报告分配失败，不能把两个 HashSet 偷记到 server tenant。
// ASAN 绕过租户 allocator，不适用此内存归属断言。
TEST(TestDirectLoadStreamQueueList, hash_sets_follow_tenant_allocation_failure)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObAddr> alive_servers;
  const ObAddr addr = make_addr("127.0.0.1", 2882);
  lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
  const uint64_t tenant_id = 15999;
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 2)));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(addr));
  ASSERT_TRUE(nullptr == allocator->get_tenant_ctx_allocator(
                             tenant_id, ObCtxIds::DEFAULT_CTX_ID));
  share::ObTenantBase tenant(tenant_id);
  share::ObTenantSwitchGuard tenant_guard(&tenant);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1001, 2, entries));
  EXPECT_TRUE(entries.empty());
}
#endif

// 验证默认 queue entry 无效，queue_count 初始为 0。
TEST(TestDirectLoadStreamQueueList, default_queue_entry_is_invalid)
{
  ObDirectLoadStreamQueueEntry entry;
  EXPECT_FALSE(entry.is_valid());
  EXPECT_EQ(0, entry.queue_count_);
}

// 验证默认 queue descriptor 无效，避免未初始化 queue_id 被路由使用。
TEST(TestDirectLoadStreamQueueList, default_queue_desc_is_invalid)
{
  ObDirectLoadStreamQueueDesc queue_desc;
  EXPECT_FALSE(queue_desc.is_valid());
  EXPECT_EQ(OB_INVALID_ID, queue_desc.queue_id_);
}

// 验证 queue_list 按 entry 顺序展开为连续 queue_id，并保留各 owner 地址。
TEST(TestDirectLoadStreamQueueList, expand_multi_server_to_contiguous_queue_ids)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueDesc> queue_descs;
  const ObAddr addr1 = make_addr("127.0.0.1", 2882);
  const ObAddr addr2 = make_addr("127.0.0.2", 3882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr1, 2)));
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr2, 3)));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamQueueListCodec::expand(entries, queue_descs));
  ASSERT_EQ(5, queue_descs.count());
  for (int64_t i = 0; i < queue_descs.count(); ++i) {
    EXPECT_TRUE(queue_descs.at(i).is_valid());
    EXPECT_EQ(static_cast<uint64_t>(i), queue_descs.at(i).queue_id_);
    EXPECT_EQ(i < 2 ? addr1 : addr2, queue_descs.at(i).server_addr_);
  }
}

// 验证非法输入不会把旧结果或部分展开结果暴露给调用方。
TEST(TestDirectLoadStreamQueueList, expand_invalid_input_clears_output)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueDesc> queue_descs;
  const ObAddr addr1 = make_addr("127.0.0.1", 2882);
  const ObAddr addr2 = make_addr("127.0.0.2", 3882);

  ASSERT_EQ(OB_SUCCESS, queue_descs.push_back(ObDirectLoadStreamQueueDesc(7, addr1)));
  ASSERT_EQ(OB_SUCCESS,
            entries.push_back(ObDirectLoadStreamQueueEntry(addr1, 32768)));
  ASSERT_EQ(OB_SUCCESS,
            entries.push_back(ObDirectLoadStreamQueueEntry(addr2, 32769)));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::expand(entries, queue_descs));
  EXPECT_EQ(0, queue_descs.count());
}

// 验证发布结果分配失败时保持 failure atomicity，不留下部分 queue descriptor。
TEST(TestDirectLoadStreamQueueList, expand_allocation_failure_clears_output)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObSEArray<ObDirectLoadStreamQueueDesc, 1, ObNullAllocator> queue_descs;
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 2)));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObDirectLoadStreamQueueListCodec::expand(entries, queue_descs));
  EXPECT_EQ(0, queue_descs.count());
}

// 验证单 owner 展开也从 queue_id=0 开始，并覆盖调用方的旧结果。
TEST(TestDirectLoadStreamQueueList, expand_single_server_replaces_output)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueDesc> queue_descs;
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, queue_descs.push_back(ObDirectLoadStreamQueueDesc(7, addr)));
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 1)));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamQueueListCodec::expand(entries, queue_descs));
  ASSERT_EQ(1, queue_descs.count());
  EXPECT_EQ(0, queue_descs.at(0).queue_id_);
  EXPECT_EQ(addr, queue_descs.at(0).server_addr_);
}

// 验证资源 budget=1 时只冻结一个 owner 和一个 queue。
TEST(TestDirectLoadStreamQueueList, build_topology_with_single_resource)
{
  ObArray<ObAddr> alive_servers;
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  const ObAddr addr1 = make_addr("127.0.0.3", 2882);
  const ObAddr addr2 = make_addr("127.0.0.1", 2882);
  const ObAddr addr3 = make_addr("127.0.0.2", 2882);

  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(addr1));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(addr2));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(addr3));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1001, 1, entries));
  ASSERT_EQ(1, entries.count());
  EXPECT_EQ(1, entries.at(0).queue_count_);
  EXPECT_TRUE(entries.at(0).server_addr_ == addr1 ||
              entries.at(0).server_addr_ == addr2 ||
              entries.at(0).server_addr_ == addr3);
}

// 验证 budget 等于 server 数时每个 owner 恰好分到一个 queue。
TEST(TestDirectLoadStreamQueueList, build_topology_budget_equal_to_server_count)
{
  ObArray<ObAddr> alive_servers;
  ObArray<ObDirectLoadStreamQueueEntry> entries;

  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.3", 2882)));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.1", 2882)));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.2", 2882)));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1002, 3, entries));
  ASSERT_EQ(3, entries.count());
  for (int64_t i = 0; i < entries.count(); ++i) {
    EXPECT_EQ(1, entries.at(i).queue_count_);
  }
}

// 验证 budget 大于 server 数时余数按旋转后的 owner 顺序均匀分配，差值最多 1。
TEST(TestDirectLoadStreamQueueList, build_topology_distributes_remainder_evenly)
{
  ObArray<ObAddr> alive_servers;
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueDesc> queue_descs;

  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.3", 2882)));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.1", 2882)));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.2", 2882)));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1003, 8, entries));
  ASSERT_EQ(3, entries.count());
  EXPECT_EQ(3, entries.at(0).queue_count_);
  EXPECT_EQ(3, entries.at(1).queue_count_);
  EXPECT_EQ(2, entries.at(2).queue_count_);
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamQueueListCodec::expand(entries, queue_descs));
  ASSERT_EQ(8, queue_descs.count());
}

// 验证 alive server 输入顺序不影响同一 stream 的冻结拓扑。
TEST(TestDirectLoadStreamQueueList, build_topology_is_deterministic_after_sort)
{
  ObArray<ObAddr> alive_servers1;
  ObArray<ObAddr> alive_servers2;
  ObArray<ObDirectLoadStreamQueueEntry> entries1;
  ObArray<ObDirectLoadStreamQueueEntry> entries2;
  const ObAddr addr1 = make_addr("127.0.0.1", 2882);
  const ObAddr addr2 = make_addr("127.0.0.2", 2882);
  const ObAddr addr3 = make_addr("127.0.0.3", 2882);

  ASSERT_EQ(OB_SUCCESS, alive_servers1.push_back(addr3));
  ASSERT_EQ(OB_SUCCESS, alive_servers1.push_back(addr1));
  ASSERT_EQ(OB_SUCCESS, alive_servers1.push_back(addr2));
  ASSERT_EQ(OB_SUCCESS, alive_servers2.push_back(addr2));
  ASSERT_EQ(OB_SUCCESS, alive_servers2.push_back(addr3));
  ASSERT_EQ(OB_SUCCESS, alive_servers2.push_back(addr1));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers1, 1004, 5, entries1));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers2, 1004, 5, entries2));
  ASSERT_EQ(entries1.count(), entries2.count());
  for (int64_t i = 0; i < entries1.count(); ++i) {
    EXPECT_EQ(entries1.at(i).server_addr_, entries2.at(i).server_addr_);
    EXPECT_EQ(entries1.at(i).queue_count_, entries2.at(i).queue_count_);
  }
}

// 验证空列表、重复 owner、非法 budget 和防御上限均被拒绝且不泄漏旧输出。
TEST(TestDirectLoadStreamQueueList, build_topology_rejects_invalid_arguments)
{
  ObArray<ObAddr> alive_servers;
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 1)));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1005, 1, entries));
  EXPECT_EQ(0, entries.count());
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(addr));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(addr));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1005, 2, entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1005, 0, entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1005, 65537, entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 0, 1, entries));
  EXPECT_EQ(0, entries.count());
}

// 验证 topology 最终发布分配失败时保持 failure atomicity。
TEST(TestDirectLoadStreamQueueList, build_topology_allocation_failure_clears_output)
{
  ObArray<ObAddr> alive_servers;
  ObSEArray<ObDirectLoadStreamQueueEntry, 1, ObNullAllocator> entries;

  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.1", 2882)));
  ASSERT_EQ(OB_SUCCESS, alive_servers.push_back(make_addr("127.0.0.2", 2882)));
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObDirectLoadStreamQueueListCodec::build_topology(
                alive_servers, 1006, 2, entries));
  EXPECT_EQ(0, entries.count());
}

// 验证单个 server 的 queue list 可 canonical 编码并无损解码。
TEST(TestDirectLoadStreamQueueList, encode_decode_single_server)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueEntry> decoded_entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 2)));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
  expect_string_equal(json,
      "{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\",\"port\":2882,"
      "\"queue_count\":2}]}");

  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::decode(json, allocator, decoded_entries));
  ASSERT_EQ(1, decoded_entries.count());
  EXPECT_EQ(addr, decoded_entries.at(0).server_addr_);
  EXPECT_EQ(2, decoded_entries.at(0).queue_count_);
}

// 验证多个 server 的顺序和 queue_count 在编解码后保持不变。
TEST(TestDirectLoadStreamQueueList, encode_decode_multi_server)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueEntry> decoded_entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  const ObAddr addr1 = make_addr("127.0.0.1", 2882);
  const ObAddr addr2 = make_addr("127.0.0.2", 3882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr1, 1)));
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr2, 3)));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
  expect_string_equal(json,
                      "{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                      "\"port\":2882,\"queue_count\":1},"
                      "{\"ip\":\"127.0.0.2\",\"port\":3882,\"queue_count\":3}]}");

  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::decode(json, allocator, decoded_entries));
  ASSERT_EQ(2, decoded_entries.count());
  EXPECT_EQ(addr1, decoded_entries.at(0).server_addr_);
  EXPECT_EQ(1, decoded_entries.at(0).queue_count_);
  EXPECT_EQ(addr2, decoded_entries.at(1).server_addr_);
  EXPECT_EQ(3, decoded_entries.at(1).queue_count_);
}

// 验证 IPv6 owner 地址可按 canonical JSON 编码并无损解码。
TEST(TestDirectLoadStreamQueueList, encode_decode_ipv6_server)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueEntry> decoded_entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  const ObAddr addr = make_addr("2001:db8::1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 2)));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
  expect_string_equal(json,
      "{\"version\":1,\"queues\":[{\"ip\":\"2001:db8::1\",\"port\":2882,"
      "\"queue_count\":2}]}");

  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::decode(json, allocator, decoded_entries));
  ASSERT_EQ(1, decoded_entries.count());
  EXPECT_EQ(addr, decoded_entries.at(0).server_addr_);
  EXPECT_EQ(2, decoded_entries.at(0).queue_count_);
}

// 验证非 canonical JSON 可解码，并重新编码为稳定的 canonical 格式。
TEST(TestDirectLoadStreamQueueList, decode_pretty_json_and_reencode_canonical)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString canonical_json;
  const char *pretty_json =
      "{ \"queues\" : [ { \"queue_count\" : 2, \"port\" : 2882,"
      " \"ip\" : \"127.0.0.1\" } ], \"version\" : 1 }";

  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::decode(make_json(pretty_json), allocator, entries));
  ASSERT_EQ(OB_SUCCESS,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, canonical_json));
  expect_string_equal(canonical_json,
      "{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\",\"port\":2882,"
      "\"queue_count\":2}]}");
}

// 验证空 queue list 在校验、编码和解码路径均被拒绝。
TEST(TestDirectLoadStreamQueueList, reject_empty_queues)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueEntry> decoded_entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;

  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[]}"), allocator, decoded_entries));
}

// 验证非当前版本的 queue_list metadata 被拒绝。
TEST(TestDirectLoadStreamQueueList, reject_invalid_version)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);

  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":2,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":1}]}"),
                allocator, entries));
}

// 验证非法 IP 和端口不能进入 queue owner 拓扑。
TEST(TestDirectLoadStreamQueueList, reject_invalid_addr)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);

  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"bad-ip\","
                          "\"port\":2882,\"queue_count\":1}]}"),
                allocator, entries));
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":0,\"queue_count\":1}]}"),
                allocator, entries));
}

// 验证端口超过 TCP/UDP 上限时 validate 和 encode 均拒绝该 owner。
TEST(TestDirectLoadStreamQueueList, reject_out_of_range_queue_owner_port)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  ObAddr addr;

  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 65536));
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 1)));
  EXPECT_FALSE(entries.at(0).is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
}

// 验证 UNIX 地址不能作为持久 queue owner，避免 encode 产生无法 decode 的 JSON。
TEST(TestDirectLoadStreamQueueList, reject_unix_queue_owner_addr)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  ObAddr addr;

  ASSERT_TRUE(addr.set_unix_addr("/path/to/file"));
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 1)));
  EXPECT_FALSE(entries.at(0).is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
}

// 验证显式长度 JSON 中的 raw NUL 在 parser 截断前被拒绝。
TEST(TestDirectLoadStreamQueueList, reject_raw_nul_in_json)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  const char json_with_raw_nul[] =
      "{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\",\"port\":2882,"
      "\"queue_count\":1}]}\0trailing";

  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                ObString(static_cast<int32_t>(sizeof(json_with_raw_nul) - 1), json_with_raw_nul),
                allocator, entries));
}

// 验证 JSON 转义产生的内嵌 NUL 不会被地址解析接受。
TEST(TestDirectLoadStreamQueueList, reject_ip_with_embedded_nul)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);

  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\\u0000bad\","
                          "\"port\":2882,\"queue_count\":1}]}"),
                allocator, entries));
}

// 验证 queue_count 为 0 时内存校验和 metadata 解码均失败。
TEST(TestDirectLoadStreamQueueList, reject_non_positive_queue_count)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 0)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  entries.reset();
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":0}]}"),
                allocator, entries));
}

// 验证相同 server address 不能重复出现在 queue 拓扑中。
TEST(TestDirectLoadStreamQueueList, reject_duplicate_queue_owner_addr)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArray<ObDirectLoadStreamQueueEntry> decoded_entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 1)));
  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 2)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":1},"
                          "{\"ip\":\"127.0.0.1\",\"port\":2882,\"queue_count\":2}]}"),
                allocator, decoded_entries));
}

// 验证单个 owner 的 queue_count 超过总量上限时被拒绝。
TEST(TestDirectLoadStreamQueueList, reject_queue_count_overflow)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 65537)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  entries.reset();
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":65537}]}"),
                allocator, entries));
}

// 验证各 entry 均合法但累计 queue_count 超过总量上限时被拒绝。
TEST(TestDirectLoadStreamQueueList, reject_cumulative_queue_count_overflow)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  const ObAddr addr1 = make_addr("127.0.0.1", 2882);
  const ObAddr addr2 = make_addr("127.0.0.2", 2882);

  ASSERT_EQ(OB_SUCCESS,
            entries.push_back(ObDirectLoadStreamQueueEntry(addr1, 32768)));
  ASSERT_EQ(OB_SUCCESS,
            entries.push_back(ObDirectLoadStreamQueueEntry(addr2, 32769)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
  entries.reset();
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":32768},"
                          "{\"ip\":\"127.0.0.2\",\"port\":2882,"
                          "\"queue_count\":32769}]}"),
                allocator, entries));
}

TEST(TestDirectLoadStreamQueueList, reject_oversized_queue_list_json_on_encode)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;

  // 物理字段是 varchar:65536，encode 不能产生表字段写不下的 canonical JSON。
  for (int64_t i = 0; i < 2000; ++i) {
    ASSERT_EQ(OB_SUCCESS,
              entries.push_back(
                  ObDirectLoadStreamQueueEntry(
                      make_addr("10.0.0.1", 10000 + static_cast<int32_t>(i)), 1)));
  }
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
}

TEST(TestDirectLoadStreamQueueList, reject_oversized_queue_list_json_before_parse)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  const int64_t json_length = MAX_TEST_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH + 1;
  char *json_buf = static_cast<char *>(allocator.alloc(json_length));
  ASSERT_NE(nullptr, json_buf);
  memset(json_buf, 0x20, json_length);

  // 超长 varchar 必须在 JSON tree 构建前拒绝，避免污染元数据放大解析内存和 CPU。
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                ObString(static_cast<int32_t>(json_length), json_buf), allocator, entries));
}

TEST(TestDirectLoadStreamQueueList, reject_too_many_queue_owners)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString json;
  const ObAddr addr = make_addr("127.0.0.1", 2882);

  // owner 数量上限属于 queue_list 语义校验，不能只依赖 varchar 长度兜底。
  for (int64_t i = 0; i <= MAX_TEST_DIRECT_LOAD_STREAM_QUEUE_COUNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, entries.push_back(ObDirectLoadStreamQueueEntry(addr, 1)));
  }
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObDirectLoadStreamQueueListCodec::validate(entries));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObDirectLoadStreamQueueListCodec::encode(entries, allocator, json));
}

// 验证 JSON 输入首次分配失败时透传 allocator 错误。
TEST(TestDirectLoadStreamQueueList, decode_preserves_initial_allocator_failure)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObNullAllocator allocator;

  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":1}]}"),
                allocator, entries));
  EXPECT_EQ(0, entries.count());
}

// 验证 root 和 entry 的重复字段均按歧义 metadata 拒绝。
TEST(TestDirectLoadStreamQueueList, reject_duplicate_json_fields)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);

  // root 重复 queues 字段不能使用后值覆盖前值。
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":1}],"
                          "\"queues\":[{\"ip\":\"127.0.0.2\",\"port\":2882,"
                          "\"queue_count\":1}]}"),
                allocator, entries));

  // entry 重复 ip 字段不能使用后值覆盖前值。
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"ip\":\"127.0.0.2\",\"port\":2882,"
                          "\"queue_count\":1}]}"),
                allocator, entries));
}

// 验证普通 JSON 语法错误统一转换为 metadata 数据错误。
TEST(TestDirectLoadStreamQueueList, reject_malformed_json)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);

  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":["), allocator, entries));
  EXPECT_EQ(0, entries.count());
}

// 验证 root 未知字段和 entry 缺失字段均按坏 metadata 拒绝。
TEST(TestDirectLoadStreamQueueList, reject_unknown_or_missing_json_fields)
{
  ObArray<ObDirectLoadStreamQueueEntry> entries;
  ObArenaAllocator allocator(ObModIds::TEST);

  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\","
                          "\"port\":2882,\"queue_count\":1}],"
                          "\"extra\":1}"),
                allocator, entries));
  EXPECT_EQ(OB_INVALID_DATA,
            ObDirectLoadStreamQueueListCodec::decode(
                make_json("{\"version\":1,\"queues\":[{\"ip\":\"127.0.0.1\",\"port\":2882}]}"),
                allocator, entries));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::share::ObTenantBase tenant(oceanbase::common::OB_SYS_TENANT_ID);
  oceanbase::share::ObTenantSwitchGuard tenant_guard(&tenant);
  OB_LOGGER.set_file_name("test_direct_load_stream_queue_list.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
