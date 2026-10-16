/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/table/ob_direct_load_stream_queue_list.h"

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_sort.h"
#include "share/rc/ob_tenant_base.h"

#include <stdint.h>

namespace oceanbase
{
namespace table
{
namespace
{

// codec 防御上限，不代表产品支持的 queue/PX 并行度。
const int64_t MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT = 65536;
// queue_list 物理字段为 varchar:65536，codec 也按同一 byte 上限拒绝异常输入和输出。
const int64_t MAX_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH = 65536;

bool is_invalid_json_error(const int ret)
{
  return OB_ERR_INVALID_JSON_TEXT == ret
      || OB_ERR_INVALID_JSON_TEXT_IN_PARAM == ret
      || OB_ERR_JSON_OUT_OF_DEPTH == ret
      || OB_ERR_DUPLICATE_KEY == ret;
}

int get_required_json_field(const common::ObIJsonBase &object, const char *key,
                            common::ObIJsonBase *&value)
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_UNLIKELY(object.json_type() != common::ObJsonNodeType::J_OBJECT)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("json node is not object", K(ret), K(object.json_type()));
  } else if (OB_FAIL(object.get_object_value(key, value))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("fail to get required json field", K(ret), K(key));
  }
  return ret;
}

int get_int64_value(const common::ObIJsonBase &object, const char *key, int64_t &value)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *node = nullptr;
  value = 0;
  if (OB_FAIL(get_required_json_field(object, key, node))) {
  } else if (node->json_type() == common::ObJsonNodeType::J_INT) {
    value = node->get_int();
  } else if (node->json_type() == common::ObJsonNodeType::J_UINT) {
    const uint64_t uint_value = node->get_uint();
    if (OB_UNLIKELY(uint_value > static_cast<uint64_t>(INT64_MAX))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("json integer value is too large", K(ret), K(key), K(uint_value));
    } else {
      value = static_cast<int64_t>(uint_value);
    }
  } else {
    ret = OB_INVALID_DATA;
    LOG_WARN("json field is not integer", K(ret), K(key), K(node->json_type()));
  }
  return ret;
}

int get_string_value(const common::ObIJsonBase &object, const char *key, common::ObString &value)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *node = nullptr;
  value.reset();
  if (OB_FAIL(get_required_json_field(object, key, node))) {
  } else if (node->json_type() != common::ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_DATA;
    LOG_WARN("json field is not string", K(ret), K(key), K(node->json_type()));
  } else {
    value.assign_ptr(node->get_data(), node->get_data_length());
  }
  return ret;
}

bool has_nul_char(const common::ObString &str)
{
  return nullptr != str.find('\0');
}

int parse_queue_entry(const common::ObIJsonBase &entry_node, ObDirectLoadStreamQueueEntry &entry)
{
  int ret = OB_SUCCESS;
  common::ObString ip;
  int64_t port = 0;
  int64_t queue_count = 0;
  common::ObAddr server_addr;
  if (OB_UNLIKELY(entry_node.json_type() != common::ObJsonNodeType::J_OBJECT
                  || entry_node.element_count() != 3)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid queue entry json object", K(ret), K(entry_node.json_type()),
             K(entry_node.element_count()));
  } else if (OB_FAIL(get_string_value(entry_node, "ip", ip))) {
  } else if (OB_FAIL(get_int64_value(entry_node, "port", port))) {
  } else if (OB_FAIL(get_int64_value(entry_node, "queue_count", queue_count))) {
  } else if (OB_UNLIKELY(ip.empty() || port <= 0 || port > 65535 || queue_count <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid queue entry value", K(ret), K(ip), K(port), K(queue_count));
  } else if (OB_UNLIKELY(has_nul_char(ip))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("queue owner ip contains nul char", K(ret), K(ip), K(port));
  } else if (OB_UNLIKELY(!server_addr.set_ip_addr(ip, static_cast<int32_t>(port)))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("fail to parse queue owner address", K(ret), K(ip), K(port));
  } else {
    entry.server_addr_ = server_addr;
    entry.queue_count_ = queue_count;
    if (OB_UNLIKELY(!entry.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("queue entry is invalid", K(ret), K(entry));
    }
  }
  return ret;
}

} // namespace

ObDirectLoadStreamQueueEntry::ObDirectLoadStreamQueueEntry()
  : server_addr_(), queue_count_(0)
{
}

ObDirectLoadStreamQueueEntry::ObDirectLoadStreamQueueEntry(
    const common::ObAddr &server_addr, const int64_t queue_count)
  : server_addr_(server_addr), queue_count_(queue_count)
{
}

bool ObDirectLoadStreamQueueEntry::is_valid() const
{
  return server_addr_.is_valid()
      && (server_addr_.using_ipv4() || server_addr_.using_ipv6())
      && server_addr_.get_port() > 0
      && server_addr_.get_port() <= 65535
      && queue_count_ > 0;
}

ObDirectLoadStreamQueueDesc::ObDirectLoadStreamQueueDesc()
  : queue_id_(OB_INVALID_ID), server_addr_()
{
}

ObDirectLoadStreamQueueDesc::ObDirectLoadStreamQueueDesc(
    const uint64_t queue_id, const common::ObAddr &server_addr)
  : queue_id_(queue_id), server_addr_(server_addr)
{
}

bool ObDirectLoadStreamQueueDesc::is_valid() const
{
  return OB_INVALID_ID != queue_id_
      && server_addr_.is_valid()
      && (server_addr_.using_ipv4() || server_addr_.using_ipv6())
      && server_addr_.get_port() > 0
      && server_addr_.get_port() <= 65535;
}

int ObDirectLoadStreamQueueListCodec::encode(
    const common::ObIArray<ObDirectLoadStreamQueueEntry> &entries,
    common::ObIAllocator &allocator, common::ObString &json)
{
  int ret = OB_SUCCESS;
  common::ObSqlString output;
  json.reset();
  if (OB_FAIL(validate(entries))) {
    LOG_WARN("invalid stream queue list entries", K(ret), K(entries.count()));
  } else if (OB_FAIL(output.append_fmt("{\"version\":1,\"queues\":["))) {
    LOG_WARN("fail to append queue list json prefix", K(ret));
  } else {
    char ip_buf[common::OB_IP_STR_BUFF];
    for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
      const ObDirectLoadStreamQueueEntry &entry = entries.at(i);
      MEMSET(ip_buf, 0, sizeof(ip_buf));
      if (OB_UNLIKELY(!entry.server_addr_.ip_to_string(ip_buf, sizeof(ip_buf)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to stringify queue owner address", K(ret), K(entry));
      } else if (OB_FAIL(output.append_fmt(
                     "%s{\"ip\":\"%s\",\"port\":%d,\"queue_count\":%ld}",
                     0 == i ? "" : ",", ip_buf, entry.server_addr_.get_port(),
                     entry.queue_count_))) {
        LOG_WARN("fail to append queue entry json", K(ret), K(i), K(entry));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(output.append_fmt("]}"))) {
        LOG_WARN("fail to append queue list json suffix", K(ret));
      } else if (OB_UNLIKELY(output.length() > MAX_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("queue list json length exceeds implementation limit",
                 K(ret), K(output.length()), K(MAX_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH));
      } else if (OB_FAIL(common::ob_write_string(allocator, output.string(), json))) {
        LOG_WARN("fail to copy queue list json", K(ret), K(output.length()));
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamQueueListCodec::decode(
    const common::ObString &json, common::ObIAllocator &allocator,
    common::ObIArray<ObDirectLoadStreamQueueEntry> &entries)
{
  int ret = OB_SUCCESS;
  common::ObIJsonBase *root = nullptr;
  common::ObIJsonBase *queues_node = nullptr;
  common::ObIJsonBase *entry_node = nullptr;
  common::ObSEArray<ObDirectLoadStreamQueueEntry, 4> tmp_entries;
  int64_t version = 0;
  const int64_t json_length = json.length();
  const uint32_t parse_flag = common::ObJsonParser::JSN_UNIQUE_FLAG;
  if (OB_UNLIKELY(json.empty())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("queue list json is empty", K(ret));
  } else if (OB_UNLIKELY(json_length > MAX_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("queue list json length exceeds implementation limit",
             K(ret), K(json_length), K(MAX_DIRECT_LOAD_STREAM_QUEUE_LIST_JSON_LENGTH));
  } else if (OB_UNLIKELY(has_nul_char(json))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("queue list json contains raw nul char", K(ret), K(json_length));
  } else if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
                 &allocator, json, common::ObJsonInType::JSON_TREE,
                 common::ObJsonInType::JSON_TREE, root, parse_flag))) {
    const int parse_ret = ret;
    if (is_invalid_json_error(parse_ret)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid queue list json text", K(ret), K(parse_ret), K(json_length));
    } else {
      LOG_WARN("fail to build queue list json tree", K(ret), K(json_length));
    }
  } else if (OB_UNLIKELY(root->json_type() != common::ObJsonNodeType::J_OBJECT
                         || root->element_count() != 2)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid queue list root object", K(ret), K(root->json_type()),
             K(root->element_count()));
  } else if (OB_FAIL(get_int64_value(*root, "version", version))) {
  } else if (OB_UNLIKELY(1 != version)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("unsupported queue list json version", K(ret), K(version));
  } else if (OB_FAIL(get_required_json_field(*root, "queues", queues_node))) {
  } else if (OB_UNLIKELY(queues_node->json_type() != common::ObJsonNodeType::J_ARRAY
                         || 0 == queues_node->element_count())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid queue list array", K(ret), K(queues_node->json_type()),
             K(queues_node->element_count()));
  } else if (OB_UNLIKELY(
                 queues_node->element_count() >
                 static_cast<uint64_t>(MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("queue list owner count exceeds implementation limit", K(ret),
             K(queues_node->element_count()));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < queues_node->element_count(); ++i) {
      ObDirectLoadStreamQueueEntry entry;
      if (OB_FAIL(queues_node->get_array_element(i, entry_node))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("fail to get queue entry json node", K(ret), K(i));
      } else if (OB_ISNULL(entry_node)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("queue entry json node is null", K(ret), K(i));
      } else if (OB_FAIL(parse_queue_entry(*entry_node, entry))) {
        LOG_WARN("fail to parse queue entry json", K(ret), K(i));
      } else if (OB_FAIL(tmp_entries.push_back(entry))) {
        LOG_WARN("fail to push parsed queue entry", K(ret), K(i), K(entry));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(validate(tmp_entries))) {
        if (OB_INVALID_ARGUMENT == ret) {
          ret = OB_INVALID_DATA;
        }
        LOG_WARN("parsed queue list is invalid", K(ret), K(tmp_entries.count()));
      } else if (OB_FAIL(entries.assign(tmp_entries))) {
        LOG_WARN("fail to assign decoded queue entries", K(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadStreamQueueListCodec::validate(
    const common::ObIArray<ObDirectLoadStreamQueueEntry> &entries)
{
  int ret = OB_SUCCESS;
  int64_t total_queue_count = 0;
  common::hash::ObHashSet<common::ObAddr> server_addrs;
  if (OB_UNLIKELY(entries.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("queue list is empty", K(ret));
  } else if (OB_UNLIKELY(entries.count() > MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("queue owner count exceeds implementation limit", K(ret), K(entries.count()));
  } else if (OB_FAIL(server_addrs.create(
                 entries.count(), common::ObMemAttr(MTL_ID(), "DLStreamQValid")))) {
    LOG_WARN("fail to create queue owner address set", K(ret), K(entries.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
    const ObDirectLoadStreamQueueEntry &entry = entries.at(i);
    if (OB_UNLIKELY(!entry.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid queue entry", K(ret), K(i), K(entry));
    } else if (OB_HASH_EXIST ==
               (ret = server_addrs.set_refactored(entry.server_addr_, 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("duplicate queue owner address", K(ret), K(i), K(entry));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to add queue owner address", K(ret), K(i), K(entry));
    } else if (OB_UNLIKELY(
                   entry.queue_count_ > MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT
                   || total_queue_count >
                       MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT - entry.queue_count_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("queue count exceeds implementation limit", K(ret), K(i),
               K(total_queue_count), K(entry));
    } else {
      total_queue_count += entry.queue_count_;
    }
  }
  return ret;
}

int ObDirectLoadStreamQueueListCodec::expand(
    const common::ObIArray<ObDirectLoadStreamQueueEntry> &entries,
    common::ObIArray<ObDirectLoadStreamQueueDesc> &queue_descs)
{
  int ret = OB_SUCCESS;
  int64_t total_queue_count = 0;
  common::ObSEArray<ObDirectLoadStreamQueueDesc, 16> tmp_queue_descs;
  queue_descs.reset();
  if (OB_FAIL(validate(entries))) {
    LOG_WARN("invalid stream queue list entries", K(ret), K(entries.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < entries.count(); ++i) {
    const ObDirectLoadStreamQueueEntry &entry = entries.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < entry.queue_count_; ++j) {
      const ObDirectLoadStreamQueueDesc queue_desc(
          static_cast<uint64_t>(total_queue_count + j), entry.server_addr_);
      if (OB_FAIL(tmp_queue_descs.push_back(queue_desc))) {
        LOG_WARN("fail to append expanded queue descriptor", K(ret), K(i), K(j));
      }
    }
    if (OB_SUCC(ret)) {
      total_queue_count += entry.queue_count_;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(queue_descs.assign(tmp_queue_descs))) {
    LOG_WARN("fail to publish expanded queue descriptors", K(ret), K(total_queue_count));
  }
  if (OB_SUCCESS != ret) {
    queue_descs.reset();
  }
  return ret;
}

int ObDirectLoadStreamQueueListCodec::build_topology(
    const common::ObIArray<common::ObAddr> &alive_servers,
    const int64_t stream_id,
    const int64_t resource_budget,
    common::ObIArray<ObDirectLoadStreamQueueEntry> &entries)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObAddr, 8> sorted_servers;
  common::ObSEArray<ObDirectLoadStreamQueueEntry, 8> tmp_entries;
  common::ObSEArray<ObDirectLoadStreamQueueDesc, 16> queue_descs;
  common::hash::ObHashSet<common::ObAddr> server_set;
  entries.reset();
  if (OB_UNLIKELY(stream_id <= 0 || resource_budget <= 0
                  || resource_budget > MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT
                  || alive_servers.empty()
                  || alive_servers.count() > MAX_DIRECT_LOAD_STREAM_QUEUE_COUNT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stream queue topology build argument",
             K(ret), K(stream_id), K(resource_budget), K(alive_servers.count()));
  } else if (OB_FAIL(server_set.create(
                 alive_servers.count(), common::ObMemAttr(MTL_ID(), "DLStreamTopo")))) {
    LOG_WARN("fail to create alive server set", K(ret), K(alive_servers.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < alive_servers.count(); ++i) {
    const common::ObAddr &server = alive_servers.at(i);
    if (OB_UNLIKELY(!server.is_valid()
                    || (!server.using_ipv4() && !server.using_ipv6())
                    || server.get_port() <= 0 || server.get_port() > 65535)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid alive server for stream queue topology", K(ret), K(i), K(server));
    } else if (OB_HASH_EXIST == (ret = server_set.set_refactored(server, 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("duplicate alive server for stream queue topology", K(ret), K(i), K(server));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("fail to add alive server to stream topology set", K(ret), K(i), K(server));
    } else if (OB_FAIL(sorted_servers.push_back(server))) {
      LOG_WARN("fail to copy alive server for stream queue topology", K(ret), K(i), K(server));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(&sorted_servers.at(0),
                 &sorted_servers.at(0) + sorted_servers.count());
    const int64_t owner_count = resource_budget < sorted_servers.count()
        ? resource_budget : sorted_servers.count();
    const int64_t base_queue_count = resource_budget / owner_count;
    const int64_t remainder = resource_budget % owner_count;
    const uint64_t stream_hash = common::murmurhash(&stream_id, sizeof(stream_id), 0);
    const int64_t start_idx = static_cast<int64_t>(
        stream_hash % static_cast<uint64_t>(sorted_servers.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < owner_count; ++i) {
      const int64_t server_idx = (start_idx + i) % sorted_servers.count();
      const int64_t queue_count = base_queue_count + (i < remainder ? 1 : 0);
      const ObDirectLoadStreamQueueEntry entry(sorted_servers.at(server_idx), queue_count);
      if (OB_UNLIKELY(!entry.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("generated invalid stream queue entry", K(ret), K(i), K(entry));
      } else if (OB_FAIL(tmp_entries.push_back(entry))) {
        LOG_WARN("fail to append generated stream queue entry", K(ret), K(i), K(entry));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expand(tmp_entries, queue_descs))) {
      LOG_WARN("fail to verify generated stream queue topology", K(ret), K(stream_id));
    } else if (OB_UNLIKELY(queue_descs.count() != resource_budget)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated stream queue topology budget mismatch",
               K(ret), K(queue_descs.count()), K(resource_budget));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue_descs.count(); ++i) {
    if (OB_UNLIKELY(queue_descs.at(i).queue_id_ != static_cast<uint64_t>(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated stream queue id is not contiguous", K(ret), K(i), K(queue_descs.at(i)));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(entries.assign(tmp_entries))) {
    LOG_WARN("fail to publish generated stream queue topology", K(ret), K(stream_id));
  }
  if (OB_SUCCESS != ret) {
    entries.reset();
  }
  return ret;
}

} // namespace table
} // namespace oceanbase
