/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>

#define USING_LOG_PREFIX SQL

#include "plugin/v2/external_table/ob_ext_json_protocol.h"
#include "sql/optimizer/file_prune/ob_i_lake_table_file_pruner.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#define private public
#include "sql/optimizer/file_prune/ob_ext_file_pruner.h"
#undef private

namespace oceanbase
{
namespace sql
{
namespace unittest
{

using namespace common;
using namespace share;

// Mirror the original task wire layout so compatibility is checked against
// an independent encoder and decoder, rather than another new-format task.
struct LegacyPluginScanTask : public ObFileScanTask
{
  OB_UNIS_VERSION_V(1);

public:
  LegacyPluginScanTask()
      : ObFileScanTask(LakeFileType::EXT_PLUGIN), legacy_reader_type_(0), assigned_file_bytes_(-1)
  {}

  int init_with_opt_lake_table_file(ObIAllocator &, const ObIOptLakeTableFile &) override
  {
    return OB_NOT_SUPPORTED;
  }

  ObString plugin_task_json_;
  int32_t legacy_reader_type_;
  int64_t assigned_file_bytes_;
};

OB_SERIALIZE_MEMBER((LegacyPluginScanTask, ObFileScanTask),
                    part_id_,
                    plugin_task_json_,
                    record_count_,
                    legacy_reader_type_,
                    assigned_file_bytes_);

struct LegacyPluginScanTaskNoReader : public ObFileScanTask
{
  OB_UNIS_VERSION_V(1);

public:
  LegacyPluginScanTaskNoReader() : ObFileScanTask(LakeFileType::EXT_PLUGIN) {}
  int init_with_opt_lake_table_file(ObIAllocator &, const ObIOptLakeTableFile &) override
  {
    return OB_NOT_SUPPORTED;
  }
  ObString plugin_task_json_;
};

OB_SERIALIZE_MEMBER((LegacyPluginScanTaskNoReader, ObFileScanTask),
                    part_id_, plugin_task_json_, record_count_);

struct LegacyPluginScanTaskNoAssignedBytes : public ObFileScanTask
{
  OB_UNIS_VERSION_V(1);

public:
  LegacyPluginScanTaskNoAssignedBytes()
      : ObFileScanTask(LakeFileType::EXT_PLUGIN), legacy_reader_type_(0)
  {}
  int init_with_opt_lake_table_file(ObIAllocator &, const ObIOptLakeTableFile &) override
  {
    return OB_NOT_SUPPORTED;
  }
  ObString plugin_task_json_;
  int32_t legacy_reader_type_;
};

OB_SERIALIZE_MEMBER((LegacyPluginScanTaskNoAssignedBytes, ObFileScanTask),
                    part_id_, plugin_task_json_, record_count_, legacy_reader_type_);

class TestExtPluginTask : public ::testing::Test
{
protected:
  TestExtPluginTask() : allocator_("ExtPluginUT") {}

  void check_round_trip(const ObPluginScanTask &source)
  {
    const int64_t size = source.get_serialize_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    int64_t pos = 0;
    ASSERT_NE(nullptr, buf);
    ASSERT_EQ(OB_SUCCESS, source.serialize(buf, size, pos));

    ObPluginScanTask target;
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS, target.deserialize(buf, pos, decode_pos));
    EXPECT_EQ(source.reader_type_, target.reader_type_);
    EXPECT_EQ(source.plugin_task_json_, target.plugin_task_json_);
    EXPECT_EQ(source.file_url_, target.file_url_);
    EXPECT_EQ(source.file_size_, target.file_size_);
    EXPECT_EQ(source.modification_time_, target.modification_time_);
    EXPECT_EQ(source.part_id_, target.part_id_);
    EXPECT_EQ(source.record_count_, target.record_count_);
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestExtPluginTask, ReaderFormatPreservesLegacyWireValues)
{
  const ObExternalFileFormat::FormatType formats[] = {
      ObExternalFileFormat::INVALID_FORMAT,
      ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT,
      ObExternalFileFormat::PARQUET_FORMAT,
      ObExternalFileFormat::ORC_FORMAT};
  for (int32_t wire_type = 0; wire_type < 4; ++wire_type) {
    SCOPED_TRACE(wire_type);
    LegacyPluginScanTask legacy;
    ObPluginScanTask current;
    legacy.file_url_ = current.file_url_ = ObString::make_string("data.parquet");
    legacy.plugin_task_json_ = current.plugin_task_json_ =
        ObString::make_string(R"({"plugin_split":"old-task"})");
    legacy.part_id_ = current.part_id_ = 11;
    legacy.file_size_ = current.file_size_ = 1024;
    legacy.record_count_ = current.record_count_ = 7;
    legacy.assigned_file_bytes_ = current.assigned_file_bytes_ = 4096;
    legacy.legacy_reader_type_ = wire_type;
    current.reader_type_ = formats[wire_type];

    char old_buf[4096];
    char new_buf[4096];
    int64_t old_len = 0;
    int64_t new_len = 0;
    ASSERT_EQ(OB_SUCCESS, legacy.serialize(old_buf, sizeof(old_buf), old_len));
    ASSERT_EQ(OB_SUCCESS, current.serialize(new_buf, sizeof(new_buf), new_len));
    ASSERT_EQ(legacy.get_serialize_size(), old_len);
    ASSERT_EQ(current.get_serialize_size(), new_len);
    ASSERT_EQ(old_len, new_len);
    EXPECT_EQ(0, MEMCMP(old_buf, new_buf, old_len));

    ObPluginScanTask decoded_current;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded_current.deserialize(old_buf, old_len, pos));
    EXPECT_EQ(old_len, pos);
    EXPECT_EQ(formats[wire_type], decoded_current.reader_type_);
    EXPECT_EQ(11, decoded_current.part_id_);
    EXPECT_EQ(4096, decoded_current.assigned_file_bytes_);

    LegacyPluginScanTask decoded_legacy;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded_legacy.deserialize(new_buf, new_len, pos));
    EXPECT_EQ(new_len, pos);
    EXPECT_EQ(wire_type, decoded_legacy.legacy_reader_type_);
    EXPECT_EQ(11, decoded_legacy.part_id_);
    EXPECT_EQ(4096, decoded_legacy.assigned_file_bytes_);
  }
}

TEST_F(TestExtPluginTask, ReaderFormatRejectsInvalidAndTruncatedWireData)
{
  LegacyPluginScanTask legacy;
  legacy.plugin_task_json_ = ObString::make_string(
      "012345678901234567890123456789012345678901234567890123456789");
  legacy.legacy_reader_type_ = 4;
  char buf[4096];
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(buf, sizeof(buf), len));

  ObPluginScanTask decoded;
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_DATA, decoded.deserialize(buf, len, pos));

  legacy.legacy_reader_type_ = 1;
  len = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(buf, sizeof(buf), len));
  pos = 0;
  EXPECT_NE(OB_SUCCESS, decoded.deserialize(buf, len / 2, pos));

  ObPluginScanTask invalid;
  invalid.reader_type_ = ObExternalFileFormat::ODPS_FORMAT;
  pos = 0;
  EXPECT_EQ(OB_INVALID_DATA, invalid.serialize(buf, sizeof(buf), pos));
  invalid.reader_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  pos = 0;
  EXPECT_EQ(OB_INVALID_DATA, invalid.serialize(buf, sizeof(buf), pos));
}

TEST_F(TestExtPluginTask, ReaderFormatAcceptsLegacyFieldBoundaryTruncation)
{
  char buf[4096];
  int64_t len = 0;
  LegacyPluginScanTaskNoReader without_reader;
  without_reader.part_id_ = 9;
  without_reader.plugin_task_json_ = ObString::make_string("legacy");
  without_reader.record_count_ = 13;
  ASSERT_EQ(OB_SUCCESS, without_reader.serialize(buf, sizeof(buf), len));
  ObPluginScanTask decoded_without_reader;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_without_reader.deserialize(buf, len, pos));
  EXPECT_EQ(len, pos);
  EXPECT_EQ(9, decoded_without_reader.part_id_);
  EXPECT_EQ(13, decoded_without_reader.record_count_);
  EXPECT_EQ(ObExternalFileFormat::INVALID_FORMAT, decoded_without_reader.reader_type_);
  EXPECT_EQ(-1, decoded_without_reader.assigned_file_bytes_);

  LegacyPluginScanTaskNoAssignedBytes without_bytes;
  without_bytes.part_id_ = 8;
  without_bytes.legacy_reader_type_ = 2;
  len = 0;
  ASSERT_EQ(OB_SUCCESS, without_bytes.serialize(buf, sizeof(buf), len));
  ObPluginScanTask decoded_without_bytes;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_without_bytes.deserialize(buf, len, pos));
  EXPECT_EQ(len, pos);
  EXPECT_EQ(8, decoded_without_bytes.part_id_);
  EXPECT_EQ(ObExternalFileFormat::PARQUET_FORMAT, decoded_without_bytes.reader_type_);
  EXPECT_EQ(-1, decoded_without_bytes.assigned_file_bytes_);
}

TEST_F(TestExtPluginTask, plugin_reader_task_keeps_root_json)
{
  ObOptPluginFile opt_file(allocator_);
  opt_file.reader_type_ = ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT;
  opt_file.plugin_task_json_ = ObString::make_string(R"({"plugin_split":"eA=="})");
  opt_file.part_id_ = 3;
  opt_file.record_count_ = 11;

  ObPluginScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init_with_opt_lake_table_file(allocator_, opt_file));
  EXPECT_EQ(ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT, task.reader_type_);
  EXPECT_FALSE(task.plugin_task_json_.empty());
  EXPECT_TRUE(task.file_url_.empty());
  EXPECT_EQ(11, task.record_count_);
  check_round_trip(task);
}

TEST_F(TestExtPluginTask, ob_file_task_is_a_physical_file)
{
  ObOptPluginFile opt_file(allocator_);
  opt_file.reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  opt_file.file_url_ = ObString::make_string("data.parquet");
  opt_file.file_size_ = 1024;
  opt_file.part_id_ = 7;
  opt_file.record_count_ = 37;

  ObPluginScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init_with_opt_lake_table_file(allocator_, opt_file));
  EXPECT_EQ(ObExternalFileFormat::PARQUET_FORMAT, task.reader_type_);
  EXPECT_TRUE(task.plugin_task_json_.empty());
  EXPECT_EQ(ObString::make_string("data.parquet"), task.file_url_);
  EXPECT_EQ(-1, task.record_count_);
  EXPECT_EQ(0, task.modification_time_);
  check_round_trip(task);
}

TEST_F(TestExtPluginTask, cache_version_survives_copy_and_px_transport)
{
  const char *formats[] = {"parquet", "orc"};
  for (const char *format : formats) {
    const std::string json = std::string(R"({"ob_file_scan":{"version":1,"file_format":")")
        + format + R"(","files":[{"path":"file","byte_size":10,"row_count":3,)"
        + R"("cache_version":1737111915429}]}})";
    ObExtFileScanDescriptor *descriptor = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObExtFileScanDescriptor::parse(
        allocator_, json.data(), json.size(), descriptor));
    ASSERT_NE(nullptr, descriptor);
    ASSERT_EQ(1, descriptor->file_count_);
    EXPECT_EQ(1737111915429LL, descriptor->files_[0].cache_version_);

    ObOptPluginFile source(allocator_);
    source.reader_type_ = descriptor->file_format_ == ObExtFileScanFormat::PARQUET
        ? ObExternalFileFormat::PARQUET_FORMAT : ObExternalFileFormat::ORC_FORMAT;
    source.file_url_ = descriptor->files_[0].file_path_;
    source.file_size_ = descriptor->files_[0].byte_size_;
    source.cache_version_ = descriptor->files_[0].cache_version_;
    source.record_count_ = descriptor->files_[0].row_count_;
    ObOptPluginFile copied(allocator_);
    ASSERT_EQ(OB_SUCCESS, copied.assign(source));
    source.reset();
    EXPECT_EQ(0, source.cache_version_);
    ObPluginScanTask task;
    ASSERT_EQ(OB_SUCCESS, task.init_with_opt_lake_table_file(allocator_, copied));
    EXPECT_EQ(1737111915429LL, task.modification_time_);
    EXPECT_EQ(-1, task.record_count_);  // Do not enable the metadata COUNT shortcut.
    check_round_trip(task);
  }
}

TEST_F(TestExtPluginTask, cache_version_requires_positive_int64)
{
  const char *valid_versions[] = {"1", "9223372036854775807"};
  const char *invalid_versions[] = {
      "0", "-1", "null", "true", "1.5", "\"123\"", "{}", "[]",
      "9223372036854775808", "18446744073709551615"};
  const std::string prefix = R"({"ob_file_scan":{"version":1,"file_format":"parquet",
      "files":[{"path":"a.parquet","byte_size":10,"row_count":3,"cache_version":)";
  for (const char *version : valid_versions) {
    SCOPED_TRACE(version);
    const std::string json = prefix + version + "}]}}";
    ObExtFileScanDescriptor *descriptor = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObExtFileScanDescriptor::parse(
        allocator_, json.data(), json.size(), descriptor));
    ASSERT_NE(nullptr, descriptor);
    EXPECT_GT(descriptor->files_[0].cache_version_, 0);
  }
  for (const char *version : invalid_versions) {
    SCOPED_TRACE(version);
    const std::string json = prefix + version + "}]}}";
    ObExtFileScanDescriptor *descriptor = nullptr;
    EXPECT_EQ(OB_INVALID_DATA, ObExtFileScanDescriptor::parse(
        allocator_, json.data(), json.size(), descriptor));
    EXPECT_EQ(nullptr, descriptor);
  }
}

TEST_F(TestExtPluginTask, descriptor_and_partition_protocol)
{
  const char *json = "{\"plugin_split\":\"eA==\","
                     "\"partition_values\":[{\"field_id\":1,\"value\":\"a\"}],"
                     "\"ob_file_scan\":{\"version\":1,\"file_format\":\"parquet\","
                     "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10,"
                     "\"row_count\":3},{\"path\":\"b.parquet\","
                     "\"byte_size\":20,\"row_count\":4}]}}";
  ObExtFileScanDescriptor *descriptor = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObExtFileScanDescriptor::parse(
      allocator_, json, STRLEN(json), descriptor));
  ASSERT_NE(nullptr, descriptor);
  EXPECT_EQ(ObExtFileScanFormat::PARQUET, descriptor->file_format_);
  ASSERT_EQ(2, descriptor->file_count_);
  EXPECT_EQ(ObString::make_string("a.parquet"), descriptor->files_[0].file_path_);
  EXPECT_EQ(3, descriptor->files_[0].row_count_);
  EXPECT_EQ(0, descriptor->files_[0].cache_version_);
  EXPECT_EQ(ObString::make_string("b.parquet"), descriptor->files_[1].file_path_);
  EXPECT_EQ(4, descriptor->files_[1].row_count_);
  EXPECT_EQ(0, descriptor->files_[1].cache_version_);

  ObExtTaskPartitionValues values;
  ASSERT_EQ(OB_SUCCESS, ObExtTaskPartitionValues::parse(
      allocator_, json, STRLEN(json), values));
  ASSERT_EQ(1, values.count_);
  EXPECT_EQ(1, values.values_[0].field_id_);
  EXPECT_EQ(ObString::make_string("a"), values.values_[0].value_);
}

TEST_F(TestExtPluginTask, absent_ob_file_scan_keeps_plugin_reader)
{
  const char *json = R"({"plugin_split":"eA=="})";
  ObExtFileScanDescriptor *descriptor = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObExtFileScanDescriptor::parse(
      allocator_, json, STRLEN(json), descriptor));
  EXPECT_EQ(nullptr, descriptor);
}

TEST_F(TestExtPluginTask, invalid_ob_file_scan_is_error)
{
  const char *invalid[] = {
      "{\"ob_file_scan\":{\"version\":99,\"file_format\":\"parquet\","
      "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10,\"row_count\":3}]}}",
      "{\"ob_file_scan\":{\"version\":1,\"file_format\":\"parquet\","
      "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10}]}}",
      "{\"ob_file_scan\":{\"version\":1,\"file_format\":\"parquet\","
      "\"files\":[{\"path\":\"a.parquet\",\"byte_size\":10,\"row_count\":-1}]}}",
  };
  ObExtFileScanDescriptor *descriptor = nullptr;
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(invalid) / sizeof(invalid[0])); ++i) {
    ASSERT_EQ(OB_INVALID_DATA, ObExtFileScanDescriptor::parse(
        allocator_, invalid[i], STRLEN(invalid[i]), descriptor));
    EXPECT_EQ(nullptr, descriptor);
  }

  const char *malformed = "{";
  EXPECT_NE(OB_SUCCESS, ObExtFileScanDescriptor::parse(
      allocator_, malformed, STRLEN(malformed), descriptor));
  EXPECT_EQ(nullptr, descriptor);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObExtFileScanDescriptor::parse(
      allocator_, nullptr, 0, descriptor));
  EXPECT_EQ(nullptr, descriptor);
}

TEST_F(TestExtPluginTask, ParseAndLowerMixedTasks)
{
  ObArenaAllocator arena;
  const char *json = R"json({"tasks":[
    {"plugin_split":"cGFycXVldA==","row_count":7,"byte_size":30,
     "partition_values":[{"field_id":1,"value":"a"}],
     "ob_file_scan":{"version":1,"file_format":"parquet","files":[
       {"path":"a.parquet","byte_size":10,"row_count":3,"cache_version":101},
       {"path":"b.parquet","byte_size":20,"row_count":4,"cache_version":202}]}},
    {"plugin_split":"c2Rr","row_count":11,"byte_size":40,
     "partition_values":[{"field_id":1,"value":"a"}]}
  ]})json";
  ObExtScanTaskArray parsed;
  ASSERT_EQ(OB_SUCCESS,
            parse_scan_tasks_json(arena, json, STRLEN(json), parsed));
  ASSERT_EQ(2, parsed.count);
  ObExtTaskPartitionValues partition;
  ASSERT_EQ(
      OB_SUCCESS,
      ObExtTaskPartitionValues::parse(
          arena,
          parsed.tasks[0].task_json,
          parsed.tasks[0].task_json_len,
          partition));
  ASSERT_EQ(1, partition.count_);
  EXPECT_EQ(1, partition.values_[0].field_id_);
  EXPECT_EQ(ObString::make_string("a"), partition.values_[0].value_);

  ObExtFilePruner pruner(arena);
  ObSEArray<uint64_t, 1> partition_col_ids;
  ASSERT_EQ(OB_SUCCESS, partition_col_ids.push_back(OB_APP_MIN_COLUMN_ID + 1));
  ObSEArray<ObRawExpr *, 1> partition_filters;
  ObSEArray<ObPluginSplitDesc *, 3> splits;
  ASSERT_EQ(
      OB_SUCCESS,
      pruner.build_plugin_splits(
          json,
          static_cast<int32_t>(STRLEN(json)),
          partition_col_ids,
          partition_filters,
          ObString(),
          ObString(),
          splits));
  ASSERT_EQ(3, splits.count());
  const int64_t bytes[] = {10, 20, 40};
  const int64_t rows[] = {3, 4, 11};
  const int64_t cache_versions[] = {101, 202, 0};
  for (int i = 0; i < 3; ++i) {
    ASSERT_NE(nullptr, splits.at(i));
    EXPECT_EQ(
        i == 2 ? ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT : ObExternalFileFormat::PARQUET_FORMAT,
        splits.at(i)->reader_type_);
    EXPECT_EQ(bytes[i], splits.at(i)->assigned_file_bytes_);
    EXPECT_EQ(rows[i], splits.at(i)->record_count_);
    EXPECT_EQ(cache_versions[i], splits.at(i)->cache_version_);
    EXPECT_EQ(i < 2 ? 1 : 2, splits.at(i)->part_id_);
  }
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(ObString::make_string(i == 0 ? "a.parquet" : "b.parquet"), splits.at(i)->file_url_);
    EXPECT_EQ(bytes[i], splits.at(i)->file_size_);
    EXPECT_TRUE(splits.at(i)->plugin_task_json_.empty());
  }
  // The SDK split stays atomic and retains its complete JSON.
  EXPECT_EQ(
      ObString(parsed.tasks[1].task_json_len, parsed.tasks[1].task_json),
      splits.at(2)->plugin_task_json_);
  EXPECT_TRUE(splits.at(2)->file_url_.empty());
  EXPECT_EQ(0, splits.at(2)->file_size_);
  const char *invalid = R"({"ob_file_scan":{"version":99,"file_format":"parquet",
    "files":[{"path":"bad.parquet","byte_size":10,"row_count":3}]}})";
  ObExtFileScanDescriptor *descriptor = nullptr;
  EXPECT_EQ(
      OB_INVALID_DATA,
      ObExtFileScanDescriptor::parse(arena, invalid, STRLEN(invalid), descriptor));
  EXPECT_EQ(nullptr, descriptor);
}

TEST_F(TestExtPluginTask, CopyAndSerializationRoundTrip)
{
  ObArenaAllocator arena;
  ObOptPluginFile native(arena);
  native.reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  native.file_url_ = ObString::make_string("data.parquet");
  native.file_size_ = 1024;
  native.cache_version_ = 1737111915429LL;
  native.assigned_file_bytes_ = 1024;
  native.part_id_ = 7;
  native.record_count_ = 37;
  ObOptPluginFile sdk(arena);
  sdk.reader_type_ = ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT;
  sdk.plugin_task_json_ = ObString::make_string(
      R"({"plugin_split":"eA==","partition_values":[{"field_id":1,"value":"a"}],"byte_size":3072})");
  sdk.assigned_file_bytes_ = 3072;
  sdk.part_id_ = 3;
  sdk.record_count_ = 11;
  for (const auto *source : {&native, &sdk}) {
    ObOptPluginFile copied(arena);
    ASSERT_EQ(OB_SUCCESS, copied.assign(*source));
    EXPECT_EQ(source->reader_type_, copied.reader_type_);
    EXPECT_EQ(source->file_url_, copied.file_url_);
    EXPECT_EQ(source->plugin_task_json_, copied.plugin_task_json_);
    EXPECT_EQ(source->file_size_, copied.file_size_);
    EXPECT_EQ(source->cache_version_, copied.cache_version_);
    EXPECT_EQ(source->assigned_file_bytes_, copied.assigned_file_bytes_);
    EXPECT_EQ(source->part_id_, copied.part_id_);
    EXPECT_EQ(source->record_count_, copied.record_count_);
    ObPluginScanTask task;
    ASSERT_EQ(OB_SUCCESS, task.init_with_opt_lake_table_file(arena, copied));
    const int64_t size = task.get_serialize_size();
    char *buf = static_cast<char *>(arena.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, task.serialize(buf, size, pos));
    ObPluginScanTask decoded;
    int64_t decode_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
    EXPECT_EQ(pos, decode_pos);
    for (const auto *result : {&task, &decoded}) {
      EXPECT_EQ(source->reader_type_, result->reader_type_);
      EXPECT_EQ(source->file_url_, result->file_url_);
      EXPECT_EQ(source->plugin_task_json_, result->plugin_task_json_);
      EXPECT_EQ(source->file_size_, result->file_size_);
      EXPECT_EQ(source->cache_version_, result->modification_time_);
      EXPECT_EQ(source->assigned_file_bytes_, result->assigned_file_bytes_);
      EXPECT_EQ(source->part_id_, result->part_id_);
      EXPECT_EQ(source == &sdk ? 11 : -1, result->record_count_);
    }
  }
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
