/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <memory>
#include "ob_log_config.h"
#include "ob_log_instance.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
class ObTestUnsupportedLobHandlingMode : public ::testing::Test
{
protected:
  void SetUp() override
  {
    config_.reset(new ObLogConfig());
    ASSERT_EQ(OB_SUCCESS, config_->init());
    ASSERT_EQ(OB_SUCCESS, config_->check_all());
  }

  std::unique_ptr<ObLogConfig> config_;
};

TEST_F(ObTestUnsupportedLobHandlingMode, default_is_error)
{
  EXPECT_STREQ("ERROR", config_->unsupported_lob_handling_mode.str());
  ObUnsupportedLobHandlingMode mode = ObUnsupportedLobHandlingMode::PADDING;
  ASSERT_EQ(OB_SUCCESS, parse_unsupported_lob_handling_mode(
      config_->unsupported_lob_handling_mode.str(), mode));
  EXPECT_EQ(ObUnsupportedLobHandlingMode::ERROR, mode);
}

TEST_F(ObTestUnsupportedLobHandlingMode, checker_and_parser_accept_case_variants)
{
  const char *values[] = {"ERROR", "error", "Error", "eRrOr", "PADDING", "padding", "Padding", "pAdDiNg"};
  for (int64_t i = 0; i < ARRAYSIZEOF(values); ++i) {
    SCOPED_TRACE(values[i]);
    std::map<std::string, std::string> configs;
    configs["unsupported_lob_handling_mode"] = values[i];
    ASSERT_EQ(OB_SUCCESS, config_->load_from_map(configs));
    EXPECT_EQ(OB_SUCCESS, config_->check_all());
    ObUnsupportedLobHandlingMode mode = ObUnsupportedLobHandlingMode::ERROR;
    ASSERT_EQ(OB_SUCCESS, parse_unsupported_lob_handling_mode(
        config_->unsupported_lob_handling_mode.str(), mode));
    EXPECT_EQ(i < 4 ? ObUnsupportedLobHandlingMode::ERROR : ObUnsupportedLobHandlingMode::PADDING, mode);
  }
}

TEST_F(ObTestUnsupportedLobHandlingMode, invalid_effective_values_fail_validation_without_fallback)
{
  const char *values[] = {"", "PADDDING", "IGNORE", "NULL", "1", " ERROR", "PADDING ", "ERROR,PADDING"};
  for (int64_t i = 0; i < ARRAYSIZEOF(values); ++i) {
    SCOPED_TRACE(values[i]);
    std::map<std::string, std::string> configs;
    configs["unsupported_lob_handling_mode"] = values[i];
    ASSERT_EQ(OB_SUCCESS, config_->load_from_map(configs));
    EXPECT_EQ(OB_INVALID_CONFIG, config_->check_all());
    ObUnsupportedLobHandlingMode mode = ObUnsupportedLobHandlingMode::PADDING;
    EXPECT_EQ(OB_INVALID_CONFIG, parse_unsupported_lob_handling_mode(
        config_->unsupported_lob_handling_mode.str(), mode));
    EXPECT_EQ(ObUnsupportedLobHandlingMode::PADDING, mode);
  }
  ObUnsupportedLobHandlingMode mode = ObUnsupportedLobHandlingMode::ERROR;
  EXPECT_EQ(OB_INVALID_ARGUMENT, parse_unsupported_lob_handling_mode(nullptr, mode));
  EXPECT_EQ(ObUnsupportedLobHandlingMode::ERROR, mode);
}

TEST_F(ObTestUnsupportedLobHandlingMode, buffer_loader_keeps_existing_empty_value_behavior)
{
  const char *empty = "unsupported_lob_handling_mode=\n";
  ASSERT_EQ(OB_SUCCESS, config_->load_from_buffer(empty, strlen(empty)));
  EXPECT_STREQ("ERROR", config_->unsupported_lob_handling_mode.str());
  EXPECT_EQ(OB_SUCCESS, config_->check_all());

  const char *padding = "unsupported_lob_handling_mode=PaDdInG\n";
  ASSERT_EQ(OB_SUCCESS, config_->load_from_buffer(padding, strlen(padding)));
  EXPECT_STREQ("PaDdInG", config_->unsupported_lob_handling_mode.str());
  ASSERT_EQ(OB_SUCCESS, config_->load_from_buffer(empty, strlen(empty)));
  EXPECT_STREQ("PaDdInG", config_->unsupported_lob_handling_mode.str());
  EXPECT_EQ(OB_SUCCESS, config_->check_all());
}

TEST_F(ObTestUnsupportedLobHandlingMode, buffer_loader_rejects_invalid_value_at_check_all)
{
  const char *invalid = "unsupported_lob_handling_mode=IGNORE\n";
  ASSERT_EQ(OB_SUCCESS, config_->load_from_buffer(invalid, strlen(invalid)));
  EXPECT_STREQ("IGNORE", config_->unsupported_lob_handling_mode.str());
  EXPECT_EQ(OB_INVALID_CONFIG, config_->check_all());
}

class ObLobPolicyInstance : public ObLogInstance
{
public:
  ObLobPolicyInstance() : ObLogInstance() {}
};

TEST_F(ObTestUnsupportedLobHandlingMode, instance_uses_fixed_parsed_mode)
{
  std::unique_ptr<ObLobPolicyInstance> instance(new ObLobPolicyInstance());
  EXPECT_FALSE(instance->is_unsupported_lob_padding_mode());
  ASSERT_TRUE(config_->unsupported_lob_handling_mode.set_value("Padding"));
  ASSERT_EQ(OB_SUCCESS, parse_unsupported_lob_handling_mode(
      config_->unsupported_lob_handling_mode.str(), instance->unsupported_lob_handling_mode_));
  EXPECT_TRUE(instance->is_unsupported_lob_padding_mode());

  // Reloading the string item does not mutate the policy already used by workers.
  std::map<std::string, std::string> configs;
  configs["unsupported_lob_handling_mode"] = "ERROR";
  ASSERT_EQ(OB_SUCCESS, config_->load_from_map(configs));
  ASSERT_EQ(OB_SUCCESS, config_->check_all());
  EXPECT_TRUE(instance->is_unsupported_lob_padding_mode());
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  system("rm -f ./test_unsupported_lob_handling_mode.log*");
  logger.set_file_name("test_unsupported_lob_handling_mode.log", true /*no_redirect_flag*/);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
