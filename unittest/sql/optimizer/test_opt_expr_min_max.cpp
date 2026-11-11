/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <cstring>
#include "lib/oblog/ob_warning_buffer.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class MinMaxWarningScope
{
public:
  MinMaxWarningScope()
      : warning_buffer_(), old_buffer_(ob_get_tsi_warning_buffer()),
        old_warn_log_on_(ObWarningBuffer::is_warn_log_on())
  {
    ob_setup_tsi_warning_buffer(&warning_buffer_);
    ObWarningBuffer::set_warn_log_on(true);
  }
  ~MinMaxWarningScope()
  {
    ob_setup_tsi_warning_buffer(old_buffer_);
    ObWarningBuffer::set_warn_log_on(old_warn_log_on_);
  }
  ObWarningBuffer warning_buffer_;

private:
  ObWarningBuffer *old_buffer_;
  bool old_warn_log_on_;
};

class TestOptExprMinMax : public ::testing::Test
{
public:
  TestOptExprMinMax()
      : tenant_(OB_SYS_TENANT_ID, 0), previous_tenant_(nullptr), lob_manager_(nullptr),
        allocator_(ObMemAttr(OB_SYS_TENANT_ID, "OptMinMaxUT")), exec_ctx_(allocator_), addr_(),
        global_hint_(allocator_), expr_factory_(allocator_),
        opt_ctx_(nullptr, &exec_ctx_, nullptr, nullptr, allocator_, nullptr, addr_, nullptr,
                 global_hint_, expr_factory_, nullptr, false),
        table_metas_(allocator_), selectivity_ctx_(opt_ctx_, nullptr, nullptr, allocator_)
  {}

  void SetUp() override
  {
    previous_tenant_ = share::ObTenantEnv::get_tenant();
    share::ObTenantEnv::set_tenant(&tenant_);
    ASSERT_EQ(OB_SUCCESS, storage::ObLobManager::mtl_new(lob_manager_));
    tenant_.set<storage::ObLobManager *>(lob_manager_);
    // Refresh the thread-local module snapshot after registration.
    share::ObTenantEnv::set_tenant(&tenant_);
    ASSERT_EQ(lob_manager_, MTL(storage::ObLobManager *));
    ASSERT_EQ(OB_SUCCESS, exec_ctx_.create_physical_plan_ctx());
  }

  void TearDown() override
  {
    tenant_.set<storage::ObLobManager *>(nullptr);
    share::ObTenantEnv::set_tenant(&tenant_);
    if (OB_NOT_NULL(lob_manager_)) {
      storage::ObLobManager::mtl_destroy(lob_manager_);
    }
    share::ObTenantEnv::set_tenant(previous_tenant_);
  }

protected:
  share::ObTenantBase tenant_;
  share::ObTenantBase *previous_tenant_;
  storage::ObLobManager *lob_manager_;
  ObArenaAllocator allocator_;
  ObExecContext exec_ctx_;
  ObAddr addr_;
  ObGlobalHint global_hint_;
  ObRawExprFactory expr_factory_;
  ObOptimizerContext opt_ctx_;
  OptTableMetas table_metas_;
  OptSelectivityCtx selectivity_ctx_;
};

TEST_F(TestOptExprMinMax, external_constant_has_unknown_bounds_without_reading_content)
{
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://no-access/minmax-object");
  info.offset_ = 0;
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  info.has_length_ = true;
  storage::ObMemLobLocatorBuilder builder;
  ObLobLocatorV2 locator;
  ASSERT_EQ(OB_SUCCESS, builder.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, builder.build(locator));
  ASSERT_TRUE(locator.is_external_temp_lob());

  ObObj value;
  value.set_extblob(ObString(locator.size_, locator.ptr_));
  value.set_collation_type(CS_TYPE_BINARY);
  ObConstRawExpr expr(value, T_VARCHAR);
  ASSERT_EQ(OB_SUCCESS, expr.add_flag(IS_CONST));
  ASSERT_EQ(ObExtendBLobType, expr.get_data_type());
  ASSERT_TRUE(expr.is_static_scalar_const_expr());

  MinMaxWarningScope warning_scope;
  ObString payload;
  // The ordinary read entry must continue to reject temporary External LOBs.
  EXPECT_EQ(OB_NOT_SUPPORTED, value.read_lob_data(allocator_, payload));
  EXPECT_EQ(OB_NOT_SUPPORTED, warning_scope.warning_buffer_.get_err_code());
  EXPECT_NE(nullptr, std::strstr(warning_scope.warning_buffer_.get_err_msg(),
                               "reading a temporary external LOB"));
  warning_scope.warning_buffer_.reset();
  const int original_error_code = warning_scope.warning_buffer_.get_err_code();

  ObObj min_value;
  ObObj max_value;
  ASSERT_EQ(OB_SUCCESS, ObOptSelectivity::calc_expr_min_max(
      table_metas_, selectivity_ctx_, &expr, min_value, max_value));
  EXPECT_TRUE(min_value.is_min_value());
  EXPECT_TRUE(max_value.is_max_value());
  // The old implementation returned the same bounds after a failed content comparison.
  EXPECT_EQ(original_error_code, warning_scope.warning_buffer_.get_err_code());
  EXPECT_STREQ("", warning_scope.warning_buffer_.get_err_msg());
}

TEST_F(TestOptExprMinMax, internal_extblob_constant_has_unknown_bounds)
{
  ObString payload = ObString::make_string("hello");
  ObString inrow_lob;
  ASSERT_EQ(OB_SUCCESS, storage::ObLobManager::fill_lob_header(allocator_, payload, inrow_lob));
  ObObj value;
  value.set_extblob(inrow_lob);
  value.set_collation_type(CS_TYPE_BINARY);
  ObString actual_payload;
  ASSERT_EQ(OB_SUCCESS, value.read_lob_data(allocator_, actual_payload));
  ASSERT_EQ(payload, actual_payload);

  ObConstRawExpr expr(value, T_VARCHAR);
  ASSERT_EQ(OB_SUCCESS, expr.add_flag(IS_CONST));
  ASSERT_EQ(ObExtendBLobType, expr.get_data_type());
  ASSERT_TRUE(expr.is_static_scalar_const_expr());
  ObObj min_value;
  ObObj max_value;
  ASSERT_EQ(OB_SUCCESS, ObOptSelectivity::calc_expr_min_max(
      table_metas_, selectivity_ctx_, &expr, min_value, max_value));
  EXPECT_TRUE(min_value.is_min_value());
  EXPECT_TRUE(max_value.is_max_value());
}

TEST_F(TestOptExprMinMax, integer_constant_keeps_exact_bounds)
{
  ObObj value;
  value.set_int(7);
  ObConstRawExpr expr(value, T_INT);
  ASSERT_EQ(OB_SUCCESS, expr.add_flag(IS_CONST));
  ObObj min_value;
  ObObj max_value;
  ASSERT_EQ(OB_SUCCESS, ObOptSelectivity::calc_expr_min_max(
      table_metas_, selectivity_ctx_, &expr, min_value, max_value));
  ASSERT_TRUE(min_value.is_int());
  ASSERT_TRUE(max_value.is_int());
  EXPECT_EQ(7, min_value.get_int());
  EXPECT_EQ(7, max_value.get_int());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_opt_expr_min_max.log", true);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
