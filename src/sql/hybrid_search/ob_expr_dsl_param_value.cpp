/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_ENG
#include "sql/hybrid_search/ob_expr_dsl_param_value.h"
#include "lib/json_type/ob_json_tree.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprDslParamValue::ObExprDslParamValue(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_DSL_PARAM_VALUE, "DSL_PARAM_VALUE", 2,
                       NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

int ObExprDslParamValue::calc_result_type2(ObExprResType &type, ObExprResType &input,
    ObExprResType &mode, ObExprTypeCtx &ctx) const
{
  int ret = OB_SUCCESS;
  int64_t kind = 0;
  if (OB_FAIL(mode.get_param().get_int(kind))) {
    LOG_WARN("invalid DSL conversion mode", K(ret));
  } else if (kind == BOOLEAN) {
    type.set_tinyint();
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
    type.set_scale(0);
    input.set_calc_type(ObVarcharType);
  } else if (kind == ARRAY_FLOAT || kind == ARRAY_FLOAT_NARROW || kind == ARRAY_FLOAT_IN_RANGE) {
    if (kind == ARRAY_FLOAT_NARROW) {
      type.set_float();
    } else if (kind == ARRAY_FLOAT_IN_RANGE) {
      type.set_tinyint();
      type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
      type.set_scale(0);
    } else {
      type.set_double();
    }
    if (!ob_is_string_type(input.get_type()) && input.get_type() != ObUInt64Type) {
      input.set_calc_type(ObDoubleType);
    }
  } else if (kind >= INTEGER_CANDIDATE_BASE && kind < INTEGER_CANDIDATE_BASE + ObMaxType
             && ob_is_integer_type(static_cast<ObObjType>(kind - INTEGER_CANDIDATE_BASE))) {
    type.set_int();
  } else if (kind >= INTEGER_TERMS_BASE && kind < INTEGER_TERMS_BASE + ObMaxType
             && ob_is_integer_type(static_cast<ObObjType>(kind - INTEGER_TERMS_BASE))) {
    const ObObjType target_type = static_cast<ObObjType>(kind - INTEGER_TERMS_BASE);
    type.set_type(target_type);
    type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[target_type]);
    if (input.get_type() == ObDecimalIntType) {
      input.set_calc_type(ObNumberType);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported DSL conversion mode", K(ret), K(kind));
  }
  return ret;
}

ObExprDslParamValue::IntegerCandidateKind ObExprDslParamValue::classify_integer_candidate(
    const ObObj &value, ObObjType target_type)
{
  bool fits = false;
  if (ob_is_integer_type(target_type)) {
    if (ob_is_uint_tc(value.get_type())) {
      const uint64_t v = value.get_uint64();
      fits = ob_is_uint_tc(target_type) ? v <= UINT_MAX_VAL[target_type]
          : v <= static_cast<uint64_t>(INT_MAX_VAL[target_type]);
    } else if (ob_is_int_tc(value.get_type())) {
      const int64_t v = value.get_int();
      fits = ob_is_uint_tc(target_type) ? v >= 0 && static_cast<uint64_t>(v) <= UINT_MAX_VAL[target_type]
          : v >= INT_MIN_VAL[target_type] && v <= INT_MAX_VAL[target_type];
    } else if (value.get_type() == ObDoubleType) {
      const double v = value.get_double();
      // Exclusive power-of-two upper bounds avoid rounding INT64/UINT64_MAX up.
      if (!isnan(v) && !isinf(v) && trunc(v) == v) {
        if (ob_is_uint_tc(target_type)) {
          fits = v >= 0 && v < 18446744073709551616.0
              && static_cast<uint64_t>(v) <= UINT_MAX_VAL[target_type];
        } else {
          fits = v >= -9223372036854775808.0 && v < 9223372036854775808.0
              && static_cast<int64_t>(v) >= INT_MIN_VAL[target_type]
              && static_cast<int64_t>(v) <= INT_MAX_VAL[target_type];
        }
      }
    }
  }
  const bool fractional = value.is_double() && !isnan(value.get_double()) && !isinf(value.get_double())
      && trunc(value.get_double()) != value.get_double();
  return fits ? INTEGER_VALUE : fractional ? FRACTIONAL_VALUE : SQL_FALLBACK;
}

int ObExprDslParamValue::convert_integer_terms(ObIAllocator &allocator, ObIJsonBase &node,
    ObObjType target_type, ObObj &result)
{
  int ret = OB_SUCCESS;
  result.set_null();
  if (!ob_is_integer_type(target_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid integer terms target type", K(ret), K(target_type));
  } else if (!ObIJsonBase::is_json_number_type(node.json_type()) && node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query: expected a number or numeric string for an integer field");
    LOG_WARN("invalid terms value type for integer field", K(ret), K(target_type), K(node.json_type()));
  } else if (node.json_type() == ObJsonNodeType::J_DOUBLE &&
             (isnan(node.get_double()) || isinf(node.get_double()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query: numeric values for an integer field must be finite");
    LOG_WARN("non-finite terms value for integer field", K(ret), K(target_type), K(node.get_double()));
  } else if (node.json_type() == ObJsonNodeType::J_DOUBLE && trunc(node.get_double()) != node.get_double()) {
    // Fractional candidates cannot match an integer field. Keep validating subsequent candidates.
  } else {
    // Parse numeric strings as NUMBER, not double: BIGINT boundaries must retain all digits.
    number::ObNumber value;
    int64_t int_value = 0;
    uint64_t uint_value = 0;
    bool has_fraction = false;
    if (node.json_type() == ObJsonNodeType::J_STRING) {
      double parsed_double = 0;
      // Validate the complete string first; NUMBER alone accepts sign-only strings in MySQL mode.
      // Use double for syntax/fraction checks, never to construct the integer constant.
      if (OB_FAIL(node.to_double(parsed_double))) {
        LOG_WARN("invalid numeric string in terms", K(ret));
      } else if (isnan(parsed_double) || isinf(parsed_double)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("non-finite numeric string in terms", K(ret), K(target_type), K(parsed_double));
      } else if (trunc(parsed_double) != parsed_double) {
        // Do not let NUMBER underflow turn a small fractional string into an integer zero.
        has_fraction = true;
      } else if (parsed_double == 0) {
        // Syntax is already validated. A nonzero significand rounded to double zero
        // denotes underflow, in either scientific or ordinary decimal notation.
        const char *data = node.get_data();
        for (uint64_t i = 0; !has_fraction && i < node.get_data_length(); ++i) {
          if (data[i] == 'e' || data[i] == 'E') {
            break;
          }
          has_fraction = data[i] >= '1' && data[i] <= '9';
        }
        if (!has_fraction) {
          value.set_zero();
        }
      } else if (OB_FAIL(value.from_sci_opt(node.get_data(), node.get_data_length(), allocator))) {
        LOG_WARN("failed to parse numeric terms string", K(ret));
      }
    } else if (node.json_type() == ObJsonNodeType::J_DOUBLE) {
      const double number = node.get_double();
      // The upper int64/uint64 limits round up in double precision.
      if (ob_is_uint_tc(target_type)) {
        if (number < 0 || number >= static_cast<double>(ULLONG_MAX)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          ret = value.from(static_cast<uint64_t>(number), allocator);
        }
      } else if (number < static_cast<double>(LLONG_MIN) || number >= static_cast<double>(LLONG_MAX)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ret = value.from(static_cast<int64_t>(number), allocator);
      }
    } else if (OB_FAIL(node.to_number(&allocator, value))) {
      LOG_WARN("failed to parse integer terms value", K(ret), K(target_type));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to convert integer terms value", K(ret), K(target_type), K(node.json_type()));
      if (ret != OB_ALLOCATE_MEMORY_FAILED) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query: invalid numeric value or value out of range for the integer field");
      }
    } else if (has_fraction || !value.is_integer()) {
      // A null output denotes a valid, non-matching fractional candidate.
    } else if (ob_is_uint_tc(target_type)) {
      if (!value.is_valid_uint64(uint_value) || uint_value > UINT_MAX_VAL[target_type]) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query: value is out of range for the unsigned integer field");
        LOG_WARN("terms value out of unsigned integer range", K(ret), K(target_type), K(value), K(uint_value));
      } else {
        result.set_uint(target_type, uint_value);
      }
    } else if (!value.is_valid_int64(int_value) || int_value < INT_MIN_VAL[target_type]
               || int_value > INT_MAX_VAL[target_type]) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "terms query: value is out of range for the integer field");
      LOG_WARN("terms value out of integer range", K(ret), K(target_type), K(value), K(int_value));
    } else {
      result.set_int(target_type, int_value);
    }
  }
  return ret;
}

int ObExprDslParamValue::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw, ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (raw.get_param_count() != 2 || OB_ISNULL(raw.get_param_expr(1))
      || !raw.get_param_expr(1)->is_const_raw_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DSL conversion expression", K(ret), K(raw));
  } else {
    expr.extra_ = static_cast<const ObConstRawExpr *>(raw.get_param_expr(1))->get_value().get_int();
    expr.eval_func_ = eval_value;
  }
  return ret;
}

int ObExprDslParamValue::eval_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *input = nullptr;
  if (expr.arg_cnt_ != 2 || OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid DSL conversion arguments", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, input))) {
    LOG_WARN("failed to evaluate DSL value", K(ret));
  } else if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null DSL input datum", K(ret));
  } else if (input->is_null()) {
    result.set_null();
  } else if (expr.extra_ == BOOLEAN) {
    ObString text = input->get_string().trim();
    if (text.case_compare("true") == 0) {
      result.set_int(1);
    } else if (text.case_compare("false") == 0) {
      result.set_int(0);
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT,
          "boolean value, should be \"true\" or \"false\" when it is a string");
    }
  } else if (expr.extra_ == ARRAY_FLOAT || expr.extra_ == ARRAY_FLOAT_NARROW
             || expr.extra_ == ARRAY_FLOAT_IN_RANGE) {
    double value = 0.0;
    const ObObjType input_type = expr.args_[0]->datum_meta_.type_;
    if (ob_is_string_type(input_type)) {
      // Reuse the resolver's strict JSON string-to-number conversion. SQL CAST
      // has different truncation/error semantics for malformed numeric strings.
      ObJsonString text(input->get_string());
      if (OB_FAIL(text.to_double(value))) {
        ret = OB_INVALID_ARGUMENT;
      }
    } else if (input_type == ObUInt64Type) {
      value = static_cast<double>(input->get_uint());
    } else {
      value = input->get_double();
    }
    if (OB_SUCC(ret)) {
      if (expr.extra_ == ARRAY_FLOAT_NARROW) {
        result.set_float(static_cast<float>(value));
      } else if (expr.extra_ == ARRAY_FLOAT_IN_RANGE) {
        result.set_int(!ObArithExprOperator::is_float_out_of_range(static_cast<float>(value)));
      } else {
        const float narrowed = static_cast<float>(value);
        // Match FLOAT-array resolver rounding, preserving out-of-range values
        // as DOUBLE instead of saturating or overflowing into a false match.
        result.set_double(ObArithExprOperator::is_float_out_of_range(narrowed)
            ? value : static_cast<double>(narrowed));
      }
    }
  } else if (expr.extra_ >= INTEGER_CANDIDATE_BASE
             && expr.extra_ < INTEGER_CANDIDATE_BASE + ObMaxType) {
    ObObj value;
    const ObObjType input_type = expr.args_[0]->datum_meta_.type_;
    if (ob_is_int_tc(input_type)) {
      value.set_int(input->get_int());
    } else if (ob_is_uint_tc(input_type)) {
      value.set_uint64(input->get_uint());
    } else if (input_type == ObDoubleType) {
      value.set_double(input->get_double());
    }
    result.set_int(classify_integer_candidate(value, static_cast<ObObjType>(expr.extra_ - INTEGER_CANDIDATE_BASE)));
  } else if (expr.extra_ >= INTEGER_TERMS_BASE && expr.extra_ < INTEGER_TERMS_BASE + ObMaxType) {
    ObEvalCtx::TempAllocGuard guard(ctx);
    const ObObjType target_type = static_cast<ObObjType>(expr.extra_ - INTEGER_TERMS_BASE);
    const ObObjType input_type = expr.args_[0]->datum_meta_.type_;
    ObObj converted;
    // Keep raw SQL input types: converting integers or numeric strings through
    // DOUBLE would lose BIGINT/UINT64 boundary digits before the strict check.
    if (ob_is_string_type(input_type)) {
      ObJsonString node(input->get_string());
      OZ(convert_integer_terms(guard.get_allocator(), node, target_type, converted));
    } else if (ob_is_int_tc(input_type)) {
      ObJsonInt node(input->get_int());
      OZ(convert_integer_terms(guard.get_allocator(), node, target_type, converted));
    } else if (ob_is_uint_tc(input_type)) {
      ObJsonUint node(input->get_uint());
      OZ(convert_integer_terms(guard.get_allocator(), node, target_type, converted));
    } else if (input_type == ObDoubleType) {
      ObJsonDouble node(input->get_double());
      OZ(convert_integer_terms(guard.get_allocator(), node, target_type, converted));
    } else if (ob_is_number_tc(input_type)) {
      ObJsonDecimal node(input->get_number());
      OZ(convert_integer_terms(guard.get_allocator(), node, target_type, converted));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid integer terms input type", K(ret), K(input_type));
    }
    OZ(result.from_obj(converted));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unsupported DSL conversion mode", K(ret), K(expr.extra_));
  }
  return ret;
}
} // namespace sql
} // namespace oceanbase
