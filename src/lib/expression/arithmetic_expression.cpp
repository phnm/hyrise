#include "arithmetic_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"
#include "expression_utils.hpp"

namespace opossum {

ArithmeticExpression::ArithmeticExpression(const ArithmeticOperator arithmetic_operator,
                                           const std::shared_ptr<AbstractExpression>& left_operand,
                                           const std::shared_ptr<AbstractExpression>& right_operand):
AbstractExpression(ExpressionType::Arithmetic, {left_operand, right_operand}), arithmetic_operator(arithmetic_operator) {}

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::left_operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::right_operand() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression> ArithmeticExpression::deep_copy() const {
  return std::make_shared<ArithmeticExpression>(arithmetic_operator, left_operand()->deep_copy(), right_operand()->deep_copy());
}

DataType ArithmeticExpression::data_type() const {
  return expression_common_type(left_operand()->data_type(), right_operand()->data_type());
}

std::string ArithmeticExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool ArithmeticExpression::is_nullable() const {
  return AbstractExpression::is_nullable() ||
         arithmetic_operator == ArithmeticOperator::Division ||
         arithmetic_operator == ArithmeticOperator::Modulo ||
         arithmetic_operator == ArithmeticOperator::Power;
}

bool ArithmeticExpression::_shallow_equals(const AbstractExpression& expression) const {
  return arithmetic_operator == static_cast<const ArithmeticExpression&>(expression).arithmetic_operator;
}

size_t ArithmeticExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(arithmetic_operator));
}

}  // namespace opossum