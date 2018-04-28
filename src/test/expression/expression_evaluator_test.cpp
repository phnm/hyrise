#include "gtest/gtest.h"

#include <optional>

#include "expression/expression_evaluator.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_placeholder_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

namespace opossum {

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_a = load_table("src/test/tables/expression_evaluator/input_a.tbl");
    chunk_a = table_a->get_chunk(ChunkID{0});
    evaluator.emplace(chunk_a);

    a = std::make_shared<PQPColumnExpression>(ColumnID{0}, table_a->column_data_type(ColumnID{0}), table_a->column_is_nullable(ColumnID{0}));
    b = std::make_shared<PQPColumnExpression>(ColumnID{1}, table_a->column_data_type(ColumnID{1}), table_a->column_is_nullable(ColumnID{1}));
    c = std::make_shared<PQPColumnExpression>(ColumnID{2}, table_a->column_data_type(ColumnID{2}), table_a->column_is_nullable(ColumnID{2}));
    s1 = std::make_shared<PQPColumnExpression>(ColumnID{3}, table_a->column_data_type(ColumnID{3}), table_a->column_is_nullable(ColumnID{3}));
    s2 = std::make_shared<PQPColumnExpression>(ColumnID{4}, table_a->column_data_type(ColumnID{4}), table_a->column_is_nullable(ColumnID{4}));
    a_plus_b = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, b);
    a_plus_c = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, c);
    s1_gt_s2 = std::make_shared<BinaryPredicateExpression>(ArithmeticOperator::Addition, a, c);

    table_b = load_table("src/test/tables/expression_evaluator/input_b.tbl");
  }

  std::shared_ptr<Table> table_a, table_b;
  std::shared_ptr<Chunk> chunk_a;
  std::optional<ExpressionEvaluator> evaluator;

  std::shared_ptr<PQPColumnExpression> a, b, c, s1, s2;
  std::shared_ptr<ArithmeticExpression> a_plus_b;
  std::shared_ptr<ArithmeticExpression> a_plus_c;
  std::shared_ptr<ArithmeticExpression> s1_gt_s2;
};

TEST_F(ExpressionEvaluatorTest, ArithmeticExpression) {
  const auto expected_result = std::vector<int32_t>({3, 5, 7, 9});
  EXPECT_EQ(boost::get<std::vector<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_b)), expected_result);
}

TEST_F(ExpressionEvaluatorTest, ArithmeticExpressionWithNull) {
  const auto actual_result = boost::get<ExpressionEvaluator::NullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_c));
  const auto& actual_values = actual_result.first;
  const auto& actual_nulls = actual_result.second;

  ASSERT_EQ(actual_nulls.size(), 4u);
  EXPECT_EQ(actual_values.at(0), 34);
  EXPECT_EQ(actual_values.at(2), 37);

  std::vector<bool> expected_nulls = {false, true, false, true};
  EXPECT_EQ(actual_nulls, expected_nulls);
}

TEST_F(ExpressionEvaluatorTest, PredicateWithStrings) {
  const auto actual_result = boost::get<ExpressionEvaluator::NullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_c));
  const auto& actual_values = actual_result.first;
  const auto& actual_nulls = actual_result.second;

  ASSERT_EQ(actual_nulls.size(), 4u);
  EXPECT_EQ(actual_values.at(0), 34);
  EXPECT_EQ(actual_values.at(2), 37);

  std::vector<bool> expected_nulls = {false, true, false, true};
  EXPECT_EQ(actual_nulls, expected_nulls);
}

TEST_F(ExpressionEvaluatorTest, PQPSelectExpression) {
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_b);
  const auto x = std::make_shared<PQPColumnExpression>(ColumnID{0}, table_b->column_data_type(ColumnID{0}), table_b->column_is_nullable(ColumnID{0}));
  const auto external_b = std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{0});
  const auto b_plus_x = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, external_b, x);
  const auto inner_expressions = std::vector<std::shared_ptr<AbstractExpression>>({b_plus_x, x});
  const auto inner_projection = std::make_shared<Projection>(table_wrapper_b, inner_expressions);
  const auto table_scan = std::make_shared<TableScan>(inner_projection, ColumnID{0}, PredicateCondition::Equals, 12);
  const auto aggregates = std::vector<AggregateColumnDefinition>({{AggregateFunction::Sum, ColumnID{1}}});
  const auto aggregate = std::make_shared<Aggregate>(table_scan, aggregates, std::vector<ColumnID>{});

  const auto parameters = std::vector<ColumnID>({ColumnID{1}});
  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(aggregate, DataType::Int, true, parameters);

  const auto expected_result = std::vector<int64_t>({20, 9, 24, 7});
  EXPECT_EQ(boost::get<std::vector<int64_t>>(evaluator->evaluate_expression<int64_t>(*pqp_select_expression)), expected_result);
}

}  // namespace opossum