#include <gtest/gtest.h>

#include "../../base_test.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/jit_operator/jit_aware_lqp_translator.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_filter.hpp"
#include "operators/jit_operator/operators/jit_read_tuples.hpp"
#include "operators/jit_operator/operators/jit_write_tuples.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class JitAwareLQPTranslatorTest : public BaseTest {
 protected:
  void SetUp() override {
    const auto int_int_int_table = load_table("src/test/tables/int_int_int.tbl");
    const auto int_float_null_table = load_table("src/test/tables/int_float_null_sorted_asc.tbl");
    StorageManager::get().add_table("table_a", int_int_int_table);
    StorageManager::get().add_table("table_b", int_float_null_table);
  }

  void TearDown() override { StorageManager::get().reset(); }

  // Creates an (unoptimized) LQP from a given SQL query string and passes the LQP to the jit-aware translator.
  // This allows for creating different LQPs for testing with little code. The result of the translation
  // (which could be any AbstractOperator) is dynamically cast to a JitOperatorWrapper pointer. Thus, a simple nullptr
  // check can be used to test whether a JitOperatorWrapper has been created by the translator as the root node of the
  // PQP.
  std::shared_ptr<JitOperatorWrapper> translate_query(const std::string& sql) const {
    const auto lqp = SQLPipelineBuilder(sql).create_pipeline_statement(nullptr).get_unoptimized_logical_plan();
    JitAwareLQPTranslator lqp_translator;
    return std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(lqp));
  }
};

TEST_F(JitAwareLQPTranslatorTest, RequiresAtLeastTwoJittableOperators) {
  {
    const auto jit_operator_wrapper = translate_query("SELECT a FROM table_a");
    ASSERT_EQ(jit_operator_wrapper, nullptr);
  }
  {
    const auto jit_operator_wrapper = translate_query("SELECT a FROM table_a WHERE a > 1");
    ASSERT_NE(jit_operator_wrapper, nullptr);
  }
}

TEST_F(JitAwareLQPTranslatorTest, JitPipelineRequiresASingleInputNode) {
  {
    // A UnionNode with two distinct input nodes. If the jit-aware translator is not able to determine a single input
    // node to the (intended) operator pipeline, it should not create the pipeline in the first place.
    const auto stored_table_node_1 = std::make_shared<StoredTableNode>("table_a");
    const auto stored_table_node_2 = std::make_shared<StoredTableNode>("table_a");
    const auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);

    union_node->set_left_input(stored_table_node_1);
    union_node->set_right_input(stored_table_node_2);

    JitAwareLQPTranslator lqp_translator;
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(union_node));
    ASSERT_EQ(jit_operator_wrapper, nullptr);
  }
  {
    // Although both inputs of the UnionNode eventually lead to the same StoredTableNode (i.e., the LQP has a diamond
    // shape), one of the paths contains a non-jittable SortNode. Thus the jit-aware translator should reject the LQP
    // and not create an operator pipeline.
    const auto stored_table_node = std::make_shared<StoredTableNode>("table_a");
    const auto column_a = LQPColumnReference{stored_table_node, ColumnID{0}};
    const auto sort_node = std::make_shared<SortNode>(OrderByDefinitions{{column_a, OrderByMode::Ascending}});
    const auto union_node = std::make_shared<UnionNode>(UnionMode::Positions);

    sort_node->set_left_input(stored_table_node);
    union_node->set_left_input(stored_table_node);
    union_node->set_right_input(sort_node);

    JitAwareLQPTranslator lqp_translator;
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(union_node));
    ASSERT_EQ(jit_operator_wrapper, nullptr);
  }
}

TEST_F(JitAwareLQPTranslatorTest, JitOperatorsRejectIndexScan) {
  // The jit operators do not yet support index scans and should thus reject translating them
  const auto stored_table_node = std::make_shared<StoredTableNode>("table_a");
  const auto column_a = LQPColumnReference{stored_table_node, ColumnID{0}};
  const auto predicate_node_1 = std::make_shared<PredicateNode>(column_a, PredicateCondition::GreaterThan, 1);
  const auto predicate_node_2 = std::make_shared<PredicateNode>(column_a, PredicateCondition::LessThan, 10);

  predicate_node_1->set_left_input(stored_table_node);
  predicate_node_2->set_left_input(predicate_node_1);

  JitAwareLQPTranslator lqp_translator;
  {
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(predicate_node_2));
    ASSERT_NE(jit_operator_wrapper, nullptr);
  }

  {
    predicate_node_1->set_scan_type(ScanType::IndexScan);
    const auto jit_operator_wrapper =
        std::dynamic_pointer_cast<JitOperatorWrapper>(lqp_translator.translate_node(predicate_node_2));
    ASSERT_EQ(jit_operator_wrapper, nullptr);
  }
}

TEST_F(JitAwareLQPTranslatorTest, InputColumnsAreAddedToJitReadTupleAdapter) {
  // The query reads two columns from the input table. These input columns must be added to the JitReadTuples adapter to
  // make their data accessible by other JitOperators.
  const auto jit_operator_wrapper = translate_query("SELECT a, b FROM table_b WHERE a > 1");
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  // Check that the first operator is in fact a JitReadTuples instance
  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_NE(jit_read_tuples, nullptr);

  // There should be two input columns
  const auto input_columns = jit_read_tuples->input_columns();
  ASSERT_EQ(input_columns.size(), 2u);

  ASSERT_EQ(input_columns[0].column_id, ColumnID{0});
  ASSERT_EQ(input_columns[0].tuple_value.data_type(), DataType::Int);
  ASSERT_EQ(input_columns[0].tuple_value.is_nullable(), true);

  ASSERT_EQ(input_columns[1].column_id, ColumnID{1});
  ASSERT_EQ(input_columns[1].tuple_value.data_type(), DataType::Float);
  ASSERT_EQ(input_columns[1].tuple_value.is_nullable(), true);
}

TEST_F(JitAwareLQPTranslatorTest, LiteralValuesAreAddedToJitReadTupleAdapter) {
  // The query contains two literals. Literals are treated like values read from a column inside the operator pipeline.
  // The JitReadTuples adapter is responsible for making these literals available from within the pipeline.
  const auto jit_operator_wrapper = translate_query("SELECT a, b FROM table_b WHERE a > 1 AND b > 1.2");
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  // Check that the first operator is in fact a JitReadTuples instance
  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  ASSERT_NE(jit_read_tuples, nullptr);

  // There should be two literals read from the query
  const auto input_literals = jit_read_tuples->input_literals();
  ASSERT_EQ(input_literals.size(), 2u);

  ASSERT_TRUE(input_literals[0].value == AllTypeVariant(1));
  ASSERT_EQ(input_literals[0].tuple_value.data_type(), DataType::Int);
  ASSERT_EQ(input_literals[0].tuple_value.is_nullable(), false);

  ASSERT_TRUE(input_literals[1].value == AllTypeVariant(1.2f));
  ASSERT_EQ(input_literals[1].tuple_value.data_type(), DataType::Float);
  ASSERT_EQ(input_literals[1].tuple_value.is_nullable(), false);
}

TEST_F(JitAwareLQPTranslatorTest, SelectedColumnsAreOutputInCorrectOrder) {
  {
    // Select a subset of columns
    const auto jit_operator_wrapper = translate_query("SELECT a FROM table_a WHERE a > 1");
    const auto jit_operators = jit_operator_wrapper->jit_operators();
    ASSERT_EQ(jit_operators.size(), 4u);

    const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
    ASSERT_NE(jit_read_tuples, nullptr);
    const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[3]);
    ASSERT_NE(jit_write_tuples, nullptr);

    const auto output_columns = jit_write_tuples->output_columns();
    ASSERT_EQ(output_columns.size(), 1u);

    // The column output in the JitWriteTuples adapter should match the column read by the JitReadTuples adapter
    ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[0].tuple_value) == ColumnID{0});
  }
  {
    // Select all columns
    const auto jit_operator_wrapper = translate_query("SELECT * FROM table_a WHERE a > 1");
    const auto jit_operators = jit_operator_wrapper->jit_operators();
    ASSERT_EQ(jit_operators.size(), 4u);

    const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
    ASSERT_NE(jit_read_tuples, nullptr);
    const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[3]);
    ASSERT_NE(jit_write_tuples, nullptr);

    const auto output_columns = jit_write_tuples->output_columns();
    ASSERT_EQ(output_columns.size(), 3u);
    ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[0].tuple_value) == ColumnID{0});
    ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[1].tuple_value) == ColumnID{1});
    ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[2].tuple_value) == ColumnID{2});
  }
  {
    // Select columns in different order
    const auto jit_operator_wrapper = translate_query("SELECT c, a FROM table_a WHERE a > 1");
    const auto jit_operators = jit_operator_wrapper->jit_operators();
    ASSERT_EQ(jit_operators.size(), 4u);

    const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
    ASSERT_NE(jit_read_tuples, nullptr);
    const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[3]);
    ASSERT_NE(jit_write_tuples, nullptr);

    const auto output_columns = jit_write_tuples->output_columns();
    ASSERT_EQ(output_columns.size(), 2u);
    ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[0].tuple_value) == ColumnID{2});
    ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[1].tuple_value) == ColumnID{0});
  }
}

TEST_F(JitAwareLQPTranslatorTest, OutputColumnNamesAndAlias) {
  const auto jit_operator_wrapper = translate_query("SELECT a, b as b_new FROM table_a WHERE a > 1");
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operators.size(), 4u);

  const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[3]);
  ASSERT_NE(jit_write_tuples, nullptr);

  const auto output_columns = jit_write_tuples->output_columns();
  ASSERT_EQ(output_columns.size(), 2u);
  ASSERT_EQ(output_columns[0].column_name, "a");
  ASSERT_EQ(output_columns[1].column_name, "b_new");
}

TEST_F(JitAwareLQPTranslatorTest, ConsecutivePredicatesGetTransformedToConjunction) {
  const auto jit_operator_wrapper = translate_query("SELECT a, b, c FROM table_a WHERE a > b AND b > c AND c > a");
  ASSERT_NE(jit_operator_wrapper, nullptr);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_compute = std::dynamic_pointer_cast<JitCompute>(jit_operators[1]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);
  const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[3]);
  ASSERT_NE(jit_read_tuples, nullptr);
  ASSERT_NE(jit_compute, nullptr);
  ASSERT_NE(jit_filter, nullptr);
  ASSERT_NE(jit_write_tuples, nullptr);

  // Check the structure of the computed expression
  const auto expression = jit_compute->expression();
  ASSERT_EQ(expression->expression_type(), ExpressionType::And);
  ASSERT_EQ(expression->left_child()->expression_type(), ExpressionType::And);

  const auto a_gt_b = expression->left_child()->left_child();
  const auto b_gt_c = expression->left_child()->right_child();
  const auto c_gt_a = expression->right_child();

  ASSERT_EQ(a_gt_b->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(a_gt_b->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(a_gt_b->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_gt_b->left_child()->result()) == ColumnID{0});
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_gt_b->right_child()->result()) == ColumnID{1});

  ASSERT_EQ(b_gt_c->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(b_gt_c->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(b_gt_c->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(b_gt_c->left_child()->result()) == ColumnID{1});
  ASSERT_TRUE(jit_read_tuples->find_input_column(b_gt_c->right_child()->result()) == ColumnID{2});

  ASSERT_EQ(c_gt_a->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(c_gt_a->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(c_gt_a->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(c_gt_a->left_child()->result()) == ColumnID{2});
  ASSERT_TRUE(jit_read_tuples->find_input_column(c_gt_a->right_child()->result()) == ColumnID{0});

  // Check that the filter operates on the computed value
  ASSERT_TRUE(jit_filter->condition() == expression->result());
}

TEST_F(JitAwareLQPTranslatorTest, UnionsGetTransformedToDisjunction) {
  const auto jit_operator_wrapper = translate_query("SELECT a, b, c FROM table_a WHERE a > b OR b > c OR c > a");
  ASSERT_NE(jit_operator_wrapper, nullptr);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 4u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_compute = std::dynamic_pointer_cast<JitCompute>(jit_operators[1]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);
  const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[3]);
  ASSERT_NE(jit_read_tuples, nullptr);
  ASSERT_NE(jit_compute, nullptr);
  ASSERT_NE(jit_filter, nullptr);
  ASSERT_NE(jit_write_tuples, nullptr);

  // Check the structure of the computed expression
  const auto expression = jit_compute->expression();
  ASSERT_EQ(expression->expression_type(), ExpressionType::Or);
  ASSERT_EQ(expression->left_child()->expression_type(), ExpressionType::Or);

  const auto a_gt_b = expression->left_child()->left_child();
  const auto b_gt_c = expression->left_child()->right_child();
  const auto c_gt_a = expression->right_child();

  ASSERT_EQ(a_gt_b->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(a_gt_b->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(a_gt_b->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_gt_b->left_child()->result()) == ColumnID{0});
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_gt_b->right_child()->result()) == ColumnID{1});

  ASSERT_EQ(b_gt_c->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(b_gt_c->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(b_gt_c->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(b_gt_c->left_child()->result()) == ColumnID{1});
  ASSERT_TRUE(jit_read_tuples->find_input_column(b_gt_c->right_child()->result()) == ColumnID{2});

  ASSERT_EQ(c_gt_a->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(c_gt_a->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(c_gt_a->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(c_gt_a->left_child()->result()) == ColumnID{2});
  ASSERT_TRUE(jit_read_tuples->find_input_column(c_gt_a->right_child()->result()) == ColumnID{0});

  // Check that the filter operates on the computed value
  ASSERT_TRUE(jit_filter->condition() == expression->result());
}

TEST_F(JitAwareLQPTranslatorTest, AMoreComplexQuery) {
  const auto jit_operator_wrapper = translate_query("SELECT a, (a + b) * c FROM table_a WHERE a <= b AND b > a + c");
  ASSERT_NE(jit_operator_wrapper, nullptr);

  // Check the type of jit operators in the operator pipeline
  const auto jit_operators = jit_operator_wrapper->jit_operators();
  ASSERT_EQ(jit_operator_wrapper->jit_operators().size(), 5u);

  const auto jit_read_tuples = std::dynamic_pointer_cast<JitReadTuples>(jit_operators[0]);
  const auto jit_compute_1 = std::dynamic_pointer_cast<JitCompute>(jit_operators[1]);
  const auto jit_filter = std::dynamic_pointer_cast<JitFilter>(jit_operators[2]);
  const auto jit_compute_2 = std::dynamic_pointer_cast<JitCompute>(jit_operators[3]);
  const auto jit_write_tuples = std::dynamic_pointer_cast<JitWriteTuples>(jit_operators[4]);
  ASSERT_NE(jit_read_tuples, nullptr);
  ASSERT_NE(jit_compute_1, nullptr);
  ASSERT_NE(jit_filter, nullptr);
  ASSERT_NE(jit_compute_2, nullptr);
  ASSERT_NE(jit_write_tuples, nullptr);

  // Check the structure of the computed filter expression
  const auto expression_1 = jit_compute_1->expression();
  ASSERT_EQ(expression_1->expression_type(), ExpressionType::And);

  const auto a_lte_b = expression_1->left_child();
  ASSERT_EQ(a_lte_b->expression_type(), ExpressionType::LessThanEquals);
  ASSERT_EQ(a_lte_b->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(a_lte_b->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_lte_b->left_child()->result()) == ColumnID{0});
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_lte_b->right_child()->result()) == ColumnID{1});

  const auto b_gt_a_plus_c = expression_1->right_child();
  ASSERT_EQ(b_gt_a_plus_c->expression_type(), ExpressionType::GreaterThan);
  ASSERT_EQ(b_gt_a_plus_c->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(b_gt_a_plus_c->left_child()->result()) == ColumnID{1});

  const auto a_plus_c = b_gt_a_plus_c->right_child();
  ASSERT_EQ(a_plus_c->expression_type(), ExpressionType::Addition);
  ASSERT_EQ(a_plus_c->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(a_plus_c->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_plus_c->left_child()->result()) == ColumnID{0});
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_plus_c->right_child()->result()) == ColumnID{2});

  // Check that the filter operates on the computed value
  ASSERT_TRUE(jit_filter->condition() == expression_1->result());

  // Check the structure of the computed expression
  const auto expression_2 = jit_compute_2->expression();
  ASSERT_EQ(expression_2->expression_type(), ExpressionType::Multiplication);
  ASSERT_EQ(expression_2->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(expression_2->right_child()->result()) == ColumnID{2});

  const auto a_plus_b = expression_2->left_child();
  ASSERT_EQ(a_plus_b->expression_type(), ExpressionType::Addition);
  ASSERT_EQ(a_plus_b->left_child()->expression_type(), ExpressionType::Column);
  ASSERT_EQ(a_plus_b->right_child()->expression_type(), ExpressionType::Column);
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_plus_b->left_child()->result()) == ColumnID{0});
  ASSERT_TRUE(jit_read_tuples->find_input_column(a_plus_b->right_child()->result()) == ColumnID{1});

  const auto output_columns = jit_write_tuples->output_columns();
  ASSERT_EQ(output_columns.size(), 2u);
  ASSERT_TRUE(jit_read_tuples->find_input_column(output_columns[0].tuple_value) == ColumnID{0});
  ASSERT_TRUE(expression_2->result() == std::make_optional(output_columns[1].tuple_value));
}

}  // namespace opossum