#include "strategy_base_test.hpp"

#include <memory>
#include <string>
#include <utility>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "optimizer/strategy/abstract_rule.hpp"

namespace opossum {

std::shared_ptr<AbstractLQPNode> StrategyBaseTest::apply_rule(const std::shared_ptr<AbstractRule>& rule,
                                                              const std::shared_ptr<AbstractLQPNode>& input) {
  // Add explicit root node
  const auto root_node = LogicalPlanRootNode::make();
  root_node->set_left_child(input);

  rule->apply_to(root_node);

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parents();

  return optimized_node;
}

}  // namespace opossum
