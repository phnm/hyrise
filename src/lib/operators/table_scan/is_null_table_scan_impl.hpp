#pragma once

#include <functional>
#include <memory>

#include "abstract_single_column_table_scan_impl.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;
class BaseValueSegment;

class IsNullTableScanImpl : public AbstractSingleColumnTableScanImpl {
 public:
  IsNullTableScanImpl(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                      const PredicateCondition& predicate_condition);

  std::string description() const override;

  // We need to override scan_chunk because we do not want ReferenceSegments to be resolved (which would remove NullValues from the referencing PosList)
  std::shared_ptr<PosList> scan_chunk(const ChunkID chunk_id) const override;

 protected:
  void _on_scan(const BaseSegment& segment, const ChunkID chunk_id, PosList& results,
                const std::shared_ptr<const PosList>& position_filter) const override;

  void _scan_segment(const BaseSegment& segment, const ChunkID chunk_id, PosList& results,
                     const std::shared_ptr<const PosList>& position_filter) const;
  void _scan_segment(const BaseValueSegment& segment, const ChunkID chunk_id, PosList& results,
                     const std::shared_ptr<const PosList>& position_filter) const;

  /**
   * @defgroup Methods used for handling value segments
   * @{
   */

  bool _matches_all(const BaseValueSegment& segment) const;

  bool _matches_none(const BaseValueSegment& segment) const;

  void _add_all(const ChunkID chunk_id, PosList& results, const std::shared_ptr<const PosList>& position_filter,
                const size_t segment_size) const;

  /**@}*/
};

}  // namespace opossum
