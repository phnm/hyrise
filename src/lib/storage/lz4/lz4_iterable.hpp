#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/lz4_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class LZ4Iterable : public PointAccessibleSegmentIterable<LZ4Iterable<T>> {
 public:
  using ValueType = T;

  explicit LZ4Iterable(const LZ4Segment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto decompressed_segment = _segment.decompress();
    // alias the data type of the constant iterator over the decompressed data
    using ValueIteratorT = decltype(decompressed_segment->cbegin());

    // create iterator instances for the begin and end
    auto begin = Iterator<ValueIteratorT>{decompressed_segment->cbegin(), _segment.null_values()->cbegin()};
    auto end = Iterator<ValueIteratorT>{decompressed_segment->cend(), _segment.null_values()->cend()};

    // call the functor on the iterators (until the begin iterator equals the end iterator)
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    // for now we also decompress the whole segment instead of having an actual point access
    auto decompressed_segment = _segment.decompress();
    // alias the data type of the constant iterator over the decompressed data
    using ValueIteratorT = decltype(decompressed_segment->cbegin());

    // create point access iterator instances for the begin and end
    auto begin = PointAccessIterator<ValueIteratorT>{decompressed_segment, *_segment.null_values(),
                                                     position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator<ValueIteratorT>{decompressed_segment, *_segment.null_values(),
                                                   position_filter->cbegin(), position_filter->cend()};

    // call the functor on the iterators (until the begin iterator equals the end iterator)
    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const LZ4Segment<T>& _segment;

 private:
  template <typename ValueIteratorT>
  class Iterator : public BaseSegmentIterator<Iterator<ValueIteratorT>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4Iterable<T>;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    // Begin and End Iterator
    explicit Iterator(ValueIteratorT data_it, const NullValueIterator null_value_it)
      : _chunk_offset{0u},
        _data_it{data_it},
        _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
      ++_data_it;
      ++_null_value_it;
    }

    bool equal(const Iterator& other) const { return _data_it == other._data_it; }

    SegmentPosition<T> dereference() const {
      return SegmentPosition<T>{*_data_it, *_null_value_it, _chunk_offset};
    }

   private:
    ChunkOffset _chunk_offset;
    ValueIteratorT _data_it;
    NullValueIterator _null_value_it;
  };

  template <typename ValueIteratorT>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<ValueIteratorT>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4Iterable<T>;

    // Begin Iterator
    PointAccessIterator(std::shared_ptr<std::vector<T>> data, const pmr_vector<bool>& null_values,
                        const PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<ValueIteratorT>,
                                         SegmentPosition<T>>{std::move(position_filter_begin),
                                                             std::move(position_filter_it)},
        _data{data},
        _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      const auto value = (*_data)[chunk_offsets.offset_in_referenced_chunk];
      const auto is_null = _null_values[chunk_offsets.offset_in_referenced_chunk];
      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    std::shared_ptr<std::vector<T>> _data;
    const pmr_vector<bool>& _null_values;
  };
};

}  // namespace opossum
