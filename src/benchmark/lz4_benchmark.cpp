#include "micro_benchmark_basic_fixture.hpp"

#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/lz4/lz4_encoder.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/dictionary_segment/dictionary_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/run_length_segment/run_length_encoder.hpp"
#include "storage/run_length_segment.hpp"
#include "storage/frame_of_reference/frame_of_reference_encoder.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "tpch/tpch_table_generator.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class LZ4MicroBenchmarkFixture : public MicroBenchmarkBasicFixture {
public:
  void SetUp(::benchmark::State& state) {
    auto& sm = StorageManager::get();
    // we want enough entries to fill a whole chunk. this results in 120.350 rows, of which 100.000 are in the first
    // chunk
    const auto scale_factor = 0.02f;
    const auto default_encoding = EncodingType::Unencoded;

    // there is no other way to change the encoding
    auto default_benchmark_config = BenchmarkConfig::get_default_config();
    auto benchmark_config = BenchmarkConfig{default_benchmark_config.benchmark_mode,
                                            default_benchmark_config.chunk_size,
                                            EncodingConfig{SegmentEncodingSpec{default_encoding}},
                                            default_benchmark_config.max_num_query_runs,
                                            default_benchmark_config.max_duration,
                                            default_benchmark_config.warmup_duration,
                                            default_benchmark_config.use_mvcc,
                                            default_benchmark_config.output_file_path,
                                            default_benchmark_config.enable_scheduler,
                                            default_benchmark_config.cores,
                                            default_benchmark_config.clients,
                                            default_benchmark_config.enable_visualization,
                                            default_benchmark_config.verify,
                                            default_benchmark_config.cache_binary_tables};

    if (!sm.has_table("lineitem")) {
      std::cout << "Generating TPC-H data set with scale factor " << scale_factor << " and "
                << encoding_type_to_string.left.at(default_encoding) << " encoding:" << std::endl;
      TpchTableGenerator(scale_factor, std::make_shared<BenchmarkConfig>(benchmark_config)).generate_and_store();
    }

    _lineitem_table = sm.get_table("lineitem");
    auto chunk = _lineitem_table->get_chunk(ChunkID{0});
    auto base_segment = chunk->get_segment(_lineitem_table->column_id_by_name("l_comment"));
    _l_comment_segment = std::dynamic_pointer_cast<ValueSegment<std::string>>(base_segment);

    base_segment = chunk->get_segment(_lineitem_table->column_id_by_name("l_tax"));
    _l_tax_segment = std::dynamic_pointer_cast<ValueSegment<float>>(base_segment);

    base_segment = chunk->get_segment(_lineitem_table->column_id_by_name("l_linenumber"));
    _l_linenumber_segment = std::dynamic_pointer_cast<ValueSegment<int>>(base_segment);

    _lz4_encoder = LZ4Encoder();
    _dict_encoder = DictionaryEncoder<EncodingType::Dictionary>();
    _rle_encoder = RunLengthEncoder();
    _for_encoder = FrameOfReferenceEncoder();

  }

  // Required to avoid resetting of StorageManager in MicroBenchmarkBasicFixture::TearDown()
  void TearDown(::benchmark::State&) {}

  std::shared_ptr<Table> _lineitem_table;
  std::shared_ptr<ValueSegment<std::string>> _l_comment_segment;
  std::shared_ptr<ValueSegment<float>> _l_tax_segment;
  std::shared_ptr<ValueSegment<int>> _l_linenumber_segment;

  LZ4Encoder _lz4_encoder;
  DictionaryEncoder<EncodingType::Dictionary> _dict_encoder;
  RunLengthEncoder _rle_encoder;
  FrameOfReferenceEncoder _for_encoder;
};

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_LZ4EncodeString)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_comment_segment = _lz4_encoder.encode(_l_comment_segment, DataType::String);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_LZ4EncodeFloat)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_tax_segment = _lz4_encoder.encode(_l_tax_segment, DataType::Float);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_LZ4EncodeInt)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_linenumber_segment = _lz4_encoder.encode(_l_linenumber_segment, DataType::Int);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_DictionaryEncodeString)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_comment_segment = _dict_encoder.encode(_l_comment_segment, DataType::String);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_DictionaryEncodeFloat)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_tax_segment = _dict_encoder.encode(_l_tax_segment, DataType::Float);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_DictionaryEncodeInt)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_linenumber_segment = _dict_encoder.encode(_l_linenumber_segment, DataType::Int);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_RunLengthEncodeString)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_comment_segment = _rle_encoder.encode(_l_comment_segment, DataType::String);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_RunLengthEncodeFloat)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_tax_segment = _rle_encoder.encode(_l_tax_segment, DataType::Float);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_RunLengthEncodeInt)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_linenumber_segment = _rle_encoder.encode(_l_linenumber_segment, DataType::Int);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_FrameOfReferenceEncodeInt)(benchmark::State& state) {
  for (auto _ : state) {
    auto encoded_linenumber_segment = _for_encoder.encode(_l_linenumber_segment, DataType::Int);
  }
}

BENCHMARK_F(LZ4MicroBenchmarkFixture, BM_CompareEncodedSize)(benchmark::State& state) {
  if (HYRISE_DEBUG) {
    for (auto _ : state) {
      // Uncompressed
      std::cout << "Uncompressed string memory:\t" << _l_comment_segment->estimate_memory_usage() << std::endl;
      std::cout << "Uncompressed float memory:\t" << _l_tax_segment->estimate_memory_usage() << std::endl;
      std::cout << "Uncompressed int memory:\t" << _l_linenumber_segment->estimate_memory_usage() << std::endl;

      // LZ4
      auto lz4_comment = _lz4_encoder.encode(_l_comment_segment, DataType::String);
      auto lz4_str_segment = std::dynamic_pointer_cast<opossum::LZ4Segment<std::string>>(lz4_comment);
      std::cout << "LZ4 string memory:\t" << lz4_str_segment->estimate_memory_usage() << std::endl;

      auto lz4_tax = _lz4_encoder.encode(_l_tax_segment, DataType::Float);
      auto lz4_float_segment = std::dynamic_pointer_cast<opossum::LZ4Segment<float>>(lz4_tax);
      std::cout << "LZ4 float memory:\t" << lz4_float_segment->estimate_memory_usage() << std::endl;

      auto lz4_linenumber = _lz4_encoder.encode(_l_linenumber_segment, DataType::Int);
      auto lz4_int_segment = std::dynamic_pointer_cast<opossum::LZ4Segment<int32_t>>(lz4_linenumber);
      std::cout << "LZ4 int memory:\t" << lz4_int_segment->estimate_memory_usage() << std::endl;

      // Dictionary
      auto dict_comment = _dict_encoder.encode(_l_comment_segment, DataType::String);
      auto dict_str_segment = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(dict_comment);
      std::cout << "Dict string memory:\t" << dict_str_segment->estimate_memory_usage() << std::endl;

      auto dict_tax = _dict_encoder.encode(_l_tax_segment, DataType::Float);
      auto dict_float_segment = std::dynamic_pointer_cast<opossum::DictionarySegment<float>>(dict_tax);
      std::cout << "Dict float memory:\t" << dict_float_segment->estimate_memory_usage() << std::endl;

      auto dict_linenumber = _dict_encoder.encode(_l_linenumber_segment, DataType::Int);
      auto dict_int_segment = std::dynamic_pointer_cast<opossum::DictionarySegment<int32_t>>(dict_linenumber);
      std::cout << "Dict int memory:\t" << dict_int_segment->estimate_memory_usage() << std::endl;

      // Run Length
      auto rle_comment = _rle_encoder.encode(_l_comment_segment, DataType::String);
      auto rle_str_segment = std::dynamic_pointer_cast<opossum::RunLengthSegment<std::string>>(rle_comment);
      std::cout << "RLE string memory:\t" << rle_str_segment->estimate_memory_usage() << std::endl;

      auto rle_tax = _rle_encoder.encode(_l_tax_segment, DataType::Float);
      auto rle_float_segment = std::dynamic_pointer_cast<opossum::RunLengthSegment<float>>(rle_tax);
      std::cout << "RLE float memory:\t" << rle_float_segment->estimate_memory_usage() << std::endl;

      auto rle_linenumber = _rle_encoder.encode(_l_linenumber_segment, DataType::Int);
      auto rle_int_segment = std::dynamic_pointer_cast<opossum::RunLengthSegment<int32_t>>(rle_linenumber);
      std::cout << "RLE int memory:\t" << rle_int_segment->estimate_memory_usage() << std::endl;

      // Frame of Reference
      auto for_linenumber = _for_encoder.encode(_l_linenumber_segment, DataType::Int);
      auto for_int_segment = std::dynamic_pointer_cast<opossum::FrameOfReferenceSegment<int32_t>>(for_linenumber);
      std::cout << "FOR int memory:\t" << for_int_segment->estimate_memory_usage() << std::endl;
    }
  }
}

}  // namespace opossum
