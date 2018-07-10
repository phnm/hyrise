#pragma once

#include "abstract_logger.hpp"

#include <fstream>
#include <mutex>

#include "types.hpp"
#include "../../utils/loop_thread.hpp"

namespace opossum {

class LogEntry;

/*
 *  Logger that gathers multiple log entries in a buffer before flushing them to disk.
 */
class GroupCommitLogger : public AbstractLogger {
 public:
  GroupCommitLogger(const GroupCommitLogger&) = delete;
  GroupCommitLogger& operator=(const GroupCommitLogger&) = delete;

  void commit(const TransactionID transaction_id, std::function<void(TransactionID)> callback) override;

  void value(const TransactionID transaction_id, const std::string& table_name, const RowID row_id,
             const std::vector<AllTypeVariant> values) override;

  void invalidate(const TransactionID transaction_id, const std::string& table_name, const RowID row_id) override;

  void load_table(const std::string& file_path, const std::string& table_name) override;

  void flush() override;

  void recover() override;

 private:
  friend class Logger;

  GroupCommitLogger();
  
  // Called by tests before switching to another implementation.
  void _shut_down() override;

  void _write_buffer_to_logfile();
  void _write_to_buffer(std::vector<char>& entry);

  void _open_logfile();

  char* _buffer;
  const uint32_t _buffer_capacity;  // uint32_t: Max buffer capacity ~ 4GB
  uint32_t _buffer_position;
  bool _has_unflushed_buffer; 
  std::mutex _buffer_mutex;

  std::vector<std::pair<std::function<void(TransactionID)>, TransactionID>> _commit_callbacks;

  std::mutex _file_mutex;
  std::fstream _log_file;

  std::unique_ptr<LoopThread> _flush_thread;
};

}  // namespace opossum
