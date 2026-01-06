
#pragma once
#include "io/file_writer.hpp"
#include <filesystem>
#include <vector>

struct WALRecord {
  std::vector<std::byte> key_;
  std::vector<std::byte> value_;

  static const uint16_t KEY_LENGTH_ENCODED_SIZE = 2;
  static const uint16_t VALUE_LENGTH_ENCODED_SIZE = 2;
  std::vector<std::byte> encode() const;

  bool operator==(const WALRecord &other) const {
    return key_ == other.key_ && value_ == other.value_;
  }
};

class FileWriter;
class WAL {
public:
  WAL(const std::filesystem::path &);
  void add_record_and_sync(const WALRecord &wal_record);
  void add_record(const WALRecord &wal_record);
  static std::vector<WALRecord> read_wal(const std::filesystem::path &);
  ~WAL();

private:
  std::unique_ptr<FileWriter> writer_;
  std::vector<WALRecord> records_;
};
