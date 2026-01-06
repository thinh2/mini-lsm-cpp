#include "wal/wal.hpp"
#include "io/file_reader.hpp"
#include "io/file_writer.hpp"
#include "utils.hpp"

std::vector<std::byte> WALRecord::encode() const {
  std::vector<std::byte> encoded_bytes;
  encoded_bytes.reserve(key_.size() + WALRecord::KEY_LENGTH_ENCODED_SIZE +
                        value_.size() + WALRecord::VALUE_LENGTH_ENCODED_SIZE);
  encoded_bytes.append_range(encode_uint16_t(key_.size()));
  encoded_bytes.append_range(key_);
  encoded_bytes.append_range(encode_uint16_t(value_.size()));
  encoded_bytes.append_range(value_);
  return encoded_bytes;
}

WAL::WAL(const std::filesystem::path &path)
    : writer_(std::make_unique<FileWriter>(path)) {}

void WAL::add_record_and_sync(const WALRecord &wal_record) {
  auto encoded_record = wal_record.encode();
  writer_->append_and_sync(encoded_record);
}

void WAL::add_record(const WALRecord &wal_record) {
  records_.push_back(wal_record);
}

std::vector<WALRecord> WAL::read_wal(const std::filesystem::path &path) {
  FileReader reader(path);
  std::vector<WALRecord> wal_records_;
  uint64_t curr_offset = 0;
  std::vector<std::byte> buffer;
  while (curr_offset < reader.file_size()) {
    WALRecord record;

    buffer.resize(WALRecord::KEY_LENGTH_ENCODED_SIZE);
    reader.read(curr_offset, WALRecord::KEY_LENGTH_ENCODED_SIZE, buffer);
    std::span<const std::byte, WALRecord::KEY_LENGTH_ENCODED_SIZE> buffer_span{
        buffer.data(), buffer.size()};
    auto key_length = decode_uint16_t(buffer_span);

    curr_offset += WALRecord::KEY_LENGTH_ENCODED_SIZE;
    buffer.resize(key_length);
    reader.read(curr_offset, key_length, buffer);
    record.key_ = std::move(buffer);

    curr_offset += key_length;
    buffer.resize(WALRecord::VALUE_LENGTH_ENCODED_SIZE);
    reader.read(curr_offset, WALRecord::VALUE_LENGTH_ENCODED_SIZE, buffer);
    buffer_span =
        std::span<const std::byte, WALRecord::VALUE_LENGTH_ENCODED_SIZE>{
            buffer.data(), buffer.size()};
    auto value_length = decode_uint16_t(buffer_span);

    curr_offset += WALRecord::VALUE_LENGTH_ENCODED_SIZE;
    buffer.resize(value_length);
    reader.read(curr_offset, value_length, buffer);
    record.value_ = std::move(buffer);

    curr_offset += value_length;
    wal_records_.emplace_back(record);
  }

  return wal_records_;
}

WAL::~WAL() {
  if (!records_.empty()) {
    std::vector<std::byte> encoded_records;
    for (auto &record : records_) {
      encoded_records.append_range(record.encode());
    }
    writer_->append_and_sync(encoded_records);
  }
  writer_.reset();
}
