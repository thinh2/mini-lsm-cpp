#include "sst/sst.hpp"

#include "io/file_reader.hpp"
#include "sst/block.hpp"
#include "utils.hpp"
#include <filesystem>
#include <memory>

SST::SST(const std::filesystem::path &file_name) {
  io_ = std::make_unique<FileReader>(file_name);
  read_block_metadata();
}

std::optional<std::vector<std::byte>> SST::get(std::vector<std::byte> &key) {
  for (auto &block_metadata : block_metadata_) {
    if (block_metadata.first_key_ == key ||
        (block_metadata.first_key_ < key && block_metadata.last_key_ < key) ||
        block_metadata.last_key_ == key) {
      auto block = std::move(read_block(block_metadata));
      return block.get(key);
    }
  }
  return std::nullopt;
}

void SST::read_block_metadata() {
  std::vector<std::byte> buffer;
  // read number of block
  size_t number_of_block_offset =
      io_->file_size() - 1 - SST::NUMBER_OF_BLOCK_VAL_SIZE;
  uint32_t n_blocks = decode_uint64(number_of_block_offset);
  if (n_blocks == 0)
    return;

  // read block_metadata_offset
  std::vector<size_t> block_metadata_offset;
  block_metadata_offset.resize(n_blocks);
  for (size_t block_id = n_blocks - 1,
              curr_offset =
                  number_of_block_offset - BLOCK_METADATA_OFFSET_VAL_SIZE - 1;
       block_id >= 0;
       block_id--, curr_offset -= BLOCK_METADATA_OFFSET_VAL_SIZE) {
    block_metadata_offset[block_id] = decode_uint64(curr_offset);
  }

  // read block_metadata
  block_metadata_.resize(n_blocks);
  for (size_t block_id = 0; block_id < n_blocks; block_id++) {
    auto curr_offset = block_metadata_offset[block_id];
    block_metadata_[block_id].offset_ = decode_uint64(curr_offset);

    curr_offset += BlockMetadata::BLOCK_OFFSET_VAL_SIZE;
    block_metadata_[block_id].size_ = decode_uint64(curr_offset);

    curr_offset += BlockMetadata::BLOCK_SIZE_VAL_SIZE;
    block_metadata_[block_id].first_key_ =
        std::move(decode_var_string(curr_offset));

    curr_offset += block_metadata_[block_id].first_key_.size();
    block_metadata_[block_id].last_key_ =
        std::move(decode_var_string(curr_offset));
  }
}

Block SST::read_block(const BlockMetadata &block_metadata) {
  std::vector<std::byte> buffer;
  buffer.resize(block_metadata.size_);
  io_->read(block_metadata.offset_, block_metadata.size_, buffer);
  Block block = Block::decode(std::move(buffer));
  return block;
}

// TODO: make this func as template
uint64_t SST::decode_uint64(size_t offset) {
  std::vector<std::byte> buffer;
  // read number of block
  buffer.resize(4);
  io_->read(offset, 4, buffer);

  std::span<const std::byte, 4> buffer_span{buffer.data(), buffer.size()};
  return decode_uint64_t(buffer_span);
}

std::vector<std::byte> SST::decode_var_string(size_t offset) {
  std::vector<std::byte> buffer;
  buffer.resize(2);
  io_->read(offset, 2, buffer);
  std::span<const std::byte, 2> buffer_span{buffer.data(), buffer.size()};

  offset += 2;
  auto string_len = decode_uint16_t(buffer_span);
  buffer.resize(string_len);
  io_->read(offset, string_len, buffer);
  return buffer;
}
