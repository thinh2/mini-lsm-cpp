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

const std::vector<BlockMetadata> &SST::get_block_metadata() const {
  return block_metadata_;
}

void SST::read_block_metadata() {
  std::vector<std::byte> buffer;
  // read number of block
  size_t number_of_block_offset =
      io_->file_size() - SST::NUMBER_OF_BLOCK_VAL_SIZE;
  uint32_t n_blocks = decode_uint64(number_of_block_offset);
  if (n_blocks == 0)
    return;

  // read block_metadata_offset
  std::vector<size_t> block_metadata_offset;
  block_metadata_offset.resize(n_blocks);
  for (int block_id = n_blocks - 1,
           curr_offset =
               number_of_block_offset - BLOCK_METADATA_OFFSET_VAL_SIZE;
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

    curr_offset += BlockMetadata::BLOCK_FIRST_KEY_LEN_VAL_SIZE +
                   block_metadata_[block_id].first_key_.size();
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
  buffer.resize(8);
  io_->read(offset, 8, buffer);

  std::span<const std::byte, 8> buffer_span{buffer.data(), buffer.size()};
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

std::vector<std::byte> BlockMetadata::encode() {
  std::vector<std::byte> encoded_block_metadata;
  encoded_block_metadata.reserve(
      BLOCK_OFFSET_VAL_SIZE + BLOCK_SIZE_VAL_SIZE +
      BLOCK_FIRST_KEY_LEN_VAL_SIZE + first_key_.size() +
      BLOCK_LAST_KEY_LEN_VAL_SIZE + last_key_.size());

  auto encoded_offset = encode_uint64_t(offset_);
  encoded_block_metadata.append_range(encoded_offset);

  auto encoded_size = encode_uint64_t(size_);
  encoded_block_metadata.append_range(encoded_size);

  auto encoded_first_key_len = encode_uint16_t(first_key_.size());
  encoded_block_metadata.append_range(encoded_first_key_len);
  encoded_block_metadata.append_range(first_key_);

  auto encoded_last_key_len = encode_uint16_t(last_key_.size());
  encoded_block_metadata.append_range(encoded_last_key_len);
  encoded_block_metadata.append_range(last_key_);

  return encoded_block_metadata;
}

bool BlockMetadata::operator==(const BlockMetadata &other) const {
  return offset_ == other.offset_ && size_ == other.size_ &&
         first_key_ == other.first_key_ && last_key_ == other.last_key_;
}
