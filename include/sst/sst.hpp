#pragma once

#include "io/file_reader.hpp"
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

class FileReader;
class Block;
class BlockIterator;
class BlockBuilder;

/**
 * @brief SST encoded format
 * block | ... | block | block_metadata | ... | block_metadata |
 * block_metadata_offset (u64) | ... | block_metadata_offset (u64) |
 * n_block (u64)
 *
 * block_metadata encoded format:
 *  block_offset (8 bytes) | block_size (8 bytes) | first_key_len (2 bytes) |
 * first_key | last_key_len (2 bytes) | last_key
 *
 * constructor will read the block_metadata into the memory.
 * block is accessed on demand from disk to avoid OOM.
 */
class BlockMetadata {
public:
  uint64_t offset_;
  uint64_t size_;
  std::vector<std::byte> first_key_;
  std::vector<std::byte> last_key_;
  std::vector<std::byte> encode();
  bool operator==(const BlockMetadata &) const;

public:
  static const int BLOCK_OFFSET_VAL_SIZE = 8;
  static const int BLOCK_SIZE_VAL_SIZE = 8;
  static const int BLOCK_FIRST_KEY_LEN_VAL_SIZE = 2;
  static const int BLOCK_LAST_KEY_LEN_VAL_SIZE = 2;
};

class SST {
public:
  SST(const std::filesystem::path &file_name);
  std::optional<std::vector<std::byte>> get(std::vector<std::byte> &key);

  const std::vector<BlockMetadata> &get_block_metadata() const;
  Block get_block(size_t block_idx) const;
  size_t number_of_block() const;

private:
  static const uint32_t NUMBER_OF_BLOCK_VAL_SIZE = 8;
  static const uint32_t BLOCK_METADATA_OFFSET_VAL_SIZE = 8;

private:
  void read_block_metadata();
  Block read_block(const BlockMetadata &) const;
  uint64_t decode_uint64(size_t offset);
  std::vector<std::byte> decode_var_string(size_t offset);

private:
  std::vector<BlockMetadata> block_metadata_;
  std::unique_ptr<FileReader> io_;
};
