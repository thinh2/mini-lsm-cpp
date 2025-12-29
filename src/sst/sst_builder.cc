#include "sst/sst_builder.hpp"
#include "sst/block.hpp"
#include "sst/sst.hpp"
#include "sst/sst_builder.hpp"
#include "utils.hpp"

SSTBuilder::SSTBuilder(const std::filesystem::path &path, SSTConfig &sst_config)
    : finished_(false), sst_config_(sst_config), path_(path) {
  out_ = std::ofstream(path, std::ios::binary);
}

void SSTBuilder::add_entry(std::vector<std::byte> &key,
                           std::vector<std::byte> &val) {
  if (finished_) {
    throw std::runtime_error("SSTBuilder build finished");
  }

  if (2 + key.size() + 2 + val.size() + block_builder_.get_size() >
      sst_config_.block_size_) {
    write_block();
  }
  block_builder_.add_entry(key, val);
}

SST SSTBuilder::build() {
  if (block_builder_.get_size() > 0) {
    write_block();
  }

  std::vector<uint64_t> block_metadata_offsets;
  block_metadata_offsets.reserve(block_metadata_.size());

  // write block metadata
  for (auto &block_metadata : block_metadata_) {
    auto encoded_block_metadata = block_metadata.encode();

    uint64_t offset = static_cast<uint64_t>(out_.tellp());
    block_metadata_offsets.push_back(offset);

    out_.write(reinterpret_cast<char *>(encoded_block_metadata.data()),
               encoded_block_metadata.size());
  }

  // write block_metadata_offset
  for (auto &offset : block_metadata_offsets) {
    auto encoded_offset = encode_uint64_t(offset);
    out_.write(reinterpret_cast<char *>(encoded_offset.data()),
               encoded_offset.size());
  }

  // write number of block
  auto encoded_num_block_val = encode_uint64_t(block_metadata_.size());
  out_.write(reinterpret_cast<char *>(encoded_num_block_val.data()),
             encoded_num_block_val.size());

  finished_ = true;
  out_.close();

  // TODO: consider copying the block_metadata to avoid reading?
  return SST(path_);
}

const std::vector<BlockMetadata> &SSTBuilder::get_block_metadata() const {
  return block_metadata_;
}
void SSTBuilder::write_block() {
  auto block = block_builder_.build();
  auto encoded_block = block.encode();

  uint64_t offset = static_cast<uint64_t>(out_.tellp());
  out_.write(reinterpret_cast<char *>(encoded_block.data()),
             encoded_block.size());
  block_metadata_.emplace_back(offset, encoded_block.size(),
                               block.get_first_key(), block.get_last_key());
  block_builder_ = BlockBuilder();
}
