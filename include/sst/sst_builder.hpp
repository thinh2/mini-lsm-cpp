#pragma once
#include "sst/block_builder.hpp"
#include <filesystem>
#include <fstream>

class SST;
class BlockMetadata;

struct SSTConfig {
  size_t block_size_;
  std::filesystem::path sst_directory_;
};

class SSTBuilder {
public:
  SSTBuilder(const std::filesystem::path &path, SSTConfig &sst_config);
  void add_entry(std::vector<std::byte> &key, std::vector<std::byte> &val);
  SST build();

  // for testing only
  const std::vector<BlockMetadata> &get_block_metadata() const;

private:
  void write_block();

private:
  bool finished_;
  SSTConfig sst_config_;
  std::ofstream out_;
  BlockBuilder block_builder_;
  std::vector<BlockMetadata> block_metadata_;
  std::filesystem::path path_;
};
