#include "sst/block.hpp"
#include "sst/block_builder.hpp"
#include "sst/sst.hpp"
#include "test_utilities.hpp"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <optional>
#include <sst/sst_builder.hpp>
#include <tuple>

using test_utils::MakeBytesVector;

class SSTTest : public ::testing::Test {
protected:
  void SetUp() { tmp_file_ = std::ofstream(FILE_NAME_); }

  void TearDown() {
    unlink(FILE_NAME_.c_str());
    for (auto &path : sst_paths) {
      unlink(path.c_str());
    }
  }

  std::tuple<SST, SSTBuilder> make_sst_table(size_t n_entries,
                                             std::filesystem::path &&path_name,
                                             SSTConfig &config) {
    sst_paths.push_back(path_name);
    SSTBuilder sst_builder(path_name, config);
    std::vector<std::pair<std::string, std::string>> entries;
    for (size_t i = 0; i < n_entries; i++) {
      auto key = "key" + std::to_string(i);
      auto val = "value" + std::to_string(i);
      entries.emplace_back(key, val);
    }

    std::sort(entries.begin(), entries.end());
    for (auto &entry : entries) {
      auto key_vec = MakeBytesVector(std::move(entry.first));
      auto val_vec = MakeBytesVector(std::move(entry.second));
      sst_builder.add_entry(key_vec, val_vec);
    }

    return std::make_tuple(sst_builder.build(), std::move(sst_builder));
  }

  std::ofstream tmp_file_;
  const std::filesystem::path FILE_NAME_{"/tmp/sst-test"};
  std::vector<std::filesystem::path> sst_paths;
};

TEST_F(SSTTest, TestZeroDataBlock) {
  std::vector<std::byte> footer{std::byte(0), std::byte(0), std::byte(0),
                                std::byte(0)};
  tmp_file_.write(reinterpret_cast<char *>(footer.data()), footer.size());
  tmp_file_.close();
  SST sst(FILE_NAME_);

  std::vector<std::byte> key = MakeBytesVector("hello");
  EXPECT_EQ(std::nullopt, sst.get(key));
}

TEST_F(SSTTest, TestSingleEntry) {
  SSTConfig config{.block_size_ = 1024};

  int n_entries = 1;
  auto [sst, sst_builder] =
      make_sst_table(n_entries, std::filesystem::path("sst-get-test"), config);

  auto sst_block_metadata = sst.get_block_metadata();
  auto sst_builder_block_metadata = sst_builder.get_block_metadata();
  for (int i = 0; i < sst_builder_block_metadata.size(); i++) {
    EXPECT_EQ(sst_builder_block_metadata[i].offset_,
              sst_block_metadata[i].offset_);
    EXPECT_EQ(sst_builder_block_metadata[i].size_, sst_block_metadata[i].size_);
    EXPECT_EQ(sst_builder_block_metadata[i].first_key_,
              sst_block_metadata[i].first_key_);
    EXPECT_EQ(sst_builder_block_metadata[i].last_key_,
              sst_block_metadata[i].last_key_);
  }
}

TEST_F(SSTTest, TestBlockMetadata) {
  SSTConfig config{.block_size_ = 1024};
  int n_entries = 100;
  auto [sst, sst_builder] =
      make_sst_table(n_entries, std::filesystem::path("sst-get-test"), config);

  auto sst_block_metadata = sst.get_block_metadata();
  auto sst_builder_block_metadata = sst_builder.get_block_metadata();

  EXPECT_EQ(sst.get_block_metadata(), sst_builder.get_block_metadata());
}

TEST_F(SSTTest, TestSSTGet) {
  SSTConfig config{.block_size_ = 1024};
  int n_entries = 1000;
  auto [sst, _] =
      make_sst_table(n_entries, std::filesystem::path("sst-get-test"), config);
  for (size_t i = 0; i < n_entries; i++) {
    auto key_vec = MakeBytesVector("key" + std::to_string(i));
    auto expected_val = MakeBytesVector("value" + std::to_string(i));
    auto val_get = sst.get(key_vec);
    EXPECT_TRUE(val_get.has_value());
    EXPECT_EQ(val_get.value(), expected_val);
  }

  {
    auto key_vec = MakeBytesVector("hello");
    auto val_get = sst.get(key_vec);
    EXPECT_FALSE(val_get.has_value());
  }
}
