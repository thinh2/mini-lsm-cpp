#include "sst/block.hpp"
#include "sst/block_builder.hpp"
#include "sst/sst.hpp"
#include "test_utilities.hpp"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <optional>
using test_utils::MakeBytesVector;

class SSTTest : public ::testing::Test {
protected:
  void SetUp() { tmp_file_ = std::ofstream(FILE_NAME_); }

  void TearDown() { unlink(FILE_NAME_.c_str()); }

  std::ofstream tmp_file_;
  const std::filesystem::path FILE_NAME_{"/tmp/sst-test"};
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
