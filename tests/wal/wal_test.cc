#include "test_utilities.hpp"
#include "wal/wal.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <vector>

using test_utils::MakeBytesVector;
class WALTest : public ::testing::Test {
protected:
  const std::filesystem::path wal_path_{"wal_test.wal"};
  std::unique_ptr<WAL> wal_;

  void SetUp() override { wal_ = std::make_unique<WAL>(wal_path_); }

  void TearDown() override {
    wal_.reset();
    std::filesystem::remove(wal_path_);
  }
};

TEST_F(WALTest, AddRecordTest) {
  std::vector<WALRecord> records{
      {MakeBytesVector("key_1"), MakeBytesVector("value_1")},
      {MakeBytesVector("key_2"), MakeBytesVector("value_2")},
      {MakeBytesVector("key_3"), MakeBytesVector("value_3")},
  };
  for (auto &record : records) {
    wal_->add_record(record);
  }
  wal_.reset();

  auto decoded_record = WAL::read_wal(wal_path_);
  EXPECT_EQ(records, decoded_record);
}
