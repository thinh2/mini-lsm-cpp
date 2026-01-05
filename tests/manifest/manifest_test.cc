#include "fcntl.h"
#include "manifest/manifest.hpp"
#include <gtest/gtest.h>
class ManifestTest : public ::testing::Test {
protected:
  void TearDown() { unlink(test_path.c_str()); }
  const std::filesystem::path test_path{"manifest-test"};
};

TEST_F(ManifestTest, EncodeTest) {
  std::vector<ManifestRecord> records{
      {.id_ = 1, .type_ = ManifestRecordType::SST},
      {.id_ = 2, .type_ = ManifestRecordType::SST},
      {.id_ = 3, .type_ = ManifestRecordType::SST}};
  {
    auto [manifest, _] = Manifest::recover(test_path);
    for (auto &record : records)
      manifest.add_record(record);
  }

  {
    auto [_, decode_records] = Manifest::recover(test_path);
    EXPECT_EQ(records, decode_records);
  }
}
