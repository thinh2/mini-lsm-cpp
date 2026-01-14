#include "fcntl.h"
#include "manifest/manifest.hpp"
#include "version_edit.hpp"
#include <gtest/gtest.h>
class ManifestTest : public ::testing::Test {
protected:
  void TearDown() { unlink(test_path.c_str()); }
  const std::filesystem::path test_path{"manifest-test"};
};

TEST_F(ManifestTest, EncodeTest) {
  std::vector<VersionEdit> records;
  VersionEdit v0, v1, v2;
  v0.add_new_file(0, 1);
  v1.add_new_file(0, 2);
  v2.add_new_file(0, 3);
  records.emplace_back(v0);
  records.emplace_back(v1);
  records.emplace_back(v2);
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
