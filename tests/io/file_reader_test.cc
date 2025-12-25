#include "io/file_reader.hpp"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

class FileReaderTest : public ::testing::Test {
protected:
  void SetUp() override { test_file_ = std::ofstream(FILE_PATH_); }

  void TearDown() override { unlink(FILE_PATH_.c_str()); }

protected:
  std::ofstream test_file_;
  const std::string FILE_PATH_ = "/tmp/mini_lsm_file_reader_test";
};

TEST_F(FileReaderTest, SizeTest) {
  test_file_.write("hello_world!", 12);
  test_file_.close();

  FileReader reader{std::filesystem::path(FILE_PATH_)};
  EXPECT_EQ(reader.file_size(), 12);
}

TEST_F(FileReaderTest, ReadTest) {
  test_file_.write("hello_world!", 12);
  test_file_.close();

  FileReader reader{std::filesystem::path(FILE_PATH_)};
  std::vector<std::byte> buffer;
  std::vector<std::byte> expected_buffer{std::byte('l'), std::byte('l'),
                                         std::byte('o'), std::byte('_'),
                                         std::byte('w')};
  buffer.resize(5);

  reader.read(2, 5, buffer);
  EXPECT_EQ(buffer, expected_buffer);
}
