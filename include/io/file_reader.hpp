#pragma once
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

class FileReader {
public:
  FileReader(const fs::path &path);
  void read(size_t offsets, size_t length, std::vector<std::byte> &buffer);
  void close();
  size_t file_size();
  ~FileReader();

private:
  std::ifstream in_;
  fs::path path_name_;
  size_t file_size_;
};
