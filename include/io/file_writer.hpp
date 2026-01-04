#pragma once
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;
/**
 * @brief TODO: implement buffered writer for the FileWriter.
 *
 */
class FileWriter {
public:
  FileWriter(const fs::path &path);
  void append(std::vector<std::byte> &buffer);
  void append_and_sync(std::vector<std::byte> &buffer);
  void sync();
  void close();
  void flush();
  uint64_t file_size();
  ~FileWriter();

private:
  fs::path path_name_;
  int fd_{-1};

  void ensure_open() const;
  void write_all(const std::byte *data, std::size_t size);
};
