#include "io/file_reader.hpp"

FileReader::FileReader(const fs::path &path) : path_name_(path) {
  in_ = std::ifstream(path_name_, std::ios::in | std::ios::binary);
  file_size_ = std::filesystem::file_size(path_name_);
}

void FileReader::read(size_t offsets, size_t length,
                      std::vector<std::byte> &buffer) {
  in_.seekg(offsets);
  in_.read(reinterpret_cast<char *>(buffer.data()), length);
}

void FileReader::close() { in_.close(); }

size_t FileReader::file_size() { return file_size_; }

FileReader::~FileReader() { close(); }
