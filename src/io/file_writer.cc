#include "io/file_writer.hpp"

#include <cerrno>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <system_error>
#include <unistd.h>

namespace {

int open_file(const fs::path &path) {
  int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
  int fd = ::open(path.c_str(), flags, 0644);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "failed to open file " + path.string());
  }
  return fd;
}

void fsync_with_full_barrier(int fd) {
#ifdef __APPLE__
  if (::fcntl(fd, F_FULLFSYNC, 0) == 0) {
    return;
  }
  // Fallback to fsync below
#endif
  if (::fsync(fd) != 0) {
    throw std::system_error(errno, std::generic_category(), "fsync failed");
  }
}

} // namespace

FileWriter::FileWriter(const fs::path &path) : path_name_(path) {
  auto parent = path_name_.parent_path();
  if (!parent.empty() && !fs::exists(parent)) {
    fs::create_directories(parent);
  }
  fd_ = open_file(path_name_);
}

void FileWriter::append(std::vector<std::byte> &buffer) {
  if (buffer.empty()) {
    return;
  }
  ensure_open();
  write_all(buffer.data(), buffer.size());
}

void FileWriter::append_and_sync(std::vector<std::byte> &buffer) {
  append(buffer);
  if (!buffer.empty()) {
    sync();
  }
}

void FileWriter::sync() {
  ensure_open();
  fsync_with_full_barrier(fd_);
}

void FileWriter::close() {
  if (fd_ == -1) {
    return;
  }
  if (::close(fd_) != 0) {
    throw std::system_error(errno, std::generic_category(), "close failed");
  }
  fd_ = -1;
}

void FileWriter::flush() { sync(); }

uint64_t FileWriter::file_size() {
  if (fd_ == -1) {
    return fs::file_size(path_name_);
  }
  struct stat st{};
  if (::fstat(fd_, &st) != 0) {
    throw std::system_error(errno, std::generic_category(), "fstat failed");
  }
  return static_cast<uint64_t>(st.st_size);
}

FileWriter::~FileWriter() {
  try {
    close();
  } catch (...) {
    // Destructors must not throw
  }
}

void FileWriter::ensure_open() const {
  if (fd_ == -1) {
    throw std::runtime_error("file descriptor is not open");
  }
}

void FileWriter::write_all(const std::byte *data, std::size_t size) {
  std::size_t written = 0;
  while (written < size) {
    ssize_t rv = ::write(fd_, reinterpret_cast<const void *>(data + written),
                         size - written);
    if (rv < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "write failed");
    }
    written += static_cast<std::size_t>(rv);
  }
}
