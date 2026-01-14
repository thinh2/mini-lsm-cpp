#pragma once
#include "io/file_writer.hpp"
#include <filesystem>
#include <set>
#include <utility>
#include <vector>

class FileReader;
class VersionEdit;

class Manifest {
public:
  static std::pair<Manifest, std::vector<VersionEdit>>
  recover(const std::filesystem::path &path);
  void add_record(const VersionEdit &record);

  Manifest() = default;
  //  Manifest(const Manifest &other) = delete;
  //  Manifest(Manifest &&other) = default;
  //  Manifest &operator=(const Manifest &) = delete; // No copy assignment
  //  Manifest &operator=(Manifest &&) = default;     // Move assignment
private:
  Manifest(const std::filesystem::path &path);

private:
  std::unique_ptr<FileWriter> writer_;
};
