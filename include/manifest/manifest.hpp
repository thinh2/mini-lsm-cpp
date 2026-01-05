#pragma once
#include "io/file_writer.hpp"
#include <filesystem>
#include <utility>
#include <vector>

class FileReader;
enum class ManifestRecordType {
  SST,
};

struct ManifestRecord {
  ManifestRecordType type_;
  uint64_t id_;
  bool operator==(const ManifestRecord &other) const {
    return id_ == other.id_ && type_ == other.type_;
  };
};

class Manifest {
public:
  static std::pair<Manifest, std::vector<ManifestRecord>>
  recover(const std::filesystem::path &path);
  void add_record(const ManifestRecord &record);

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
