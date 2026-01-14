#include "manifest/manifest.hpp"
#include "io/file_reader.hpp"
#include "version_edit.hpp"
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

// NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(VersionEdit, new_files_, wal_addition_);

Manifest::Manifest(const std::filesystem::path &path)
    : writer_(std::make_unique<FileWriter>(path)) {}

std::pair<Manifest, std::vector<VersionEdit>>
Manifest::recover(const std::filesystem::path &path) {
  if (!std::filesystem::exists(path)) {
    return {std::move(Manifest{path}), {}};
  }

  std::vector<VersionEdit> records;
  std::ifstream in_{path};
  std::string line;
  while (std::getline(in_, line)) {
    auto j = json::parse(line);
    auto record = j.get<VersionEdit>();
    records.push_back(record);
  }
  return {std::move(Manifest{path}), records};
}

void Manifest::add_record(const VersionEdit &record) {
  auto encoded_json = json(record).dump();
  encoded_json.append("\n");
  std::vector<std::byte> bytes;
  bytes.resize(encoded_json.size());
  std::memcpy(bytes.data(), encoded_json.data(), encoded_json.size());
  writer_->append_and_sync(bytes);
}
