#include "version_edit.hpp"

void VersionEdit::add_new_file(uint64_t level, uint64_t file_id) {
  new_files_.insert(NewFileMetadata{.level_ = level, .file_id_ = file_id});
}

void VersionEdit::add_new_wal(uint64_t wal_id) {
  wal_addition_ = WALAddition{.file_id_ = wal_id};
}

const std::set<NewFileMetadata> &VersionEdit::get_new_file() const {
  return new_files_;
}

const std::optional<WALAddition> &VersionEdit::get_wal_addition() const {
  return wal_addition_;
}
