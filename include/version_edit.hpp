#include <nlohmann/json.hpp>
#include <optional>
#include <set>
#include <tuple>

struct DeletedFileMetadata {
  uint64_t level_;
  uint64_t file_id_;

  bool operator<(const DeletedFileMetadata &other) const {
    return std::tie(level_, file_id_) < std::tie(other.level_, other.file_id_);
  }
};

struct NewFileMetadata {
  uint64_t level_;
  uint64_t file_id_;

  bool operator<(const NewFileMetadata &other) const {
    return std::tie(level_, file_id_) < std::tie(other.level_, other.file_id_);
  }

  bool operator==(const NewFileMetadata &other) const {
    return std::tie(level_, file_id_) == std::tie(other.level_, other.file_id_);
  }
};

struct WALAddition {
  uint64_t file_id_;
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(NewFileMetadata, level_, file_id_);
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(WALAddition, file_id_);

// Add these in a .cpp file or after struct definitions
inline void to_json(nlohmann::json &j, const std::optional<WALAddition> &opt) {
  if (opt)
    j = *opt;
  else
    j = nullptr;
}
inline void from_json(const nlohmann::json &j,
                      std::optional<WALAddition> &opt) {
  if (j.is_null())
    opt = std::nullopt;
  else
    opt = j.get<WALAddition>();
}

class VersionEdit {
public:
  void add_new_file(uint64_t level, uint64_t file_id);
  void add_new_wal(uint64_t wal_id);
  const std::set<NewFileMetadata> &get_new_file() const;
  const std::optional<WALAddition> &get_wal_addition() const;
  bool operator==(const VersionEdit &other) const {
    return new_files_ == other.new_files_;
  };

public:
  std::set<NewFileMetadata> new_files_;
  std::optional<WALAddition> wal_addition_;
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(VersionEdit, new_files_, wal_addition_);
