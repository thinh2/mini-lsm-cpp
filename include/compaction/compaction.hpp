#pragma once
#include <memory>
#include <vector>

struct StorageStateSnapshot;
class SST;

struct CompactionOp {
  std::vector<std::shared_ptr<SST>> deleted_files_;
  std::vector<std::shared_ptr<SST>> new_files_;
};

class Compaction {
public:
  virtual CompactionOp compact(const StorageStateSnapshot &snapshot) = 0;
};
