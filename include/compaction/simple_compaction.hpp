#pragma once
#include "compaction/compaction.hpp"

struct SimpleCompactionOption {
  uint64_t level_1_max_size_;
  uint64_t size_ratio_;
};

class SimpleCompaction : public Compaction {
public:
  SimpleCompaction(SimpleCompactionOption &option);
  CompactionOp compact(const StorageStateSnapshot &state_snapshot);

private:
  SimpleCompactionOption opt_;
};
