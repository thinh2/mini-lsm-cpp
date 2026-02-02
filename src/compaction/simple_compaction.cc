#include "compaction/simple_compaction.hpp"
#include "storage.hpp"

SimpleCompaction::SimpleCompaction(SimpleCompactionOption &option)
    : opt_(option) {}

CompactionOp
SimpleCompaction::compact(const StorageStateSnapshot &state_snapshot) {
  // merge l0_sst_
}
