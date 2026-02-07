#include "compaction/simple_compaction.hpp"
#include "sst/sst.hpp"
#include "sst/sst_builder.hpp"
#include "sst/sst_iterator.hpp"
#include "storage.hpp"
#include "test_utilities.hpp"
#include "gtest/gtest.h"
#include <memory>
#include <vector>

using test_utils::MakeKeyValueEntryFromString;

// Helper to create SSTs using real SSTBuilder
std::shared_ptr<SST>
CreateSST(Storage &storage,
          std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>>
              &entries) {
  auto builder = storage.allocate_new_sst_builder(true);
  for (auto &kv : entries) {
    builder.add_entry(kv.first, kv.second);
  }
  return std::make_shared<SST>(builder.build());
}

TEST(SimpleCompactionTest, DeletedFilesPresence) {
  auto l0_entries = MakeKeyValueEntryFromString({{"a", "l0-a"}});
  auto l1_entries = MakeKeyValueEntryFromString({{"b", "l1-b"}});
  Storage storage(StorageOption{.sst_size_ = 4096});
  auto l0_sst = CreateSST(storage, l0_entries);
  auto l1_sst = CreateSST(storage, l1_entries);
  StorageStateSnapshot snapshot{&storage, {l0_sst}, {l1_sst}};
  SimpleCompactionOption opt;
  SimpleCompaction compaction(opt);
  auto op = compaction.compact(snapshot);
  EXPECT_EQ(op.deleted_files_.size(), 2);
  EXPECT_NE(
      std::find(op.deleted_files_.begin(), op.deleted_files_.end(), l0_sst),
      op.deleted_files_.end());
  EXPECT_NE(
      std::find(op.deleted_files_.begin(), op.deleted_files_.end(), l1_sst),
      op.deleted_files_.end());
}

TEST(SimpleCompactionTest, EmptyL1ProducesL0Only) {
  auto l0_entries = MakeKeyValueEntryFromString({{"a", "l0-a"}, {"b", "l0-b"}});
  Storage storage(StorageOption{.sst_size_ = 4096});
  auto l0_sst = CreateSST(storage, l0_entries);
  StorageStateSnapshot snapshot{&storage, {l0_sst}, {}};
  SimpleCompactionOption opt;
  SimpleCompaction compaction(opt);
  auto op = compaction.compact(snapshot);
  // All entries should be from L0
  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>> merged;
  EXPECT_EQ(op.new_files_.size(), 1);
  for (auto &sst_ptr : op.new_files_) {
    SSTIterator iter(sst_ptr);
    while (iter.is_valid()) {
      merged.emplace_back(iter.key(), iter.value());
      iter.next();
    }
  }
  EXPECT_EQ(merged, l0_entries);
}

TEST(SimpleCompactionTest, MultipleSortedL1SSTs) {
  // L0 SST: keys a, b
  auto l0_entries = MakeKeyValueEntryFromString({{"a", "l0-a"}, {"b", "l0-b"}});
  // L1 SSTs: [c, d], [e, f], [g, h]
  auto l1_entries1 =
      MakeKeyValueEntryFromString({{"c", "l1-c"}, {"d", "l1-d"}});
  auto l1_entries2 =
      MakeKeyValueEntryFromString({{"e", "l1-e"}, {"f", "l1-f"}});
  auto l1_entries3 =
      MakeKeyValueEntryFromString({{"g", "l1-g"}, {"h", "l1-h"}});

  Storage storage(StorageOption{.sst_size_ = 4096});
  auto l0_sst = CreateSST(storage, l0_entries);
  auto l1_sst1 = CreateSST(storage, l1_entries1);
  auto l1_sst2 = CreateSST(storage, l1_entries2);
  auto l1_sst3 = CreateSST(storage, l1_entries3);

  std::vector<std::shared_ptr<SST>> l1_ssts = {l1_sst1, l1_sst2, l1_sst3};
  StorageStateSnapshot snapshot{&storage, {l0_sst}, l1_ssts};
  SimpleCompactionOption opt;
  SimpleCompaction compaction(opt);
  auto op = compaction.compact(snapshot);

  // Collect all entries from new_files
  std::vector<std::pair<std::vector<std::byte>, std::vector<std::byte>>> merged;
  for (auto &sst_ptr : op.new_files_) {
    SSTIterator iter(sst_ptr);
    while (iter.is_valid()) {
      merged.emplace_back(iter.key(), iter.value());
      iter.next();
    }
  }

  // Expected merged sequence
  auto expected = MakeKeyValueEntryFromString({{"a", "l0-a"},
                                               {"b", "l0-b"},
                                               {"c", "l1-c"},
                                               {"d", "l1-d"},
                                               {"e", "l1-e"},
                                               {"f", "l1-f"},
                                               {"g", "l1-g"},
                                               {"h", "l1-h"}});

  EXPECT_EQ(merged, expected);
  // Also check that keys are sorted
  for (size_t i = 1; i < merged.size(); ++i) {
    EXPECT_LT(merged[i - 1].first, merged[i].first);
  }
}
