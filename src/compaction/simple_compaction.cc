#include "compaction/simple_compaction.hpp"
#include "concat_iterator.hpp"
#include "sst/sst_builder.hpp"
#include "sst/sst_iterator.hpp"
#include "storage.hpp"
#include "two_merge_iterator.hpp"
#include <iostream>
#include <string>
SimpleCompaction::SimpleCompaction(SimpleCompactionOption &option)
    : opt_(option) {}

CompactionOp
SimpleCompaction::compact(const StorageStateSnapshot &state_snapshot) {
  // merge one l0_sst_ with l1_sst_

  std::vector<std::shared_ptr<SST>> new_sst_list;
  // build l0 iterator
  auto l0_merge_sst = state_snapshot.l0_sst_.front();
  std::unique_ptr<Iterator> l0_iter =
      std::move(std::make_unique<SSTIterator>(SSTIterator(l0_merge_sst)));

  // build l1 concat iterator
  std::vector<std::unique_ptr<Iterator>> l1_iter;
  l1_iter.reserve(state_snapshot.l1_sst_.size());
  for (auto &sst : state_snapshot.l1_sst_) {
    l1_iter.push_back(std::make_unique<SSTIterator>(SSTIterator(sst)));
  }
  auto l1_concat_iter = std::make_unique<ConcatIterator>(std::move(l1_iter));

  auto merge_iter =
      TwoMergeIterator(std::move(l0_iter), std::move(l1_concat_iter));

  auto sst_builder = state_snapshot.storage_->allocate_new_sst_builder();
  while (merge_iter.is_valid()) {
    auto key = merge_iter.key();
    auto val = merge_iter.value();
    auto can_add_new_entry = sst_builder.add_entry(key, val);

    // std::cout << "Key: " << key_str << ", Value: " << val_str << std::endl;
    if (!can_add_new_entry) {
      auto sst = sst_builder.build();
      new_sst_list.push_back(std::make_shared<SST>(std::move(sst)));
      sst_builder = state_snapshot.storage_->allocate_new_sst_builder();
      sst_builder.add_entry(key, val);
    }
    // handle the case when builder exceed the size ?
    merge_iter.next();
  }

  auto sst = sst_builder.build();
  new_sst_list.push_back(std::make_shared<SST>(std::move(sst)));

  std::vector<std::shared_ptr<SST>> delete_sst_list;
  delete_sst_list.reserve(1 + state_snapshot.l1_sst_.size());
  delete_sst_list.push_back(l0_merge_sst);
  for (auto &sst : state_snapshot.l1_sst_) {
    delete_sst_list.push_back(sst);
  }

  return CompactionOp{.deleted_files_ = delete_sst_list,
                      .new_files_ = new_sst_list};
}
