#include "storage.hpp"
#include "memtable.hpp"
#include "sst/sst_builder.hpp"
#include "utils.hpp"
#include <algorithm>
#include <chrono>
#include <format>
#include <mutex>
#include <string_view>

Storage::Storage(StorageOption opt)
    : opt_(std::move(opt)), latest_table_id_(0), active_memtable_(nullptr) {
  if (!opt_.sst_directory_.empty()) {
    std::filesystem::create_directories(opt_.sst_directory_);
  }

  auto [manifest, manifest_records] = Manifest::recover(opt_.manifest_path_);
  manifest_ = std::move(manifest);
  recover(manifest_records);

  active_memtable_ =
      std::make_unique<MemTable>(opt_.mem_table_size_, latest_table_id_++);
  stopped_.store(false, std::memory_order_relaxed);
  flush_thread_ = std::thread([this]() { this->flush_thread(); });
};

void Storage::put(std::vector<std::byte> &key, std::vector<std::byte> &value) {
  if (stopped_.load(std::memory_order_acquire)) {
    throw std::runtime_error("storage stopped");
  }

  uint64_t record_size = key.size() + value.size();
  std::lock_guard lk{mu_};
  if (record_size + active_memtable_->size() > opt_.mem_table_size_) {
    active_memtable_->freeze();
    immutable_memtable_.push_back(std::move(active_memtable_));
    active_memtable_ =
        std::make_unique<MemTable>(opt_.mem_table_size_, latest_table_id_++);
  }

  active_memtable_->put(key, value);
}

std::optional<std::vector<std::byte>>
Storage::get(std::vector<std::byte> &key) {
  if (stopped_.load(std::memory_order_acquire)) {
    throw std::runtime_error("storage stopped");
  }

  std::shared_lock lk{mu_};
  std::optional<std::vector<std::byte>> value_slice;
  value_slice = active_memtable_->get(key);
  if (value_slice.has_value()) {
    if (value_slice.value().size() > 0) {
      return value_slice;
    }
    return std::nullopt;
  }

  for (auto it = immutable_memtable_.rbegin(); it != immutable_memtable_.rend();
       it = std::next(it)) {
    value_slice = (*it)->get(key);
    if (value_slice == std::nullopt)
      continue;
    if (value_slice.value().size() > 0) {
      return value_slice;
    }
    return std::nullopt;
  }

  for (auto it = sst_.rbegin(); it != sst_.rend(); ++it) {
    value_slice = (*it)->get(key);
    if (value_slice == std::nullopt)
      continue;
    if (value_slice.value().size() > 0) {
      return value_slice;
    }
    return std::nullopt;
  }
  return std::nullopt;
}

void Storage::remove(std::vector<std::byte> &key) { put(key, TOMBSTONE); }

std::vector<std::unique_ptr<SST>>
Storage::flush_to_SST(std::vector<std::shared_ptr<MemTable>> &mem_table_ptr) {
  std::vector<std::unique_ptr<SST>> new_sst;
  SSTConfig sst_config{.block_size_ = opt_.max_sst_block_size_,
                       .sst_directory_ = opt_.sst_directory_};
  for (auto &mem_table : mem_table_ptr) {
    new_sst.emplace_back(std::make_unique<SST>(mem_table->flush(sst_config)));
  }
  return new_sst;
}

void Storage::flush_thread() {
  while (!stopped_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    flush_run(false);
  }
}

void Storage::recover(const std::vector<ManifestRecord> &manifest_records) {
  auto sst_pattern = opt_.sst_directory_ / "sst_{}";
  auto sst_pattern_view = sst_pattern.string();

  if (manifest_records.empty()) {
    latest_table_id_ = 0;
    return;
  }

  for (auto &record : manifest_records) {
    auto path =
        std::vformat(sst_pattern_view, std::make_format_args(record.id_));
    sst_.emplace_back(std::make_unique<SST>(path));
  }

  std::sort(sst_.begin(), sst_.end(),
            [](const std::unique_ptr<SST> &a, const std::unique_ptr<SST> &b) {
              return a->get_id() < b->get_id();
            });

  latest_table_id_ = sst_.back()->get_id() + 1;
}

void Storage::flush_run(bool flush_all) {
  std::vector<std::shared_ptr<MemTable>> flush_memtables;
  int flush_memtable_count = 0;

  {
    std::shared_lock lk{mu_};
    if (flush_all) {
      flush_memtables = immutable_memtable_;
    } else {
      flush_memtable_count =
          std::max(static_cast<int>(immutable_memtable_.size()) -
                       static_cast<int>(opt_.max_number_of_memtable_),
                   0);
      if (!flush_memtable_count)
        return;
      flush_memtables = std::vector<std::shared_ptr<MemTable>>(
          immutable_memtable_.begin(),
          immutable_memtable_.begin() + flush_memtable_count);
    }
  }

  auto sst = flush_to_SST(flush_memtables);

  {
    std::lock_guard lk{mu_};
    for (auto &table : sst) {
      manifest_.add_record(
          {.id_ = table->get_id(), .type_ = ManifestRecordType::SST});
    }
    sst_.insert(sst_.end(), std::make_move_iterator(sst.begin()),
                std::make_move_iterator(sst.end()));
    immutable_memtable_.erase(immutable_memtable_.begin(),
                              immutable_memtable_.begin() +
                                  flush_memtable_count);
  }
}

void Storage::close() {
  stopped_.store(true, std::memory_order_release);
  flush_thread_.join();

  active_memtable_->freeze();
  immutable_memtable_.push_back(std::move(active_memtable_));
  flush_run(true);
}

uint64_t Storage::get_current_table_id() { return latest_table_id_; }

Storage::~Storage() {
  // TODO: release the unique_ptr, shared_ptr?
  if (!stopped_.load(std::memory_order_acquire))
    close();
}
