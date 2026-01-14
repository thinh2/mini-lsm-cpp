#include "storage.hpp"
#include "memtable.hpp"
#include "sst/sst_builder.hpp"
#include "utils.hpp"
#include "version_edit.hpp"
#include <algorithm>
#include <chrono>
#include <format>
#include <mutex>
#include <string_view>

Storage::Storage(StorageOption opt)
    : opt_(std::move(opt)), latest_table_id_(0), active_memtable_(nullptr),
      active_wal_(nullptr) {
  if (!opt_.sst_directory_.empty()) {
    std::filesystem::create_directories(opt_.sst_directory_);
  }

  auto [manifest, manifest_records] = Manifest::recover(opt_.manifest_path_);
  manifest_ = std::move(manifest);
  recover(manifest_records);

  new_active_memtable();
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
    new_active_memtable();
  }

  if (opt_.wal_sync_option == WALSyncOption::SYNC_ON_WRITE) {
    active_wal_->add_record_and_sync({.key_ = key, .value_ = value});
  } else {
    active_wal_->add_record({.key_ = key, .value_ = value});
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

void Storage::recover(const std::vector<VersionEdit> &manifest_records) {
  auto sst_pattern = opt_.sst_directory_ / "sst_{}";
  auto sst_pattern_view = sst_pattern.string();

  auto wal_pattern = opt_.wal_directory_ / "{}.wal";
  auto wal_pattern_view = wal_pattern.string();

  if (manifest_records.empty()) {
    latest_table_id_ = 0;
    return;
  }

  std::vector<uint64_t> wal;
  std::map<uint64_t, std::vector<uint64_t>> leveled;
  uint64_t min_recover_wal_id = 0;
  for (const auto &record : manifest_records) {
    if (record.get_wal_addition().has_value()) {
      wal.emplace_back(record.get_wal_addition()->file_id_);
    }

    // sst file
    if (!record.get_new_file().empty()) {
      for (const auto &new_file : record.get_new_file()) {
        leveled[new_file.level_].emplace_back(new_file.file_id_);
        min_recover_wal_id =
            std::max(new_file.file_id_ + 1, min_recover_wal_id);
      }
    }
  }

  // assume SST has 1 level.
  for (auto [level_id, level_data] : leveled) {
    for (auto &file_id : level_data) {
      auto path =
          std::vformat(sst_pattern_view, std::make_format_args(file_id));
      sst_.emplace_back(std::make_unique<SST>(path));
    }
  }

  for (auto &wal_id : wal) {
    auto path = std::vformat(wal_pattern_view, std::make_format_args(wal_id));
    immutable_memtable_.emplace_back(
        MemTable::recover(path, wal_id, opt_.mem_table_size_));
  }

  if (!sst_.empty()) {
    latest_table_id_ = sst_.back()->get_id() + 1;
  }

  if (!immutable_memtable_.empty()) {
    latest_table_id_ =
        std::max(latest_table_id_, immutable_memtable_.back()->get_id() + 1);
  }
}

void Storage::new_active_memtable() {
  latest_table_id_++;
  active_memtable_ =
      std::make_unique<MemTable>(opt_.mem_table_size_, latest_table_id_);
  auto wal_path =
      opt_.wal_directory_ / (std::to_string(latest_table_id_) + ".wal");
  active_wal_ = std::make_unique<WAL>(wal_path);
  VersionEdit version_edit;
  version_edit.add_new_wal(latest_table_id_);
  manifest_.add_record(version_edit);
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
    VersionEdit version_edit;
    for (auto &table : sst) {
      version_edit.add_new_file(0, table->get_id());
    }
    manifest_.add_record(version_edit);
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
