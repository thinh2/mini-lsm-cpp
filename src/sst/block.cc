#include "sst/block.hpp"
#include "utils.hpp"
#include <algorithm>
#include <span>

std::vector<std::byte> Block::encode() {
  std::vector<std::byte> encoded_data;
  encoded_data.append_range(data_);
  for (auto &offset : offsets_) {
    encoded_data.append_range(encode_uint16_t(offset));
  }
  encoded_data.append_range(encode_uint16_t(offsets_.size()));
  return encoded_data;
}

Block Block::decode(const std::vector<std::byte> &data) {
  if (data.size() < FooterLenSize) {
    throw std::runtime_error("Block data should have the footer's length");
  }

  int sz = data.size();
  std::array<std::byte, FooterLenSize> footer = {data[sz - 2], data[sz - 1]};
  uint16_t entries_num = decode_uint16_t(footer);

  std::vector<uint16_t> offsets;
  offsets.resize(entries_num);

  // TODO: handle the case offsets_start_idx < 0
  size_t offsets_start_idx = sz - entries_num * OffsetSize - FooterLenSize;
  size_t data_block_length = offsets_start_idx;

  for (size_t entry_idx = 0; entry_idx < entries_num;
       entry_idx++, offsets_start_idx += OffsetSize) {
    std::array<std::byte, OffsetSize> offset_data{data[offsets_start_idx],
                                                  data[offsets_start_idx + 1]};
    offsets[entry_idx] = decode_uint16_t(offset_data);
  }

  std::vector<std::byte> data_block(data.begin(),
                                    data.begin() + data_block_length);

  return Block(data_block, offsets);
}

Block::Entry Block::get_entry(size_t entry_idx) {
  if (entry_idx > offsets_.size())
    throw std::runtime_error("out of bound entry index");
  Block::Entry result;
  std::array<std::byte, 2> len{data_[offsets_[entry_idx]],
                               data_[offsets_[entry_idx] + 1]};
  uint16_t key_len = decode_uint16_t(len);
  result.key_.resize(key_len);
  std::copy(data_.begin() + offsets_[entry_idx] + Block::EntryKeyLenSize,
            data_.begin() + offsets_[entry_idx] + Block::EntryKeyLenSize +
                key_len,
            result.key_.begin());

  size_t value_offset = offsets_[entry_idx] + Block::EntryKeyLenSize + key_len;
  len = {data_[value_offset], data_[value_offset + 1]};
  uint16_t value_len = decode_uint16_t(len);
  result.value_.resize(value_len);
  std::copy(data_.begin() + value_offset + Block::EntryValueLenSize,
            data_.begin() + value_offset + Block::EntryValueLenSize + value_len,
            result.value_.begin());
  return result;
}

size_t Block::size() { return offsets_.size(); }

// TODO: implement binary search for faster key finding
std::optional<std::vector<std::byte>>
Block::get(const std::vector<std::byte> &key) {
  for (size_t i = 0; i < offsets_.size(); i++) {
    auto entry = get_entry(i);
    if (entry.key_ == key) {
      return entry.value_;
    }
  }
  return std::nullopt;
}
