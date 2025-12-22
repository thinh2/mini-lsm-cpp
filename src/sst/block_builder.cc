#include "sst/block_builder.hpp"
#include "sst/block.hpp"
#include "utils.hpp"
/**
 * @brief
 *  entry format in binary: key_len (2byte), key, value_len (2byte), value
 *  offset format in binary:
 */
void BlockBuilder::add_entry(std::vector<std::byte> &key,
                             std::vector<std::byte> &value) {
  uint16_t key_size = key.size();
  uint16_t value_size = value.size();
  offsets_.push_back(static_cast<uint16_t>(size_));

  data_.append_range(encode_uint16_t(key_size));
  data_.append_range(key);
  data_.append_range(encode_uint16_t(value_size));
  data_.append_range(value);

  size_ += Block::EntryKeyLenSize + key.size() + Block::EntryValueLenSize +
           value.size();
}

Block BlockBuilder::build() { return Block(data_, offsets_); }

size_t BlockBuilder::get_size() { return size_; }
