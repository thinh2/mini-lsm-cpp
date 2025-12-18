#pragma once
#include <expected>
#include <vector>

/*
  The iterator is inspired by the RocksDB IteratorBase
  https://github.com/facebook/rocksdb/blob/main/include/rocksdb/iterator_base.h
*/

class Iterator {
public:
  virtual void next() = 0;
  virtual std::vector<std::byte> key() = 0;
  virtual std::vector<std::byte> value() = 0;
  virtual bool is_valid() = 0;
  virtual ~Iterator() {};
};
