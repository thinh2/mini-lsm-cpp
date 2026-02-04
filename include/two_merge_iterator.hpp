#include "iterator.hpp"
#include <memory>
#include <span>
class TwoMergeIterator : public Iterator {
public:
  TwoMergeIterator(std::unique_ptr<Iterator> left,
                   std::unique_ptr<Iterator> right);
  void next();
  std::vector<std::byte> key();
  std::vector<std::byte> value();
  bool is_valid();
  ~TwoMergeIterator() = default;

private:
  enum class ActiveSide { None, Left, Right };

  void refresh_active();
  void skip_right_duplicates(const std::vector<std::byte> &key);

  std::unique_ptr<Iterator> left_;
  std::unique_ptr<Iterator> right_;
  ActiveSide current_side_{ActiveSide::None};
};
