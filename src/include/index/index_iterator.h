/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(int, B_PLUS_TREE_LEAF_PAGE_TYPE*, BufferPoolManager*);
  ~IndexIterator();

  bool isEnd() {
    return item_ == nullptr;
  }

  const MappingType &operator*() {
    return item_->GetItem(index_);
  }

  IndexIterator &operator++() {
    //std::cout << "index=" << index_ << " GetSize()= " << item_->GetSize() << std::endl;
    if (index_ == item_->GetSize()-1) {
	  std::cout << "prev_page_id= " << cur_page_->GetPageId() << std::endl;
	  auto next_page_id = item_->GetNextPageId();
	  UnlockAndUnPin();
	  if (next_page_id == INVALID_PAGE_ID) 
	    item_ = nullptr;
      else {
	    cur_page_ = buffer_pool_manager_->FetchPage(next_page_id);
		cur_page_->RLatch();
		item_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(cur_page_->GetData());
		index_ = 0;
	  }
	} else 
	    ++index_;
	return *this;
  }

private:
  // add your own private member variables here
  int index_;
  Page *cur_page_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *item_;
  BufferPoolManager *buffer_pool_manager_;

  void UnlockAndUnPin() {
    cur_page_->RUnlatch();   
	buffer_pool_manager_->UnpinPage(cur_page_->GetPageId(), false);
  }
};

} // namespace cmudb
