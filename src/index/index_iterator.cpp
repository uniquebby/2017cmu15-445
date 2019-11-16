/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(int index, 
                                  B_PLUS_TREE_LEAF_PAGE_TYPE* item,
								  BufferPoolManager *bm)
    : index_(index), item_(item), buffer_pool_manager_(bm) {
  cur_page_ = bm->FetchPage(item->GetPageId());	
  bm->UnpinPage(item->GetPageId(), false);
  cur_page_->RLatch();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (item_ != nullptr) 
    UnlockAndUnPin(); 
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
