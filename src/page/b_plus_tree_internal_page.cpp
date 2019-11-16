/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1); 
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (array[i].second == value) return i; 
  }
  return -1;
}
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { 
  assert(index >=0 && index < GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  assert(GetSize() > 1);     
  int start = 1, end = GetSize() - 1;
  while (start <= end) {
    int mid = (end - start) / 2 + start;
	if (comparator(array[mid].first, key) <= 0) 
	  start = mid + 1;
	else end = mid - 1;
  }
  return array[start-1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int index = ValueIndex(old_value) + 1;
  assert(index != -1);
  for (int i = GetSize()-1; i >= index; --i) {
    array[i+1].first = array[i].first; 
    array[i+1].second= array[i].second; 
  }
  IncreaseSize(1);
  array[index].first = new_key;
  array[index].second = new_value;
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  int size = GetSize();
  assert(size == (GetMaxSize() + 1));
  int index = (size-1)/2 + 1;
  for (int i = index; i < size; ++i) {
    recipient->array[i-index] = array[i];
	auto page =  buffer_pool_manager->FetchPage(array[i].second);
    BPlusTreePage *child_tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData()); 	
	child_tree_page->SetParentPageId(recipient->GetPageId());
	buffer_pool_manager->UnpinPage(array[i].second, true);
  }
  SetSize(index);
  //because of the first key & value copied from the old page will be 
  //no used, so the size is seted to size-index rather than size-index+1.
  recipient->SetSize(size - index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  for (int i = index+1; i < GetSize(); ++i) {
    array[i-1].first = array[i].first; 
    array[i-1].second = array[i].second; 
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto res = ValueAt(0);
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return res;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  auto recp_size = recipient->GetSize();
  auto size = GetSize();
  assert(size + recp_size <= recipient->GetMaxSize()); 	
  //a. move to recipient.(set chlidrens' parent id)
  for (int i = 0; i < size; ++i) {
    recipient->array[recp_size+i] = array[i];    
    recipient->IncreaseSize(1);
	auto page = buffer_pool_manager->FetchPage(array[i].second);
	assert(page != nullptr);
	BPlusTreePage *child_page 
	    = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child_page->SetParentPageId(recipient->GetPageId());    
	buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  //b. update parent
  buffer_pool_manager->UnpinPage(GetPageId(), true); 
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true); 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  auto recp_size = recipient->GetSize();
  auto size = GetSize();
  assert(recp_size < recipient->GetMaxSize() && size > 0);
  MappingType pair{KeyAt(0), ValueAt(0)};
  for (int i = 1; i < size; ++i) {
    array[i-1] = array[i];
  }
  IncreaseSize(-1);
  recipient->CopyLastFrom(pair, buffer_pool_manager);
  auto child_page_id = pair.second;
  auto page = buffer_pool_manager->FetchPage(child_page_id);
  assert(page != nullptr);
  BPlusTreePage *child = 
      reinterpret_cast<BPlusTreePage*>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent = 
      reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMaxSize());   	
  array[GetSize()] = pair;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  auto recp_size = recipient->GetSize();
  auto size = GetSize();
  //bug:
  //assert(size < GetMaxSize() && recp_size > 0);
  assert(recp_size < recipient->GetMaxSize() && size > 0);
  MappingType pair{KeyAt(size-1), ValueAt(size-1)};
  IncreaseSize(-1); 
  auto child_page_id = pair.second;
  //reset childrens' parent id
  auto page = buffer_pool_manager->FetchPage(child_page_id);
  assert(page != nullptr);
  BPlusTreePage *child = 
      reinterpret_cast<BPlusTreePage*>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() < GetMaxSize());	
  for (int i = GetSize(); i > 0; i--) {
    array[i] = array[i-1];
  }
  array[0] = pair;
  IncreaseSize(1);
  //reset parent key
  auto page = buffer_pool_manager->FetchPage(GetParentPageId());
  assert(page != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent = 
       reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(page->GetData());
  parent->SetKeyAt(parent_index, pair.first); 
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
