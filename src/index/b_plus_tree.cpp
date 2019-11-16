/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  auto leaf_page_ptr = FindLeafPage(key);  
//  std::cout << "GetValue: page_id=" << leaf_page_ptr->GetPageId() << std::endl;
  if (leaf_page_ptr == nullptr) return false;
  result.resize(1);
  auto res = leaf_page_ptr->Lookup(key, result[0], comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
	return true;
  }
  bool res = InsertIntoLeaf(key, value, transaction);   
  return res;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  auto page_ptr = buffer_pool_manager_->NewPage(page_id);
  assert(page_ptr != nullptr); 
  //cast new page to leaf_page.
  B_PLUS_TREE_LEAF_PAGE_TYPE *root = 
    reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());
  //bug: forget to init it.
  root->Init(page_id, INVALID_PAGE_ID);
  //update root_page_id_.
  root_page_id_ = page_id;
  //insert entry.
  root->Insert(key, value, comparator_);

  //unpin page
  buffer_pool_manager_->UnpinPage(page_id, true);
}
/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  auto leaf_page_ptr = FindLeafPage(key);
  if (leaf_page_ptr == nullptr) return false;
  ValueType v;
  auto is_already_exist = leaf_page_ptr->Lookup(key, v, comparator_);
  if (is_already_exist) {
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
	return false;
  }
  leaf_page_ptr->Insert(key, value, comparator_);
  if (leaf_page_ptr->GetSize() > leaf_page_ptr->GetMaxSize()) {
    auto new_leaf_page_ptr = Split(leaf_page_ptr);
	InsertIntoParent(leaf_page_ptr, new_leaf_page_ptr->KeyAt(0), 
	                 new_leaf_page_ptr, transaction);
    buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) { 
  //1. ask for new page and cast to N
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);
  assert(new_page != nullptr);
  N *new_pageN = reinterpret_cast<N*>(new_page);
  //2. mova half to newly page
  new_pageN->Init(new_page_id, node->GetParentPageId());
  node->MoveHalfTo(new_pageN, buffer_pool_manager_);
  //3. return 
  return new_pageN;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {

  /* insert into new root */
  if (old_node->IsRootPage()) {
//    std::cout << "split root..................................." << std::endl;
    //1. ask new page and cast to internal page
    auto page_ptr = buffer_pool_manager_->NewPage(root_page_id_);
	assert(page_ptr != nullptr);
    B_PLUS_TREE_INTERNAL_PAGE *new_root = 
	  reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(page_ptr->GetData());
    //2. init new root
	new_root->Init(root_page_id_, INVALID_PAGE_ID);
	//bug forget to update childrens parent id.
	old_node->SetParentPageId(root_page_id_);
	new_node->SetParentPageId(root_page_id_);
    //3. populate new root
	new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
	//4. unpin new root
	buffer_pool_manager_->UnpinPage(root_page_id_, true);
	return;
  }
  /* insert into exist parent */
  //1. fetch exist parent and cast to internal page
  auto page_ptr = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  assert(page_ptr != nullptr);
  B_PLUS_TREE_INTERNAL_PAGE *parent = 
    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(page_ptr->GetData());
  //2. insert into parent
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  //3. if parent is full, then split it recursively
  if (parent->GetSize() > parent->GetMaxSize()) {
    auto new_parent = Split(parent);
  //4. insert parent into parent's parent
	InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
    buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
  }
  //5. unpin all page
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  auto leaf_page_ptr = FindLeafPage(key);
  if (leaf_page_ptr == nullptr) return;
//  std::cout << "before remove: " <<  leaf_page_ptr->ToString(true) << std::endl;
  int size = leaf_page_ptr->RemoveAndDeleteRecord(key, comparator_);
  if (size < leaf_page_ptr->GetMinSize()) 
    CoalesceOrRedistribute(leaf_page_ptr, transaction);
//  std::cout << "after remove: " <<  leaf_page_ptr->ToString(true) << std::endl;
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // a. get sibling node
  //case1: node is root page
  if (node->IsRootPage())
    return AdjustRoot(node); 
  auto page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  B_PLUS_TREE_INTERNAL_PAGE *parent = 
    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(page->GetData());
  auto index = parent->ValueIndex(node->GetPageId());
  N *sibling_node;
  //case2: node is the first node(index == 0)    
  if (index == 0) {
    // get right sibling
	auto right_sibling_page = 
	  buffer_pool_manager_->FetchPage(parent->ValueAt(index+1));
    sibling_node = reinterpret_cast<N*>(right_sibling_page->GetData());
  }
  //case3: node is not the last node and have left sibling.
  else {
    // get left sibling 
	auto left_sibling_page =
	  buffer_pool_manager_->FetchPage(parent->ValueAt(index-1));
    sibling_node = reinterpret_cast<N*>(left_sibling_page->GetData());
  }
  
  // b.judge whether to coalesce or redistribute
  bool res;
  if (sibling_node->GetSize() > sibling_node->GetMinSize()) {//redistribute
    Redistribute(sibling_node, node, index);  
	res = false;
  } else {//coalesce
    Coalesce(sibling_node, node, parent, index, transaction);  
	res = true;
  }
  // c. unpin all pages
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
  return res;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  N *left_node, *right_node;
  if (index != 0) {
    left_node = neighbor_node;
	right_node = node;
  } else {
    left_node = node;
	right_node = neighbor_node;
  }

  //move right node all to left node and remove right node
  right_node->MoveAllTo(left_node, index, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(right_node->GetPageId(), false);
  buffer_pool_manager_->DeletePage(right_node->GetPageId());
  //bug: forget the following one row code
  index = index ? index : 1;
  parent->Remove(index);
  //deal with coalesce or redistribute recursively
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index != 0) { 
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_); 
  } else {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  buffer_pool_manager_->DeletePage(root_page_id_);
  if (!old_root_node->IsLeafPage()) {
	assert(old_root_node->GetSize() == 1);
    //case 1
	B_PLUS_TREE_INTERNAL_PAGE *root = 
	    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(old_root_node);
    root_page_id_ = root->RemoveAndReturnOnlyChild();     
    UpdateRootPageId();
	auto page = buffer_pool_manager_->FetchPage(root_page_id_);
	assert(page != nullptr);
	B_PLUS_TREE_INTERNAL_PAGE *new_root = 
	  reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE*>(page->GetData());
	new_root->SetParentPageId(INVALID_PAGE_ID);
	buffer_pool_manager_->UnpinPage(root_page_id_, true);
	buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
	buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
	return true;
  } else {
	//case 2
	assert(old_root_node->GetSize() == 0);
	buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
	buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
	root_page_id_ = INVALID_PAGE_ID;   
	UpdateRootPageId();
	return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key; 
  return INDEXITERATOR_TYPE(0, FindLeafPage(key, true), buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf_page = FindLeafPage(key, false);
  return INDEXITERATOR_TYPE(leaf_page->KeyIndex(key, comparator_), 
	  leaf_page, 
							buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
	bool leftMost) {
  if (IsEmpty()) return nullptr;
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  assert(page != nullptr);
  BPlusTreePage *cur_page = 
	reinterpret_cast<BPlusTreePage*>(page->GetData());
  for (auto cur_page_id = root_page_id_; !cur_page->IsLeafPage();) {
	B_PLUS_TREE_INTERNAL_PAGE *cur_internal_page = 
	  static_cast<B_PLUS_TREE_INTERNAL_PAGE*>(cur_page);
	    std::cout << "FindLeafPage: " <<  cur_internal_page->ToString(true) << std::endl;
	if (leftMost) 
	  cur_page_id = cur_internal_page->ValueAt(0);
	else 
	  cur_page_id = cur_internal_page->Lookup(key, comparator_);
	page = buffer_pool_manager_->FetchPage(cur_page_id);	
	buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
	cur_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
	//     std::cout << "FindLeafPage: cur_page->GetPageId()= " << cur_page->GetPageId() << std::endl;
	//	std::cout << "FindLeafPage: cur_page_id=" << cur_page_id << std::endl;
  }
  auto res = static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(cur_page);
  std::cout << "FindLeafPage: " <<  res->ToString(true) << std::endl;
  return res;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
	  buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
	// create a new record<index_name + root_page_id> in header_page
	header_page->InsertRecord(index_name_, root_page_id_);
  else
	// update root_page_id in header_page
	header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
	Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
	input >> key;

    KeyType index_key;
	index_key.SetFromInteger(key);
	RID rid(key);
	Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
	Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
	input >> key;
	KeyType index_key;
	index_key.SetFromInteger(key);
	Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
