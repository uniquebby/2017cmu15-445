#include "buffer/buffer_pool_manager.h"
using namespace std;

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) { 
  lock_guard<mutex> lck(latch_);
  Page *res;
  if (page_table_->Find(page_id, res)) {
    res->WLatch();
    res->pin_count_++;
    res->WUnlatch();
    replacer_->Erase(res);
//    std::cout << "FetchPage: page_id=" << res->GetPageId() 
//              << " pin_count= " << res->pin_count_ << std::endl;
    return res;
  }
  if (!free_list_->empty()) {
    res = free_list_->front(); 
    free_list_->pop_front();
  } 
  else if (!replacer_->Victim(res)) {
    std::cout << "victim: all page is pined" << std::endl;
	assert(false);
    return nullptr; 
  }
  else {
//    std::cout << "victim size  is +++++++++++++++++++++++++++++" << replacer_->Size() << std::endl;
//    std::cout << "victim page id is +++++++++++++++++++++++++++++" << res->GetPageId() << std::endl;
    page_table_->Remove(res->page_id_);
    if (res->is_dirty_) 
      disk_manager_->WritePage(res->page_id_, res->data_);
  }  
  res->WLatch();
  res->pin_count_ = 1;
  res->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, res->data_);
//  std::cout << "read page id" << std::endl;
  res->WUnlatch();
  page_table_->Insert(page_id, res);

//  std::cout << "FetchPage: page_id=" << res->GetPageId() 
 //           << " pin_count= " << res->pin_count_ << std::endl;
  return res;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  lock_guard<mutex> lck(latch_);
  Page *p;
  if(page_table_->Find(page_id, p)) {
    auto pin_count = p->pin_count_;
//    std::cout << "before UnpinPage : " << "page_id = " 
//	          << page_id << " id_dirty= "
 //             << is_dirty << " pin_count=" << pin_count 
//			  << " p->is_dirty_=" << p->is_dirty_ << std::endl;
    if (pin_count > 0) {
      p->WLatch();
      --p->pin_count_; 
	  if (is_dirty)
        p->is_dirty_ = is_dirty;
      p->WUnlatch();

      p->RLatch();
      pin_count = p->pin_count_;
      p->RUnlatch();
 //     std::cout << "aftre UnpinPage : " << "page_id = " 
//	            << page_id << " id_dirty= "
 //               << is_dirty << " pin_count=" << pin_count 
//			    << " p->is_dirty_=" << p->is_dirty_ << std::endl;
      if (pin_count <= 0) {
	    replacer_->Insert(p);
	  }
      return true;
    }
    return false;
  }
  return false;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { 
  lock_guard<mutex> lck(latch_);
  Page *p;
  if (!page_table_->Find(page_id, p)) return false;
  if (page_id == INVALID_PAGE_ID) return false; 
  disk_manager_->WritePage(page_id, p->data_);
  return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { 
  lock_guard<mutex> lck(latch_);
  Page *p;
  if (page_table_->Find(page_id, p)) {
    p->RLatch();
    auto pin_count = p->pin_count_;
    p->RUnlatch();
    if (pin_count != 0) return false;

    page_table_->Remove(page_id);
	//bug: forget to erase page from lru replacer.
	replacer_->Erase(p);
    p->WLatch();
    p->pin_count_ = 0;
    p->is_dirty_ = false;
    p->WUnlatch();
    free_list_->push_back(p);
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  return false;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) { 
  lock_guard<mutex> lck(latch_);
  Page *p;
  page_id = disk_manager_->AllocatePage(); 
  if (!free_list_->empty()) {
    p = free_list_->front();
    free_list_->pop_front();
  } else {
//    std::cout << "victim size  is +++++++++++++++++++++++++++++" << replacer_->Size() << std::endl;
    if (!replacer_->Victim(p)) {
	  assert(false);
	  return nullptr;
	}
 //   std::cout << "victim page id is +++++++++++++++++++++++++++++" << p->GetPageId() << std::endl;
    page_table_->Remove(p->page_id_);
	if (p->is_dirty_) 
      disk_manager_->WritePage(p->page_id_, p->data_);
  }
  p->WLatch();
  p->page_id_ = page_id;
  p->pin_count_++;
  p->WUnlatch();
  //zero out memory.
  p->ResetMemory();
  //insert to hash table.
  page_table_->Insert(page_id, p);
  return p;
}
} // namespace cmudb
