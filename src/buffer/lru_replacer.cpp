/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
using namespace std;

namespace cmudb {

  template <typename T> LRUReplacer<T>::LRUReplacer() {
    head_ = make_shared<Node>();
    tail_ = make_shared<Node>();
    head_->next = tail_;
    tail_->prev = head_;
  }

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  lock_guard<mutex> lck(latch_);
  auto iter = map_.find(value);
  shared_ptr<Node> cur_node_ptr;
  //if value is already insert, just adjust its node to head.
  if (iter != map_.end()) {
    cur_node_ptr = iter->second;
    if (cur_node_ptr->prev == head_) return;

    //pick this node out.
    cur_node_ptr->prev->next = cur_node_ptr->next;
    cur_node_ptr->next->prev = cur_node_ptr->prev;
 } else { //creat a new node.
    cur_node_ptr = make_shared<Node>(value);
    map_[value] = cur_node_ptr;
  }
    //insert to head.
    cur_node_ptr->prev = head_;
    cur_node_ptr->next = head_->next;
    head_->next->prev = cur_node_ptr;
    head_->next = cur_node_ptr;
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  lock_guard<mutex> lck(latch_);
  if (map_.empty()) return false;
  //pick the tail member;
  auto cur_ptr = tail_->prev;
  tail_->prev = cur_ptr->prev;
  cur_ptr->prev->next = tail_;
  //save result
  value = cur_ptr->value;
  map_.erase(cur_ptr->value);
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  lock_guard<mutex> lck(latch_);
  auto iter = map_.find(value);
  if (iter == map_.end()) return false;
  
  auto cur_node_ptr = iter->second;
  cur_node_ptr->prev->next = cur_node_ptr->next;
  cur_node_ptr->next->prev = cur_node_ptr->prev;
  map_.erase(value);
  return true;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  lock_guard<mutex> lck(latch_);
  return map_.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
