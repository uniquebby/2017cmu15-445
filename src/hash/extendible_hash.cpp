#include <list>
#include <functional>

#include "hash/extendible_hash.h"
#include "page/page.h"

using namespace std;
namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
    :global_depth_(0),
     item_size_(0),
     bucket_nums_(1),
     size_each_bucket_(size) {
       buckets_.push_back(make_shared<bucket>(0));
     }

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return hash<K>{}(key);
}
template <typename K, typename V>
size_t ExtendibleHash<K, V>::GetIdx(const K &key) {
  return HashKey(key) & ((1 << global_depth_) - 1);
}
/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  lock_guard<mutex> lock(latch_);
  return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  if (bucket_id > static_cast<int>(buckets_.size())) return -1;
  
  lock_guard<mutex> lock(buckets_[bucket_id]->latch_);
  return buckets_[bucket_id]->local_depth_;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  lock_guard<mutex> lock(latch_);
  return bucket_nums_;
}
//std:hash 头文件
/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  auto index = GetIdx(key);
  if (index > buckets_.size()) return false;
  auto bucket_ptr = buckets_[index];
  if (!bucket_ptr) return false;

  lock_guard<mutex> lock(bucket_ptr->latch_);
  auto iter = bucket_ptr->map_.find(key);
  if (iter != bucket_ptr->map_.end()) {
    value = iter->second;
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  auto index = GetIdx(key);
  if (index > buckets_.size()) return false;
  auto bucket_ptr = buckets_[index];

  lock_guard<mutex> lock(bucket_ptr->latch_);
  auto iter = bucket_ptr->map_.find(key);
  if (iter != bucket_ptr->map_.end()) {
    bucket_ptr->map_.erase(key);
    return true;
  }
  return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  auto index = GetIdx(key); 
  auto bucket_ptr = buckets_[index];

  lock_guard<mutex> lock1(latch_);
  lock_guard<mutex> lock(bucket_ptr->latch_);
  
  //if the bucket is not full, just insert 
  if (bucket_ptr->map_.find(key) != bucket_ptr->map_.end() ||
      bucket_ptr->map_.size() < size_each_bucket_) {
    bucket_ptr->map_[key] = value;
    return;
  }
  //if the bucket is full, rebuilt it.
  while (bucket_ptr->map_.size() >= size_each_bucket_) {
      //adjust global and local depth.
      bucket_ptr->local_depth_++;
      if (bucket_ptr->local_depth_ > global_depth_) {
	    size_t size = buckets_.size();
        for (size_t i = 0; i < size; ++i) 
          buckets_.push_back(buckets_[i]);
        global_depth_++;
	  }
      //new bucket.
      auto new_bkt_ptr = make_shared<bucket>(bucket_ptr->local_depth_);
      bucket_nums_++;
      //move item
      auto mask = 1 << (bucket_ptr->local_depth_ - 1);
      auto iter = bucket_ptr->map_.begin();
      while (iter != bucket_ptr->map_.end()) {
        //if the last effective bit is 1, move it to new bucket
        if (HashKey(iter->first) & mask) { 
          new_bkt_ptr->map_.insert(*iter);
          iter = bucket_ptr->map_.erase(iter);
        } else {
          iter++;
        }
      }
      //adjust ptr begining at new added ptr(the half)
      for(size_t i = 0; i < buckets_.size(); ++i) {
        if (buckets_[i] == bucket_ptr && (i & mask)) buckets_[i] = new_bkt_ptr;
      }
      //insert item
      index = GetIdx(key); 
      bucket_ptr = buckets_[index];
      if (bucket_ptr->map_.size() < size_each_bucket_) {
        bucket_ptr->map_[key] = value;
        return;
      }
    }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
