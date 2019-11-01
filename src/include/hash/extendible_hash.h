/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>

#include <vector>
#include <map>
#include <memory>
#include <mutex>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
  struct bucket {
    bucket(int depth) : local_depth_(depth) {}
    int local_depth_;
    std::mutex latch_;
    std::map<K, V> map_;
  };
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  // add your own member variables here
  int global_depth_;
  mutable std::mutex latch_;
  size_t item_size_;
  size_t bucket_nums_;
  size_t size_each_bucket_;
  std::vector<std::shared_ptr<bucket>> buckets_;

  size_t GetIdx(const K &key);
};
} // namespace cmudb
