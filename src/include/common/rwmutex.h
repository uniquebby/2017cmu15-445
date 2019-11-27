/**
 * rwmutex.h
 *
 * Reader-Writer lock
 */

#pragma once

#include <climits>
#include <condition_variable>
#include <mutex>
#include <iostream>

namespace cmudb {
class RWMutex {

  typedef std::mutex mutex_t;
  typedef std::condition_variable cond_t;
  static const uint32_t max_readers_ = UINT_MAX;

public:
  RWMutex() : reader_count_(0), writer_entered_(false) {}

  ~RWMutex() { std::lock_guard<mutex_t> guard(mutex_); }

  RWMutex(const RWMutex &) = delete;
  RWMutex &operator=(const RWMutex &) = delete;

  void WLock() {
	std::cout << "WLock: ---------------: reader_count= " << reader_count_ << std::endl;
    std::unique_lock<mutex_t> lock(mutex_);
    while (writer_entered_)
      reader_.wait(lock);
    writer_entered_ = true;
    while (reader_count_ > 0)
      writer_.wait(lock);
	std::cout << "WLock: ---------------: done " << reader_count_ << std::endl;
  }

  void WUnlock() {
	std::cout << "WUnLock: ---------------: reader_count= " << reader_count_ << std::endl;
    std::lock_guard<mutex_t> guard(mutex_);
    writer_entered_ = false;
    reader_.notify_all();
	std::cout << "WUnLock: ---------------: done " << reader_count_ << std::endl;
  }

  void RLock() {
    std::unique_lock<mutex_t> lock(mutex_);
    while (writer_entered_ || reader_count_ == max_readers_)
      reader_.wait(lock);
    reader_count_++;
	std::cout << "RLock: ---------------: reader_count= " << reader_count_ << std::endl;
  }

  void RUnlock() {
    std::lock_guard<mutex_t> guard(mutex_);
    reader_count_--;
	std::cout << "RUnLock: ---------------: reader_count= " << reader_count_ << std::endl;
    if (writer_entered_) {
      if (reader_count_ == 0)
        writer_.notify_one();
    } else {
      if (reader_count_ == max_readers_ - 1)
        reader_.notify_one();
    }
  }

private:
  mutex_t mutex_;
  cond_t writer_;
  cond_t reader_;
  uint32_t reader_count_;
  bool writer_entered_;
};
} // namespace cmudb
