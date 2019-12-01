/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

enum class LockMode { SHARED, EXCLUSIVE, UPGRADE }; 

class LockManager {

  struct Lock {
    Lock (txn_id_t txn_id, bool is_granted, LockMode mode) 
	    : txn_id_(txn_id), is_granted_(is_granted), mode_(mode) {}

	void Wait() {
	  std::unique_lock<std::mutex> ul(mutex_);
	  cv_.wait(ul, [this] { return this->is_granted_; } );
	}

	void Grant() {
	  std::lock_guard<std::mutex> lg(mutex_);
	  is_granted_ = true;
	  cv_.notify_one();
	}
	
	std::mutex mutex_;
	std::condition_variable cv_;
	txn_id_t txn_id_;
	bool is_granted_;
	LockMode mode_;
  };

  struct LockList {
    bool CanGrant(LockMode mode) {
	  if (list_.empty()) return true;
	  return (mode == LockMode::SHARED && 
	          list_.back().mode_ == LockMode::SHARED && 
			  list_.back().is_granted_);
	}

    void Insert(Transaction *txn, const RID &rid, LockMode mode,
  	            bool can_grant, std::unique_lock<std::mutex> *lk) {
      auto upgrading = (mode == LockMode::UPGRADE);
	  if (upgrading && can_grant) {
	    mode = LockMode::EXCLUSIVE;
	  }
	  list_.emplace_back(txn->GetTransactionId(), can_grant, mode);
	  if (!can_grant) {
	    is_upgrading_ |= upgrading;
		lk->unlock();
		list_.back().Wait();
	  }
      if (mode == LockMode::SHARED) {
	    txn->GetSharedLockSet()->insert(rid);
	  } else {
	    txn->GetExclusiveLockSet()->insert(rid);
	  }
    }

    std::mutex mutex_;
    std::list<Lock> list_;
	bool is_upgrading_ = false;
  };

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  std::mutex mutex_;
  bool strict_2PL_;
  std::unordered_map<RID, LockList> lock_table_;

  bool GetLock(Transaction *txn, const RID &rid, LockMode mode);
};

} // namespace cmudb
