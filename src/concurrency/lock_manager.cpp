/**
 * lock_manager.cpp
 */

#include <algorithm>
#include <cassert>
#include "concurrency/lock_manager.h"
using namespace std;

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  return GetLock(txn, rid, LockMode::SHARED);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  return GetLock(txn, rid, LockMode::EXCLUSIVE);
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  return GetLock(txn, rid, LockMode::UPGRADE);
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  auto state = txn->GetState();
  if (strict_2PL_) {
    if (state != TransactionState::COMMITTED && state != TransactionState::ABORTED) {
      txn->SetState(TransactionState::ABORTED);
	  return false;
    }
  } else if (state == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  //1.get lock list
  unique_lock<mutex> table_latch(mutex_);  
  LockList &value = lock_table_[rid];
  unique_lock<mutex> list_latch(value.mutex_);  
  //2.find lock
  auto iter = find_if(value.list_.begin(), value.list_.end(), 
                      [txn] (const Lock &lock) { 
					    return lock.txn_id_ == txn->GetTransactionId();
					  });
  assert(iter != value.list_.end()); 
  //3.erase lock from lock list and transaction lock set.
  
  auto lock_set = iter->mode_ == LockMode::SHARED ? 
                  txn->GetSharedLockSet() : txn->GetExclusiveLockSet(); 
  assert(lock_set->erase(rid) == 1);
  value.list_.erase(iter);
  //4.bug forget erase rid if list is empty
  if (value.list_.empty()) {
    lock_table_.erase(rid);
	return true;
  }
  table_latch.unlock();
  //5.notify other passible waiting locks.
  for (auto &lock : value.list_) {
    //if have other granted lock, that's to say cur lock is shared lock
	//and its unlock is no impact on other locks.
    if (lock.is_granted_) 
	  break;
	lock.Grant();
	if (lock.mode_ == LockMode::SHARED) {
	  continue; 
    }
	if (lock.mode_ == LockMode::UPGRADE) {
	  value.is_upgrading_ = false;
	  lock.mode_ = LockMode::EXCLUSIVE;
	}
	break;
  }
  return true;
}

bool LockManager::GetLock(Transaction *txn, const RID &rid, LockMode mode) {
  //assert mode is legal
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  //get lock list
  unique_lock<mutex> table_latch(mutex_);  
  LockList &value = lock_table_[rid];
  unique_lock<mutex> list_latch(value.mutex_);  
  table_latch.unlock();
  //special deal with upgrade mode 
  if (mode == LockMode::UPGRADE) {
    if (value.is_upgrading_) {
      txn->SetState(TransactionState::ABORTED);
	  return false;
	}
    auto iter = find_if(value.list_.begin(), value.list_.end(), 
                        [txn] (const Lock &lock) { 
	  				      return lock.txn_id_ == txn->GetTransactionId();
					  });
    if (iter == value.list_.end() || iter->mode_ != LockMode::SHARED
        || !iter->is_granted_) {
      txn->SetState(TransactionState::ABORTED);
	  return false;
    }
    assert(txn->GetSharedLockSet()->erase(rid) == 1);
    value.list_.erase(iter);
  }
  //insert to lock list
  auto can_grant = value.CanGrant(mode);
  //wait-die
  if (!can_grant && value.list_.back().txn_id_ < txn->GetTransactionId()) {
     txn->SetState(TransactionState::ABORTED);
  	 return false;
  }
  value.Insert(txn, rid, mode, can_grant, &list_latch);
  return true;
}


} // namespace cmudb
