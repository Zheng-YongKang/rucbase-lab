/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 申请行级S锁前，需持有表级IS锁
    lock_IS_on_table(txn, tab_fd);

    std::unique_lock<std::mutex> lk(latch_);
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &queue = lock_table_[lock_id];

    // 检查是否已持有锁
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn_id) {
            if (req.lock_mode_ == LockMode::SHARED || req.lock_mode_ == LockMode::EXLUCSIVE) {
                return true; 
            }
        }
    }

    // 定义兼容性检查 Lambda
    auto is_compatible = [](LockMode held) -> bool {
        // S 锁与 IS, S 兼容
        return held == LockMode::INTENTION_SHARED || held == LockMode::SHARED;
    };

    // No-Wait 检查冲突
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ != txn_id && req.granted_) {
            if (!is_compatible(req.lock_mode_)) {
                throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    queue.request_queue_.emplace_back(txn_id, LockMode::SHARED);
    queue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->insert(lock_id);
    
    return true;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }
    
    // 申请行级X锁前，需持有表级IX锁
    lock_IX_on_table(txn, tab_fd);

    std::unique_lock<std::mutex> lk(latch_);
    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &queue = lock_table_[lock_id];

    // 检查是否已有锁或需要升级
    auto it = queue.request_queue_.begin();
    for (; it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn_id) {
            if (it->lock_mode_ == LockMode::EXLUCSIVE) return true; // 已经是 X
            if (it->lock_mode_ == LockMode::SHARED) {
                // 升级 S -> X，要求队列中无其他事务（无论是否 granted，为了简化 No-Wait 处理，直接互斥）
                for (const auto& req : queue.request_queue_) {
                    if (req.txn_id_ != txn_id) {
                        throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
                    }
                }
                it->lock_mode_ = LockMode::EXLUCSIVE;
                return true;
            }
        }
    }

    // X 锁与任何其他锁都不兼容，队列必须没有其他事务
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ != txn_id) {
             throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
        }
    }

    queue.request_queue_.emplace_back(txn_id, LockMode::EXLUCSIVE);
    queue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->insert(lock_id);

    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::unique_lock<std::mutex> lk(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &queue = lock_table_[lock_id];

    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn_id) {
            if (req.lock_mode_ == LockMode::SHARED || 
                req.lock_mode_ == LockMode::EXLUCSIVE || 
                req.lock_mode_ == LockMode::S_IX) return true;
        }
    }

    auto is_compatible = [](LockMode held) -> bool {
        // S 锁与 IS, S 兼容
        return held == LockMode::INTENTION_SHARED || held == LockMode::SHARED;
    };

    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ != txn_id && req.granted_) {
            if (!is_compatible(req.lock_mode_)) {
                throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    queue.request_queue_.emplace_back(txn_id, LockMode::SHARED);
    queue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::unique_lock<std::mutex> lk(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &queue = lock_table_[lock_id];

    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn_id) {
             if (req.lock_mode_ == LockMode::EXLUCSIVE) return true;
        }
    }

    // X 锁互斥所有
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ != txn_id) {
             throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
        }
    }

    queue.request_queue_.emplace_back(txn_id, LockMode::EXLUCSIVE);
    queue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::unique_lock<std::mutex> lk(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &queue = lock_table_[lock_id];

    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn_id) return true; // IS 最弱，任何已有锁都包含它
    }

    // IS 只与 X 冲突
    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ != txn_id && req.granted_) {
            if (req.lock_mode_ == LockMode::EXLUCSIVE) {
                throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    queue.request_queue_.emplace_back(txn_id, LockMode::INTENTION_SHARED);
    queue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::unique_lock<std::mutex> lk(latch_);
    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t txn_id = txn->get_transaction_id();
    auto &queue = lock_table_[lock_id];

    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ == txn_id) {
            if (req.lock_mode_ == LockMode::INTENTION_EXCLUSIVE || 
                req.lock_mode_ == LockMode::EXLUCSIVE || 
                req.lock_mode_ == LockMode::S_IX) return true;
        }
    }

    auto is_compatible = [](LockMode held) -> bool {
        // IX 仅与 IS, IX 兼容
        return held == LockMode::INTENTION_SHARED || held == LockMode::INTENTION_EXCLUSIVE;
    };

    for (const auto& req : queue.request_queue_) {
        if (req.txn_id_ != txn_id && req.granted_) {
            if (!is_compatible(req.lock_mode_)) {
                throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
    }

    queue.request_queue_.emplace_back(txn_id, LockMode::INTENTION_EXCLUSIVE);
    queue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->insert(lock_id);
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lk(latch_);
    
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }

    auto it_tab = lock_table_.find(lock_data_id);
    if (it_tab == lock_table_.end()) return true; 

    auto &queue = it_tab->second;
    bool found = false;

    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            it = queue.request_queue_.erase(it);
            found = true;
        } else {
            ++it;
        }
    }

    txn->get_lock_set()->erase(lock_data_id);

    if (queue.request_queue_.empty()) {
        lock_table_.erase(it_tab);
    }
    
    return found;
}

