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
    // 多粒度：行S锁之前，必须先持有表级 IS（或更强）
    lock_IS_on_table(txn, tab_fd);
    // 2PL：事务处于收缩阶段禁止加锁
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    txn_id_t my_id = txn->get_transaction_id();

    auto compatible = [](LockMode req, LockMode held) -> bool {
        // req 与 held 是否兼容（多粒度锁相容关系）
        if (held == LockMode::EXLUCSIVE) return false;
        if (req == LockMode::EXLUCSIVE) return false;

        if (req == LockMode::INTENTION_SHARED) {
            // IS 与除 X 之外都兼容（S/IX/SIX/IS）
            return true;
        }
        if (req == LockMode::INTENTION_EXCLUSIVE) {
            // IX 仅与 IS/IX 兼容
            return held == LockMode::INTENTION_SHARED || held == LockMode::INTENTION_EXCLUSIVE;
        }
        if (req == LockMode::SHARED) {
            // S 与 IS/S 兼容
            return held == LockMode::INTENTION_SHARED || held == LockMode::SHARED;
        }
        if (req == LockMode::S_IX) {
            // SIX 仅与 IS 兼容
            return held == LockMode::INTENTION_SHARED;
        }
        return false;
    };

    auto subsumes = [](LockMode held, LockMode req) -> bool {
        // held 是否“强于/等于” req（避免重复加锁）
        if (held == LockMode::EXLUCSIVE) return true;
        if (held == req) return true;
        if (held == LockMode::SHARED) return req == LockMode::SHARED;
        return false;
    };

    std::unique_lock<std::mutex> lk(latch_);
    auto &queue = lock_table_[lock_id];

    // 已经持有足够强的锁：直接返回
    for (auto &r : queue.request_queue_) {
        if (r.txn_id_ == my_id && r.granted_ && subsumes(r.lock_mode_, LockMode::SHARED)) {
            txn->get_lock_set()->insert(lock_id);
            return true;
        }
    }

    // 入队申请
    queue.request_queue_.emplace_back(my_id, LockMode::SHARED);
    auto it = std::prev(queue.request_queue_.end());

    auto can_grant_now = [&]() -> bool {
        // FIFO：在我之前的未授予请求，会阻塞我
        for (auto iter = queue.request_queue_.begin(); iter != it; ++iter) {
            if (!iter->granted_) return false;
        }
        // 与当前已授予锁兼容
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue; // 同一事务不互相冲突
            if (!compatible(LockMode::SHARED, r.lock_mode_)) return false;
        }
        return true;
    };

    while (!can_grant_now()) {
        // wait-die：若与已授予锁冲突且我更“年轻”，则回滚避免死锁
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::SHARED, r.lock_mode_)) {
                if (my_id > r.txn_id_) {
                    queue.request_queue_.erase(it);
                    throw TransactionAbortException(my_id, AbortReason::DEADLOCK_PREVENTION);
                }
            }
        }
        queue.cv_.wait(lk);
        if (txn->get_state() == TransactionState::SHRINKING) {
            queue.request_queue_.erase(it);
            throw TransactionAbortException(my_id, AbortReason::LOCK_ON_SHIRINKING);
        }
    }

    it->granted_ = true;
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
    // 多粒度：行X锁之前，必须先持有表级 IX（或更强）
    lock_IX_on_table(txn, tab_fd);
    // 2PL：事务处于收缩阶段禁止加锁
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    LockDataId lock_id(tab_fd, rid, LockDataType::RECORD);
    txn_id_t my_id = txn->get_transaction_id();

    auto compatible = [](LockMode req, LockMode held) -> bool {
        if (held == LockMode::EXLUCSIVE) return false;
        if (req == LockMode::EXLUCSIVE) return false;

        if (req == LockMode::INTENTION_SHARED) return true;
        if (req == LockMode::INTENTION_EXCLUSIVE)
            return held == LockMode::INTENTION_SHARED || held == LockMode::INTENTION_EXCLUSIVE;
        if (req == LockMode::SHARED)
            return held == LockMode::INTENTION_SHARED || held == LockMode::SHARED;
        if (req == LockMode::S_IX)
            return held == LockMode::INTENTION_SHARED;
        return false;
    };

    auto subsumes = [](LockMode held, LockMode req) -> bool {
        if (held == LockMode::EXLUCSIVE) return true;
        if (held == req) return true;
        if (held == LockMode::SHARED) return req == LockMode::SHARED;
        return false;
    };

    std::unique_lock<std::mutex> lk(latch_);
    auto &queue = lock_table_[lock_id];

    // 如果已经持有 X：直接返回
    for (auto &r : queue.request_queue_) {
        if (r.txn_id_ == my_id && r.granted_ && subsumes(r.lock_mode_, LockMode::EXLUCSIVE)) {
            txn->get_lock_set()->insert(lock_id);
            return true;
        }
    }

    // 简化升级：若已持有 S 且当前无其他授予者，则直接升级为 X
    for (auto it0 = queue.request_queue_.begin(); it0 != queue.request_queue_.end(); ++it0) {
        if (it0->txn_id_ == my_id && it0->granted_ && it0->lock_mode_ == LockMode::SHARED) {
            bool others_hold = false;
            for (auto &r : queue.request_queue_) {
                if (!r.granted_) continue;
                if (r.txn_id_ == my_id) continue;
                others_hold = true; break;
            }
            if (!others_hold) {
                it0->lock_mode_ = LockMode::EXLUCSIVE;
                txn->get_lock_set()->insert(lock_id);
                return true;
            }
            // 有其他授予者：用 wait-die 处理（年轻者直接回滚）
            break;
        }
    }

    // 入队申请 X
    queue.request_queue_.emplace_back(my_id, LockMode::EXLUCSIVE);
    auto it = std::prev(queue.request_queue_.end());

    auto can_grant_now = [&]() -> bool {
        for (auto iter = queue.request_queue_.begin(); iter != it; ++iter) {
            if (!iter->granted_) return false;
        }
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            // X 与任何已授予锁都不兼容
            return false;
        }
        return true;
    };

    while (!can_grant_now()) {
        // wait-die：与任意已授予锁冲突（必然冲突），年轻者 die
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (my_id > r.txn_id_) {
                queue.request_queue_.erase(it);
                throw TransactionAbortException(my_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
        queue.cv_.wait(lk);
        if (txn->get_state() == TransactionState::SHRINKING) {
            queue.request_queue_.erase(it);
            throw TransactionAbortException(my_id, AbortReason::LOCK_ON_SHIRINKING);
        }
    }

    it->granted_ = true;
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

    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t my_id = txn->get_transaction_id();

    auto compatible = [](LockMode req, LockMode held) -> bool {
        if (held == LockMode::EXLUCSIVE) return false;
        if (req == LockMode::EXLUCSIVE) return false;

        if (req == LockMode::INTENTION_SHARED) return true;
        if (req == LockMode::INTENTION_EXCLUSIVE)
            return held == LockMode::INTENTION_SHARED || held == LockMode::INTENTION_EXCLUSIVE;
        if (req == LockMode::SHARED)
            return held == LockMode::INTENTION_SHARED || held == LockMode::SHARED;
        if (req == LockMode::S_IX)
            return held == LockMode::INTENTION_SHARED;
        return false;
    };

    auto subsumes = [](LockMode held, LockMode req) -> bool {
        if (held == LockMode::EXLUCSIVE) return true;
        if (held == LockMode::S_IX) return req != LockMode::EXLUCSIVE; // SIX 覆盖 IS/IX/S
        if (held == req) return true;
        if (held == LockMode::SHARED) return req == LockMode::SHARED || req == LockMode::INTENTION_SHARED;
        if (held == LockMode::INTENTION_EXCLUSIVE) return req == LockMode::INTENTION_EXCLUSIVE || req == LockMode::INTENTION_SHARED;
        if (held == LockMode::INTENTION_SHARED) return req == LockMode::INTENTION_SHARED;
        return false;
    };

    std::unique_lock<std::mutex> lk(latch_);
    auto &queue = lock_table_[lock_id];

    for (auto &r : queue.request_queue_) {
        if (r.txn_id_ == my_id && r.granted_ && subsumes(r.lock_mode_, LockMode::SHARED)) {
            txn->get_lock_set()->insert(lock_id);
            return true;
        }
    }

    queue.request_queue_.emplace_back(my_id, LockMode::SHARED);
    auto it = std::prev(queue.request_queue_.end());

    auto can_grant_now = [&]() -> bool {
        for (auto iter = queue.request_queue_.begin(); iter != it; ++iter) {
            if (!iter->granted_) return false;
        }
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::SHARED, r.lock_mode_)) return false;
        }
        return true;
    };

    while (!can_grant_now()) {
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::SHARED, r.lock_mode_) && my_id > r.txn_id_) {
                queue.request_queue_.erase(it);
                throw TransactionAbortException(my_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
        queue.cv_.wait(lk);
        if (txn->get_state() == TransactionState::SHRINKING) {
            queue.request_queue_.erase(it);
            throw TransactionAbortException(my_id, AbortReason::LOCK_ON_SHIRINKING);
        }
    }

    it->granted_ = true;
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

    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t my_id = txn->get_transaction_id();

    auto subsumes = [](LockMode held, LockMode req) -> bool {
        if (held == LockMode::EXLUCSIVE) return true;
        return held == req;
    };

    std::unique_lock<std::mutex> lk(latch_);
    auto &queue = lock_table_[lock_id];

    for (auto &r : queue.request_queue_) {
        if (r.txn_id_ == my_id && r.granted_ && subsumes(r.lock_mode_, LockMode::EXLUCSIVE)) {
            txn->get_lock_set()->insert(lock_id);
            return true;
        }
    }

    queue.request_queue_.emplace_back(my_id, LockMode::EXLUCSIVE);
    auto it = std::prev(queue.request_queue_.end());

    auto can_grant_now = [&]() -> bool {
        for (auto iter = queue.request_queue_.begin(); iter != it; ++iter) {
            if (!iter->granted_) return false;
        }
        // X 只能在没有其他授予锁时授予
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            return false;
        }
        return true;
    };

    while (!can_grant_now()) {
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (my_id > r.txn_id_) {
                queue.request_queue_.erase(it);
                throw TransactionAbortException(my_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
        queue.cv_.wait(lk);
        if (txn->get_state() == TransactionState::SHRINKING) {
            queue.request_queue_.erase(it);
            throw TransactionAbortException(my_id, AbortReason::LOCK_ON_SHIRINKING);
        }
    }

    it->granted_ = true;
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

    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t my_id = txn->get_transaction_id();

    auto compatible = [](LockMode req, LockMode held) -> bool {
        if (held == LockMode::EXLUCSIVE) return false;
        if (req == LockMode::EXLUCSIVE) return false;
        // IS 与除 X 之外都兼容
        if (req == LockMode::INTENTION_SHARED) return true;
        // 其余情况用不到
        return true;
    };

    auto subsumes = [](LockMode held, LockMode req) -> bool {
        if (held == LockMode::EXLUCSIVE) return true;
        if (held == LockMode::S_IX) return true;  // SIX 覆盖 IS
        if (held == LockMode::SHARED) return true; // S 覆盖 IS
        if (held == LockMode::INTENTION_EXCLUSIVE) return true; // IX 覆盖 IS
        return held == LockMode::INTENTION_SHARED;
    };

    std::unique_lock<std::mutex> lk(latch_);
    auto &queue = lock_table_[lock_id];

    for (auto &r : queue.request_queue_) {
        if (r.txn_id_ == my_id && r.granted_ && subsumes(r.lock_mode_, LockMode::INTENTION_SHARED)) {
            txn->get_lock_set()->insert(lock_id);
            return true;
        }
    }

    queue.request_queue_.emplace_back(my_id, LockMode::INTENTION_SHARED);
    auto it = std::prev(queue.request_queue_.end());

    auto can_grant_now = [&]() -> bool {
        for (auto iter = queue.request_queue_.begin(); iter != it; ++iter) {
            if (!iter->granted_) return false;
        }
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::INTENTION_SHARED, r.lock_mode_)) return false;
        }
        return true;
    };

    while (!can_grant_now()) {
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::INTENTION_SHARED, r.lock_mode_) && my_id > r.txn_id_) {
                queue.request_queue_.erase(it);
                throw TransactionAbortException(my_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
        queue.cv_.wait(lk);
        if (txn->get_state() == TransactionState::SHRINKING) {
            queue.request_queue_.erase(it);
            throw TransactionAbortException(my_id, AbortReason::LOCK_ON_SHIRINKING);
        }
    }

    it->granted_ = true;
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

    LockDataId lock_id(tab_fd, LockDataType::TABLE);
    txn_id_t my_id = txn->get_transaction_id();

    auto compatible = [](LockMode req, LockMode held) -> bool {
        if (held == LockMode::EXLUCSIVE) return false;
        if (req == LockMode::EXLUCSIVE) return false;

        // IX 仅与 IS/IX 兼容
        if (req == LockMode::INTENTION_EXCLUSIVE) {
            return held == LockMode::INTENTION_SHARED || held == LockMode::INTENTION_EXCLUSIVE;
        }
        return false;
    };

    auto subsumes = [](LockMode held, LockMode req) -> bool {
        if (held == LockMode::EXLUCSIVE) return true;
        if (held == LockMode::S_IX) return true; // SIX 覆盖 IX
        if (held == LockMode::INTENTION_EXCLUSIVE) return true;
        return false;
    };

    std::unique_lock<std::mutex> lk(latch_);
    auto &queue = lock_table_[lock_id];

    for (auto &r : queue.request_queue_) {
        if (r.txn_id_ == my_id && r.granted_ && subsumes(r.lock_mode_, LockMode::INTENTION_EXCLUSIVE)) {
            txn->get_lock_set()->insert(lock_id);
            return true;
        }
    }

    queue.request_queue_.emplace_back(my_id, LockMode::INTENTION_EXCLUSIVE);
    auto it = std::prev(queue.request_queue_.end());

    auto can_grant_now = [&]() -> bool {
        for (auto iter = queue.request_queue_.begin(); iter != it; ++iter) {
            if (!iter->granted_) return false;
        }
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::INTENTION_EXCLUSIVE, r.lock_mode_)) return false;
        }
        return true;
    };

    while (!can_grant_now()) {
        for (auto &r : queue.request_queue_) {
            if (!r.granted_) continue;
            if (r.txn_id_ == my_id) continue;
            if (!compatible(LockMode::INTENTION_EXCLUSIVE, r.lock_mode_) && my_id > r.txn_id_) {
                queue.request_queue_.erase(it);
                throw TransactionAbortException(my_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
        queue.cv_.wait(lk);
        if (txn->get_state() == TransactionState::SHRINKING) {
            queue.request_queue_.erase(it);
            throw TransactionAbortException(my_id, AbortReason::LOCK_ON_SHIRINKING);
        }
    }

    it->granted_ = true;
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
    if (txn == nullptr) return false;

    // 2PL：第一次解锁开始进入 SHRINKING（之后再加锁必须 abort）
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }

    txn_id_t my_id = txn->get_transaction_id();
    std::unique_lock<std::mutex> lk(latch_);

    auto it_tab = lock_table_.find(lock_data_id);
    if (it_tab == lock_table_.end()) return false;

    auto &queue = it_tab->second;

    bool erased = false;
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ) {
        if (it->txn_id_ == my_id) {
            it = queue.request_queue_.erase(it);
            erased = true;
        } else {
            ++it;
        }
    }
    if (!erased) return false;

    // 从事务锁集删除
    txn->get_lock_set()->erase(lock_data_id);

    // 队列空则可删除锁表项
    if (queue.request_queue_.empty()) {
        lock_table_.erase(it_tab);
        return true;
    }

    // 唤醒等待者重新竞争
    queue.cv_.notify_all();
    return true;
}