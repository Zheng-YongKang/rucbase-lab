/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    
    // 判断传入事务参数是否为空指针
    if (txn == nullptr) {
        // 如果为空指针，创建新事务
        txn_id_t txn_id = next_txn_id_.fetch_add(1);
        txn = new Transaction(txn_id);
    }

    // 事务开始：进入增长阶段 + 分配开始时间戳
    txn->set_state(TransactionState::GROWING);
    txn->set_start_ts(next_timestamp_.fetch_add(1));

    // 把开始事务加入到全局事务表中
    std::lock_guard<std::mutex> lk(latch_);
    txn_map[txn->get_transaction_id()] = txn;

    // 返回当前事务指针
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    if (txn == nullptr) return;

    // 进入收缩阶段（2PL：释放锁前先转 SHRINKING）
    txn->set_state(TransactionState::SHRINKING);

    // 如果存在未提交的写操作，提交所有的写操作
    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        auto *wr = write_set->front();
        write_set->pop_front();
        delete wr;
    }

    // 释放所有锁
    auto lock_set = txn->get_lock_set();
    while (!lock_set->empty()) {
        auto it = lock_set->begin();
        LockDataId lock_id = *it;
        lock_manager_->unlock(txn, lock_id);
        // unlock 可能会自己维护 txn->lock_set_，这里 erase 一次更稳（重复 erase 也没事）
        lock_set->erase(lock_id);
    }

    // 释放事务相关资源
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    // 把事务日志刷入磁盘中
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }

    // 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    
    // 空指针直接返回
    if (txn == nullptr) return;

    // 进入收缩阶段（2PL：释放锁前先 SHRINKING）
    txn->set_state(TransactionState::SHRINKING);

    // 回滚所有写操作
    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        WriteRecord *wr = write_set->back();
        write_set->pop_back();

        const std::string &tab_name = wr->GetTableName();
        Rid rid = wr->GetRid();
        WType wtype = wr->GetWriteType();

        // 拿到对应表的文件句柄
        RmFileHandle *fh = sm_manager_->fhs_.at(tab_name).get();

        if (wtype == WType::INSERT_TUPLE) {
            // 撤销插入：删除该 rid 的记录
            fh->delete_record(rid, nullptr);
        } else if (wtype == WType::DELETE_TUPLE) {
            // 撤销删除：把旧记录插回原rid位置
            RmRecord &old_rec = wr->GetRecord();
            fh->insert_record(rid, old_rec.data);
        } else if (wtype == WType::UPDATE_TUPLE) {
            // 撤销更新：用旧值覆盖回去
            RmRecord &old_rec = wr->GetRecord();
            fh->update_record(rid, old_rec.data, nullptr);
        }

        delete wr;
    }

    // 释放所有锁
    auto lock_set = txn->get_lock_set();
    while (!lock_set->empty()) {
        auto it = lock_set->begin();
        LockDataId lock_id = *it;
        lock_manager_->unlock(txn, lock_id);
        lock_set->erase(lock_id);
    }

    // 清空事务相关资源（索引页 latch / 删除页集合等）
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    // 把事务日志刷入磁盘中
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }

    // 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}