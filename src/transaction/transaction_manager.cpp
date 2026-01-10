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
#include "common/context.h"

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

    // 清理写操作集合 (Commit 不需要回滚，只需要释放记录对象的内存)
    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        auto *wr = write_set->front();
        write_set->pop_front();
        delete wr;
    }

    // 释放所有锁 (关键：防止迭代器失效)
    auto lock_set = txn->get_lock_set();
    // 创建副本以安全遍历
    std::vector<LockDataId> locks_to_release;
    for (auto lock : *lock_set) {
        locks_to_release.push_back(lock);
    }
    // 遍历副本进行解锁
    for (auto lock_id : locks_to_release) {
        lock_manager_->unlock(txn, lock_id);
    }
    // 此时 lock_set 应该已经被 unlock 维护清空，但为了保险再次 clear
    lock_set->clear();

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
    
    if (txn == nullptr) return;

    // 回滚所有写操作 (逆序遍历)
    auto write_set = txn->get_write_set();
    Context ctx(lock_manager_, log_manager, txn);

    while (!write_set->empty()) {
        WriteRecord *wr = write_set->back();
        write_set->pop_back();

        const std::string &tab_name = wr->GetTableName();
        Rid rid = wr->GetRid();
        WType wtype = wr->GetWriteType();
        
        // 获取表文件句柄和元数据
        RmFileHandle *fh = sm_manager_->fhs_.at(tab_name).get();
        auto &tab_meta = sm_manager_->db_.get_table(tab_name);

        if (wtype == WType::INSERT_TUPLE) {
            // 遍历所有索引，删除对应的 Key
            RmRecord &new_record = wr->GetRecord(); 

            for (auto &index : tab_meta.indexes) {
                auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
                auto ih = sm_manager_->ihs_.at(ix_name).get();
                
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i) {
                    memcpy(key + offset, new_record.data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                
                ih->delete_entry(key, txn);
                delete[] key;
            }
            
            // 从表中物理删除
            fh->delete_record(rid, &ctx);

        } else if (wtype == WType::DELETE_TUPLE) {
            // 回滚删除 = 重新插入
            RmRecord &old_rec = wr->GetRecord();
            
            // A. 将数据插回表中
            Rid new_rid = fh->insert_record(old_rec.data, &ctx);

            // B. 遍历所有索引，插入 Key
            for (auto &index : tab_meta.indexes) {
                auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
                auto ih = sm_manager_->ihs_.at(ix_name).get();
                
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i) {
                    memcpy(key + offset, old_rec.data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                
                ih->insert_entry(key, new_rid, txn);
                delete[] key;
            }

        } else if (wtype == WType::UPDATE_TUPLE) {
            // 回滚更新 = 将新值改回旧值
            RmRecord &old_rec = wr->GetRecord();
            
            // 获取当前的脏数据（即事务做出的修改后的值），我们需要删掉这个新值对应的索引
            auto new_rec_ptr = fh->get_record(rid, &ctx);

            for (auto &index : tab_meta.indexes) {
                auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
                auto ih = sm_manager_->ihs_.at(ix_name).get();
                
                // A. 删除Key
                char *new_key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t i = 0; i < index.col_num; ++i) {
                    memcpy(new_key + offset, new_rec_ptr->data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                ih->delete_entry(new_key, txn);
                delete[] new_key;

                // B. 插入Key
                char *old_key = new char[index.col_tot_len];
                offset = 0;
                for (size_t i = 0; i < index.col_num; ++i) {
                    memcpy(old_key + offset, old_rec.data + index.cols[i].offset, index.cols[i].len);
                    offset += index.cols[i].len;
                }
                // Update 通常不改变 RID，所以这里沿用 rid
                ih->insert_entry(old_key, rid, txn);
                delete[] old_key;
            }

            // 将表中的数据恢复为旧值
            fh->update_record(rid, old_rec.data, &ctx);
        }
        delete wr;
    }

    // 释放所有锁
    txn->set_state(TransactionState::SHRINKING);

    auto lock_set = txn->get_lock_set();
    std::vector<LockDataId> locks_to_release;
    locks_to_release.reserve(lock_set->size());
    for (auto lock : *lock_set) {
        locks_to_release.push_back(lock);
    }
    for (auto lock_id : locks_to_release) {
        lock_manager_->unlock(txn, lock_id);
    }
    lock_set->clear();

    // 清空事务相关资源
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();

    // 刷盘
    if (log_manager != nullptr) {
        log_manager->flush_log_to_disk();
    }
    
    // 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}