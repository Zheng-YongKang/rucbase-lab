/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;             // 系统管理器指针

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();        // 获取表的数据文件句柄
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;      // 整条记录的大小

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        // 申请表级S锁
        if (context_ != nullptr && context_->txn_->get_txn_mode()) {
            context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        }
        
        scan_ = std::make_unique<RmScan>(fh_);     // 建立针对该表的记录扫描器

        // 从当前scan_指向的位置开始往后找，直到遇到第一条满足谓词的元组
        while (!is_end()) {
            Rid cur_rid = scan_->rid();
            auto rec = fh_->get_record(cur_rid, context_);  // 取出这一条记录的内容
            if (eval_conds(rec.get(), fed_conds_, cols_)) { // 检查是否满足所有选择条件
                rid_ = cur_rid;
                break;
            }
            scan_->next();  // 否则继续向后扫描
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        // 从当前scan_继续往后扫描
        if (is_end()) {
            return;
        }
        scan_->next();

        // 一边走一边判断条件
        while (!is_end()) {
            Rid cur_rid = scan_->rid();
            auto rec = fh_->get_record(cur_rid, context_);  // 取出这一条记录的内容
            if (eval_conds(rec.get(), fed_conds_, cols_)) { // 检查是否满足所有选择条件
                rid_ = cur_rid;
                break;
            }
            scan_->next();  // 否则继续向后扫描
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_); // 直接根据 rid_ 向表文件句柄要出这一条记录
    }

    Rid &rid() override { return rid_; }

    /**
     * @brief 判断是否没有结果了
     *
     */
    bool is_end() const override { return (scan_ == nullptr || scan_->is_end()); }
    
    std::string getType() override { return "SeqScanExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    private:
    /**
     * @brief 判断⼀个元组是否满⾜⼀个基本选择条件
     * @param rec 指向元组的指针
     * @param cond 基本选择条件
     * @param rec_cols 结果元组各列的元数据
     */
    bool eval_cond(const RmRecord *rec, const Condition &cond, const std::vector<ColMeta> &rec_cols) {
        // 取条件左边的值 (lhs)
        const ColMeta &lhs_meta = *get_col(rec_cols, cond.lhs_col);
        const char *lhs_ptr = rec->data + lhs_meta.offset;
        ColType lhs_type = lhs_meta.type;
        int lhs_len = lhs_meta.len;

        // 取条件右边的值 (rhs)
        const char *rhs_ptr = nullptr;
        ColType rhs_type;
        int rhs_len;

        if (cond.is_rhs_val) {  // 右边是一个常量
            rhs_type = cond.rhs_val.type;
            switch (rhs_type) {
                case TYPE_INT: {
                    rhs_ptr = reinterpret_cast<const char *>(&cond.rhs_val.int_val);
                    rhs_len = sizeof(int);
                    break;
                }
                case TYPE_FLOAT: {
                    rhs_ptr = reinterpret_cast<const char *>(&cond.rhs_val.float_val);
                    rhs_len = sizeof(float);
                    break;
                }
                case TYPE_STRING: {
                    rhs_ptr = cond.rhs_val.str_val.c_str();
                    rhs_len = static_cast<int>(cond.rhs_val.str_val.size());
                    break;
                }
            }
        } else {    // 右边是另一列
            const ColMeta &rhs_meta = *get_col(rec_cols, cond.rhs_col);
            rhs_ptr = rec->data + rhs_meta.offset;
            rhs_type = rhs_meta.type;
            rhs_len = rhs_meta.len;
        }

        // 做比较
        int cmp = ix_compare(lhs_ptr, rhs_ptr, lhs_type, lhs_len);

        // 根据比较符号 (cond.op) 得到布尔值
        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_LE: return cmp <= 0;
            case OP_GT: return cmp > 0;
            case OP_GE: return cmp >= 0;
            default:    return false;  // 理论上不会走到这里
        }
    }

    bool eval_conds(const RmRecord *rec, const std::vector<Condition> &conds, const std::vector<ColMeta> &rec_cols) {
        return std::all_of(conds.begin(), conds.end(),
            [&](const Condition &cond) { return eval_cond(rec, cond, rec_cols); }
        );
    }
};