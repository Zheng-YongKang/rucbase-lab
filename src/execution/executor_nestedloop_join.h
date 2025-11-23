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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;                                 // 是否扫描结束

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();    // 右儿子整体移动左儿子大小
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());    // 右儿子和左儿子字段合并
        isend = false;
        fed_conds_ = std::move(conds);

    }

    void beginTuple() override {
        isend = false;

        // 先让左、右子节点都定位到各自的第一条元组
        left_->beginTuple();
        if (left_->is_end()) {          // 左表本身就空，join 结果一定为空
            isend = true;
            return;
        }

        right_->beginTuple();
        if (right_->is_end()) {         // 右表为空，join 结果也一定为空
            isend = true;
            return;
        }

        // 寻找第一对满足 join 条件的元组
        while (!left_->is_end()) {
            while (!right_->is_end()) {
                auto lhs_rec = left_->Next();
                auto rhs_rec = right_->Next();
                if (eval_conds(lhs_rec.get(), rhs_rec.get(), fed_conds_, cols_)) {
                    // 找到一对满足条件的 (left, right)
                    _abstract_rid = left_->rid();   // 选一个 rid 作为当前rid
                    return;
                }
                right_->nextTuple();   // 右表往后走
            }
            // 当前这一行 left_ 扫完了所有 right_ 都没有匹配，换下一行left_
            left_->nextTuple();
            if (left_->is_end()) {
                break;
            }
            right_->beginTuple();      // 右表从头再扫一遍
        }

        // 所有组合都不满足 join 条件
        isend = true;
    }

    void nextTuple() override {
        if (is_end()) {
            return;
        }

        // 从当前 (left, right) 的“下一对”开始找：先把右表往后挪一条
        right_->nextTuple();

        while (!left_->is_end()) {
            while (!right_->is_end()) {
                auto lhs_rec = left_->Next();
                auto rhs_rec = right_->Next();
                if (eval_conds(lhs_rec.get(), rhs_rec.get(), fed_conds_, cols_)) {
                    _abstract_rid = left_->rid();
                    return;
                }
                right_->nextTuple();
            }
            // 当前这一行 left_ 扫完了所有 right_，换下一行 left_
            left_->nextTuple();
            if (left_->is_end()) {
                break;
            }
            right_->beginTuple();
        }

        // 已经没有更多匹配的 (left, right) 了
        isend = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }

        // 当前 left_ / right_ 所在位置的两条记录
        auto lhs_rec = left_->Next();
        auto rhs_rec = right_->Next();
        if (!lhs_rec || !rhs_rec) {
            return nullptr;
        }

        auto res = std::make_unique<RmRecord>(len_);

        // 先拷贝左表的元组
        size_t left_len = left_->tupleLen();
        memcpy(res->data, lhs_rec->data, left_len);

        // 再拷贝右表的元组，拼在后面
        size_t right_len = right_->tupleLen();
        memcpy(res->data + left_len, rhs_rec->data, right_len);

        return res;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return isend; }
    
    std::string getType() override { return "NestedLoopJoinExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    private:
    bool eval_cond(const RmRecord *lhs_rec, const RmRecord *rhs_rec, const Condition &cond, const std::vector<ColMeta> &rec_cols) {
        // 1. 取条件左边表达式的值
        const ColMeta &lhs_meta = *get_col(rec_cols, cond.lhs_col);
        size_t left_len = left_->tupleLen();

        const char *lhs_ptr = nullptr;
        if (lhs_meta.offset < left_len) {
            // 来自左表
            lhs_ptr = lhs_rec->data + lhs_meta.offset;
        } else {
            // 来自右表（offset 里已经加过 left_len，要减回来）
            lhs_ptr = rhs_rec->data + (lhs_meta.offset - left_len);
        }
        ColType lhs_type = lhs_meta.type;
        int lhs_len = lhs_meta.len;

        // 2. 取条件右边表达式的值
        const char *rhs_ptr = nullptr;
        ColType rhs_type;
        int rhs_len;

        if (cond.is_rhs_val) {  // 右边是常量
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
            if (rhs_meta.offset < left_len) {
                rhs_ptr = lhs_rec->data + rhs_meta.offset;
            } else {
                rhs_ptr = rhs_rec->data + (rhs_meta.offset - left_len);
            }
            rhs_type = rhs_meta.type;
            rhs_len = rhs_meta.len;
        }

        // 3. 做比较
        int cmp = ix_compare(lhs_ptr, rhs_ptr, lhs_type, lhs_len);

        switch (cond.op) {
            case OP_EQ: return cmp == 0;
            case OP_NE: return cmp != 0;
            case OP_LT: return cmp < 0;
            case OP_LE: return cmp <= 0;
            case OP_GT: return cmp > 0;
            case OP_GE: return cmp >= 0;
            default:    return false;
        }
    }

    bool eval_conds(const RmRecord *lhs_rec, const RmRecord *rhs_rec, const std::vector<Condition> &conds, const std::vector<ColMeta> &rec_cols) {
        return std::all_of(conds.begin(), conds.end(),
            [&](const Condition &cond) { return eval_cond(lhs_rec, rhs_rec, cond, rec_cols); }
        );
    }
};