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
#include <algorithm>

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        // 1. 加表级S锁
        if (context_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        }

        // 获取索引句柄
        auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        auto ih = sm_manager_->ihs_.at(ix_name).get();

        // 确定扫描范围
        Iid lower = ih->leaf_begin();
        Iid upper = ih->leaf_end();

        // 我们查找 fed_conds_ 中针对索引第一列的条件
        for (auto &cond : fed_conds_) {
            if (cond.lhs_col.col_name == index_col_names_[0] && cond.is_rhs_val) {
                // 构建 Key
                char *key = new char[index_meta_.col_tot_len];
                memset(key, 0, index_meta_.col_tot_len);
                
                auto &val = cond.rhs_val;
                if (val.type == TYPE_INT) {
                    memcpy(key, &val.int_val, sizeof(int));
                } else if (val.type == TYPE_FLOAT) {
                    memcpy(key, &val.float_val, sizeof(float));
                } else if (val.type == TYPE_STRING) {
                    memcpy(key, val.str_val.c_str(), val.str_val.length());
                }

                // 根据操作符缩小范围
                if (cond.op == OP_EQ) {
                    lower = ih->lower_bound(key);
                    upper = ih->upper_bound(key);
                } else if (cond.op == OP_GE) {
                    lower = ih->lower_bound(key);
                } else if (cond.op == OP_GT) {
                    lower = ih->upper_bound(key);
                } else if (cond.op == OP_LT) {
                    upper = ih->lower_bound(key);
                } else if (cond.op == OP_LE) {
                    upper = ih->upper_bound(key);
                }
                
                delete[] key;
                break; // 只利用第一个匹配条件
            }
        }

        // 创建 IxScan 对象并赋值给父类指针 scan_
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
    }

    void nextTuple() override {
        if (scan_) {
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (!scan_) {
            beginTuple();
        }

        // 遍历 B+ 树叶子
        while (!scan_->is_end()) {
            // 获取当前索引指向的 RID
            Rid rid = scan_->rid();
            
            // 立即移动游标到下一个位置，为下一次循环或下次调用做准备
            nextTuple();

            // 回表查询：拿到 RID 后，去记录文件查数据
            auto rec = fh_->get_record(rid, context_);

            // 行内校验所有条件
            bool satisfy = true;
            for (auto &cond : fed_conds_) {
                // 获取左值数据 (LHS)
                auto lhs_col = tab_.get_col(cond.lhs_col.col_name);
                char *lhs = rec->data + lhs_col->offset;
                
                // 获取右值数据 (RHS)
                char *rhs;
                ColType rhs_type = lhs_col->type; // 默认类型相同
                if (cond.is_rhs_val) {
                    auto &val = cond.rhs_val;
                    rhs_type = val.type;
                    if (rhs_type == TYPE_INT) {
                        rhs = (char *)&val.int_val;
                    } else if (rhs_type == TYPE_FLOAT) {
                        rhs = (char *)&val.float_val;
                    } else { // STRING
                        rhs = (char *)val.str_val.c_str();
                    }
                } else {
                    auto rhs_col = tab_.get_col(cond.rhs_col.col_name);
                    rhs = rec->data + rhs_col->offset;
                    rhs_type = rhs_col->type;
                }

                // 使用全局 ix_compare 进行比较
                int cmp_res = ix_compare(lhs, rhs, lhs_col->type, lhs_col->len);
                
                // 判断条件是否满足
                bool cond_met = false;
                switch (cond.op) {
                    case OP_EQ: cond_met = (cmp_res == 0); break;
                    case OP_NE: cond_met = (cmp_res != 0); break;
                    case OP_LT: cond_met = (cmp_res < 0); break;
                    case OP_GT: cond_met = (cmp_res > 0); break;
                    case OP_LE: cond_met = (cmp_res <= 0); break;
                    case OP_GE: cond_met = (cmp_res >= 0); break;
                }

                if (!cond_met) {
                    satisfy = false;
                    break; 
                }
            }

            // 如果满足所有条件，返回记录
            if (satisfy) {
                rid_ = rid; // 更新 executor 的当前 rid
                return rec;
            }
            // 如果不满足，while 循环继续，检查下一条
        }

        // 扫描结束，没有更多数据
        return nullptr;
    }

    Rid &rid() override { return rid_; }
};