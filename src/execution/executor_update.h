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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                       // 表的元数据
    std::vector<Condition> conds_;      // update的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<Rid> rids_;             // 需要更新的记录的位置
    std::string tab_name_;              // 表名
    std::vector<SetClause> set_clauses_; // 更新的字段和值
    SmManager *sm_manager_;             // 系统管理器指针

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 对 rids_ 中记录的所有元组执行 UPDATE（记录文件 + 索引文件）
        for (auto &rid : rids_) {
            // 1. 从记录文件中读出这一条旧记录
            auto rec = fh_->get_record(rid, context_);

            // 2. 按照所有 SET 子句更新记录中的对应列
            for (auto &set_clause : set_clauses_) {
                // 找到要更新的列的列信息（offset / len / type）
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                // 右值在构造 set_clauses_ 时已经 init_raw 过，这里直接用 raw->data
                memcpy(rec->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }

            // 3. 把修改后的记录写回记录文件
            fh_->update_record(rid, rec->data, context_);

        }

        // DML 的 Next 只负责“把事情干完”，返回值没人用
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};