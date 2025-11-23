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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  // 选的列数

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();            // 子节点的所有列
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col); // 找到这一列
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;    // 拷贝一份
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);   // 加入投影后的列集合
        }
        len_ = curr_offset;
    }

    void beginTuple() override {
        // 先让子节点定位到第一条结果元组
        prev_->beginTuple();

        // 如果子节点还有数据，同步当前执行器的 rid
        if (!prev_->is_end()) {
            _abstract_rid = prev_->rid();
        }
    }

    void nextTuple() override {
        if (prev_ == nullptr) {
            return;
        }
        // 子节点移动到下一条结果元组
        prev_->nextTuple();

        // 仍然有数据的话，同步 rid
        if (!prev_->is_end()) {
            _abstract_rid = prev_->rid();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        // 没有更多输入元组了
        if (is_end()) {
            return nullptr;
        }

        // 先从子节点拿到当前那条“完整”的记录
        auto child_rec = prev_->Next();
        if (!child_rec) {
            return nullptr;
        }

        // 为投影后的记录申请空间，长度是 len_
        auto proj_rec = std::make_unique<RmRecord>(len_);

        // 子节点的列元信息
        const auto &prev_cols = prev_->cols();

        // 按 sel_idxs_ 里记录的索引，一列一列拷贝
        for (size_t i = 0; i < sel_idxs_.size(); ++i) {
            const ColMeta &src_meta = prev_cols[sel_idxs_[i]]; // 原记录中的那一列
            const ColMeta &dst_meta = cols_[i];                // 投影后记录中的对应列

            // 从原记录中把这一列拷贝到新记录中对应的位置
            memcpy(proj_rec->data + dst_meta.offset,
                   child_rec->data + src_meta.offset,
                   src_meta.len);
        }

        return proj_rec;
    }

    Rid &rid() override { return _abstract_rid; }

    bool is_end() const override { return prev_ == nullptr || prev_->is_end(); }
    
    std::string getType() override { return "ProjectionExecutor"; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }
};