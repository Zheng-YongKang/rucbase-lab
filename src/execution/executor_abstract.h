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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {    // 抽象执行器类
   public:
    Rid _abstract_rid;      // 当前记录的rid

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };  // 每条记录的长度

    virtual const std::vector<ColMeta> &cols() const {  // 返回记录的字段信息
        std::vector<ColMeta> *_cols = nullptr;
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };   // 返回执行器类型

    virtual void beginTuple(){};    // 初始化，开始扫描第一条记录

    virtual void nextTuple(){};     // 迭代到下一条记录

    virtual bool is_end() const { return true; };   // 判断是否扫描结束

    virtual Rid &rid() = 0;   // 返回当前记录的rid

    virtual std::unique_ptr<RmRecord> Next() = 0;   // 返回下一条记录

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};  // 返回字段的偏移量

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {  // 返回字段的迭代器
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }
};