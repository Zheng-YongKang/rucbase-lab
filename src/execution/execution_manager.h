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

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "execution_defs.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "common/common.h"
#include "optimizer/plan.h"
#include "executor_abstract.h"
#include "transaction/transaction_manager.h"


class QlManager {   //  查询语言管理器，负责执行SQL语句
   private:
    SmManager *sm_manager_; // 系统管理器指针
    TransactionManager *txn_mgr_;   // 事务管理器指针

   public:
    QlManager(SmManager *sm_manager, TransactionManager *txn_mgr) 
        : sm_manager_(sm_manager),  txn_mgr_(txn_mgr) {}

    void run_mutli_query(std::shared_ptr<Plan> plan, Context *context);     // 主要负责执行DDL语句
    void run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context);   // 执行utility语句
    void select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,  // 执行select语句
                        Context *context);

    void run_dml(std::unique_ptr<AbstractExecutor> exec);   // 执行DML语句
};
