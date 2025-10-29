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

#include "index/ix.h"
#include "record/rm_file_handle.h"
#include "sm_defs.h"
#include "sm_meta.h"
#include "common/context.h"

class Context;

struct ColDef {
    std::string name;  // Column name
    ColType type;      // Type of column
    int len;           // Length of column
};

/* 系统管理器，负责元数据管理和DDL语句的执行 */
class SmManager {
   public:
    DbMeta db_;             // 当前打开的数据库的元数据
    std::unordered_map<std::string, std::unique_ptr<RmFileHandle>> fhs_;    // file name -> record file handle, 当前数据库中每张表的数据文件
    std::unordered_map<std::string, std::unique_ptr<IxIndexHandle>> ihs_;   // file name -> index file handle, 当前数据库中每个索引的文件
   private:
    DiskManager* disk_manager_;     // 磁盘管理器
    BufferPoolManager* buffer_pool_manager_;    // 缓冲区管理器
    RmManager* rm_manager_; // 记录管理器
    IxManager* ix_manager_; // 索引管理器

   public:
    SmManager(DiskManager* disk_manager, BufferPoolManager* buffer_pool_manager, RmManager* rm_manager,
              IxManager* ix_manager)
        : disk_manager_(disk_manager),
          buffer_pool_manager_(buffer_pool_manager),
          rm_manager_(rm_manager),
          ix_manager_(ix_manager) {}

    ~SmManager() {}

    BufferPoolManager* get_bpm() { return buffer_pool_manager_; }   // 获取缓冲区管理器指针

    RmManager* get_rm_manager() { return rm_manager_; }  // 获取记录管理器指针

    IxManager* get_ix_manager() { return ix_manager_; }  // 获取索引管理器指针

    bool is_dir(const std::string& db_name);    // 判断数据库目录是否存在

    void create_db(const std::string& db_name);  // 创建数据库目录

    void drop_db(const std::string& db_name);   // 删除数据库目录

    void open_db(const std::string& db_name);   // 打开数据库目录

    void close_db();    // 关闭数据库目录

    void flush_meta();  // 将元数据写回磁盘

    void show_tables(Context* context);  // 显示当前数据库中的所有表

    void desc_table(const std::string& tab_name, Context* context); // 显示指定表的表结构

    void create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context);  // 创建表

    void drop_table(const std::string& tab_name, Context* context); // 删除表

    void create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);    // 创建索引

    void drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context);  // 删除索引
    
    void drop_index(const std::string& tab_name, const std::vector<ColMeta>& col_names, Context* context);  // 删除索引
};