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

#include <fcntl.h>     
#include <sys/stat.h>  
#include <unistd.h>    

#include <atomic>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "errors.h"  

/**
 * @description: DiskManager的作用主要是根据上层的需要对磁盘文件进行操作
 */
class DiskManager {
   public:
    explicit DiskManager();     // explicit 构造函数：防止隐式类型转换

    ~DiskManager() = default;

    //  页面读写操作
    void write_page(int fd, page_id_t page_no, const char *offset, int num_bytes);      // num_bytes是要读写的字节数

    void read_page(int fd, page_id_t page_no, char *offset, int num_bytes);

    page_id_t allocate_page(int fd);    // 分配一个新的页号

    void deallocate_page(page_id_t page_id);    // 页号释放

    /*目录操作*/
    bool is_dir(const std::string &path);       // 判断指定路径是否为目录

    void create_dir(const std::string &path);   // 创建指定路径目录

    void destroy_dir(const std::string &path);  // 删除指定路径目录

    /*文件操作*/
    bool is_file(const std::string &path);      // 判断指定路径文件是否存在

    void create_file(const std::string &path);  // 创建指定路径文件

    void destroy_file(const std::string &path); // 删除指定路径文件

    int open_file(const std::string &path);     // 打开指定路径文件

    void close_file(int fd);    // 关闭指定路径文件

    int get_file_size(const std::string &file_name);    // 获得文件的大小

    std::string get_file_name(int fd);  // 根据文件句柄获得文件名

    int get_file_fd(const std::string &file_name);      // 获得文件名对应的文件句柄

    /*日志操作*/
    int read_log(char *log_data, int size, int offset); // 读取日志文件内容

    void write_log(char *log_data, int size);   // 写入日志文件内容

    void SetLogFd(int log_fd) { log_fd_ = log_fd; }     // 设置日志文件的文件句柄

    int GetLogFd() { return log_fd_; }  // 获得日志文件的文件句柄

    /**
     * @description: 设置文件已经分配的页面个数
     * @param {int} fd 文件对应的文件句柄
     * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
     */
    void set_fd2pageno(int fd, int start_page_no) { fd2pageno_[fd] = start_page_no; }

    /**
     * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
     * @return {page_id_t} 已分配的页面个数 
     * @param {int} fd 文件对应的句柄
     */
    page_id_t get_fd2pageno(int fd) { return fd2pageno_[fd]; }

    static constexpr int MAX_FD = 8192; // 最多支持 8192 个文件句柄

   private:
    // 文件打开列表，用于记录文件是否被打开
    // unordered_map是C++ STL中的哈希表
    std::unordered_map<std::string, int> path2fd_;  //<Page文件磁盘路径,Page fd>哈希表
    std::unordered_map<int, std::string> fd2path_;  //<Page fd,Page文件磁盘路径>哈希表

    int log_fd_ = -1;                             // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
    // atomic原子操作，保证多线程下对fd2pageno_的操作安全
    std::atomic<page_id_t> fd2pageno_[MAX_FD]{};  // 文件中已经分配的页面个数，初始值为0
};