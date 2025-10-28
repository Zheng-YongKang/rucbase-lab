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

#include "common/config.h"

/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    int fd;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    page_id_t page_no = INVALID_PAGE_ID;    //  页号，默认值为无效页号常量

    friend bool operator==(const PageId &x, const PageId &y) { return x.fd == y.fd && x.page_no == y.page_no; } //  friend是友元的意思可以访问当前类的private和protected成员
    bool operator<(const PageId& x) const { //  隐含了一个左操作数
        if(fd < x.fd) return true;
        return page_no < x.page_no;
    }

    std::string toString() {    //  把PageId格式化为字符串
        return "{fd: " + std::to_string(fd) + " page_no: " + std::to_string(page_no) + "}";     //  std::to_string把输入的数字格式化为十进制字符串
    }

    inline int64_t Get() const {    // 把PageId编码为一个int64_t整数, 用于哈希函数
        return (static_cast<int64_t>(fd << 16) | page_no);  // fd左移16位，处于高位 static_cast<T>(expr)显式类型转换运算符
    }
};

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash { // 他是一个仿函数
    size_t operator()(const PageId &x) const { return (x.fd << 16) | x.page_no; }
};

template <> //  模板特化, 专门化模板类或函数的某个版本
struct std::hash<PageId> {
    size_t operator()(const PageId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/**
 * @description: Page类声明, Page是RMDB数据块的单位、是负责数据操作Record模块的操作对象，
 * Page对象在磁盘上有文件存储, 若在Buffer中则有帧偏移, 并非特指Buffer或Disk上的数据
 */
class Page {
    friend class BufferPoolManager; //  因为BufferPoolManager需要访问Page的私有成员

   public:
    
    Page() { reset_memory(); }  //  构造 Page 对象时调用 reset_memory() 把数据缓冲区清零

    ~Page() = default;  //  默认析构函数，对象生命结束的时候调用

    PageId get_page_id() const { return id_; }

    inline char *get_data() { return data_; }

    bool is_dirty() const { return is_dirty_; }

    //  static constexpr：编译器常量
    static constexpr size_t OFFSET_PAGE_START = 0;  //  页数据的起始偏移位置
    static constexpr size_t OFFSET_LSN = 0; //  页里记录日志序列号的偏移位置
    static constexpr size_t OFFSET_PAGE_HDR = 4;    //  页头其他字段的起始位置

    inline lsn_t get_page_lsn() { return *reinterpret_cast<lsn_t *>(get_data() + OFFSET_LSN) ; }    //  reinterpret_cast是C++的强制类型转换运算符, 它可以将一个指针类型转换为另一个不相关的指针类型

    inline void set_page_lsn(lsn_t page_lsn) { memcpy(get_data() + OFFSET_LSN, &page_lsn, sizeof(lsn_t)); }

   private:
    void reset_memory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** page的唯一标识符 */
    PageId id_;

    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};

    /** 脏页判断 */
    bool is_dirty_ = false;

    /** The pin count of this page. */
    int pin_count_ = 0;
};