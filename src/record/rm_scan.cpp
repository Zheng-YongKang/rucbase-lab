/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）

    rid_.page_no = RM_FIRST_RECORD_PAGE;
    rid_.slot_no = -1;
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置

    if (is_end()) {
        return;
    }

    const auto &fhdr = file_handle_->file_hdr_;             // 文件头信息
    const int last_page = fhdr.num_pages;                   // 末尾判定用
    const int slots_per_page = fhdr.num_records_per_page;   // 每页槽位数

    for (int page_no = rid_.page_no; page_no < last_page; ++page_no) {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
        const int start_slot = (page_no == rid_.page_no) ? rid_.slot_no : -1;
        int slot_no = Bitmap::next_bit(true, page_handle.bitmap, slots_per_page, start_slot);
        file_handle_->buffer_pool_manager_->unpin_page(PageId{file_handle_->fd_, page_no}, false);  // 释放页面句柄
        if (slot_no < slots_per_page) { // 找到下一个有记录的slot
            rid_.page_no = page_no;
            rid_.slot_no = slot_no;
            return;
        }
    }
    
    rid_.page_no = last_page;  // 标记为无效位置
    rid_.slot_no = -1;
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    return (rid_.page_no == file_handle_->file_hdr_.num_pages) && (rid_.slot_no == -1);
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}