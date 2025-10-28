/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 初始化一个指向RmRecord的指针（赋值其内部的data和size）

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  // 获取rid.page_no对应的page handle
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page || !Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char* record_data = page_handle.get_slot(rid.slot_no);      // 获取rid.slot_no对应的slot的首地址
    std::unique_ptr<RmRecord> record = std::make_unique<RmRecord>(file_hdr_.record_size);
    record->SetData(record_data);
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, false);
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // Todo:
    // 1. 获取当前未满的page handle
    // 2. 在page handle中找到空闲slot位置
    // 3. 将buf复制到空闲slot位置
    // 4. 更新page_handle.page_hdr中的数据结构
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no

    RmPageHandle page_handle = create_page_handle();    // 获取当前未满的page handle
    // 使用位图查找空闲slot
    int slot_no = -1;
    for (int i = 0; i < file_hdr_.num_records_per_page; ++i) {
        if (!Bitmap::is_set(page_handle.bitmap, i)) {  // 位图为0，表示该槽位是空闲的
            slot_no = i;  // 找到第一个空闲的槽位
            Bitmap::set(page_handle.bitmap, i);  // 将该槽位标记为已占用
            break;
        }
    }
    char* slot_data = page_handle.get_slot(slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
    page_handle.page_hdr->num_records++;    // 更新当前页面中已经存储的记录个数
    // 如果当前页面已满，更新file_hdr_.first_free_page_no
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;  // 当前页面已满，设置为无效页
    }
    int page_no = page_handle.page->get_page_id().page_no;
    buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, true);
    return Rid{page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  // 获取rid.page_no对应的page handle
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, false);
        throw InternalError("insert_record: slot_no out of range");
    }  // 槽位合法性检验
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, false);
        throw InternalError("insert_record: target slot already occupied");
    }  // 槽位占用检验
    char* slot_data = page_handle.get_slot(rid.slot_no);        // 获取rid.slot_no对应的slot的首地址
    memcpy(slot_data, buf, file_hdr_.record_size);
    Bitmap::set(page_handle.bitmap, rid.slot_no);               // 将该槽位标记为已占用
    page_handle.page_hdr->num_records++;                        // 更新当前页面中已经存储的记录个数
    // 如果当前页面已满，更新file_hdr_.first_free_page_no
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        if (file_hdr_.first_free_page_no == rid.page_no) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;  // 当前页面已满，设置为无效页
        } else {                // 寻找前驱
            int pre_page_no = file_hdr_.first_free_page_no;
            while (pre_page_no != RM_NO_PAGE) {
                RmPageHandle pre_page_handle = fetch_page_handle(pre_page_no);
                if (pre_page_handle.page_hdr->next_free_page_no == rid.page_no) {
                    pre_page_handle.page_hdr->next_free_page_no = page_handle.page_hdr->next_free_page_no;
                    buffer_pool_manager_->unpin_page(PageId{fd_, pre_page_no}, true);
                    page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;  // 当前页面已满，设置为无效页
                    break;
                }
                int next_page_no = pre_page_handle.page_hdr->next_free_page_no;
                buffer_pool_manager_->unpin_page(PageId{fd_, pre_page_no}, false);
                pre_page_no = next_page_no;
            }
        }
    }
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新page_handle.page_hdr中的数据结构
    // 注意考虑删除一条记录后页面未满的情况，需要调用release_page_handle()
    
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  // 获取rid.page_no对应的page handle
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page || !Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char* slot_data = page_handle.get_slot(rid.slot_no);        // 获取rid.slot_no对应的slot的首地址
    memset(slot_data, 0, file_hdr_.record_size);
    Bitmap::reset(page_handle.bitmap, rid.slot_no);             // 将该槽位标记为未占用
    page_handle.page_hdr->num_records--;                        // 更新当前页面中已经存储的记录个数
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {   // 当从没有空闲空间的状态变为有空闲空间状态时
        release_page_handle(page_handle);
        return;
    }
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}


/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // Todo:
    // 1. 获取指定记录所在的page handle
    // 2. 更新记录

    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  // 获取rid.page_no对应的page handle
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page || !Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    char* slot_data = page_handle.get_slot(rid.slot_no);        // 获取rid.slot_no对应的slot的首地址
    memcpy(slot_data, buf, file_hdr_.record_size);              // 将buf中的数据复制到指定槽位
    buffer_pool_manager_->unpin_page(PageId{fd_, rid.page_no}, true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/
/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // Todo:
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    // if page_no is invalid, throw PageNotExistError exception

    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("Page number is invalid:", page_no);
    }
    PageId page_id{fd_, page_no};
    Page* page = buffer_pool_manager_->fetch_page(page_id);  // 获取页面数据
    if (page == nullptr) {
        throw InternalError("Buffer pool failed to fetch page " + std::to_string(page_no));
    }
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // Todo:
    // 1.使用缓冲池来创建一个新page
    // 2.更新page handle中的相关信息
    // 3.更新file_hdr_

    PageId new_page_id;
    new_page_id.fd = fd_;
    Page* new_page = buffer_pool_manager_->new_page(&new_page_id);  // 创建一个新页面
    if (new_page == nullptr) {
        throw InternalError("create_new_page_handle: new_page failed");
    }
    RmPageHandle page_handle(&file_hdr_, new_page);
    page_handle.page_hdr->num_records = 0;  // 新页面开始时没有记录
    memset(page_handle.bitmap, 0, file_hdr_.bitmap_size);  // 初始化位图为0，表示所有槽位均为空
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;  // 新页面的下一个空闲页面是当前文件头中的第一个空闲页面
    file_hdr_.first_free_page_no = new_page_id.page_no;  // 更新文件头中的第一个空闲页面为新页面
    file_hdr_.num_pages++;
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // Todo:
    // 1. 判断file_hdr_中是否还有空闲页
    //     1.1 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
    //     1.2 有空闲页：直接获取第一个空闲页
    // 2. 生成page handle并返回给上层

    if (file_hdr_.first_free_page_no != RM_NO_PAGE) {
        int free_page_no = file_hdr_.first_free_page_no;    // 如果有空闲页面，获取第一个空闲页面
        RmPageHandle page_handle = fetch_page_handle(free_page_no); // 从缓冲池中获取该页面
        // file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;     // 更新file_hdr_.first_free_page_no为下一个空闲页面
        // page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;  // 当前页面已被占用，设置为无效页
        return page_handle;         // 返回获取到的空闲页面句柄
    } else {
        return create_new_page_handle();  // 调用create_new_page_handle来创建新页面
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle&page_handle) {
    // Todo:
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    int page_no = page_handle.page->get_page_id().page_no;
    file_hdr_.first_free_page_no = page_no;          // 更新file_hdr_.first_free_page_no为当前页面
    // 将该页面标记为已更新，准备写回磁盘
    buffer_pool_manager_->unpin_page(PageId{fd_, page_no}, true);
}