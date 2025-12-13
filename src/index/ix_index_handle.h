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

#include "ix_defs.h"
#include "transaction/transaction.h"

enum class Operation { FIND = 0, INSERT, DELETE };  // 三种操作：查找、插入、删除

static const bool binary_search = false;    // 是否使用二分查找，false表示使用顺序查找

// 比较两个键值的大小，a<b返回负数，a==b返回0，a>b返回正数
inline int ix_compare(const char *a, const char *b, ColType type, int col_len) {
    switch (type) {
        case TYPE_INT: {
            int ia = *(int *)a;
            int ib = *(int *)b;
            return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
        }
        case TYPE_FLOAT: {
            float fa = *(float *)a;
            float fb = *(float *)b;
            return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
        }
        case TYPE_STRING:
            return memcmp(a, b, col_len);
        default:
            throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char* a, const char* b, const std::vector<ColType>& col_types, const std::vector<int>& col_lens) {
    int offset = 0;
    for(size_t i = 0; i < col_types.size(); ++i) {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if(res != 0) return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
class IxNodeHandle {
    friend class IxIndexHandle;     // B+树
    friend class IxScan;            // B+树的扫描器

   private:
    const IxFileHdr *file_hdr;      // 节点所在文件的头部信息
    Page *page;                     // 存储节点的页面
    IxPageHdr *page_hdr;            // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                     // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                      // page->data的第三部分，指针指向首地址

   public:
    IxNodeHandle() = default;

    IxNodeHandle(const IxFileHdr *file_hdr_, Page *page_) : file_hdr(file_hdr_), page(page_) {
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    int get_size() { return page_hdr->num_key; }    // 获取当前节点中已插入的键值对数量

    void set_size(int size) { page_hdr->num_key = size; }   // 设置当前节点中已插入的键值对数量

    int get_max_size() { return file_hdr->btree_order_ + 1; }   // 获取当前节点中最多键值对数量

    int get_min_size() { return get_max_size() / 2; }   // 获取当前节点中最少键值对数量

    int key_at(int i) { return *(int *)get_key(i); }    // 获取第i个键值

    /* 得到第i个孩子结点的page_no */
    page_id_t value_at(int i) { return get_rid(i)->page_no; }   // 获取第i个键值对应的Rid的page_no

    page_id_t get_page_no() { return page->get_page_id().page_no; } // 获取当前节点所在页面的页号

    PageId get_page_id() { return page->get_page_id(); }    // 获取当前节点所在页面的PageId

    page_id_t get_next_leaf() { return page_hdr->next_leaf; }   // 获取当前叶子节点的下一个叶子节点的页号

    page_id_t get_prev_leaf() { return page_hdr->prev_leaf; }   // 获取当前叶子节点的上一个叶子节点的页号

    page_id_t get_parent_page_no() { return page_hdr->parent; } // 获取当前节点的父节点的页号

    bool is_leaf_page() { return page_hdr->is_leaf; }   // 判断当前节点是否为叶子节点

    bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; } // 判断当前节点是否为根节点

    void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }    // 设置当前叶子节点的下一个叶子节点的页号

    void set_prev_leaf(page_id_t page_no) { page_hdr->prev_leaf = page_no; }    // 设置当前叶子节点的上一个叶子节点的页号

    void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }    // 设置当前节点的父节点的页号

    char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }   // 获取第key_idx个键值

    Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }  // 获取第rid_idx个Rid

    void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }    // 设置第key_idx个键值

    void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }    // 设置第rid_idx个Rid

    int lower_bound(const char *target) const;  // 查找第一个大于等于target的key的位置，范围为[0,num_key)

    int upper_bound(const char *target) const;  // 查找第一个大于target的key的位置，范围为[0,num_key)

    void insert_pairs(int pos, const char *key, const Rid *rid, int n);     // 在pos位置插入n个键值对

    page_id_t internal_lookup(const char *key);  // 在内部节点中查找key对应的孩子节点的page_no

    bool leaf_lookup(const char *key, Rid **value); // 在叶子节点中查找key对应的Rid，找到返回true，value指向对应的Rid；否则返回false

    int insert(const char *key, const Rid &value);  // 在叶子节点中插入key和Rid

    // 用于在结点中的指定位置插入单个键值对
    void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    void erase_pair(int pos);   // 删除pos位置的键值对

    int remove(const char *key);    // 在叶子节点中删除key，返回删除的键值对数量，0或1

    /**
     * @brief used in internal node to remove the last key in root node, and return the last child，只剩下一个孩子节点时的根节点处理
     *
     * @return the last child
     */
    page_id_t remove_and_return_only_child() {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);   // 获取唯一孩子节点的页号
        erase_pair(0);  // 删除唯一的键值对
        assert(get_size() == 0);
        return child_page_no;   // 返回唯一孩子节点的页号
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle *child) {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
            if (get_rid(rid_idx)->page_no == child->get_page_no()) {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }
};

/* B+树 */
class IxIndexHandle {
    friend class IxScan;        // B+树的扫描器
    friend class IxManager;     // B+树的管理器

   private:
    DiskManager *disk_manager_;                 // 磁盘管理器
    BufferPoolManager *buffer_pool_manager_;    // 缓冲池管理器
    int fd_;                                    // 存储B+树的文件
    IxFileHdr* file_hdr_;                       // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    std::mutex root_latch_;                     // 保护root_page的互斥锁

   public:
    IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd);

    // for search
    bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);    // 查找key对应的Rid，找到返回true，result存放对应的Rid；否则返回false

    std::pair<IxNodeHandle *, bool> find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                                 bool find_first = false);  // 查找key对应的叶子节点

    // for insert
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction);    // 插入key和Rid，返回根节点页号

    IxNodeHandle *split(IxNodeHandle *node);    // 分裂节点，返回新节点

    void insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node, Transaction *transaction);     // 将old_node和new_node通过key连接起来，插入到父节点中

    // for delete
    bool delete_entry(const char *key, Transaction *transaction);   // 删除key，返回是否删除成功

    bool coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction = nullptr,
                                bool *root_is_latched = nullptr);   // 合并或重分配节点
    bool adjust_root(IxNodeHandle *old_root_node);  // 调整根节点

    void redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index);    // 重分配节点

    bool coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                  Transaction *transaction, bool *root_is_latched); // 合并节点

    Iid lower_bound(const char *key);   // 查找第一个大于等于key的Iid

    Iid upper_bound(const char *key);   // 查找第一个大于key的Iid

    Iid leaf_end() const;   // 返回叶子节点的结束Iid

    Iid leaf_begin() const;   // 返回叶子节点的开始Iid

   private:
    // 辅助函数
    void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }  // 更新根节点页号

    bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }   // 判断B+树是否为空

    // for get/create node
    IxNodeHandle *fetch_node(int page_no) const;    // 获取page_no对应的节点

    IxNodeHandle *create_node();    // 创建一个新的节点，返回节点句柄

    // for maintain data structure
    void maintain_parent(IxNodeHandle *node);   // 维护节点的父节点信息

    void erase_leaf(IxNodeHandle *leaf);   // 删除叶子节点

    void release_node_handle(IxNodeHandle &node);   // 释放节点句柄

    void maintain_child(IxNodeHandle *node, int child_idx);   // 维护子节点信息

    // for index test
    Rid get_rid(const Iid &iid) const;  // 根据iid获取对应的rid
};