/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include "ix_scan.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较

    int n = page_hdr->num_key;  // 插入的键值对数量

    if (!binary_search) {
        // 顺序扫描版本
        int i = 0;
        for (; i < n; ++i) {
            if (ix_compare(get_key(i), target, file_hdr->col_types_, file_hdr->col_lens_) >= 0) {
                break;
            }
        }
        return i;
    }

    // 二分查找版本
    int l = 0, r = n;   // 区间 [l, r)
    while (l < r) {
        int mid = (l + r) / 2;
        int cmp = ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp < 0) {
            // key[mid] < target，答案在右半边
            l = mid + 1;
        } else {
            // key[mid] >= target，收缩右边界
            r = mid;
        }
    }
    return l;   // l == r，为第一个 >= target 的位置，可能等于 n
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较

    int n = page_hdr->num_key;

    if (!binary_search) {
        // 顺序扫描版本
        int i = 1;  // 从 1 开始，避开内部结点里“最小 key”那一格
        for (; i < n; ++i) {
            if (ix_compare(get_key(i), target, file_hdr->col_types_, file_hdr->col_lens_) > 0) {
                break;
            }
        }
        return i;
    }

    // 二分查找版本，区间 [l, r)
    int l = 1, r = n;
    while (l < r) {
        int mid = (l + r) / 2;
        int cmp = ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp > 0) {
            // key[mid] > target，答案在左半边（含 mid）
            r = mid;
        } else {
            // key[mid] <= target，答案在右半边
            l = mid + 1;
        }
    }
    return l;   // [1, n]，n 表示“没有比 target 更大的 key”
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。

    // 在叶子节点中获取目标key所在位置
    int pos = lower_bound(key);

    // 判断是否越界，以及是否真正相等
    if ((pos < page_hdr->num_key) && (ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) == 0)) {
        // 取出对应的 rid
        *value = get_rid(pos);
        return true;
    }
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号

    // upper_bound 找到第一个 > key 的 key 的位置
    int pos = upper_bound(key);

    // 孩子下标就是 pos - 1，对应的 value 是子结点页号
    return value_at(pos - 1);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量

    // 判断 pos 合法性
    int size = get_size();
    assert(pos >= 0 && pos <= size);
    // 至多使用 get_max_size() 个槽位
    assert(size + n <= get_max_size());

    int key_len = file_hdr->col_tot_len_;

    // 为插入腾出空间：将 [pos, size) 段整体后移 n 个位置
    int move_cnt = size - pos;
    if (move_cnt > 0) {
        // keys 向后移动
        memmove(get_key(pos + n), get_key(pos), (size - pos) * key_len);
        // rids 向后移动
        memmove(get_rid(pos + n), get_rid(pos), (size - pos) * sizeof(Rid));
    }

    // 写入新的 n 个 key
    memcpy(get_key(pos), key, n * key_len);

    // 写入新的 n 个 rid
    memcpy(get_rid(pos), rid, n * sizeof(Rid));

    // 更新节点键数量
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量

    // 找到插入位置：第一个 >= key 的位置
    int pos = lower_bound(key);
    int size = get_size();

    // 如果该位置已经有相同 key，则为唯一索引，不插入
    if (pos < size && ix_compare(key, get_key(pos), file_hdr->col_types_, file_hdr->col_lens_) == 0) {
        return size;    // 大小不变
    }

    // 插入该键值对
    insert_pairs(pos, key, &value, 1);

    // 返回插入后的键值对数量
    return get_size();
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量

    int size = get_size();
    assert(pos >= 0 && pos < size);

    int key_len = file_hdr->col_tot_len_;

    // 把 [pos+1, size) 整体左移一格覆盖 pos
    int move_cnt = size - pos - 1;
    if (move_cnt > 0) {
        memmove(get_key(pos), get_key(pos + 1), move_cnt * key_len);
        memmove(get_rid(pos), get_rid(pos + 1), move_cnt * sizeof(Rid));
    }

    page_hdr->num_key -= 1;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量

    int size = get_size();
    int pos = lower_bound(key);

    // 不存在：直接返回原大小
    if (pos >= size ||
        ix_compare(get_key(pos), key, file_hdr->col_types_, file_hdr->col_lens_) != 0) {
        return size;
    }

    erase_pair(pos);
    return get_size();
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char* buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点

    // 从根结点开始
    page_id_t page_no = file_hdr_->root_page_;
    IxNodeHandle *node = fetch_node(page_no);

    // 一直向下走，直到叶子结点
    while (!node->is_leaf_page()) {
        page_id_t child_page_no = node->internal_lookup(key);
        // 当前是内部结点，用完就可以 unpin
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        // 继续向下
        node = fetch_node(child_page_no);
    
    }
    Rid *dummy = nullptr;
    bool found = node->leaf_lookup(key, &dummy);
    return std::make_pair(node, found);
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁

    result->clear();

    // 找到所在叶子
    auto [leaf, found] = find_leaf_page(key, Operation::FIND, transaction, false);

    // 在叶子里取 rid
    Rid *rid_ptr = nullptr;
    bool ok = leaf->leaf_lookup(key, &rid_ptr);
    if (ok) {
        result->push_back(*rid_ptr);
    }

    // 用完 unpin
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);

    return ok;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())

    // 创建新结点
    IxNodeHandle *new_node = create_node();

    // 初始化 page_hdr
    new_node->page_hdr->next_free_page_no = IX_NO_PAGE;
    new_node->page_hdr->is_leaf = node->is_leaf_page();
    new_node->page_hdr->parent = node->get_parent_page_no();
    new_node->set_size(0);

    int old_size = node->get_size();
    int move_cnt = old_size / 2;  // 右边移动 move_cnt 个键到新结点
    int key_len = file_hdr_->col_tot_len_;

    // 将右半部分键值对拷贝到新结点（无论叶子还是内部）
    char *src_keys = node->get_key(old_size - move_cnt);
    Rid *src_rids = node->get_rid(old_size - move_cnt);
    memcpy(new_node->keys, src_keys, move_cnt * key_len);
    memcpy(new_node->rids, src_rids, move_cnt * sizeof(Rid));
    new_node->set_size(move_cnt);
    node->set_size(old_size - move_cnt);

    // 叶子结点：维护叶子链表与 first/last_leaf
    if (node->is_leaf_page()) {
        // new_node 插在 node 的右边
        new_node->set_next_leaf(node->get_next_leaf());
        new_node->set_prev_leaf(node->get_page_no());
        // 更新原后继结点的 prev_leaf
        if (node->get_next_leaf() != IX_NO_PAGE) {
            IxNodeHandle *next = fetch_node(node->get_next_leaf());
            next->set_prev_leaf(new_node->get_page_no());
            buffer_pool_manager_->unpin_page(next->get_page_id(), true);
        }
        // 更新 node 的 next_leaf
        node->set_next_leaf(new_node->get_page_no());
        // 如果 node 原来是最后一个叶子，则更新 file_hdr_->last_leaf_
        if (file_hdr_->last_leaf_ == node->get_page_no()) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }
    } else {
        // 内部结点：需要更新新结点所有孩子的 parent 指针
        for (int i = 0; i < new_node->get_size(); ++i) {
            maintain_child(new_node, i);
        }
    }

    return new_node;  // 需要在外面 unpin
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page

    // old_node 是根结点，需要新建根
    if (old_node->is_root_page()) {
        IxNodeHandle *root = create_node();
        root->page_hdr->next_free_page_no = IX_NO_PAGE;
        root->page_hdr->is_leaf = false;
        root->page_hdr->parent = IX_NO_PAGE;
        root->set_size(0);

        // root 的两个孩子：old_node 和 new_node
        old_node->set_parent_page_no(root->get_page_no());
        new_node->set_parent_page_no(root->get_page_no());

        // root 中第一个 key 代表 old_node 的最小键
        Rid rid_old;
        rid_old.page_no = old_node->get_page_no();
        rid_old.slot_no = 0;
        root->insert_pair(0, old_node->get_key(0), rid_old);

        // root 中第二个 key 代表 new_node 的最小键（传入的 key）
        Rid rid_new;
        rid_new.page_no = new_node->get_page_no();
        rid_new.slot_no = 0;
        root->insert_pair(1, key, rid_new);

        // 更新根页号
        update_root_page_no(root->get_page_no());

        buffer_pool_manager_->unpin_page(root->get_page_id(), true);
        return;
    }

    // 一般情况：在父结点中，在 old_node 对应孩子之后插入 (key, new_node)
    page_id_t parent_page_no = old_node->get_parent_page_no();
    IxNodeHandle *parent = fetch_node(parent_page_no);
    int index = parent->find_child(old_node);  // old_node 在 parent 中的 rid_idx

    Rid rid_new;
    rid_new.page_no = new_node->get_page_no();
    rid_new.slot_no = 0;

    new_node->set_parent_page_no(parent->get_page_no());
    parent->insert_pair(index + 1, key, rid_new);

    // 父结点若满，则继续向上分裂
    if (parent->get_size() >= parent->get_max_size()) {
        IxNodeHandle *new_parent = split(parent);
        char *new_parent_key = new_parent->get_key(0);
        insert_into_parent(parent, new_parent_key, new_parent, transaction);
        buffer_pool_manager_->unpin_page(new_parent->get_page_id(), true);
    }

    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁

    // 找到应插入的叶子结点
    auto [leaf, found] = find_leaf_page(key, Operation::INSERT, transaction, false);

    // 在叶子上插入键值对
    int old_size = leaf->get_size();
    leaf->insert(key, value);

    // 如果 key 已存在（唯一索引，不插入）
    if (leaf->get_size() == old_size) {
        page_id_t pid = leaf->get_page_no();
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return pid;
    }

    // 如果没满，直接结束
    if (leaf->get_size() < leaf->get_max_size()) {
        page_id_t pid = leaf->get_page_no();
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
        return pid;
    }

    // 节点已满，需要分裂
    IxNodeHandle *new_leaf = split(leaf);
    
    // 新叶子结点的首 key 上推到父结点
    const char *push_up_key = new_leaf->get_key(0);
    insert_into_parent(leaf, push_up_key, new_leaf, transaction);

    // 维护 last_leaf_
    if (file_hdr_->last_leaf_ == leaf->get_page_no()) {
        file_hdr_->last_leaf_ = new_leaf->get_page_no();
    }

    // unpin 页面
    page_id_t pid = leaf->get_page_no();
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    buffer_pool_manager_->unpin_page(new_leaf->get_page_id(), true);

    return pid;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁

    // 找到叶子
    auto [leaf, found] = find_leaf_page(key, Operation::DELETE, transaction, false);

    // 不存在直接返回
    if (!found) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return false;
    }

    // 删除叶子上的 pair
    int pos = leaf->lower_bound(key);
    leaf->remove(key);
    bool first_key_changed = (pos == 0);

    // 如有必要，先维护祖先的“最小 key”
    if (first_key_changed && leaf->get_size() > 0) {
        maintain_parent(leaf);
    }

    // 若下溢，做合并/重分配
    if (!leaf->is_root_page() && leaf->get_size() < leaf->get_min_size()) {
        bool root_is_latched = false;
        coalesce_or_redistribute(leaf, transaction, &root_is_latched);
    } else if (leaf->is_root_page()) {
        // 根也交给 adjust_root 处理
        bool root_is_latched = false;
        coalesce_or_redistribute(leaf, transaction, &root_is_latched);
    }

    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）

    // 根结点特殊处理
    if (node->is_root_page()) {
        return adjust_root(node);
    }

    // 未下溢：不用处理
    if (node->get_size() >= node->get_min_size()) {
        return false;
    }

    // 找父结点
    IxNodeHandle *parent = fetch_node(node->get_parent_page_no());
    int index = parent->find_child(node);

    // 找兄弟（优先前驱）
    int neighbor_index = (index > 0) ? (index - 1) : (index + 1);
    IxNodeHandle *neighbor = fetch_node(parent->value_at(neighbor_index));

    // 够“二者都维持在 min_size” -> 重分配，否则合并
    if (neighbor->get_size() + node->get_size() >= 2 * node->get_min_size()) {
        redistribute(neighbor, node, parent, index);
        buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
        return false;
    }

    bool parent_should_delete = coalesce(&neighbor, &node, &parent, index, transaction, root_is_latched);

    buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    return parent_should_delete;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作

    // 情况1：内部根且 size==1，把唯一孩子提升为新根
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
        page_id_t child_page = old_root_node->remove_and_return_only_child();
        update_root_page_no(child_page);

        IxNodeHandle *child = fetch_node(child_page);
        child->set_parent_page_no(INVALID_PAGE_ID);
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        return true;
    }

    // 情况2：叶子根且 size==0，树变空
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
        update_root_page_no(IX_NO_PAGE);
        file_hdr_->first_leaf_ = IX_LEAF_HEADER_PAGE;
        file_hdr_->last_leaf_  = IX_LEAF_HEADER_PAGE;
        return true;
    }

    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论

    int key_len = file_hdr_->col_tot_len_;
    if (index == 0) {
        // node 在左，neighbor 在右：把 neighbor 的第 0 个挪到 node 尾部
        Rid moved = *neighbor_node->get_rid(0);
        const char *moved_key = neighbor_node->get_key(0);
        node->insert_pair(node->get_size(), moved_key, moved);
        neighbor_node->erase_pair(0);

        // internal 结点要维护孩子 parent
        if (!node->is_leaf_page()) {
            maintain_child(node, node->get_size() - 1);
        }

        // neighbor 的 first key 变了，更新 parent 中 neighbor 的 key（位置 = 1）
        if (neighbor_node->get_size() > 0) {
            memcpy(parent->get_key(1), neighbor_node->get_key(0), key_len);
        }
    } else {
        // neighbor 在左，node 在右：把 neighbor 的最后一个挪到 node 头部
        int last = neighbor_node->get_size() - 1;
        Rid moved = *neighbor_node->get_rid(last);
        const char *moved_key = neighbor_node->get_key(last);
        node->insert_pair(0, moved_key, moved);
        neighbor_node->erase_pair(last);

        if (!node->is_leaf_page()) {
            maintain_child(node, 0);
        }

        // node 的 first key 变了，更新 parent 中 node 的 key（位置 = index）
        if (node->get_size() > 0) {
            memcpy(parent->get_key(index), node->get_key(0), key_len);
        }
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf

    // 保证 (*neighbor_node) 在左，(*node) 在右
    if (index == 0) {
        std::swap(*neighbor_node, *node);
        index = 1;
    }

    IxNodeHandle *left = *neighbor_node;
    IxNodeHandle *right = *node;
    IxNodeHandle *p = *parent;
    int left_old_size = left->get_size();
    int move_cnt = right->get_size();

    // 把 right 全部搬到 left 尾部
    left->insert_pairs(left_old_size, right->keys, right->rids, move_cnt);

    // internal 结点：维护搬过去的孩子 parent 指针
    if (!left->is_leaf_page()) {
        for (int i = left_old_size; i < left_old_size + move_cnt; i++) {
            maintain_child(left, i);
        }
    } else {
        // 叶子：从链表中摘掉 right，并更新 last_leaf_
        if (right->get_page_no() == file_hdr_->last_leaf_) {
            file_hdr_->last_leaf_ = left->get_page_no();
        }
        if (right->get_page_no() == file_hdr_->first_leaf_) {
            file_hdr_->first_leaf_ = right->get_next_leaf();
        }
        erase_leaf(right);
    }

    // parent 删除 right 对应的入口
    p->erase_pair(index);

    // parent 可能下溢：递归向上处理
    bool parent_should_delete = false;
    if (p->is_root_page()) {
        parent_should_delete = adjust_root(p);
    } else if (p->get_size() < p->get_min_size()) {
        parent_should_delete = coalesce_or_redistribute(p, transaction, root_is_latched);
    }

    return parent_should_delete;
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    auto [leaf, found] = find_leaf_page(key, Operation::FIND, nullptr, false);
    int slot = leaf->lower_bound(key);

    // 如果落在叶子末尾，尝试跳到下一叶
    if (slot == leaf->get_size() && leaf->get_next_leaf() != IX_LEAF_HEADER_PAGE) {
        page_id_t next = leaf->get_next_leaf();
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return Iid{next, 0};
    }

    page_id_t pid = leaf->get_page_no();
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return Iid{pid, slot};
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    auto [leaf, found] = find_leaf_page(key, Operation::FIND, nullptr, false);
    int slot = leaf->upper_bound(key);

    if (slot == leaf->get_size() && leaf->get_next_leaf() != IX_LEAF_HEADER_PAGE) {
        page_id_t next = leaf->get_next_leaf();
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
        return Iid{next, 0};
    }

    page_id_t pid = leaf->get_page_no();
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    return Iid{pid, slot};
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}