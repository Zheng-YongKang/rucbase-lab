/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

// 每个字节都置为 0
DiskManager::DiskManager() { memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char))); }

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等时 throw InternalError("DiskManager::write_page Error");
    off_t pos = static_cast<off_t>(page_no) * PAGE_SIZE;    // off_t是文件偏移量类型
    if (lseek(fd, pos, SEEK_SET) == (off_t)-1) {    /*
        lseek函数用于移动文件读写指针，返回新文件偏移位置，出现错误返回(off_t)-1，SEEK_SET表示从文件开头开始移动*/ 
        throw InternalError("lseek failed");
    }
    ssize_t n = write(fd, offset, num_bytes);   // 返回值 n 是实际写入的字节数，类型 ssize_t 是有符号大小类型，返回值可为 -1（表示错误）
    if (n != num_bytes) {
        throw InternalError("DiskManager::write_page Error");
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager::read_page Error");
    off_t pos = static_cast<off_t>(page_no) * PAGE_SIZE;
    if (lseek(fd, pos, SEEK_SET) == (off_t)-1) {
        throw InternalError("lseek failed");
    }
    ssize_t n = read(fd, offset, num_bytes);
    if (n != num_bytes) {
        throw InternalError("DiskManager::read_page Error");
    }

}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;    // x++（后缀自增）会返回旧值，再把值+1
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {} // __attribute__((unused))告诉编译器“这个参数在函数体里没有被用到”，不要产生“unused parameter”警告

bool DiskManager::is_dir(const std::string& path) { //  判断返回的路径是否是一个目录
    struct stat st; // struct stat在<sys/stat.h>中定义，用于存储文件信息
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode); // stat在这里用于获取指定路径的状态信息，S_ISDIR宏判断st_mode是否表示一个目录
}

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true 
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode); //  S_ISREG宏判断st_mode是否表示一个常规文件
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    if (is_file(path)) {
        throw FileExistsError(path); // 或者自定义：不能重复创建
    }
    // O_RDWR: 读写模式 | O_CREAT: 文件不存在则创建 | O_EXCL: 与O_CREAT一起使用，文件已存在则出错
    // 0644: 用户可读写，组用户和其他用户可读rw-r--r--
    int fd = open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) {
        throw UnixError();
    }
    // 立即关闭
    close(fd);
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    // 若此文件在打开表中，不允许删除
    if (path2fd_.count(path)) { //  path2fd_记录了所有打开的文件，count查询是否在表格中
        throw InternalError("destroy_file: file is still open: " + path); // 自定义错误：文件未关闭
    }
    if (!is_file(path)) {
        throw FileNotFoundError(path);
    }
    if (unlink(path.c_str()) < 0) {
        throw UnixError();
    }
}


/**
 * @description: 打开指定路径文件 
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    // 如果已打开，直接复用旧 fd
    std::unordered_map<std::string, int>::iterator it = path2fd_.find(path);    //  unordered_map是哈希表，iterator是迭代器
    if (it != path2fd_.end()) { // 如果找到了
        return it->second;
    }

    int fd = open(path.c_str(), O_RDWR);
    if (fd < 0) {
        if (errno == ENOENT) {
            throw FileNotFoundError("No such file or directory: " + path);
        }
        
        throw UnixError();
    }

    // 更新双向表
    path2fd_[path] = fd;
    fd2path_[fd]   = path;

    // 校验 fd 合法以便用于 fd2pageno_
    if (fd < 0 || fd >= MAX_FD) {   // 如果无效
        // 回滚
        fd2path_.erase(fd);
        path2fd_.erase(path);
        close(fd);
        throw InternalError("fd out of range");
    }
    return fd;
}

/**
 * @description:用于关闭指定路径文件 
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    auto it = fd2path_.find(fd);
    if (it == fd2path_.end()) {
        throw FileNotOpenError(fd); // 未打开
    }
    std::string path = it->second;
    if (close(fd) < 0) {
        throw UnixError();
    }
    // 两张表一起删
    fd2path_.erase(it);
    path2fd_.erase(path);
}


/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}


/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    if (log_fd_ == -1) {    //  如果还没打开log文件
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {   //  偏移无效
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}


/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}