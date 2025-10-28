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
 * Replacer is an abstract class that tracks page usage.只是一个接口类，具体实现在派生类中实现
 */
class Replacer {
   public:
    Replacer() = default;       // 默认构造函数
    virtual ~Replacer() = default;

    /**
     * Remove the victim frame as defined by the replacement policy.寻找替换帧
     * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
     * @return true if a victim frame was found, false otherwise
     */
    virtual bool victim(frame_id_t *frame_id) = 0;

    /**
     * Pins a frame, indicating that it should not be victimized until it is unpinned.固定帧
     * @param frame_id the id of the frame to pin
     */
    virtual void pin(frame_id_t frame_id) = 0;

    /**
     * Unpins a frame, indicating that it can now be victimized.取消固定帧
     * @param frame_id the id of the frame to unpin
     */
    virtual void unpin(frame_id_t frame_id) = 0;

    /** @return the number of elements in the replacer that can be victimized */
    // 返回可被替换的页面数量
    virtual size_t Size() = 0;
};
