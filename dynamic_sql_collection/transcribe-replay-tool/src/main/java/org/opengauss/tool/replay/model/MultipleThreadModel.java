/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.replay.model;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * MultipleThreadModel
 *
 * @since 2024-07-01
 */
public class MultipleThreadModel {
    private static MultipleThreadModel instance;

    private final AtomicInteger lastSqlId;
    private final AtomicInteger finishThreadCount;

    private MultipleThreadModel() {
        this.lastSqlId = new AtomicInteger(0);
        this.finishThreadCount = new AtomicInteger(0);
    }

    /**
     * get MultipleThreadModel instance
     *
     * @return instance
     */
    public static synchronized MultipleThreadModel getInstance() {
        if (instance == null) {
            instance = new MultipleThreadModel();
        }
        return instance;
    }

    /**
     * getLastSqlId
     *
     * @return lastSqlId
     */
    public int getLastSqlId() {
        return lastSqlId.get();
    }

    /**
     * setLastSqlId
     *
     * @param id id
     */
    public void setLastSqlId(int id) {
        lastSqlId.set(id);
    }

    /**
     * getFinishThreadCount
     *
     * @return finishThreadCount
     */
    public int getFinishThreadCount() {
        return finishThreadCount.get();
    }

    /**
     * incrementFinishCount
     */
    public void incrementFinishCount() {
        finishThreadCount.getAndIncrement();
    }
}
