/*
 * Copyright (c) 2023-2023 Huawei Technologies Co.,Ltd.
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

package org.opengauss.parser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Description: save output sql file locks
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class FileLocks {
    /**
     * synchronization lock
     */
    public static final Object lockersSync = new Object();
    private static Map<String, ReentrantReadWriteLock> lockers = new ConcurrentHashMap<>();

    /**
     * lock the file and add file lock to map,
     * avoiding assessment thread read file before write.
     *
     * @param filename String
     * @return ReentrantReadWriteLock
     */
    public static ReentrantReadWriteLock addLockerAndLockFile(String filename) {
        ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
        synchronized (lockersSync) {
            lockers.put(filename, locker);
            locker.writeLock().lock();
        }
        return locker;
    }

    /**
     * add file lock to map.
     *
     * @param filename String
     */
    public static void addLocker(String filename) {
        lockers.put(filename, new ReentrantReadWriteLock());
    }

    /**
     * get file lock by filename
     *
     * @param filename String
     * @return ReentrantReadWriteLock
     */
    public static ReentrantReadWriteLock getLockByFile(String filename) {
        return lockers.get(filename);
    }

    /**
     * get all file locks
     *
     * @return Map<String, ReentrantReadWriteLock>
     */
    public static Map<String, ReentrantReadWriteLock> getLockers() {
        return lockers;
    }
}
