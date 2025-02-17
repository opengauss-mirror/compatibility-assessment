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

import org.opengauss.tool.replay.task.SingleReplayThread;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SingleThreadModel
 *
 * @since 2024-07-01
 */
public class SingleThreadModel {
    private static SingleThreadModel instance;

    private final AtomicInteger threadCount;
    private final AtomicBoolean isClose;
    private final ConcurrentMap<String, Integer> sessionThreadMap;
    private final ConcurrentMap<Integer, SingleReplayThread> threadMap;

    private SingleThreadModel() {
        this.threadCount = new AtomicInteger(0);
        this.isClose = new AtomicBoolean(false);
        this.sessionThreadMap = new ConcurrentHashMap<>();
        this.threadMap = new ConcurrentHashMap<>();
    }

    /**
     * get SingleThreadModel instance
     *
     * @return instance
     */
    public static synchronized SingleThreadModel getInstance() {
        if (instance == null) {
            instance = new SingleThreadModel();
        }
        return instance;
    }

    /**
     * getThreadCount
     *
     * @return threadCount
     */
    public int getThreadCount() {
        return threadCount.get();
    }

    /**
     * incrementThreadCount
     */
    public void incrementThreadCount() {
        threadCount.getAndIncrement();
    }

    /**
     * decrementThreadCount
     */
    public void decrementThreadCount() {
        threadCount.getAndDecrement();
    }

    /**
     * putThreadMap
     *
     * @param threadId threadId
     * @param thread thread
     */
    public void putThreadMap(int threadId, SingleReplayThread thread) {
        threadMap.put(threadId, thread);
    }

    /**
     * getThreadMap
     *
     * @return threadMap
     */
    public Map<Integer, SingleReplayThread> getThreadMap() {
        return threadMap;
    }

    /**
     * putSessionThreadMap
     *
     * @param session session
     * @param threadId threadId
     */
    public void putSessionThreadMap(String session, int threadId) {
        sessionThreadMap.put(session, threadId);
    }

    /**
     * getSessionThreadMap
     *
     * @return sessionThreadMap
     */
    public Map<String, Integer> getSessionThreadMap() {
        return sessionThreadMap;
    }

    /**
     * removeSession
     *
     * @param sessionSet session set
     */
    public void removeSession(Set<String> sessionSet) {
        for (String session : sessionSet) {
            sessionThreadMap.remove(session);
        }
    }

    /**
     * removeThread
     *
     * @param name thread name
     */
    public void removeThread(String name) {
        int threadId = Integer.parseInt(name.split("-")[1]);
        threadMap.remove(threadId);
    }

    /**
     * isClose
     *
     * @return is all thread close flag
     */
    public Boolean isClose() {
        return isClose.get();
    }

    /**
     * setClose
     */
    public void setClose() {
        isClose.set(true);
    }

    /**
     * clearAllThreads
     */
    public synchronized void clearAllThreads() {
        for (SingleReplayThread thread : threadMap.values()) {
            if (thread.isAlive()) {
                thread.handleThreadData(thread);
            }
        }
    }
}

