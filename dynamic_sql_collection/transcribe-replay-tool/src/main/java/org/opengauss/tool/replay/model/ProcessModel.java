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

import lombok.Getter;
import org.jfree.data.xy.XYSeries;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ProcessModel
 *
 * @since 2024-07-01
 */
public class ProcessModel {
    private static ProcessModel instance;

    private final AtomicInteger sqlCount;
    private final AtomicInteger successCount;
    private final AtomicInteger skipCount;
    private final AtomicInteger failCount;
    private final AtomicInteger slowCount;
    private final AtomicInteger replayCount;
    private final AtomicInteger multipleQueryCount;
    private final AtomicBoolean isReplayFinish;
    private final AtomicBoolean isReadFinish;
    private final AtomicBoolean isAssignFinish;

    @Getter
    private final PriorityQueue<SqlModel> slowSqlQueue;
    @Getter
    private XYSeries mysqlSeries;
    @Getter
    private XYSeries opgsSeries;

    private ProcessModel() {
        this.sqlCount = new AtomicInteger(0);
        this.successCount = new AtomicInteger(0);
        this.skipCount = new AtomicInteger(0);
        this.failCount = new AtomicInteger(0);
        this.slowCount = new AtomicInteger(0);
        this.replayCount = new AtomicInteger(0);
        this.multipleQueryCount = new AtomicInteger(0);
        this.isReplayFinish = new AtomicBoolean(false);
        this.isAssignFinish = new AtomicBoolean(false);
        this.isReadFinish = new AtomicBoolean(false);
        this.slowSqlQueue = new PriorityQueue<>(Comparator.comparingLong(SqlModel::getOpgsDuration));
        this.mysqlSeries = new XYSeries("SourceDB");
        this.opgsSeries = new XYSeries("SinkDB");
    }

    /**
     * get Process instance
     *
     * @return instance
     */
    public static synchronized ProcessModel getInstance() {
        if (instance == null) {
            instance = new ProcessModel();
        }
        return instance;
    }

    /**
     * getSqlCount
     *
     * @return sqlCount
     */
    public int getSqlCount() {
        return sqlCount.get();
    }

    /**
     * addSqlCount
     *
     * @param number number
     */
    public void addSqlCount(int number) {
        sqlCount.getAndAdd(number);
    }

    /**
     * getSuccessCount
     *
     * @return successCount
     */
    public int getSuccessCount() {
        return successCount.get();
    }

    /**
     * incrementSuccessCount
     */
    public void incrementSuccessCount() {
        successCount.getAndIncrement();
    }

    /**
     * addSuccessCount
     *
     * @param number number
     */
    public void addSuccessCount(int number) {
        successCount.getAndAdd(number);
    }

    /**
     * getSkipCount
     *
     * @return skipCount
     */
    public int getSkipCount() {
        return skipCount.get();
    }

    /**
     * incrementSkipCount
     */
    public void incrementSkipCount() {
        skipCount.getAndIncrement();
    }

    /**
     * getFailCount
     *
     * @return failCount
     */
    public int getFailCount() {
        return failCount.get();
    }

    /**
     * incrementFailCount
     */
    public void incrementFailCount() {
        failCount.getAndIncrement();
    }

    /**
     * addFailCount
     *
     * @param number number
     */
    public void addFailCount(int number) {
        failCount.getAndAdd(number);
    }

    /**
     * getSlowCount
     *
     * @return slowCount
     */
    public int getSlowCount() {
        return slowCount.get();
    }

    /**
     * incrementSlowCount
     */
    public void incrementSlowCount() {
        slowCount.getAndIncrement();
    }

    /**
     * addSlowCount
     *
     * @param number number
     */
    public void addSlowCount(int number) {
        slowCount.getAndAdd(number);
    }

    /**
     * getReplayCount
     *
     * @return replayCount
     */
    public int getReplayCount() {
        return replayCount.get();
    }

    /**
     * incrementReplayCount
     */
    public void incrementReplayCount() {
        replayCount.getAndIncrement();
    }

    /**
     * getMultipleQueryCount
     *
     * @return multipleQueryCount
     */
    public int getMultipleQueryCount() {
        return multipleQueryCount.get();
    }

    /**
     * incrementMultipleQueryCount
     */
    public void incrementMultipleQueryCount() {
        multipleQueryCount.getAndIncrement();
    }

    /**
     * isReplayFinish
     *
     * @return isReplayFinish
     */
    public boolean isReplayFinish() {
        return isReplayFinish.get();
    }

    /**
     * setReplayFinish
     */
    public void setReplayFinish() {
        isReplayFinish.set(true);
    }

    /**
     * isAssignFinish
     *
     * @return isAssignFinish
     */
    public boolean isAssignFinish() {
        return isAssignFinish.get();
    }

    /**
     * isAssignFinish
     */
    public void setAssignFinish() {
        isAssignFinish.set(true);
    }

    /**
     * isReadFinish
     *
     * @return isReadFinish
     */
    public boolean isReadFinish() {
        return isReadFinish.get();
    }

    /**
     * setReadFinish
     */
    public void setReadFinish() {
        isReadFinish.set(true);
    }

    /**
     * addMysqlSeries
     *
     * @param id id
     * @param duration duration
     */
    public void addMysqlSeries(int id, long duration) {
        mysqlSeries.add(id, duration);
    }

    /**
     * addOpgsSeries
     *
     * @param id id
     * @param duration duration
     */
    public void addOpgsSeries(int id, long duration) {
        opgsSeries.add(id, duration);
    }

    /**
     * pollSlowSqlQueue
     */
    public void pollSlowSqlQueue() {
        slowSqlQueue.poll();
    }

    /**
     * offerSlowSqlQueue
     *
     * @param sqlModel sqlModel
     */
    public void offerSlowSqlQueue(SqlModel sqlModel) {
        slowSqlQueue.offer(sqlModel);
    }
}
