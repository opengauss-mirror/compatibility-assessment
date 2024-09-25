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

package org.opengauss.tool.replay.task;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.model.ExecuteResponse;
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReplayThread
 *
 * @since 2024-07-01
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor(force = true)
public class ReplayThread extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayThread.class);

    /**
     * replayConfig
     */
    protected final ReplayConfig replayConfig;

    /**
     * isThreadStop
     */
    protected final AtomicBoolean isThreadStop;

    /**
     * processModel
     */
    protected final ProcessModel processModel;

    /**
     * sqlModelQueue
     */
    protected BlockingQueue<SqlModel> sqlModelQueue;

    /**
     * successCount
     */
    protected int successCount = 0;

    /**
     * failCount
     */
    protected int failCount = 0;

    /**
     * slowCount
     */
    protected int slowCount = 0;

    private final Map<Integer, Long> mysqlDurationMap;
    private final Map<Integer, Long> opgsDurationMap;
    private final PriorityQueue<SqlModel> slowSqlQueue =
            new PriorityQueue<>(Comparator.comparingLong(SqlModel::getOpgsDuration));

    /**
     * init replay thread
     *
     * @param threadId     threadId
     * @param replayConfig replayConfig
     */
    public ReplayThread(int threadId, ReplayConfig replayConfig) {
        setName("replay-" + threadId);
        this.replayConfig = replayConfig;
        this.mysqlDurationMap = new HashMap<>();
        this.opgsDurationMap = new HashMap<>();
        this.sqlModelQueue = new LinkedBlockingQueue<>(10000);
        this.isThreadStop = new AtomicBoolean(false);
        this.processModel = ProcessModel.getInstance();
    }

    /**
     * add sqlModel to sqlModelQueue
     *
     * @param sqlModel sqlModel
     */
    public void addSqlModel(SqlModel sqlModel) {
        try {
            sqlModelQueue.put(sqlModel);
        } catch (InterruptedException e) {
            LOGGER.error("add sql to replay queue failed, error message:{}", e.getMessage());
        }
    }

    /**
     * handleResult
     *
     * @param sqlModel     sqlModel
     * @param replayConfig replayConfig
     * @param response     response
     */
    protected void handleResult(SqlModel sqlModel, ReplayConfig replayConfig, ExecuteResponse response) {
        if (response.isSuccess()) {
            sqlModel.setOpgsDuration(response.getOpgsDuration());
            sqlModel.setSqlExplain(response.getSlowSqlExplain());
            updateQueue(sqlModel);
            successCount++;
            updateSlowCount(response);
            updateDurationMap(sqlModel, replayConfig);
        } else {
            failCount++;
        }
    }

    /**
     * updateDurationMap
     *
     * @param sqlModel     sqlModel
     * @param replayConfig replayConfig
     */
    protected void updateDurationMap(SqlModel sqlModel, ReplayConfig replayConfig) {
        if (sqlModel.getId() % replayConfig.getCollectIdThreshold() == 0) {
            mysqlDurationMap.put(sqlModel.getId(), sqlModel.getMysqlDuration());
            opgsDurationMap.put(sqlModel.getId(), sqlModel.getOpgsDuration());
            LOGGER.debug("current sql id:{}", sqlModel.getId());
        }
    }

    /**
     * updateSlowCount
     *
     * @param response response
     */
    protected void updateSlowCount(ExecuteResponse response) {
        if (response.isSlowSql()) {
            slowCount++;
        }
    }

    /**
     * updateQueue
     *
     * @param sqlModel sqlModel
     */
    protected void updateQueue(SqlModel sqlModel) {
        if (slowSqlQueue.size() >= replayConfig.getSlowSqlTopNum() && sqlModel.getOpgsDuration()
                <= slowSqlQueue.peek().getOpgsDuration()) {
            return;
        }
        if (slowSqlQueue.peek() != null && sqlModel.getOpgsDuration() > slowSqlQueue.peek().getOpgsDuration()) {
            slowSqlQueue.poll();
        }
        slowSqlQueue.offer(sqlModel);
    }

    /**
     * collectDuration
     *
     * @param thread thread
     */
    protected synchronized void collectDuration(ReplayThread thread) {
        for (Map.Entry<Integer, Long> entry : thread.mysqlDurationMap.entrySet()) {
            processModel.addMysqlSeries(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<Integer, Long> entry : thread.opgsDurationMap.entrySet()) {
            processModel.addOpgsSeries(entry.getKey(), entry.getValue());
        }
    }

    /**
     * updateSlowSqlQueue
     *
     * @param threadSlowSqlQueue threadSlowSqlQueue
     */
    protected synchronized void updateSlowSqlQueue(PriorityQueue<SqlModel> threadSlowSqlQueue) {
        while (!threadSlowSqlQueue.isEmpty()) {
            SqlModel sqlModel = threadSlowSqlQueue.poll();
            PriorityQueue<SqlModel> currentSlowSqlQueue = processModel.getSlowSqlQueue();
            if (currentSlowSqlQueue.peek() != null) {
                long fastQueueDuration = currentSlowSqlQueue.peek().getOpgsDuration();
                if (currentSlowSqlQueue.size() >= replayConfig.getSlowSqlTopNum()
                        && sqlModel.getOpgsDuration() <= fastQueueDuration) {
                    continue;
                }
                if (sqlModel.getOpgsDuration() > fastQueueDuration) {
                    processModel.pollSlowSqlQueue();
                }
            }
            processModel.offerSlowSqlQueue(sqlModel);
        }
    }
}
