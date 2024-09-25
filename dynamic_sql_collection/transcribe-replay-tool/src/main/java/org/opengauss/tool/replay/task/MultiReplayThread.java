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

import org.opengauss.tool.config.DatabaseConfig;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.factory.ReplayConnectionFactory;
import org.opengauss.tool.replay.model.ExecuteResponse;
import org.opengauss.tool.replay.model.MultipleThreadModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.replay.operator.ReplaySqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * MultiReplayThread
 *
 * @since 2024-07-01
 */
public class MultiReplayThread extends ReplayThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayThread.class);
    private static final int POLL_TIMEOUT = 5;

    private final ReplaySqlOperator replaySqlOperator;
    private final MultipleThreadModel multipleThreadModel;
    private final PriorityQueue<SqlModel> slowSqlQueue =
            new PriorityQueue<>(Comparator.comparingLong(SqlModel::getOpgsDuration));

    /**
     * constructor
     *
     * @param threadId threadId
     * @param replayConfig replayConfig
     */
    public MultiReplayThread(int threadId, ReplayConfig replayConfig) {
        super(threadId, replayConfig);
        setName("replay-" + threadId);
        this.sqlModelQueue = new LinkedBlockingQueue<>(100);
        this.replaySqlOperator = new ReplaySqlOperator(replayConfig);
        this.multipleThreadModel = MultipleThreadModel.getInstance();
    }

    private void close() {
        isThreadStop.set(true);
        LOGGER.info("thread:{} will be interrupted", this.getName());
        multipleThreadModel.incrementFinishCount();
        processModel.addSuccessCount(successCount);
        processModel.addFailCount(failCount);
        processModel.addSlowCount(slowCount);
        if ("replay-1".equals(this.getName())) {
            updateSlowSqlQueue(slowSqlQueue);
            collectDuration(this);
        }
        LOGGER.info("after interrupt, thread:{}, threadFinishCount:{}", this.getName(),
                multipleThreadModel.getFinishThreadCount());
        this.interrupt();
    }

    @Override
    public void run() {
        while (!isThreadStop.get()) {
            try {
                SqlModel sqlModel = sqlModelQueue.poll(POLL_TIMEOUT, TimeUnit.SECONDS);
                if (sqlModel == null && processModel.isAssignFinish()) {
                    LOGGER.info("replay finished...");
                    close();
                    break;
                }
                if (sqlModel != null) {
                    replayQuerySql(sqlModel, replayConfig.getTargetDbConfig());
                }
            } catch (InterruptedException e) {
                LOGGER.info("take thread has been interrupted, error message:{}", e.getMessage());
            }
        }
    }

    private void replayQuerySql(SqlModel sqlModel, DatabaseConfig databaseConfigConfig) {
        Optional<Connection> replayConnOptional = ReplayConnectionFactory.getInstance().getConnection(
                databaseConfigConfig, sqlModel.getSchema(), replayConfig.getSchemaMap());
        if (!replayConnOptional.isPresent()) {
            LOGGER.error("sql replay fail due to connection is null, please check sql.replay.database.schema.map of "
                    + "replay.properties, sql id:{}, schema:{}", sqlModel.getId(), sqlModel.getSchema());
            return;
        }
        Connection replayConn = replayConnOptional.get();
        processModel.incrementMultipleQueryCount();
        ExecuteResponse response = sqlModel.isPrepared()
                ? replaySqlOperator.executePrepareSql(replayConn, sqlModel)
                : replaySqlOperator.executeStmtSql(replayConn, sqlModel);
        handleResult(sqlModel, replayConfig, response);
        if (processModel.isAssignFinish() && sqlModel.getId() == multipleThreadModel.getLastSqlId()) {
            close();
        }
    }
}
