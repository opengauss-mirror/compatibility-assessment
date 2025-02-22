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

import org.apache.commons.lang3.StringUtils;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.factory.ReplayConnectionFactory;
import org.opengauss.tool.replay.model.ExecuteResponse;
import org.opengauss.tool.replay.model.SingleThreadModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.replay.operator.ReplayLogOperator;
import org.opengauss.tool.replay.operator.ReplaySqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SingleReplayThread
 *
 * @since 2024-07-01
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class SingleReplayThread extends ReplayThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayThread.class);
    private static final ConcurrentMap<Integer, Long> EXECUTE_SQL_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Integer, String> READY_SQL_MAP = new ConcurrentHashMap<>();
    private static final AtomicLong CURRENT_SQL_END_TIME = new AtomicLong(Long.MAX_VALUE);
    private static final AtomicInteger MIN_READY_SQL_ID = new AtomicInteger(0);
    private static final Lock LOCK = new ReentrantLock();
    private static final int POLL_TIMEOUT = 5;

    private final Set<String> sessionSet;
    private final ReplaySqlOperator replaySqlOperator;
    private final ReplayLogOperator replayLogOperator;
    private final SingleThreadModel singleThreadModel;
    private long preStartTime = 0L;
    private long replyTime = 0L;

    /**
     * init replay thread
     *
     * @param threadId threadId
     * @param replayConfig replayConfig
     */
    public SingleReplayThread(int threadId, ReplayConfig replayConfig) {
        super(threadId, replayConfig);
        this.sessionSet = new HashSet<>();
        this.replaySqlOperator = new ReplaySqlOperator(replayConfig);
        this.replayLogOperator = new ReplayLogOperator();
        this.singleThreadModel = SingleThreadModel.getInstance();
    }

    private void close() {
        isThreadStop.set(true);
        this.interrupt();
        LOGGER.info("after interrupt, thread:{}, threadCount:{}", this.getName(), singleThreadModel.getThreadCount());
    }

    @Override
    public void run() {
        while (!isThreadStop.get() && !singleThreadModel.isClose()) {
            try {
                SqlModel sqlModel = sqlModelQueue.poll(POLL_TIMEOUT, TimeUnit.SECONDS);
                if (sqlModel == null) {
                    if (!singleThreadModel.isClose()) {
                        LOGGER.info("the sql queue has been empty for {}s, thread name :{}, session:{}", POLL_TIMEOUT,
                            this.getName(), this.sessionSet);
                        closeThread(sessionSet);
                    }
                    break;
                }
                replay(sqlModel);
            } catch (InterruptedException e) {
                LOGGER.debug("poll thread has been interrupted, error message:{}", e.getMessage());
            }
        }
    }

    private void replay(SqlModel sqlModel) {
        Optional<Connection> replayConnOptional = ReplayConnectionFactory.getInstance()
            .getConnection(replayConfig.getTargetDbConfig(), sqlModel.getSchema(), replayConfig.getSchemaMap());
        if (!replayConnOptional.isPresent()) {
            processModel.incrementFailCount();
            String errorMessage = String.format("sql replay fail due to connection is null, please check sql.replay."
                + "database.schema.map of replay.properties, schema:%s", sqlModel.getSchema());
            replayLogOperator.printFailSqlLog(sqlModel, errorMessage);
            processModel.incrementReplayCount();
        } else if ("quit".equals(sqlModel.getSql())) {
            processModel.incrementSuccessCount();
            String session = sqlModel.getSession();
            sessionSet.remove(session);
            processModel.incrementReplayCount();
            closeThread(Collections.singleton(session));
        } else {
            while (true) {
                try {
                    LOCK.lock();
                    long currentEndTime = CURRENT_SQL_END_TIME.get();
                    int currentThreadCount = singleThreadModel.getThreadCount();
                    Set<Integer> readySqlIdSet = READY_SQL_MAP.keySet();
                    int minReadyId = MIN_READY_SQL_ID.get();
                    if (currentEndTime != Long.MAX_VALUE && sqlModel.getStartTime() < currentEndTime
                        || readySqlIdSet.size() == currentThreadCount && sqlModel.getId() == minReadyId) {
                        ExecuteResponse response = execute(sqlModel, currentEndTime, replayConnOptional.get());
                        handleResult(sqlModel, replayConfig, response);
                        break;
                    }
                    if (!readySqlIdSet.contains(sqlModel.getId())) {
                        READY_SQL_MAP.put(sqlModel.getId(), sqlModel.getSession());
                        MIN_READY_SQL_ID.set(READY_SQL_MAP.keySet().stream().sorted().findFirst().orElse(0));
                    }
                } finally {
                    LOCK.unlock();
                }
            }
            processModel.incrementReplayCount();
        }
        if (processModel.isReadFinish() && !singleThreadModel.isClose()
            && processModel.getReplayCount() == processModel.getSqlCount()) {
            singleThreadModel.setClose();
            singleThreadModel.clearAllThreads();
        }
    }

    private ExecuteResponse execute(SqlModel sqlModel, long currentEndTime, Connection replayConn) {
        long endTime = sqlModel.getEndTime();
        READY_SQL_MAP.remove(sqlModel.getId());
        MIN_READY_SQL_ID.set(READY_SQL_MAP.keySet().stream().sorted().findFirst().orElse(0));
        EXECUTE_SQL_MAP.put(sqlModel.getId(), sqlModel.getEndTime());
        if (endTime < currentEndTime) {
            CURRENT_SQL_END_TIME.set(endTime);
        }
        if (replaySqlOperator.getReplayConfig().isSourceTimeInterval()) {
            sourceTimeInterval(sqlModel);
        }
        ExecuteResponse response = new ExecuteResponse();
        response = sqlModel.isPrepared()
            ? replaySqlOperator.executePrepareSql(replayConn, sqlModel)
            : replaySqlOperator.executeStmtSql(replayConn, sqlModel);
        EXECUTE_SQL_MAP.remove(sqlModel.getId());
        if (CURRENT_SQL_END_TIME.get() == endTime) {
            CURRENT_SQL_END_TIME.set(EXECUTE_SQL_MAP.values().stream().sorted().findFirst().orElse(Long.MAX_VALUE));
        }
        return response;
    }

    private void sourceTimeInterval(SqlModel sqlModel) {
        if (preStartTime > 0) {
            long separation = (sqlModel.getStartTime() - preStartTime) / 1000;
            long diffTime = System.currentTimeMillis() - replyTime;
            if (separation > diffTime) {
                try {
                    sleep(separation - diffTime);
                } catch (InterruptedException e) {
                    LOGGER.error("InterruptedException occurred while reply sql, error message is: {}.",
                        e.getMessage());
                }
            }
        }
        replyTime = System.currentTimeMillis();
        preStartTime = sqlModel.getStartTime();
    }

    private synchronized void closeThread(Set<String> sessions) {
        String firstSession = sessions.stream().findFirst().orElse(StringUtils.EMPTY);
        if (!singleThreadModel.getSessionThreadMap().containsKey(firstSession)) {
            return;
        }
        int id = singleThreadModel.getSessionThreadMap().get(firstSession);
        SingleReplayThread thread = singleThreadModel.getThreadMap().get(id);
        for (String session : sessions) {
            singleThreadModel.getSessionThreadMap().remove(session);
        }
        if (!singleThreadModel.getSessionThreadMap().containsValue(id)) {
            handleThreadData(thread);
            singleThreadModel.getThreadMap().put(id, new SingleReplayThread(id, replayConfig));
        }
    }

    /**
     * handleThreadData
     *
     * @param thread thread
     */
    public synchronized void handleThreadData(SingleReplayThread thread) {
        processModel.addSuccessCount(successCount);
        processModel.addFailCount(failCount);
        processModel.addSlowCount(slowCount);
        updateSlowSqlQueue(thread.getSlowSqlQueue());
        collectDuration(thread);
        LOGGER.info("thread will be stop, name: {}", thread.getName());
        thread.close();
        singleThreadModel.removeThread(getName());
        singleThreadModel.removeSession(sessionSet);
        singleThreadModel.decrementThreadCount();
        if (processModel.isReadFinish() && processModel.getReplayCount() == processModel.getSqlCount()
            && singleThreadModel.getThreadCount() == 0) {
            processModel.setReplayFinish();
        }
    }
}
