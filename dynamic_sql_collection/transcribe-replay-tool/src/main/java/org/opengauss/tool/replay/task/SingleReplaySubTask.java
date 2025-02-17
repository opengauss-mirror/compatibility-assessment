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

import lombok.NoArgsConstructor;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.replay.model.SingleThreadModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.replay.operator.ReplaySqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * SingleReplaySubTask
 *
 * @since 2024-07-01
 */
@NoArgsConstructor(force = true)
public class SingleReplaySubTask implements ReplaySubTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaySubTask.class);

    private final ReplayConfig replayConfig;
    private final ReplaySqlOperator replaySqlOperator;

    private int sessionCount;
    private int maximumPoolSize;
    private SingleThreadModel singleThreadModel;

    /**
     * constructor
     *
     * @param replayConfig replayConfig
     */
    public SingleReplaySubTask(ReplayConfig replayConfig) {
        this.replayConfig = replayConfig;
        this.replaySqlOperator = new ReplaySqlOperator(replayConfig);
    }

    @Override
    public void init() {
        sessionCount = 0;
        maximumPoolSize = replayConfig.getMaxPoolSize();
        singleThreadModel = SingleThreadModel.getInstance();
    }

    @Override
    public void distribute(SqlModel sqlModel) {
        String session = sqlModel.getSession();
        ProcessModel processModel = ProcessModel.getInstance();
        if (replaySqlOperator.isSkipSql(sqlModel)) {
            processModel.incrementSkipCount();
            processModel.incrementReplayCount();
            if (processModel.isReadFinish() && processModel.getReplayCount() == processModel.getSqlCount()
                    && singleThreadModel.getThreadCount() == 0) {
                processModel.setReplayFinish();
            }
            return;
        }
        if (!singleThreadModel.getSessionThreadMap().containsKey(session)) {
            if (singleThreadModel.getThreadMap().size() < maximumPoolSize) {
                int threadId = findThreadId(singleThreadModel.getThreadMap().keySet(), maximumPoolSize);
                SingleReplayThread thread = new SingleReplayThread(threadId, replayConfig);
                thread.getSessionSet().add(session);
                thread.start();
                singleThreadModel.putThreadMap(threadId, thread);
                singleThreadModel.putSessionThreadMap(session, threadId);
                singleThreadModel.incrementThreadCount();
            } else {
                int threadId = sessionCount % maximumPoolSize;
                singleThreadModel.putSessionThreadMap(session, threadId);
                SingleReplayThread thread;
                do {
                    thread = singleThreadModel.getThreadMap().get(threadId);
                } while (thread.getIsThreadStop().get());
                thread.getSessionSet().add(session);
                if (!thread.isAlive()) {
                    thread.start();
                    singleThreadModel.incrementThreadCount();
                    LOGGER.info("start thread:{}", thread.getName());
                }
            }
            sessionCount++;
        }
        int threadId = singleThreadModel.getSessionThreadMap().get(session);
        SingleReplayThread thread = singleThreadModel.getThreadMap().get(threadId);
        thread.addSqlModel(sqlModel);
    }

    private int findThreadId(Set<Integer> threadIdSet, int maximumPoolSize) {
        for (int i = 0; i < maximumPoolSize; i++) {
            if (!threadIdSet.contains(i)) {
                return i;
            }
        }
        return 0;
    }
}
