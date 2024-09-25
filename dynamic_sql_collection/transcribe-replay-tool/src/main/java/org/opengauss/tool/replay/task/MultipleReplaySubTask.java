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

import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.model.MultipleThreadModel;
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.replay.operator.ReplaySqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * MultipleReplaySubTask
 *
 * @since 2024-07-01
 */
public class MultipleReplaySubTask implements ReplaySubTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultipleReplaySubTask.class);

    private final List<MultiReplayThread> multiReplayThreads = new ArrayList<>();
    private final ReplaySqlOperator replaySqlOperator;
    private final ReplayConfig replayConfig;

    /**
     * constructor
     *
     * @param replayConfig replayConfig
     */
    public MultipleReplaySubTask(ReplayConfig replayConfig) {
        this.replayConfig = replayConfig;
        this.replaySqlOperator = new ReplaySqlOperator(replayConfig);
    }

    @Override
    public void init() {
        for (int i = 0; i < replayConfig.getMultiple(); i++) {
            MultiReplayThread thread = new MultiReplayThread(i, replayConfig);
            multiReplayThreads.add(thread);
            thread.start();
        }
    }

    @Override
    public void distribute(SqlModel sqlModel) {
        ProcessModel processModel = ProcessModel.getInstance();
        MultipleThreadModel multipleThreadModel = MultipleThreadModel.getInstance();
        if (processModel.isReadFinish() && sqlModel.getId() == processModel.getSqlCount()) {
            processModel.setAssignFinish();
        }
        if (!replaySqlOperator.isSkipSql(sqlModel)) {
            multipleThreadModel.setLastSqlId(sqlModel.getId());
            multiReplayThreads.forEach(thread -> {
                thread.addSqlModel(sqlModel);
            });
        } else {
            processModel.incrementSkipCount();
        }
        if (processModel.isReadFinish()) {
            while (multipleThreadModel.getFinishThreadCount() < replayConfig.getMultiple()) {
                try {
                    LOGGER.info("replay thread has finished:{}", false);
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LOGGER.error("00000000000");
                }
            }
            processModel.setReplayFinish();
        }
    }
}
