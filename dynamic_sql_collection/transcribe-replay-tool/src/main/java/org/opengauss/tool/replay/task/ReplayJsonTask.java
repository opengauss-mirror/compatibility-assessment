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
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ReplayJsonTask
 *
 * @since 2024-07-01
 */
public class ReplayJsonTask extends ReplayMainTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayJsonTask.class);
    private static final BlockingQueue<List<SqlModel>> sqlModelListQueue = new LinkedBlockingQueue<>(5);
    private static final int MIN_FREE_MEMORY = 512000;
    private static final int SLEEP_MILLIS = 1000;
    private static final int MAX_RETRY_COUNT = 30;

    private final AtomicBoolean isFileParseEnd;
    private final ReplayConfig replayConfig;
    private final ProcessModel processModel;

    /**
     * constructor
     *
     * @param replayConfig replayConfig
     */
    public ReplayJsonTask(ReplayConfig replayConfig) {
        super(replayConfig);
        this.replayConfig = replayConfig;
        this.processModel = ProcessModel.getInstance();
        this.isFileParseEnd = new AtomicBoolean(false);
    }

    @Override
    public void replay() {
        try {
            Thread parseThread = new Thread(this::parseFile);
            parseThread.setUncaughtExceptionHandler((t, e) -> LOGGER.error("An uncaught exception occurred in "
                    + "parseThread " + t.getName() + e.getMessage()));
            parseThread.start();
            while (!isFileParseEnd.get() || !sqlModelListQueue.isEmpty()) {
                List<SqlModel> sqlModelList = sqlModelListQueue.poll(1, TimeUnit.SECONDS);
                if (sqlModelList == null) {
                    continue;
                }
                processModel.addSqlCount(sqlModelList.size());
                for (int i = 0; i < sqlModelList.size(); i++) {
                    if (isFileParseEnd.get() && sqlModelListQueue.isEmpty() && i == sqlModelList.size() - 1) {
                        processModel.setReadFinish();
                    }
                    replaySql(sqlModelList.get(i));
                }
            }
            LOGGER.info("parse thread will be interrupted");
            parseThread.interrupt();
        } catch (InterruptedException e) {
            LOGGER.error("replay sql from json has occurred an interruptedException, error message:{}", e.getMessage());
        }
    }

    private void parseFile() {
        int point = 0;
        long startTimeMillis = System.currentTimeMillis();
        while (true) {
            int fileCount = FileUtils.getFileCount(replayConfig);
            if (fileCount == 0 && !FileUtils.isFinished(replayConfig.getFileCatalogue())) {
                sleep(1000);
                continue;
            }
            if (FileUtils.isFinished(replayConfig.getFileCatalogue())) {
                pushQueue(fileCount, point);
                isFileParseEnd.set(true);
                break;
            }

            pushQueue(fileCount - 1, point);
            point = fileCount - 1;
            int currentFileCount = FileUtils.getFileCount(replayConfig);
            if (currentFileCount == fileCount && !FileUtils.isFinished(replayConfig.getFileCatalogue())) {
                long replayTime = (System.currentTimeMillis() - startTimeMillis) / 60000;
                if (replayConfig.getReplayMaxTime() > 0 && replayTime >= replayConfig.getReplayMaxTime()) {
                    pushQueue(fileCount, point);
                    isFileParseEnd.set(true);
                    break;
                }
            }
        }
    }

    private void pushQueue(int fileCount, int point) {
        String fileTemplate = replayConfig.getFileCatalogue() + File.separator + replayConfig.getFileName()
                + "-" + "%d" + ".json";
        Runtime runtime = Runtime.getRuntime();
        try {
            for (int order = point + 1; order <= fileCount; order++) {
                String filePath = String.format(fileTemplate, order);
                int waitCount = 0;
                while (runtime.freeMemory() < MIN_FREE_MEMORY) {
                    if (waitCount >= MAX_RETRY_COUNT) {
                        System.exit(-1);
                    }
                    LOGGER.warn("============runtime.freeMemory() < 512000===============");
                    Thread.sleep(SLEEP_MILLIS);
                    waitCount++;
                }
                List<List<SqlModel>> sqlModelLists = FileUtils.parseFile(filePath);
                for (List<SqlModel> sqlModelList : sqlModelLists) {
                    if (sqlModelList.isEmpty()) {
                        continue;
                    }
                    sqlModelListQueue.put(sqlModelList);
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("parse sql from json has occurred an interruptedException, error message:{}", e.getMessage());
        }
    }
}
