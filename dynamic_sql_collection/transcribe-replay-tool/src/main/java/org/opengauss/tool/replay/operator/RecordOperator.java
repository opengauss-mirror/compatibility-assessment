/*
 * Copyright (c) 2025-2025 Huawei Technologies Co.,Ltd.
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

package org.opengauss.tool.replay.operator;

import com.alibaba.fastjson2.JSONObject;

import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RecordOperator
 *
 * @since 2025-01-23
 */
public class RecordOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordOperator.class);
    private static final String PROCESS_FILE_NAME = "replay-process.txt";
    private static final String DURATION_FILE_NAME = "duration.json";
    private static final String BASE_PATH = FileUtils.getJarPath() + File.separator + "%s";
    private static final int RECORD_PERIOD = 5;

    private ScheduledExecutorService executorService;

    /**
     * recordSqlCount: sqlCount and replay Count
     */
    public void recordSqlCount() {
        String filePath = String.format(BASE_PATH, PROCESS_FILE_NAME);
        FileUtils.createFile(filePath);
        executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(this::recordProcess, 0, RECORD_PERIOD, TimeUnit.SECONDS);
    }

    /**
     * stop record
     */
    public void stopRecord() {
        recordProcess();
        executorService.shutdown();
    }

    private void recordProcess() {
        String filePath = String.format(BASE_PATH, PROCESS_FILE_NAME);
        ProcessModel processModel = ProcessModel.getInstance();
        FileUtils.write2File(String.valueOf(processModel.getReplayCount()), filePath);
    }

    /**
     * recordDuration: record duration of source and target
     */
    public void recordDuration() {
        String filePath = String.format(BASE_PATH, DURATION_FILE_NAME);
        FileUtils.createFile(filePath);
        JSONObject jsonObject = new JSONObject();
        ProcessModel processModel = ProcessModel.getInstance();
        jsonObject.put("source", processModel.getMysqlSeries().getItems().toString());
        jsonObject.put("target", processModel.getOpgsSeries().getItems().toString());
        FileUtils.write2File(jsonObject.toJSONString(), filePath);
    }
}
