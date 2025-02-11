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

import org.opengauss.tool.Starter;
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.utils.FailSqlFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    private static final String PROCESS_FILE_NAME = "process.json";
    private static final String DURATION_FILE_NAME = "duration.json";
    private static final int RECORD_PERIOD = 5;

    private ScheduledExecutorService executorService;

    /**
     * recordSqlCount: sqlCount and replay Count
     */
    public void recordSqlCount() {
        String filePath = String.format("%s/%s", FailSqlFileUtils.getJarPath(Starter.class), PROCESS_FILE_NAME);
        createFile(filePath);
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
        String filePath = String.format("%s/%s", FailSqlFileUtils.getJarPath(Starter.class), PROCESS_FILE_NAME);
        JSONObject processData = generateProcessData();
        write2File(processData, filePath);
    }

    private void createFile(String fileName) {
        File file = new File(fileName);
        try {
            file.createNewFile();
            LOGGER.info("file {} has been created successfully.", fileName);
        } catch (IOException ex) {
            LOGGER.error("create file {} failed, error message:{}.", fileName, ex.getMessage());
        }
    }

    private JSONObject generateProcessData() {
        JSONObject jsonObject = new JSONObject();
        ProcessModel processModel = ProcessModel.getInstance();
        jsonObject.put("parseCount", processModel.getSqlCount());
        jsonObject.put("replayCount", processModel.getReplayCount());
        return jsonObject;
    }

    private void write2File(JSONObject object, String fileName) {
        try (FileWriter writer = new FileWriter(fileName)) {
            writer.write(object.toJSONString());
        } catch (IOException ex) {
            LOGGER.error("write file {} failed, error message:{}.", fileName, ex.getMessage());
        }
    }

    /**
     * recordDuration: record duration of source and target
     */
    public void recordDuration() {
        String filePath = String.format("%s/%s", FailSqlFileUtils.getJarPath(Starter.class), DURATION_FILE_NAME);
        createFile(filePath);
        JSONObject jsonObject = new JSONObject();
        ProcessModel processModel = ProcessModel.getInstance();
        jsonObject.put("source", processModel.getMysqlSeries().getItems().toString());
        jsonObject.put("target", processModel.getOpgsSeries().getItems().toString());
        write2File(jsonObject, filePath);
    }
}
