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

package org.opengauss.tool.utils;

import com.alibaba.fastjson.JSON;

import org.opengauss.tool.replay.model.FailSqlModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;

/**
 * FailSqlFileUtils
 *
 * @since 2025-02-10
 */

public class FailSqlFileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailSqlFileUtils.class);
    private static final int FAIL_SQL_FILE_SIZE = 100;
    private static final String FAIL_SQL_NAME = "fail_sql_%s.json";
    private static final String OBJECT_SEPARATOR = "<<<END_OF_OBJECT>>>";
    private static int failSqlLogNumbers = 0;
    private static int failSqlFileNumbers = 0;
    private static String failSqlFilePath;

    /**
     * write fail sql log entities to alert file
     *
     * @param sqlModel log entity list
     */
    public static synchronized void writeFailLogsToFile(FailSqlModel sqlModel) {
        generateFailSqlFilePath();
        failSqlLogNumbers += 1;
        writeLogToFile(sqlModel);
    }

    private static void generateFailSqlFilePath() {
        if (failSqlLogNumbers / FAIL_SQL_FILE_SIZE + 1 > failSqlFileNumbers) {
            failSqlFileNumbers = failSqlLogNumbers / FAIL_SQL_FILE_SIZE + 1;
            String jarPath = FileUtils.getJarPath();
            String failPath = jarPath + "/" + FAIL_SQL_NAME;
            failSqlFilePath = String.format(failPath, failSqlFileNumbers);
            LOGGER.debug("Generate a new failSql file.");
        }
    }

    private static void writeLogToFile(FailSqlModel sqlModel) {
        BufferedWriter writer = null;
        try (RandomAccessFile raf = new RandomAccessFile(failSqlFilePath, "rw");
            FileChannel channel = raf.getChannel();
            FileLock lock = channel.lock()) {
            raf.seek(raf.length());
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                raf.getFD()), StandardCharsets.UTF_8));
            String jsonString = JSON.toJSONString(sqlModel, true);
            writer.write(jsonString);
            writer.write(OBJECT_SEPARATOR);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            LOGGER.error("Failed to write failSql logs to file", e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to close writer", e);
                }
            }
        }
    }
}
