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

package org.opengauss.tool.utils;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.model.SqlModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * FileUtils
 *
 * @since 2024-07-01
 */
public final class FileUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);
    private static final int CHUNK_SIZE = 500;

    /**
     * get file count
     *
     * @param replayConfig replayConfig
     * @return fileCount
     */
    public static int getFileCount(ReplayConfig replayConfig) {
        File directory = new File(replayConfig.getFileCatalogue());
        if (!directory.exists() || !directory.isDirectory()) {
            LOGGER.error("file path is invalid, please check your config");
            return 0;
        }
        int count = 0;
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().startsWith(replayConfig.getFileName())
                        && file.getName().endsWith(".json")) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * parse json to sqlModel
     *
     * @param filePath filePath
     * @return List<List<SqlModel>>
     */
    public static List<List<SqlModel>> parseFile(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            LOGGER.error("json file path is empty...");
            System.exit(-1);
        }
        LOGGER.info("parse sql file:{} start", filePath);
        BufferedReader reader = null;
        List<SqlModel> sqlModels = new ArrayList<>();
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (StringUtils.isNotEmpty(line)) {
                    JSONObject jsonObject = new JSONObject(line);
                    if (jsonObject.get("sql").equals("finished")) {
                        break;
                    }
                    SqlModel sqlModel = new SqlModel(jsonObject);
                    sqlModels.add(sqlModel);
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("File not found. Error message:{}", e.getMessage());
            return new ArrayList<>();
        } catch (IOException | JSONException e) {
            LOGGER.error("Parse file failed. Error message:{}", e.getMessage());
            return new ArrayList<>();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("Close file failed. Error message:{}", e.getMessage());
                }
            }
        }
        return splitSqlModelList(sqlModels);
    }

    private static List<List<SqlModel>> splitSqlModelList(List<SqlModel> sqlModelList) {
        List<List<SqlModel>> sqlLists = new ArrayList<>();
        for (int i = 0; i < sqlModelList.size(); i += CHUNK_SIZE) {
            int toIndex = Math.min(sqlModelList.size(), i + CHUNK_SIZE);
            sqlLists.add(sqlModelList.subList(i, toIndex));
        }
        return sqlLists;
    }

    /**
     * isFinished
     *
     * @param fileCatalogue file catalogue
     * @return boolean
     */
    public static boolean isFinished(String fileCatalogue) {
        String filePath = fileCatalogue + File.separator + "endFile";
        File file = new File(filePath);
        if (file.exists()) {
            return true;
        }
        return false;
    }
}
