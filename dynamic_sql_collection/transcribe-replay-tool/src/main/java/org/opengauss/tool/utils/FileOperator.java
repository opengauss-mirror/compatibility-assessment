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

import lombok.Data;
import org.opengauss.tool.Starter;
import org.opengauss.tool.config.FileConfig;
import org.opengauss.tool.parse.object.SessionInfo;
import org.opengauss.tool.parse.object.SqlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

/**
 * Description: File operator
 *
 * @author wangzhengyuan
 * @since 2024/06/26
 **/
@Data
public final class FileOperator {
    /**
     * current path
     */
    public static final String CURRENT_PATH;
    private static final Logger LOGGER = LoggerFactory.getLogger(FileOperator.class);

    static {
        CURRENT_PATH = getCurrentPath();
    }

    private FileConfig config;
    private int fileId = 1;
    private int sessionFileId = 1;
    private long sqlId;

    /**
     * Constructor
     *
     * @param config FileConfig the config
     */
    public FileOperator(FileConfig config) {
        this.config = config;
    }

    /**
     * Write sql to file
     *
     * @param sqlList List<SqlObject> the sqlList
     * @param isIncludeExecuteDuration boolean the isIncludeExecuteDuration
     */
    public void writeSqlToFile(List<SqlInfo> sqlList, boolean isIncludeExecuteDuration) {
        String fileFullPath = config.getFilePath() + config.getFileName() + "-" + fileId + ".json";
        File sqlFile = new File(fileFullPath);
        if (!sqlFile.exists()) {
            try {
                sqlFile.createNewFile();
            } catch (IOException e) {
                LOGGER.error("IOException occurred while creating sql file {}, error message is: {}.",
                        fileFullPath, e.getMessage());
            }
            writeSqlToFile(sqlList, isIncludeExecuteDuration);
        } else if (sqlFile.length() >= (long) config.getFileSize() * 1024 * 1024) {
            fileId++;
            writeSqlToFile(sqlList, isIncludeExecuteDuration);
        } else {
            writeStringToFile(sqlList, fileFullPath, isIncludeExecuteDuration);
        }
    }

    /**
     * write session to file
     *
     * @param sessionSet Set<SessionInfo> the sessionSet
     */
    public void writeSessionToFile(Set<SessionInfo> sessionSet) {
        String fileFullPath = config.getFilePath() + "session-mapping" + "-" + sessionFileId + ".json";
        File sessionFile = new File(fileFullPath);
        if (!sessionFile.exists()) {
            try {
                sessionFile.createNewFile();
            } catch (IOException e) {
                LOGGER.error("IOException occurred while creating session file {}, error message is: {}.",
                        fileFullPath, e.getMessage());
            }
            writeSessionToFile(sessionSet);
        } else if (sessionFile.length() >= (long) config.getFileSize() * 1024 * 1024) {
            sessionFileId++;
            writeSessionToFile(sessionSet);
        } else {
            writeStringToFile(sessionSet, fileFullPath);
        }
    }

    /**
     * Format file path
     *
     * @param originPath String the origin path
     * @return String the formatted file path
     */
    public static String formatFilePath(String originPath) {
        String modifiedPath;
        if (!originPath.endsWith(File.separator)) {
            modifiedPath = originPath + File.separator;
        } else {
            modifiedPath = originPath;
        }
        return modifiedPath;
    }

    /**
     * Write String to file
     *
     * @param sqlList  List<SqlObject> the sqlList
     * @param filePath String the file path
     * @param isExecuteDuration boolean the isExecuteDuration
     */
    public void writeStringToFile(List<SqlInfo> sqlList, String filePath, boolean isExecuteDuration) {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filePath, true), StandardCharsets.UTF_8))) {
            for (SqlInfo content : sqlList) {
                content.setSqlId(++sqlId);
                writer.write(content.format(isExecuteDuration));
                writer.newLine();
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred during SQL statement writing to file, error message is: {}",
                    exception.getMessage());
        }
    }

    /**
     * write string to file
     *
     * @param sessionSet Set<SessionInfo> the sessionSet
     * @param filePath string the file path
     */
    public void writeStringToFile(Set<SessionInfo> sessionSet, String filePath) {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filePath, true), StandardCharsets.UTF_8))) {
            for (SessionInfo content : sessionSet) {
                writer.write(content.toString());
                writer.newLine();
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred during session information writing to file, error message is: {}",
                    exception.getMessage());
        }
    }

    /**
     * send finished flag
     */
    public void sendFinishedFlag() {
        String endFilePath = config.getFilePath() + File.separator + "endFile";
        File endFile = new File(endFilePath);
        try {
            if (!endFile.exists()) {
                endFile.createNewFile();
            }
        } catch (IOException e) {
            LOGGER.error("failed to write end flag.");
        }
    }

    /**
     * Create path
     *
     * @param path String the file path
     */
    public static void createPath(String path) {
        File target = new File(path);
        if (!target.exists()) {
            try {
                Files.createDirectories(Paths.get(path));
            } catch (IOException e) {
                LOGGER.error("IOException occurred while creating directory {}, error message is: {}.",
                        path, e.getMessage());
            }
        }
    }

    /**
     * Get current class path
     *
     * @return String the class path
     */
    public static String getCurrentPath() {
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            return "C:\\";
        }
        String path = Starter.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        StringBuilder sb = new StringBuilder();
        String[] paths = path.split(File.separator);
        for (int i = 0; i < paths.length - 1; i++) {
            sb.append(paths[i]).append(File.separator);
        }
        return sb.toString();
    }

    /**
     * Get root path
     *
     * @param originPath String the origin path
     * @return String the root path
     */
    public static String getRootPath(String originPath) {
        return File.separator + originPath.split(File.separator)[1];
    }
}
