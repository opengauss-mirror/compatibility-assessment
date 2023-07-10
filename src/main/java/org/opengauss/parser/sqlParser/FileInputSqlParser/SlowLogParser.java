/*
 * Copyright (c) 2023-2023 Huawei Technologies Co.,Ltd.
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

package org.opengauss.parser.sqlparser.fileinputsqlparser;

import org.opengauss.parser.FilesOperation;
import org.opengauss.parser.configure.RegixInfoManager;
import org.opengauss.parser.sqlparser.SqlParseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Description: parse sql statement from slow log
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class SlowLogParser extends FileInputSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlowLogParser.class);
    private static final String REGIX_PATTERN = RegixInfoManager.getSlowlogRegix();
    private static final Pattern PATTERN = Pattern.compile(REGIX_PATTERN);
    private static final String SET_TIMESTAMP = "SET timestamp";

    private File file;

    /**
     * Constructor
     */
    public SlowLogParser() {
    }

    /**
     * Constructor
     *
     * @param file File
     */
    public SlowLogParser(File file) {
        this.file = file;
    }

    /**
     * implement the run method in Runnable
     */
    @Override
    public void run() {
        parseSql();
    }

    /**
     * parse sql from single slow log file
     */
    public void parseSql() {
        if (this.file == null) {
            LOGGER.error("slowlogParser: file is null");
            return;
        }
        parseSql(file);
    }

    /**
     * parse sql by file
     *
     * @param file File
     */
    protected void parseSql(File file) {
        String line;
        StringBuilder builder = new StringBuilder();
        File newFile = getOutputFile(file, outputDir);
        if (!FilesOperation.isCreateOutputFile(newFile, outputDir)) {
            LOGGER.warn("create outputFile failed, it may already exists! inputfile: " + file.getAbsolutePath());
        }
        try (BufferedReader bufReader = FilesOperation.getBufferedReader(file);
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(newFile, false)) {
            while ((line = bufReader.readLine()) != null) {
                readBeforeSql(line, bufReader, builder);
            }
            SqlParseController.writeSqlToFile(newFile.getName(), bufWriter, builder);
        } catch (IOException exp) {
            LOGGER.error("IOException occured!", exp);
        }
    }

    private void readBeforeSql(String record, BufferedReader bufReader, StringBuilder builder) throws IOException {
        String line = record;
        if (isNewRecord(line)) {
            while ((line = bufReader.readLine()) != null) {
                if (line.startsWith(SET_TIMESTAMP)) {
                    break;
                }
            }
            readCompleteSql(bufReader, builder);
        }
    }

    private void readCompleteSql(BufferedReader bufReader, StringBuilder builder) throws IOException {
        String sql = "";
        String line;
        while ((line = bufReader.readLine()) != null) {
            sql = (sql == "") ? line : (sql.trim() + " " + line);
            if (line.endsWith(";")) {
                if (SqlParseController.isNeedFormat(sql)) {
                    builder.append(SqlParseController.format(sql));
                } else {
                    builder.append(sql.replaceAll(SqlParseController.REPLACEBLANK, " ") + System.lineSeparator());
                }
                break;
            }
        }
    }

    private boolean isNewRecord(String line) {
        return PATTERN.matcher(line).find();
    }
}
