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
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Description: parse sql statement from general log
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class GeneralLogParser extends FileInputSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralLogParser.class);
    private static final String REGIX_PATTERN = RegixInfoManager.getGenlogRegix();
    private static final Pattern PATTERN = Pattern.compile(REGIX_PATTERN);
    private static final Integer IDCOMMAND_INDEX = 1;
    private static final Integer ARGUMENT_INDEX = 2;
    private static final Integer COMMAND_SUBINDEX = 1;
    private static final Integer IDCOMMAND_COL_NUM = 2;
    private static final List<String> COMMAND_LIST = new ArrayList<>() {
        {
            add("Query");
            add("Execute");
        }
    };

    private File file;
    private int pos = 1;
    private int sqlFileNums = 0;

    /**
     * Constructor
     */
    public GeneralLogParser() {
    }

    /**
     * Constructor
     *
     * @param file File
     */
    public GeneralLogParser(File file) {
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
     * parse sql from single general log file
     */
    public void parseSql() {
        if (this.file == null) {
            LOGGER.error("generallogParser: file is null");
            return;
        }
        parseSql(this.file);
    }

    /**
     * parse sql from file
     *
     * @param file File
     */
    protected void parseSql(File file) {
        String line;
        String completeSql = "";
        StringBuilder builder = new StringBuilder();
        File newFile = getOutputFile(file, outputDir, FileInputSqlParser.GENERAL_CODE);
        if (!FilesOperation.isCreateOutputFile(newFile, outputDir)) {
            LOGGER.warn("create outputFile failed, it may already exists! inputfile: " + file.getAbsolutePath());
        }
        try (BufferedReader bufReader = FilesOperation.getBufferedReader(file);
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(newFile, false)) {
            while ((line = bufReader.readLine()) != null) {
                if (isNewRecordLine(line)) {
                    completeSql = writeAndUpdateSql(line, completeSql, builder);
                } else {
                    if (!("".equals(completeSql))) {
                        completeSql = completeSql.trim() + " " + line;
                        sqlFileNums++;
                    } else {
                        pos++;
                    }
                }
            }

            writeLastRecord(completeSql, builder);
            SqlParseController.writeSqlToFile(newFile.getName(), bufWriter, builder);
        } catch (IOException exp) {
            handleFileLockWhenExp(newFile.getName());
            LOGGER.error("parse general log occur IOException. filename: " + file.getName(), exp);
        }
    }

    private String writeAndUpdateSql(String line, String sql, StringBuilder builder) {
        String completeSql = sql;
        String[] lineElems = line.split("\t");
        String commandType = lineElems[IDCOMMAND_INDEX].trim().split(" ", IDCOMMAND_COL_NUM)[COMMAND_SUBINDEX];
        if (COMMAND_LIST.contains(commandType)) {
            if (!("".equals(completeSql))) {
                SqlParseController.appendJsonLine(builder, pos, completeSql);
                pos += sqlFileNums;
            }
            sqlFileNums = 1;
            completeSql = lineElems[ARGUMENT_INDEX];
        } else {
            sqlFileNums++;
        }
        return completeSql;
    }

    private static boolean isNewRecordLine(String line) {
        return PATTERN.matcher(line).find();
    }

    private void writeLastRecord(String sql, StringBuilder builder) {
        if (!("".equals(sql))) {
            SqlParseController.appendJsonLine(builder, pos, sql);
        }
    }
}
