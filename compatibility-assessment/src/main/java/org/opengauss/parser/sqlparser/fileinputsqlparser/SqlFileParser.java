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
import org.opengauss.parser.sqlparser.SqlParseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Locale;

/**
 * Description: parse sql statement from sql file
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class SqlFileParser extends FileInputSqlParser {
    /**
     * delimiter
     */
    public static final String DELIMITER = "DELIMITER";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlFileParser.class);

    private File file;

    /**
     * Constructor
     *
     * @param file File
     */
    public SqlFileParser(File file) {
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
     * parse sql from all sql files in dataDir
     */
    public void parseSql() {
        if (this.file == null) {
            LOGGER.error("sqlFileParser: file is null");
            return;
        }
        parseSql(file);
    }

    /**
     * parse sql from file
     *
     * @param file File
     */
    protected void parseSql(File file) {
        StringBuilder builder = new StringBuilder();
        File newFile = getOutputFile(file, outputDir, FileInputSqlParser.SQL_CODE);
        if (!FilesOperation.isCreateOutputFile(newFile, outputDir)) {
            LOGGER.warn("create outputFile failed, it may already exists! inputfile: " + file.getAbsolutePath());
        }
        try (BufferedReader bufReader = FilesOperation.getBufferedReader(file);
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(newFile, false)) {
            extractSql(bufReader, builder);
            SqlParseController.writeSqlToFile(newFile.getName(), bufWriter, builder);
        } catch (IOException exp) {
            handleFileLockWhenExp(newFile.getName());
            LOGGER.error("parsing sql file occur IOException. file: " + file.getName());
        }
    }

    private void extractSql(BufferedReader bufferedReader, StringBuilder builder) throws IOException {
        int pos = 1;
        int sqlLineNums = 0;
        String sqlLine;
        String delimiter = ";";
        StringBuffer buffer = new StringBuffer();
        boolean isDelimiter = false;
        while ((sqlLine = bufferedReader.readLine()) != null) {
            sqlLine = FilesOperation.trimIllegalCharacter(sqlLine.trim());

            if (sqlLine.equals("") || sqlLine.startsWith("--")) {
                if (buffer.length() == 0) {
                    pos++;
                } else {
                    sqlLineNums++;
                }
                continue;
            }

            /* The delimiter keyword has been read, but no delimiters were obtained */
            if (isDelimiter) {
                isDelimiter = false;
                delimiter = sqlLine.replace(delimiter, "").trim();
                pos++;
                /* read delimiter keyword */
            } else if (sqlLine.toUpperCase(Locale.ROOT).startsWith(DELIMITER)) {
                sqlLine = sqlLine.toUpperCase(Locale.ROOT).replace(DELIMITER, "")
                        .replace(delimiter, "").trim();
                /* no delimiters were obtained */
                if (sqlLine.equals("")) {
                    isDelimiter = true;
                } else {
                    delimiter = sqlLine;
                }
                pos++;
            } else if (!sqlLine.endsWith(delimiter)) {
                buffer.append(sqlLine);
                buffer.append(" ");
                sqlLineNums++;
            } else {
                buffer.append(sqlLine.substring(0, sqlLine.length() - delimiter.length()));
                SqlParseController.appendJsonLine(builder, pos, buffer.toString());
                buffer.setLength(0);
                pos += sqlLineNums + 1;
                sqlLineNums = 0;
            }
        }
    }
}
