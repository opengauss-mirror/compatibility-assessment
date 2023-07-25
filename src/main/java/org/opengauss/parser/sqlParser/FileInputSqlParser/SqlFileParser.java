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
import java.util.List;

/**
 * Description: parse sql statement from sql file
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class SqlFileParser extends FileInputSqlParser {
    /**
     * sql file suffix
     */
    public static final String SQLFILE_SUFFIX = ".sql";
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlFileParser.class);

    private List<File> files;

    /**
     * Constructor
     *
     * @param files List<File>
     */
    public SqlFileParser(List<File> files) {
        this.files = files;
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
        for (File file : files) {
            parseSql(file);
        }
    }

    /**
     * parse sql from file
     *
     * @param file File
     */
    protected void parseSql(File file) {
        String line = "";
        StringBuilder builder = new StringBuilder();
        File newFile = getOutputFile(file, outputDir);
        if (!FilesOperation.isCreateOutputFile(newFile, outputDir)) {
            LOGGER.warn("create outputFile failed, it may already exists! inputfile: " + file.getAbsolutePath());
        }
        try (BufferedReader bufReader = FilesOperation.getBufferedReader(file);
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(newFile, false)) {
            while ((line = bufReader.readLine()) != null) {
                builder.append(line + System.lineSeparator());
            }
            SqlParseController.writeSqlToFile(newFile.getName(), bufWriter, builder);
        } catch (IOException exp) {
            LOGGER.error("parsing sql file occur IOException. inputFile: " + file.getName());
        }
    }
}
