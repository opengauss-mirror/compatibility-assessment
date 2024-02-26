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

/**
 * Description: parse sql statement from dynamic collect output file
 *
 * @author jianghongbo
 * @since 2024/2/26
 */
public class DynamicCollectParser extends FileInputSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicCollectParser.class);

    private File file;

    /**
     * Constructor
     *
     * @param file File
     */
    public DynamicCollectParser(File file) {
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
     * parse sql from dynamic collect output file
     */
    public void parseSql() {
        if (this.file == null) {
            LOGGER.error("DynamicCollectParser: file is null");
            return;
        }
        parseSql(file);
    }

    /**
     * parse sql from dynamic collect output file
     *
     * @param file File
     */
    protected void parseSql(File file) {
        StringBuilder builder = new StringBuilder();
        File newFile = getOutputFile(file, outputDir, FileInputSqlParser.ATTACH_CODE);
        if (!FilesOperation.isCreateOutputFile(newFile, outputDir)) {
            LOGGER.warn("create outputFile failed, it may already exists! inputfile: " + file.getAbsolutePath());
        }
        try (BufferedReader bufReader = FilesOperation.getBufferedReader(file);
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(newFile, false)) {
            String line;
            while ((line = bufReader.readLine()) != null) {
                builder.append(line + System.lineSeparator());
            }
            SqlParseController.writeSqlToFile(newFile.getName(), bufWriter, builder);
        } catch (IOException exp) {
            handleFileLockWhenExp(newFile.getName());
            LOGGER.error("parsing sql file occur IOException. file: " + file.getName());
        }
    }
}
