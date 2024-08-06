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

package org.opengauss.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import static org.opengauss.parser.sqlparser.fileinputsqlparser.FileInputSqlParser.EMPTY_STR;
import static org.opengauss.parser.sqlparser.fileinputsqlparser.FileInputSqlParser.UNICODE_FEFF;

/**
 * Description: handle files, util class
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class FilesOperation {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilesOperation.class);

    /**
     * get buffered reader by file
     *
     * @param file File
     * @return BufferedReader
     */
    public static BufferedReader getBufferedReader(File file) {
        BufferedReader bufReader = null;
        try {
            bufReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException exp) {
            LOGGER.error("FileNotFoundException occured! file: " + file.getAbsolutePath());
        }
        return bufReader;
    }

    /**
     * get buffered writer by file
     *
     * @param file File
     * @param isAppend boolean
     * @return BufferedWriter
     */
    public static BufferedWriter getBufferedWriter(File file, boolean isAppend) {
        BufferedWriter bufWriter = null;
        try {
            bufWriter = new BufferedWriter(new FileWriter(file, isAppend));
        } catch (IOException exp) {
            LOGGER.error("IOException ouucred! file: " + file.getAbsolutePath(), exp.getMessage());
        }
        return bufWriter;
    }

    /**
     * create new output sql file
     *
     * @param newFile File
     * @param outputDir String
     * @return boolean
     */
    public static boolean isCreateOutputFile(File newFile, String outputDir) {
        boolean hasCreateStatus = false;
        File outputDirFile = new File(outputDir);
        if (!outputDirFile.exists()) {
            outputDirFile.mkdirs();
        }
        if (!newFile.exists()) {
            try {
                hasCreateStatus = newFile.createNewFile();
            } catch (IOException exp) {
                LOGGER.error("create newFile occur IOException! newfile: " + newFile.getName(), exp);
            }
        }
        return hasCreateStatus;
    }

    /**
     * clear dictionary
     *
     * @param dir File
     */
    public static void clearDir(File dir) {
        delete(dir);
    }

    /**
     * trim illegal characters like "\uFEFF"
     *
     * @param sqlLine String
     * @return boolean
     */
    public static String trimIllegalCharacter(String sqlLine) {
        if (sqlLine == null) {
            return EMPTY_STR;
        }
        String legalLine = EMPTY_STR;
        if (sqlLine.startsWith(UNICODE_FEFF) || sqlLine.endsWith(UNICODE_FEFF)) {
            legalLine = sqlLine.replace(UNICODE_FEFF, EMPTY_STR);
        }
        return legalLine;
    }

    private static void delete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File subFile : files) {
                delete(subFile);
            }
        }
        file.delete();
    }
}

