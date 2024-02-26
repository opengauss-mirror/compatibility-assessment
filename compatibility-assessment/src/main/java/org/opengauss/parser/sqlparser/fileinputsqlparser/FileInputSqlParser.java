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

import org.apache.commons.io.FilenameUtils;
import org.opengauss.parser.FileLocks;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.opengauss.parser.sqlparser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.ibatis.scripting.xmltags.StrategyChoice;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Description: base class for all type files parser
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public abstract class FileInputSqlParser implements SqlParser, Runnable {
    /**
     * general file code
     */
    public static final Integer GENERAL_CODE = 1;

    /**
     * slow file code
     */
    public static final Integer SLOW_CODE = 2;

    /**
     * mapper file code
     */
    public static final Integer MAPPER_CODE = 3;

    /**
     * sql file code
     */
    public static final Integer SQL_CODE = 4;

    /**
     * dynamic collect file code
     */
    public static final Integer ATTACH_CODE = 5;

    /**
     * sql output dir path
     */
    protected static String outputDir = AssessmentInfoManager.getInstance().getSqlOutDir();
    private static final Logger LOGGER = LoggerFactory.getLogger(FileInputSqlParser.class);
    private static final String DOTS = "\\.";
    private static final String BAR = "-";
    private static Map<Integer, String> codeToExtension = new HashMap<>() {
        {
            put(GENERAL_CODE, "general");
            put(SLOW_CODE, "slow");
            put(MAPPER_CODE, "mapper");
            put(SQL_CODE, "sql");
            put(ATTACH_CODE, "attach");
        }
    };

    /**
     * abstract function parse file
     */
    public abstract void parseSql();

    /**
     * abstract function parse single file
     *
     * @param file File
     */
    protected abstract void parseSql(File file);

    /**
     * create output sql file by filename and outputdir
     *
     * @param inputFile File
     * @param outputDir String
     * @param fileTypeCode Integer
     * @return File
     */
    protected File getOutputFile(File inputFile, String outputDir, Integer fileTypeCode) {
        File newFile = null;
        try {
            String fileName = inputFile.getCanonicalPath().substring(1)
                    .replaceAll(File.separator, "_");
            String newSuffix = BAR + codeToExtension.get(fileTypeCode);
            if (StrategyChoice.getStrategy() == StrategyChoice.STATEGY_JUDGE_KEYWORD) {
                newSuffix += BAR + StrategyChoice.STATEGY_JUDGE_KEYWORD;
            }

            String newFilename = outputDir + File.separator + fileName.replaceAll(DOTS
                    + FilenameUtils.getExtension(inputFile.getCanonicalPath()), newSuffix);
            newFile = new File(newFilename);
        } catch (IOException exp) {
            LOGGER.error("create outputfile file occur IOException, inputFile: %s", inputFile.getName());
        }
        return newFile;
    }

    /**
     * handle file lock if exception occured when parsing.
     *
     * @param filename String
     */
    protected void handleFileLockWhenExp(String filename) {
        if (!FileLocks.getLockers().containsKey(filename)) {
            FileLocks.addLocker(filename);
        } else {
            FileLocks.getLockers().get(filename).writeLock().unlock();
        }
    }
}
