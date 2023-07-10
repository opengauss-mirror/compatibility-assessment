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
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.opengauss.parser.sqlparser.SqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Description: base class for all type files parser
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public abstract class FileInputSqlParser implements SqlParser, Runnable {
    /**
     * sql output dir path
     */
    protected static String outputDir = AssessmentInfoManager.getInstance().getSqlOutDir();
    private static final Logger LOGGER = LoggerFactory.getLogger(FileInputSqlParser.class);
    private static final String DOTS = "\\.";

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
     * @return File
     */
    protected File getOutputFile(File inputFile, String outputDir) {
        File newFile = null;
        try {
            String fileName = inputFile.getName();
            String newFilename = outputDir + "/" + fileName.replaceAll(DOTS
                    + FilenameUtils.getExtension(inputFile.getCanonicalPath()), SqlFileParser.SQLFILE_SUFFIX);
            newFile = new File(newFilename);
        } catch (IOException exp) {
            LOGGER.error("create outputfile file occur IOException, inputFile: %s", inputFile.getName());
        }
        return newFile;
    }
}
