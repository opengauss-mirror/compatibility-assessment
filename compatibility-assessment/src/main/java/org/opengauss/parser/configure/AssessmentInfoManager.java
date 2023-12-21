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

package org.opengauss.parser.configure;

import org.opengauss.parser.command.Commander;
import org.opengauss.parser.exception.SqlParseExceptionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Description: assessment configuration info manager
 *
 * @author jianghongbo
 * @since 2023/7/17
 */
public class AssessmentInfoManager implements ConfigureInfoManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(AssessmentInfoManager.class);
    private static String confFile = System.getProperty("user.dir") + File.separator + "assessment.properties";
    private static String sqlOutDir = System.getProperty("user.dir") + File.separator + "sqlFiles";
    private static Properties props;

    private int outputSqlFileCount = 0;
    private int inputFileCount = 0;
    private boolean canStartAssessment = false;

    private AssessmentInfoManager() {
        loadProperties();
    }

    private static class Inner {
        private static final AssessmentInfoManager INSTANCE = new AssessmentInfoManager();
    }

    /**
     * get instance
     *
     * @return AssessmentInfoManager
     */
    public static AssessmentInfoManager getInstance() {
        return Inner.INSTANCE;
    }

    /**
     * get property from configure file
     *
     * @param first String
     * @param second String
     * @return String
     */
    public String getProperty(String first, String second) {
        return props.getProperty(first + "." + second);
    }

    /**
     * get property from configure file by key
     *
     * @param key String
     * @return String
     */
    public String getProperty(String key) {
        return props.getProperty(key);
    }

    /**
     * get output sql path
     *
     * @return String
     */
    public static String getSqlOutDir() {
        return sqlOutDir;
    }

    /**
     * get output sql count
     *
     * @return int
     */
    public int getOutputSqlFileCount() {
        return outputSqlFileCount;
    }

    /**
     * get assessment flag
     *
     * @return boolean
     */
    public boolean getAssessmentFlag() {
        return canStartAssessment;
    }

    public void setOutputSqlFileCount(int outputSqlFileCount) {
        this.outputSqlFileCount = outputSqlFileCount;
    }

    public void setinputFileCount(int inputFileCount) {
        this.inputFileCount = inputFileCount;
    }

    /**
     * get input count
     *
     * @return int
     */
    public int getInputFileCount() {
        return this.inputFileCount;
    }

    public void setAssessmentFlag(boolean canStartAssessment) {
        this.canStartAssessment = canStartAssessment;
    }

    private void loadProperties() {
        if (Commander.getConfFile() != null) {
            confFile = Commander.getConfFile();
        }
        props = new Properties();
        try (FileInputStream input = new FileInputStream(confFile)) {
            props.load(input);
            AssessmentInfoChecker.checkAssessmentInfo(props);
            setOutputSqlFileCount(props);
        } catch (IOException exp) {
            LOGGER.error("load properties failed.", exp);
            throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.LOADFILEEXCEPTION_CODE,
                    "load property file " + confFile + " failed.");
        }
    }

    private void setOutputSqlFileCount(Properties props) {
        if (Commander.DATAFROM_COLLECT.equalsIgnoreCase(Commander.getDataSource())) {
            int count = AssessmentInfoChecker.ASSESSMENT_ALL.equalsIgnoreCase(
                    props.getProperty(AssessmentInfoChecker.ASSESSMENTTYPE)) ? 2 : 1;
            outputSqlFileCount = count;
            canStartAssessment = true;
        }
    }
}
