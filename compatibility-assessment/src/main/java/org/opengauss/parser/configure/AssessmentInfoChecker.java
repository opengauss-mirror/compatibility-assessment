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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Description: check Assessment configuration info
 *
 * @author jianghongbo
 * @since 2023/7/18
 */
public class AssessmentInfoChecker {
    /**
     * assessmenttype object
     */
    public static final String ASSESSMENT_OBJECT = "OBJECT";

    /**
     * assessmenttype both object and sql
     */
    public static final String ASSESSMENT_ALL = "ALL";

    /**
     * assessmenttype sql
     */
    public static final String ASSESSMENT_SQL = "SQL";

    /**
     * sqltype slow
     */
    public static final String SLOW_SQL = "SLOW";

    /**
     * sqltype general
     */
    public static final String GENERAL_SQL = "GENERAL";

    /**
     * db mysql
     */
    public static final String MYSQL = "mysql";

    /**
     * db opengauss
     */
    public static final String OPENGAUSS = "opengauss";

    /**
     * property key assessmenttype
     */
    public static final String ASSESSMENTTYPE = "assessmenttype";

    /**
     * property key sqltype
     */
    public static final String SQLTYPE = "sqltype";

    /**
     * property key osuser
     */
    public static final String OSUSER = "osuser";

    /**
     * property key ospassword
     */
    public static final String OSPASSWORD = "ospassword";

    /**
     * property key filedir
     */
    public static final String FILEDIR = "filedir";

    /**
     * property key user
     */
    public static final String USER = "user";

    /**
     * property key password
     */
    public static final String PASSWORD = "password";

    /**
     * property key host
     */
    public static final String HOST = "host";

    /**
     * property key port
     */
    public static final String PORT = "port";

    /**
     * property key dbname
     */
    public static final String DBNAME = "dbname";

    /**
     * mysql jdbc url param
     */
    public static final String JDBCPARAM = "jdbcparam";

    /**
     * plugin create wait time
     */
    public static final String PLUGIN_WAITTIME = "plugin.createtime";
    private static final Set<String> dbinfo = new HashSet<>(
            Arrays.asList(USER, PASSWORD, HOST, PORT, DBNAME));
    private static final Logger LOGGER = LoggerFactory.getLogger(AssessmentInfoChecker.class);

    /**
     * check Assessment configuration info
     *
     * @param props Properties
     */
    public static void checkAssessmentInfo(Properties props) {
        checkDatafromCollect(props);
        checkDatafromFile(props);
        checkDolphin(props);
        checkOgInfo(props);
    }

    private static void checkMysqlInfo(Properties props) {
        for (String field : dbinfo) {
            if (props.getProperty(MYSQL + "." + field) == null
                    || props.getProperty(MYSQL + "." + field).length() == 0) {
                LOGGER.error("mysql properties is wrong, please check configure file.");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                        "mysql properties is wrong.");
            }
        }
    }

    private static void checkOgInfo(Properties props) {
        for (String field : dbinfo) {
            if (DBNAME.equals(field)) {
                continue;
            }
            if (props.getProperty(OPENGAUSS + "." + field) == null
                    || props.getProperty(OPENGAUSS + "." + field).length() == 0) {
                LOGGER.error("opengauss properties is wrong, please check configure file.");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                        "opengauss properties is wrong.");
            }
        }
    }

    private static void checkDatafromCollect(Properties props) {
        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_COLLECT)) {
            String assessmentType = props.getProperty(ASSESSMENTTYPE);

            if (!ASSESSMENT_ALL.equalsIgnoreCase(assessmentType)
                    && !ASSESSMENT_OBJECT.equalsIgnoreCase(assessmentType)
                    && !ASSESSMENT_SQL.equalsIgnoreCase(assessmentType)) {
                LOGGER.error("assessmenttype is wrong value, please check configure file.");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                        "assessmenttype is wrong value, please check configure file.");
            }
            if (!ASSESSMENT_OBJECT.equalsIgnoreCase(assessmentType)) {
                String sqlType = props.getProperty(SQLTYPE);
                if (!SLOW_SQL.equalsIgnoreCase(sqlType) && !GENERAL_SQL.equalsIgnoreCase(sqlType)) {
                    LOGGER.error("sqltype is wrong value, please check configure file.");
                    throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                            "sqltype is wrong value, please check configure file.");
                }
            }
            checkMysqlInfo(props);
        }
    }


    private static void checkDatafromFile(Properties props) {
        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
            String fileDir = props.getProperty(FILEDIR);
            if (fileDir == null || fileDir.length() == 0) {
                LOGGER.error("filedir is wrong value, please check configure file.");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                        "filedir is wrong value, please check configure file.");
            }
            File file = new File(fileDir);
            if (!file.exists() || !file.isDirectory()) {
                LOGGER.error("filedir does not exists or not a dictionary.");
                throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                        "filedir does not exists or not a dictionary.");
            }
        }
    }

    private static void checkDolphin(Properties props) {
        String evalDbname = props.getProperty(OPENGAUSS + ".dbname");
        String osUser = props.getProperty(OSUSER);
        String osPassword = props.getProperty(OSPASSWORD);
        if (evalDbname == null && (osUser == null || osPassword == null)) {
            LOGGER.error("opengauss.dbname or (osuser and ospassword) is required, please check configure file.");
            throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.CONFIGUREEXCEPTION_CODE,
                    "opengauss.dbname or (osuser and ospassword) is required, please check configure file.");
        }
    }
}
