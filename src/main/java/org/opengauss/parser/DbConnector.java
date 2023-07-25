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

import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Description: mysql database connector
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class DbConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbConnector.class);
    private static AssessmentInfoManager assessmentInfo = AssessmentInfoManager.getInstance();

    /**
     * get connection to mysql
     *
     * @return Connection
     */
    public static Connection getMysqlConnection() {
        String host = assessmentInfo.getProperty(AssessmentInfoChecker.MYSQL, AssessmentInfoChecker.HOST);
        String port = assessmentInfo.getProperty(AssessmentInfoChecker.MYSQL, AssessmentInfoChecker.PORT);
        String dbname = assessmentInfo.getProperty(AssessmentInfoChecker.MYSQL, AssessmentInfoChecker.DBNAME);
        String user = assessmentInfo.getProperty(AssessmentInfoChecker.MYSQL, AssessmentInfoChecker.USER);
        String password = assessmentInfo.getProperty(AssessmentInfoChecker.MYSQL, AssessmentInfoChecker.PASSWORD);
        String jdbcparam = assessmentInfo.getProperty(AssessmentInfoChecker.MYSQL, AssessmentInfoChecker.JDBCPARAM);
        String url = String.format(Locale.ROOT, "jdbc:mysql://%s:%s/%s?useSSL=false",
                host, port, dbname);
        if (jdbcparam != null && jdbcparam.length() > 0) {
            url = url.substring(0, url.length() - 12) + jdbcparam;
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException exp) {
            LOGGER.error("getMysqlConnection failed!", exp.getMessage());
        }
        return connection;
    }
}
