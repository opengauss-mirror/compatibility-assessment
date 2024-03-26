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

package org.opengauss.assessment.utils;

import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Jdbc connection utils.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class ConnectionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionUtils.class);

    /**
     * Get jdbc connection.
     *
     * @param dbname : assessment database name.
     * @return Connection
     */
    public static Connection getConnection(String dbname) {
        String host = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                AssessmentInfoChecker.HOST);
        String port = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                AssessmentInfoChecker.PORT);
        String user = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                AssessmentInfoChecker.USER);
        String password = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                AssessmentInfoChecker.PASSWORD);
        String url = "jdbc:opengauss://" + host + ":" + port + "/" + dbname
                + "?useUnicode=false&characterEncoding=utf8&usessL=false";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            LOGGER.warn("jdbc connect failed, maybe dolphin plugin does not create complete.");
        }
        return connection;
    }
}
