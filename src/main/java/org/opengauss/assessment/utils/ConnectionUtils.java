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
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * Close jdbc connection.
     *
     * @param connection : jdbc connection
     */
    public static void closeConnection(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
