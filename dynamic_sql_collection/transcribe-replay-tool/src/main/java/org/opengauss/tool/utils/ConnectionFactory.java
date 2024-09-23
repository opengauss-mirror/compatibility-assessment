/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
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

package org.opengauss.tool.utils;

import org.opengauss.tool.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Description: Config reader
 *
 * @author wangzhengyuan
 * @since 2024/06/26
 **/
public final class ConnectionFactory {
    /**
     * MySQL
     */
    public static final String MYSQL = "MySQL";

    /**
     * openGauss
     */
    public static final String OPENGAUSS = "openGauss";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactory.class);
    private static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String CLUSTER_JDBC_URL = "jdbc:postgresql://%s/%s?currentSchema=%s&targetServerType=master"
            + "&loggerLevel=error";


    private static Connection createMysqlConnection(DatabaseConfig config) {
        String url = "jdbc:mysql://" + config.getDbIp() + ":" + config.getDbPort()
                + "/" + config.getDbName() + "?useSSL=false&allowPublicKeyRetrieval=true&"
                + "rewriteBatchedStatements=true&allowLoadLocalInfile=true&serverTimezone=UTC&";
        Connection connection = null;
        try {
            Class.forName(MYSQL_JDBC_DRIVER);
            connection = DriverManager.getConnection(url, config.getUsername(), config.getPassword());
        } catch (ClassNotFoundException | SQLException exp) {
            LOGGER.error("Create MySQL connection failed, error message is: {}", exp.getMessage());
        }
        return connection;
    }

    /**
     * Refresh connection
     *
     * @param originConnection origin connection
     * @param connectionType   connection type
     * @param config           DatabaseConfig the config
     * @return Connection the connection
     */
    public static Connection refreshConnection(Connection originConnection, String connectionType,
                                               DatabaseConfig config) {
        try {
            if (originConnection.isValid(1)) {
                return originConnection;
            }
        } catch (SQLException e) {
            LOGGER.warn("Something went wrong while checking connection.", e);
        }
        if (MYSQL.equals(connectionType)) {
            return createMysqlConnection(config);
        } else {
            return createOpengaussConnection(config);
        }
    }

    /**
     * Create connection
     *
     * @param config DatabaseConfig the config
     * @param dbType database type
     * @return Connection the connection
     */
    public static Connection createConnection(DatabaseConfig config, String dbType) {
        if (MYSQL.equalsIgnoreCase(dbType)) {
            return createMysqlConnection(config);
        } else {
            return createOpengaussConnection(config);
        }
    }

    private static Connection createOpengaussConnection(DatabaseConfig config) {
        String sourceUrl = getSourceUrl(config);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(sourceUrl, config.getUsername(), config.getPassword());
        } catch (SQLException e) {
            LOGGER.error("Unable to connect to database {}:{}, error message is: {}", config.getDbIp(),
                    config.getDbPort(), e.getMessage());
            System.exit(-1);
        }
        return connection;
    }

    /**
     * refreshClusterOpgsConnection
     *
     * @param config config
     * @return Connection
     */
    public static Connection refreshClusterOpgsConnection(DatabaseConfig config) {
        String sourceUrl = getSourceUrl(config);
        Connection connection = null;
        int tryCount = 0;
        while (connection == null && tryCount < 30) {
            try {
                Thread.sleep(10000);
                LOGGER.info("try re-connect ing");
                connection = DriverManager.getConnection(sourceUrl, config.getUsername(), config.getPassword());
            } catch (SQLException | InterruptedException e) {
                LOGGER.error("Unable to connect to database {}:{}, error message is: {}", config.getDbIp(),
                        config.getDbPort(), e.getMessage());
            }
            tryCount++;
        }
        return connection;
    }

    private static String getSourceUrl(DatabaseConfig config) {
        String sourceUrl;
        String dbName = config.getDbName();
        String schema = "public";
        int pointIndex = dbName.indexOf(".");
        if (pointIndex > -1) {
            schema = dbName.split("\\.")[1];
            dbName = dbName.split("\\.")[0];
        }
        if (config.isCluster()) {
            String[] ipArray = config.getDbIp().split(",");
            String[] portArray = config.getDbPort().split(",");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ipArray.length; i++) {
                sb.append(ipArray[i]).append(":").append(portArray[i]).append(",");
            }
            sourceUrl = String.format(CLUSTER_JDBC_URL, sb.substring(0, sb.length() - 1), dbName, schema);
        } else {
            sourceUrl = "jdbc:postgresql://" + config.getDbIp() + ":" + config.getDbPort() + "/" + dbName
                    + "?currentSchema=" + schema + "&loggerLevel=error";
        }
        return sourceUrl;
    }
}
