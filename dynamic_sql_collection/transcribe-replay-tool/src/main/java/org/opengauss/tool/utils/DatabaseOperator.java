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

import lombok.Data;
import org.opengauss.tool.config.DatabaseConfig;
import org.opengauss.tool.parse.object.PreparedValue;
import org.opengauss.tool.parse.object.SessionInfo;
import org.opengauss.tool.parse.object.SqlInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

/**
 * Description: Database operator
 *
 * @author wangzhengyuan
 * @since 2024/06/05
 **/
@Data
public final class DatabaseOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseOperator.class);
    private DatabaseConfig config;
    private Connection connection;
    private String dbType;
    private String insertToSqlTable;
    private String insertToParaTable;
    private String insertToSession;
    private String queryFromGeneralLog;
    private long sqlId;

    /**
     * Constructor
     *
     * @param config DatabaseConfig the config
     * @param dbType database type
     */
    public DatabaseOperator(DatabaseConfig config, String dbType) {
        this.config = config;
        this.dbType = dbType;
        this.connection = ConnectionFactory.createConnection(config, dbType);
    }

    /**
     * Create connection
     *
     * @return Connection the connection
     */
    public Connection createConnection() {
        return ConnectionFactory.createConnection(config, dbType);
    }

    /**
     * Refresh connection
     */
    public void refreshConnection() {
        connection = ConnectionFactory.refreshConnection(connection, dbType, config);
    }

    /**
     * Initialize storage
     *
     * @param opengaussConfig     DatabaseConfig the openGauss connection config
     * @param isNeedForeignTable    boolean the need foreign table
     * @param shouldDropSameTable boolean the should drop same table
     */
    public void initStorage(DatabaseConfig opengaussConfig, boolean isNeedForeignTable, boolean shouldDropSameTable) {
        String tableName = opengaussConfig.getTableName();
        initSqlTable(tableName, "SQL", shouldDropSameTable);
        this.insertToSqlTable = String.format("insert into %s values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tableName);
        if (isNeedForeignTable) {
            initSqlTable(tableName + "_paras", "PARA", shouldDropSameTable);
            this.insertToParaTable = String.format("insert into %s values(?, ?, ?, ?)", tableName + "_paras");
            initSqlTable(tableName + "_session", "SESSION", shouldDropSameTable);
            this.insertToSession = String.format("insert into %s values(?, ?, ?)", tableName + "_session");
        }
    }

    private void initSqlTable(String tableName, String tableType, boolean shouldDropSameTable) {
        try {
            if (hasSameNameTable(tableName)) {
                if (shouldDropSameTable) {
                    dropTable(tableName);
                    createTable(tableName, tableType);
                } else {
                    LOGGER.error("The sql storage {} already exists, please change your table name to storage sql.",
                            tableName);
                    System.exit(0);
                }
            } else {
                createTable(tableName, tableType);
            }
        } catch (SQLException e) {
            LOGGER.error("Initialize sql storage failed, the message is: {}.", e.getMessage());
            System.exit(0);
        }
    }

    private void dropTable(String tableName) throws SQLException {
        String dropTable = String.format("drop table if exists %s", tableName);
        executeSql(dropTable);
    }

    private void createTable(String tableName, String tableType) throws SQLException {
        String createTable;
        if ("SQL".equals(tableType)) {
            createTable = String.format("create table %s(id bigint primary key, is_query boolean, "
                    + "is_prepared boolean, session char(100), username char(100), schema char(100), sql text, "
                    + "start_time bigint, end_time bigint, execute_duration bigint)", tableName);
        } else if ("PARA".equals(tableType)) {
            createTable = String.format("create table %s(id bigint, para_index integer, "
                    + "para_type char(10), para_value text)", tableName);
        } else {
            createTable = String.format("create table %s(session char(100), username char(100), schema char(100))",
                    tableName);
        }
        executeSql(createTable);
    }

    private void executeSql(String sql) throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute(sql);
        stmt.close();
        LOGGER.info("Execute sql '{}' successfully.", sql);
    }

    private boolean hasSameNameTable(String tableName) {
        String query = String.format("select * from pg_tables where schemaname='public' and tablename = '%s'",
                tableName);
        try (Statement stmt = connection.createStatement(); ResultSet resultSet = stmt.executeQuery(query)) {
            if (resultSet.next()) {
                return true;
            }
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while query same table, error message is: {}.", e.getMessage());
        }
        return false;
    }

    /**
     * Insert sql to database
     *
     * @param sqlList       List<SqlObject> the sqlList
     * @param isNeedParaTable boolean the need parameter table
     */
    public void insertSqlToDatabase(List<SqlInfo> sqlList, boolean isNeedParaTable) {
        PreparedStatement psPrimary = null;
        PreparedStatement psForeign = null;
        try {
            if (isNeedParaTable) {
                psForeign = connection.prepareStatement(insertToParaTable);
            }
            psPrimary = connection.prepareStatement(insertToSqlTable);
            for (SqlInfo sql : sqlList) {
                psPrimary.setLong(1, ++sqlId);
                psPrimary.setBoolean(2, sql.isQuery());
                psPrimary.setBoolean(3, sql.isPbe());
                psPrimary.setString(4, sql.getSessionId());
                psPrimary.setString(5, sql.getUsername());
                psPrimary.setString(6, sql.getSchema());
                psPrimary.setString(7, sql.getSql());
                psPrimary.setLong(8, sql.getStartTime());
                psPrimary.setLong(9, sql.getEndTime());
                psPrimary.setLong(10, sql.getExecuteDuration());
                psPrimary.executeUpdate();
                if (sql.isPbe()) {
                    int index = 0;
                    for (PreparedValue parameter : sql.getParameterList()) {
                        psForeign.setLong(1, sql.getSqlId());
                        psForeign.setInt(2, ++index);
                        psForeign.setString(3, parameter.getType());
                        psForeign.setString(4, parameter.getValue());
                        psForeign.executeUpdate();
                    }
                }
            }
            LOGGER.info("Commit {} sql to database.", sqlList.size());
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while insert sql to database, error message is {}.", e.getMessage());
        }
        closeStatement(psPrimary);
        closeStatement(psForeign);
    }

    /**
     * Add index to table
     *
     * @param tableName String the parameters table name
     */
    public void addIndexToTable(String tableName) {
        String addIndex = String.format("create index sql_paras_table_index on %s using btree(id)", tableName);
        try {
            executeSql(addIndex);
        } catch (SQLException e) {
            LOGGER.error("Failed to create index on {}.", tableName);
        }
    }

    /**
     * Insert sesssion to database
     *
     * @param sessionInfoSet Set<SessionInfo> the sessionInfoSet
     */
    public synchronized void insertSessionToDb(Set<SessionInfo> sessionInfoSet) {
        refreshConnection();
        PreparedStatement psSession = null;
        try {
            psSession = connection.prepareStatement(insertToSession);
            for (SessionInfo sessionInfo : sessionInfoSet) {
                psSession.setString(1, sessionInfo.getSessionId());
                psSession.setString(2, sessionInfo.getUsername());
                psSession.setString(3, sessionInfo.getSchema());
                psSession.execute();
            }
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while insert session information to database, error message is {}.",
                    e.getMessage());
        }
        closeStatement(psSession);
    }

    /**
     * Close database operator
     */
    public void close() {
        closeConnection(connection);
    }

    /**
     * Close prepared statement
     *
     * @param ps PreparedStatement the os
     */
    public static void closeStatement(Statement ps) {
        if (ps == null) {
            return;
        }
        try {
            ps.close();
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while closing prepared statement, error message: {}.", e.getMessage());
        }
    }

    /**
     * Close result set
     *
     * @param res ResultSet the res
     */
    public static void closeResultSet(ResultSet res) {
        if (res == null) {
            return;
        }
        try {
            res.close();
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while closing result set, error message: {}.", e.getMessage());
        }
    }

    /**
     * Close connection
     *
     * @param connection Connection the connection
     */
    public static void closeConnection(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while closing connection, error message: {}.", e.getMessage());
        }
    }
}
