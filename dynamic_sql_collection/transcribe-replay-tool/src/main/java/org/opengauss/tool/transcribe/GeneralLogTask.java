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

package org.opengauss.tool.transcribe;

import org.opengauss.tool.config.transcribe.TranscribeConfig;
import org.opengauss.tool.parse.object.SqlInfo;
import org.opengauss.tool.utils.ConnectionFactory;
import org.opengauss.tool.utils.DatabaseOperator;
import org.opengauss.tool.utils.FileUtils;
import org.opengauss.tool.utils.ThreadExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Description: General log task
 *
 * @author wangzhengyuan
 * @since 2024/06/26
 **/
public class GeneralLogTask extends TranscribeTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralLogTask.class);
    private static final String QUERY_SQL = "select thread_id, command_type, argument from mysql.general_log where "
            + "command_type in ('Query', 'Execute', 'Connect') and event_time >= ? order by event_time limit ?, ?";

    private DatabaseOperator mysqlOperator;
    private Connection sourceConnection;
    private int sqlLimit;
    private long sqlId;
    private String username;
    private String sessionId;
    private String schema;

    /**
     * Constructor
     *
     * @param config TranscribeConfig the config
     */
    public GeneralLogTask(TranscribeConfig config) {
        super(config);
        mysqlOperator = new DatabaseOperator(config.getMysqlConfig(), ConnectionFactory.MYSQL);
        buildQueryConnection();
    }

    private void buildQueryConnection() {
        this.sqlLimit = config.getSqlBatch();
        this.sourceConnection = mysqlOperator.createConnection();
    }

    @Override
    public void start() {
        threadPool.execute(this::querySql);
    }

    private void querySql() {
        Thread.currentThread().setUncaughtExceptionHandler(new ThreadExceptionHandler());
        startTime = LocalDateTime.now();
        mysqlOperator.refreshConnection();
        PreparedStatement ps;
        try {
            ps = sourceConnection.prepareStatement(QUERY_SQL);
            ps.setString(1, config.getStartTime());
            ps.setInt(3, sqlLimit);
        } catch (SQLException e) {
            LOGGER.error("SQLException occurred while creating query PreparedStatement.error message is: {}",
                    e.getMessage());
            return;
        }
        ResultSet res;
        List<SqlInfo> sqlList = new ArrayList<>();
        int count = 0;
        long startIndex = 0L;
        while (true) {
            try {
                ps.setLong(2, startIndex);
                res = ps.executeQuery();
                if (res == null) {
                    break;
                }
                String commandType;
                while (res.next()) {
                    count++;
                    commandType = res.getString("command_type");
                    if ("Connect".equalsIgnoreCase(commandType)) {
                        refreshConnectionInfo(res);
                    }
                    if ("Query".equalsIgnoreCase(commandType) || "Execute".equalsIgnoreCase(commandType)) {
                        SqlInfo sql = buildSql(res);
                        if (sql != null) {
                            sqlList.add(sql);
                        }
                    }
                }
                storageSql(sqlList);
                sqlList.clear();
                if (count < sqlLimit || sqlId >= 100) {
                    break;
                }
                startIndex += count;
                count = 0;
            } catch (SQLException e) {
                DatabaseOperator.closeStatement(ps);
                LOGGER.error("SQLException occurred while querying general sql, error message is: {}.", e.getMessage());
                return;
            }
        }
        sqlList.add(new SqlInfo(sqlId + 1, false, "finished"));
        storageSql(sqlList);
        sqlList.clear();
        FileUtils.createFile(config.getFileConfig().getFilePath() + "parseEndFile");
        DatabaseOperator.closeStatement(ps);
        DatabaseOperator.closeResultSet(res);
        DatabaseOperator.closeConnection(sourceConnection);
        stat();
        threadPool.shutdown();
    }

    @Override
    public void stat() {
        LocalDateTime curr = LocalDateTime.now();
        String table = "       ";
        String line = System.lineSeparator();
        String start = TIME_FORMATTER.format(startTime);
        String current = TIME_FORMATTER.format(curr);
        long duration = curr.toEpochSecond(ZoneOffset.UTC) - startTime.toEpochSecond(ZoneOffset.UTC);
        String res = String.format("%s Start Time: %s%s%s End Time: %s%s%s Duration: %s seconds%s%s SQL Count: %s",
                table, start, line, table, current, line, table, duration, line, table, sqlId);
        LOGGER.info("Transcribe finished, the statistical results are as follows:{}{}", line, res);
    }

    private void storageSql(List<SqlInfo> sqlList) {
        if (sqlList.isEmpty()) {
            return;
        }
        if (config.isWriteToFile()) {
            fileOperator.writeSqlToFile(sqlList, false);
            LOGGER.info("Commit {} sql to file.", sqlList.size());
        } else {
            opengaussOperator.insertSqlToDatabase(sqlList, false);
        }
        FileUtils.write2File(String.valueOf(sqlId), processPath);
    }

    private SqlInfo buildSql(ResultSet res) throws SQLException {
        String argument = res.getString("argument");
        if (sessionId != null) {
            SqlInfo sql = new SqlInfo(++sqlId, false, argument);
            sql.setSessionId(sessionId);
            sql.setUsername(username);
            sql.setSchema(schema);
            return sql;
        }
        return null;
    }

    private void refreshConnectionInfo(ResultSet res) throws SQLException {
        this.sessionId = res.getString("thread_id");
        String argument = res.getString("argument");
        this.username = refreshUsername(argument);
        this.schema = refreshSchema(argument);
    }

    private String refreshSchema(String argument) {
        return argument.split(" ")[2];
    }

    private String refreshUsername(String argument) {
        return argument.split("@")[0];
    }
}
