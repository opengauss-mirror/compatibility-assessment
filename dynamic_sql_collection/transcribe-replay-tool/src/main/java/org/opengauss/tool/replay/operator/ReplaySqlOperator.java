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

package org.opengauss.tool.replay.operator;

import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.factory.ReplayConnectionFactory;
import org.opengauss.tool.replay.model.ExecuteResponse;
import org.opengauss.tool.replay.model.ParamModel;
import org.opengauss.tool.replay.model.ParameterTypeEnum;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.utils.ConnectionFactory;
import org.opengauss.tool.utils.DatabaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * ReplaySqlOperator
 *
 * @since 2024-07-01
 */
public class ReplaySqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaySqlOperator.class);
    private static final Pattern DML_PATTERN = Pattern.compile("^update|select");

    private final ReplayLogOperator replayLogOperator;
    private final ReplayConfig replayConfig;
    private final SlowSqlOperator slowSqlOperator;

    /**
     * constructor
     *
     * @param replayConfig replayConfig
     */
    public ReplaySqlOperator(ReplayConfig replayConfig) {
        this.replayConfig = replayConfig;
        slowSqlOperator = new SlowSqlOperator(replayConfig);
        replayLogOperator = new ReplayLogOperator();
    }

    /**
     * execute pbe sql
     *
     * @param replayConn replayConn
     * @param sqlModel sqlModel
     * @return ExecuteResponse
     */
    public ExecuteResponse executePrepareSql(Connection replayConn, SqlModel sqlModel) {
        ExecuteResponse response = new ExecuteResponse();
        PreparedStatement preSqlStmt = null;
        try {
            preSqlStmt = replayConn.prepareStatement(sqlModel.getSql());
            for (ParamModel paramModel : sqlModel.getParameters()) {
                ParameterTypeEnum type = ParameterTypeEnum.fromTypeName(paramModel.getType());
                type.setParam(preSqlStmt, paramModel.getId(), paramModel.getValue());
            }
            if (!isDmlSql(sqlModel.getSql())) {
                preSqlStmt.execute();
            } else {
                response = execute(replayConn, sqlModel, preSqlStmt.toString());
            }
        } catch (SQLException e) {
            if (replayConfig.getTargetDbConfig().isCluster() && e.getMessage().contains("connection")) {
                // 主备倒换 重建数据库连接 重新执行当前语句
                response = reTryExecute(sqlModel, e);
            } else {
                replayLogOperator.printFailSqlLog(sqlModel, e.getMessage());
                response.setSuccess(false);
            }
        } finally {
            DatabaseOperator.closeStatement(preSqlStmt);
        }
        LOGGER.debug("end to execute sql id:{}", sqlModel.getId());
        return response;
    }

    private ExecuteResponse execute(Connection replayConn, SqlModel sqlModel, String sql)
            throws SQLException {
        String explainStr = getExplainSb(replayConn, sql);
        int lastColonsIndex = explainStr.lastIndexOf(":");
        int msIndex = explainStr.lastIndexOf("ms");
        long duration = (long) (Double.parseDouble(explainStr.substring(lastColonsIndex + 1, msIndex).trim()) * 1000);
        return getExecuteResponse(sqlModel, duration, explainStr);
    }

    private ExecuteResponse getExecuteResponse(SqlModel sqlModel, long duration,
                                               String explain) {
        ExecuteResponse response = new ExecuteResponse();
        response.setOpgsDuration(duration);
        if (isSlowSql(duration, sqlModel.getMysqlDuration())) {
            slowSqlOperator.recordSlowSql(sqlModel, duration, explain);
            response.setSlowSql(true);
            response.setSlowSqlExplain(explain);
        }
        return response;
    }

    /**
     * execute stmt sql
     *
     * @param replayConn replayConn
     * @param sqlModel sqlModel
     * @return ExecuteResponse
     */
    public ExecuteResponse executeStmtSql(Connection replayConn, SqlModel sqlModel) {
        ExecuteResponse response = new ExecuteResponse();
        Statement stmt = null;
        try {
            stmt = replayConn.createStatement();
            String sql = sqlModel.getSql();
            if (!isDmlSql(sql)) {
                stmt.execute(sql);
            } else {
                response = execute(replayConn, sqlModel, sql);
            }
            DatabaseOperator.closeStatement(stmt);
        } catch (SQLException e) {
            if (replayConfig.getTargetDbConfig().isCluster() && e.getMessage().contains("connection")) {
                // 主备倒换 重建数据库连接 重新执行当前语句
                response = reTryExecute(sqlModel, e);
            } else {
                replayLogOperator.printFailSqlLog(sqlModel, e.getMessage());
                response.setSuccess(false);
            }
        } finally {
            DatabaseOperator.closeStatement(stmt);
        }
        LOGGER.debug("end to execute sql id:{}", sqlModel.getId());
        return response;
    }

    private ExecuteResponse reTryExecute(SqlModel sqlModel, SQLException e) {
        ExecuteResponse response = new ExecuteResponse();
        Connection replayConn = ConnectionFactory.refreshClusterOpgsConnection(replayConfig.getTargetDbConfig());
        if (replayConn == null) {
            replayLogOperator.printFailSqlLog(sqlModel, e.getMessage());
            response.setSuccess(false);
            LOGGER.error("re-connect failed, please checking the status of database");
            System.exit(-1);
        }
        LOGGER.info("end to re-connect");
        String connectionKey = Thread.currentThread().getName() + sqlModel.getSchema();
        ReplayConnectionFactory.getInstance().updateOgConnectionMap(connectionKey, replayConn);
        if (sqlModel.isPrepared()) {
            response = executePrepareSql(replayConn, sqlModel);
        } else {
            response = executeStmtSql(replayConn, sqlModel);
        }
        return response;
    }

    private boolean isSlowSql(long opgsDuration, long mysqlDuration) {
        return replayConfig.getSlowSqlRule() == 1 ? opgsDuration - mysqlDuration > replayConfig.getDurationDiff()
                : opgsDuration > replayConfig.getSlowThreshold();
    }

    private String getExplainSb(Connection replayConn, String sql) throws SQLException {
        String explainSql = "EXPLAIN ANALYZE " + sql;
        Statement st = null;
        ResultSet explainRs = null;
        StringBuffer explainSb = new StringBuffer();
        try {
            st = replayConn.createStatement();
            explainRs = st.executeQuery(explainSql);
            while (explainRs.next()) {
                for (int i = 1; i <= explainRs.getMetaData().getColumnCount(); i++) {
                    explainSb.append(explainRs.getString(i));
                    explainSb.append(System.lineSeparator()).append("        ");
                }
            }
        } finally {
            DatabaseOperator.closeResultSet(explainRs);
            DatabaseOperator.closeStatement(st);
        }
        return explainSb.toString();
    }

    /**
     * get sql count
     *
     * @param conn connection
     * @param tableName tableName
     * @return sql count
     * @throws SQLException SQLException
     */
    public int getSqlCount(Connection conn, String tableName) throws SQLException {
        int rowCount = 0;
        String queryCountSql = String.format("select count(*) from %s", tableName);
        if (conn != null) {
            Statement statement = null;
            ResultSet rs = null;
            try {
                statement = conn.createStatement();
                rs = statement.executeQuery(queryCountSql);
                if (rs.next()) {
                    rowCount = rs.getInt(1);
                }
            } finally {
                DatabaseOperator.closeResultSet(rs);
                DatabaseOperator.closeStatement(statement);
            }
        }
        return rowCount;
    }

    /**
     * get sql result set
     *
     * @param conn connection
     * @param tableName tableName
     * @param pagination pagination
     * @param page page
     * @return resultSet
     * @throws SQLException SQLException
     */
    public ResultSet getSqlResultSet(Connection conn, String tableName, int pagination, int page) throws SQLException {
        int offset = (page - 1) * pagination;
        String querySql = String.format(Locale.ROOT, "select * from %s order by id limit %d offset %d",
                tableName, pagination, offset);
        ResultSet rs = null;
        if (conn != null) {
            Statement statement = conn.createStatement();
            rs = statement.executeQuery(querySql);
        }
        return rs;
    }

    /**
     * is skip sql
     *
     * @param sqlModel sqlModel
     * @return check result
     */
    public boolean isSkipSql(SqlModel sqlModel) {
        String sql = sqlModel.getSql().toLowerCase(Locale.ROOT);
        if (isInvalidSession(sqlModel) || (replayConfig.isOnlyReplayQuery() && !sqlModel.isQuery())) {
            return true;
        }
        return sql.startsWith("/*") || sql.startsWith("show ") || sql.startsWith("set")
                || sql.contains("@@session.transaction_read_only");
    }

    private boolean isInvalidSession(SqlModel sqlModel) {
        return !replayConfig.getSessionBlackList().isEmpty() && replayConfig.getSessionBlackList().stream()
                .map(session -> session.replace("[", "").replace("]", ""))
                .anyMatch(session -> sqlModel.getSession().startsWith(session))
                || !replayConfig.getSessionWhiteList().isEmpty() && replayConfig.getSessionWhiteList().stream()
                .map(session -> session.replace("[", "").replace("]", ""))
                .noneMatch(session -> sqlModel.getSession().startsWith(session));
    }

    private boolean isDmlSql(String sql) {
        return DML_PATTERN.matcher(sql.toLowerCase(Locale.ROOT)).matches();
    }
}
