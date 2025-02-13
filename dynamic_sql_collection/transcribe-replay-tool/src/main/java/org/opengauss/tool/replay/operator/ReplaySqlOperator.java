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

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.factory.ReplayConnectionFactory;
import org.opengauss.tool.replay.model.ExecuteResponse;
import org.opengauss.tool.replay.model.ParamModel;
import org.opengauss.tool.replay.model.ParameterTypeEnum;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.replay.model.ResultModel;
import org.opengauss.tool.utils.ConnectionFactory;
import org.opengauss.tool.utils.DatabaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * ReplaySqlOperator
 *
 * @since 2024-07-01
 */
public class ReplaySqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaySqlOperator.class);
    private static final Pattern DML_PATTERN = Pattern.compile("^(update|select|insert|delete)");

    private final ReplayLogOperator replayLogOperator;
    private final ReplayConfig replayConfig;
    private final SlowSqlOperator slowSqlOperator;
    private int resultFilePoint = 0;
    private List<ResultModel> resultModels = new ArrayList<>();
    private Map<Long, ResultModel> resultMap = new HashMap<>();

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
     * getReplayConfig
     *
     * @return ReplayConfig
     */
    public ReplayConfig getReplayConfig() {
        return replayConfig;
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
            String sql = sqlModel.getSql();
            if (isDmlSql(sql)) {
                preSqlStmt = replayConn.prepareStatement("EXPLAIN ANALYZE " + sql);
                response = executePreparedDml(sqlModel, preSqlStmt);
            } else {
                preSqlStmt = replayConn.prepareStatement(sql);
                executeDdl(sqlModel, preSqlStmt);
            }
            DatabaseOperator.closeStatement(preSqlStmt);
            if (sqlModel.isQuery() && replayConfig.isCompareResult()) {
                List<List<String>> data = getPrepareData(replayConn, sqlModel);
                compareSelectResult(sqlModel, data);
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

    private void executeDdl(SqlModel sqlModel, PreparedStatement preSqlStmt) throws SQLException {
        int paraCount = preSqlStmt.getParameterMetaData().getParameterCount();
        List<ParamModel> paraList = sqlModel.getParameters();
        if (paraCount == paraList.size()) {
            bindParameters(preSqlStmt, paraList);
            preSqlStmt.execute();
        } else {
            bindBatchParameters(paraCount, preSqlStmt, paraList);
            preSqlStmt.executeBatch();
            preSqlStmt.clearBatch();
        }
    }

    private ExecuteResponse executePreparedDml(SqlModel sqlModel, PreparedStatement preSqlStmt) throws SQLException {
        int paraCount = preSqlStmt.getParameterMetaData().getParameterCount();
        List<ParamModel> paraList = sqlModel.getParameters();
        if (paraCount == paraList.size()) {
            bindParameters(preSqlStmt, paraList);
            return execute(sqlModel, preSqlStmt);
        } else {
            return bindBatchAndExecute(paraCount, sqlModel, preSqlStmt);
        }
    }

    private void bindBatchParameters(int paraCount, PreparedStatement preSqlStmt, List<ParamModel> paraList)
        throws SQLException {
        for (int i = 0; i < paraList.size(); i++) {
            ParamModel parameter = paraList.get(i);
            ParameterTypeEnum type = ParameterTypeEnum.fromTypeName(parameter.getType());
            type.setParam(preSqlStmt, parameter.getId() % paraCount == 0 ? paraCount : parameter.getId() % paraCount,
                parameter.getValue());
            if ((i + 1) % paraCount == 0) {
                preSqlStmt.addBatch();
            }
        }
    }

    private void bindParameters(PreparedStatement preSqlStmt, List<ParamModel> paraList) throws SQLException {
        for (int i = 0; i < paraList.size(); i++) {
            ParamModel parameter = paraList.get(i);
            ParameterTypeEnum type = ParameterTypeEnum.fromTypeName(parameter.getType());
            type.setParam(preSqlStmt, parameter.getId(), parameter.getValue());
        }
    }

    private ExecuteResponse bindBatchAndExecute(int paraCount, SqlModel sqlModel, PreparedStatement preSqlStmt)
        throws SQLException {
        ExecuteResponse response = new ExecuteResponse();
        List<ParamModel> paramModels = sqlModel.getParameters();
        for (int i = 0; i < paramModels.size(); i++) {
            ParamModel parameter = paramModels.get(i);
            ParameterTypeEnum type = ParameterTypeEnum.fromTypeName(parameter.getType());
            type.setParam(preSqlStmt, parameter.getId() % paraCount == 0 ? paraCount : parameter.getId() % paraCount,
                parameter.getValue());
            if ((i + 1) % paraCount == 0) {
                executeAndRefresh(preSqlStmt, response);
            }
        }
        if (isSlowSql(response.getOpgsDuration(), sqlModel.getMysqlDuration())) {
            slowSqlOperator.recordSlowSql(sqlModel, response.getOpgsDuration(), response.getSlowSqlExplain());
            response.setSlowSql(true);
            response.setSlowSqlExplain(response.getSlowSqlExplain());
        }
        return response;
    }

    private ExecuteResponse execute(Connection replayConn, SqlModel sqlModel, String sql) throws SQLException {
        String explainStr = executeAndGetExplain(replayConn, sql);
        int lastColonsIndex = explainStr.lastIndexOf(":");
        int msIndex = explainStr.lastIndexOf("ms");
        long duration = (long) (Double.parseDouble(explainStr.substring(lastColonsIndex + 1, msIndex).trim()) * 1000);
        return getExecuteResponse(sqlModel, duration, explainStr);
    }

    private ExecuteResponse execute(SqlModel sqlModel, PreparedStatement preSqlStmt) throws SQLException {
        String explainStr = executeAndGetExplain(preSqlStmt);
        long duration = getExecuteDuration(explainStr);
        return getExecuteResponse(sqlModel, duration, explainStr);
    }

    private void executeAndRefresh(PreparedStatement preSqlStmt, ExecuteResponse response)
        throws SQLException {
        String explainStr = executeAndGetExplain(preSqlStmt);
        long duration = getExecuteDuration(explainStr);
        response.refresh(duration, explainStr);
    }

    private long getExecuteDuration(String explainStr) {
        int lastColonsIndex = explainStr.lastIndexOf(":");
        int msIndex = explainStr.lastIndexOf("ms");
        return (long) (Double.parseDouble(explainStr.substring(lastColonsIndex + 1, msIndex).trim()) * 1000);
    }

    private ExecuteResponse getExecuteResponse(SqlModel sqlModel, long duration, String explain) {
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
            if (sqlModel.isQuery() && replayConfig.isCompareResult()) {
                List<List<String>> data = getStmtData(replayConn, sqlModel.getSql());
                compareSelectResult(sqlModel, data);
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
        return replayConfig.getSlowSqlRule() == 1
            ? opgsDuration - mysqlDuration > replayConfig.getDurationDiff()
            : opgsDuration > replayConfig.getSlowThreshold();
    }

    private String executeAndGetExplain(Connection replayConn, String sql) throws SQLException {
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

    private void compareSelectResult(SqlModel sqlModel, List<List<String>> data) throws SQLException {
        if (resultFilePoint > 0 && resultMap.isEmpty()) {
            replayLogOperator.printNullDataDiffLog(sqlModel, data);
            return;
        }
        while (!resultMap.containsKey(sqlModel.getPacketId())) {
            readResultFile();
            if (resultMap.isEmpty()) {
                replayLogOperator.printNullDataDiffLog(sqlModel, data);
                return;
            }
        }

        ResultModel resultModel = resultMap.get(sqlModel.getPacketId());
        List<List<String>> sourceResult = new ArrayList<>();
        JSONArray array = resultModel.getData();
        for (int i = 0; i < array.length(); i++) {
            JSONArray rowArr = array.getJSONArray(i);
            List<String> row = new ArrayList<>();
            for (int j = 0; j < rowArr.length(); j++) {
                row.add(rowArr.get(j).toString());
            }
            sourceResult.add(row);
        }
        if (!data.equals(sourceResult)) {
            replayLogOperator.printDataDiffLog(sqlModel, sourceResult, data);
        }
    }

    private List<List<String>> getStmtData(Connection replayConn, String sql) throws SQLException {
        Statement st = null;
        ResultSet rs = null;
        List<List<String>> data = new ArrayList<>();
        try {
            st = replayConn.createStatement();
            rs = st.executeQuery(sql);
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) {
                    String str = rs.getString(i);
                    row.add(str == null ? "null" : str);
                }
                data.add(row);
            }
        } finally {
            DatabaseOperator.closeResultSet(rs);
            DatabaseOperator.closeStatement(st);
        }
        return data;
    }

    private List<List<String>> getPrepareData(Connection replayConn, SqlModel sqlModel) throws SQLException {
        PreparedStatement preSqlStmt = null;
        ResultSet rs = null;
        List<List<String>> data = new ArrayList<>();
        try {
            preSqlStmt = replayConn.prepareStatement(sqlModel.getSql());
            int paraCount = preSqlStmt.getParameterMetaData().getParameterCount();
            List<ParamModel> paraList = sqlModel.getParameters();
            if (paraCount == paraList.size()) {
                bindParameters(preSqlStmt, paraList);
            } else {
                for (int i = 0; i < paraList.size(); i++) {
                    ParamModel parameter = paraList.get(i);
                    ParameterTypeEnum type = ParameterTypeEnum.fromTypeName(parameter.getType());
                    type.setParam(preSqlStmt, parameter.getId() % paraCount == 0 ? paraCount
                                    : parameter.getId() % paraCount, parameter.getValue());
                }
            }
            rs = preSqlStmt.executeQuery();
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) {
                    String str = rs.getString(i);
                    row.add(str == null ? "null" : str);
                }
                data.add(row);
            }
        } finally {
            DatabaseOperator.closeResultSet(rs);
            DatabaseOperator.closeStatement(preSqlStmt);
        }
        return data;
    }

    private void readResultFile() {
        resultFilePoint++;
        resultModels.clear();
        String filePath = replayConfig.getSelectResultPath() + File.separator
                + replayConfig.getResultFileName() + "-" + resultFilePoint + ".json";
        LOGGER.info("read result file:{} start", filePath);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (StringUtils.isNotEmpty(line)) {
                    JSONObject jsonObject = new JSONObject(line);
                    ResultModel resultModel = new ResultModel(jsonObject);
                    resultModels.add(resultModel);
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("File not found. Error message:{}", e.getMessage());
        } catch (IOException | JSONException e) {
            LOGGER.error("read result file failed. Error message:{}", e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("Close file failed. Error message:{}", e.getMessage());
                }
            }
        }

        resultMap = resultModels.stream().collect(Collectors.toMap(ResultModel::getSqlPacketId,
                result -> result, (existing, replacement) -> existing));
    }

    private String executeAndGetExplain(PreparedStatement preparedStatement) throws SQLException {
        ResultSet explainRs = null;
        StringBuffer explainSb = new StringBuffer();
        try {
            explainRs = preparedStatement.executeQuery();
            while (explainRs.next()) {
                for (int i = 1; i <= explainRs.getMetaData().getColumnCount(); i++) {
                    explainSb.append(explainRs.getString(i));
                    explainSb.append(System.lineSeparator()).append("        ");
                }
            }
        } finally {
            DatabaseOperator.closeResultSet(explainRs);
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
     * @param point point
     * @return resultSet
     * @throws SQLException SQLException
     */
    public ResultSet getSqlResultSet(Connection conn, String tableName, int pagination, int point) throws SQLException {
        String querySql = String.format(Locale.ROOT, "select * from %s order by id limit %d offset %d",
                tableName, pagination, point);
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
        if (sqlModel.isJdbcSql()) {
            return true;
        }
        return sql.contains("mysql-connector-java") || sql.startsWith("show ") || sql.startsWith("set") || sql.contains(
            "@@session.transaction_read_only");
    }

    private boolean isInvalidSession(SqlModel sqlModel) {
        return !replayConfig.getSessionBlackList().isEmpty() && replayConfig.getSessionBlackList()
            .stream()
            .map(session -> session.replace("[", "").replace("]", ""))
            .anyMatch(session -> sqlModel.getSession().startsWith(session))
            || !replayConfig.getSessionWhiteList().isEmpty() && replayConfig.getSessionWhiteList()
            .stream()
            .map(session -> session.replace("[", "").replace("]", ""))
            .noneMatch(session -> sqlModel.getSession().startsWith(session));
    }

    private boolean isDmlSql(String sql) {
        Matcher matcher = DML_PATTERN.matcher(sql.toLowerCase(Locale.ROOT));
        return matcher.find() && matcher.start() == 0;
    }
}
