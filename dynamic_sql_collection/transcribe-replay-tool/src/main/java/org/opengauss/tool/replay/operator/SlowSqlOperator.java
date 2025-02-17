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

import lombok.NoArgsConstructor;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.factory.ReplayConnectionFactory;
import org.opengauss.tool.replay.model.LrukCache;
import org.opengauss.tool.replay.model.SlowSqlModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.utils.DatabaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SlowSqlOperator
 *
 * @since 2024-07-01
 */
@NoArgsConstructor
public class SlowSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplaySqlOperator.class);
    private static final String PARAM_REGEX = "'[^']*'|\\b\\d+\\.\\d+\\b|\\b\\d+\\b";
    private static final Pattern PARAM_PATTERN = Pattern.compile(PARAM_REGEX);

    private final ReplayLogOperator replayLogOperator = new ReplayLogOperator();
    private final LrukCache<Integer, SlowSqlModel> cache = new LrukCache<>(500, 5);

    private ReplayConfig replayConfig;

    /**
     * SlowSqlOperator instructor
     *
     * @param replayConfig replayConfig
     */
    public SlowSqlOperator(ReplayConfig replayConfig) {
        this.replayConfig = replayConfig;
    }

    /**
     * createSlowTable
     */
    public void createSlowTable() {
        String dropTableSql = "drop table if exists slow_table";
        String createTableSql = "create table slow_table(uniqueCode bigint primary key, "
            + "sql text, mysql_duration bigint, opgs_duration bigint, explain text, count bigint)";
        Optional<Connection> connectionOptional = getSlowDbConnection();
        connectionOptional.ifPresent(connection -> {
            Statement statement = null;
            try {
                statement = connection.createStatement();
                statement.execute(dropTableSql);
                statement.execute(createTableSql);
            } catch (SQLException e) {
                LOGGER.error("create slow_table error, errorMsg:{}", e.getMessage());
                System.exit(-1);
            } finally {
                DatabaseOperator.closeStatement(statement);
            }
        });
    }

    /**
     * exportSlowSql to csv
     */
    public void exportSlowSql() {
        BufferedWriter fileWriter = null;
        Statement statement = null;
        ResultSet rs = null;
        Optional<Connection> connectionOptional = getSlowDbConnection();
        try {
            if (connectionOptional.isPresent()) {
                statement = connectionOptional.get().createStatement();
                rs = statement.executeQuery("select * from slow_table");
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                String csvPath = replayConfig.getCsvDir() + File.separator + "slow_" + getCurrentTime() + ".csv";
                File csvFile = new File(csvPath);
                fileWriter = new BufferedWriter(new FileWriter(csvFile));
                wirte2Csv(columnCount, fileWriter, metaData, rs);
                LOGGER.info("write to csv has finished, path:{}", csvPath);
            }
        } catch (SQLException | IOException e) {
            LOGGER.error("write to csv failed, error message:{}", e.getMessage());
        } finally {
            DatabaseOperator.closeResultSet(rs);
            DatabaseOperator.closeStatement(statement);
            try {
                if (fileWriter != null) {
                    fileWriter.close();
                }
            } catch (IOException e) {
                LOGGER.error("close resource failed, error message:{}", e.getMessage());
            }
        }
    }

    private void wirte2Csv(int columnCount, BufferedWriter fileWriter, ResultSetMetaData metaData, ResultSet rs)
        throws IOException, SQLException {
        for (int i = 1; i <= columnCount; i++) {
            fileWriter.write(metaData.getColumnName(i));
            if (i < columnCount) {
                fileWriter.write(",");
            }
        }
        fileWriter.newLine();
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String value = formatValue(rs.getString(i));
                fileWriter.write(value);
                if (i < columnCount) {
                    fileWriter.write(",");
                }
            }
            fileWriter.newLine();
        }
    }

    /**
     * record slow sql to db
     *
     * @param sqlModel sqlModel
     * @param duration opengauss duration
     * @param explain explain
     */
    public synchronized void recordSlowSql(SqlModel sqlModel, long duration, String explain) {
        Statement statement = null;
        try {
            int uniqueCode = getNormalizeSql(sqlModel).hashCode();
            Optional<Connection> connectionOptional = getSlowDbConnection();
            if (!connectionOptional.isPresent()) {
                return;
            }
            statement = connectionOptional.get().createStatement();
            Optional<SlowSqlModel> slowSqlModelOptional = cache.get(uniqueCode);
            SlowSqlModel slowSqlModel = new SlowSqlModel();
            SlowSqlModel oldSlowSqlModel = new SlowSqlModel();
            if (!slowSqlModelOptional.isPresent() && !isSlowSqlInDb(statement, uniqueCode)) {
                slowSqlModel = insertNewSlowSql(uniqueCode, sqlModel, duration, explain, statement);
            } else {
                oldSlowSqlModel = slowSqlModelOptional.isPresent()
                    ? slowSqlModelOptional.get()
                    : getOldSlowSqlModel(statement, uniqueCode);
                slowSqlModel = updateExistingSlowSql(oldSlowSqlModel, uniqueCode, sqlModel, duration, statement);
            }
            cache.put(uniqueCode, slowSqlModel);
        } catch (SQLException e) {
            LOGGER.error("record slow sql has an error, message:{}", e.getMessage());
        } finally {
            DatabaseOperator.closeStatement(statement);
        }
    }

    private Optional<Connection> getSlowDbConnection() {
        Optional<Connection> connectionOptional = ReplayConnectionFactory.getInstance()
            .getConnection(replayConfig.getTargetDbConfig(), ConfigReader.SLOW_DB, replayConfig.getSchemaMap());
        if (!connectionOptional.isPresent()) {
            LOGGER.error("Connection is not exist, please check slow_db schema");
        }
        return connectionOptional;
    }

    private String getNormalizeSql(SqlModel sqlModel) {
        return sqlModel.isPrepared() ? sqlModel.getSql() : normalizeSql(sqlModel.getSql());
    }

    private String normalizeSql(String sql) {
        String tempSql = sql;
        if (tempSql == null || tempSql.trim().isEmpty()) {
            return tempSql;
        }
        tempSql = tempSql.replaceAll("\\s+", " ").trim();
        tempSql = tempSql.toUpperCase(Locale.ROOT);
        tempSql = parameterizeSql(tempSql);
        tempSql = removeComments(tempSql);
        return tempSql;
    }

    private String parameterizeSql(String sql) {
        Matcher matcher = PARAM_PATTERN.matcher(sql);
        StringBuilder result = new StringBuilder();
        int lastIndex = 0;
        while (matcher.find()) {
            result.append(sql, lastIndex, matcher.start());
            result.append("?");
            lastIndex = matcher.end();
        }
        result.append(sql.substring(lastIndex));
        return result.toString();
    }

    private String removeComments(String sql) {
        String tempSql = sql;
        tempSql = tempSql.replaceAll("--.*", "");
        tempSql = tempSql.replaceAll("/\\*.*?\\*/", "");
        return tempSql;
    }

    private String formatValue(String originalValue) {
        if (originalValue != null) {
            String replacedValue = originalValue.replace("\"", "\"\"");
            if (replacedValue.contains(",") || replacedValue.contains("\n") || replacedValue.contains("\r")) {
                return "\"" + replacedValue + "\"";
            } else {
                return replacedValue;
            }
        } else {
            return StringUtils.EMPTY;
        }
    }

    private String getCurrentTime() {
        LocalDateTime currentTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return currentTime.format(formatter);
    }

    private boolean isSlowSqlInDb(Statement statement, int uniqueCode) throws SQLException {
        ResultSet rs = statement.executeQuery(
            String.format(Locale.ROOT, "select * from slow_table where uniqueCode=%d", uniqueCode));
        return rs.next();
    }

    private SlowSqlModel insertNewSlowSql(int uniqueCode, SqlModel sqlModel, long duration, String explain,
        Statement statement) throws SQLException {
        SlowSqlModel slowSqlModel = new SlowSqlModel();
        slowSqlModel.setMysqlDuration(sqlModel.getMysqlDuration());
        slowSqlModel.setOpgsDuration(duration);
        slowSqlModel.setCount(1);
        statement.execute(String.format(Locale.ROOT, "insert into slow_table (uniqueCode, sql, mysql_duration, "
                + "opgs_duration, explain, count) values (%d, '%s', %d, %d, E'%s', 1)", uniqueCode,
            getNormalizeSql(sqlModel), slowSqlModel.getMysqlDuration(), duration, explain.replaceAll("'", "\\\\'")));
        replayLogOperator.printSlowSqlLog(sqlModel, duration, explain);
        return slowSqlModel;
    }

    private SlowSqlModel updateExistingSlowSql(SlowSqlModel oldSlowSqlModel, int uniqueCode, SqlModel sqlModel,
        long duration, Statement statement) throws SQLException {
        long mysqlDuration = oldSlowSqlModel.getMysqlDuration();
        long opgsDuration = oldSlowSqlModel.getOpgsDuration();
        long count = oldSlowSqlModel.getCount();
        long avgMysqlDuration = (mysqlDuration * count + sqlModel.getMysqlDuration()) / (count + 1);
        long avgOpgsDuration = (opgsDuration * count + duration) / (count + 1);
        SlowSqlModel slowSqlModel = new SlowSqlModel(avgMysqlDuration, avgOpgsDuration, count + 1);
        slowSqlModel.setMysqlDuration(avgMysqlDuration);
        slowSqlModel.setOpgsDuration(avgOpgsDuration);
        slowSqlModel.setCount(count + 1);
        statement.executeUpdate(String.format(Locale.ROOT,
            "update slow_table set mysql_duration=%d, opgs_duration=%d,  count=count+1 where uniqueCode=%d",
            avgMysqlDuration, avgOpgsDuration, uniqueCode));
        return slowSqlModel;
    }

    private SlowSqlModel getOldSlowSqlModel(Statement statement, int uniqueCode) throws SQLException {
        ResultSet rs = statement.executeQuery(
            String.format(Locale.ROOT, "select * from slow_table where uniqueCode=%d", uniqueCode));
        SlowSqlModel slowSqlModel = new SlowSqlModel();
        if (rs.next()) {
            slowSqlModel.setMysqlDuration(rs.getLong("mysql_duration"));
            slowSqlModel.setOpgsDuration(rs.getLong("opgs_duration"));
            slowSqlModel.setCount(rs.getLong("count"));
        }
        return slowSqlModel;
    }
}
