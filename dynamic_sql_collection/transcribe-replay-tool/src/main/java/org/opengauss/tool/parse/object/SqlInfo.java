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

package org.opengauss.tool.parse.object;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description: Sql object
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/05
 */
@Data
public class SqlInfo {
    private static final Pattern PATTERN = Pattern.compile("\\$\\d+");
    private static final Pattern FUNCTION_PATTERN = Pattern.compile("FROM\\s+\\S+\\s*\\(");

    private long sqlId;
    private long packetId;
    private boolean isPbe;
    private String sql;
    private String sessionId;
    private List<PreparedValue> parameterList;
    private List<String> typeList;
    private int paraNum;
    private String username;
    private String schema;
    private boolean isQuery;
    private long startTime;
    private long endTime;
    private long executeDuration;
    private JSONObject json;

    /**
     * Constructor
     */
    public SqlInfo() {
        init();
    }

    /**
     * Constructor
     *
     * @param paraNum int the parameter number
     * @param sql String the sql
     */
    public SqlInfo(int paraNum, String sql) {
        init();
        this.paraNum = paraNum;
        this.sql = sql;
        if (paraNum > 0) {
            buildStandardizedPrepareSql();
            this.isPbe = true;
        }
        setQuery();
    }

    /**
     * Constructor
     *
     * @param sqlId long the sql id
     * @param isPbe Is prepared sql
     * @param sql   String the sql
     */
    public SqlInfo(long sqlId, boolean isPbe, String sql) {
        init();
        this.sqlId = sqlId;
        this.packetId = sqlId;
        this.isPbe = isPbe;
        this.sql = sql;
        setQuery();
    }

    private void init() {
        this.parameterList = new ArrayList<>();
        this.typeList = new ArrayList<>();
        this.json = new JSONObject();
    }

    /**
     * Encapsulate sql
     *
     * @param username   String the username
     * @param schema     String the schema name
     * @param sendSource String the send source
     * @param paraNum    parameter number
     */
    public void encapsulateSql(String username, String schema, String sendSource, int paraNum) {
        encapsulateSql(username, schema, sendSource);
        this.paraNum = paraNum;
    }

    public void encapsulateSql(String username, String schema, String sendSource) {
        this.username = username;
        this.schema = schema;
        this.sessionId = sendSource;
    }

    @Override
    public SqlInfo clone() {
        SqlInfo copy = new SqlInfo();
        copyToOther(copy);
        return copy;
    }

    /**
     * Copy attribute to other object
     *
     * @param copy other SqlInfo object
     */
    public void copyToOther(SqlInfo copy) {
        copy.sqlId = this.sqlId;
        copy.packetId = this.packetId;
        copy.isQuery = this.isQuery;
        copy.isPbe = this.isPbe;
        copy.sessionId = this.sessionId;
        copy.username = this.username;
        copy.schema = this.schema;
        copy.sql = this.sql;
        copy.paraNum = this.paraNum;
        copy.parameterList.addAll(this.parameterList);
        copy.startTime = startTime;
        copy.endTime = endTime;
        copy.executeDuration = this.executeDuration;
        copy.typeList.addAll(this.typeList);
    }

    /**
     * To determine if SQL is a query statement
     */
    public void setQuery() {
        String upperSql = sql.toUpperCase(Locale.ROOT);
        if (upperSql.startsWith("/*")) {
            upperSql = upperSql.substring(upperSql.indexOf("*/") + 2);
        }
        if (upperSql.startsWith("SHOW")) {
            this.isQuery = true;
            return;
        }
        if (upperSql.startsWith("SELECT")) {
            this.isQuery = isQuerySql(upperSql);
            return;
        }
        this.isQuery = false;
    }

    private boolean isQuerySql(String upperSql) {
        if (!upperSql.contains("FROM")) {
            return false;
        }
        return !isContainFunctionCall(upperSql.substring(upperSql.indexOf("FROM")));
    }

    private boolean isContainFunctionCall(String upperSql) {
        Matcher matcher = FUNCTION_PATTERN.matcher(upperSql);
        return matcher.find();
    }

    /**
     * Set sql execute duration
     *
     * @param startTime long the sql execute start time
     * @param endTime   long the sql execute end time
     */
    public void setExecuteDuration(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.executeDuration = endTime - startTime;
    }

    /**
     * Set duration
     *
     * @param endTime long the end time
     */
    public void setExecuteDuration(long endTime) {
        if (this.endTime == 0) {
            this.endTime = endTime;
            this.executeDuration = endTime - startTime;
        }
    }

    private void buildStandardizedPrepareSql() {
        if (sql == null || sql.isEmpty()) {
            return;
        }
        Matcher matcher = PATTERN.matcher(sql);
        StringBuilder result = new StringBuilder();
        int lastEnd = 0;
        while (matcher.find()) {
            result.append(sql, lastEnd, matcher.start()).append('?');
            lastEnd = matcher.end();
        }
        result.append(sql.substring(lastEnd));
        sql = result.toString();
    }

    /**
     * Format sql information
     *
     * @param isIncludeExecuteDuration true if sql information include execute duration
     * @return Formatted json string of sql information
     */
    public String format(boolean isIncludeExecuteDuration) {
        json.clear();
        json.fluentPut("id", sqlId)
                .fluentPut("packetId", packetId)
                .fluentPut("isQuery", isQuery)
                .fluentPut("isPrepared", isPbe)
                .fluentPut("session", sessionId)
                .fluentPut("username", username)
                .fluentPut("schema", schema)
                .fluentPut("sql", sql);
        JSONArray jsonArray = new JSONArray();
        for (PreparedValue param : parameterList) {
            jsonArray.fluentAdd(new JSONObject().fluentPut(param.getType(), param.getValue()));
        }
        json.fluentPut("parameters", jsonArray);
        if (isIncludeExecuteDuration) {
            json.fluentPut("startTime", startTime)
                    .fluentPut("endTime", endTime)
                    .fluentPut("executeDuration", executeDuration);
        }
        return json.toString();
    }
}
