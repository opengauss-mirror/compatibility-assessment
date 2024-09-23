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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Description: Sql object
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/05
 */
@Data
public class SqlInfo {
    private long sqlId;
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
        this.parameterList = new ArrayList<>();
        this.typeList = new ArrayList<>();
        this.json = new JSONObject(true);
    }

    /**
     * Constructor
     *
     * @param sqlId long the sql id
     * @param isPbe Is prepared sql
     * @param sql   String the sql
     */
    public SqlInfo(long sqlId, boolean isPbe, String sql) {
        this.parameterList = new ArrayList<>();
        this.typeList = new ArrayList<>();
        this.json = new JSONObject(true);
        this.sqlId = sqlId;
        this.isPbe = isPbe;
        this.sql = sql;
        setQuery();
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
        this.username = username;
        this.schema = schema;
        this.sessionId = sendSource;
        this.paraNum = paraNum;
    }

    @Override
    public SqlInfo clone() {
        SqlInfo copy = new SqlInfo();
        copy.sqlId = this.sqlId;
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
        return copy;
    }

    private void setQuery() {
        String upperSql = sql.toUpperCase(Locale.ROOT);
        this.isQuery = upperSql.startsWith("SELECT") || upperSql.startsWith("SHOW");
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

    public void setExecuteDuration(long endTime) {
        this.endTime = endTime;
        this.executeDuration = endTime - startTime;
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
                .fluentPut("isQuery", isQuery)
                .fluentPut("isPrepared", isPbe)
                .fluentPut("session", sessionId)
                .fluentPut("username", username)
                .fluentPut("schema", schema)
                .fluentPut("sql", sql);
        JSONArray jsonArray = new JSONArray();
        for (PreparedValue param : parameterList) {
            jsonArray.fluentAdd(new JSONObject(true).fluentPut(param.getType(), param.getValue()));
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
