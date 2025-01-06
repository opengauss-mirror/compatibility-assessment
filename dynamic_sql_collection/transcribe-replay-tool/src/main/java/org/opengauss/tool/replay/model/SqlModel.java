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

package org.opengauss.tool.replay.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opengauss.tool.utils.DatabaseOperator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * SqlModel
 *
 * @since 2024-07-01
 */
@Getter
@Setter
@NoArgsConstructor
public class SqlModel {
    private int id;
    private long packetId;
    private boolean isQuery;
    private boolean isPrepared;
    private String session;
    private String username;
    private String schema;
    private String sql;
    private List<ParamModel> parameters;
    private long startTime = 0L;
    private long endTime = 0L;
    private long mysqlDuration = 0L;
    private long opgsDuration = 0L;
    private String sqlExplain;

    /**
     * SqlModel instructor from db
     *
     * @param rs rs
     * @param storeConn storeConn
     * @param tableName tableName
     * @throws SQLException SQLException
     */
    public SqlModel(ResultSet rs, Connection storeConn, String tableName) throws SQLException {
        this.id = rs.getInt("id");
        this.packetId = rs.getLong("packet_id");
        this.isQuery = rs.getBoolean("is_query");
        this.isPrepared = rs.getBoolean("is_prepared");
        this.session = rs.getString("session").trim();
        this.username = rs.getString("username").trim();
        this.schema = rs.getString("schema").trim();
        this.sql = rs.getString("sql");
        this.parameters = this.isPrepared() ? getParamModels(storeConn, tableName, this.id) : new ArrayList<>();
        if (isHasTimeColumn(rs)) {
            this.startTime = rs.getLong("start_time");
            this.endTime = rs.getLong("end_time");
            this.mysqlDuration = rs.getLong("execute_duration");
        }
    }

    /**
     * SqlModel instructor from json
     *
     * @param jsonObject jsonObject
     */
    public SqlModel(JSONObject jsonObject) {
        this.id = jsonObject.getInt("id");
        this.packetId = jsonObject.getLong("packetId");
        this.isQuery = jsonObject.getBoolean("isQuery");
        this.isPrepared = jsonObject.getBoolean("isPrepared");
        this.session = jsonObject.getString("session");
        this.username = jsonObject.getString("username");
        this.schema = jsonObject.getString("schema");
        this.sql = jsonObject.getString("sql");
        this.parameters = getParamModels(jsonObject);
        if (jsonObject.has("startTime")) {
            this.startTime = jsonObject.getLong("startTime");
            this.endTime = jsonObject.getLong("endTime");
            this.mysqlDuration = jsonObject.getLong("executeDuration");
        }
    }

    private boolean isHasTimeColumn(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            if ("start_time".equals(rsmd.getColumnName(i))) {
                return true;
            }
        }
        return false;
    }

    private List<ParamModel> getParamModels(Connection storeConn, String tableName, int id) throws SQLException {
        String queryParamsSql = String.format(Locale.ROOT, "select * from %s where id = %d",
                tableName + "_paras", id);
        Statement stmt = null;
        ResultSet paramRs = null;
        List<ParamModel> parameterList = new ArrayList<>();
        try {
            stmt = storeConn.createStatement();
            paramRs = stmt.executeQuery(queryParamsSql);
            while (paramRs.next()) {
                parameterList.add(new ParamModel(paramRs));
            }
        } finally {
            DatabaseOperator.closeResultSet(paramRs);
            DatabaseOperator.closeStatement(stmt);
        }
        return parameterList;
    }

    private List<ParamModel> getParamModels(JSONObject jsonObject) {
        List<ParamModel> parameterList = new ArrayList<>();
        JSONArray jsonArray = jsonObject.getJSONArray("parameters");
        if (!jsonArray.isEmpty()) {
            for (int i = 0; i < jsonArray.length(); i++) {
                String paramStr = String.valueOf(jsonArray.get(i));
                parameterList.add(new ParamModel(paramStr, i));
            }
        }
        return parameterList;
    }
}