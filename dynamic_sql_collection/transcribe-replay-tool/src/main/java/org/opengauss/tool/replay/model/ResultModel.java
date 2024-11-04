/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.replay.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * ResultModel
 *
 * @author : zhangting
 * @since : 2024/12/26
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResultModel {
    private long packetId;
    private long sqlPacketId;
    private JSONArray data;
    private long rowsCount;
    private String sql;

    public ResultModel(JSONObject jsonObject) {
        this.packetId = jsonObject.getInt("packetId");
        this.sqlPacketId = jsonObject.getInt("sqlPacketId");
        this.data = jsonObject.getJSONArray("data");
        this.rowsCount = jsonObject.getInt("rowsCount");
        this.sql = jsonObject.getString("sql");
    }
}