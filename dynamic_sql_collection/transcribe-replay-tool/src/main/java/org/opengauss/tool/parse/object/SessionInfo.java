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

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * Description: Session information
 *
 * @author : wang_zhengyuan
 * @since : 2024/08/01
 */
@Data
public class SessionInfo {
    private String sessionId;
    private String username;
    private String schema;
    private JSONObject json;

    public SessionInfo(String sessionId, String username, String schema) {
        this.sessionId = sessionId;
        this.username = username;
        this.schema = schema;
        this.json = new JSONObject(true);
    }

    @Override
    public String toString() {
        json.clear();
        json.fluentPut("session", sessionId)
                .fluentPut("username", username)
                .fluentPut("schema", schema);
        return json.toString();
    }
}
