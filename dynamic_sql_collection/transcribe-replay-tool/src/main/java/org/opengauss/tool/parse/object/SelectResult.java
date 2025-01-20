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

package org.opengauss.tool.parse.object;

import lombok.Data;

import java.util.List;

/**
 * Description: select result object
 *
 * @author : zhangting
 * @since : 2025/01/14
 */
@Data
public class SelectResult {
    private long packetId;
    private SqlInfo previousSql;
    private long rowCount;
    private List<List<String>> dataList;

    public SelectResult(long packetId, SqlInfo previousSql, long rowCount, List<List<String>> dataList) {
        this.packetId = packetId;
        this.previousSql = previousSql;
        this.rowCount = rowCount;
        this.dataList = dataList;
    }
}
