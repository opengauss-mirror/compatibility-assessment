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

import lombok.Data;

/**
 * Description: Prepared value
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/15
 */
@Data
public class PreparedValue {
    private String type;
    private String value;
    private int offset;

    /**
     * Constructor
     *
     * @param value  String the value
     * @param offset int the offset
     */
    public PreparedValue(String value, int offset) {
        this.value = value;
        this.offset = offset;
    }

    /**
     * Constructor
     */
    public PreparedValue() {
    }

    /**
     * Constructor
     *
     * @param type String the type
     */
    public PreparedValue(String type) {
        this.type = type;
    }

    /**
     * Set prepared value type
     *
     * @param type String the type
     */
    public void setType(String type) {
        switch (type) {
            case "03":
                this.type = "int";
                break;
            case "05":
                this.type = "double";
                break;
            case "06":
                this.type = "null";
                break;
            case "0c":
                this.type = "timestamp";
                break;
            case "fd":
                this.type = "string";
                break;
            default:
                this.type = "unknow";
        }
    }
}
