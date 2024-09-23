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
 * Description: timestamp object
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/05
 */
@Data
public class TimestampObj {
    private int year;
    private int month;
    private int day;
    private int hour;
    private int minute;
    private double second;

    /**
     * Set second
     *
     * @param second            int the second
     * @param billionthOfSecond int the billionth of second
     */
    public void setSecond(int second, int billionthOfSecond) {
        this.second = second + billionthOfSecond / Math.pow(10, 9);
    }

    @Override
    public String toString() {
        return year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;
    }
}
