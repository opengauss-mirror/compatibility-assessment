/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * transcribe-replay-tool is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.kit.agent.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * DataUtil
 *
 * @author liu
 * @since 2023-09-17
 */
public class DataUtil {
    /**
     * getTimeNow
     *
     * @return String String
     */
    public static String getTimeNow() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss ");
        LocalDateTime ldt = LocalDateTime.now();
        return ldt.format(dtf);
    }

    /**
     * getDate
     *
     * @return String String
     */
    public static String getDate() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime ldt = LocalDateTime.now();
        return ldt.format(dtf);
    }
}
