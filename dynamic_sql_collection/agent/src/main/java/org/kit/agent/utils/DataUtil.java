/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
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
