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

package org.kit.collect.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * DateUtil
 *
 * @author liu
 * @since 2023-09-17
 */
public class DateUtil {
    /**
     * DEFAULT_PATTERN
     */
    public static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * stringToDate
     *
     * @param dateString dateString
     * @param pattern    pattern
     * @return Date Date
     */
    public static Date stringToDate(String dateString, String pattern) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
            return dateFormat.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
            return new Date();
        }
    }

    /**
     * stringToDate
     *
     * @param dateString dateString
     * @return Date Date
     */
    public static Date stringToDate(String dateString) {
        return stringToDate(dateString, DEFAULT_PATTERN);
    }

    /**
     * dateToString
     *
     * @param date    date
     * @param pattern pattern
     * @return String
     */
    public static String dateToString(Date date, String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }

    /**
     * dateToString
     *
     * @param date date
     * @return String String
     */
    public static String dateToString(Date date) {
        return dateToString(date, DEFAULT_PATTERN);
    }

    /**
     * getDate
     *
     * @return String String
     */
    public static String getDate() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd_");
        LocalDateTime ldt = LocalDateTime.now();
        return ldt.format(dtf);
    }
}
