/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
}
