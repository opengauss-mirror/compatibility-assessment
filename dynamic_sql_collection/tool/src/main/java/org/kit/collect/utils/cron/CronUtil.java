/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils.cron;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

/**
 * CronUtil
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class CronUtil {
    /**
     * generateCron
     *
     * @param dateTimeString dateTimeString
     * @return String
     */
    public static String generateCron(String dateTimeString) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date dateTime = null;
        try {
            dateTime = dateFormat.parse(dateTimeString);
        } catch (ParseException exception) {
            log.error("CronUtil dateFormat parse fail {}", exception.getMessage());
        }
        // extract time information
        String second = new SimpleDateFormat("ss").format(dateTime);
        String minute = new SimpleDateFormat("mm").format(dateTime);
        String hour = new SimpleDateFormat("HH").format(dateTime);
        String dayOfMonth = new SimpleDateFormat("dd").format(dateTime);
        String month = new SimpleDateFormat("MM").format(dateTime);
        String year = new SimpleDateFormat("yyyy").format(dateTime);
        // building cron expressions
        return second + " " + minute + " " + hour + " " + dayOfMonth + " " + month + " ? " + year;
    }
}