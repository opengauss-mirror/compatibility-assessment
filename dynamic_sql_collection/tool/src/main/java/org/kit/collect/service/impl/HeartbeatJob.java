/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.kit.collect.utils.JschUtil;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * HeartbeatJob
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class HeartbeatJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String command = dataMap.getString("command");
        String res = JschUtil.executeCommand(JschUtil.obtainSession(), command);
        if (res.contains("yes")) {
            log.info("SQL extraction task is in progress, please wait for the result");
        } else {
            log.info("SQL extraction task terminated due to non-existent or uncontrollable target process. "
                    + "Please restart");
        }
    }
}
