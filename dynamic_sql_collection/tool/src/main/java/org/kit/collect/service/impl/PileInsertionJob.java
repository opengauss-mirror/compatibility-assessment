/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.service.impl;

import com.jcraft.jsch.Session;
import lombok.extern.slf4j.Slf4j;
import org.kit.collect.common.Constant;
import org.kit.collect.config.LinuxConfig;
import org.kit.collect.utils.JschUtil;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * PileInsertionJob
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class PileInsertionJob implements Job {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        log.info("start inserting piles............");
        JobDataMap dataMap = context.getJobDetail().getJobDataMap();
        String time = dataMap.getString("time");
        String path = LinuxConfig.getFilePath();
        String pid = LinuxConfig.getPid();
        String command = String.format(Constant.COMMAND, path, pid, path, "false", time, path);
        Session session = JschUtil.obtainSession();
        JschUtil.executeTask(command, session);
    }
}
