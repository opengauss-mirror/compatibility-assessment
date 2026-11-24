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
