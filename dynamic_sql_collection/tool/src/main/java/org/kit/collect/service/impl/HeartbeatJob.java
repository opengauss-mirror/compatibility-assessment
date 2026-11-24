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
