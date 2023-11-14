/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils;

import org.kit.collect.common.Constant;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

/**
 * SchedulerUtil
 *
 * @author liu
 * @since 2023-09-17
 */
public class SchedulerUtil {
    /**
     * getTrigger
     *
     * @param name           name
     * @param group          group
     * @param cronExpression cronExpression
     * @return Trigger
     */
    public static Trigger getTrigger(String name, String group, String cronExpression) {
        return TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
                .build();
    }

    /**
     * heartbeatTrigger
     *
     * @param triggerName triggerName
     * @param interval    interval
     * @param cronStart   cronStart
     * @param cronEndTime cronEndTime
     * @return Trigger
     */
    public static Trigger heartbeatTrigger(String triggerName, int interval, String cronStart, String cronEndTime) {
        return TriggerBuilder.newTrigger()
                .withIdentity(triggerName, Constant.SCHEDULER_GROUP)
                .startAt(DateUtil.stringToDate(cronStart))
                .withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(interval))
                .endAt(DateUtil.stringToDate(cronEndTime))
                .build();
    }

    /**
     * getJobDetail
     *
     * @param jobName  jobName
     * @param group    group
     * @param time     time
     * @param jobClass jobClass
     * @return JobDetail JobDetail
     */
    public static JobDetail getJobDetail(String jobName, String group, String time, Class<? extends Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(jobName, group)
                .usingJobData("time", time)
                .build();
    }

    /**
     * heartbeatDetail
     *
     * @param jobName  jobName
     * @param jobClass jobClass
     * @param command  command
     * @return JobDetail JobDetail
     */
    public static JobDetail heartbeatDetail(String jobName, String command, Class<? extends Job> jobClass) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(jobName, Constant.SCHEDULER_GROUP)
                .usingJobData("command", command)
                .build();
    }
}

