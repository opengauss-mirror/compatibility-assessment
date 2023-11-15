/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.preload;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.kit.collect.common.Constant;
import org.kit.collect.config.LinuxConfig;
import org.kit.collect.config.StakeConfig;
import org.kit.collect.domain.Regular;
import org.kit.collect.domain.RegularTask;
import org.kit.collect.service.impl.HeartbeatJob;
import org.kit.collect.service.impl.PileInsertionJob;
import org.kit.collect.utils.JschUtil;
import org.kit.collect.utils.SchedulerUtil;
import org.kit.collect.utils.cron.CronUtil;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * DynamicStakeExecutor
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
@Component
public class DynamicStakeExecutor {
    private Scheduler scheduler;

    /**
     * StartInsertingPiles
     *
     * @throws SchedulerException SchedulerException
     */
    @PostConstruct
    public void startInsertingPiles() throws SchedulerException {
        if (Regular.isNeverStop) {
            String command = StakeConfig.getCommand() + "neverStop=true executionTime=" + "3" + " " + "unit=minutes";
            JschUtil.executeTask(command);
        } else {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            scheduler = schedulerFactory.getScheduler();
            // create scheduler task
            List<RegularTask> tasks = Regular.tasks;
            for (RegularTask task : tasks) {
                String cron = CronUtil.generateCron(task.getStartTime());
                String executionTime = calculateMinutes(task.getStartTime(), task.getEndTime());
                Trigger trigger = SchedulerUtil.getTrigger(task.getName(), Constant.SCHEDULER_GROUP, cron);
                JobDetail detail = SchedulerUtil.getJobDetail(task.getName(), Constant.SCHEDULER_GROUP,
                        executionTime, PileInsertionJob.class);
                scheduler.scheduleJob(detail, trigger);
                // Heartbeat detection
                String command = "if ps -p" + LinuxConfig.getPid() + "> /dev/null; "
                        + "then echo \"yes\"; else echo \"not exist\"; fi";
                Trigger beatTrigger = SchedulerUtil.heartbeatTrigger(task.getName() + "Heartbeat",
                        getInterval(executionTime), task.getStartTime(), task.getEndTime());
                JobDetail beatDetail = SchedulerUtil.heartbeatDetail(task.getName() + "Heartbeat",
                        command, HeartbeatJob.class);
                scheduler.scheduleJob(beatDetail, beatTrigger);
            }
            scheduler.start();
        }
    }

    private int getInterval(String executionTime) {
        int num = Integer.parseInt(executionTime);
        if (num < 60) {
            return 10;
        } else if (num < 120) {
            return 30;
        } else {
            return 60;
        }
    }

    private String calculateMinutes(String timeStr1, String timeStr2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date1 = sdf.parse(timeStr1);
            Date date2 = sdf.parse(timeStr2);
            long differenceInMillis = Math.abs(date1.getTime() - date2.getTime());
            long minutesDifference = TimeUnit.MILLISECONDS.toMinutes(differenceInMillis);
            return String.valueOf(Math.max(1, minutesDifference));
        } catch (ParseException exception) {
            log.error("calculateMinutesDifference fail");
            return "1";
        }
    }

    /**
     * stopScheduler
     *
     * @throws SchedulerException SchedulerException
     */
    @PreDestroy
    public void stopScheduler() throws SchedulerException {
        scheduler.shutdown();
    }
}