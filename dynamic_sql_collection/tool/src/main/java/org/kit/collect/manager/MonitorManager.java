/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.manager;

import java.io.File;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.kit.collect.utils.MonitSpringUtils;
import org.kit.collect.utils.MonitorThreads;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * MonitorManager
 *
 * @author liu
 * @since 2022-10-01
 */
public class MonitorManager {
    private static MonitorManager manager = new MonitorManager();

    /**
     * 异步操作任务调度线程池
     */
    private ScheduledExecutorService scheduledExecutor = MonitSpringUtils.getStr("monitorExecutorService");

    /**
     * 异步操作任务调度线程池
     */
    private ThreadPoolTaskExecutor threadPoolExecutor = MonitSpringUtils.getStr("threadPoolTaskExecutor");

    /**
     * 单例模式
     */
    private MonitorManager() {
    }

    /**
     * me
     *
     * @return AsyncManager
     */
    public static MonitorManager mine() {
        return manager;
    }

    /**
     * workScheduled
     *
     * @param task        task
     * @param executeTime executeTime
     */
    public void workScheduled(TimerTask task, long executeTime) {
        scheduledExecutor.schedule(task, executeTime, TimeUnit.MINUTES);
    }

    /**
     * workExecute
     *
     * @param task task
     * @return String
     */
    public Future<String> workExecute(TimerTask task) {
        Callable<String> callableTask = () -> {
            task.run();
            return "Task completed successfully";  // You can customize the return value
        };
        return threadPoolExecutor.submit(callableTask);
    }

    /**
     * workExecute
     *
     * @param callableTask callableTask
     * @return Future Future
     */
    public Future<File> workExecute(Callable<File> callableTask) {
        return threadPoolExecutor.submit(callableTask);
    }

    /**
     * stopTask
     */
    public void stopTask() {
        MonitorThreads.scheduledShutdown(scheduledExecutor);
        MonitorThreads.executeShutdown(threadPoolExecutor);
    }
}
