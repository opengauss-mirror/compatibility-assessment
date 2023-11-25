/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * MonitorThreads
 *
 * @author liu
 * @since 2022-10-01
 */
@Slf4j
public class MonitorThreads {
    /**
     * scheduledShutdown
     *
     * @param executorService executorService
     */
    public static void scheduledShutdown(ExecutorService executorService) {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(100, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(100, TimeUnit.SECONDS)) {
                        log.info("executorService not stop");
                    }
                }
            } catch (InterruptedException exception) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * executeShutdown
     *
     * @param threadPoolExecutor threadPoolExecutor
     */
    public static void executeShutdown(ThreadPoolTaskExecutor threadPoolExecutor) {
        threadPoolExecutor.shutdown();
    }
}
