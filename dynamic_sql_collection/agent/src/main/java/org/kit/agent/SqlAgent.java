/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.agent;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;
import org.kit.agent.impl.PreparedFormer;
import org.kit.agent.impl.StateFormer;

/**
 * SqlAgent
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class SqlAgent {
    /**
     * 文件写入路径
     */
    public static String path = "/kit/file/";

    /**
     * Should transcribe
     */
    public static final AtomicBoolean SHOULD_TRANSCRIBE = new AtomicBoolean(false);

    /**
     * 文件大小阈值单位是字节，默认是10Mb
     */
    public static int fileSize = 10 * 1024 * 1024;

    private static final ClassFileTransformer STATEFORMER = new StateFormer();
    private static final ClassFileTransformer PREPAREDFORMER = new PreparedFormer();
    private static final List<String> names = Arrays.asList("com.mysql.cj.jdbc.StatementImpl",
            "com.mysql.jdbc.StatementImpl",
            "com.microsoft.sqlserver.jdbc.SQLServerStatement",
            "org.postgresql.jdbc.PgStatement",
            "oracle.jdbc.driver.OracleStatement",
            "com.mysql.cj.jdbc.ClientPreparedStatement",
            "com.mysql.jdbc.PreparedStatement",
            "com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement",
            "org.postgresql.jdbc.PgPreparedStatement");
    private static Class[] loadedClasses = null;

    /**
     * agentmain
     *
     * @param agentArgs agentArgs
     * @param inst      inst
     * @throws UnmodifiableClassException UnmodifiableClassException
     */
    public static void agentmain(String agentArgs, Instrumentation inst) throws UnmodifiableClassException {
        loadedClasses = inst.getAllLoadedClasses();
        inst.addTransformer(STATEFORMER, true);
        inst.addTransformer(PREPAREDFORMER, true);
        for (Class clazz : loadedClasses) {
            Boolean isCont = names.contains(clazz.getName());
            if (isCont) {
                inst.retransformClasses(clazz);
            }
        }
        List<String> params = Arrays.asList(agentArgs.split(" "));
        boolean isNeverStop = false;
        long executionTime = 0L;
        TimeUnit unit = TimeUnit.MINUTES;
        for (String param : params) {
            String[] keyValue = param.split("=");
            String key = keyValue[0];
            String value = keyValue[1];
            if (key.equals("neverStop") && value.equalsIgnoreCase("true")) {
                isNeverStop = true;
            }
            if (key.equals("executionTime")) {
                executionTime = Long.parseLong(value);
            }
            if (key.equals("writePath")) {
                path = value;
            }
            if ("shouldTranscribe".equals(key) && "true".equalsIgnoreCase(value)) {
                SHOULD_TRANSCRIBE.set(true);
            }
            // 参数单位为Mb 需要转换为字节
            if (key.equals("threshold")) {
                fileSize = Integer.parseInt(value) * 1024 * 1024;
            }
            if (key.equals("unit")) {
                if (value.equalsIgnoreCase("seconds")) {
                    unit = TimeUnit.SECONDS;
                } else if (value.equalsIgnoreCase("minutes")) {
                    unit = TimeUnit.MINUTES;
                } else {
                    log.info("nothing");
                }
            }
        }
        if (isNeverStop) {
            return;
        }
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        scheduleRemoveTransformers(inst, executionTime, unit, executor);
        // 使用完毕后关闭线程池
        shutdownExecutor(executor);
    }

    private static void scheduleRemoveTransformers(Instrumentation inst, long executionTime, TimeUnit unit,
                                                   ScheduledExecutorService executor) {
        executor.schedule(() -> {
            inst.removeTransformer(STATEFORMER);
            inst.removeTransformer(PREPAREDFORMER);
            loadedClasses = inst.getAllLoadedClasses();
            for (Class clazz : loadedClasses) {
                Boolean isCont = names.contains(clazz.getName());
                if (isCont) {
                    try {
                        log.info("start reconversion operation---->" + clazz);
                        inst.retransformClasses(clazz);
                        log.info("reconversion operation end---->" + clazz);
                    } catch (UnmodifiableClassException e) {
                        log.error("Failed to retransform class: " + clazz.getName() + ". Error: " + e.getMessage());
                    }
                }
            }
        }, executionTime, unit);
    }

    private static void shutdownExecutor(ScheduledExecutorService executor) {
        if (executor != null) {
            log.info("Close thread pool");
            executor.shutdown();
        }
    }
}
