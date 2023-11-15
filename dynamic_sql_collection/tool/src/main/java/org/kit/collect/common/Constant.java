/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.common;

import lombok.extern.slf4j.Slf4j;

/**
 * Constant
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class Constant {
    /**
     * PROCESS_NOT_EXIST
     */
    public static final String PROCESS_NOT_EXIST = "Could not find a process with PID";

    /**
     * STAKE_INSERTION_SUCCESSFUL
     */
    public static final String STAKE_INSERTION_SUCCESSFUL = "Agent successfully loaded to the target process";

    /**
     * INSERTION_FAILED
     */
    public static final String INSERTION_FAILED = "failed to load agent to the target process.Error";

    /**
     * COMMAND_FAULD
     */
    public static final String COMMAND_FAULD = "Usage: java -jar attach.jar "
            + "18487 agent.jar install neverStop=true executionTime=3 unit=minutes";

    /**
     * SQL_TYPE
     */
    public static final String SQL_TYPE = "sql";

    /**
     * STACK_TYPE
     */
    public static final String STACK_TYPE = "stack";

    /**
     * SQL_NAME
     */
    public static final String SQL_NAME = "collection.sql";

    /**
     * STACK_NAME
     */
    public static final String STACK_NAME = "stack.txt";

    /**
     * SCHEDULER_GROUP
     */
    public static final String SCHEDULER_GROUP = "default";

    /**
     * SQL_PATH
     */
    public static final String SQL_PATH = "/kit/file/collection.sql";

    /**
     * STACK_PATH
     */
    public static final String STACK_PATH = "/kit/file/stack.txt";
}
