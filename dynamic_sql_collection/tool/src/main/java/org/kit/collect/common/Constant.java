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
     * PILE_SUCCESS
     */
    public static final String PILE_SUCCESS = "Agent successfully loaded to the target process";

    /**
     * SQL_TYPE
     */
    public static final String SQL_TYPE = "sql";

    /**
     * COMMAND
     */
    public static final String COMMAND = "source /etc/profile;java -jar %sattach.jar %s %sagent.jar "
            + "install neverStop=%s executionTime=%s unit=minutes "
            + "writePath=%s threshold=10";

    /**
     * STACK_TYPE
     */
    public static final String STACK_TYPE = "stack";

    /**
     * LINUX_CONF
     */
    public static final String LINUX_SESSION = "linuxSession";

    /**
     * LINUX_CONF
     */
    public static final String LINUX_PID = "pid";

    /**
     * EXECUTE_TIME
     */
    public static final String EXECUTE_TIME = "executeTime";

    /**
     * EXECUTE_TIME
     */
    public static final String TASK = "task";

    /**
     * TASK_RUN
     */
    public static final String TASK_RUN = "running";

    /**
     * TASK_STOP
     */
    public static final String TASK_STOP = "stopping";

    /**
     * TASK_COMPLETED
     */
    public static final String TASK_COMPLETED = "completed";

    /**
     * EXECUTE_TIME
     */
    public static final String CONFIG = "config";

    /**
     * SCHEDULER_GROUP
     */
    public static final String SCHEDULER_GROUP = "default";

    /**
     * INSERTION_AGENTNAME
     */
    public static final String INSERTION_AGENTNAME = "agent.jar";

    /**
     * INSERTION_ATTACHNAME
     */
    public static final String INSERTION_ATTACHNAME = "attach.jar";

    /**
     * INSERTION_AGENTNAME
     */
    public static final String INSERTION_AGENTNAME_PATH = "stake/agent.jar";

    /**
     * INSERTION_ATTACHNAME
     */
    public static final String INSERTION_ATTACHNAME_PATH = "stake/attach.jar";

    /**
     * ASSESS_PROPERTIES
     */
    public static final String ASSESS_PROPERTIES = "assessment.properties";

    /**
     * CHECK
     */
    public static final String CHECK = "if ps -p %s  > /dev/null ; then [ ! -f %scollect.json ] && mkdir -p %s "
            + "&& touch %scollect.json;echo \"yes\"; else echo \"not exist\" ; fi";

    /**
     * FIND_ALL_JAVA
     */
    public static final String FIND_ALL_JAVA = "ps -ef | grep java | grep -v grep | "
            + "awk '{print \"pid:\" $2 \"  javaName:\" $NF}'";
}
