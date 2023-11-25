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
    public static final String COMMAND = "source /etc/profile;java -jar /kit/file/attach.jar pid /kit/file/agent.jar "
            + "install neverStop=false executionTime=time unit=minutes";

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
     * ASSESS_SH
     */
    public static final String ASSESS_SH = "start.sh";

    /**
     * ASSESS_JAR
     */
    public static final String ASSESS_JAR = "compatibility-assessment-5.1.0.jar";

    /**
     * ASSESS_PROPERTIES
     */
    public static final String ASSESS_PROPERTIES = "assessment.properties";

    /**
     * ASSESS_PROPERTIES
     */
    public static final String ASSESS_REPORT = "report.html";

    /**
     * ASSESS_SH
     */
    public static final String ASSESS_SH_PATH = "assessment/start.sh";

    /**
     * ASSESS_SH
     */
    public static final String ASSESS_REPORT_PATH = "assessment/report.html";

    /**
     * ASSESS_JAR
     */
    public static final String ASSESS_JAR_PATH = "assessment/compatibility-assessment-5.1.0.jar";

    /**
     * ASSESS_PROPERTIES
     */
    public static final String ASSESS_PROPERTIES_PATH = "data/assessment.properties";

    /**
     * ASSESS_FILE
     */
    public static final String ASSESS_FILE = "file";

    /**
     * ASSESS_COLLECT
     */
    public static final String ASSESS_COLLECT = "collect";

    /**
     * ASSESS_PID
     */
    public static final String ASSESS_PID = "proccessPid";

    /**
     * INSERTION_AGENTNAME
     */
    public static final String INSERTION_SQL = "collection.sql";

    /**
     * INSERTION_ATTACHNAME
     */
    public static final String INSERTION_STACK = "stack.txt";

    /**
     * INSERTION_ENVIRONMENT
     */
    public static final String INSERTION_ENVIRONMENT = "source /etc/profile";

    /**
     * INSERTION_PREFIX
     */
    public static final String INSERTION_PREFIX = "java -jar";

    /**
     * INSERTION_UPLOADPATH
     */
    public static final String INSERTION_UPLOADPATH = "/kit/file";

    /**
     * SQL_path
     */
    public static final String SQL_PATH = "/kit/file/collection.sql";

    /**
     * SQL_path
     */
    public static final String ASSESS_PATH = "/kit/file/report.html";

    /**
     * stack_path
     */
    public static final String STACK_PATH = "/kit/file/stack.txt";

    /**
     * CHECK_PREFIX
     */
    public static final String CHECK_PREFIX = "if ps -p ";

    /**
     * CHECK_SUFFIX
     */
    public static final String CHECK_SUFFIX = " > /dev/null ; then [ ! -f /kit/file/collection.sql ] && mkdir"
            + " -p /kit/file && touch /kit/file/collection.sql; [ ! -f /kit/file/stack.txt ] && mkdir "
            + "-p /kit/file && touch /kit/file/stack.txt ; echo \"yes\"; else echo \"not exist\" ; fi";

    /**
     * FIND_ALL_JAVA
     */
    public static final String FIND_ALL_JAVA = "ps -ef | grep java | grep -v grep | "
            + "awk '{print \"pid:\" $2 \"  javaName:\" $NF}'";
}
