/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * StakeConfig
 *
 * @author liu
 * @since 2023-09-17
 */
@Component
@ConfigurationProperties(prefix = "dynamic.insertion")
public class StakeConfig {
    private static String agentName;
    private static String attachName;
    private static String environment;
    private static String prefix;
    private static String command;
    private static String findSql;
    private static String findStack;
    private static String resourcePath;

    /**
     * init
     */
    @PostConstruct
    public void init() {
        command = environment + ";" + prefix + " " + LinuxConfig.getUploadPath() + "/" + attachName + " "
                + LinuxConfig.getPid() + " " + LinuxConfig.getUploadPath() + "/" + agentName + " " + "install" + " ";
        setCommand(command);
    }

    public static String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        StakeConfig.environment = environment;
    }

    public static String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        StakeConfig.prefix = prefix;
    }

    public static String getAgentName() {
        return agentName;
    }

    public void setAgentName(String agentName) {
        StakeConfig.agentName = agentName;
    }

    public static String getAttachName() {
        return attachName;
    }

    public void setAttachName(String attachName) {
        StakeConfig.attachName = attachName;
    }

    public static String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        StakeConfig.command = command;
    }

    public static String getFindSql() {
        return findSql;
    }

    public void setFindSql(String findSql) {
        StakeConfig.findSql = findSql;
    }

    public static String getFindStack() {
        return findStack;
    }

    public void setFindStack(String findStack) {
        StakeConfig.findStack = findStack;
    }

    public static String getResourcePath() {
        return resourcePath;
    }

    public void setResourcePath(String resourcePath) {
        StakeConfig.resourcePath = resourcePath;
    }
}
