/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * LinuxConfig
 *
 * @author liu
 * @since 2023-09-17
 */
@Component
@ConfigurationProperties(prefix = "linux.config")
public class LinuxConfig {
    private static String host;
    private static Integer port;
    private static String userName;
    private static String linuxSecret;
    private static String pid;

    public static String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public static Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public static String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public static String getLinuxSecret() {
        return linuxSecret;
    }

    public void setLinuxSecret(String linuxSecret) {
        this.linuxSecret = linuxSecret;
    }

    public static String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }
}