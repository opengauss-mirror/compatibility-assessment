/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * transcribe-replay-tool is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
    private static String filePath;

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

    public static String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}