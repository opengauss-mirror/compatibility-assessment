/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * FileDownloadConfig
 *
 * @author liu
 * @since 2023-09-17
 */
@Component
@ConfigurationProperties(prefix = "file")
public class FileDownloadConfig {
    private static String downloadPathSql;
    private static String downloadPathStack;

    public static String getDownloadPathSql() {
        return downloadPathSql;
    }

    public void setDownloadPathSql(String downloadPathSql) {
        FileDownloadConfig.downloadPathSql = downloadPathSql;
    }

    public static String getDownloadPathStack() {
        return downloadPathStack;
    }

    public void setDownloadPathStack(String downloadPathStack) {
        FileDownloadConfig.downloadPathStack = downloadPathStack;
    }
}
