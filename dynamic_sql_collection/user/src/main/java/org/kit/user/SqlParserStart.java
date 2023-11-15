/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.user;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * SqlParserStart
 *
 * @author liu
 * @since 2023-09-17
 */
@EnableScheduling
@SpringBootApplication
public class SqlParserStart {
    public static void main(String[] args) {
        SpringApplication.run(SqlParserStart.class);
    }
}