/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * SqlCollect
 *
 * @author liu
 * @since 2023-09-17
 */
@EnableScheduling
@SpringBootApplication
public class SqlCollect {
    public static void main(String[] args) {
        SpringApplication.run(SqlCollect.class, args);
    }
}
