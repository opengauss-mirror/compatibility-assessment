/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.domain;

import lombok.Data;
import org.springframework.stereotype.Component;

/**
 * RegularTask
 *
 * @author liu
 * @since 2023-09-17
 */
@Data
@Component
public class RegularTask {
    private String name;

    private String startTime;

    private String endTime;
}