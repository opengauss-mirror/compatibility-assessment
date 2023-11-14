/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.collect.domain;

import java.util.List;

/**
 * Regular
 *
 * @author liu
 * @since 2023-09-17
 */
public class Regular {
    /**
     * isNeverStop
     */
    public static Boolean isNeverStop;

    /**
     * tasks
     */
    public static List<RegularTask> tasks;

    public static void setNeverStop(Boolean isNeverStop) {
        Regular.isNeverStop = isNeverStop;
    }

    public static void setTasks(List<RegularTask> tasks) {
        Regular.tasks = tasks;
    }
}
