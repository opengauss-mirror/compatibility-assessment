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

import java.util.ArrayList;
import java.util.List;
import org.kit.collect.domain.Regular;
import org.kit.collect.domain.RegularTask;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * RegularTaskConfig
 *
 * @author liu
 * @since 2023-09-17
 */
@Component
public class RegularTaskConfig {
    @Value("${scheduled.neverStop}")
    private boolean isNeverStop;

    @Value("${scheduled.task1.startTime}")
    private String task1StartTime;

    @Value("${scheduled.task1.endTime}")
    private String task1EndTime;

    @Value("${scheduled.task2.startTime}")
    private String task2StartTime;

    @Value("${scheduled.task2.endTime}")
    private String task2EndTime;

    @Value("${scheduled.task3.startTime}")
    private String task3StartTime;

    @Value("${scheduled.task3.endTime}")
    private String task3EndTime;

    @PostConstruct
    private void setNeverStop() {
        List<RegularTask> tasks = new ArrayList<>();
        RegularTask task1 = createRegularTask("task1", task1StartTime, task1EndTime);
        RegularTask task2 = createRegularTask("task2", task2StartTime, task2EndTime);
        RegularTask task3 = createRegularTask("task3", task3StartTime, task3EndTime);
        tasks.add(task1);
        tasks.add(task2);
        tasks.add(task3);
        Regular.setTasks(tasks);
        Regular.setNeverStop(isNeverStop);
    }

    private RegularTask createRegularTask(String name, String startTime, String endTime) {
        RegularTask task = new RegularTask();
        task.setName(name);
        task.setStartTime(startTime);
        task.setEndTime(endTime);
        return task;
    }
}
