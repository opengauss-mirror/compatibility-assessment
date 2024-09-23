/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.tool.replay.factory;

import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.replay.task.ReplayDbTask;
import org.opengauss.tool.replay.task.ReplayJsonTask;
import org.opengauss.tool.replay.task.ReplayMainTask;

import java.util.HashMap;
import java.util.Map;

/**
 * ReplayFactory
 *
 * @since 2024-07-01
 */
public class ReplayFactory {
    /**
     * strategyMap
     */
    public static final Map<String, ReplayMainTask> strategyMap = new HashMap<>();

    /**
     * build strategyMap
     *
     * @param replayConfig replayConfig
     */
    public static void buildStrategyMap(ReplayConfig replayConfig) {
        strategyMap.put("db", new ReplayDbTask(replayConfig));
        strategyMap.put("json", new ReplayJsonTask(replayConfig));
    }

    /**
     * get replay strategy
     *
     * @param commandType commandType
     * @return ReplayTask
     */
    public static ReplayMainTask getReplayStrategy(String commandType) {
        return strategyMap.get(commandType);
    }
}
