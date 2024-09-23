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

package org.opengauss.tool;

import org.opengauss.tool.dispatcher.TaskDispatcher;
import org.opengauss.tool.utils.ConfigReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Description: Task starter.
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/17
 */
public class Starter {
    private static Map<String, String> commandMap = new HashMap<>();

    /**
     * The  command args entry, and sql parse entry
     *
     * @param args args String[], include command type and configure file path
     */
    public static void main(String[] args) {
        for (int i = 0; i < args.length; i += 2) {
            commandMap.put(args[i], args[i + 1]);
        }
        String command = commandMap.get(ConfigReader.COMMAND_TYPE);
        String configPath = commandMap.get(ConfigReader.CONFIG_FILE);
        TaskDispatcher dispatcher = new TaskDispatcher(command, configPath);
        dispatcher.dispatch();
    }
}
