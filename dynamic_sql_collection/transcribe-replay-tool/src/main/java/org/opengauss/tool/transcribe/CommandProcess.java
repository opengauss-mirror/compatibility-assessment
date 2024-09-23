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

package org.opengauss.tool.transcribe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Description: Command process
 *
 * @author wangzhengyuan
 * @since 2024/05/30
 **/
public class CommandProcess implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandProcess.class);
    private String command;
    private Process process;

    /**
     * Constructor
     *
     * @param command String the command
     */
    public CommandProcess(String command) {
        this.command = command;
    }

    /**
     * stop
     */
    public void stop() {
        process.destroyForcibly();
    }

    @Override
    public void run() {
        try {
            process = Runtime.getRuntime().exec(command);
            process.waitFor();
        } catch (IOException e) {
            LOGGER.error("The command {} execute failed, error message is {}.", command, e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.info("The command {} execute finished.", command);
        }
    }
}
