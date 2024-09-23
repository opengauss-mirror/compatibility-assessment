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

package org.opengauss.tool.dispatcher;

import org.opengauss.tool.transcribe.GeneralLogTask;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.config.parse.ParseConfig;
import org.opengauss.tool.config.transcribe.TranscribeConfig;
import org.opengauss.tool.parse.ParseTask;
import org.opengauss.tool.transcribe.AttachTask;
import org.opengauss.tool.transcribe.TranscribeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: Task dispatcher
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/25
 */
public class TaskDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskDispatcher.class);
    private WorkTask task;
    private String command;
    private String configPath;
    private TranscribeConfig transcribeConfig;
    private ParseConfig parseConfig;

    /**
     * Constructor
     *
     * @param command    String the task command
     * @param configPath String the config path
     */
    public TaskDispatcher(String command, String configPath) {
        this.command = command;
        this.configPath = configPath;
    }

    /**
     * Dispatch task
     */
    public void dispatch() {
        initTask();
        if (task != null) {
            task.start();
        }
    }

    private void initTask() {
        switch (command) {
            case ConfigReader.TRANSCRIBE:
                initTranscribeTask();
                break;
            case ConfigReader.PARSE:
                initParseTask();
                break;
            default:
                LOGGER.error(" The parameter '-t' value is incorrect, it must be one of the transcribe or parse or"
                        + " replay.");
        }
    }

    private void initParseTask() {
        this.parseConfig = ConfigReader.initParseConfig(configPath);
        this.task = new ParseTask(parseConfig);
    }

    private void initTranscribeTask() {
        this.transcribeConfig = ConfigReader.initTranscribeConfig(configPath);
        String transcribeMode = transcribeConfig.getTranscribeMode();
        if (ConfigReader.TCPDUMP.equalsIgnoreCase(transcribeMode)) {
            this.task = new TranscribeTask(transcribeConfig);
        } else if (ConfigReader.ATTACH.equalsIgnoreCase(transcribeMode)) {
            this.task = new AttachTask(transcribeConfig);
        } else {
            this.task = new GeneralLogTask(transcribeConfig);
        }
    }
}
