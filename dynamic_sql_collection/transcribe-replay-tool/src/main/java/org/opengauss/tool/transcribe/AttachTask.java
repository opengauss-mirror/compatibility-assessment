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

import org.opengauss.tool.config.transcribe.TranscribeConfig;
import org.opengauss.tool.utils.FileOperator;
import org.opengauss.tool.utils.FileUtils;

import java.io.File;

/**
 * Description: Command process
 *
 * @author wangzhengyuan
 * @since 2024/06/05
 **/
public class AttachTask extends TranscribeTask {
    private static final String JSON_SUFFIX = ".json";

    /**
     * Constructor
     *
     * @param config TranscribeConfig the config
     */
    public AttachTask(TranscribeConfig config) {
        super(config);
    }

    @Override
    protected void initCommandParameters() {
    }

    @Override
    protected String buildCommand() {
        String pluginPath = config.getPluginPath();
        return String.format("java -jar %sattach.jar %s %sagent.jar install neverStop=false executionTime=%s "
                        + "unit=minutes writePath=%s shouldTranscribe=true", pluginPath,
                config.getPid(), pluginPath, config.getCaptureDuration(), config.getConfigPath());
    }

    @Override
    protected boolean isValidFile(String fileName) {
        return fileName.startsWith(config.getFileConfig().getFileName());
    }

    @Override
    protected int getFileIndex(String name) {
        return Integer.parseInt(name.substring(name.lastIndexOf("-") + 1, name.indexOf(JSON_SUFFIX)));
    }

    @Override
    protected void initStorage() {
        this.processPath = FileUtils.getJarPath() + File.separator + PROCESS_FILE_NAME;
        FileUtils.createFile(processPath);
        FileOperator.createPath(config.getFileConfig().getFilePath(), config.getFileConfig().getFileName());
        fileOperator = new FileOperator(config.getFileConfig());
    }
}
