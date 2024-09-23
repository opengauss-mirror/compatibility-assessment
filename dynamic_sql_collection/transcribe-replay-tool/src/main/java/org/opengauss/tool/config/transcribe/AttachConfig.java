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

package org.opengauss.tool.config.transcribe;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.utils.FileOperator;

import java.io.File;
import java.util.Properties;

/**
 * Description: Configure of attaching client program
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/25
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class AttachConfig extends TranscribeConfig {
    private String configPath;
    private long pid;

    @Override
    public void load(Properties props) {
        this.shouldCheckResource = Boolean.parseBoolean(props.getProperty(ConfigReader.SHOULD_CHECK_SYSTEM, "true"));
        this.shouldSendFile = Boolean.parseBoolean(props.getProperty(ConfigReader.SHOULD_SEND_FILE, "true"));
        super.load(props);
        this.pluginPath = FileOperator.formatFilePath(props.getProperty(ConfigReader.ATTACH_PLUGIN_PATH, FileOperator
                .CURRENT_PATH + ConfigReader.PLUGIN + File.separator));
        this.captureDuration = Integer.parseInt(props.getProperty(ConfigReader.ATTACH_CAPTURE_DURATION, "1"));
        this.pid = Long.parseLong(props.getProperty(ConfigReader.ATTACH_PROCESS_PID));
        loadFileConfig(props);
    }

    @Override
    protected void loadFileConfig(Properties props) {
        super.loadFileConfig(props);
        fileConfig.setFilePath(FileOperator.formatFilePath(props.getProperty(ConfigReader.SQL_FILE_PATH, FileOperator
                .CURRENT_PATH + ConfigReader.DEFAULT_SQL_FILES + File.separator)));
        fileConfig.setFileName(props.getProperty(ConfigReader.SQL_FILE_NAME, ConfigReader.DEFAULT_SQL_FILE));
        fileConfig.setFileSize(Integer.parseInt(props.getProperty(ConfigReader.SQL_FILE_SIZE, "10")));
    }

    @Override
    public boolean shouldCheckResource() {
        return shouldCheckResource;
    }

    @Override
    public boolean shouldSendFile() {
        return shouldSendFile;
    }
}
