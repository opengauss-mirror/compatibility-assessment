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
 * Description: General log config
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/25
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class GeneralLogConfig extends TranscribeConfig {
    private int sqlBatch;
    private String storageMode;
    private String startTime;

    @Override
    public void load(Properties props) {
        super.load(props);
        this.storageMode = props.getProperty(ConfigReader.SQL_STORAGE_MODE, ConfigReader.JSON);
        this.sqlBatch = Integer.parseInt(props.getProperty(ConfigReader.GENERAL_SQL_BATCH, "1000"));
        this.startTime = props.getProperty(ConfigReader.GENERAL_START_TIME, ConfigReader.DEFAULT_START_TIME);
        loadMysqlConfig(props);
        if (isWriteToFile()) {
            loadFileConfig(props);
        } else {
            loadOpengaussConfig(props);
            this.isDropPreviousSql = Boolean.parseBoolean(props.getProperty(ConfigReader.SQL_TABLE_DROP,
                    "false"));
        }
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
    public boolean isWriteToFile() {
        return ConfigReader.JSON.equalsIgnoreCase(storageMode);
    }
}
