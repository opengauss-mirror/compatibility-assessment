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

package org.opengauss.tool.config.parse;

import lombok.Data;
import org.opengauss.tool.config.DatabaseConfig;
import org.opengauss.tool.config.FileConfig;
import org.opengauss.tool.utils.ConfigReader;
import org.opengauss.tool.utils.FileOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Description: The configure class to parse MySQL packet file.
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/17
 */
@Data
public class ParseConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseConfig.class);

    private FileConfig fileConfig;
    private DatabaseConfig opengaussConfig;
    private String storageMode;
    private String packetFilePath;
    private String mysqlServerIp;
    private int mysqlServerPort;
    private int queueSizeLimit;
    private int packetBatchSize;
    private boolean isDropPreviousSql;

    /**
     * Load parse configure properties
     *
     * @param props Properties the props
     */
    public void load(Properties props) {
        this.storageMode = props.getProperty(ConfigReader.SQL_STORAGE_MODE, ConfigReader.JSON);
        this.packetFilePath = props.getProperty(ConfigReader.TCPDUMP_FILE_PATH);
        this.mysqlServerIp = props.getProperty(ConfigReader.TCPDUMP_DATABASE_IP);
        this.mysqlServerPort = Integer.parseInt(props.getProperty(ConfigReader.TCPDUMP_DATABASE_PORT));
        this.queueSizeLimit = Integer.parseInt(props.getProperty(ConfigReader.QUEUE_SIZE_LIMIT, "10000"));
        this.packetBatchSize = Integer.parseInt(props.getProperty(ConfigReader.PACKET_BATCH_SIZE, "10000"));
        if (ConfigReader.JSON.equalsIgnoreCase(storageMode)) {
            loadFileConfig(props);
        } else {
            this.isDropPreviousSql = Boolean.parseBoolean(props.getProperty(ConfigReader.SQL_TABLE_DROP,
                    "false"));
            loadOpengaussConfig(props);
        }
    }

    /**
     * Load file configure about packet files
     *
     * @param props Properties the props
     */
    public void loadFileConfig(Properties props) {
        fileConfig = new FileConfig();
        fileConfig.setFilePath(FileOperator.formatFilePath(props.getProperty(ConfigReader.SQL_FILE_PATH,
                FileOperator.CURRENT_PATH + ConfigReader.DEFAULT_SQL_FILES + File.separator)));
        fileConfig.setFileName(props.getProperty(ConfigReader.SQL_FILE_NAME, ConfigReader.DEFAULT_SQL_FILE));
        fileConfig.setFileSize(Integer.parseInt(props.getProperty(ConfigReader.SQL_FILE_SIZE, "10")));
    }

    /**
     * Load openGauss connection configure to storage sql
     *
     * @param props Properties the props
     */
    public void loadOpengaussConfig(Properties props) {
        opengaussConfig = new DatabaseConfig();
        opengaussConfig.loadOpengaussConnectionConfig(props);
    }
}
