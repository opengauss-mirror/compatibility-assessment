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

package org.opengauss.tool.config;

import lombok.Data;
import org.opengauss.tool.utils.ConfigReader;

import java.util.Properties;

/**
 * Description: Database config
 *
 * @author : wang_zhengyuan
 * @since : 2024/06/26
 */
@Data
public class DatabaseConfig {
    private String dbIp;
    private String dbPort;
    private String username;
    private String dbName;
    private String password;
    private String tableName;
    private boolean isCluster;

    /**
     * Load openGauss connection config
     *
     * @param props Properties the props
     */
    public void loadOpengaussConnectionConfig(Properties props) {
        this.dbIp = props.getProperty(ConfigReader.SQL_DATABASE_IP);
        this.dbPort = props.getProperty(ConfigReader.SQL_DATABASE_PORT);
        this.username = props.getProperty(ConfigReader.SQL_DATABASE_USERNAME);
        this.dbName = props.getProperty(ConfigReader.SQL_DATABASE_NAME);
        this.password = props.getProperty(ConfigReader.SQL_DATABASE_PASSWORD);
        this.tableName = props.getProperty(ConfigReader.SQL_TABLE_NAME, ConfigReader.DEFAULT_SQL_TABLE);
    }

    /**
     * Load MySQL connection config
     *
     * @param props Properties the props
     */
    public void loadMysqlConnectionConfig(Properties props) {
        this.dbIp = props.getProperty(ConfigReader.GENERAL_DATABASE_IP);
        this.dbPort = props.getProperty(ConfigReader.GENERAL_DATABASE_PORT, "3306");
        this.username = props.getProperty(ConfigReader.GENERAL_DATABASE_USERNAME);
        this.dbName = "mysql";
        this.password = props.getProperty(ConfigReader.GENERAL_DATABASE_PASSWORD);
        this.tableName = "general_log";
    }
}
