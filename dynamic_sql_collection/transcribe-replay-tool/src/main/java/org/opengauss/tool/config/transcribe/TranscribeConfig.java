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
import org.opengauss.tool.config.DatabaseConfig;
import org.opengauss.tool.config.FileConfig;
import org.opengauss.tool.config.RemoteConfig;
import org.opengauss.tool.utils.ConfigReader;

import java.util.Properties;

/**
 * Description: Transcribe config
 *
 * @author : wang_zhengyuan
 * @since : 2024/05/25
 */
@Data
public class TranscribeConfig {
    /**
     * Transcribe mode
     */
    protected String transcribeMode;

    /**
     * Sql storage mode
     */
    protected String storageMode;

    /**
     * File config
     */
    protected FileConfig fileConfig;

    /**
     * openGauss config
     */
    protected DatabaseConfig opengaussConfig;

    /**
     * MySQL config
     */
    protected DatabaseConfig mysqlConfig;

    /**
     * Remote node config
     */
    protected RemoteConfig remoteConfig;

    /**
     * CPU max threshold
     */
    protected double cpuThreshold;

    /**
     * System memory max threshold
     */
    protected double memoryThreshold;

    /**
     * Current file system disk max threshold
     */
    protected double diskThreshold;

    /**
     * Transcribe plugin path
     */
    protected String pluginPath;

    /**
     * Capture packet or sql duration
     */
    protected int captureDuration;

    /**
     * Config path
     */
    protected String configPath;

    /**
     * Is drop previous sql table
     */
    protected boolean isDropPreviousSql;

    /**
     * Should check local system resource
     */
    protected boolean shouldCheckResource;

    /**
     * Should send file to remote
     */
    protected boolean shouldSendFile;

    /**
     * Load transcribe config
     *
     * @param props Properties the props
     */
    public void load(Properties props) {
        this.transcribeMode = props.getProperty(ConfigReader.SQL_TRANSCRIBE_MODE);
        if (shouldCheckResource()) {
            loadSystemConfig(props);
        }
        if (shouldSendFile()) {
            loadRemoteConfig(props);
        }
    }

    /**
     * Is send file to remote node
     *
     * @return true if send file to remote node
     */
    public boolean shouldSendFile() {
        return false;
    }

    private void loadSystemConfig(Properties props) {
        this.cpuThreshold = Float.parseFloat(props.getProperty(ConfigReader.MAX_CPU_THRESHOLD,
                ConfigReader.DEFAULT_DOUBLE_VALUE));
        this.memoryThreshold = Float.parseFloat(props.getProperty(ConfigReader.MAX_MEMORY_THRESHOLD,
                ConfigReader.DEFAULT_DOUBLE_VALUE));
        this.diskThreshold = Float.parseFloat(props.getProperty(ConfigReader.MAX_DISK_THRESHOLD,
                ConfigReader.DEFAULT_DOUBLE_VALUE));
    }

    /**
     * Should check system resource
     *
     * @return true if check system resource
     */
    public boolean shouldCheckResource() {
        return false;
    }

    /**
     * Load openGauss config
     *
     * @param props Properties the props
     */
    protected void loadOpengaussConfig(Properties props) {
        opengaussConfig = new DatabaseConfig();
        opengaussConfig.loadOpengaussConnectionConfig(props);
    }

    /**
     * Load MySQL config
     *
     * @param props Properties the props
     */
    protected void loadMysqlConfig(Properties props) {
        mysqlConfig = new DatabaseConfig();
        mysqlConfig.loadMysqlConnectionConfig(props);
    }

    /**
     * Load file config
     *
     * @param props Properties the props
     */
    protected void loadFileConfig(Properties props) {
        fileConfig = new FileConfig();
        fileConfig.setFileCount(Integer.parseInt(props.getProperty(ConfigReader.FILE_COUNT_LIMIT, "100")));
    }

    private void loadRemoteConfig(Properties props) {
        remoteConfig = new RemoteConfig();
        remoteConfig.load(props);
    }

    /**
     * Is write sql or packet to file
     *
     * @return true if write content to file
     */
    public boolean isWriteToFile() {
        return true;
    }

    /**
     * Get command line pid
     *
     * @return long the pid
     */
    public long getPid() {
        return -1;
    }

    /**
     * Get network interface to capture
     *
     * @return String the name of network interface
     */
    public String getNetworkInterface() {
        return "eth0";
    }

    /**
     * Get capture port of source database
     *
     * @return int the database port
     */
    public int getCapturePort() {
        return 3306;
    }

    /**
     * Get sql batch of every query
     *
     * @return int the sql batch
     */
    public int getSqlBatch() {
        return 10;
    }

    /**
     * Get general log start time
     *
     * @return String the start time
     */
    public String getStartTime() {
        return "1970-01-01 00:00:01";
    }
}
