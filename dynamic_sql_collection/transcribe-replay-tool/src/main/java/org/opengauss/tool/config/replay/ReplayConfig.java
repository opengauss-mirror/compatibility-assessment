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

package org.opengauss.tool.config.replay;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.tool.config.DatabaseConfig;
import org.opengauss.tool.utils.ConfigReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ReplayConfig
 *
 * @since 2024-07-01
 */
@Data
public class ReplayConfig {
    private String storageMode;
    private String fileCatalogue;
    private String fileName;
    private String strategy;
    private int multiple;
    private boolean isOnlyReplayQuery;
    private int maxPoolSize;
    private int durationDiff;
    private int slowSqlRule;
    private int slowThreshold;
    private String csvDir;
    private int slowSqlTopNum;
    private int collectIdThreshold;
    private List<String> sessionWhiteList;
    private List<String> sessionBlackList;
    private Map<String, String> schemaMap;
    private DatabaseConfig sourceDbConfig;
    private DatabaseConfig targetDbConfig;
    private int replayMaxTime;
    private boolean isSourceTimeInterval;
    private boolean isCompareResult;
    private String selectResultPath;
    private String resultFileName;
    private boolean isRecordProcess;

    /**
     * load replayConfig from props
     *
     * @param props props
     */
    public void load(Properties props) {
        this.storageMode = props.getProperty(ConfigReader.SQL_STORAGE_MODE);
        this.fileCatalogue = props.getProperty(ConfigReader.SQL_FILE_PATH);
        this.fileName = props.getProperty(ConfigReader.SQL_FILE_NAME);
        this.strategy = props.getProperty(ConfigReader.SQL_REPLAY_STRATEGY);
        this.multiple = Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_MULTIPLE, "1"));
        this.isOnlyReplayQuery = isOnlyQuery(props);
        this.maxPoolSize = ConfigReader.SERIAL_REPLAY.equals(strategy) ? 1
                : Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_MAX_POOL_SIZE, "1"));
        this.slowSqlRule = Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_SLOW_SQL_RULE, "2"));
        this.durationDiff = Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_SLOW_SQL_TIME_DIFF, "1000"));
        this.slowThreshold = Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_SLOW_SQL_DURATION_THRESHOLD,
                "1000"));
        this.csvDir = props.getProperty(ConfigReader.SQL_REPLAY_SLOW_SQL_CSV_DIR);
        this.slowSqlTopNum = Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_SLOW_TOP_NUM, "5"));
        this.collectIdThreshold = Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_DRAW_THRESHOLD, "1000"));
        this.sessionWhiteList = getSessionList(props.getProperty(ConfigReader.SQL_REPLAY_SESSION_WHITE_LIST, "[]"));
        this.sessionBlackList = getSessionList(props.getProperty(ConfigReader.SQL_REPLAY_SESSION_BLACK_LIST, "[]"));
        this.schemaMap = getSchemaMapping(props.getProperty(ConfigReader.SQL_REPLAY_DATABASE_SCHEMA_MAP));
        this.replayMaxTime = Integer.parseInt(props.getProperty(ConfigReader.REPLAY_MAX_TIME, "0"));
        this.isSourceTimeInterval = Boolean.parseBoolean(
                props.getProperty(ConfigReader.SOURCE_TIME_INTERVAL_REPLAY, "true"));
        this.isRecordProcess = Boolean.parseBoolean(
            props.getProperty(ConfigReader.SQL_REPLAY_RECORD_PROCESS, "true"));
        this.isCompareResult = Boolean.parseBoolean(props.getProperty(ConfigReader.COMPARE_SELECT_RESULT, "false"));
        this.selectResultPath = props.getProperty(ConfigReader.SELECT_RESULT_PATH);
        this.resultFileName = props.getProperty(ConfigReader.RESULT_FILE_NAME);
        targetDbConfig = new DatabaseConfig();
        targetDbConfig.setDbIp(props.getProperty(ConfigReader.SQL_REPLAY_DATABASE_IP));
        targetDbConfig.setDbPort(props.getProperty(ConfigReader.SQL_REPLAY_DATABASE_PORT));
        targetDbConfig.setUsername(props.getProperty(ConfigReader.SQL_REPLAY_DATABASE_USERNAME));
        targetDbConfig.setPassword(props.getProperty(ConfigReader.SQL_REPLAY_DATABASE_PASSWORD));
        targetDbConfig.setCluster(targetDbConfig.getDbIp().contains(","));
        if (ConfigReader.DB.equalsIgnoreCase(storageMode)) {
            sourceDbConfig = new DatabaseConfig();
            sourceDbConfig.loadOpengaussConnectionConfig(props);
        }
    }

    private Map<String, String> getSchemaMapping(String prop) {
        Map<String, String> propSchemaMap = new HashMap<>();
        String[] schemaMapArray = prop.split(";");
        for (String schemaMapStr : schemaMapArray) {
            int separateIndex = schemaMapStr.indexOf(":");
            propSchemaMap.put(schemaMapStr.substring(0, separateIndex), schemaMapStr.substring(separateIndex + 1));
        }
        propSchemaMap.put(ConfigReader.SLOW_DB, schemaMapArray[0].substring(schemaMapArray[0].indexOf(":") + 1));
        return propSchemaMap;
    }


    private List<String> getSessionList(String prop) {
        String sessionStr = prop.substring(1, prop.length() - 1);
        return StringUtils.isEmpty(sessionStr) ? Collections.emptyList() : Arrays.asList(sessionStr.split(";"));
    }

    private boolean isOnlyQuery(Properties props) {
        if (Integer.parseInt(props.getProperty(ConfigReader.SQL_REPLAY_MULTIPLE, "1")) > 1) {
            return true;
        } else {
            return Boolean.parseBoolean(props.getProperty(ConfigReader.SQL_REPLAY_ONLY_QUERY, "false"));
        }
    }
}
