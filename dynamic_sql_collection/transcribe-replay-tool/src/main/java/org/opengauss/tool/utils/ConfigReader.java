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

package org.opengauss.tool.utils;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.tool.config.parse.ParseConfig;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.config.transcribe.AttachConfig;
import org.opengauss.tool.config.transcribe.GeneralLogConfig;
import org.opengauss.tool.config.transcribe.TcpdumpConfig;
import org.opengauss.tool.config.transcribe.TranscribeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Description: Config reader
 *
 * @author wangzhengyuan
 * @since 2024/05/17
 **/
public final class ConfigReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigReader.class);
    private static final Map<String, Boolean> CONFIG_MAP = new HashMap<>();

    /**
     * default sql file path
     */
    public static final String DEFAULT_SQL_FILES = "sql-files";

    /**
     * default sql file name
     */
    public static final String DEFAULT_SQL_FILE = "sql-file";

    /**
     * default sql table name
     */
    public static final String DEFAULT_SQL_TABLE = "sql_table";

    /**
     * default tcpdump file path
     */
    public static final String DEFAULT_TCPDUMP_FILES = "tcpdump-files";

    /**
     * default tcpdump file name
     */
    public static final String DEFAULT_TCPDUMP_FILE = "tcpdump-file";

    /**
     * default start time
     */
    public static final String DEFAULT_START_TIME = "1970-01-01 00:00:01";

    /**
     * default double value
     */
    public static final String DEFAULT_DOUBLE_VALUE = "0.85";

    /**
     * default plugin path
     */
    public static final String PLUGIN = "plugin";

    /**
     * command type key
     */
    public static final String COMMAND_TYPE = "-t";

    /**
     * config file key
     */
    public static final String CONFIG_FILE = "-f";

    /**
     * json
     */
    public static final String JSON = "json";

    /**
     * db
     */
    public static final String DB = "db";

    /**
     * tcpdump
     */
    public static final String TCPDUMP = "tcpdump";

    /**
     * attach
     */
    public static final String ATTACH = "attach";

    /**
     * general
     */
    public static final String GENERAL = "general";

    /**
     * transcribe
     */
    public static final String TRANSCRIBE = "transcribe";

    /**
     * parse
     */
    public static final String PARSE = "parse";

    // parse
    /**
     * sql file path
     */
    public static final String SQL_FILE_PATH = "sql.file.path";

    /**
     * tcpdump source database server type
     */
    public static final String TCPDUMP_DATABASE_TYPE = "tcpdump.database.type";

    /**
     * tcpdump database ip
     */
    public static final String TCPDUMP_DATABASE_IP = "tcpdump.database.ip";

    /**
     * sql file size
     */
    public static final String SQL_FILE_SIZE = "sql.file.size";

    /**
     * sql file name
     */
    public static final String SQL_FILE_NAME = "sql.file.name";

    /**
     * queue size limit
     */
    public static final String QUEUE_SIZE_LIMIT = "queue.size.limit";

    /**
     * packet batch size
     */
    public static final String PACKET_BATCH_SIZE = "packet.batch.size";

    // transcribe
    // general parameters
    /**
     * sql transcribe mode
     */
    public static final String SQL_TRANSCRIBE_MODE = "sql.transcribe.mode";

    // tcpdump
    /**
     * tcpdump plugin path
     */
    public static final String TCPDUMP_PLUGIN_PATH = "tcpdump.plugin.path";

    /**
     * tcpdump network interface
     */
    public static final String TCPDUMP_NETWORK_INTERFACE = "tcpdump.network.interface";

    /**
     * tcpdump database port
     */
    public static final String TCPDUMP_DATABASE_PORT = "tcpdump.database.port";

    /**
     * tcpdump capture duration
     */
    public static final String TCPDUMP_CAPTURE_DURATION = "tcpdump.capture.duration";

    /**
     * tcpdump file path
     */
    public static final String TCPDUMP_FILE_PATH = "tcpdump.file.path";

    /**
     * tcpdump file name
     */
    public static final String TCPDUMP_FILE_NAME = "tcpdump.file.name";

    /**
     * tcpdump file size
     */
    public static final String TCPDUMP_FILE_SIZE = "tcpdump.file.size";

    /**
     * file count limit
     */
    public static final String FILE_COUNT_LIMIT = "file.count.limit";

    // attach
    /**
     * attach plugin path
     */
    public static final String ATTACH_PLUGIN_PATH = "attach.plugin.path";

    /**
     * attach process pid
     */
    public static final String ATTACH_PROCESS_PID = "attach.process.pid";

    /**
     * attach target schema
     */
    public static final String ATTACH_TARGET_SCHEMA = "attach.target.schema";

    /**
     * attach capture duration
     */
    public static final String ATTACH_CAPTURE_DURATION = "attach.capture.duration";

    // tcpdump/attach

    /**
     * Should check system resource
     */
    public static final String SHOULD_CHECK_SYSTEM = "should.check.system";

    /**
     * max cpu threshold
     */
    public static final String MAX_CPU_THRESHOLD = "max.cpu.threshold";

    /**
     * max memory threshold
     */
    public static final String MAX_MEMORY_THRESHOLD = "max.memory.threshold";

    /**
     * max disk threshold
     */
    public static final String MAX_DISK_THRESHOLD = "max.disk.threshold";

    /**
     * Should send files to a remote location
     */
    public static final String SHOULD_SEND_FILE = "should.send.file";

    /**
     * remote file path
     */
    public static final String REMOTE_FILE_PATH = "remote.file.path";

    /**
     * remote receiver name
     */
    public static final String REMOTE_RECEIVER_NAME = "remote.receiver.name";

    /**
     * remote receiver password
     */
    public static final String REMOTE_RECEIVER_PASSWORD = "remote.receiver.password";

    /**
     * remote node ip
     */
    public static final String REMOTE_NODE_IP = "remote.node.ip";

    /**
     * remote node port
     */
    public static final String REMOTE_NODE_PORT = "remote.node.port";

    /**
     * remote retry count
     */
    public static final String REMOTE_RETRY_COUNT = "remote.retry.count";

    // general log
    /**
     * general database ip
     */
    public static final String GENERAL_DATABASE_IP = "general.database.ip";

    /**
     * general database port
     */
    public static final String GENERAL_DATABASE_PORT = "general.database.port";

    /**
     * general database username
     */
    public static final String GENERAL_DATABASE_USERNAME = "general.database.username";

    /**
     * general database password
     */
    public static final String GENERAL_DATABASE_PASSWORD = "general.database.password";

    /**
     * general sql batch
     */
    public static final String GENERAL_SQL_BATCH = "general.sql.batch";

    /**
     * general log start time
     */
    public static final String GENERAL_START_TIME = "general.start.time";

    // attach/general log
    /**
     * sql storage mode
     */
    public static final String SQL_STORAGE_MODE = "sql.storage.mode";

    // database
    /**
     * sql database ip
     */
    public static final String SQL_DATABASE_IP = "sql.database.ip";

    /**
     * sql database port
     */
    public static final String SQL_DATABASE_PORT = "sql.database.port";

    /**
     * sql database username
     */
    public static final String SQL_DATABASE_USERNAME = "sql.database.username";

    /**
     * sql database name
     */
    public static final String SQL_DATABASE_NAME = "sql.database.name";

    /**
     * sql database password
     */
    public static final String SQL_DATABASE_PASSWORD = "sql.database.password";

    /**
     * sql table name
     */
    public static final String SQL_TABLE_NAME = "sql.table.name";

    /**
     * sql table drop
     */
    public static final String SQL_TABLE_DROP = "sql.table.drop";

    /**
     * replay
     */
    public static final String REPLAY = "replay";

    /**
     * serial replay
     */
    public static final String SERIAL_REPLAY = "serial";

    /**
     * parallel replay
     */
    public static final String PARALLEL_REPLAY = "parallel";

    /**
     * replay strategy list
     */
    public static final List<String> REPLAY_STRATEGY_LIST = Collections.unmodifiableList(
            Arrays.asList(SERIAL_REPLAY, PARALLEL_REPLAY));

    /**
     * slow sql strategy list
     */
    public static final List<String> SLOW_SQL_STRATEGY_LIST = Collections.unmodifiableList(Arrays.asList("1", "2"));

    /**
     * sql replay database ip
     */
    public static final String SQL_REPLAY_DATABASE_IP = "sql.replay.database.ip";

    /**
     * sql replay database port"
     */
    public static final String SQL_REPLAY_DATABASE_PORT = "sql.replay.database.port";

    /**
     * sql replay database username
     */
    public static final String SQL_REPLAY_DATABASE_USERNAME = "sql.replay.database.username";

    /**
     * sql replay database name
     */
    public static final String SQL_REPLAY_DATABASE_SCHEMA_MAP = "sql.replay.database.schema.map";

    /**
     * sql replay database password
     */
    public static final String SQL_REPLAY_DATABASE_PASSWORD = "sql.replay.database.password";

    /**
     * sql replay strategy
     */
    public static final String SQL_REPLAY_STRATEGY = "sql.replay.strategy";

    /**
     * sql replay multiple
     */
    public static final String SQL_REPLAY_MULTIPLE = "sql.replay.multiple";

    /**
     * sql replay only query
     */
    public static final String SQL_REPLAY_ONLY_QUERY = "sql.replay.only.query";

    /**
     * sql replay source
     */
    public static final String SQL_REPLAY_SOURCE = "sql.replay.source";

    /**
     * sql replay parallel max pool size
     */
    public static final String SQL_REPLAY_MAX_POOL_SIZE = "sql.replay.parallel.max.pool.size";

    /**
     * sql replay slow time difference threshold
     */
    public static final String SQL_REPLAY_SLOW_SQL_TIME_DIFF = "sql.replay.slow.time.difference.threshold";

    /**
     * sql replay slow sql duration threshold
     */
    public static final String SQL_REPLAY_SLOW_SQL_DURATION_THRESHOLD = "sql.replay.slow.sql.duration.threshold";

    /**
     * sql replay slow sql csv dir
     */
    public static final String SQL_REPLAY_SLOW_SQL_CSV_DIR = "sql.replay.slow.sql.csv.dir";

    /**
     * sql replay slow sql rule
     */
    public static final String SQL_REPLAY_SLOW_SQL_RULE = "sql.replay.slow.sql.rule";

    /**
     * sql replay slow top number
     */
    public static final String SQL_REPLAY_SLOW_TOP_NUM = "sql.replay.slow.top.number";

    /**
     * sql replay draw threshold
     */
    public static final String SQL_REPLAY_DRAW_THRESHOLD = "sql.replay.draw.interval";

    /**
     * sql replay session white list
     */
    public static final String SQL_REPLAY_SESSION_WHITE_LIST = "sql.replay.session.white.list";

    /**
     * sql replay session black list
     */
    public static final String SQL_REPLAY_SESSION_BLACK_LIST = "sql.replay.session.black.list";

    /**
     * slow db
     */
    public static final String SLOW_DB = "slow_db";

    /**
     * Initialize transcribe config
     *
     * @param configPath String the config path
     * @return TranscribeConfig the config
     */
    public static TranscribeConfig initTranscribeConfig(String configPath) {
        Properties props = new Properties();
        TranscribeConfig config = null;
        try (InputStream input = Files.newInputStream(Paths.get(configPath))) {
            props.load(input);
            checkTranscribeConfig(props);
            String transcribeMode = props.getProperty(ConfigReader.SQL_TRANSCRIBE_MODE);
            if (TCPDUMP.equalsIgnoreCase(transcribeMode)) {
                config = new TcpdumpConfig();
            } else if (ATTACH.equalsIgnoreCase(transcribeMode)) {
                config = new AttachConfig();
                config.setConfigPath(configPath);
            } else {
                config = new GeneralLogConfig();
            }
            config.load(props);
        } catch (IOException e) {
            LOGGER.error("IOException occurred while read config file {}, error message is: {}.",
                    configPath, e.getMessage());
        }
        return config;
    }

    private static void checkTranscribeConfig(Properties props) {
        String transcribeMode = props.getProperty(ConfigReader.SQL_TRANSCRIBE_MODE);
        if (!TCPDUMP.equals(transcribeMode) && !ATTACH.equals(transcribeMode) && !GENERAL.equals(transcribeMode)) {
            LOGGER.error("Transcribe mode is not supported, {} must be one of the {}, {}, {}.", SQL_TRANSCRIBE_MODE,
                    TCPDUMP, ATTACH, GENERAL);
            System.exit(-1);
        }
        if (TCPDUMP.equals(transcribeMode)) {
            putTranscribeConfig(props);
            putSystemConfig(props);
        } else if (ATTACH.equals(transcribeMode)) {
            putAttachConfig(props);
            putSystemConfig(props);
        } else {
            putGeneralDatabaseConfig(props);
            String sqlStorageMode = checkSqlStorageMode(props);
            if (JSON.equals(sqlStorageMode)) {
                putSqlFileConfig(props);
            } else {
                putSqlDbConfig(props);
            }
        }
        checkResult();
    }

    /**
     * Initialize parse config
     *
     * @param configPath String the config path
     * @return ParseConfig config
     */
    public static ParseConfig initParseConfig(String configPath) {
        Properties props = new Properties();
        ParseConfig config = new ParseConfig();
        try (InputStream input = Files.newInputStream(Paths.get(configPath))) {
            props.load(input);
            checkParseConfig(props);
            config.load(props);
        } catch (IOException ex) {
            LOGGER.error("IOException occurred while read config file {}, error message is: {}.", configPath,
                    ex.getMessage());
        }
        return config;
    }

    private static void checkParseConfig(Properties props) {
        putParseConfig(props);
        String sqlStorageMode = checkSqlStorageMode(props);
        if (JSON.equals(sqlStorageMode)) {
            putSqlFileConfig(props);
        } else {
            putSqlDbConfig(props);
        }
        checkResult();
    }

    /**
     * initReplayConfig
     *
     * @param configPath configPath
     * @return ReplayConfig
     */
    public static ReplayConfig initReplayConfig(String configPath) {
        Properties props = new Properties();
        ReplayConfig config = new ReplayConfig();
        try (InputStream input = Files.newInputStream(Paths.get(configPath))) {
            props.load(input);
            checkReplayConfig(props);
            config.load(props);
        } catch (IOException ex) {
            LOGGER.error("IOException occurred while read config file {}.", configPath);
        }
        return config;
    }

    private static void checkReplayConfig(Properties props) {
        putReplayConfig(props);
        String sqlStorageMode = checkSqlStorageMode(props);
        putTargetDbConfig(props);
        if (JSON.equals(sqlStorageMode)) {
            putSqlFileConfig(props);
        } else {
            putSqlDbConfig(props);
        }
        checkResult();
    }

    private static void putSqlDbConfig(Properties props) {
        CONFIG_MAP.put(SQL_DATABASE_IP, matchIp(props.getProperty(SQL_DATABASE_IP)));
        CONFIG_MAP.put(SQL_DATABASE_PORT, matchPort(props.getProperty(SQL_DATABASE_PORT)));
        CONFIG_MAP.put(SQL_DATABASE_USERNAME, matchRegularString(props.getProperty(SQL_DATABASE_USERNAME)));
        CONFIG_MAP.put(SQL_DATABASE_NAME, matchRegularString(props.getProperty(SQL_DATABASE_NAME)));
        CONFIG_MAP.put(SQL_DATABASE_PASSWORD, matchRegularString(props.getProperty(SQL_DATABASE_PASSWORD)));
        CONFIG_MAP.put(SQL_TABLE_NAME, matchRegularString(props.getProperty(SQL_TABLE_NAME, DEFAULT_SQL_TABLE)));
        CONFIG_MAP.put(SQL_TABLE_DROP, matchBoolean(props.getProperty(SQL_TABLE_DROP, "false")));
    }

    private static void putSqlFileConfig(Properties props) {
        CONFIG_MAP.put(SQL_FILE_PATH, matchFilePath(props.getProperty(SQL_FILE_PATH, FileOperator
                .CURRENT_PATH + DEFAULT_SQL_FILES + File.separator)));
        CONFIG_MAP.put(SQL_FILE_SIZE, matchNumber(props.getProperty(SQL_FILE_SIZE, "10")));
        CONFIG_MAP.put(SQL_FILE_NAME, matchRegularString(props.getProperty(SQL_FILE_NAME, DEFAULT_SQL_FILE)));
    }

    private static void putParseConfig(Properties props) {
        CONFIG_MAP.put(TCPDUMP_FILE_PATH, matchFilePath(props.getProperty(TCPDUMP_FILE_PATH)));
        CONFIG_MAP.put(TCPDUMP_DATABASE_TYPE, matchDbType(props.getProperty(TCPDUMP_DATABASE_TYPE)));
        CONFIG_MAP.put(TCPDUMP_DATABASE_IP, matchIp(props.getProperty(TCPDUMP_DATABASE_IP)));
        CONFIG_MAP.put(TCPDUMP_DATABASE_PORT, matchPort(props.getProperty(TCPDUMP_DATABASE_PORT)));
        CONFIG_MAP.put(QUEUE_SIZE_LIMIT, matchNumber(props.getProperty(QUEUE_SIZE_LIMIT, "10000")));
        CONFIG_MAP.put(PACKET_BATCH_SIZE, matchNumber(props.getProperty(PACKET_BATCH_SIZE, "10000")));
    }

    private static void putGeneralDatabaseConfig(Properties props) {
        CONFIG_MAP.put(GENERAL_DATABASE_IP, matchIp(props.getProperty(GENERAL_DATABASE_IP)));
        CONFIG_MAP.put(GENERAL_DATABASE_PORT, matchPort(props.getProperty(GENERAL_DATABASE_PORT)));
        CONFIG_MAP.put(GENERAL_DATABASE_USERNAME, matchRegularString(props.getProperty(GENERAL_DATABASE_USERNAME)));
        CONFIG_MAP.put(GENERAL_DATABASE_PASSWORD, matchRegularString(props.getProperty(GENERAL_DATABASE_PASSWORD)));
        CONFIG_MAP.put(GENERAL_SQL_BATCH, matchNumber(props.getProperty(GENERAL_SQL_BATCH, "1000")));
        CONFIG_MAP.put(GENERAL_START_TIME, matchTimestamp(props.getProperty(GENERAL_START_TIME, DEFAULT_START_TIME)));
    }

    private static void putAttachConfig(Properties props) {
        CONFIG_MAP.put(ATTACH_PLUGIN_PATH, matchFilePath(props.getProperty(ATTACH_PLUGIN_PATH,
                FileOperator.CURRENT_PATH + PLUGIN + File.separator)));
        CONFIG_MAP.put(ATTACH_PROCESS_PID, matchNumber(props.getProperty(ATTACH_PROCESS_PID)));
        CONFIG_MAP.put(ATTACH_TARGET_SCHEMA, matchRegularString(props.getProperty(ATTACH_TARGET_SCHEMA)));
        CONFIG_MAP.put(ATTACH_CAPTURE_DURATION, matchNumber(props.getProperty(ATTACH_CAPTURE_DURATION,
                "1")));
        putSqlFileConfig(props);
    }

    private static void putTranscribeConfig(Properties props) {
        CONFIG_MAP.put(TCPDUMP_PLUGIN_PATH, matchFilePath(props.getProperty(TCPDUMP_PLUGIN_PATH,
                FileOperator.CURRENT_PATH + PLUGIN + File.separator)));
        CONFIG_MAP.put(TCPDUMP_NETWORK_INTERFACE, matchRegularString(props.getProperty(TCPDUMP_NETWORK_INTERFACE)));
        CONFIG_MAP.put(TCPDUMP_DATABASE_PORT, matchPort(props.getProperty(TCPDUMP_DATABASE_PORT)));
        CONFIG_MAP.put(TCPDUMP_CAPTURE_DURATION, matchNumber(props.getProperty(TCPDUMP_CAPTURE_DURATION,
                "1")));
        CONFIG_MAP.put(TCPDUMP_FILE_PATH, matchFilePath(props.getProperty(TCPDUMP_FILE_PATH,
                FileOperator.CURRENT_PATH + DEFAULT_SQL_FILES + File.separator)));
        CONFIG_MAP.put(TCPDUMP_FILE_NAME, matchRegularString(props.getProperty(TCPDUMP_FILE_NAME,
                "sql-file")));
        CONFIG_MAP.put(TCPDUMP_FILE_SIZE, matchNumber(props.getProperty(TCPDUMP_FILE_SIZE, "10")));
        CONFIG_MAP.put(FILE_COUNT_LIMIT, matchNumber(props.getProperty(FILE_COUNT_LIMIT, "100")));
    }

    private static void putSystemConfig(Properties props) {
        putLocalSystemConfig(props);
        putRemoteSystemConfig(props);
    }

    private static void putLocalSystemConfig(Properties props) {
        String shouldCheckResource = props.getProperty(SHOULD_CHECK_SYSTEM, "true");
        boolean isMatchBoolean = matchBoolean(shouldCheckResource);
        if (!isMatchBoolean) {
            CONFIG_MAP.put(SHOULD_CHECK_SYSTEM, false);
            return;
        }
        if (Boolean.parseBoolean(shouldCheckResource)) {
            CONFIG_MAP.put(MAX_CPU_THRESHOLD, matchDouble(props.getProperty(MAX_CPU_THRESHOLD, DEFAULT_DOUBLE_VALUE)));
            CONFIG_MAP.put(MAX_MEMORY_THRESHOLD, matchDouble(props.getProperty(MAX_MEMORY_THRESHOLD,
                    DEFAULT_DOUBLE_VALUE)));
            CONFIG_MAP.put(MAX_DISK_THRESHOLD, matchDouble(props.getProperty(MAX_DISK_THRESHOLD,
                    DEFAULT_DOUBLE_VALUE)));
        }
    }

    private static void putRemoteSystemConfig(Properties props) {
        String shouldSendFile = props.getProperty(SHOULD_SEND_FILE, "true");
        boolean isMatchBoolean = matchBoolean(shouldSendFile);
        if (!isMatchBoolean) {
            CONFIG_MAP.put(SHOULD_SEND_FILE, false);
            return;
        }
        if (Boolean.parseBoolean(shouldSendFile)) {
            CONFIG_MAP.put(REMOTE_FILE_PATH, matchFilePath(props.getProperty(REMOTE_FILE_PATH)));
            CONFIG_MAP.put(REMOTE_RECEIVER_NAME, matchRegularString(props.getProperty(REMOTE_RECEIVER_NAME)));
            CONFIG_MAP.put(REMOTE_RECEIVER_PASSWORD, matchRegularString(props.getProperty(REMOTE_RECEIVER_PASSWORD,
                    "******")));
            CONFIG_MAP.put(REMOTE_NODE_IP, matchIp(props.getProperty(REMOTE_NODE_IP, "127.0.0.1")));
            CONFIG_MAP.put(REMOTE_NODE_PORT, matchPort(props.getProperty(REMOTE_NODE_PORT, "22")));
            CONFIG_MAP.put(REMOTE_RETRY_COUNT, matchNumber(props.getProperty(REMOTE_RETRY_COUNT, "1")));
        }
    }

    private static void putReplayConfig(Properties props) {
        CONFIG_MAP.put(SQL_REPLAY_STRATEGY, REPLAY_STRATEGY_LIST.contains(props.getProperty(SQL_REPLAY_STRATEGY)));
        CONFIG_MAP.put(SQL_REPLAY_MULTIPLE, matchNumber(props.getProperty(SQL_REPLAY_MULTIPLE, "1")));
        CONFIG_MAP.put(SQL_REPLAY_ONLY_QUERY, matchBoolean(props.getProperty(SQL_REPLAY_ONLY_QUERY)));
        CONFIG_MAP.put(SQL_REPLAY_MAX_POOL_SIZE, matchNumber(props.getProperty(SQL_REPLAY_MAX_POOL_SIZE, "1")));
        CONFIG_MAP.put(SQL_REPLAY_SLOW_SQL_RULE, SLOW_SQL_STRATEGY_LIST.contains(
                props.getProperty(SQL_REPLAY_SLOW_SQL_RULE, "2")));
        CONFIG_MAP.put(SQL_REPLAY_SLOW_SQL_TIME_DIFF, matchNumber(
                props.getProperty(SQL_REPLAY_SLOW_SQL_TIME_DIFF, "1000")));
        CONFIG_MAP.put(SQL_REPLAY_SLOW_SQL_DURATION_THRESHOLD, matchNumber(
                props.getProperty(SQL_REPLAY_SLOW_SQL_DURATION_THRESHOLD, "3000")));
        CONFIG_MAP.put(SQL_REPLAY_SLOW_SQL_CSV_DIR, matchFilePath(props.getProperty(SQL_REPLAY_SLOW_SQL_CSV_DIR)));
        CONFIG_MAP.put(SQL_REPLAY_SLOW_TOP_NUM, matchNumber(props.getProperty(SQL_REPLAY_SLOW_TOP_NUM, "5")));
        CONFIG_MAP.put(SQL_REPLAY_DRAW_THRESHOLD, matchNumber(props.getProperty(SQL_REPLAY_DRAW_THRESHOLD, "1000")));
        CONFIG_MAP.put(SQL_REPLAY_SESSION_WHITE_LIST, matchSessionList(
                props.getProperty(SQL_REPLAY_SESSION_WHITE_LIST, "[]")));
        CONFIG_MAP.put(SQL_REPLAY_SESSION_BLACK_LIST, matchSessionList(
                props.getProperty(SQL_REPLAY_SESSION_BLACK_LIST, "[]")));
    }

    private static void putTargetDbConfig(Properties props) {
        CONFIG_MAP.put(SQL_REPLAY_DATABASE_IP, matchIps(props.getProperty(SQL_REPLAY_DATABASE_IP)));
        CONFIG_MAP.put(SQL_REPLAY_DATABASE_PORT, matchPorts(props.getProperty(SQL_REPLAY_DATABASE_PORT)));
        CONFIG_MAP.put(SQL_REPLAY_DATABASE_SCHEMA_MAP, matchSchemaMap(
                props.getProperty(SQL_REPLAY_DATABASE_SCHEMA_MAP)));
        CONFIG_MAP.put(SQL_REPLAY_DATABASE_USERNAME, matchRegularString(
                props.getProperty(SQL_REPLAY_DATABASE_USERNAME)));
        CONFIG_MAP.put(SQL_REPLAY_DATABASE_PASSWORD, matchRegularString(
                props.getProperty(SQL_REPLAY_DATABASE_PASSWORD)));
    }

    private static String checkSqlStorageMode(Properties props) {
        String sqlStorageMode = props.getProperty(SQL_STORAGE_MODE, JSON);
        if (!sqlStorageMode.equals(JSON) && !sqlStorageMode.equals(DB)) {
            LOGGER.error("SQL storage mode is not supported, {} value must be one of the {}, {}.",
                    SQL_STORAGE_MODE, JSON, DB);
            System.exit(-1);
        }
        return sqlStorageMode;
    }

    private static void checkResult() {
        boolean isMatchFormat = true;
        for (Map.Entry<String, Boolean> entry : CONFIG_MAP.entrySet()) {
            if (!entry.getValue()) {
                LOGGER.error("{} is not configured or formatted incorrectly, please check it", entry.getKey());
                isMatchFormat = false;
            }
        }
        if (!isMatchFormat) {
            System.exit(-1);
        }
    }


    private static boolean matchBoolean(String dropTable) {
        if (dropTable == null) {
            return false;
        }
        return "true".equalsIgnoreCase(dropTable) || "false".equalsIgnoreCase(dropTable);
    }

    private static boolean matchRegularString(String username) {
        return username != null && !username.trim().isEmpty();
    }

    private static Boolean matchDouble(String decimal) {
        if ("1".equals(decimal)) {
            return true;
        }
        String regex = "^\\d+(\\.\\d+)$";
        if (!decimal.matches(regex)) {
            return false;
        }
        double res = Double.parseDouble(decimal);
        return res <= 1.0d && res > 0.0d;
    }

    private static boolean matchNumber(String port) {
        if (port == null) {
            return false;
        }
        String regex = "^[1-9][0-9]*$";
        return port.matches(regex);
    }

    private static boolean matchIp(String ip) {
        try {
            InetAddress.getByName(ip);
            return true;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    private static boolean matchFilePath(String filePath) {
        return !"".equals(filePath) && filePath != null;
    }

    private static Boolean matchTimestamp(String timestamp) {
        if (timestamp == null) {
            return false;
        }
        String regex = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$";
        return timestamp.matches(regex);
    }

    private static Boolean matchSessionList(String sessionProp) {
        if (StringUtils.isEmpty(sessionProp) || "[]".equals(sessionProp)) {
            return true;
        }
        String[] sessionList = sessionProp.substring(1, sessionProp.length() - 1).split(";");
        int portIndex;
        String ip;
        String port = null;
        for (String session : sessionList) {
            if (session.contains("[")) {
                // ipv6
                portIndex = session.indexOf("]:");
                if (portIndex == -1) {
                    ip = session.substring(1, session.length() - 1);
                } else {
                    ip = session.substring(1, portIndex);
                    port = session.substring(portIndex + 2, session.length() - 1);
                }
            } else {
                // ipv4
                portIndex = session.indexOf(":");
                if (portIndex == -1) {
                    ip = session;
                } else {
                    ip = session.substring(0, portIndex);
                    port = session.substring(portIndex + 1);
                }
            }
            if (!matchIp(ip) || (port != null && !matchPort(port))) {
                return false;
            }
        }
        return true;
    }

    private static Boolean matchIps(String prop) {
        String[] ipArray = prop.split(",");
        if (ipArray.length == 0) {
            return false;
        }
        for (String ip : ipArray) {
            if (!matchIp(ip)) {
                return false;
            }
        }
        return true;
    }

    private static Boolean matchPorts(String prop) {
        String[] portArray = prop.split(",");
        if (portArray.length == 0) {
            return false;
        }
        for (String port : portArray) {
            if (!matchPort(port)) {
                return false;
            }
        }
        return true;
    }

    private static Boolean matchSchemaMap(String schemas) {
        String[] schemaMapArray = schemas.split(";");
        if (schemaMapArray.length == 0) {
            return false;
        }
        for (String schemaMapStr : schemaMapArray) {
            int separateIndex = schemaMapStr.indexOf(":");
            if (separateIndex < 0) {
                return false;
            }
            String mysqlSchema = schemaMapStr.substring(0, separateIndex);
            String ogSchema = schemaMapStr.substring(separateIndex + 1);
            if (!matchRegularString(mysqlSchema) || !matchRegularString(ogSchema)) {
                return false;
            }
        }
        return true;
    }

    private static Boolean matchPort(String port) {
        if (!matchNumber(port)) {
            return false;
        }
        return Integer.parseInt(port) > 0 && Integer.parseInt(port) <= 65535;
    }

    private static Boolean matchDbType(String tcpdumpDatabaseType) {
        return "mysql".equalsIgnoreCase(tcpdumpDatabaseType);
    }
}
