/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.agent.transcribe;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.kit.agent.common.Constant;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;

/**
 * Sql catcher
 *
 * @author wang_zhengyuan
 * @since 2024-06-07
 */
@Slf4j
public class SqlCatcher {
    private static final String ATTACH_TARGET_SCHEMA = "attach.target.schema";
    private static final String SQL_FILE_PATH = "sql.file.path";
    private static final String SQL_FILE_NAME = "sql.file.name";
    private static final String SQL_FILE_SIZE = "sql.file.size";

    private JSONObject json;
    private String sqlFilePath;
    private String sqlFileName;
    private int fileSizeLimit;
    private long sqlId;
    private int fileId = 1;
    private String schema;

    /**
     * Constructor
     *
     * @param configPath String the attach config path
     */
    public SqlCatcher(String configPath) {
        loadConfig(configPath);
        this.json = JSONUtil.createObj();
    }

    /**
     * Capture sql
     *
     * @param sql String the sql
     */
    public void catchSql(String sql) {
        String str = sql;
        if (str.contains("com.mysql")) {
            str = str.substring(str.indexOf(":") + 1);
        }
        str = Constant.dealQuery(str);
        sqlId++;
        String res = formatSql(str);
        writeSqlToFile(res);
    }

    private void writeSqlToFile(String res) {
        String fileFullPath = sqlFilePath + sqlFileName + "-" + fileId + ".json";
        File sqlFile = new File(fileFullPath);
        if (!sqlFile.exists()) {
            try {
                sqlFile.createNewFile();
            } catch (IOException e) {
                log.error("IOException occurred while creating sql file {}, error message is: {}.",
                        fileFullPath, e.getMessage());
            }
            writeSqlToFile(res);
        } else if (sqlFile.length() >= (long) fileSizeLimit * 1024 * 1024) {
            fileId++;
            writeSqlToFile(res);
        } else {
            Constant.writeStringToFile(res, fileFullPath);
        }
    }

    private String formatSql(String sql) {
        boolean isQuery = identifySqlType(sql);
        json.set("id", sqlId)
                .set("isQuery", isQuery)
                .set("isPrepared", false)
                .set("session", "Stack Trace")
                .set("username", "client_user")
                .set("schema", schema)
                .set("sql", sql)
                .set("parameters", new ArrayList<String>());
        return json.toString();
    }

    private boolean identifySqlType(String sql) {
        String upperSql = sql.toUpperCase(Locale.ROOT);
        return upperSql.startsWith("SELECT") || upperSql.startsWith("SHOW");
    }

    private void loadConfig(String configPath) {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(configPath)) {
            props.load(input);
            this.schema = props.getProperty(ATTACH_TARGET_SCHEMA);
            loadFileConfig(props);
        } catch (IOException e) {
            log.error("IOException occurred while read config file {}, error message is: {}.",
                    configPath, e.getMessage());
        }
    }

    private void loadFileConfig(Properties props) {
        this.sqlFilePath = props.getProperty(SQL_FILE_PATH, getCurrentPath()
                + "sql-files" + File.separator);
        if (!sqlFilePath.endsWith(File.separator)) {
            sqlFilePath += File.separator;
        }
        this.sqlFileName = props.getProperty(SQL_FILE_NAME, "sql-file");
        this.fileSizeLimit = Integer.parseInt(props.getProperty(SQL_FILE_SIZE, "10"));
    }

    private static String getCurrentPath() {
        String path = SqlCatcher.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        StringBuilder sb = new StringBuilder();
        String[] paths = path.split(File.separator);
        for (int i = 0; i < paths.length - 2; i++) {
            sb.append(paths[i]).append(File.separator);
        }
        return sb.toString();
    }
}
