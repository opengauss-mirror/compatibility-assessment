/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2022. All rights reserved.
 */

package org.kit.agent.common;

import cn.hutool.core.util.StrUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.kit.agent.utils.DataUtil;

/**
 * Constant
 *
 * @author liu
 * @since 2023-09-17
 */
@Slf4j
public class Constant {
    /**
     * STAT_MYSQL8
     */
    public static final String STAT_MYSQL8 = "com/mysql/cj/jdbc/StatementImpl";

    /**
     * STAT_MYSQL5
     */
    public static final String STAT_MYSQL5 = "com/mysql/jdbc/StatementImpl";

    /**
     * STAT_SQLSERVER
     */
    public static final String STAT_SQLSERVER = "com/microsoft/sqlserver/jdbc/SQLServerStatement";

    /**
     * STAT_OPENGAUSS
     */
    public static final String STAT_OPENGAUSS = "org/postgresql/jdbc/PgStatement";

    /**
     * STAT_ORACLE
     */
    public static final String STAT_ORACLE = "oracle/jdbc/driver/OracleStatement";

    /**
     * PRE_MYSQL8
     */
    public static final String PRE_MYSQL8 = "com/mysql/cj/jdbc/ClientPreparedStatement";

    /**
     * PRE_MYSQL5
     */
    public static final String PRE_MYSQL5 = "com/mysql/jdbc/PreparedStatement";

    /**
     * PRE_SQLSERVER
     */
    public static final String PRE_SQLSERVER = "com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement";

    /**
     * PRE_OPENGAUSS
     */
    public static final String PRE_OPENGAUSS = "org/postgresql/jdbc/PgPreparedStatement";

    /**
     * PRE_ORACLE
     */
    public static final String PRE_ORACLE = "oracle/jdbc/driver/OraclePreparedStatement";

    /**
     * RECORD
     */
    public static final String RECORD = "org/kit/agent/common/Constant";

    /**
     * statClassName
     */
    public static final List<String> STATCLASSNAME = List.of(
            STAT_MYSQL8, STAT_MYSQL5, STAT_SQLSERVER, STAT_OPENGAUSS, STAT_ORACLE);

    /**
     * preClassName
     */
    public static final List<String> PRECLASSNAME = List.of(
            PRE_MYSQL8, PRE_MYSQL5, PRE_SQLSERVER, PRE_OPENGAUSS, PRE_ORACLE);
    private static final String NEWLINE = StrUtil.LF;
    private static final String SQL_NAME = "collection.sql";
    private static final String STACK_NAME = "stack.txt";
    private static final String PATH = "/kit/file/";

    /**
     * sqlRecord
     *
     * @param className className
     * @param query     query
     */
    public static void sqlRecord(String className, String query) {
        String sqlPath = PATH + DataUtil.getDate() + SQL_NAME;
        createFile(sqlPath);
        String str = query;
        if (str.contains("PreparedStatement")) {
            str = str.substring(str.indexOf(":") + 1);
        }
        str = dealQuery(str);
        if (isSqlRepeat(str, sqlPath)) {
            return;
        }
        log.info(DataUtil.getTimeNow() + " start recording sql statements");
        writeStringToFile(str, sqlPath);
    }

    /**
     * stakeRecord
     *
     * @param sql        className
     * @param stackTrace stackTrace
     */
    public static void stakeRecord(String sql, StackTraceElement[] stackTrace) {
        String stackPath = PATH + DataUtil.getDate() + STACK_NAME;
        createFile(stackPath);
        String str = sql;
        if (str.contains("PreparedStatement")) {
            str = str.substring(str.indexOf(":") + 1);
        }
        str = dealQuery(str);
        StringBuilder sb = new StringBuilder();
        sb.append("Sql: ").append(str).append(NEWLINE);
        sb.append("Stack Trace:" + NEWLINE);
        for (StackTraceElement element : stackTrace) {
            sb.append(element.toString()).append(NEWLINE);
        }
        String logMessage = sb.toString();
        if (isStackRepeat(logMessage, stackPath)) {
            return;
        }
        log.info(DataUtil.getTimeNow() + " start recording call stack information");
        writeStringToFile(logMessage, stackPath);
    }

    private static String dealQuery(String query) {
        String str = query;
        str = str.toLowerCase(Locale.ROOT).replace("\"", "")
                .replace(NEWLINE, " ")
                .replaceAll("\\s{2,}", " ").trim();
        return str;
    }

    private static void writeStringToFile(String content, String filePath) {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8"))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException exception) {
            log.error("Error occurred during SQL statement writing to file-->{}", exception.getMessage());
        }
    }

    private static void createFile(String filePath) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
        } catch (IOException e) {
            log.error("agent Constant createFile method occur error{}", e.getMessage());
        }
    }

    private static boolean isSqlRepeat(String str, String filePath) {
        try {
            Path path = Paths.get(filePath);
            if (!Files.exists(path)) {
                return false;
            }
            try (Stream<String> lines = Files.lines(path)) {
                return lines.anyMatch(line -> line.equals(str));
            }
        } catch (IOException exception) {
            log.error("Error occurred during file content reading -->{}", exception.getMessage());
        }
        return false;
    }

    private static boolean isStackRepeat(String str, String filePath) {
        try (FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);
            String fileContent = StandardCharsets.UTF_8.decode(buffer).toString();
            if (fileContent.contains(str)) {
                return true;
            }
        } catch (IOException e) {
            log.error("agent Constant isStackRepeat method occur error{}", e.getMessage());
        }
        return false;
    }
}
