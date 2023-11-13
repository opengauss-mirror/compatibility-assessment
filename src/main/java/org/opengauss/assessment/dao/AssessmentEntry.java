/*
 * Copyright (c) 2023-2023 Huawei Technologies Co.,Ltd.
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

package org.opengauss.assessment.dao;

import org.opengauss.parser.FileLocks;
import org.opengauss.parser.FilesOperation;
import org.opengauss.parser.command.Commander;
import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Queue;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.opengauss.assessment.dao.AssessmentType.UNSUPPORTED;
import static org.opengauss.assessment.dao.AssessmentType.COMMENT;
import static org.opengauss.assessment.dao.CompatibilityType.COMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.SKIP_COMMAND;
import static org.opengauss.assessment.dao.CompatibilityType.AST_COMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.UNSUPPORTED_COMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.INCOMPATIBLE;
import static org.opengauss.assessment.utils.ConnectionUtils.getConnection;
import static org.opengauss.assessment.utils.JSchConnectionUtils.getJSchConnect;

/**
 * Assessment module entry.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class AssessmentEntry {
    private static AssessmentSettings assessmentSettings = new AssessmentSettings();
    private static PsqlSettings pset = new PsqlSettings();
    private static final int DB_CMPT_A = 0;
    private static final int DB_CMPT_B = 1;
    private static final int DB_CMPT_C = 2;
    private static final int DB_CMPT_PG = 3;
    private static DBCompatibilityAttr[] dbCompatArray = {new DBCompatibilityAttr(DB_CMPT_A, "A"),
            new DBCompatibilityAttr(DB_CMPT_B, "B"),
            new DBCompatibilityAttr(DB_CMPT_C, "C"),
            new DBCompatibilityAttr(DB_CMPT_PG, "PG")};
    private static String[] dbPlugins = {"whale", "dolphin", null, null};
    private static final int MAX_RETRY_COUNT = 50000;
    private static int globalDatabaseType = -1;
    private static final int OUTPUT_SQL_FILE_COUNT = AssessmentInfoManager.getInstance().getOutputSqlFileCount();
    private static final Logger LOGGER = LoggerFactory.getLogger(AssessmentEntry.class);
    private static final String DELIMITER = "delimiter";

    /**
     * assessment function.
     */
    public void assessment() {
        initPSqlSettings();

        initParam("B");

        checkOutputPrivilege();

        sanityCheck();

        if (assessmentSettings.isNeedCreateDatabase()) {
            createAssessmentDatabase();
        }

        try (Connection connection = getConnection(assessmentSettings.getDbname())) {
            /* create plugin and extension */
            installPlugins(connection);
            /* suspend notice when exec command like 'drop table if exists xxx' */
            try (PreparedStatement statement = connection.prepareStatement("set client_min_messages=error")) {
                statement.execute();
            }

            CompatibilityTable compatibilityTable = new CompatibilityTable();
            if (!compatibilityTable.generateReportHeader(assessmentSettings.getOutputFile(),
                    dbCompatArray[globalDatabaseType].getName())) {
                LOGGER.error(pset.getProname() + ": can not write to file \"" + assessmentSettings.getOutputFile() + "\"");
            }

            setAutoCommit(connection);
            sqlAssessment(compatibilityTable, connection);

            if (!compatibilityTable.generateReportEnd()) {
                LOGGER.error(assessmentSettings.getOutputFile() + ": can not write to file \"" + pset.getProname() + "\"");
            }
        } catch (SQLException e) {
            LOGGER.error("sql assessment occur SQLException! exception = " + e.getMessage());
        }

        if (assessmentSettings.getDatabase() >= 0 && LOGGER.isInfoEnabled()) {
            LOGGER.info(pset.getProname() + ": Create database " + assessmentSettings.getDbname()
                    + " automatically, clear it manually!");
        }
    }

    /**
     * assessment sql.
     *
     * @param compatibilityTable : record assessment information.
     */
    private void sqlAssessment(CompatibilityTable compatibilityTable, Connection connection) {
        Path path = Paths.get(assessmentSettings.getInputDir());
        AtomicInteger fileCount = new AtomicInteger(0);
        while (fileCount.intValue() < OUTPUT_SQL_FILE_COUNT) {
            if (Files.exists(path) && Files.isDirectory(path)) {
                try (Stream<Path> files = Files.list(path)) {
                    sqlAssessmentHelper(compatibilityTable, fileCount, files, connection);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * assessment sql helper.
     *
     * @param compatibilityTable : record assessment information.
     * @param fileCount          : file index.
     * @param files              : file stream.
     */
    private void sqlAssessmentHelper(CompatibilityTable compatibilityTable, AtomicInteger fileCount,
                                     Stream<Path> files, Connection connection) {
        files.parallel().forEachOrdered(inputPath -> {
            String fileName = inputPath.getFileName().toString();
            Map<String, ReentrantReadWriteLock> lockers = FileLocks.getLockers();
            if (lockers.containsKey(fileName)) {
                ReentrantReadWriteLock.ReadLock readLock;
                synchronized (FileLocks.lockersSync) {
                    readLock = lockers.get(fileName).readLock();
                }
                lockers.remove(fileName);
                fileCount.getAndIncrement();
                readLock.lock();
                try {
                    Queue<ScanSingleSql> allSql = getSQL(inputPath);
                    int sqlSize = allSql.size();
                    gramTest(sqlSize, allSql, compatibilityTable, connection);
                    if (!compatibilityTable.generateSQLCompatibilityStatistic(fileName)) {
                        LOGGER.error(assessmentSettings.getOutputFile() + "%s: can not write to file \""
                                + pset.getProname() + "\"");
                    }
                } finally {
                    readLock.unlock();
                }
            }
        });
    }

    /**
     * Check if we need create database.
     */
    private void sanityCheck() {
        if (assessmentSettings.getDbname() == null) {
            assessmentSettings.setNeedCreateDatabase(true);
        }
    }

    /**
     * Split sql file.
     *
     * @param path : sql file path.
     * @return ScanSingleSql
     */
    private Queue<ScanSingleSql> getSQL(Path path) {
        Queue<ScanSingleSql> allSql = new LinkedList<>();
        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
            getSQLHelper(allSql, bufferedReader);
        } catch (IOException e) {
            return allSql;
        }

        return allSql;
    }

    /**
     * Split sql file.
     *
     * @param allSql         : store sql.
     * @param bufferedReader : bufferReader
     * @throws IOException : throw IOException
     */
    private static void getSQLHelper(Queue<ScanSingleSql> allSql, BufferedReader bufferedReader)
            throws IOException {
        String sqlLine;
        String delimiter = ";";
        StringBuffer buffer = new StringBuffer();
        boolean isDelimiter = false;
        int line = 1;
        while ((sqlLine = bufferedReader.readLine()) != null) {
            sqlLine = sqlLine.trim();
            if (sqlLine.equals("")) {
                continue;
            }

            if (sqlLine.startsWith("--")) {
                allSql.offer(new ScanSingleSql(sqlLine, line++));
                continue;
            }

            /* The delimiter keyword has been read, but no delimiters were obtained */
            if (isDelimiter) {
                isDelimiter = false;
                delimiter = sqlLine.replace(delimiter, "").trim();
                /* read delimiter keyword */
            } else if (isStartWithDelimiter(sqlLine)) {
                sqlLine = sqlLine.replace(DELIMITER, "").replace(DELIMITER.toUpperCase(Locale.ROOT), "")
                        .replace(delimiter, "").trim();
                /* no delimiters were obtained */
                if (sqlLine.equals("")) {
                    isDelimiter = true;
                } else {
                    delimiter = sqlLine;
                }
            } else if (!sqlLine.endsWith(delimiter)) {
                buffer.append(sqlLine);
                buffer.append(" ");
            } else {
                buffer.append(sqlLine.replace(delimiter, ""));
                allSql.offer(new ScanSingleSql(buffer.toString().trim(), line++));
                buffer.setLength(0);
            }
        }
    }

    /**
     * Determine if sqlLine starts with delimiter.
     *
     * @param sqlLine : sql line
     * @return boolean
     */
    private static boolean isStartWithDelimiter(String sqlLine) {
        return sqlLine.startsWith(DELIMITER) || sqlLine.startsWith(DELIMITER.toUpperCase(Locale.ROOT));
    }

    /**
     * Split sql file.
     *
     * @param allSql         : store sql.
     * @param bufferedReader : bufferReader
     * @throws IOException : throw IOException
     */
    private static void splitSQLFileHelper(Queue<ScanSingleSql> allSql, BufferedReader bufferedReader)
            throws IOException {
        StringBuffer buffer = new StringBuffer();
        String sqlLine;
        while ((sqlLine = bufferedReader.readLine()) != null) {
            buffer.append(sqlLine + " ");
        }

        String sqlStr = buffer.toString();
        buffer.delete(0, buffer.length());
        String[] sqlArr;
        List<String> delimiters = new ArrayList<>();
        String delimiterRegex = "(delimiter|DELIMITER)(\\s+)//|(delimiter|DELIMITER)(\\s+);";
        Matcher matcher = Pattern.compile(delimiterRegex).matcher(sqlStr);
        String filterQuotes = "(?=(?:[^\"\']*[\"\'][^\"\']*[\"\'])*[^\"\']*$)";
        while (matcher.find()) {
            delimiters.add(matcher.group());
        }
        if (!delimiters.isEmpty()) {
            splitSQLFileWithDelimiter(allSql, sqlStr, delimiters, filterQuotes, delimiterRegex);
        } else {
            sqlArr = sqlStr.split(";" + filterQuotes);
            int line = 1;
            for (String sql : sqlArr) {
                sql = sql.trim();
                if (!sql.equals("")) {
                    allSql.offer(new ScanSingleSql(sql, line++));
                }
            }
        }
    }

    /**
     * Split sql file.
     *
     * @param allSql         : store sql.
     * @param sqlStr         : sql string.
     * @param delimiters     : delimiter sql.
     * @param filterQuotes   : filter quotes regex.
     * @param delimiterRegex : delimiter regex
     */
    private static void splitSQLFileWithDelimiter(Queue<ScanSingleSql> allSql, String sqlStr, List<String> delimiters,
                                                  String filterQuotes, String delimiterRegex) {
        String[] sqlArr;
        String separator = ";";
        int line = 1;
        String[] sqlStrs = sqlStr.split(delimiterRegex);
        for (int i = 0; i < sqlStrs.length; i++) {
            sqlArr = sqlStrs[i].split(separator + filterQuotes);
            for (String sql : sqlArr) {
                sql = sql.trim();
                if (!sql.equals("")) {
                    allSql.offer(new ScanSingleSql(sql, line++));
                }
            }

            if (i < delimiters.size()) {
                String delimiter = delimiters.get(i);
                separator = delimiter.replace("delimiter", "").replace("DELIMITER", "").trim();
            }
        }
    }

    /**
     * Check outputFile privilege.
     */
    private void checkOutputPrivilege() {
        if (assessmentSettings.getOutputFile() == null) {
            LOGGER.error(pset.getProname() + ": The output file must be specific");
        } else {
            File outputFd = new File(assessmentSettings.getOutputFile());
            File parent = new File(outputFd.getAbsoluteFile().getParent());
            try {
                if (!FilesOperation.isCreateOutputFile(outputFd, parent.getCanonicalPath())) {
                    LOGGER.warn(pset.getProname() + ": create report file failed, it may already exists! outputFile :"
                            + outputFd.getName());
                }
            } catch (IOException e) {
                LOGGER.warn("create newFile occur IOException! newfile: " + outputFd.getName(), e);
            }
        }
    }

    /**
     * Gram test.
     *
     * @param sqlSize            : sql num.
     * @param allSql             : store sql.
     * @param compatibilityTable : record assessment information.
     */
    private void gramTest(int sqlSize, Queue<ScanSingleSql> allSql, CompatibilityTable compatibilityTable,
                          Connection connection) {
        long index = 0L;
        while (!allSql.isEmpty()) {
            gramTestHelper(allSql, compatibilityTable, connection);
            index++;
        }

        printProcess(sqlSize, index);
    }

    /**
     * Gram test.
     *
     * @param allSql             : store sql.
     * @param compatibilityTable : record assessment information.
     * @param connection         : jdbc connection.
     */
    private static void gramTestHelper(Queue<ScanSingleSql> allSql, CompatibilityTable compatibilityTable,
                                       Connection connection) {
        ScanSingleSql scanSingleSql = allSql.poll();
        String sql = scanSingleSql.getSql();
        String str = translateExplainTblnameSql(sql);
        CompatibilityType compatibilityType = UNSUPPORTED_COMPATIBLE;
        String errorResult = "";
        if (assessmentSettings.isPlugin()) {
            String querySql = "ast " + str;
            try (PreparedStatement statement = connection.prepareStatement(querySql)) {
                statement.execute();
                compatibilityType = AST_COMPATIBLE;
            } catch (SQLException e) {
                commit(connection);
                compatibilityType = INCOMPATIBLE;
                errorResult = e.getMessage();
            }
        }

        AssessmentType assessmentType = UNSUPPORTED;
        try {
            assessmentType = getAssessmentType(connection, str);
            compatibilityType = AST_COMPATIBLE;
            errorResult = "";
        } catch (SQLException e) {
            commit(connection);
        }

        if (compatibilityType == AST_COMPATIBLE) {
            if (assessmentType == COMMENT) {
                compatibilityType = SKIP_COMMAND;
            } else {
                try (PreparedStatement statement = connection.prepareStatement(str)) {
                    statement.execute();
                    compatibilityType = COMPATIBLE;
                } catch (SQLException e) {
                    commit(connection);
                    errorResult = e.getMessage();
                }
            }
        }

        compatibilityTable.appendOneSQL(scanSingleSql.getLine(), sql, assessmentType, compatibilityType, errorResult);
    }

    /**
     * translate explain tblname sql.
     *
     * @param sql : explain tblname sql
     * @return String
     */
    private static String translateExplainTblnameSql(String sql) {
        String str = sql;
        if (str.contains("explain")) {
            String[] strings = str.split(" ");
            if (strings.length == 2) {
                str = str.replace("explain", "desc");
            }
        }
        return str;
    }

    /**
     * get assessmentType
     *
     * @param connection : jdbc connection.
     * @param str        : query sql string.
     * @return AssessmentType
     * @throws SQLException : throw SQLException
     */
    private static AssessmentType getAssessmentType(Connection connection, String str)
            throws SQLException {
        String querySql = "select public.ast_support(?)";
        Optional optional = queryOne(connection, querySql, "ast_support", 1,
                str.replace("''", "'"));
        if (!optional.isEmpty()) {
            AssessmentType[] assessmentTypes = AssessmentType.values();
            return assessmentTypes[Integer.parseInt(String.valueOf(optional.get()))];
        }
        return UNSUPPORTED;
    }

    /**
     * set autoCommit.
     *
     * @param connection : jdbc connection.
     */
    private static void setAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            LOGGER.warn("set untoCommit failed.");
        }
    }

    /**
     * Execute a query sql.
     *
     * @param connection  : jdbc connection.
     * @param sql         : query sql
     * @param columnLabel : query column label.
     * @return String
     * @throws SQLException : throw SQLException
     */
    private static Optional<Object> queryOne(Connection connection, String sql, String columnLabel)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            try (ResultSet resultSets = statement.executeQuery()) {
                if (resultSets.next()) {
                    return Optional.of(resultSets.getString(columnLabel));
                }
                return Optional.empty();
            }
        }
    }

    /**
     * Execute a query sql.
     *
     * @param connection  : jdbc connection.
     * @param sql         : query sql
     * @param columnLabel : query column label.
     * @param index       : placeholder index.
     * @param str         : placeholder string.
     * @return String
     * @throws SQLException : throw SQLException
     */
    private static Optional<Object> queryOne(Connection connection, String sql, String columnLabel, int index,
                                             String str) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(index, str);
            try (ResultSet resultSets = statement.executeQuery()) {
                if (resultSets.next()) {
                    return Optional.of(resultSets.getString(columnLabel));
                }
                return Optional.empty();
            }
        }
    }

    /**
     * commit.
     *
     * @param connection : jdbc connection..
     */
    private static void commit(Connection connection) {
        try {
            connection.commit();
        } catch (SQLException ex) {
            LOGGER.warn("commit failed.");
        }
    }

    /**
     * Init params.
     *
     * @param compat : database compatibility
     */
    private void initParam(String compat) {
        for (int i = DB_CMPT_A; i <= DB_CMPT_PG; i++) {
            if (dbCompatArray[i].getName().equalsIgnoreCase(compat)) {
                assessmentSettings.setDatabase(i);
                break;
            }
        }

        if (assessmentSettings.getDatabase() == -1) {
            LOGGER.error(pset.getProname() + ": compatibility: only support A\\B\\C\\PG, current is " + compat + ".");
        }

        assessmentSettings.setDbname(AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                AssessmentInfoChecker.DBNAME));
        assessmentSettings.setInputDir(AssessmentInfoManager.getSqlOutDir());
        assessmentSettings.setOutputFile(Commander.getReportFile());
    }

    /**
     * create assessment database.
     */
    private void createAssessmentDatabase() {
        long currentTimeMillis = System.currentTimeMillis();
        String dbname = "assessment_" + currentTimeMillis;
        String sqlCommand = "create database " + dbname + " dbcompatibility " + "'"
                + dbCompatArray[assessmentSettings.getDatabase()].getName() + "'";

        try (Connection connection = getConnection("postgres")) {
            try (PreparedStatement statement = connection.prepareStatement(sqlCommand)) {
                statement.execute();
                assessmentSettings.setDbname(dbname);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(pset.getProname() + ": create database " + dbname + " automatically.");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Install plugins.
     *
     * @param connection : jdbc connection.
     * @throws SQLException : throw SQLException
     */
    private void installPlugins(Connection connection) throws SQLException {
        globalDatabaseType = assessmentSettings.getDatabase();
        if (globalDatabaseType < 0) {
            String extensionSql = "show sql_compatibility";
            PreparedStatement statement = connection.prepareStatement(extensionSql);
            ResultSet rs = statement.executeQuery();
            String result = null;
            if (rs.next()) {
                result = rs.getString("sql_compatibility");
            }

            for (int i = DB_CMPT_A; i <= DB_CMPT_PG; i++) {
                if (dbCompatArray[i].getName().equalsIgnoreCase(result)) {
                    globalDatabaseType = i;
                    break;
                }
            }

            if (globalDatabaseType < 0) {
                LOGGER.error(pset.getProname() + ": compatibility: only support A\\B\\C\\PG, current is "
                        + result + ".");
            }
        }

        createPlugin(connection);
        createAssessmentPlugin(connection);
    }

    /**
     * create assessment plugin.
     *
     * @param connection : jdbc connection.
     * @throws SQLException : throw SQLException
     */
    private static void createAssessmentPlugin(Connection connection) throws SQLException {
        String extensionSql = "select installed_version is not null as isIncludeExtension from pg_available_extensions"
                + " where name = 'assessment'";
        Optional optional = queryOne(connection, extensionSql, "isIncludeExtension");
        if (!optional.isEmpty()) {
            String result = String.valueOf(optional.get());
            if (result.equals("")) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(pset.getProname() + ": \"assessment\" extension is needed.");
                }
            } else if (result.equals("f") || result.equals("0")) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(pset.getProname() + " : Create extension[assessment] automatically.");
                }
                assessmentSettings.setExtension(true);
                try (PreparedStatement statement = connection.prepareStatement("create extension assessment")) {
                    statement.execute();
                }
            } else if (result.equals("t") || result.equals("1")) {
                assessmentSettings.setExtension(true);
            } else {
                LOGGER.warn("query result exception.");
            }
        }
    }

    /**
     * create whale/dolphin plugin.
     *
     * @param connection : jdbc connection.
     * @throws SQLException : throw SQLException
     */
    private void createPlugin(Connection connection) throws SQLException {
        if (dbPlugins[globalDatabaseType] != null) {
            String pluginName = dbPlugins[globalDatabaseType];
            String extensionSql = "select installed_version is not null as isIncludeExtension from"
                    + " pg_available_extensions where name = ?";
            Optional opt = queryOne(connection, extensionSql, "isIncludeExtension", 1, pluginName);
            if (!opt.isEmpty()) {
                createPluginHelper(connection, pluginName, extensionSql, String.valueOf(opt.get()));
            }
        }
    }

    /**
     * create whale/dolphin plugin helper.
     *
     * @param connection   : jdbc connection.
     * @param pluginName   : plugin name.
     * @param extensionSql : query sql.
     * @param result       : query result.
     * @throws SQLException : throw SQLException
     */
    private void createPluginHelper(Connection connection, String pluginName, String extensionSql, String result)
            throws SQLException {
        if (result.equals("f") || result.equals("0")) {
            assessmentSettings.setPlugin(true);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(pset.getProname() + ": Create Plugin[" + pluginName + "] automatically.");
            }
            String host = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                    AssessmentInfoChecker.HOST);
            String port = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.OPENGAUSS,
                    AssessmentInfoChecker.PORT);
            getJSchConnect(host, port, assessmentSettings.getDbname(), pluginName);
            checkPlugin(connection, pluginName, extensionSql, result);
        } else if (result.equals("t") || result.equals("1")) {
            assessmentSettings.setPlugin(true);
        } else {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(pset.getProname() + ": " + dbPlugins[globalDatabaseType]
                        + " is recommended in database " + assessmentSettings.getDbname() + ".");
            }
        }
    }

    /**
     * check if plugin is installed.
     *
     * @param connection   : jdbc connection
     * @param pluginName   : plugin name.
     * @param extensionSql : query sql.
     * @param result       : query result.
     * @throws SQLException : throw SQLException
     */
    private void checkPlugin(Connection connection, String pluginName, String extensionSql, String result)
            throws SQLException {
        int retryCount = 0;
        String tempResult = result;
        while (!(tempResult.equals("t") || tempResult.equals("1"))) {
            if (retryCount++ > MAX_RETRY_COUNT) {
                LOGGER.error(pset.getProname() + ": Create Plugin[" + pluginName + "] failed.");
                return;
            }

            Optional opt = queryOne(connection, extensionSql, "isIncludeExtension", 1, pluginName);
            if (!opt.isEmpty()) {
                tempResult = String.valueOf(opt.get());
            }
        }
    }

    /**
     * Print process.
     *
     * @param sqlSize  : total sql num.
     * @param curIndex : current sql index.
     */
    private void printProcess(long sqlSize, long curIndex) {
        if (sqlSize == 0) {
            LOGGER.warn(pset.getProname() + ": this file has no sql.");
            return;
        }

        double value = (double) (curIndex * 100 / sqlSize);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(pset.getProname() + ": Analysing[" + String.format("%.2f", value) + "]:" + curIndex
                    + "/" + sqlSize);
        }
    }

    /**
     * Init psql settings.
     */
    private void initPSqlSettings() {
        pset.setDbname(null);
        pset.setProname("gs_assessment");
    }
}
