package org.opengauss.assessment.dao;

import org.opengauss.parser.FileLocks;
import org.opengauss.parser.configure.ConfigureInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.opengauss.assessment.dao.AssessmentType.*;
import static org.opengauss.assessment.dao.CompatibilityType.*;
import static org.opengauss.assessment.dao.CompatibilityType.UNSUPPORTED_COMPATIBLE;
import static org.opengauss.assessment.utils.Utils_Connection.*;
import static org.opengauss.assessment.utils.Utils_JSchConnect.getJSchConnect;

public class AssessmentEntry {
    private final AssessmentSettings assessmentSettings = new AssessmentSettings();
    private final PsqlSettings pset = new PsqlSettings();
    private static final int DB_CMPT_A = 0;
    private static final int DB_CMPT_B = 1;
    private static final int DB_CMPT_C = 2;
    private static final int DB_CMPT_PG = 3;
    private final DB_CompatibilityAttr[] dbCompatArray = {new DB_CompatibilityAttr(DB_CMPT_A, "A"),
            new DB_CompatibilityAttr(DB_CMPT_B, "B"),
            new DB_CompatibilityAttr(DB_CMPT_C, "C"),
            new DB_CompatibilityAttr(DB_CMPT_PG, "PG")};
    private final String[] dbPlugins = {"whale", "dolphin", null, null};
    private static final int MAX_RETRY_COUNT = 5000;
    private static int globalDatabaseType = -1;

    private final int outputSqlFileCount = ConfigureInfo.getConfigureInfo().getOutputSqlFileCount();

    private static final Logger LOGGER = LoggerFactory.getLogger(AssessmentEntry.class);

    public void assessment() {
        PSqlPostInit();

        InitParam("B");

        CheckOutputPrivilege();

        SanityCheck();

        if (assessmentSettings.isNeedCreateDatabase()) {
            CreateAssessmentDatabase();
        }

        Connection connection = getConnection(assessmentSettings.getDbname());
        PreparedStatement statement = null;
        ResultSet rs = null;

        try {
            /* create plugin and extension */
            InstallPlugins(connection);
            /* suspend notice when exec command like 'drop table if exists xxx' */
            statement = connection.prepareStatement("set client_min_messages=error");
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            ResourceClose(connection, statement, rs);
        }

        CompatibilityTable compatibilityTable = new CompatibilityTable();
        if (!compatibilityTable.GenerateReportHeader(assessmentSettings.getOutputFile(), dbCompatArray[globalDatabaseType].getName())) {
            LOGGER.error(pset.getProname() + ": can not write to file \""  + assessmentSettings.getOutputFile() + "\"");
            System.exit(1);
        }

        Path path = Paths.get(assessmentSettings.getInputDir());
        AtomicInteger fileCount = new AtomicInteger(0);
        while (fileCount.intValue() < outputSqlFileCount) {
            if (Files.exists(path) && Files.isDirectory(path)) {
                try (Stream<Path> files = Files.list(path)) {
                    files.parallel().forEachOrdered(inputPath -> {
                        String fileName = inputPath.getFileName().toString();
                        Map<String, ReentrantReadWriteLock> lockers = FileLocks.getLockers();
                        if (lockers.containsKey(fileName)) {
                            ReentrantReadWriteLock.ReadLock readLock = lockers.get(fileName).readLock();
                            lockers.remove(fileName);
                            fileCount.getAndIncrement();
                            readLock.lock();
                            try {
                                Queue<ScanSingleSql> allSql = SplitSQLFile(inputPath);
                                int sqlSize = allSql.size();
                                GramTest(0, sqlSize, allSql, compatibilityTable);
                                if (!compatibilityTable.GenerateSQLCompatibilityStatistic(fileName)) {
                                    LOGGER.error(assessmentSettings.getOutputFile() + "%s: can not write to file \""+ pset.getProname() + "\"");
                                    System.exit(1);
                                }
                            } finally {
                                readLock.unlock();
                            }
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (!compatibilityTable.GenerateReportEnd()) {
            LOGGER.error(assessmentSettings.getOutputFile() + ": can not write to file \"" + pset.getProname() +"\"");
            System.exit(1);
        }

        if (assessmentSettings.getDatabase() >= 0) {
            LOGGER.info(pset.getProname() + ": Create database " + assessmentSettings.getDbname() + " automatically, clear it manually!");
        }
    }

    private void SanityCheck() {
        if (assessmentSettings.getDbname() == null) {
            assessmentSettings.setNeedCreateDatabase(true);
        }
    }

    private Queue<ScanSingleSql> SplitSQLFile(Path path) {
        Queue<ScanSingleSql> allSql = new LinkedList<>();

        try (BufferedReader bufferedReader = Files.newBufferedReader(path)) {
            StringBuffer buffer = new StringBuffer();
            String sqlLine;
            while ((sqlLine = bufferedReader.readLine()) != null) {
                buffer.append(sqlLine + " ");
            }

            String sqlStr = buffer.toString();
            buffer.delete(0, buffer.length());
            int line = 1;
            String[] sqlArr;
            List<String> delimiters = new ArrayList<>();
            Matcher matcher = Pattern.compile("delimiter(\\s+)//|delimiter(\\s+);").matcher(sqlStr);
            while (matcher.find()) {
                delimiters.add(matcher.group());
            }
            if (!delimiters.isEmpty()) {
                String separator = ";";
                String[] sqlStrs = sqlStr.split("delimiter(\\s+)//|delimiter(\\s+);");
                for (int i = 0; i < sqlStrs.length; i++) {
                    sqlArr = sqlStrs[i].split(separator + "((?=(?:[^\"\']*[\"\'][^\"']*[\"\'])*[^\"\']*$))");
                    for (String sql : sqlArr) {
                        sql = sql.trim();
                        if (!sql.equals("")) {
                            allSql.offer(new ScanSingleSql(sql, line++));
                        }
                    }

                    if (i < delimiters.size()) {
                        String delimiter = delimiters.get(i);
                        separator = delimiter.replace("delimiter", "").trim();
                    }
                }
            } else {
                sqlArr = sqlStr.split(";(?=(?:[^\"\']*[\"\'][^\"\']*[\"\'])*[^\"\']*$)");
                for (String sql : sqlArr) {
                    sql = sql.trim();
                    if (!sql.equals("")) {
                        allSql.offer(new ScanSingleSql(sql, line++));
                    }
                }
            }
        } catch (IOException e) {
            return allSql;
        }

        return allSql;
    }

    private void CheckOutputPrivilege() {
        if (assessmentSettings.getOutputFile() == null) {
            LOGGER.error(pset.getProname() + ": The output file must be specific\n");
            System.exit(1);
        } else {
            File outputFd = new File(assessmentSettings.getOutputFile());

            if (outputFd.getAbsoluteFile().isFile()) {
                if (outputFd.exists()) {
                    outputFd.delete();
                }

                try {
                    outputFd.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            } else {
                LOGGER.error(pset.getProname() + ": The outputPath must be a file\n");
            }
        }
    }

    private void GramTest(long index, int sqlSize, Queue<ScanSingleSql> allSql, CompatibilityTable compatibilityTable) {
        Connection connection = getConnection(assessmentSettings.getDbname());
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        PreparedStatement statement = null;
        while (!allSql.isEmpty()) {
            ScanSingleSql scanSingleSql = allSql.poll();
            String str = scanSingleSql.getSql();
            boolean gramTest = true;
            AssessmentType assessmentType = UNSUPPORTED;
            CompatibilityType compatibilityType = UNSUPPORTED_COMPATIBLE;
            String errorResult = "";
            if (assessmentSettings.isPlugin()) {
                try {
                    String querySql = "ast " + str;
                    statement = connection.prepareStatement(querySql);
                    statement.execute();
                } catch (SQLException e) {
                    try {
                        connection.commit();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }

                    compatibilityType = INCOMPATIBLE;
                    gramTest = false;
                }
            }

            if (gramTest) {
                ResultSet resultSets;
                String result = null;

                try {
                    String querySql = "select public.ast_support(?)";
                    statement = connection.prepareStatement(querySql);
                    statement.setString(1, str.replace("''", "'"));
                    resultSets = statement.executeQuery();

                    if (resultSets.next()) {
                        result = resultSets.getString("ast_support");
                    }

                    AssessmentType[] assessmentTypes = AssessmentType.values();
                    assessmentType = assessmentTypes[Integer.parseInt(result)];
                } catch (SQLException e) {
                    try {
                        connection.commit();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex);
                    }

                    compatibilityType = INCOMPATIBLE;
                    errorResult = e.getMessage();
                }

                switch (assessmentType) {
                    case DDL:
                    case EXPLAIN:
                    case DML:
                        try {
                            statement = connection.prepareStatement(str);
                            statement.execute();
                            compatibilityType = COMPATIBLE;
                        } catch (SQLException e) {
                            try {
                                connection.commit();
                            } catch (SQLException ex) {
                                throw new RuntimeException(ex);
                            }
                            compatibilityType = AST_COMPATIBLE;
                            errorResult = e.getMessage();
                        }
                        break;
                    case COMMENT:
                        compatibilityType = SKIP_COMMAND;
                        break;
                    case DCL:
                    case DATABASE_COMMAND:
                    case TRANSACTION:
                    case SET_VARIABLE:
                    case TODO:
                    case UNSUPPORTED:
                    default:
                        compatibilityType = UNSUPPORTED_COMPATIBLE;
                }
            }

            compatibilityTable.AppendOneSQL(scanSingleSql.getLine(), str, assessmentType, compatibilityType, errorResult);
            index++;
        }

        PrintProcess(sqlSize, index);
        System.out.printf("%s", "\n");
        ResourceClose(connection, statement);
    }

    private void InitParam(String compat) {
        for (int i = DB_CMPT_A; i <= DB_CMPT_PG; i++) {
            if (dbCompatArray[i].getName().equalsIgnoreCase(compat)) {
                assessmentSettings.setDatabase(i);
                break;
            }
        }

        if (assessmentSettings.getDatabase() == -1) {
            LOGGER.error(pset.getProname() + ": compatibility: only support A\\B\\C\\PG, current is " + compat + "\n.");
            System.exit(1);
        }

        assessmentSettings.setDbname(ConfigureInfo.getConfigureInfo().getOgInfo().getDbname());
        assessmentSettings.setInputDir(ConfigureInfo.getConfigureInfo().getOutputDir());
        assessmentSettings.setOutputFile(ConfigureInfo.getConfigureInfo().getReportFile());
    }

    private void ResourceClose(Connection connection, PreparedStatement statement, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        ResourceClose(connection, statement);
    }

    private void CreateAssessmentDatabase() {
        Connection connection = getConnection("postgres");
        long currentTimeMillis = System.currentTimeMillis();
        String dbname = "assessment_" + currentTimeMillis;
        PreparedStatement statement = null;

        try {
            String sqlCommand = "create database " + dbname + " dbcompatibility " + "'" + dbCompatArray[assessmentSettings.getDatabase()].getName() + "'";
            statement = connection.prepareStatement(sqlCommand);
            statement.execute();
            assessmentSettings.setDbname(dbname);
            LOGGER.info(pset.getProname() + ": create database \" + dbname +\" automatically.\n");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            ResourceClose(connection, statement);
        }
    }

    private void ResourceClose(Connection connection, PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        getConnectionClose(connection);
    }

    private void InstallPlugins(Connection connection) throws SQLException {
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
                LOGGER.error(pset.getProname() + ": compatibility: only support A\\B\\C\\PG, current is " + result + "\n.");
                System.exit(1);
            }
        }

        if (dbPlugins[globalDatabaseType] != null) {
            String pluginName = dbPlugins[globalDatabaseType];
            String extensionSql = "select installed_version is not null as isIncludeExtension from pg_available_extensions where name = ?";
            PreparedStatement statement = connection.prepareStatement(extensionSql);
            statement.setString(1, pluginName);
            ResultSet rs = statement.executeQuery();
            String result = null;

            if (rs.next()) {
                result = rs.getString("isIncludeExtension");
            }

            if (result != null) {
                if (result.equals("f") || result.equals("0")) {
                    assessmentSettings.setPlugin(true);
                    LOGGER.info(pset.getProname() + ": Create Plugin[" + pluginName + "] automatically.\n");
                    String host = ConfigureInfo.getConfigureInfo().getOgInfo().getHost();
                    String port = ConfigureInfo.getConfigureInfo().getOgInfo().getPort();
                    getJSchConnect(host, port, assessmentSettings.getDbname(), pluginName);
                    CheckPlugin(connection, pluginName, extensionSql, result);
                } else if (result.equals("t") || result.equals("1")) {
                    assessmentSettings.setPlugin(true);
                } else {
                    LOGGER.info(pset.getProname() + ": " + dbPlugins[globalDatabaseType] + " is recommended in database " + assessmentSettings.getDbname() + ".\n");
                }
            }
        }

        String extensionSql = "select installed_version is not null as isIncludeExtension from pg_available_extensions where name = 'org.opengauss.assessment'";
        PreparedStatement statement = connection.prepareStatement(extensionSql);
        ResultSet rs = statement.executeQuery();
        String result = null;

        if (rs.next()) {
            result = rs.getString("isIncludeExtension");
        }

        if (result != null) {
            if (result.equals("")) {
                LOGGER.info(pset.getProname() + ": \"org.opengauss.assessment\" extension is needed.");
                System.exit(1);
            } else if (result.equals("f") || result.equals("0")) {
                LOGGER.info(pset.getProname() + " : Create extension[org.opengauss.assessment] automatically.\n");
                assessmentSettings.setExtension(true);
                statement = connection.prepareStatement("create extension org.opengauss.assessment");
                statement.execute();
            } else if (result.equals("t") || result.equals("1")) {
                assessmentSettings.setExtension(true);
            }
        }
    }

    private void CheckPlugin(Connection connection, String pluginName, String extensionSql, String result) throws SQLException {
        int retryCount = 0;
        while (!(result.equals("t") || result.equals("1"))) {
            if (retryCount++ > MAX_RETRY_COUNT) {
                LOGGER.error(pset.getProname() + ": Create Plugin[" + pluginName + "] failed.\n");
                System.exit(1);
            }

            PreparedStatement statement = connection.prepareStatement(extensionSql);
            statement.setString(1, pluginName);
            ResultSet rs = statement.executeQuery();

            if (rs.next()) {
                result = rs.getString("isIncludeExtension");
            }
        }
    }

    private void PrintProcess(long sqlSize, long curIndex) {
        if (sqlSize == 0) {
            LOGGER.warn(pset.getProname() + ": this file has no sql.\n");
            return;
        }

        double value = curIndex * 100 / sqlSize;
        System.out.printf("%s: Analysing[%.2f%%]:%d/%d", pset.getProname(), value > 100 ? 100 : value, curIndex, sqlSize);

        if (curIndex < sqlSize) {
            System.out.printf("%s", "\r");
        }
    }

    private void PSqlPostInit() {
        pset.setDbname(null);
        pset.setProname("assessment_database");
    }
}