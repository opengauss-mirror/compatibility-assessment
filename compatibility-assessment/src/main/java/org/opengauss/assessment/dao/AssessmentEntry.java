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

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastjson.JSONObject;
import javafx.util.Pair;
import org.opengauss.parser.FileLocks;
import org.opengauss.parser.FilesOperation;
import org.opengauss.parser.command.Commander;
import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.opengauss.parser.sqlparser.SqlParseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.BufferedWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Locale;
import java.util.stream.Collectors;
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
    /**
     * pos in report
     */
    public static final String RESULT_KEY_ID = "id";

    /**
     * line number in report
     */
    public static final String RESULT_KEY_LINE = "line";

    /**
     * sql number in report
     */
    public static final String RESULT_KEY_SQL = "sql";

    /**
     * compatibility type
     */
    public static final String RESULT_KEY_TYPE = "compatibilityType";

    /**
     * compatibility detail
     */
    public static final String RESULT_KEY_ERINFO = "errDetail";

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
    private static final String RESULT_DIR = System.getProperty("user.dir") + File.separator + "result";
    private static final String RESULTFILE_SUFFIX = ".result";
    private static final String BLANKS = "\\t|( )+|\u3000";

    private static Map<String, String> suffixMap = new HashMap<>() {
        {
            put("-mapper", ".xml");
            put("-mapper-1", ".xml");
            put("-sql", ".sql");
            put("-general", ".general");
            put("-slow", ".slow");
        }
    };
    private static int totalSql = 0;

    /**
     * assessment sql.
     *
     * @param number : int
     */
    public static void increTotalSql(int number) {
        AssessmentEntry.totalSql += number;
    }

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

            generateFileReport(compatibilityTable);

            if (!compatibilityTable.generateReportEnd()) {
                LOGGER.error(assessmentSettings.getOutputFile() + ": can not write to file \"" + pset.getProname() + "\"");
            }
            LOGGER.info("total sql: {}", totalSql);
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
        FilesOperation.clearDir(new File(RESULT_DIR));
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
                    gramTest(sqlSize, allSql, compatibilityTable, connection, getOriginFilenameByInput(fileName));
                } finally {
                    readLock.unlock();
                }
            }
        });
    }

    private String getOriginFilenameByInput(String inputFilename) {
        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_COLLECT)) {
            return inputFilename + RESULTFILE_SUFFIX;
        }

        String resultName = "";
        for (String middleSuffix : suffixMap.keySet()) {
            if (inputFilename.endsWith(middleSuffix)) {
                String tempName = inputFilename.substring(0, inputFilename.length() - middleSuffix.length());
                resultName = tempName + middleSuffix + RESULTFILE_SUFFIX;
                break;
            }
        }
        return resultName;
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
        String jsonLine;
        int line = 1;
        while ((jsonLine = bufferedReader.readLine()) != null) {
            JSONObject jsonObject = JSONObject.parseObject(jsonLine);
            String id = jsonObject.getString(SqlParseController.KEY_ID);
            String sql = jsonObject.getString(SqlParseController.KEY_SQL)
                    .replaceAll(BLANKS, " ");
            String tag = jsonObject.getString(SqlParseController.KEY_TAG);
            String filename = jsonObject.getString(SqlParseController.KEY_FILE);
            allSql.offer(new ScanSingleSql(sql.trim(), id, line++, tag, filename));
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
                          Connection connection, String resultName) {
        long index = 0L;
        if (!FilesOperation.isCreateOutputFile(new File(RESULT_DIR + File.separator + resultName), RESULT_DIR)) {
            LOGGER.warn("create result file failed, it may already exists. resultfile: " + resultName);
        }
        while (!allSql.isEmpty()) {
            gramTestHelper(allSql, compatibilityTable, connection, resultName);
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
    private void gramTestHelper(Queue<ScanSingleSql> allSql, CompatibilityTable compatibilityTable,
                                Connection connection, String resultName) {
        ScanSingleSql scanSingleSql = allSql.poll();
        String sql = scanSingleSql.getSql();
        String str = translateExplainTblnameSql(sql);
        CompatibilityType compatibilityType = UNSUPPORTED_COMPATIBLE;
        String errorResult = "";
        Pair<CompatibilityType, String> assessInfo = executeAssment(str, connection);
        Pair<Boolean, String> filterInfo;
        if (assessInfo.getKey() == INCOMPATIBLE) {
            filterInfo = InCompatibilityFilter.filterIncompaSql(str, assessInfo.getValue());
            while (filterInfo.getKey()) {
                assessInfo = executeAssment(filterInfo.getValue(), connection);
                if (assessInfo.getKey() != INCOMPATIBLE) {
                    break;
                }
                filterInfo = InCompatibilityFilter.filterIncompaSql(filterInfo.getValue(), assessInfo.getValue());
            }
            str = filterInfo.getValue();
            scanSingleSql.setSql(str);
            assessInfo = executeAssment(str, connection);
            compatibilityType = assessInfo.getKey();
            errorResult = assessInfo.getValue();
        } else {
            compatibilityType = assessInfo.getKey();
            errorResult = assessInfo.getValue();
        }

        if (compatibilityType == INCOMPATIBLE) {
            errorResult = InCompatibilityFilter.filterKeyWord(errorResult);
            if (!validateSQL(str) || !InCompatibilityFilter.filterErrorSql(str)) {
                errorResult = "语法错误" + System.lineSeparator();
            }
            errorResult += getXmlContext(scanSingleSql.getTag(), scanSingleSql.getFilename(), scanSingleSql.getId());
        }
        writeResToFile(resultName, scanSingleSql, compatibilityType, errorResult);
    }

    private static Pair<CompatibilityType, String> executeAssment(String sql, Connection connection) {
        CompatibilityType compaType = UNSUPPORTED_COMPATIBLE;
        String errorResult = "";
        if (assessmentSettings.isPlugin()) {
            String querySql = "ast " + sql;
            try (PreparedStatement statement = connection.prepareStatement(querySql)) {
                statement.execute();
                compaType = AST_COMPATIBLE;
            } catch (SQLException e) {
                commit(connection);
                compaType = INCOMPATIBLE;
                errorResult = e.getMessage();
            }
        }

        AssessmentType assessmentType = UNSUPPORTED;
        try {
            assessmentType = getAssessmentType(connection, sql);
            compaType = AST_COMPATIBLE;
            errorResult = "";
        } catch (SQLException e) {
            commit(connection);
        }
        if (compaType == AST_COMPATIBLE) {
            if (assessmentType == COMMENT) {
                compaType = SKIP_COMMAND;
            } else {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.execute();
                    compaType = COMPATIBLE;
                } catch (SQLException e) {
                    commit(connection);
                    errorResult = e.getMessage();
                }
            }
        }
        return new Pair<>(compaType, errorResult);
    }

    private String getXmlContext(String tag, String filename, String id) {
        String context = "";
        if (filename == null || !filename.endsWith(".xml")) {
            return context;
        }

        File file = new File(filename);
        try (BufferedReader bufReader = new BufferedReader(new FileReader(file))) {
            context = filterAndCheckXmlContext(bufReader, tag, id);
            if (context.length() > 0) {
                context = "<xmp>" + context + "</xmp>";
            }
        } catch (IOException exp) {
            LOGGER.warn("get origin xml context failed, please check manually.");
        }
        return context;
    }

    private String filterAndCheckXmlContext(BufferedReader bufReader, String tag, String id) throws IOException {
        String line;
        String context = "";
        boolean isFiltering = false;
        String tagHeader = "<" + tag;
        String tagTail = "</" + tag + ">";
        while ((line = bufReader.readLine()) != null) {
            String trimLine = line.trim();
            if ((!trimLine.startsWith(tagHeader) || trimLine.startsWith("<selectKey")) && !isFiltering) {
                continue;
            }

            if (trimLine.startsWith(tagHeader) && trimLine.endsWith(tagTail)) {
                context += line + System.lineSeparator();
                if (checkXmlContext(context, id)) {
                    break;
                }
                context = "";
                isFiltering = false;
            } else if (trimLine.startsWith(tagHeader) && !trimLine.endsWith(tagTail)) {
                isFiltering = true;
                context += line + System.lineSeparator();
            } else if (!trimLine.startsWith(tagHeader) && trimLine.endsWith(tagTail)) {
                context += line + System.lineSeparator();
                if (checkXmlContext(context, id)) {
                    break;
                }
                context = "";
                isFiltering = false;
            } else {
                context += line + System.lineSeparator();
            }
        }
        return context;
    }

    private boolean checkXmlContext(String xmlContext, String id) {
        try {
            DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = builderFactory.newDocumentBuilder();
            Document doc = builder.parse(new InputSource(new StringReader(xmlContext)));
            Element element = doc.getDocumentElement();
            if (element.hasAttribute("id") && element.getAttribute("id").equals(id)) {
                return true;
            }
        } catch (SAXException | IOException | ParserConfigurationException exp) {
            LOGGER.warn("check origin xml context id failed. id is " + id);
        }
        return false;
    }

    private static boolean validateSQL(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            parser.parseStatementList();
        } catch (Exception exp) {
            return false;
        }
        return true;
    }

    private void readResFromFile(CompatibilityTable table) {
        File dir = new File(RESULT_DIR);
        File[] files = dir.listFiles();
        Map<String, Boolean> fileNameMap = Arrays.stream(files).collect(
                Collectors.toMap(File::getAbsolutePath, (element) -> false));

        for (String fileName : fileNameMap.keySet()) {
            if (fileNameMap.get(fileName)) {
                continue;
            }
            List<SQLCompatibility> result;
            table.getSqlCompatibilities().clear();
            ArrayList<SQLCompatibility> arrayList1 = parseObject(fileName);
            if (isMapperFile(fileName)) {
                String anotherName;
                if (fileName.endsWith("-mapper.result")) {
                    int index = fileName.indexOf("-mapper.result");
                    anotherName = fileName.substring(0, index) + "-mapper-1.result";
                } else {
                    int index = fileName.indexOf("-mapper-1.result");
                    anotherName = fileName.substring(0, index) + "-mapper.result";
                }
                ArrayList<SQLCompatibility> arrayList2 = parseObject(anotherName);
                result = mergeResult(arrayList1, arrayList2);
                fileNameMap.put(anotherName, true);
            } else {
                result = arrayList1;
            }
            table.appendMultipleSQL(result);
            for (String suffix : suffixMap.keySet()) {
                String inputFile = fileName.substring(0, fileName.lastIndexOf('.'));
                if (inputFile.endsWith(suffix)) {
                    fileName = inputFile.substring(0, inputFile.length() - suffix.length()) + suffixMap.get(suffix);
                }
            }
            table.generateSQLCompatibilityStatistic(fileName);
        }
    }

    private boolean isMapperFile(String fileName) {
        return fileName.endsWith("-mapper.result") || fileName.endsWith("-mapper-1.result");
    }

    private ArrayList<SQLCompatibility> parseObject(String fileName) {
        ArrayList<SQLCompatibility> arrayList = new ArrayList<>();
        File file = new File(fileName);
        String line;
        try (BufferedReader br = FilesOperation.getBufferedReader(file)) {
            while ((line = br.readLine()) != null) {
                SQLCompatibility object = JSONObject.parseObject(line, SQLCompatibility.class);
                arrayList.add(object);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }

    private static List<SQLCompatibility> mergeResult(List<SQLCompatibility> list1, List<SQLCompatibility> list2) {
        List<SQLCompatibility> result = new ArrayList<>();
        Map<String, SQLCompatibility> map = list2.stream().collect(Collectors.toMap(
                SQLCompatibility::getId, (element) -> element));
        for (SQLCompatibility object1 : list1) {
            if (map.containsKey(object1.getId())) {
                SQLCompatibility object2 = map.get(object1.getId());
                result.add(chooseOne(object1, object2));
                map.remove(object1.getId());
            } else {
                result.add(object1);
            }
        }
        result.addAll(map.values());
        return result;
    }

    private static SQLCompatibility chooseOne(SQLCompatibility object1, SQLCompatibility object2) {
        if (CompatibilityType.isCompatible(object1) && CompatibilityType.isIncompatible(object2)) {
            return object1;
        }
        if (CompatibilityType.isIncompatible(object1) && CompatibilityType.isCompatible(object2)) {
            return object2;
        }
        return object1;
    }

    private static void writeResToFile(String resFilename, ScanSingleSql scanSingleSql,
                                CompatibilityType compatibilityType, String errorInfo) {
        File resFile = new File(RESULT_DIR + File.separator + resFilename);
        try (BufferedWriter bufWriter = FilesOperation.getBufferedWriter(resFile, true)) {
            JSONObject jsonObject = new JSONObject()
                    .fluentPut(RESULT_KEY_LINE, scanSingleSql.getLine())
                    .fluentPut(RESULT_KEY_SQL, scanSingleSql.getSql())
                    .fluentPut(RESULT_KEY_TYPE, compatibilityType)
                    .fluentPut(RESULT_KEY_ERINFO, errorInfo)
                    .fluentPut(RESULT_KEY_ID, scanSingleSql.getId());
            bufWriter.write(jsonObject.toJSONString() + System.lineSeparator());
        } catch (IOException exp) {
            LOGGER.error("write assessment result record to file failed, filename is " + resFilename);
        }
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

    /**
     * Print process.
     *
     * @param compatibilityTable  : CompatibilityTable.
     */
    public void generateFileReport(CompatibilityTable compatibilityTable) {
        readResFromFile(compatibilityTable);
    }
}
