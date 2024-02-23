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

package org.opengauss.parser.sqlparser;

import com.alibaba.fastjson.JSONObject;
import org.opengauss.parser.FileDistributer;
import org.opengauss.parser.FileLocks;
import org.opengauss.parser.command.Commander;
import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.opengauss.parser.exception.SqlParseExceptionFactory;
import org.opengauss.parser.filehandler.SingleFileHandler;
import org.opengauss.parser.sqlparser.collectsqlparser.AllTableParser;
import org.opengauss.parser.sqlparser.collectsqlparser.GeneralTableParser;
import org.opengauss.parser.sqlparser.collectsqlparser.ObjectSqlParser;
import org.opengauss.parser.sqlparser.collectsqlparser.SlowTableParser;
import org.opengauss.parser.sqlparser.fileinputsqlparser.FilesSqlParser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * Description: SqlParseController, init SqlParser by confiuration
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class SqlParseController {
    /**
     * blank characters regix
     */
    public static final String REPLACEBLANK = "\\s+|\r|\n|\t";

    /**
     * semicolon and enter, line break
     */
    public static final String DELICRLF = ";\r\n|;\r|;\n";

    /**
     * key for origin position or id in middle file
     */
    public static final String KEY_ID = "id";

    /**
     * key for sql statement in middle file
     */
    public static final String KEY_SQL = "sql";

    /**
     * tag for sql statement in middle file
     */
    public static final String KEY_TAG = "tag";

    /**
     * origin xml filename
     */
    public static final String KEY_FILE = "file";
    private static final String FORMAT_REGIX = "^CREATE\\s+(FUNCTION|TRIGGER|PROCEDURE)";
    private static final Pattern SQLFORMATPATTERN = Pattern.compile(FORMAT_REGIX, Pattern.CASE_INSENSITIVE);
    private static final String DELIMITER = "delimiter";
    private static final String SEMI = ";";
    private static final String SLASH = "//";
    private static final List<File> DEFAULT_FILE_LIST = new ArrayList<>();

    private SqlParser sqlParser = null;

    /**
     * Constructor
     */
    public SqlParseController() {
        initSqlParser();
    }

    /**
     * sql parsing entry function
     */
    public void parseSql() {
        try {
            sqlParser.parseSql();
        } catch (IOException | SQLException exp) {
            throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.PARSEEXCEPTION_CODE,
                    "sql parse occur exception. exp: " + exp.getMessage());
        }
    }

    /**
     * identify whether sql is definiation of function, procedure or trigger
     *
     * @param sql String
     * @return boolean
     */
    public static boolean isNeedFormat(String sql) {
        return SQLFORMATPATTERN.matcher(sql).find();
    }

    /**
     * format sql
     *
     * @param sql String
     * @return String
     */
    public static String format(String sql) {
        String newSql = DELIMITER + " " + SLASH + System.lineSeparator();
        newSql += sql.replaceAll(DELICRLF, ";").trim();
        if (newSql.endsWith(";")) {
            newSql = newSql.substring(0, newSql.length() - 1);
        }
        newSql += SLASH + System.lineSeparator();
        newSql += DELIMITER + " " + SEMI + System.lineSeparator();
        return newSql;
    }

    /**
     * write sql to sqlfile
     *
     * @param filename String
     * @param bufWriter BufferedWriter
     * @param builder StringBuilder
     * @throws IOException ioexception
     */
    public static void writeSqlToFile(String filename, BufferedWriter bufWriter, StringBuilder builder)
            throws IOException {
        ReentrantReadWriteLock locker = FileLocks.addLockerAndLockFile(filename);
        bufWriter.write(builder.toString());
        locker.writeLock().unlock();
    }

    /**
     * write sql to sqlfile
     *
     * @param builder StringBuilder
     * @param valueId Object
     * @param valueSql String
     */
    public static void appendJsonLine(StringBuilder builder, Object valueId, String valueSql) {
        builder.append(new JSONObject().fluentPut(SqlParseController.KEY_ID, valueId)
                .fluentPut(SqlParseController.KEY_SQL, valueSql)
                .toJSONString() + System.lineSeparator());
    }

    /**
     * write sql to sqlfile
     *
     * @param builder StringBuilder
     * @param valueId Object
     * @param valueSql String
     * @param valueTag String
     * @param filename String
     */
    public static void appendJsonLine(StringBuilder builder, Object valueId, String valueSql, String valueTag,
                                      String filename) {
        builder.append(new JSONObject().fluentPut(SqlParseController.KEY_ID, valueId)
                .fluentPut(SqlParseController.KEY_SQL, valueSql)
                .fluentPut(SqlParseController.KEY_TAG, valueTag)
                .fluentPut(SqlParseController.KEY_FILE, filename)
                .toJSONString() + System.lineSeparator());
    }

    private void initSqlParser() {
        AssessmentInfoManager assessmentInfo = AssessmentInfoManager.getInstance();
        FileDistributer fileDistributer = new FileDistributer();
        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
            String dataDir = assessmentInfo.getProperty(AssessmentInfoChecker.FILEDIR);
            Map<String, List<File>> filesMap = fileDistributer.distributeFiles(dataDir);
            sqlParser = new FilesSqlParser(
                    filesMap.getOrDefault(SingleFileHandler.SQLFILE_EXTENSION, DEFAULT_FILE_LIST),
                    filesMap.getOrDefault(SingleFileHandler.GENERALLOG_EXTENSION, DEFAULT_FILE_LIST),
                    filesMap.getOrDefault(SingleFileHandler.SLOWLOG_EXTENSION, DEFAULT_FILE_LIST),
                    filesMap.getOrDefault(SingleFileHandler.MAPPER_EXTENSION, DEFAULT_FILE_LIST));
        } else {
            String assessmentType = assessmentInfo.getProperty(AssessmentInfoChecker.ASSESSMENTTYPE);
            if (assessmentType.equalsIgnoreCase(AssessmentInfoChecker.ASSESSMENT_OBJECT)) {
                sqlParser = new ObjectSqlParser();
            } else if (assessmentType.equalsIgnoreCase(AssessmentInfoChecker.ASSESSMENT_SQL)) {
                String sqlType = assessmentInfo.getProperty(AssessmentInfoChecker.SQLTYPE);
                sqlParser = (AssessmentInfoChecker.GENERAL_SQL).equalsIgnoreCase(sqlType)
                        ? new GeneralTableParser() : new SlowTableParser();
            } else {
                sqlParser = new AllTableParser();
            }
        }
    }
}

