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
    private static final String FORMAT_REGIX = "^CREATE\\s+(FUNCTION|TRIGGER|PROCEDURE)";
    private static final Pattern SQLFORMATPATTERN = Pattern.compile(FORMAT_REGIX, Pattern.CASE_INSENSITIVE);
    private static final String DELIMITER = "delimiter";
    private static final String SEMI = ";";
    private static final String SLASH = "//";

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
        newSql += sql.replaceAll(REPLACEBLANK, " ");
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
        ReentrantReadWriteLock locker = FileLocks.addLocker(filename);
        ReentrantReadWriteLock.WriteLock writeLocker = locker.writeLock();
        writeLocker.lock();
        bufWriter.write(builder.toString());
        writeLocker.unlock();
    }

    private void initSqlParser() {
        AssessmentInfoManager assessmentInfo = AssessmentInfoManager.getInstance();
        FileDistributer fileDistributer = new FileDistributer();
        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
            String dataDir = assessmentInfo.getProperty(AssessmentInfoChecker.FILEDIR);
            Map<String, List<File>> filesMap = fileDistributer.distributeFiles(dataDir);
            sqlParser = new FilesSqlParser(filesMap.get(SingleFileHandler.SQLFILE_EXTENSION),
                    filesMap.get(SingleFileHandler.GENERALLOG_EXTENSION),
                    filesMap.get(SingleFileHandler.SLOWLOG_EXTENSION),
                    filesMap.get(SingleFileHandler.MAPPER_EXTENSION));
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

