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

package org.opengauss.parser.sqlparser.collectsqlparser;

import org.opengauss.parser.DbConnector;
import org.opengauss.parser.FilesOperation;
import org.opengauss.parser.sqlparser.SqlParseController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description: get sql statement from mysql.general_log
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class GeneralTableParser extends CollectSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneralTableParser.class);
    private static final String COMMAND_CONNECT = "Connect";
    private static final String COMMAND_INITDB = "Init DB";
    private static final String OUTPUTFILE = "collect_general";
    private static final String SELECT_GENERALSQL = "select thread_id, command_type, argument from mysql.general_log";
    private static final Integer ARGUMENT_SPLITNUM = 5;
    private static final Integer DBINDEX_IN_ARGUMENT = 2;
    private static final List<String> COMMAND_LIST = new ArrayList<>() {
        {
            add("Query");
            add("Execute");
        }
    };

    /**
     * parse sql from mysql.general_log, need specify dbname
     *
     * @throws SQLException sqlexception
     * @throws IOException ioexception
     */
    public void parseSql() throws SQLException, IOException {
        String threadId;
        String commandType;
        String argument;
        StringBuilder builder = new StringBuilder();
        Map<String, String> threadidToDb = new HashMap<String, String>();
        String outPutfilename = outputDir + File.separator + OUTPUTFILE;
        File outPutFile = new File(outPutfilename);
        if (!FilesOperation.isCreateOutputFile(outPutFile, outputDir)) {
            LOGGER.warn("create collect_general outputfile failed, it may already exists. outputfile: "
                    + outPutfilename);
        }
        try (Connection connection = DbConnector.getMysqlConnection();
             Statement statement = connection.createStatement();
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(outPutFile, false);
             ResultSet rs = statement.executeQuery(SELECT_GENERALSQL)) {
            while (rs.next()) {
                threadId = rs.getString(1);
                commandType = rs.getString(2);
                argument = rs.getString(3);
                if (isUpdateThreadidToDB(threadidToDb, threadId, commandType, argument)) {
                    continue;
                }
                appendSqlToBuilder(threadId, commandType, argument, threadidToDb, builder);
            }
            SqlParseController.writeSqlToFile(outPutFile.getName(), bufWriter, builder);
        } catch (SQLException | IOException exp) {
            LOGGER.error("parse general table occur SQLException or IOException", exp.getMessage());
            throw exp;
        }
    }

    private void appendSqlToBuilder(String threadId, String commandType, String argument,
                                    Map<String, String> threadidToDb, StringBuilder builder) {
        if (threadidToDb.get(threadId) != null
                && threadidToDb.get(threadId).equals(dbname)) {
            if (COMMAND_LIST.contains(commandType)) {
                SqlParseController.appendJsonLine(builder, null, argument);
            }
        }
    }

    private static boolean isUpdateThreadidToDB(Map<String, String> threadidToDb, String threadId,
                                                String commandType, String argument) {
        if (COMMAND_CONNECT.equals(commandType)) {
            if (argument.split("\\s+").length == ARGUMENT_SPLITNUM) {
                threadidToDb.put(threadId, argument.split("\\s+")[DBINDEX_IN_ARGUMENT]);
                return true;
            }
        }
        if (COMMAND_INITDB.equals(commandType)) {
            threadidToDb.put(threadId, argument);
            return true;
        }
        return false;
    }
}
