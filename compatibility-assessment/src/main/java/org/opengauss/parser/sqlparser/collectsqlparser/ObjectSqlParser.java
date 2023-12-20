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
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description: get object definiation by connecting to mysql
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class ObjectSqlParser extends CollectSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CollectSqlParser.class);
    private static final String VIEWDDL_PREFIX = "Create View";
    private static final String OUTPUTFILE = "collect_object";
    private static final String REGIX_VIEW = "^CREATE(.+)DEFINER VIEW?";
    private static final Pattern PATTERN_VIEW = Pattern.compile(REGIX_VIEW, Pattern.CASE_INSENSITIVE);
    private static Map<String, String> objectCreatestmtCol = new HashMap<>() {
        {
            put("table", "Create Table");
            put("view", "Create View");
            put("trigger", "SQL Original Statement");
            put("function", "Create Function");
            put("procedure", "Create Procedure");
        }
    };
    private static Map<String, String> sqlGetObject = new HashMap<>() {
        {
            put("table", "select table_name as object_name from information_schema.tables"
                    + " where table_schema = ? and table_type = 'BASE TABLE'");
            put("view", "select table_name as object_name from information_schema.views "
                    + "where table_schema = ?");
            put("trigger", "select trigger_name as object_name from information_schema.triggers"
                    + " where trigger_schema = ?");
            put("function", "select routine_name as object_name from information_schema.ROUTINES"
                    + " where ROUTINE_SCHEMA = ? and ROUTINE_TYPE = 'FUNCTION'");
            put("procedure", "select routine_name as object_name from information_schema.ROUTINES"
                    + " where ROUTINE_SCHEMA = ? and ROUTINE_TYPE = 'PROCEDURE'");
        }
    };


    /**
     * get all object ddl
     *
     * @throws IOException ioexception
     * @throws SQLException sqlexception
     */
    public void parseSql() throws IOException, SQLException {
        String outPutfilename = outputDir + "/" + OUTPUTFILE;
        StringBuilder builder = new StringBuilder();
        File outPutFile = new File(outPutfilename);
        if (!FilesOperation.isCreateOutputFile(outPutFile, outputDir)) {
            LOGGER.warn("create object outputfile failed, it may already exists! outputfile: " + outPutfilename);
        }
        try (Connection connection = DbConnector.getMysqlConnection();
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(outPutFile, false)) {
            getDDL(connection, builder);
            SqlParseController.writeSqlToFile(outPutFile.getName(), bufWriter, builder);
        } catch (IOException | SQLException exp) {
            LOGGER.warn("get object sql_statement occur IOException or SQLException.", exp);
            throw exp;
        }
    }

    private void getDDL(Connection connection, StringBuilder builder) {
        String[] objects = {"table", "view", "trigger", "function", "procedure"};
        for (String object : objects) {
            String sqlGetddl = "show create " + object + " %s";
            List<String> objectddls = getDDLListByObject(connection, sqlGetObject.get(object), sqlGetddl, object);
            addDDLToBuilder(objectddls, object, builder);
        }
    }

    private List<String> getDDLListByObject(Connection connection, String sqlGetObject, String sqlGetddl,
                                            String objectType) {
        List<String> ddlList = new ArrayList<>();
        ResultSet rsObjectList = null;
        try (PreparedStatement pstmtGetObject = connection.prepareStatement(sqlGetObject)) {
            pstmtGetObject.setString(1, dbname);
            rsObjectList = pstmtGetObject.executeQuery();
            while (rsObjectList.next()) {
                try (Statement stmtGetDDL = connection.createStatement();
                     ResultSet rsDDL = stmtGetDDL.executeQuery(
                            String.format(sqlGetddl, rsObjectList.getString("object_name")))) {
                    if (rsDDL.next()) {
                        ddlList.add(rsDDL.getString(objectCreatestmtCol.get(objectType)));
                    }
                }
            }
        } catch (SQLException exp) {
            LOGGER.error("get object " + objectType + " occur exception.");
        } finally {
            try {
                rsObjectList.close();
            } catch (SQLException exp) {
                LOGGER.error("close object " + objectType + " resultset occur exception.");
            }
        }
        return ddlList;
    }

    private void addDDLToBuilder(List<String> ddls, String objectType, StringBuilder builder) {
        for (String ddl : ddls) {
            if ("table".equalsIgnoreCase(objectType) || "view".equalsIgnoreCase(objectType)) {
                if ("view".equalsIgnoreCase(objectType)) {
                    Matcher matcher = PATTERN_VIEW.matcher(ddl);
                    ddl = matcher.replaceAll(VIEWDDL_PREFIX);
                }
                SqlParseController.appendJsonLine(builder, null,
                        ddl.replaceAll(SqlParseController.REPLACEBLANK, " "));
            } else {
                String[] strs = ddl.split(" ", 3);
                ddl = strs[0] + " " + strs[2];
                SqlParseController.appendJsonLine(builder, null, ddl);
            }
        }
    }
}
