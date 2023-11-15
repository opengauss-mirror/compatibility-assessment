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

/**
 * Description: get sql statement from mysql.slow_log
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class SlowTableParser extends CollectSqlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SlowTableParser.class);
    private static final String OUTPUTFILE = "collect_slow.sql";
    private static final String QUERY_SQL = "select sql_text from mysql.slow_log where db = '%s'";

    /**
     * parse sql from mysql.slow_log, need specify dbname
     *
     * @throws IOException ioexception
     * @throws SQLException sqlexception
     */
    public void parseSql() throws IOException, SQLException {
        String sql = String.format(QUERY_SQL, dbname);
        String outPutfilename = outputDir + File.separator + OUTPUTFILE;
        StringBuilder builder = new StringBuilder();
        File outPutFile = new File(outPutfilename);
        if (!FilesOperation.isCreateOutputFile(outPutFile, outputDir)) {
            LOGGER.warn("create collect_slow outputfile failed, it may already exists. outputfile: " + outPutfilename);
        }
        try (Connection connection = DbConnector.getMysqlConnection();
             Statement statement = connection.createStatement();
             BufferedWriter bufWriter = FilesOperation.getBufferedWriter(outPutFile, false);
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String slowSql = rs.getString(1);
                if (SqlParseController.isNeedFormat(slowSql)) {
                    builder.append(SqlParseController.format(slowSql));
                } else {
                    builder.append(slowSql.replaceAll(SqlParseController.REPLACEBLANK, " ")
                            + ";" + System.lineSeparator());
                }
            }
            SqlParseController.writeSqlToFile(outPutFile.getName(), bufWriter, builder);
        } catch (SQLException | IOException exp) {
            LOGGER.error("parse slow table occur SQLException or IOException.", exp.getMessage());
            throw exp;
        }
    }
}
