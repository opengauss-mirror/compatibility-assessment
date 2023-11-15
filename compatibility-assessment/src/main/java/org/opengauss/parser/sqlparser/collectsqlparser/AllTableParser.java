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

import org.opengauss.parser.configure.AssessmentInfoChecker;
import org.opengauss.parser.configure.AssessmentInfoManager;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Description: assessment for both sql and object
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class AllTableParser extends CollectSqlParser {
    private GeneralTableParser generalParser;
    private SlowTableParser slowParser;
    private ObjectSqlParser objectSqlParser;

    /**
     * Constructor
     */
    public AllTableParser() {
        initSqlParsers();
    }

    /**
     * collect sql statement from object table and sql table
     *
     * @throws SQLException sqlexception
     * @throws IOException ioexception
     */
    public void parseSql() throws SQLException, IOException {
        objectSqlParser.parseSql();
        if (generalParser != null) {
            generalParser.parseSql();
        }
        if (slowParser != null) {
            slowParser.parseSql();
        }
    }

    private void initSqlParsers() {
        objectSqlParser = new ObjectSqlParser();
        String sqlType = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.SQLTYPE);
        if (sqlType.equalsIgnoreCase(AssessmentInfoChecker.GENERAL_SQL)) {
            generalParser = new GeneralTableParser();
        } else {
            slowParser = new SlowTableParser();
        }
    }
}
