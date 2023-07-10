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
import org.opengauss.parser.sqlparser.SqlParser;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Description: get sql statement by connecting to mysql
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public abstract class CollectSqlParser implements SqlParser {
    /**
     * collect sql from which db in mysql
     */
    protected static String dbname = AssessmentInfoManager.getInstance().getProperty(AssessmentInfoChecker.MYSQL
            + "." + AssessmentInfoChecker.DBNAME);

    /**
     * get output sql path
     */
    protected static String outputDir = AssessmentInfoManager.getInstance().getSqlOutDir();

    /**
     * get sql statement by collecting
     *
     * @throws IOException ioexception
     * @throws SQLException sqlexception
     */
    public abstract void parseSql() throws IOException, SQLException;
}
