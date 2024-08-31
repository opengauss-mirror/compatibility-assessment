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

package org.opengauss.parser.entry;

import org.opengauss.parser.FilesOperation;
import org.opengauss.parser.command.Commander;
import org.opengauss.parser.configure.AssessmentInfoManager;
import org.opengauss.parser.exception.SqlParseExceptionFactory;
import org.opengauss.parser.sqlparser.SqlParseController;

import java.io.File;

/**
 * Description: parse module entry
 *
 * @author jianghongbo
 * @since 2023/6/30
 */
public class CommandEntry {
    private Commander commander;

    /**
     * Constructor
     */
    public CommandEntry() {
        commander = new Commander();
    }

    /**
     * parse commandline args and properties entry, and sql parse entry
     *
     * @param args String[]
     */
    public void mainEntry(String[] args) {
        commander.parseCmd(args);
        try {
            FilesOperation.clearDir(new File(AssessmentInfoManager.getInstance().getSqlOutDir()));
        } catch (NullPointerException exp) {
            throw SqlParseExceptionFactory.getException(SqlParseExceptionFactory.FILESEXCEPTION_CODE,
                    "clean sqlFiles occur exception. exp: " + exp.getMessage());
        }
        SqlParseController sqlParseController = new SqlParseController();
        sqlParseController.parseSql();
    }
}

