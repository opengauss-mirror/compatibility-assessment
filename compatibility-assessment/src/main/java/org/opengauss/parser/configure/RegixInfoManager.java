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

package org.opengauss.parser.configure;

import java.util.regex.Pattern;

/**
 * Description: Regix expression manager
 *
 * @author jianghongbo
 * @since 2023/7/18
 */
public class RegixInfoManager {
    private static final String SLOWLOG_REGIX = "# Query_time: \\d+\\.\\d+  Lock_time: \\d+\\.\\d+ Rows_sent:"
            + " \\d+  Rows_examined: \\d+";
    private static final String GENLOG_TIME = "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{6}(Z|(\\+08:00))";
    private static final String GENLOG_THREADID = "\\d+";
    private static final String[] GENLOG_CMDTYPES = {"Quit", "Init DB", "Query", "Field List", "Connect", "Execute",
            "Prepare", "Close stmt"};
    private static final String GENLOG_CMDTYPES_REGIX = String.join("|", GENLOG_CMDTYPES);
    private static final String GENLOG_REGIX = GENLOG_TIME
            + ")\\s+(" + GENLOG_THREADID + ")\\s+(" + GENLOG_CMDTYPES_REGIX + ")";
    private static final String[] SQLSTMT_HEADERS = {"SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
            "DESC"};
    private static final String SQLSTMT_HEADERS_REGIX = "^(" + String.join("|", SQLSTMT_HEADERS) + ")\\s+";

    /**
     * get slow log regix
     *
     * @return String
     */
    public static String getSlowlogRegix() {
        return SLOWLOG_REGIX;
    }

    /**
     * get general log regix
     *
     * @return String
     */
    public static String getGenlogRegix() {
        return GENLOG_REGIX;
    }

    /**
     * get sql statement header regix
     *
     * @return Pattern
     */
    public static Pattern getSqlstmtHeadersRegix() {
        return Pattern.compile(SQLSTMT_HEADERS_REGIX, Pattern.CASE_INSENSITIVE);
    }
}
