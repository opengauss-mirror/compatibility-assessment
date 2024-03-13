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

import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * filter incompatibility sql
 *
 * @author : jianghongbo
 * @since : 2024/2/21
 */
public class InCompatibilityFilter {
    private static final Logger LOGGER = LoggerFactory.getLogger(InCompatibilityFilter.class);
    private static final String ORDERBY_REG_SINCOL = "order\\s+by\\s+\\S+\\s+";
    private static final String ORDERBY_REG_MULCOL = "order\\s+by\\s+(\\S+(\\s+ASC|\\s+DESC)?(\\s+)?,(\\s+)?)+\\S+\\s+";
    private static final String ERRORINFO_REG = "ERROR: syntax error at or near ";
    private static final String SQL_HEADER = "^(select|insert\\s+into|update\\s+(\\w+)\\s+set"
            + "|create|alter|drop|delete\\s+from)\\s+";
    private static final String SELECTFROM = "^select\\s+from";
    private static final List<String> keyWordList = Arrays.asList("status", "level", "domain", "in", "order", "content",
            "month", "year", "account", "target", "name", "role", "ip", "text", "hour", "mid", "version", "date",
            "ratio", "time", "columns", "day", "number", "password", "timestamp", "temporary", "plans", "catalog_name",
            "rows", "percent", "names", "limit", "authid", "location", "cluster", "storage", "shrink", "column_name",
            "system", "language", "datetime", "disk", "enable", "copy", "mapping", "checkpoint", "operator", "verify",
            "sysid", "workload", "comment", "schedule", "no", "extract", "channel", "view", "share", "owner",
            "resource", "source", "node", "label", "performance", "freeze");
    private static final Pattern SQLHEADER_PAT = Pattern.compile(SQL_HEADER, Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECTFROM_PAT = Pattern.compile(SELECTFROM, Pattern.CASE_INSENSITIVE);

    /**
     * filter and replace incompatibility sql, return pair.
     * the first element means if sql be replaced, the second element
     * means sql after replace.
     *
     * @param sql String
     * @param errInfo String
     * @return Pair
     */
    public static Pair<Boolean, String> filterIncompaSql(String sql, String errInfo) {
        String errKey = getErrKey(errInfo);
        if (errKey.length() == 0) {
            return new Pair<>(false, sql);
        }
        String replaceSql = filterOrderBy(sql, errKey, errInfo);
        return new Pair<>(!sql.equals(replaceSql), replaceSql);
    }

    /**
     * identify keyword from error message, and update error message.
     *
     * @param errInfo String
     * @return String
     */
    public static String filterKeyWord(String errInfo) {
        String errKey = getErrKey(errInfo);
        String newErrInfo = errInfo;
        if (errKey.length() == 0 || !keyWordList.contains(errKey.toLowerCase(Locale.ROOT))) {
            return newErrInfo;
        }
        newErrInfo = "关键字问题：" + errKey + System.lineSeparator() + errInfo;
        return newErrInfo;
    }

    /**
     * filter sql contains syntax errors.
     *
     * @param sql String
     * @return boolean
     */
    public static boolean filterErrorSql(String sql) {
        if (!SQLHEADER_PAT.matcher(sql).find()) {
            return false;
        }
        if (SELECTFROM_PAT.matcher(sql).find()) {
            return false;
        }
        return true;
    }

    private static String getErrKey(String errInfo) {
        int errIndex;
        if ((errIndex = errInfo.indexOf(ERRORINFO_REG)) == -1) {
            return "";
        }

        String subErr = errInfo.substring(errIndex + ERRORINFO_REG.length() + 1);
        String[] arr = subErr.split("\\s+");
        return arr[0].substring(0, arr[0].length() - 1);
    }

    private static int getErrKeyPos(String errInfo) {
        int pos = -1;
        int index;
        if ((index = errInfo.indexOf("位置：")) != -1) {
            index += 3;
            pos = Integer.valueOf(errInfo.substring(index));
        }
        return pos;
    }

    private static String filterOrderBy(String sql, String errKey, String errInfo) {
        try {
            Pattern orderSincolPat = Pattern.compile(ORDERBY_REG_SINCOL + errKey, Pattern.CASE_INSENSITIVE);
            Pattern orderMulcolPat = Pattern.compile(ORDERBY_REG_MULCOL + errKey, Pattern.CASE_INSENSITIVE);
            if (orderMulcolPat.matcher(sql).find() || orderSincolPat.matcher(sql).find()) {
                return replaceErrKey(sql, errKey, getErrKeyPos(errInfo));
            }
        } catch (PatternSyntaxException exp) {
            LOGGER.warn("handle incompatible sql ouucr PatternSyntaxException, mode: order by, errkey is " + errKey);
        }
        return sql;
    }

    private static String replaceErrKey(String sql, String errKey, int errPos) {
        int keyLen = errKey.length();
        int index = 0;
        String replaceSql = "";
        while (index < sql.length() && (index = sql.indexOf(errKey, index)) != -1) {
            if (index + keyLen >= errPos) {
                replaceSql = (sql.substring(0, index) + sql.substring(index + keyLen));
                break;
            }
            index += keyLen;
        }
        return replaceSql;
    }
}