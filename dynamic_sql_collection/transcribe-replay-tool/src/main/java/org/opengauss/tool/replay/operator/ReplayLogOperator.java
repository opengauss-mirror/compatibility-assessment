/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
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

package org.opengauss.tool.replay.operator;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.tool.replay.model.ParamModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * ReplayLogOperator
 *
 * @since 2024-07-01
 */
public class ReplayLogOperator {
    private static final Logger FAIL_SQL_LOGGER =
            LoggerFactory.getLogger("org.opengauss.tools.replay.FailSqlLogger");
    private static final Logger SLOW_SQL_LOGGER =
            LoggerFactory.getLogger("org.opengauss.tools.replay.SlowSqlLogger");
    private static final Logger TOP_SLOW_SQL_LOGGER =
            LoggerFactory.getLogger("org.opengauss.tools.replay.TopSlowSqlLogger");
    private static final Logger SUMMARY_LOGGER = LoggerFactory.getLogger("org.opengauss.tools.replay.Summary");
    private static final int SUFFIX_LENGTH = 9;

    /**
     * print slow sql log
     *
     * @param sqlModel sqlModel
     * @param opgsDuration opgsDuration
     * @param explain explain
     */
    public void printSlowSqlLog(SqlModel sqlModel, long opgsDuration, String explain) {
        SLOW_SQL_LOGGER.info("{}Sql Id is: {}{}Sql is: {} {}Sql Parameters: {} {}Execute Plan is :{} "
                        + "{}Source database Execute Duration:{} μs {}Sink database Execute Duration:{} μs",
                System.lineSeparator() + "        ", sqlModel.getId(),
                System.lineSeparator() + "        ", sqlModel.getSql(),
                System.lineSeparator() + "        ", sqlModel.getParameters().toString(),
                System.lineSeparator() + "        ", StringUtils.isEmpty(explain) ? explain
                        : explain.substring(0, explain.length() - SUFFIX_LENGTH),
                System.lineSeparator() + "        ", sqlModel.getMysqlDuration(),
                System.lineSeparator() + "        ", opgsDuration);
    }

    /**
     * print fail sql log
     *
     * @param sqlModel sqlModel
     * @param message message
     */
    public void printFailSqlLog(SqlModel sqlModel, String message) {
        FAIL_SQL_LOGGER.error("{}Sql Id is: {}{}Sql is: {} {}Sql Parameters: {} {}Error Message:{}",
                System.lineSeparator() + "        ", sqlModel.getId(),
                System.lineSeparator() + "        ", sqlModel.getSql(),
                System.lineSeparator() + "        ", sqlModel.getParameters().toString(),
                System.lineSeparator() + "        ", message);
    }

    /**
     * print top n sql sql
     *
     * @param slowSqlQueue slowSqlQueue
     */
    public void printTopSlowSql(PriorityQueue<SqlModel> slowSqlQueue) {
        List<SqlModel> slowSqlList = new ArrayList<>(slowSqlQueue);
        Collections.reverse(slowSqlList);
        for (int i = 1; i <= slowSqlList.size(); i++) {
            SqlModel sqlModel = slowSqlList.get(i - 1);
            TOP_SLOW_SQL_LOGGER.info("{}Slow-Running SQL TOP{}:{}Sql Id is: {}{}Prepare Sql is: {} "
                            + "{}Sql Parameters: {} {}Execute Time(SourceDB):{} μs, Execute Time(SinkDB):{} μs "
                            + "{}Execute Plan is :{}{}{}",
                    System.lineSeparator() + "        ", i,
                    System.lineSeparator() + "        ", sqlModel.getId(),
                    System.lineSeparator() + "        ", sqlModel.getSql(),
                    System.lineSeparator() + "        ", sqlModel.getParameters().stream()
                            .map(ParamModel::getValue)
                            .collect(Collectors.joining(",")),
                    System.lineSeparator() + "        ", sqlModel.getOpgsDuration(), sqlModel.getMysqlDuration(),
                    System.lineSeparator() + "        ",
                    System.lineSeparator() + "        ", StringUtils.isEmpty(sqlModel.getSqlExplain())
                            ? StringUtils.EMPTY
                            : sqlModel.getSqlExplain().substring(0, sqlModel.getSqlExplain().length() - SUFFIX_LENGTH),
                    System.lineSeparator() + "        " + "---------------------------------------------------------"
                            + "---------------------------------------------------------------------------------------"
                            + "---------------------------------------------------------------------------"
            );
        }
    }

    /**
     * print report
     *
     * @param summary summary
     */
    public void printReport(String summary) {
        SUMMARY_LOGGER.info(summary);
    }
}
