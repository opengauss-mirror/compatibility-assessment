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

package org.opengauss.tool.replay.task;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeriesCollection;
import org.opengauss.tool.config.replay.ReplayConfig;
import org.opengauss.tool.dispatcher.WorkTask;
import org.opengauss.tool.replay.model.ProcessModel;
import org.opengauss.tool.replay.model.SqlModel;
import org.opengauss.tool.replay.operator.RecordOperator;
import org.opengauss.tool.replay.operator.ReplayLogOperator;
import org.opengauss.tool.replay.operator.ReplaySqlOperator;
import org.opengauss.tool.replay.operator.SlowSqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * ReplayMainTask
 *
 * @since 2024-07-01
 */
public abstract class ReplayMainTask extends WorkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplayMainTask.class);

    /**
     * replaySqlOperator
     */
    protected final ReplaySqlOperator replaySqlOperator;

    /**
     * slowSqlOperator
     */
    protected final SlowSqlOperator slowSqlOperator;

    /**
     * replayLogOperator
     */
    protected final ReplayLogOperator replayLogOperator;

    /**
     * processOperator
     */
    protected final RecordOperator recordOperator;

    /**
     * replayConfig
     */
    protected ReplayConfig replayConfig;

    private final ProcessModel processModel;
    private final ReplaySubTask replaySubTask;

    private long startTime;
    private long endTime;

    /**
     * constructor
     *
     * @param replayConfig replayConfig
     */
    public ReplayMainTask(ReplayConfig replayConfig) {
        this.replayConfig = replayConfig;
        this.replaySqlOperator = new ReplaySqlOperator(replayConfig);
        this.replayLogOperator = new ReplayLogOperator();
        this.slowSqlOperator = new SlowSqlOperator(replayConfig);
        this.recordOperator = new RecordOperator();
        this.processModel = ProcessModel.getInstance();
        this.replaySubTask = replayConfig.getMultiple() > 1 ? new MultipleReplaySubTask(replayConfig)
                : new SingleReplaySubTask(replayConfig);
    }

    /**
     * replay
     */
    public abstract void replay();

    @Override
    public void start() {
        startTime = System.currentTimeMillis();
        slowSqlOperator.createSlowTable();
        recordOperator.recordSqlCount();
        replaySubTask.init();
        replay();
        while (!ProcessModel.getInstance().isReplayFinish()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("wait replay finish occurred an error, e");
            }
        }
        endTime = System.currentTimeMillis();
        stat();
    }

    @Override
    public void stat() {
        printReportLog(startTime, endTime);
        replayLogOperator.printTopSlowSql(processModel.getSlowSqlQueue());
        slowSqlOperator.exportSlowSql();
        draw();
        recordOperator.stopRecord();
    }

    /**
     * replay sql
     *
     * @param sqlModel sqlModel
     */
    protected void replaySql(SqlModel sqlModel) {
        replaySubTask.distribute(sqlModel);
    }

    private void draw() {
        LOGGER.info("start to draw...");
        recordOperator.recordDuration();
        XYDataset dataset = createDataSet();
        JFreeChart chart = ChartFactory.createXYLineChart(
                "SQL Duration of Source and Sink database",
                "Sql Id",
                "Duration(μs)",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );
        try {
            ChartUtils.saveChartAsPNG(new File("compare.png"), chart, 1200, 900);
        } catch (IOException e) {
            LOGGER.error("draw png has occurred an error, error message:{}", e.getMessage());
        }
        LOGGER.info("end to draw...");
    }

    private XYDataset createDataSet() {
        XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(processModel.getMysqlSeries());
        dataset.addSeries(processModel.getOpgsSeries());
        return dataset;
    }

    private void printReportLog(long startTime, long endTime) {
        Instant startInstant = Instant.ofEpochMilli(startTime);
        LocalDateTime startDateTime = LocalDateTime.ofInstant(startInstant, ZoneId.systemDefault());
        Instant endInstant = Instant.ofEpochMilli(endTime);
        LocalDateTime endDateTime = LocalDateTime.ofInstant(endInstant, ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        printReport(startDateTime.format(formatter), endDateTime.format(formatter), startTime, endTime);
    }

    private void printReport(String startTimeFormat, String endTimeFormat, long startTime, long endTime) {
        String summary = String.format(Locale.ROOT, "%sReplay Task has finished %sParallel Number:%d %s"
                        + "Multiple Number:%d %sOnly Replay Query SQL: %s %sStart Time: %s %sEnd Time: %s "
                        + "%sDuration: %s seconds %sTotal Number: %d %sSuccess Number: %d %sSkip Number: %d %s"
                        + "Fail Number: %d %sSlow Sql Number: %d %sMultiple Query Number:%d",
                System.lineSeparator() + "        ",
                System.lineSeparator() + "        ", replayConfig.getMaxPoolSize(),
                System.lineSeparator() + "        ", replayConfig.getMultiple(),
                System.lineSeparator() + "        ", replayConfig.isOnlyReplayQuery(),
                System.lineSeparator() + "        ", startTimeFormat,
                System.lineSeparator() + "        ", endTimeFormat,
                System.lineSeparator() + "        ", (double) (endTime - startTime) / 1000,
                System.lineSeparator() + "        ", processModel.getSqlCount(),
                System.lineSeparator() + "        ", processModel.getSuccessCount(),
                System.lineSeparator() + "        ", processModel.getSkipCount(),
                System.lineSeparator() + "        ", processModel.getFailCount(),
                System.lineSeparator() + "        ", processModel.getSlowCount(),
                System.lineSeparator() + "        ", processModel.getMultipleQueryCount());
        LOGGER.info("{}{}Summary is stored in summary.log", summary, System.lineSeparator() + "        ");
        replayLogOperator.printReport(summary);
    }
}
