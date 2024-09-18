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

import org.apache.commons.io.FileUtils;
import org.opengauss.parser.command.Commander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Compatibility table.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class CompatibilityTable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompatibilityTable.class);
    private static final String JS_FILENAME = System.getProperty("user.dir") + File.separator + "js"
            + File.separator + "Chart.bundle.min.js";
    private static final String LINESEP = System.lineSeparator();

    private File fd;

    private Queue<SQLCompatibility> sqlCompatibilities = new LinkedList<>();

    private int contentTableCount = 0;

    private int chartsCount = 0;

    /**
     * Construct
     */
    public CompatibilityTable() {
        super();
    }

    /**
     * append sql compatibility to Queue
     *
     * @param arrayList List
     */
    public void appendMultipleSQL(List<SQLCompatibility> arrayList) {
        for (SQLCompatibility object : arrayList) {
            this.sqlCompatibilities.add(object);
        }
    }

    /**
     * get sql compatibilities
     *
     * @return Queue<SQLCompatibility>
     */
    public Queue<SQLCompatibility> getSqlCompatibilities() {
        return sqlCompatibilities;
    }

    /**
     * Generate report file.
     *
     * @return boolean
     */
    public boolean generateReport() {
        String strHeader = "<tr><td colspan=\"2\"><h3 class=\"wdr\">SQL 兼容详情</h3>"
            + "<div class=\"tdiff\" summary=\"This table displays SQL Assessment Data\" width=100% >"
            + "<div width=100% style=\"color: #ffffff;display: flex;\">"
            + "<div class=\"wdrbg\" style=\"background-color: #04a0e8;"
            + " flex: 0 0 2%;display: flex; justify-content: center; align-items: center;"
            + " margin: 3px 6px 0 0;\">行号</div> "
            + "<div class=\"wdrbg\" style=\"background-color: #04a0e8; "
            + "flex: 0 0 58%; display: flex; justify-content: center; align-items: center;"
            + " margin: 3px 6px 0 0;\">SQL语句</div> "
            + "<div class=\"wdrbg\" style=\"background-color: #04a0e8; "
            + "flex: 0 0 4%; display: flex; justify-content: center; align-items: center;"
            + " margin: 3px 6px 0 0;\">兼容性</div> "
            + "<div class=\"wdrbg\" style=\"background-color: #04a0e8;"
            + " flex-grow: 1; display: flex; justify-content: center; align-items: center; "
            + "margin: 3px 6px 0 0;\">兼容性详情</div> ";

        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
            strHeader += "<div class=\"wdrbg\" style=\"background-color: #04a0e8; "
                    + "flex: 0 0 15%; display: flex; justify-content: center; align-items: center;"
                    + "margin: 3px 6px 0 0 ;\">初始位置</div></div>";
        } else {
            strHeader += "</div>";
        }
        strHeader += "<div><div class=\"content-table" + contentTableCount + "\" width=100%>";
        contentTableCount++ ;

        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(strHeader);
                long index = 0L;
                while (!this.sqlCompatibilities.isEmpty()) {
                    SQLCompatibility sqlCompatibility = this.sqlCompatibilities.poll();
                    String sqlDetail = "<div class=\"content-row\"  data-label=\""
                        + getCompatibilityString(sqlCompatibility.getCompatibilityType())
                        + "\" style=\" display: flex;align-items: stretch; \">"
                        + "<div style=\"background-color: #ededed; flex: 0 0 2%;display: flex;"
                        + " justify-content: center; align-items: center; margin: 3px 6px 0 0;\">"
                        + sqlCompatibility.getLine()
                        + "</div><div style=\"background-color: #ededed; flex: 0 0 58%; display: flex;"
                        + " justify-content: center; align-items: center; margin: 3px 6px 0 0;\">"
                        + sqlCompatibility.getSql()
                        + "</div><div class=\"category-container\""
                        + "style=\"background-color: #ededed; flex: 0 0 4%; display: flex; "
                        + "justify-content: center; align-items: center; margin: 3px 6px 0 0;\">"
                        + getCompatibilityString(sqlCompatibility.getCompatibilityType())
                        + "</div><textarea style=\"background-color: #ededed; flex-grow: 1; display: flex; "
                        + "justify-content: center; align-items: center; margin: 3px 6px 0 0;resize: vertical;\" >"
                        + sqlCompatibility.getErrDetail() + "</textarea>";
                    if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
                        sqlDetail += "<div style=\"background-color: #ededed; flex: 0 0 15%; display: flex;"
                            + " justify-content: center; align-items: center;margin: 3px 6px 0 0 ;\">"
                            + sqlCompatibility.getId() + "</div>";
                    }
                    sqlDetail += "</div>" + System.lineSeparator();
                    bufferedWriter.write(sqlDetail);
                    index++;
                }
                String tablesuffix = "</div></div></div></td></tr></table></div>" + System.lineSeparator();
                bufferedWriter.write(tablesuffix);
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    /**
     * Generate sql compatibility statistic.
     *
     * @param fileName : sql file name.
     * @return boolean
     */
    public boolean generateSQLCompatibilityStatistic(String fileName) {
        String strCharts = "<h2 class=\"wdr\">" + fileName + "</h2>"
            + "<div style=\"display: block;\"><table class=\"tdiff\" style=\"width: 100%;\">"
            + "<tr><!-- 图表区 （上侧）--><td style=\"width: 17%;\" >"
            + "<canvas id=\"myCharts" + chartsCount + "\" style=\"width: 100%;height: 230px;\">环形图表区</canvas>"
            + "</td><td style=\"width: 83%;\" ><canvas id=\"myBarCharts"
            + chartsCount + "\" style=\"width: 100%;height: 250px;\">条形图表区</canvas>"
            + "</td></tr>" + System.lineSeparator();
        chartsCount++ ;
        AssessmentEntry.increTotalSql(this.sqlCompatibilities.size());
        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(strCharts);
            }
        } catch (IOException e) {
            return false;
        }
        return generateReport();
    }

    /**
     * Get compatibility string.
     *
     * @param compatibilityType : assessment result type.
     * @return String
     */
    public static String getCompatibilityString(CompatibilityType compatibilityType) {
        switch (compatibilityType) {
            case COMPATIBLE:
                return "完全兼容";
            case AST_COMPATIBLE:
                return "语法兼容";
            case INCOMPATIBLE:
                return "不兼容";
            case UNSUPPORTED_COMPATIBLE:
                return "暂不支持评估";
            case SKIP_COMMAND:
                return "忽略语句";
            default:
                return "unreached branch";
        }
    }

    /**
     * Generate report header.
     *
     * @param output : output path.
     * @param compat : assessment database compatibility
     * @return boolean
     */
    public boolean generateReportHeader(String output, String compat) {
        this.fd = new File(output);
        String jsContent = readJsContent(JS_FILENAME);
        String strCss = "<!DOCTYPE html><html lang=\"en\">"
            + "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
            + System.lineSeparator()
            + "<head><title>openGauss 兼容性评估报告</title>" + System.lineSeparator() + "<style type=\"text/css\">"
            + "body.wdr {font:12pt Arial, Helvetica, Geneva, sans-serif;color:black;background:White;}"
            + "h1.wdr {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#000000;"
            + "background-color:White;border-bottom:1px solid #cccc99;margin-top:0pt; "
            + "margin-bottom:0pt;padding:0px 0px 0px 0px;}"
            + "h2.wdr {display: block;font:bold 18pt Arial,Helvetica,Geneva,sans-serif;"
            + "color:#000000;background-color:#c1b9b9;margin-top:4pt;margin-bottom:0pt;}"
            + "h3.wdr {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#000000;"
            + "background-color:White;margin-top:4pt;margin-bottom:0pt;}"
            + "canvas {background: #ffffff;}"
            + ".content-row td{background: #f3f3f3;}textarea {font:12pt Arial, Helvetica, Geneva, sans-serif;border: none;"
            + "box-sizing: border-box;}"
            + "</style>"
            + "<script>" + jsContent + "</script>"
            + "</head><body class=\"wdr\">" + System.lineSeparator()
            + "<div id=\"collapse\">"
            + "<h1 class=\"wdr\">" + compat + " 兼容性评估报告</h1>" + System.lineSeparator();
        return canWriteData(strCss);
    }

    private String readJsContent(String filename) {
        String content = null;
        try {
            content = FileUtils.readFileToString(new File(filename), "UTF-8");
        } catch (IOException exp) {
            LOGGER.error("read js file content occur io exception");
        }
        return content;
    }

    private boolean canWriteData(String str) {
        try (FileWriter fileWriter = new FileWriter(fd)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(str);
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }


    /**
     * Generate report end.
     *
     * @return boolean
     */
    public boolean generateReportEnd() {
        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(new CompatibilityTableScript().getScript());
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}