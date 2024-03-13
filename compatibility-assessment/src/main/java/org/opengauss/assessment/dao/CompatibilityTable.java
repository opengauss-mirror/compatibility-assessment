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

import org.opengauss.parser.command.Commander;

import java.io.File;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.opengauss.assessment.dao.CompatibilityType.UNSUPPORTED_COMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.AST_COMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.COMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.INCOMPATIBLE;
import static org.opengauss.assessment.dao.CompatibilityType.SKIP_COMMAND;

/**
 * Compatibility table.
 *
 * @author : yuchao
 * @since : 2023/7/7
 */
public class CompatibilityTable {
    private File fd;

    private Queue<SQLCompatibility> sqlCompatibilities = new LinkedList<>();

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
        String str = "<a class=\"wdr\" name=\"top\"></a><h2 class=\"wdr\">SQL 兼容详情</h2>"
                + "<table class=\"tdiff\" summary=\"This table displays SQL Assessment Data\" width=100%><tr>"
                + System.lineSeparator() + "<th class=\"wdrbg\" scope=\"col\">行号</th>"
                + "<th class=\"wdrbg\" scope=\"col\">SQL语句</th>"
                + "<th class=\"wdrbg\" scope=\"col\">兼容性</th>"
                + "<th class=\"wdrbg\" scope=\"col\">兼容性详情</th>";

        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
            str += "<th class=\"wdrbg\" scope=\"col\">初始位置</th>";
            str += "<th class=\"wdrbg\" scope=\"col\">源文件名</th>";
        }
        str += "</tr>" + System.lineSeparator();

        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(str);
                long index = 0L;
                while (!this.sqlCompatibilities.isEmpty()) {
                    String type;
                    if ((index & 1) == 0) {
                        type = "wdrnc";
                    } else {
                        type = "wdrc";
                    }

                    SQLCompatibility sqlCompatibility = this.sqlCompatibilities.poll();
                    String sqlDetail = "<tr style=\"border-top-width: 2px;\">"
                            + "<td style=\"width:2%\" class=\"" + type + "\">" + sqlCompatibility.getLine()
                            + System.lineSeparator() + "</td>" + System.lineSeparator()
                            + "<td style=\"width:50%\" class=\"" + type + "\">" + sqlCompatibility.getSql()
                            + System.lineSeparator() + "</td>" + System.lineSeparator()
                            + "<td style=\"width:4%\" class=\"" + type + "\" align=\"left\" >"
                            + getCompatibilityString(sqlCompatibility.getCompatibilityType()) + "</td>"
                            + System.lineSeparator()
                            + "<td style=\"width:20%\" class=\"" + type + "_err\" align=\"left\" >"
                            + sqlCompatibility.getErrDetail();

                    if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
                        sqlDetail += "<td style=\"width:15%\" class=\"" + type + "\" align=\"center\">"
                                + sqlCompatibility.getId();
                        sqlDetail += "<td style=\"width:9%\" class=\"" + type + "\" align=\"center\">"
                                + sqlCompatibility.getOriginFileName();
                    }
                    sqlDetail += "</td>" + System.lineSeparator() + "</tr>" + System.lineSeparator();
                    bufferedWriter.write(sqlDetail);
                    index++;
                }

                String tablesuffix = "</table>" + System.lineSeparator();
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
        String str = "<a class=\"wdr\" name=\"top\"></a>"
                + "<h2 class=\"wdr\">" + fileName + "</h2>"
                + "<h3 class=\"wdr\">SQL 兼容总览</h3>"
                + "<table class=\"tdiff\" summary=\"This table displays SQL Assessment Data\"><tr>"
                + System.lineSeparator() + "<th class=\"wdrbg\" scope=\"col\">总数</th>"
                + "<th class=\"wdrbg\" scope=\"col\">" + getCompatibilityString(COMPATIBLE) + "</th>"
                + "<th class=\"wdrbg\" scope=\"col\">" + getCompatibilityString(AST_COMPATIBLE) + "</th>"
                + "<th class=\"wdrbg\" scope=\"col\">" + getCompatibilityString(INCOMPATIBLE) + "</th>"
                + "<th class=\"wdrbg\" scope=\"col\">" + getCompatibilityString(UNSUPPORTED_COMPATIBLE) + "</th>"
                + "<th class=\"wdrbg\" scope=\"col\">" + getCompatibilityString(SKIP_COMMAND)
                + "</th>" + System.lineSeparator() + "</tr>" + System.lineSeparator();

        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(str);
                writeData(bufferedWriter);
            }
        } catch (IOException e) {
            return false;
        }
        return generateReport();
    }

    /**
     * write data.
     *
     * @param bufferedWriter : bufferedWriter
     * @throws IOException : throw IOException
     */
    private void writeData(BufferedWriter bufferedWriter) throws IOException {
        int compatible = 0;
        int astCompatible = 0;
        int incompatible = 0;
        int unsupportedCompatible = 0;
        int skipCommand = 0;
        for (SQLCompatibility sqlCompatibility : this.sqlCompatibilities) {
            switch (sqlCompatibility.getCompatibilityType()) {
                case COMPATIBLE:
                    compatible++;
                    break;
                case AST_COMPATIBLE:
                    astCompatible++;
                    break;
                case INCOMPATIBLE:
                    incompatible++;
                    break;
                case UNSUPPORTED_COMPATIBLE:
                    unsupportedCompatible++;
                    break;
                case SKIP_COMMAND:
                    skipCommand++;
                    break;
                default:
                    break;
            }
        }

        int total = this.sqlCompatibilities.size();
        AssessmentEntry.increTotalSql(total);
        String data = "<tr>"
                + "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + total + "</td>"
                + System.lineSeparator()
                + "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + compatible + "</td>"
                + System.lineSeparator()
                + "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + astCompatible + "</td>"
                + System.lineSeparator()
                + "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + incompatible + "</td>"
                + System.lineSeparator()
                + "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + unsupportedCompatible + "</td>"
                + System.lineSeparator()
                + "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + skipCommand + "</td>"
                + System.lineSeparator() + "</tr>" + System.lineSeparator()
                + "</table>" + System.lineSeparator();
        bufferedWriter.write(data);
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
        String str = "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                + System.lineSeparator()
                + "<head><title>openGauss 兼容性评估报告</title>" + System.lineSeparator() + "<style type=\"text/css\">"
                + System.lineSeparator()
                + "a.wdr {font:bold 8pt Arial,Helvetica,sans-serif;color:#663300;vertical-align:top;"
                + "margin-top:0pt; margin-bottom:0pt;}" + System.lineSeparator()
                + "table.tdiff {  border_collapse: collapse; } " + System.lineSeparator()
                + "body.wdr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black; background:White;}"
                + System.lineSeparator()
                + "h1.wdr {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;background-color:White;"
                + "border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}"
                + System.lineSeparator() + "h2.wdr {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;"
                + "background-color:White;margin-top:4pt; margin-bottom:0pt;}" + System.lineSeparator()
                + "h3.wdr {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;"
                + "background-color:White;margin-top:4pt; margin-bottom:0pt;}" + System.lineSeparator()
                + "li.wdr {font: 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}"
                + System.lineSeparator() + "th.wdrnobg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:black; "
                + "background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}" + System.lineSeparator()
                + "th.wdrbg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:White; "
                + "background:#8F170B;padding-left:4px; padding-right:4px;padding-bottom:2px}" + System.lineSeparator()
                + "td.wdrnc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black; white-space:pre-wrap;word-break;;"
                + "background:#F4F6F6; vertical-align:top;word-break: break-word; width: 4%;}" + System.lineSeparator()
                + "td.wdrc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black; white-space:pre-wrap;word-break;"
                + "background:White; vertical-align:top;word-break: break-word; width: 4%;}" + System.lineSeparator()
                + "td.wdrnc_err {font:8pt fangsong;color:black; white-space:pre-wrap;word-break;;"
                + "background:#F4F6F6; vertical-align:top;word-break: break-word; width: 20%;}" + System.lineSeparator()
                + "td.wdrc_err {font:8pt fangsong;color:black; white-space:pre-wrap;word-break;"
                + "background:White; vertical-align:top;word-break: break-word; width: 20%;}" + System.lineSeparator()
                + "td.wdrtext {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;"
                + "vertical-align:top;white-space:pre-wrap;word-break: break-word;}" + "</style>"
                + "<script type=\"text/javascript\">function msg(titlename, inputname, objname) {"
                + System.lineSeparator()
                + "if (document.getElementById(inputname).value == \"1\") {" + System.lineSeparator()
                + "    document.getElementById(objname).style.display=\"block\";" + System.lineSeparator()
                + "    document.getElementById(titlename).innerHTML = \"-\" + titlename;" + System.lineSeparator()
                + "    document.getElementById(inputname).value = \"0\";" + System.lineSeparator()
                + "} else {" + System.lineSeparator()
                + "    document.getElementById(objname).style.display=\"none\";" + System.lineSeparator()
                + "    document.getElementById(titlename).innerHTML = \"+\" + titlename;" + System.lineSeparator()
                + "    document.getElementById(inputname).value = \"1\";" + System.lineSeparator()
                + "}}</script>" + System.lineSeparator() + "</head><body class=\"wdr\">" + System.lineSeparator()
                + "<h1 class=\"wdr\">" + compat + " 兼容性评估报告</h1>" + System.lineSeparator();
        return canWriteData(str);
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
        String str = System.lineSeparator() + "</body></html>";

        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(str);
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}