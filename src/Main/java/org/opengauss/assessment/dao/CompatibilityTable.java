package org.opengauss.assessment.dao;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import static org.opengauss.assessment.dao.CompatibilityType.*;

public class CompatibilityTable {
    private File fd;
    private Queue<SQLCompatibility> sqlCompatibilities = new LinkedList<>();

    public CompatibilityTable() {
    }

    public void AppendOneSQL(int line, String sql, AssessmentType assessmentType, CompatibilityType compatibilityType, String errorResult) {
        this.sqlCompatibilities.add(new SQLCompatibility(line, sql, assessmentType, compatibilityType, errorResult));
    }

    public boolean GenerateReport() {
        String str = "<a class=\"wdr\" name=\"top\"></a><h2 class=\"wdr\">SQL 兼容详情</h2>" +
                "<table class=\"tdiff\" summary=\"This table displays SQL Assessment Data\" width=100%><tr>\n" +
                "<th class=\"wdrbg\" scope=\"col\">行号</th>" +
                "<th class=\"wdrbg\" scope=\"col\">SQL语句</th>" +
                "<th class=\"wdrbg\" scope=\"col\">兼容性</th>" +
                "<th class=\"wdrbg\" scope=\"col\">兼容性详情</th>" +
                "</tr>\n";

        FileWriter fileWriter;
        BufferedWriter bufferedWriter;

        try {
            fileWriter = new FileWriter(fd, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(str);
        } catch (IOException e) {
            return false;
        }

        long index = 0;
        while (!this.sqlCompatibilities.isEmpty()) {
            String type;
            if ((index & 1) == 0) {
                type = "wdrnc";
            } else {
                type = "wdrc";
            }

            SQLCompatibility sqlCompatibility = this.sqlCompatibilities.poll();
            String sqlDetail = "<tr style=\"border-top-width: 2px;\">" +
                    "<td style=\"width:3%\" class=\"" + type + "\">" + sqlCompatibility.getLine() + "\n</td>\n" +
                    "<td style=\"width:76%\" class=\"" + type + "\">" + sqlCompatibility.getSql() + "\n</td>\n" +
                    "<td class=\"" + type + "\" align=\"left\" >" + GetCompatibilityString(sqlCompatibility.getCompatibilityType()) + "</td>\n" +
                    "<td class=\"" + type + "_err\" align=\"left\" >" + sqlCompatibility.getErrDetail() + "</td>\n" +
                    "</tr>\n";

            try {
                bufferedWriter.write(sqlDetail);
            } catch (IOException e) {
                return false;
            }
        }

        String tablesuffix = "</table>\n";

        try {
            bufferedWriter.write(tablesuffix);
        } catch (IOException e) {
            return false;
        } finally {
            WriterClose(fileWriter, bufferedWriter);
        }

        return true;
    }

    public boolean GenerateSQLCompatibilityStatistic(String fileName) {
        String str = "<a class=\"wdr\" name=\"top\"></a>" +
                "<h2 class=\"wdr\">" + fileName + "</h2>" +
                "<h3 class=\"wdr\">SQL 兼容总览</h3>" +
                "<table class=\"tdiff\" summary=\"This table displays SQL Assessment Data\"><tr>\n" +
                "<th class=\"wdrbg\" scope=\"col\">总数</th>" +
                "<th class=\"wdrbg\" scope=\"col\">" + GetCompatibilityString(COMPATIBLE) + "</th>" +
                "<th class=\"wdrbg\" scope=\"col\">" + GetCompatibilityString(AST_COMPATIBLE) + "</th>" +
                "<th class=\"wdrbg\" scope=\"col\">" + GetCompatibilityString(INCOMPATIBLE) + "</th>" +
                "<th class=\"wdrbg\" scope=\"col\">" + GetCompatibilityString(UNSUPPORTED_COMPATIBLE) + "</th>" +
                "<th class=\"wdrbg\" scope=\"col\">" + GetCompatibilityString(SKIP_COMMAND) +
                "</th>\n</tr>\n";

        FileWriter fileWriter;
        BufferedWriter bufferedWriter;
        try {
            fileWriter = new FileWriter(fd, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(str);
        } catch (IOException e) {
            return false;
        }

        int total = this.sqlCompatibilities.size();
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

        String data = "<tr>" +
                "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + total + "</td>\n" +
                "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + compatible + "</td>\n" +
                "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + astCompatible + "</td>\n" +
                "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + incompatible + "</td>\n" +
                "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + unsupportedCompatible + "</td>\n" +
                "<td class=\"wdrnc\" style=\"text-align: center;font-size: 16px;\">" + skipCommand + "</td>\n" +
                "</tr>\n" +
                "</table>\n";

        try {
            bufferedWriter.write(data);
        } catch (IOException e) {
            return false;
        } finally {
            WriterClose(fileWriter, bufferedWriter);
        }

        return GenerateReport();
    }

    private void WriterClose(FileWriter fileWriter, BufferedWriter bufferedWriter) {
        try {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            if (fileWriter != null) {
                fileWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String GetCompatibilityString(CompatibilityType compatibilityType) {
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

    public boolean GenerateReportHeader(String output, String dbName) {
        this.fd = new File(output);
        if (fd == null) {
            return false;
        }

        String str = "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n" +
                "<head><title>openGauss 兼容性评估报告</title>\n" +
                "<style type=\"text/css\">\n" +
                "a.wdr {font:bold 8pt Arial,Helvetica,sans-serif;color:#663300;vertical-align:top;" +
                "margin-top:0pt; margin-bottom:0pt;}\n" +
                "table.tdiff {  border_collapse: collapse; } \n" +
                "body.wdr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black; background:White;}\n" +
                "h1.wdr {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;background-color:White;" +
                "border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}\n" +
                "h2.wdr {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;" +
                "background-color:White;margin-top:4pt; margin-bottom:0pt;}\n" +
                "h3.wdr {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;" +
                "background-color:White;margin-top:4pt; margin-bottom:0pt;}\n" +
                "li.wdr {font: 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}\n" +
                "th.wdrnobg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:black; " +
                "background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}\n" +
                "th.wdrbg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:White; " +
                "background:#8F170B;padding-left:4px; padding-right:4px;padding-bottom:2px}\n" +
                "td.wdrnc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black; white-space:pre-wrap;word-break;;" +
                "background:#F4F6F6; vertical-align:top;word-break: break-word; width: 4%;}\n" +
                "td.wdrc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black; white-space:pre-wrap;word-break;" +
                "background:White; vertical-align:top;word-break: break-word; width: 4%;}\n" +
                "td.wdrnc_err {font:8pt fangsong;color:black; white-space:pre-wrap;word-break;;" +
                "background:#F4F6F6; vertical-align:top;word-break: break-word; width: 20%;}\n" +
                "td.wdrc_err {font:8pt fangsong;color:black; white-space:pre-wrap;word-break;" +
                "background:White; vertical-align:top;word-break: break-word; width: 20%;}\n" +
                "td.wdrtext {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;" +
                "white-space:pre-wrap;word-break: break-word;}" +
                "</style>" +
                "<script type=\"text/javascript\">function msg(titlename, inputname, objname) {\n" +
                "if (document.getElementById(inputname).value == \"1\") {\n" +
                "    document.getElementById(objname).style.display=\"block\";\n" +
                "    document.getElementById(titlename).innerHTML = \"-\" + titlename;\n" +
                "    document.getElementById(inputname).value = \"0\";\n" +
                "} else {\n" +
                "    document.getElementById(objname).style.display=\"none\";\n" +
                "    document.getElementById(titlename).innerHTML = \"+\" + titlename;\n" +
                "    document.getElementById(inputname).value = \"1\";\n" +
                "}}</script>\n" +
                "</head><body class=\"wdr\"\n>" +
                "<h1 class=\"wdr\">" + dbName + " 兼容性评估报告</h1>\n";

        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {
            fileWriter = new FileWriter(fd);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(str);
        } catch (IOException e) {
            return false;
        } finally {
            WriterClose(fileWriter, bufferedWriter);
        }

        return true;
    }

    public boolean GenerateReportEnd() {
        String str = "\n</body></html>";
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {
            fileWriter = new FileWriter(fd, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(str);
        } catch (IOException e) {
            return false;
        } finally {
            WriterClose(fileWriter, bufferedWriter);
        }

        return true;
    }
}