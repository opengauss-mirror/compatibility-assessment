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
    private static final String LINESEP = System.lineSeparator();

    private File fd;

    private Queue<SQLCompatibility> sqlCompatibilities = new LinkedList<>();

    private int contentTableCount = 0;

    private int chartsCount = 0;

    private String str = "</body><script>//饼形图" + LINESEP
        + "    " + LINESEP
        + "        " + LINESEP
        + "    (function() {  " + LINESEP
        + "        'use strict';  " + LINESEP
        + "" + LINESEP
        + "        var h2Elements = document.querySelectorAll('h2'); " + LINESEP
        + "        " + LINESEP
        + "        for (let i = 0; i < h2Elements.length; i++) {" + LINESEP
        + "            " + LINESEP
        + "            var contDiv = h2Elements[i].nextElementSibling;" + LINESEP
        + "            var elementsType = contDiv.querySelectorAll('.category-container');" + LINESEP
        + "            " + LINESEP
        + "            // 假设你有一个函数来创建和配置图表 " + LINESEP
        + "            function createDoughnutChart(canvasCtx) {" + LINESEP
        + "                // 创建一个对象来存储每个内容及其出现的次数  " + LINESEP
        + "                var contentCount = {};" + LINESEP
        + "                " + LINESEP
        + "                // 遍历元素数组  " + LINESEP
        + "                elementsType.forEach(function (element) {" + LINESEP
        + "                    var content = element.innerText; // 获取元素的文本内容  " + LINESEP
        + "                    // 检查contentCount对象中是否已经存在当前内容  " + LINESEP
        + "                    if (content in contentCount) {" + LINESEP
        + "                        contentCount[content]++; // 如果存在，增加计数" + LINESEP
        + "                    }" + LINESEP
        + "                    else {" + LINESEP
        + "                        contentCount[content] = 1; // 如果不存在，初始化计数为1  " + LINESEP
        + "                    }" + LINESEP
        + "                });" + LINESEP
        + "                var type = 'doughnut'; // pie, doughnut  ,line,bar" + LINESEP
        + "                var data = {" + LINESEP
        + "                    id:i,//添加一个唯一标识符" + LINESEP
        + "                    labels: " + LINESEP
        + "                    Object.keys(contentCount)," + LINESEP
        + "                    //[2完全兼容,语法兼容,不兼容,1暂不支持评估,忽略语句]" + LINESEP
        + "                    // ['兼容', '不兼容', '其他'],  " + LINESEP
        + "                    datasets: [{" + LINESEP
        + "                        " + LINESEP
        + "                        data: Object.values(contentCount)," + LINESEP
        + "                        // [3, 0, 0],  " + LINESEP
        + "                       " + LINESEP
        + "                        backgroundColor: ['#04a0e8', '#c1b9b9', '#59530e', '#2f7b98', '#28671d']," + LINESEP
        + "                    }]" + LINESEP
        + "                };" + LINESEP
        + "                " + LINESEP
        + "                var options = {" + LINESEP
        + "                    cutoutPercentage:65,   //用于控制饼形图的粗细，数字越大，环越细" + LINESEP
        + "                    maintainAspectRatio: true ,//此选项用于控制图表的宽高比是否应该与画布的大小相匹配。" + LINESEP
        + "                    responsive: false  ,//此选项用于控制图表是否应该根据容器的大小自动调整大小。" + LINESEP
        + "                    title: {" + LINESEP
        + "                        display: true," + LINESEP
        + "                        text: '兼容总览'," + LINESEP
        + "                        fontColor: 'black'," + LINESEP
        + "                        fontSize: '20'," + LINESEP
        + "                    }," + LINESEP
        + "                    " + LINESEP
        + "                    onClick: function (event, elements) {" + LINESEP
        + "                        if (elements.length > 0) {" + LINESEP
        + "                            var label = elements[0]._chart.config.data.labels[elements[0]._index];" + LINESEP
        + "                            " + LINESEP
        + "                            // 查找并显示所有具有匹配data-label的<tr>元素  " + LINESEP
        + " var table = document.querySelector('.content-table'+elements[0]._chart.config.data.id);" + LINESEP
        + "                            var rows = table.getElementsByClassName('content-row');" + LINESEP
        + "                            " + LINESEP
        + "                            " + LINESEP
        + "                            var displayedContent = '';" + LINESEP
        + "                            //遍历所有行，并隐藏它们  " + LINESEP
        + "                            for (var i = 0; i < rows.length; i++) {" + LINESEP
        + "                                rows[i].style.display = 'none';" + LINESEP
        + "                            }" + LINESEP
        + "" + LINESEP
        + "                            for (var i = 0; i < rows.length; i++) {" + LINESEP
        + "                                if (rows[i].getAttribute('data-label') === label) {" + LINESEP
        + "                                    rows[i].style.display = 'block'; // 显示匹配的行  " + LINESEP
        + "                               }" + LINESEP
        + "                            }" + LINESEP
        + "" + LINESEP
        + "                    }" + LINESEP
        + "                }," + LINESEP
        + "                    tooltips: {" + LINESEP
        + "                        bodyFontSize: 12, // 设置工具提示字体大小为 10px   " + LINESEP
        + "                        callbacks: {       //百分比显示" + LINESEP
        + "                            label: function (tooltipItem, data) {" + LINESEP
        + "                                var label = data.labels[tooltipItem.index];" + LINESEP
        + "                                var value = data.datasets[0].data[tooltipItem.index];" + LINESEP
        + "var percentage = (value / data.datasets[0].data.reduce("
        + "function (sum, val) { return sum + val; }, 0) * 100).toFixed(2);" + LINESEP
        + "                                return label + ': ' + value + ' (' + percentage + '%)';" + LINESEP
        + "                            }" + LINESEP
        + "                        }" + LINESEP
        + "                    }," + LINESEP
        + "                    legend: {" + LINESEP
        + "                        labels: {" + LINESEP
        + "                            fontSize: 12 // 设置图例标签字体大小为 10px  " + LINESEP
        + "                        }" + LINESEP
        + "                    }" + LINESEP
        + "                };" + LINESEP
        + "                // 使用当前上下文创建图表  " + LINESEP
        + "                return new Chart(canvasCtx, {" + LINESEP
        + "                    type: type," + LINESEP
        + "                    data: data," + LINESEP
        + "                    options: options" + LINESEP
        + "                });" + LINESEP
        + "                " + LINESEP
        + "            }" + LINESEP
        + "            " + LINESEP
        + "            function createBarChart(canvasCtx) {" + LINESEP
        + "                // 创建一个对象来存储每个内容及其出现的次数  " + LINESEP
        + "                var contentCount = {};" + LINESEP
        + "                " + LINESEP
        + "                // 遍历元素数组  " + LINESEP
        + "                elementsType.forEach(function (element) {" + LINESEP
        + "                    var content = element.innerText; // 获取元素的文本内容  " + LINESEP
        + "                    // 检查contentCount对象中是否已经存在当前内容  " + LINESEP
        + "                    if (content in contentCount) {" + LINESEP
        + "                        contentCount[content]++; // 如果存在，增加计数" + LINESEP
        + "                    }" + LINESEP
        + "                    else {" + LINESEP
        + "                        contentCount[content] = 1; // 如果不存在，初始化计数为1  " + LINESEP
        + "                    }" + LINESEP
        + "                });" + LINESEP
        + "                var type = 'bar'; // pie, doughnut  ,line,bar,horizontalBar" + LINESEP
        + "                var data = {" + LINESEP
        + "                    id:i,//添加一个唯一标识符" + LINESEP
        + "                    labels: Object.keys(contentCount)," + LINESEP
        + "                    //[完全兼容,语法兼容,不兼容,暂不支持评估,忽略语句]," + LINESEP
        + "                    // ['兼容', '不兼容', '其他'],  " + LINESEP
        + "                    datasets: [{" + LINESEP
        + "                        label:'兼容情况'," + LINESEP
        + "                        data: Object.values(contentCount)," + LINESEP
        + "                        // [3, 0, 0],  " + LINESEP
        + "                       " + LINESEP
        + "                        backgroundColor: ['#04a0e8', '#c1b9b9', '#59530e', '#2f7b98', '#28671d']," + LINESEP
        + "                       " + LINESEP
        + "                        borderWidth: 1" + LINESEP
        + "                    }]" + LINESEP
        + "                };" + LINESEP
        + "                " + LINESEP
        + "                var options = {" + LINESEP
        + "                    maintainAspectRatio: true ,//此选项用于控制图表的宽高比是否应该与画布的大小相匹配。" + LINESEP
        + "                    responsive: false  ,//此选项用于控制图表是否应该根据容器的大小自动调整大小。" + LINESEP
        + "                    title: {" + LINESEP
        + "                        display: true," + LINESEP
        + "                    }," + LINESEP
        + "                    scales: {  " + LINESEP
        + "                        xAxes:[{" + LINESEP
        + "                            barThickness: 15," + LINESEP
        + "                        }]," + LINESEP
        + "                        yAxes: [{  " + LINESEP
        + "                            ticks: {  " + LINESEP
        + "                                beginAtZero: true,  " + LINESEP
        + "                                min: 0  " + LINESEP
        + "                            }  " + LINESEP
        + "                        }]  " + LINESEP
        + "                    },  " + LINESEP
        + "                    " + LINESEP
        + "                    onClick: function (event, elements) {" + LINESEP
        + "                        if (elements.length > 0) {" + LINESEP
        + "                            var label = elements[0]._chart.config.data.labels[elements[0]._index];" + LINESEP
        + "                            " + LINESEP
        + "                            // 查找并显示所有具有匹配data-label的<tr>元素  " + LINESEP
        + "var table = document.querySelector('.content-table'+elements[0]._chart.config.data.id);" + LINESEP
        + "                            var rows = table.getElementsByClassName('content-row');" + LINESEP
        + "                            var displayedContent = '';" + LINESEP
        + "                            //遍历所有行，并隐藏它们  " + LINESEP
        + "                            for (var i = 0; i < rows.length; i++) {" + LINESEP
        + "                                rows[i].style.display = 'none';" + LINESEP
        + "                            }" + LINESEP
        + "" + LINESEP
        + "                            for (var i = 0; i < rows.length; i++) {" + LINESEP
        + "                                if (rows[i].getAttribute('data-label') === label) {" + LINESEP
        + "                                    rows[i].style.display = 'block'; // 显示匹配的行  " + LINESEP
        + "" + LINESEP
        + "                                    // 获取<tr>中的每个<td>内容，并追加到displayedContent  " + LINESEP
        + "                                    var cells = rows[i].getElementsByTagName('td');" + LINESEP
        + "                                    for (var j = 0; j < cells.length; j++) {" + LINESEP
        + " displayedContent += cells[j].outerHTML; // 使用outerHTML保留原格式  " + LINESEP
        + "                                    }" + LINESEP
        + "" + LINESEP
        + "                                    // 在每一行内容之间添加换行符  " + LINESEP
        + "                                    displayedContent += '<br>';" + LINESEP
        + "                               }" + LINESEP
        + "                            }" + LINESEP
        + "" + LINESEP
        + "                        }" + LINESEP
        + "                    }," + LINESEP
        + "                    tooltips: {" + LINESEP
        + "                        bodyFontSize: 12, // 设置工具提示字体大小为 10px   " + LINESEP
        + "                        callbacks: {       //百分比显示" + LINESEP
        + "                            label: function (tooltipItem, data) {" + LINESEP
        + "                                var label = data.labels[tooltipItem.index];" + LINESEP
        + "                                var value = data.datasets[0].data[tooltipItem.index];" + LINESEP
        + "var percentage = (value / data.datasets[0].data.reduce("
        + "function (sum, val) { return sum + val; }, 0) * 100).toFixed(2);" + LINESEP
        + "                                return label + ': ' + value + ' (' + percentage + '%)';" + LINESEP
        + "                            }" + LINESEP
        + "                        }" + LINESEP
        + "                    }," + LINESEP
        + "                    legend: {" + LINESEP
        + "                        labels: {" + LINESEP
        + "                            fontSize: 12 // 设置图例标签字体大小为 10px  " + LINESEP
        + "                        }" + LINESEP
        + "                    }" + LINESEP
        + "                };" + LINESEP
        + "                // 使用当前上下文创建图表  " + LINESEP
        + "                return new Chart(canvasCtx, {" + LINESEP
        + "                    type: type," + LINESEP
        + "                    data: data," + LINESEP
        + "                    options: options" + LINESEP
        + "                });" + LINESEP
        + "                " + LINESEP
        + "            }" + LINESEP
        + "            " + LINESEP
        + "            var canvasElements = document.getElementById('myCharts'+i);" + LINESEP
        + "            var canvasElements1 = document.getElementById('myBarCharts'+i);" + LINESEP
        + "            " + LINESEP
        + "            if(canvasElements){" + LINESEP
        + "                var ctx = canvasElements.getContext('2d');" + LINESEP
        + "                createDoughnutChart(ctx);" + LINESEP
        + "            }" + LINESEP
        + "            if(canvasElements1){" + LINESEP
        + "                var ctx1 = canvasElements1.getContext('2d');" + LINESEP
        + "                createBarChart(ctx1);" + LINESEP
        + "            }" + LINESEP
        + "                " + LINESEP
        + "            " + LINESEP
        + "        }  " + LINESEP
        + "    })();" + LINESEP
        + "</script>"
        + "<script>    //页面折叠功能" + LINESEP
        + "    const collapse = document.querySelector('#collapse');" + LINESEP
        + "    let h2s = document.querySelectorAll('h2');" + LINESEP
        + "    let divs = collapse.querySelectorAll('div');" + LINESEP
        + "    for (let i = 0; i < h2s.length; i++) {" + LINESEP
        + "            h2s[i].addEventListener('click', function () {" + LINESEP
        + "                let contDiv = this.nextElementSibling;" + LINESEP
        + "                if (contDiv.style.display == 'block') {       " + LINESEP
        + "                     contDiv.style.display = 'none';" + LINESEP
        + "                    // this.className = '';" + LINESEP
        + "                } else {" + LINESEP
        + "                    contDiv.style.display = 'block';" + LINESEP
        + "                    // this.className = 'active';" + LINESEP
        + "                }" + LINESEP
        + "            })" + LINESEP
        + "        }    " + LINESEP
        + "</script></html>";

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
        String strHeader = "<tr><td colspan=\"2\"><h3 class=\"wdr\"> SQL 兼容详情 </h3>"
            + "<table class=\"tdiff\" summary=\"This table displays SQL Assessment Data\" width=100% >"
            + "<tr><th class=\"wdrbg\" style=\"width:2%\"> 行号 </th><th class=\"wdrbg\" style=\"width:59%\"> SQL语句 "
            + "</th><th class=\"wdrbg\" style=\"width:4%\"> 兼容性 </th>"
            + "<th class=\"wdrbg\" style=\"width:20%\"> 兼容性详情 </th>";

        if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
            strHeader += "<th class=\"wdrbg\" style=\"width:15%\"> 初始位置 </th>";
        }
        strHeader += "</tr>" + System.lineSeparator();

        try (FileWriter fileWriter = new FileWriter(fd, true)) {
            try (BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
                bufferedWriter.write(strHeader);
                long index = 0L;
                while (!this.sqlCompatibilities.isEmpty()) {
                    String type;
                    if ((index & 1) == 0) {
                        type = "wdrnc";
                    } else {
                        type = "wdrc";
                    }

                    SQLCompatibility sqlCompatibility = this.sqlCompatibilities.poll();
                    String sqlDetail = "<tr><table class=\"content-table" + contentTableCount + "\" width=100%>"
                        + "<tr class=\"content-row\"  data-label=\""
                        + getCompatibilityString(sqlCompatibility.getCompatibilityType()) + "\">"
                        + "<td style=\"width:2%; \" class=\"\" align=\"center\">" + sqlCompatibility.getLine()
                        + "</td>"
                        + "<td style=\"width:59%; \" class=\"\" align=\"center\">" + sqlCompatibility.getSql()
                        + "</td><td style=\"width:4%; \" class=\"category-container\" align=center>"
                        + getCompatibilityString(sqlCompatibility.getCompatibilityType()) + "</td>"
                        + "<td style=\"width:20%; \" class=\"\" align=\"center\" >"
                        + sqlCompatibility.getErrDetail() + "</td>";
                    contentTableCount++ ;
                    if (Commander.getDataSource().equalsIgnoreCase(Commander.DATAFROM_FILE)) {
                        sqlDetail += "<td style=\"width:15%; \" align=\"center\">" + sqlCompatibility.getId();
                    }
                    sqlDetail += "</td>" + System.lineSeparator() + "</tr>" + System.lineSeparator();
                    bufferedWriter.write(sqlDetail);
                    index++;
                }

                String tablesuffix = "</table></tr></table></td></tr></table></div></div>"
                    + System.lineSeparator();
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
            + "<div style=\"display: none;\"><table class=\"tdiff\" style=\"width: 100%;\">"
            + "<tr><!-- 图表区 （上侧）--><td style=\"width: 400px;\" >"
            + "<canvas id=\"myCharts" + chartsCount + "\" style=\"width: 400px;\">环形图表区</canvas>"
            + "</td><td style=\"width: 100%;\" ><canvas id=\"myBarCharts"
            + chartsCount + "\" style=\"width: 2000px;height: 250px;\">条形图表区</canvas>"
            + "</td></tr>" + System.lineSeparator();
        chartsCount++ ;
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
                    compatible++ ;
                    break;
                case AST_COMPATIBLE:
                    astCompatible++ ;
                    break;
                case INCOMPATIBLE:
                    incompatible++ ;
                    break;
                case UNSUPPORTED_COMPATIBLE:
                    unsupportedCompatible++ ;
                    break;
                case SKIP_COMMAND:
                    skipCommand++ ;
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
        String strCss = "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
            + System.lineSeparator()
            + "<head><title>openGauss 兼容性评估报告</title>" + System.lineSeparator() + "<style type=\"text/css\">"
            + "body.wdr {font:8pt Arial, Helvetica, Geneva, sans-serif;color:black;background:White;}"
            + "h1.wdr {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#000000;"
            + "background-color:White;border-bottom:1px solid #cccc99;margin-top:0pt; "
            + "margin-bottom:0pt;padding:0px 0px 0px 0px;}"
            + "h2.wdr {display: block;font:bold 18pt Arial,Helvetica,Geneva,sans-serif;"
            + "color:#000000;background-color:#c1b9b9;margin-top:4pt;margin-bottom:0pt;}"
            + "h3.wdr {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#000000;"
            + "background-color:White;margin-top:4pt;margin-bottom:0pt;}"
            + "th.wdrbg {color:White;background:#04a0e8;padding-left:4px;padding-right:4px;"
            + "padding-bottom:2px}"
            + "canvas {background: #ffffff;}"
            + ".content-row td{background: #f3f3f3;}"
            + "</style>"
            + "<script src=\"js/Chart.bundle.min.js\"></script>"
            + "</head><body class=\"wdr\">" + System.lineSeparator()
            + "<div id=\"collapse\">"
            + "<h1 class=\"wdr\">" + compat + " 兼容性评估报告</h1>" + System.lineSeparator();
        return canWriteData(strCss);
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
                bufferedWriter.write(str);
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}