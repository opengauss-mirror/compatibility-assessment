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

/**
 * Compatibility table.
 *
 * @author : yaoboxin
 * @since : 2024/3/4
 */
public class CompatibilityTableScript {
    private static final String LINESEP = System.lineSeparator();

    private String script = "</div></body><script>//饼形图" + LINESEP
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
            + "                var colorMap = {  " + LINESEP
            + "                    '完全兼容': '#04a0e8', // 蓝色  " + LINESEP
            + "                    '语法兼容': '#c1b9b9', // 灰色  " + LINESEP
            + "                    '不兼容': '#59530e',   // 棕色或其他颜色  " + LINESEP
            + "                    '暂不支持评估': '#2f7b98', // 蓝色调  " + LINESEP
            + "                    '忽略语句': '#28671d'  // 绿色  " + LINESEP
            + "                }; " + LINESEP
            + "                // 创建一个空数组来存储对应的颜色值  " + LINESEP
            + "                var colors = [];  " + LINESEP
            + "                // 遍历键数组  " + LINESEP
            + "                Object.keys(contentCount).forEach(function(key) {  " + LINESEP
            + "                    // 检查键是否存在于colorMap中  " + LINESEP
            + "                    if (colorMap.hasOwnProperty(key)) {  " + LINESEP
            + "                        // 如果存在，将对应的颜色值添加到colors数组中  " + LINESEP
            + "                        colors.push(colorMap[key]);  " + LINESEP
            + "                    } " + LINESEP
            + "                });  " + LINESEP
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
            + "                        backgroundColor: colors," + LINESEP
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
            + "                        fontSize: '23'," + LINESEP
            + "                    }," + LINESEP
            + "                    " + LINESEP
            + "                    onClick: function (event, elements) {" + LINESEP
            + "                        if (elements.length > 0) {" + LINESEP
            + " var label = elements[0]._chart.config.data.labels[elements[0]._index];" + LINESEP
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
            + "                                    rows[i].style.display = 'flex'; // 显示匹配的行  " + LINESEP
            + "                               }" + LINESEP
            + "                            }" + LINESEP
            + "" + LINESEP
            + "                    }" + LINESEP
            + "                }," + LINESEP
            + "                    tooltips: {" + LINESEP
            + "                        bodyFontSize: 15, // 设置工具提示字体大小为 10px   " + LINESEP
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
            + "                            fontSize: 15 // 设置图例标签字体大小为 10px  " + LINESEP
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
            + "                var colorMap = {  " + LINESEP
            + "                    '完全兼容': '#04a0e8', // 蓝色  " + LINESEP
            + "                    '语法兼容': '#c1b9b9', // 灰色  " + LINESEP
            + "                    '不兼容': '#59530e',   // 棕色或其他颜色  " + LINESEP
            + "                    '暂不支持评估': '#2f7b98', // 蓝色调  " + LINESEP
            + "                    '忽略语句': '#28671d'  // 绿色  " + LINESEP
            + "                }; " + LINESEP
            + "                // 创建一个空数组来存储对应的颜色值  " + LINESEP
            + "                var colors = [];  " + LINESEP
            + "                // 遍历键数组  " + LINESEP
            + "                Object.keys(contentCount).forEach(function(key) {  " + LINESEP
            + "                    // 检查键是否存在于colorMap中  " + LINESEP
            + "                    if (colorMap.hasOwnProperty(key)) {  " + LINESEP
            + "                        // 如果存在，将对应的颜色值添加到colors数组中  " + LINESEP
            + "                        colors.push(colorMap[key]);  " + LINESEP
            + "                    } " + LINESEP
            + "                });  " + LINESEP
            + "var data = {" + LINESEP
            + "                    id:i,//添加一个唯一标识符" + LINESEP
            + "                    labels: Object.keys(contentCount)," + LINESEP
            + "                    //[完全兼容,语法兼容,不兼容,暂不支持评估,忽略语句]," + LINESEP
            + "                    // ['兼容', '不兼容', '其他'],  " + LINESEP
            + "                    datasets: [{" + LINESEP
            + "                        label:'兼容情况'," + LINESEP
            + "                        data: Object.values(contentCount)," + LINESEP
            + "                        // [3, 0, 0],  " + LINESEP
            + "                       " + LINESEP
            + "                        backgroundColor: colors," + LINESEP
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
            + "                            barThickness: 20," + LINESEP
            + "                            ticks: {fontSize: 15 // 设置x轴字体大小" + LINESEP
            + "               }," + LINESEP
            + "                        }]," + LINESEP
            + "                        yAxes: [{  " + LINESEP
            + "                            ticks: {  " + LINESEP
            + "                                beginAtZero: true,  " + LINESEP
            + "                                min: 0  " + LINESEP
            + " stepSize: Math.max(Object.values(contentCount)) >= 10 ? Math.ceil(Math.max(Object.values(contentCount))/10):1,"
            +"  }  " + LINESEP
            + "                        }]  " + LINESEP
            + "                    },  " + LINESEP
            + "                    " + LINESEP
            + "                    onClick: function (event, elements) {" + LINESEP
            + "                        if (elements.length > 0) {" + LINESEP
            + "var label = elements[0]._chart.config.data.labels[elements[0]._index];" + LINESEP
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
            + "                                    rows[i].style.display = 'flex'; // 显示匹配的行  " + LINESEP
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
            + "                        bodyFontSize: 15, // 设置工具提示字体大小为 20px   " + LINESEP
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
            + "                    legend: {display: false, // 不显示条形图的图例 " + LINESEP
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

    public String getScript() {
        return script;
    }
}
