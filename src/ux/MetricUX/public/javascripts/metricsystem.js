/// <reference path="..\..\typings\jquery\jquery.d.ts" />
/// <reference path="..\..\typings\jquery.gridster\gridster.d.ts" />
/// <reference path="..\..\typings\select2\select2.d.ts" />
/// <reference path="..\..\typings\highcharts\highstock.d.ts" />
/// <reference path="..\..\typings\bootstrap.v3.datetimepicker\bootstrap.v3.datetimepicker.d.ts" />
/// <reference path="..\..\typings\jquery.dataTables/jquery.dataTables.d.ts" />
var defaultMachineName = "127.0.0.1";
var baseUri = "/data";
var currentMachineName = "";
var currentCounterName = "";
var wires = {};
var gridster = null;
var graphToSeriesMap = {};
var seriesToMetadataMap = {};
var chartData = {}; // map from seriesId to data
function queryData(machineName, environmentName, timeoutValue, pivotDimension, counterName, limit, params, startTime, endTime, width, height, top, left, seriesId, graphId) {
    if (seriesId === "")
        seriesId = generateUuid();
    if (pivotDimension !== "") {
        params["dimension"] = pivotDimension;
    }
    var queryParams = decodeURIComponent(params) + "&" + getStartEndTimes(startTime, endTime);
    var gridData = [];
    var perDimensionData = {};
    var seriesMachines = machineName;
    if (machineName === "")
        seriesMachines = environmentName;
    var seriesDescription = counterName + " for " + seriesMachines.substr(0, 28);
    if (seriesMachines.length > 28) {
        seriesDescription += "... (" + seriesMachines.split(",").length + " machines)";
    }
    if (pivotDimension !== "") {
        seriesDescription += " (split by " + pivotDimension + ")";
    }
    if (decodeURIComponent(params) !== "") {
        seriesDescription += " [" + decodeURIComponent(params) + "]";
    }
    seriesDescription += " from " + new Date(startTime).toLocaleTimeString() + " to " + new Date(endTime).toLocaleTimeString();
    var queryPayload = { machineName: machineName, environmentName: environmentName, counterName: counterName, queryCommand: "query", queryParameters: queryParams, timeoutValue: timeoutValue };
    $.ajax({
        url: baseUri + "/query",
        type: "POST",
        data: queryPayload,
        success: (function (values) {
            var series = [];
            var seriesPrefix = "";
            if (graphToSeriesMap[graphId] !== undefined && graphToSeriesMap[graphId].length > 0) {
                seriesPrefix += "series" + graphToSeriesMap[graphId].length + " - ";
            }
            else {
                seriesPrefix += "series0 - ";
            }
            // Build data sets
            if (pivotDimension !== "") {
                values.forEach(function (value) {
                    if (!perDimensionData[value.DimensionVal]) {
                        perDimensionData[seriesPrefix + value.DimensionVal] = [];
                    }
                    perDimensionData[seriesPrefix + value.DimensionVal].push([new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue]);
                });
                series = Object.keys(perDimensionData).map(seriesFromMachine);
            }
            else {
                series = [seriesPrefix + counterName].map(seriesFromMachine);
            }
            var grid = $("<table>").addClass("ms-grid");
            var chartDiv;
            var thisChart;
            var data;
            // Add to existing chart
            if (graphToSeriesMap[graphId] !== undefined && graphToSeriesMap[graphId].length > 0) {
                chartDiv = $("#" + graphId + "_chart");
                thisChart = chartDiv.highcharts();
                data = values.map(function (value) { return [new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue]; });
                graphToSeriesMap[graphId].map(function (val) {
                    series.push(seriesToMetadataMap[val]);
                });
                chartDiv.highcharts(getHighchartsConfig(null, series, {}));
                thisChart = chartDiv.highcharts();
                highchartsResizeHack(chartDiv);
                thisChart.get(seriesFromMachine(seriesPrefix + counterName).id).setData(data, false);
                graphToSeriesMap[graphId].map(function (val) {
                    thisChart.get(seriesToMetadataMap[val].id).setData(chartData[val], false);
                });
                values.forEach(function (value) {
                    gridData.push([new Date(parseInt(value.EndTime.substr(6))).toLocaleString(), value.ChartValue, value.MachineCount]);
                });
                thisChart.redraw();
                chartData[seriesId] = data;
                graphToSeriesMap[graphId].push(seriesId);
                seriesToMetadataMap[seriesId] = seriesFromMachine(seriesPrefix + counterName);
            }
            else {
                graphToSeriesMap[graphId] = [];
                graphToSeriesMap[graphId].push(seriesId);
                seriesToMetadataMap[seriesId] = seriesFromMachine(seriesPrefix + counterName);
                chartDiv = $("<div>").addClass("chartContainer").attr("id", graphId + "_chart");
                var maxDescWidth = 84 * width;
                var seriesWrap = $("<li>").attr("id", graphId).addClass("seriesWrap").append($("<div>").addClass("seriesTitle").attr("id", graphId + "_title").attr("title", seriesDescription).text(seriesDescription.substr(0, maxDescWidth) + "..."), chartDiv.highcharts(getHighchartsConfig(null, series, {})), $("<div>").addClass("table-wrapper").attr("id", graphId + "_table").hide());
                if (top > 0 && left > 0) {
                    gridster.add_widget(seriesWrap[0], width, height, top, left);
                }
                else {
                    gridster.add_widget(seriesWrap[0], width, height);
                }
                thisChart = chartDiv.highcharts();
                highchartsResizeHack(chartDiv);
                if (pivotDimension !== "") {
                    Object.keys(perDimensionData).map(function (dimension) {
                        thisChart.get(seriesFromMachine(dimension).id).setData(perDimensionData[dimension], false);
                    });
                    chartData[seriesId] = perDimensionData;
                    values.forEach(function (value) {
                        gridData.push([new Date(parseInt(value.EndTime.substr(6))).toLocaleString(), value.ChartValue, value.MachineCount]);
                    });
                }
                else {
                    data = values.map(function (value) { return [new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue]; });
                    chartData[seriesId] = data;
                    //renderChart(data);
                    thisChart.get(seriesFromMachine(seriesPrefix + counterName).id).setData(data, false);
                    values.forEach(function (value) {
                        gridData.push([new Date(parseInt(value.EndTime.substr(6))).toLocaleString(), value.ChartValue, value.MachineCount]);
                    });
                }
            }
            $("#" + graphId + "_table").append($("<h4>").text(seriesDescription), grid);
            grid.DataTable({
                paging: false,
                scrollY: "300px",
                "data": gridData,
                "columns": [
                    { "title": "EndTime" },
                    { "title": "Value" },
                    { "title": "MachineCount" }
                ]
            });
            thisChart.redraw();
            var newCounter = {
                graphId: graphId,
                counter: counterName,
                machines: machineName,
                environmentName: environmentName,
                startTime: startTime,
                endTime: endTime,
                dimensions: replaceAll($.param(params), "&", "%26"),
                pivotDimension: pivotDimension,
                top: 1,
                left: 1,
                width: 1,
                height: 1
            };
            wires[seriesId] = newCounter;
            if (graphToSeriesMap[graphId].length == 1) {
                $("#" + graphId + "_title").append($("<i class='fa fa-times-circle'></i>").addClass("toggle").click(function () {
                    for (var val in graphToSeriesMap[graphId]) {
                        delete wires[val];
                        delete seriesToMetadataMap[val];
                    }
                    gridster.remove_widget($("#" + graphId)[0]);
                    delete graphToSeriesMap[graphId];
                    updateWires();
                }), $("<i class='fa fa-table'></i>").addClass("toggle").click(function (e) {
                    if ($("#" + graphId + "_chart").is(":hidden")) {
                        $("#" + graphId + "_table").hide();
                        $("#" + graphId + "_chart").show();
                        $(e.target).removeClass("fa-line-chart");
                        $(e.target).addClass("fa-table");
                    }
                    else {
                        $("#" + graphId + "_chart").hide();
                        $("#" + graphId + "_table").show();
                        $("th").each(function () {
                            if (this.textContent == "EndTime") {
                                this.click();
                                this.click();
                            }
                        });
                        $(e.target).removeClass("fa-table");
                        $(e.target).addClass("fa-line-chart");
                    }
                }), $("<i class='fa fa-bookmark'></i>").addClass("toggle").click(function () {
                    alert(document.URL.split("?")[0] + "?" + "wires={\"counters\":[" + JSON.stringify(wires[seriesId]) + "]}");
                }), $("<i class='fa fa-info-circle'></i>").addClass("toggle").click(function () {
                    alert(seriesDescription);
                }));
            }
            updateWires();
            refreshPath();
        })
    });
    return;
}
// Utility functions
var seriesFromMachine = function (name) {
    return {
        id: "metric-" + name,
        name: name,
        type: "line"
    };
};
function refreshMachinesList() {
    var machines = [];
    var machineList = $("#machineName");
    machineList.html("");
    if ($("#EnvironmentList").val() === "localhost") {
        machines.push(defaultMachineName);
        machineList.append($("<option class='machineNameOption' />").val(defaultMachineName).text(defaultMachineName));
        $("#machineName").hide();
    }
    else if ($("#EnvironmentList").val() === undefined || $("#EnvironmentList").val() === null) {
        machines.push(defaultMachineName);
    }
    else {
        $.get(baseUri + "/machines?environment=" + $("#EnvironmentList").val(), function (result) {
            for (var machine in result) {
                machines.push(machine);
            }
            machines.sort();
            machines.map(function (m) {
                machineList.append($("<option class='machineNameOption' />").val(m.Hostname).text(m.Hostname));
            });
            if ($("#queryEnvironment").is(":checked")) {
                $("#machineName").hide();
            }
        });
    }
    $(".machineNameOption").first().attr("checked", "checked");
}
function getMachineName() {
    var result;
    if ($("#queryEnvironment").is(":checked")) {
        result = "";
    }
    else if ($("#machineList").is(":checked")) {
        result = $("#machineName").val();
        if (result == null)
            return defaultMachineName;
        if (result.toString().indexOf("Unable") !== -1)
            return defaultMachineName;
        result = $("#machineName").val().join();
    }
    else {
        result = $("#manualMachineName").val();
    }
    return result;
}
function getStartEndTimes(startTime, endTime) {
    if (startTime !== "" && endTime !== "") {
        return "start=" + new Date(startTime).toISOString() + "&end=" + new Date(endTime).toISOString();
    }
    return "start=1 hour ago&end=now";
}
function refreshEnvironments() {
    $.get(baseUri + "/environments").done(function (data) {
        $("#EnvironmentList").select2({ data: data });
        updateCounters();
    });
}
function updateCounters() {
    if ($("#queryEnvironment").is(":checked")) {
        $("#machineName").hide();
        $("#manualMachineName").hide();
    }
    else if ($("#machineList").is(":checked")) {
        $("#machineName").show();
        $("#manualMachineName").hide();
    }
    else {
        $("#machineName").hide();
        $("#manualMachineName").show();
    }
    refreshCounters();
}
function refreshCounters() {
    var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: "/*", queryCommand: "list", queryParameters: "", timeoutValue: getTimeoutValue() };
    $.post(baseUri + "/info", queryPayload).done(function (data) {
        $("#counters").select2({ data: data });
        updateDimensions(data[0]);
    });
}
function updateDimensions(counterName) {
    var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: counterName, queryCommand: "listDimensions", queryParameters: "", timeoutValue: getTimeoutValue() };
    $("#splitBy").empty();
    $("#splitBy").append($("<option>").text("none"));
    $.post(baseUri + "/info", queryPayload).done(function (dimensions) {
        var dimensionList = $("<div>").addClass("dimensionGrid");
        dimensionList.addClass("dimensionValues");
        dimensions.forEach(function (dimension) {
            $("#dimensionGrid").empty();
            var dimensionLabel = ($("<label>").text(dimension).attr("for", "dim_" + dimension).addClass("dimValLabel"));
            var dimensionSelector = $("<li>").append($("<select>").attr("name", dimension).attr("id", dimension).addClass("dimension"));
            dimensionList.append($("<li>").append(dimensionLabel, $("<i>").addClass("fa fa-plus-circle").click(function (e) {
                updateDimensionValues(counterName, dimension, dimensionSelector, e);
            })), dimensionSelector);
            dimensionSelector.hide();
            $("#splitBy").append($("<option>").text(dimension));
        });
        $("#splitBy").select2({
            minimumResultsForSearch: 10
        });
        $("#dimensionGrid").append(dimensionList);
    });
}
function updateDimensionValues(counterName, dimensionName, dimensionSelector, e) {
    var toggleButton = $(e.target);
    if (toggleButton.hasClass("fa-plus-circle")) {
        var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: counterName, queryCommand: "listDimensionValues", queryParameters: "dimension=" + dimensionName, timeoutValue: getTimeoutValue() };
        $.post(baseUri + "/info", queryPayload).done(function (data) {
            $("#" + dimensionName).select2({ data: data });
            dimensionSelector.show();
            $(e.target).removeClass("fa-plus-circle");
            $(e.target).addClass("fa-minus-circle");
        });
    }
    else {
        $(e.target).removeClass("fa-minus-circle");
        $(e.target).addClass("fa-plus-circle");
        dimensionSelector.hide();
    }
}
;
function getJsonResponse() {
    var queryParams = getQueryParams();
    var pivotDimension = $("#splitBy").val();
    if (pivotDimension === "none") {
        pivotDimension = "";
    }
    var graphIdVal = $("#graphId").val();
    if (graphIdVal === "" || graphIdVal === "new graph" || graphIdVal === null) {
        graphIdVal = generateUuid();
    }
    queryData(getMachineName(), $("#EnvironmentList").val(), getTimeoutValue(), pivotDimension, $("#counters").val(), 10, queryParams, $("#start").data("datetimepicker").getLocalDate(), $("#end").data("datetimepicker").getLocalDate(), 1, 1, 0, 0, generateUuid(), graphIdVal);
}
function getTimeoutValue() {
    return $("#timeout").val();
}
function hydrateWires(wires) {
    if (wires != undefined) {
        $.each(wires.counters, function (index, counter) {
            queryData(counter.machines, counter.environmentName, 5000, counter.pivotDimension, counter.counter, 10, "", counter.startTime, counter.endTime, counter.width, counter.height, counter.top, counter.left, generateUuid(), counter.graphId);
        });
    }
}
function getQueryParams() {
    var queryParams = {};
    $(".dimension").each(function (i, val) {
        var dimensionName = $(val).attr("name");
        if ($(val).val() != "" && $(val).val() != null) {
            queryParams[dimensionName] = $(val).val();
        }
    });
    var percentileValue = $("#percentile").val();
    if (percentileValue != "") {
        queryParams["percentile"] = percentileValue;
    }
    return queryParams;
}
function getDefaultPercentiles() {
    var defaults = [];
    defaults.push("average");
    defaults.push("minimum");
    defaults.push("maximum");
    for (var i = 0; i < 100; i++) {
        defaults.push(i);
    }
    return defaults;
}
window.onload = function () {
    $("#start").datetimepicker({ language: "en", format: "MM/dd/yyyy HH:mm:ss PP", pick12HourFormat: true });
    var endDate = new Date();
    $("#start").data("datetimepicker").setLocalDate(new Date(endDate.getTime() - 3600000));
    $("#end").datetimepicker({ language: "en", format: "MM/dd/yyyy HH:mm:ss PP", pick12HourFormat: true });
    $("#end").data("datetimepicker").setLocalDate(endDate);
    $("#getData").click(getJsonResponse);
    $("#EnvironmentList").blur(function () {
        updateCounters();
    });
    refreshEnvironments();
    $("#queryEnvironment").click(updateCounters);
    $("#queryEnvironment").click(function () {
        if ($("#queryEnvironment").is(":checked")) {
            $("#machineNameArea").hide();
        }
        else {
            $("#machineNameArea").show();
        }
    });
    $("#machineList").click(updateCounters);
    $("#manualMode").click(updateCounters);
    $("#counters").change(function () {
        updateDimensions($("#counters").val());
    });
    $("#splitBy").select2();
    gridster = $(".gridster ul").gridster({
        widget_margins: [10, 10],
        min_cols: 2,
        widget_base_dimensions: [640, 300]
    }).data("gridster");
    $("#loading").hide();
    $(document).ajaxStart(function () {
        $("#loading").show();
    }).ajaxStop(function () {
        $("#loading").hide();
    });
};
function paramsUnserialize(p) {
    var ret = {}, seg = p.replace(/^\?/, "").split("&"), len = seg.length, i = 0, s;
    for (; i < len; i++) {
        if (!seg[i]) {
            continue;
        }
        s = seg[i].split("=");
        ret[s[0]] = s[1];
    }
    return ret;
}
function updateWires() {
    for (var key in graphToSeriesMap) {
        if (graphToSeriesMap.hasOwnProperty(key)) {
            updateWire(key);
        }
    }
}
function updateWire(graphId) {
    var masterWindow = $("#" + graphId);
    var top = masterWindow.attr("data-col");
    var left = masterWindow.attr("data-row");
    var width = masterWindow.attr("data-sizex");
    var height = masterWindow.attr("data-sizey");
    graphToSeriesMap[graphId].map(function (seriesId) {
        wires[seriesId].top = top;
        wires[seriesId].left = left;
        wires[seriesId].width = width;
        wires[seriesId].height = height;
    });
    refreshPath();
}
function refreshPath() {
    var wireArray = [];
    var keyArray = [];
    var key;
    for (key in wires) {
        if (wires.hasOwnProperty(key)) {
            wireArray.push(JSON.stringify(wires[key]));
        }
    }
    for (key in graphToSeriesMap) {
        if (graphToSeriesMap.hasOwnProperty(key)) {
            keyArray.push(key);
        }
    }
    keyArray.push("new graph");
    $("#graphId").select2({ data: keyArray });
    var newPath = "?wires={\"counters\":[" + wireArray.toString() + "]}";
    if (typeof (window.history.pushState) == "function") {
        window.history.pushState(null, newPath, newPath);
    }
    else {
        window.location.hash = "#!" + newPath;
    }
}
function generateUuid() {
    var d = new Date().getTime();
    var uuid = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
        var r = (d + Math.random() * 16) % 16 | 0;
        d = Math.floor(d / 16);
        return (c === "x" ? r : (r & 0x7 | 0x8)).toString(16);
    });
    return uuid;
}
;
function replaceAll(txt, replace, withThis) {
    return txt.replace(new RegExp(replace, "g"), withThis);
}
function highchartsResizeHack(value) {
    setTimeout(function () {
        $(window).resize();
    }, 0);
    return value;
}
function getHighchartsConfig(selectPoint, series, options) {
    Highcharts.setOptions({
        global: {
            timezoneOffset: new Date().getTimezoneOffset()
        }
    });
    return {
        chart: {
            backgroundColor: "rgba(255, 255, 255, 0)",
            plotBackgroundColor: "rgba(255, 255, 255, 0.9)",
            zoomType: "x",
            reflow: true,
            type: "StockChart",
            style: {
                fontFamily: "Segoe UI"
            }
        },
        credits: { enabled: false },
        title: {
            text: "",
            style: {
                "font-weight": "lighter"
            }
        },
        navigator: {
            enabled: true,
            baseSeries: 0,
            height: 30
        },
        tooltip: {
            shared: true,
            xDateFormat: "%A, %b %d, %Y %H:%M:%S",
            formatter: function (tooltip) {
                var items = this.points, series = items[0].series, s;
                // sort the values 
                items.sort(function (a, b) { return ((a.y < b.y) ? -1 : ((a.y > b.y) ? 1 : 0)); });
                // make them descend (so we spot high outliers)
                items.reverse();
                return tooltip.defaultFormatter.call(this, tooltip);
            }
        },
        scrollbar: { liveRedraw: false },
        rangeSelector: { enabled: false },
        legend: {
            enabled: true,
            maxHeight: 100,
            padding: 3,
            title: { text: " " }
        },
        plotOptions: {
            series: {
                shadow: false,
                marker: { enabled: false },
                point: {
                    events: selectPoint ? {
                        click: selectPoint
                    } : {}
                },
                cursor: selectPoint ? "pointer" : undefined
            },
            line: {
                animation: false
            }
        },
        xAxis: {
            type: "datetime",
            dateTimeLabelFormats: {
                second: "%Y-%m-%d<br/>%H:%M:%S",
                minute: "%m-%d<br/>%l:%M%p",
                hour: "%m-%d<br/>%l:%M%p",
                day: "%Y<br/>%m-%d",
                week: "%Y<br/>%m-%d",
                month: "%Y-%m",
                year: "%Y"
            }
        },
        yAxis: (options.separateYAxes && options.separateYAxes()) ? series.map(function (series, index) {
            return {
                min: 0,
                opposite: index % 2 === 1,
                title: {
                    text: ""
                },
                lineWidth: 1,
                id: "yAxis_" + index
            };
        }) : {
            min: 0,
            title: {
                text: ""
            },
            lineWidth: 1,
            id: "yAxis_" + 0
        },
        series: series.map(function (s, i) {
            return {
                data: [],
                id: s.id,
                name: s.name,
                type: s.type,
                tooltip: {
                    valueDecimals: 2
                },
                yAxis: "yAxis_" + ((options.separateYAxes && options.separateYAxes()) ? i : 0)
            };
        })
    };
}
//# sourceMappingURL=metricsystem.js.map