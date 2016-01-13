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

var wires: { [index: string]: any; } = {};
var gridster: Gridster = null;
var graphToSeriesMap: { [index: string]: string[]; } = {};
var seriesToMetadataMap: { [index: string]: IDataSeries; } = {};
var chartData = {}; // map from seriesId to data

interface IDataSeries {
    id: string;
    name: string;
    type: string;
}

function queryData(machineName: any, environmentName: any, timeoutValue: any, pivotDimension: any, counterName: any, limit: any, params: any, startTime: any, endTime: any, width: any, height: any, top: any, left: any, seriesId: any, graphId: any) {

    if (seriesId === "") seriesId = generateUuid();

    if (pivotDimension !== "") {
        params["dimension"] = pivotDimension;
    }

    var queryParams = decodeURIComponent(params) + "&" + getStartEndTimes(startTime, endTime);
    var gridData = [];
    var perDimensionData = {};

    var seriesMachines = machineName;
    if (machineName === "") seriesMachines = environmentName;
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
        success: (values => {
            var series: IDataSeries[] = [];

            var seriesPrefix = "";
            if (graphToSeriesMap[graphId] !== undefined && graphToSeriesMap[graphId].length > 0) {
                seriesPrefix += "series" + graphToSeriesMap[graphId].length + " - ";
            } else {
                seriesPrefix += "series0 - ";
            }

            // Build data sets
            if (pivotDimension !== "") {
                values.forEach(value => {
                    if (!perDimensionData[value.DimensionVal]) {
                        perDimensionData[seriesPrefix + value.DimensionVal] = [];
                    }
                    perDimensionData[seriesPrefix + value.DimensionVal].push([new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue]);
                });

                series = Object.keys(perDimensionData).map(seriesFromMachine);
            } else {
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
                data = values.map(value => [new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue]);

                graphToSeriesMap[graphId].map(val => {
                    series.push(seriesToMetadataMap[val]);
                });

                chartDiv.highcharts(getHighchartsConfig(null, series, {}));

                thisChart = chartDiv.highcharts();
                highchartsResizeHack(chartDiv);

                thisChart.get(seriesFromMachine(seriesPrefix + counterName).id).setData(data, false);

                graphToSeriesMap[graphId].map(val => {
                    thisChart.get(seriesToMetadataMap[val].id).setData(chartData[val], false);
                });

                values.forEach(value => {
                    gridData.push([new Date(parseInt(value.EndTime.substr(6))).toLocaleString(), value.ChartValue, value.MachineCount]);
                });

                thisChart.redraw();

                chartData[seriesId] = data;
                graphToSeriesMap[graphId].push(seriesId);
                seriesToMetadataMap[seriesId] = seriesFromMachine(seriesPrefix + counterName);

            }
            // Create new chart
            else {
                graphToSeriesMap[graphId] = [];
                graphToSeriesMap[graphId].push(seriesId);
                seriesToMetadataMap[seriesId] = seriesFromMachine(seriesPrefix + counterName);
                chartDiv = $("<div>").addClass("chartContainer").attr("id", graphId + "_chart");
                var maxDescWidth = 84 * width;

                var seriesWrap = $("<li>").attr("id", graphId).addClass("seriesWrap").append($("<div>").addClass("seriesTitle").attr("id", graphId + "_title").attr("title", seriesDescription).text(seriesDescription.substr(0, maxDescWidth) + "..."), chartDiv.highcharts(getHighchartsConfig(null, series, {})), $("<div>").addClass("table-wrapper").attr("id", graphId + "_table").hide());

                if (top > 0 && left > 0) {
                    gridster.add_widget(seriesWrap[0], width, height, top, left);
                } else {
                    gridster.add_widget(seriesWrap[0], width, height);
                }
                thisChart = chartDiv.highcharts();
                highchartsResizeHack(chartDiv);

                if (pivotDimension !== "") {
                    Object.keys(perDimensionData).map(dimension => {
                        thisChart.get(seriesFromMachine(dimension).id).setData(perDimensionData[dimension], false);
                    });

                    chartData[seriesId] = perDimensionData;

                    values.forEach(value => {
                        gridData.push([new Date(parseInt(value.EndTime.substr(6))).toLocaleString(), value.ChartValue, value.MachineCount]);
                    });
                } else {
                    data = values.map(value => [new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue]);
                    chartData[seriesId] = data;
                    //renderChart(data);


                    thisChart.get(seriesFromMachine(seriesPrefix + counterName).id).setData(data, false);

                    values.forEach(value => {
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

                $("#" + graphId + "_title").append(
                    $("<i class='fa fa-times-circle'></i>").addClass("toggle").click(() => {
                        for (var val in graphToSeriesMap[graphId]) {
                            delete wires[val];
                            delete seriesToMetadataMap[val];
                        }

                        gridster.remove_widget($("#" + graphId)[0]);
                        delete graphToSeriesMap[graphId];
                        updateWires();
                    }),
                    $("<i class='fa fa-table'></i>").addClass("toggle").click(e => {
                        if ($("#" + graphId + "_chart").is(":hidden")) {
                            $("#" + graphId + "_table").hide();
                            $("#" + graphId + "_chart").show();
                            $(e.target).removeClass("fa-line-chart");
                            $(e.target).addClass("fa-table");
                        } else {
                            $("#" + graphId + "_chart").hide();
                            $("#" + graphId + "_table").show();
                            $("th").each(function() {
                                if (this.textContent == "EndTime") {
                                    this.click();
                                    this.click();
                                }
                            });
                            $(e.target).removeClass("fa-table");
                            $(e.target).addClass("fa-line-chart");
                        }
                    }),
                    $("<i class='fa fa-bookmark'></i>").addClass("toggle").click(() => {
                            alert(document.URL.split("?")[0] + "?" + "wires={\"counters\":[" + JSON.stringify(wires[seriesId]) + "]}");
                        }
                    ),
                    $("<i class='fa fa-info-circle'></i>").addClass("toggle").click(() => {
                            alert(seriesDescription);
                        }
                    ));
            }

            updateWires();
            refreshPath();
        })
    });
    return;
}

// Utility functions

var seriesFromMachine = name => {
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
    } else if ($("#EnvironmentList").val() === undefined || $("#EnvironmentList").val() === null) {
        machines.push(defaultMachineName);
    } else {
        $.get(baseUri + "/machines?environment=" + $("#EnvironmentList").val(), result => {
            for(var machine in result) {
                machines.push(machine);
            }

            machines.sort();
            machines.map(m => {
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
    var result: string;
    if ($("#queryEnvironment").is(":checked")) {
        result = "";
    } else if ($("#machineList").is(":checked")) {
        result = $("#machineName").val();
        if (result == null) return defaultMachineName;
        if (result.toString().indexOf("Unable") !== -1) return defaultMachineName;
        result = $("#machineName").val().join();
    } else {
        result = $("#manualMachineName").val();
    }

    return result;
}

function getStartEndTimes(startTime: any, endTime: any) {
    if (startTime !== "" && endTime !== "") {
        return "start=" + new Date(startTime).toISOString() + "&end=" + new Date(endTime).toISOString();
    }
    return "start=1 hour ago&end=now";
}



function refreshEnvironments() {
    $.get(baseUri + "/environments").done(data => {
        $("#EnvironmentList").select2({ data: data });
        updateCounters();
    });
}

function updateCounters() {
    if ($("#queryEnvironment").is(":checked")) {
        $("#machineName").hide();
        $("#manualMachineName").hide();
    } else if ($("#machineList").is(":checked")) {
        $("#machineName").show();
        $("#manualMachineName").hide();
    } else {
        $("#machineName").hide();
        $("#manualMachineName").show();
    }
    refreshCounters();
}

function refreshCounters() {
    var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: "/*", queryCommand: "list", queryParameters: "", timeoutValue: getTimeoutValue() };

    $.post(baseUri + "/info", queryPayload).done(data => {

        $("#counters").select2({ data: data });
        updateDimensions(data[0]);
    });
}

function updateDimensions(counterName: string) {
    var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: counterName, queryCommand: "listDimensions", queryParameters: "", timeoutValue: getTimeoutValue() };
    $("#splitBy").empty();
    $("#splitBy").append($("<option>").text("none"));

    $.post(baseUri + "/info", queryPayload).done(dimensions => {
        var dimensionList = $("<div>").addClass("dimensionGrid");
        dimensionList.addClass("dimensionValues");
        dimensions.forEach(dimension => {
            $("#dimensionGrid").empty();

            var dimensionLabel = ($("<label>").text(dimension).attr("for", "dim_" + dimension).addClass("dimValLabel"));
            var dimensionSelector = $("<li>").append($("<select>").attr("name", dimension).attr("id", dimension).addClass("dimension"));

            dimensionList.append($("<li>").append(dimensionLabel, $("<i>").addClass("fa fa-plus-circle").click(e => { updateDimensionValues(counterName, dimension, dimensionSelector, e); })), dimensionSelector);
            dimensionSelector.hide();
            $("#splitBy").append($("<option>").text(dimension));
        });

        $("#splitBy").select2({
            minimumResultsForSearch: 10
        });

        $("#dimensionGrid").append(dimensionList);


    });
}

function updateDimensionValues(counterName: any, dimensionName: any, dimensionSelector: any, e: any) {
    var toggleButton = $(e.target);

    if (toggleButton.hasClass("fa-plus-circle")) {
        var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: counterName, queryCommand: "listDimensionValues", queryParameters: "dimension=" + dimensionName, timeoutValue: getTimeoutValue() };
        $.post(baseUri + "/info", queryPayload).done(data => {
            $("#" + dimensionName).select2({ data: data });
            dimensionSelector.show();
            $(e.target).removeClass("fa-plus-circle");
            $(e.target).addClass("fa-minus-circle");
        });
    } else {
        $(e.target).removeClass("fa-minus-circle");
        $(e.target).addClass("fa-plus-circle");
        dimensionSelector.hide();
    }
};

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

function hydrateWires(wires: any) {
    if (wires != undefined) {
        $.each(wires.counters,(index, counter) => {
            queryData(counter.machines, counter.environmentName, 5000, counter.pivotDimension, counter.counter, 10, "", counter.startTime, counter.endTime, counter.width, counter.height, counter.top, counter.left, generateUuid(), counter.graphId);
        });
    }
}

function getQueryParams() {
    var queryParams = {};

    $(".dimension").each((i, val) => {
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

window.onload = () => {


    $("#start").datetimepicker({ language: "en", format: "MM/dd/yyyy HH:mm:ss PP", pick12HourFormat: true });
    var endDate = new Date();
    $("#start").data("datetimepicker").setLocalDate(new Date(endDate.getTime() - 3600000));
    $("#end").datetimepicker({ language: "en", format: "MM/dd/yyyy HH:mm:ss PP", pick12HourFormat: true });
    $("#end").data("datetimepicker").setLocalDate(endDate);

    $("#getData").click(getJsonResponse);
    $("#EnvironmentList").blur(() => {
        updateCounters();
    });

    refreshEnvironments();

    $("#queryEnvironment").click(updateCounters);

    $("#queryEnvironment").click(() => {
        if ($("#queryEnvironment").is(":checked")) {
            $("#machineNameArea").hide();
        } else {
            $("#machineNameArea").show();
        }
    });

    $("#machineList").click(updateCounters);
    $("#manualMode").click(updateCounters);
    $("#counters").change(() => { updateDimensions($("#counters").val()); });

    $("#splitBy").select2();

    gridster = $(".gridster ul").gridster({
        widget_margins: [10, 10],
        min_cols: 2,
        widget_base_dimensions: [640, 300]
    }).data("gridster");

    $("#loading").hide();

    $(document).ajaxStart(() => {
        $("#loading").show();
    }).ajaxStop(() => {
        $("#loading").hide();
    });
}

function paramsUnserialize(p: any) {
    var ret = {},
        seg = p.replace(/^\?/, "").split("&"),
        len = seg.length,
        i = 0,
        s: string[];
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

function updateWire(graphId: any) {
    var masterWindow = $("#" + graphId);

    var top = masterWindow.attr("data-col");
    var left = masterWindow.attr("data-row");
    var width = masterWindow.attr("data-sizex");
    var height = masterWindow.attr("data-sizey");

    graphToSeriesMap[graphId].map(seriesId => {
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
    } else {
        window.location.hash = "#!" + newPath;
    }
}

function generateUuid() {
    var d = new Date().getTime();
    var uuid = "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, c => {
        var r = (d + Math.random() * 16) % 16 | 0;
        d = Math.floor(d / 16);
        return (c === "x" ? r : (r & 0x7 | 0x8)).toString(16);
    });
    return uuid;
};

function replaceAll(txt: any, replace: any, withThis: any) {
    return txt.replace(new RegExp(replace, "g"), withThis);
}

function highchartsResizeHack(value: any) {
    setTimeout(() => {
        $(window).resize();
    }, 0);
    return value;
}

function getHighchartsConfig(selectPoint: any, series: any, options: any) {
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
            formatter(tooltip) {
                var items = this.points,
                    series = items[0].series,
                    s;

                // sort the values 
                items.sort((a, b) => ((a.y < b.y) ? -1 : ((a.y > b.y) ? 1 : 0)));

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
        yAxis: (options.separateYAxes && options.separateYAxes()) ? series.map((series, index) => {
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
        series: series.map((s,i) => {
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