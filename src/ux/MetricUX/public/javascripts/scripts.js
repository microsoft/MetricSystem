// Variable definitions used to track the current sets of machines / series being queried
var defaultMachineName = "127.0.0.1";
var baseURI = "/data";

var currentMachineName = "";
var currentCounterName = "";

var wires = {};
var gridster = {};
var graphToSeriesMap = {}; // map from graphId to contained ChartIds
var seriesToMetadataMap = {};
var chartData = {}; // map from seriesId to data

function queryData(machineName, environmentName, timeoutValue, pivotDimension, counterName, limit, params, startTime, endTime, width, height, top, left, seriesId, graphId) {
    var seriesFromMachine = function (name) {
        return {
            id: "metric-" + name,
            name: name,
            type: "line"
        };
    };
    
    if (seriesId === "") seriesId = generateUUID();
    
    if (pivotDimension !== "") {
        params["dimension"] = pivotDimension;
    }
    
    var queryParams = $.param(params) + "&" + getStartEndTimes(startTime, endTime);
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
    if ($.param(params) !== "") {
        seriesDescription += " [" + $.param(params) + "]";
    }
    
    seriesDescription += " from " + new Date(startTime).toLocaleTimeString() + " to " + new Date(endTime).toLocaleTimeString();
    var queryPayload = { machineName: machineName, environmentName: environmentName, counterName: counterName, queryCommand: "query", queryParameters: queryParams, timeoutValue: timeoutValue };
    $.post(baseURI + "/query", queryPayload).done(function (values) {
        var series;
        
        var seriesPrefix = "";
        if (graphToSeriesMap[graphId] !== undefined && graphToSeriesMap[graphId].length > 0) {
            seriesPrefix += "series" + graphToSeriesMap[graphId].length + " - ";
        } else {
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
            data = values.map(function (value) {
                return [new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue];
            });
            
            $(graphToSeriesMap[graphId]).each(function (i, val) {
                series.push(seriesToMetadataMap[val]);
            });
            
            chartDiv.highcharts(getHighchartsConfig(null, series, {}));
            
            thisChart = chartDiv.highcharts();
            highchartsResizeHack(chartDiv);
            
            thisChart.get(seriesFromMachine(seriesPrefix + counterName).id).setData(data, false);
            
            $(graphToSeriesMap[graphId]).each(function (i, val) {
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

        // Create new chart
        else {
            graphToSeriesMap[graphId] = [];
            graphToSeriesMap[graphId].push(seriesId);
            seriesToMetadataMap[seriesId] = seriesFromMachine(seriesPrefix + counterName);
            chartDiv = $("<div>").addClass("chartContainer").attr("id", graphId + "_chart");
            var maxDescWidth = 84 * width;
            
            var seriesWrap = $("<li>").attr("id", graphId).addClass("seriesWrap").append($("<div>").addClass("seriesTitle").attr("id", graphId + "_title").attr("title", seriesDescription).text(seriesDescription.substr(0, maxDescWidth) + "..."), chartDiv.highcharts(getHighchartsConfig(null, series, {})), $("<div>").addClass("table-wrapper").attr("id", graphId + "_table").hide());
            
            if (top > 0 && left > 0) {
                gridster.add_widget(seriesWrap, width, height, top, left);
            }
            else {
                gridster.add_widget(seriesWrap, width, height);
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
                data = values.map(function (value) {
                    return [new Date(parseInt(value.EndTime.substr(6))).getTime(), value.ChartValue];
                });
                chartData[seriesId] = data;
                //renderChart(data);
                
                
                thisChart.get(seriesFromMachine(seriesPrefix + counterName).id).setData(data, false);
                
                values.forEach(function (value) {
                    gridData.push([new Date(parseInt(value.EndTime.substr(6))).toLocaleString(), value.ChartValue, value.MachineCount]);
                });

            }
        }
        
        $("#" + graphId + "_table").append($("<h4>").text(seriesDescription), grid);
        
        grid.dataTable({
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
            graphId: graphId, counter: counterName, machines: machineName, environmentName: environmentName, startTime: startTime, endTime: endTime, dimensions: replaceAll($.param(params), "&", "%26"), pivotDimension: pivotDimension
        };
        
        wires[seriesId] = newCounter;
        
        if (graphToSeriesMap[graphId].length == 1) {
            
            $("#" + graphId + "_title").append(
                $("<i class='fa fa-times-circle'></i>").addClass("toggle").click(function () {
                    $(graphToSeriesMap[graphId]).each(function (i, val) {
                        delete wires[val];
                        delete seriesToMetadataMap[val];
                    });
                    
                    gridster.remove_widget($("#" + graphId));
                    delete graphToSeriesMap[graphId];
                    updateWires();
                }),
                            $("<i class='fa fa-table'></i>").addClass("toggle").click(function (e) {
                    if ($("#" + graphId + "_chart").is(":hidden")) {
                        $("#" + graphId + "_table").hide();
                        $("#" + graphId + "_chart").show();
                        $(e.target).removeClass("fa-line-chart");
                        $(e.target).addClass("fa-table");
                    }
                    else {
                        $("#" + graphId + "_chart").hide();
                        $("#" + graphId + "_table").show();
                        $("th").each(function () { if (this.textContent == "EndTime") { this.click(); this.click(); } });
                        $(e.target).removeClass("fa-table");
                        $(e.target).addClass("fa-line-chart");
                    }
                }),
                            $("<i class='fa fa-bookmark'></i>").addClass("toggle").click(function () {
                    alert(document.URL.split("?")[0] + "?" + 'wires={"counters":[' + JSON.stringify(wires[seriesId]) + ']}')
                }
                ),
                            $("<i class='fa fa-info-circle'></i>").addClass("toggle").click(function () {
                    alert(seriesDescription);
                }
                ));
        }
        
        updateWires();
        refreshPath();
    });
}
// Utility functions

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
        $.get(baseURI + "/machines?environment=" + $("#EnvironmentList").val(), function (result) {
            $.each(result, function () {
                machines.push(this);
            });
            machines.sort();
            $.each(machines, function () {
                machineList.append($("<option class='machineNameOption' />").val(this.Hostname).text(this.Hostname));
            });
            if ($("#queryEnvironment").is(':checked')) {
                $("#machineName").hide();
            }

        });
    }
    
    $(".machineNameOption").first().attr('checked', 'checked');
}

function getMachineName() {
    var result;
    if ($("#queryEnvironment").is(':checked')) {
        result = "";
    }
    else if ($("#machineList").is(':checked')) {
        result = $("#machineName").val();
        if (result == null) return defaultMachineName;
        if (result.toString().indexOf("Unable") !== -1) return defaultMachineName;
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


$("#queryEnvironment").click(function () {
    if ($(this).is(':checked')) {
        $("#machineNameArea").hide();
    } else {
        $("#machineNameArea").show();
    }
});

function refreshEnvironments() {
    $.get(baseURI + "/environments").done(function (data) {
        $('#EnvironmentList').select2({ data: data });
        updateCounters();
    });
}

function updateCounters() {
    if ($("#queryEnvironment").is(':checked')) {
        $("#machineName").hide();
        $("#manualMachineName").hide();
    }
    else if ($("#machineList").is(':checked')) {
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
    
    $.post(baseURI + "/info", queryPayload).done(function (data) {
        
        $("#counters").select2({ data: data });
        updateDimensions(data[0]);
    });
}

function updateDimensions(counterName) {
    var queryPayload = { machineName: getMachineName(), environmentName: $("#EnvironmentList").val(), counterName: counterName, queryCommand: "listDimensions", queryParameters: "", timeoutValue: getTimeoutValue() };
    $("#splitBy").empty();
    $("#splitBy").append($("<option>").text("none"));
    
    $.post(baseURI + "/info", queryPayload).done(function (dimensions) {
        var dimensionList = $("<div>").addClass("dimensionGrid");
        dimensionList.addClass("dimensionValues");
        dimensions.forEach(function (dimension) {
            $("#dimensionGrid").empty();
            
            var dimensionLabel = ($("<label>").text(dimension).attr("for", "dim_" + dimension).addClass("dimValLabel"));
            var dimensionSelector = $("<li>").append($("<select>").attr("name", dimension).attr("id", dimension).addClass("dimension"));
            
            dimensionList.append($("<li>").append(dimensionLabel, $("<i>").addClass("fa fa-plus-circle").click(function (e) { updateDimensionValues(counterName, dimension, dimensionSelector, e); })), dimensionSelector);
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
        $.post(baseURI + "/info", queryPayload).done(function (data) {
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
};

function getJSONResponse() {
    var queryParams = getQueryParams();
    
    var pivotDimension = $("#splitBy").val();
    if (pivotDimension === "none") {
        pivotDimension = "";
    }
    
    var graphIdVal = $("#graphId").val();
    if (graphIdVal === "" || graphIdVal === "new graph" || graphIdVal === null) {
        graphIdVal = generateUUID();
    }
    
    queryData(getMachineName(), $("#EnvironmentList").val(), getTimeoutValue(), pivotDimension, $("#counters").val(), 10, queryParams, $("#start").data("datetimepicker").getLocalDate(), $("#end").data("datetimepicker").getLocalDate(), 1, 1, 0, 0, generateUUID(), graphIdVal);
}

function getTimeoutValue() {
    return $("#timeout").val();
}

function hydrateWires(wires) {
    if (wires != undefined) {
        $.each(wires.counters, function (index, counter) {
            queryData(counter.machines, counter.environmentName, 5000, counter.pivotDimension, counter.counter, 10, "", counter.startTime, counter.endTime, counter.width, counter.height, counter.top, counter.left, generateUUID(), counter.graphId);
        });
    }
}
function getQueryParams() {
    var queryParams = {};
    
    $(".dimension").each(function (i, val) {
        var dimensionName = $(val).attr('name');
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
    defaults.push('average');
    defaults.push('minimum');
    defaults.push('maximum');
    for (i = 0; i < 100; i++) {
        defaults.push(i);
    }
    return defaults;
}

//function renderChart(data) {
//    /*These lines are all chart setup.  Pick and choose which chart features you want to utilize. */
//    nv.addGraph(function () {
//        var chart = nv.models.lineChart()
//                .margin({ left: 100 })//Adjust chart margins to give the x-axis some breathing room.
//                .useInteractiveGuideline(true)//We want nice looking tooltips and a guideline!
//      //          .transitionDuration(350)//how fast do you want the lines to transition?
//                .showLegend(true)//Show the legend, allowing users to turn on/off line series.
//                .showYAxis(true)//Show the y-axis
//                .showXAxis(true)//Show the x-axis
//  ;



//        chart.xAxis//Chart x-axis settings
//      .axisLabel('Time')
//        .tickFormat(function (d) {
//            return d3.time.format('%x')(new Date(d))
//        });

//        chart.yAxis//Chart y-axis settings
//      .axisLabel('Requests')
//      .tickFormat(d3.format('.02f'));
//        var chartData =  [{
//            values: data.map(function (value) {
//                return { x: value[0], y: value[1] };
//            }),      //values - represents the array of {x,y} data points
//            key: 'Requests', //key  - the name of the series.
//            color: '#ff7f0e'  //color - optional: choose your own line color.
//        }];

//        d3.select('#chart svg')//Select the <svg> element you want to render the chart in.   
//      .datum(chartData)//Populate the <svg> element with chart data...
//      .call(chart);          //Finally, render the chart!

//        //Update the chart when window resizes.
//        nv.utils.windowResize(function () { chart.update() });
//        return chart;
//    });
//}

$(function () {
    $('#start').datetimepicker({ language: 'en', format: "MM/dd/yyyy HH:mm:ss PP", pick12HourFormat: true });
    var endDate = new Date();
    $("#start").data("datetimepicker").setLocalDate(new Date(endDate-3600000));
    $('#end').datetimepicker({ language: 'en', format: "MM/dd/yyyy HH:mm:ss PP", pick12HourFormat: true });
    $("#end").data("datetimepicker").setLocalDate(endDate);

    $("#getData").click(getJSONResponse);
    $("#EnvironmentList").blur(function () {
        updateCounters();
    });
    
    refreshEnvironments();
    
    $('#queryEnvironment').click(updateCounters);
    $('#machineList').click(updateCounters);
    $('#manualMode').click(updateCounters);
    $('#counters').change(function () { updateDimensions($('#counters').val()); });
    
    $("#splitBy").select2();
    
    gridster = $(".gridster ul").gridster({
        widget_margins: [10, 10],
        min_cols: 2,
        widget_base_dimensions: [640, 300],
        resize: { enabled: true, stop: function (e) { updateWires(); setTimeout($(window).resize(), 0) } },
        draggable: { stop: function (e) { updateWires(); setTimeout($(window).resize(), 0) } }
    })
        .data("gridster");
    
    $("#loading").hide();
    
    $(document).ajaxStart(function () {
        $("#loading").show();
    }).ajaxStop(function () {
        $("#loading").hide();
    });
});

function params_unserialize(p) {
    var ret = {},
        seg = p.replace(/^\?/, '').split('&'),
        len = seg.length, i = 0, s;
    for (; i < len; i++) {
        if (!seg[i]) { continue; }
        s = seg[i].split('=');
        ret[s[0]] = s[1];
    }
    return ret;
}

function updateWires() {
    for (key in graphToSeriesMap) {
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
    
    $(graphToSeriesMap[graphId]).each(function (i, seriesId) {
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
    
    if (typeof (window.history.pushState) == 'function') {
        window.history.pushState(null, newPath, newPath);
    } else {
        window.location.hash = '#!' + newPath;
    }
}

function generateUUID() {
    var d = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = (d + Math.random() * 16) % 16 | 0;
        d = Math.floor(d / 16);
        return (c == 'x' ? r : (r & 0x7 | 0x8)).toString(16);
    });
    return uuid;
};

function replaceAll(txt, replace, withThis) {
    return txt.replace(new RegExp(replace, 'g'), withThis);
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
                var items = this.points || splat(this),
                    series = items[0].series,
                    s;
                
                // sort the values 
                items.sort(function (a, b) {
                    return ((a.y < b.y) ? -1 : ((a.y > b.y) ? 1 : 0));
                });
                
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
                second: '%Y-%m-%d<br/>%H:%M:%S',
                minute: '%m-%d<br/>%l:%M%p',
                hour: '%m-%d<br/>%l:%M%p',
                day: '%Y<br/>%m-%d',
                week: '%Y<br/>%m-%d',
                month: '%Y-%m',
                year: '%Y'
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
        series: series.map(function (series, index) {
            return {
                data: [],
                id: series.id,
                name: series.name,
                type: series.type,
                tooltip: {
                    valueDecimals: 2
                },
                yAxis: "yAxis_" + ((options.separateYAxes && options.separateYAxes()) ? index : 0)
            };
        })
    };
}