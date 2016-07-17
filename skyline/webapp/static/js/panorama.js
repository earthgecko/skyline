var GRAPH_URL,
    PANORAMA_GRAPH_URL,
    FULL_NAMESPACE,
    panorama_mini_graph,
    panorama_big_graph,
    anomaly_id,
    name,
    selected,
    selected_anomaly,
    anomalous_datapoint,
    y_anomalous_datapoint,
    until_timestamp,
    full_duration,
    from_timestamp,
    created_date,
    time_shift,
    fetch_from_timestamp,
    fetch_until_timestamp,
    datapoints;

var panorama_mini_data = [];
var panorama_big_data = [];
var datapoints = [];

var initial = true;

// This function call is hardcoded as JSONP in the panorama.json file
var handle_data = function(panorama_data) {
    $('#metrics_listings').empty();

    for (i in panorama_data) {
        metric = panorama_data[i];
        anomaly_id = metric[0];
        name = metric[1];
        selected = metric[1];
        selected_anomaly = anomaly_id + '.' + selected;
        until_timestamp = parseInt(metric[3]);
        full_duration = metric[4];
        from_timestamp = (until_timestamp - full_duration);
        created_date = metric[5];

        time_shift = parseInt(full_duration) / 24;

/* @modified 20160703 - Feature #1464: Webapp rebrow
dygraph was just updated - no more strftime, now include strftime-0.9.2 for
dygraph-1.1.1 */
//        from_date = new Date(from_timestamp * 1000).strftime('%H:%M_%Y%m%d');
//        until_date = new Date(until_timestamp * 1000).strftime('%H:%M_%Y%m%d');
        from_date = strftime('%H:%M_%Y%m%d', new Date(from_timestamp * 1000));
        until_date = strftime('%H:%M_%Y%m%d', new Date(until_timestamp * 1000));


//        var src = GRAPH_URL.replace('%s', name);
        var src = PANORAMA_GRAPH_URL + '/render/?width=1400&from=' + from_date + '&until=' + until_date + '&target=' + name

        // Add a space after the metric name to make each unique
//        to_append = "<div class='sub'><a target='_blank' href='" + src + "'><div class='name'>" + anomaly_id + " </div></a>&nbsp;&nbsp;"
        to_append = "<div class='sub'><a target='_blank' href='" + src + "'><div class='anomaly_id'>" + anomaly_id + " </div></a>&nbsp;&nbsp;"
        to_append += "<div class='metric_name'>" + name + "</div>";
        to_append += "<div class='selected_anomaly_id'>" + parseInt(metric[0]) + "</div>";
        to_append += "<div class='count'>" + parseInt(metric[2]) + "</div>";
        to_append += "<div class='from_timestamp'>" + from_timestamp + "</div>";
        to_append += "<div class='until_timestamp'>" + until_timestamp + "</div>";
        to_append += "<div class='full_duration'>" + full_duration + "</div>";
        to_append += "<div class='created_date'>" + created_date + "</div>";
        to_append += "<div class='time_shift'>" + time_shift + "</div>";
        $('#metrics_listings').append(to_append);
    }

    if (initial) {
        anomaly_id = panorama_data[0][0];
        selected = panorama_data[0][1];
        selected_anomaly = anomaly_id + '.' + selected;
        anomalous_datapoint = panorama_data[0][2];
        until_timestamp = parseInt(panorama_data[0][3]);
        full_duration = panorama_data[0][4];
        created_date = panaroma_date[0][5];
        from_timestamp = (until_timestamp - full_duration);
        time_shift = parseInt(full_duration) / 24;

        // We fetch time_shift less data
        fetch_from_timestamp = (from_timestamp + time_shift);
        // We fetch time_shift more of data
        fetch_until_timestamp = (parseInt(until_timestamp) + time_shift);
        datapoints = [];
        initial = false;
    }

    handle_interaction();
}

// Panorama The callback to this function is handle_data()
var pull_panorama_data = function() {
    $.ajax({
        url: "/static/dump/panorama.json",
        dataType: 'jsonp'
    });
}

var handle_interaction = function() {
    $('.sub').removeClass('selected');
    $('.sub:contains(' + anomaly_id + ')').addClass('selected');

    selected = parseInt($($('.selected').children('.selected')).text());
    selected_anomaly_id = parseInt($($('.selected').children('.anomaly_id')).text());

    anomalous_datapoint = parseInt($($('.selected').children('.count')).text());
    metric_name = $($('.selected').children('.metric_name')).text();

    until_timestamp = parseInt($($('.selected').children('.until_timestamp')).text());
    from_timestamp = parseInt($($('.selected').children('.from_timestamp')).text());
    created_date = $($('.selected').children('.created_date')).text();

    full_duration = parseInt(until_timestamp) - parseInt(from_timestamp);
    time_shift = parseInt(full_duration) / 24;
    time_shift_hours = (parseInt(time_shift) / 60) / 60;

    // We fetch time_shift less data
    fetch_from_timestamp = (parseInt(from_timestamp) + parseInt(time_shift));
    // We fetch time_shift more data
    fetch_until_timestamp = (parseInt(until_timestamp) + parseInt(time_shift));
    api_uri = "/api?graphite_metric=" + metric_name + "&from_timestamp=" + fetch_from_timestamp + "&until_timestamp=" + fetch_until_timestamp + "&anomaly_id=" + anomaly_id
    $.get(api_uri, function(d){
        panorama_big_data = JSON.parse(d)['results'];
        big_graph.updateOptions( { 'file': panorama_big_data } );
    });

    anomaly_string = "anomaly_id: " + anomaly_id + " ";
    $('#graph_title').html(anomaly_string);

    metric_string = "metric: " + metric_name;
    graph_subtitle_string = metric_string
    $('#graph_subtitle').html(graph_subtitle_string);

    graph_ad_string = "anomalous data point: " + anomalous_datapoint;
    $('#graph_subtitle_anomalous_datapoint').html(graph_ad_string);

    graph_fd_string = "full duration: " + full_duration;
    $('#graph_subtitle_full_duration').html(graph_fd_string);

    graph_cd_string = "created: " + created_date;
    $('#graph_subtitle_created_date').html(graph_cd_string);

    graph_ts_string = "time shifted (hours): " + parseInt(time_shift_hours);
    $('#time_shift').html(graph_ts_string);

    // Bleh, hack to fix up the layout on load
    $(window).resize();
}

$(function(){
    big_graph = new Dygraph(document.getElementById("graph"), panorama_big_data, {
        labels: [ 'Date', '' ],
        labelsDiv: document.getElementById('big_label'),
/* @modified 20160703 - Feature #1464: Webapp rebrow
dygraph was just updated - per-axis defines now - dygraph-1.1.1 */
//        xAxisLabelWidth: 35,
//        yAxisLabelWidth: 35,
        axisLabelFontSize: 9,
        rollPeriod: 2,
//        drawXGrid: true,
//        drawYGrid: false,
        interactionModel: {},
        pixelsPerLabel: 14,
//        drawXAxis: true,
        underlayCallback: function(canvas, area, g) {
            var full_duration = parseInt(until_timestamp) - parseInt(from_timestamp);

            var d = new Date();
            var t = d.getTime();
            var time_now = Math.round(t / 1000);
            var anomaly_age = parseInt(time_now) - parseInt(from_timestamp)
            var time_shift = parseInt(full_duration) / 96;
            var from_here = (parseInt(until_timestamp) - parseInt(time_shift));
            if (anomaly_age < 3600) {
                var to_here = (parseInt(until_timestamp) + 900);
            } else {
                var to_here = (parseInt(until_timestamp) + parseInt(time_shift));
            }

            var bottom_left = g.toDomXCoord(from_here);
            var top_right = g.toDomXCoord(to_here);
//            canvas.fillStyle = "rgba(255, 255, 102, 1.0)";
            canvas.fillStyle = "rgba(255,165,0,1)";
            canvas.fillRect(bottom_left, area.y, top_right - bottom_left, area.h);

            var y_anomalous_datapoint = false;
            var original_anomalous_datapoint = false;
            y_range = g.yAxisRanges();
            if (anomalous_datapoint > y_range[0][1]) {
              y_anomalous_datapoint = y_range[0][1];
            };
            if (anomalous_datapoint < y_range[0][0]) {
              y_anomalous_datapoint = y_range[0][0];
            };
            if (y_anomalous_datapoint == false) {
              y_anomalous_datapoint = anomalous_datapoint;
              original_anomalous_datapoint = true;
            };
            if (original_anomalous_datapoint == false) {
              canvas.beginPath();
              canvas.font = "bold 12px sans-serif";
              canvas.fillStyle = "#ff5500";
              var approximation_text = "Aggregated data";
              canvas.fillText(approximation_text,100,30);
              canvas.fillStyle = "#1a1a1a";
              var approximation_text = "the original anomalous data point (" + anomalous_datapoint + ") is not in range";
              canvas.fillText(approximation_text,100,50);
              var approximation_text = "an approximation is shown on the edge of the y range";
              canvas.fillText(approximation_text,100,70);
              var line_width = 10;
              var line_color = "#ffa500";
            } else {
              var line_width = 1;
              var line_color = '#ff0000';
            };

            line = g.toDomYCoord(y_anomalous_datapoint);
            canvas.beginPath();
            canvas.moveTo(0, line);
            canvas.lineTo(canvas.canvas.width, line);
            canvas.lineWidth = line_width;
            canvas.strokeStyle = line_color;
            canvas.stroke();
        },
        axes : {
            x: {
                drawGrid: true,
                drawAxis: true,
                AxisLabelWidth: 60,
                pixelsPerLabel: 50,
                valueFormatter: function(ms) {
// @modified 20160703 - Feature #1464: Webapp rebrow - dygraph-1.1.1 no strftime
//                  return new Date(ms * 1000).strftime('%Y-%m-%d %H:%M') + ' ';
                  vF_now_date = strftime('%Y-%m-%d %H:%M', new Date(ms * 1000));
                  vF_string = vF_now_date + ' ';
                  return vF_string;
                },
                axisLabelFormatter: function(ms, gran, opts, g) {
// @modified 20160703 - Feature #1464: Webapp rebrow - dygraph-1.1.1 no strftime
//                    return new Date(ms * 1000).strftime('%H:%M');
                    now_date = strftime('%H:%M', new Date(ms * 1000));
                    return now_date;
                },
                ticker: Dygraph.dateTicker,
            },
            y : {
                drawGrid: true,
                AxisLabelWidth: 35,
                axisLineColor: 'white'
            },
            '' : {
                axisLineColor: 'white',
                axisLabelFormatter: function(x) {
                    return Math.round(x);
                }
            }
        }
    });

    $.get('/app_settings', function(panorama_data){
        // Get the variables from settings.py
        data = JSON.parse(panorama_data);
        FULL_NAMESPACE = data['FULL_NAMESPACE'];
        GRAPH_URL = data['GRAPH_URL'];
        PANORAMA_GRAPH_URL = data['PANORAMA_GRAPH_URL'];

        // Get initial data after getting the host variables
        pull_panorama_data();

        $(window).resize();
    })

    // Update every ... seconds
    window.setInterval(pull_panorama_data, 900000);

    // Set event listener
/* @modified 20160703 - Feature #1464: Webapp Redis browser
jquery was just updated - live() deprecated, changed to on() */
//    $('.anomaly_id').live('hover', function() {
    $( document ).on('mouseover', '.anomaly_id', function() {
        temp = $(this)[0].innerHTML;
        if (temp != anomaly_id) {
            anomaly_id = temp;
            handle_interaction();
        }
    })

    // Responsive graphs
    $(window).resize(function() {
        resize_window();
    });
});

// I deeply apologize for this abomination
var resize_window = function() {
    big_graph.resize($('#graph_container').width() - 7, ($('#graph_container').height()));
}

// Handle keyboard navigation
Mousetrap.bind(['up', 'down'], function(ev) {
    switch(ev.keyIdentifier) {
        case 'Up':
            next = $('.sub:contains(' + anomaly_id + ')').prev();
        break;

        case 'Down':
            next = $('.sub:contains(' + anomaly_id + ')').next();
        break;
    }

    if ($(next).html() != undefined) {
        anomaly_id = $(next).find('.anomaly_id')[0].innerHTML;
        handle_interaction();
    }

    return false;
}, 'keydown');
