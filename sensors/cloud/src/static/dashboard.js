google.load("visualization", "1", {packages:["corechart"]});
function drawCharts() {
  (function() {
    var g = new Dygraph(
      document.getElementById("g0"),
      "/data/0?format=csv",
      {
        title: "Records per minute",
        xlabel: "Time",
        ylabel: "Records",
        labels: ["Time", "Records"],
        legend: "always",
        labelsDiv: document.getElementById("g0-legend"),
        xValueParser: function(x) { return 1000 * parseFloat(x); },
        axes: {
          x: {
            valueFormatter: function(ms) {
              return new Date(ms).strftime('%H:%M');
            },
            axisLabelFormatter: function(d) {
              return d.strftime('%m/%d');
            },
            ticker: Dygraph.dateTicker,
            pixelsPerLabel: 100
          }
        },
        rollPeriod: 12
      });
  })();

  (function() {
    var g = new Dygraph(
      document.getElementById("g1"),
      "/data/1?format=csv",
      {
        title: "Screen and Power",
        xlabel: "Time",
        ylabel: "Fraction",
        labels: ["Time", "Screen On", "Charging", "Half Charged"],
        legend: "always",
        labelsDiv: document.getElementById("g1-legend"),
        xValueParser: function(x) { return 1000 * parseFloat(x); },
        axes: {
          x: {
            valueFormatter: function(ms) {
              return new Date(ms).strftime('%H:%M');
            },
            axisLabelFormatter: function(d) {
              return d.strftime('%m/%d');
            },
            ticker: Dygraph.dateTicker,
            pixelsPerLabel: 100
          }
        },
        rollPeriod: 12
      });
  })();

  $.ajax({
      url: "/data/2?format=datatable",
      dataType:"json",
  }).done(function (data) {
    (new google.visualization.PieChart(document.getElementById('g2')))
        .draw(new google.visualization.DataTable(data),
            {
              title: "App Usage",
              titleTextStyle: { fontSize: 20 },
              chartArea:{left:"10%", top:"10%", width:"90%", height:"90%"},
              sliceVisibilityThreshold: 1.0/120
            });
  })

  $.ajax({
      url: "/data/3?format=datatable",
      dataType:"json",
  }).done(function (data) {
    (new google.visualization.ColumnChart(document.getElementById('g3')))
        .draw(new google.visualization.DataTable(data),
            {
              title: "Zips Visited In a Day",
              titleTextStyle: { fontSize: 20 },
              legend: { position: "none" },
              vAxis: {title: "Number of Device Days"},
              hAxis: {title: "Zips Visited"},
              chartArea:{width:"70%", height:"70%"},
            });
  })
}
google.setOnLoadCallback(drawCharts);

