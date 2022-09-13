window.chartColors = {
  red: 'rgb(255, 99, 132)',
  orange: 'rgb(255, 159, 64)',
  yellow: 'rgb(255, 205, 86)',
  green: 'rgb(75, 192, 192)',
  blue: 'rgb(54, 162, 235)',
  purple: 'rgb(153, 102, 255)',
  grey: 'rgb(201, 203, 207)'
};

let chartOption = {
  spanGaps: true,
  responsive: true,
  title:{
    display:true,
    text:'MIT daily timecost',
    fontSize: 25,
    padding: 30
  },
  tooltips: {
    mode: 'index',
    intersect: false,
  },
  hover: {
    mode: 'nearest',
    intersect: true
  },
  scales: {
    xAxes: [{
      display: true,
      scaleLabel: {
        display: true,
        labelString: 'Date'
      },
    }],
    yAxes: [{
      position: "left",
      id: "duration",
      display: true,
      scaleLabel: {
        display: true,
        labelString: 'Duration (min)'
      },
      ticks: {
        beginAtZero: true
      }
    }, {
      position: "right",
      id: "count",
      display: true,
      scaleLabel: {
        display: true,
        labelString: 'count'
      },
      ticks: {
        beginAtZero: true
      }
    }]
  }
};


function requestServer(url, method, data = undefined) {
  return new Promise((resolve, reject) => {
    let xhr = new XMLHttpRequest(url);
    xhr.open(method, url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState === 4 && xhr.status === 200) {
        let res = JSON.parse(xhr.responseText);

        if (res.error == true) {
          reject("request " + url + " failed. result is " + JSON.stringify(res));
        }
        resolve(res);
      }
    }
    if (data != undefined) {
      xhr.send(JSON.stringify(data));
    } else {
      xhr.send();
    }
  });
}


function getTimeCost(start_t, end_t, j_type, num_of_nodes, inc_customized = true) {
  // "http://192.168.55.21:8888/api/" +
  let url = "detail?start_t=" + start_t
      + "&end_t=" + end_t + "&j_type=" + j_type + "&num_of_nodes=" + num_of_nodes
      + "&inc_customized=" + inc_customized;
  return requestServer(encodeURI(url), 'GET', undefined);
}


function init_config(name, legend, type) {
  let option = JSON.parse(JSON.stringify(chartOption));
  // option init
  option["title"]["text"] = name + " Duration";
  let left_y_ticks = option["scales"]["yAxes"][0]["ticks"],
      right_y_ticks = option["scales"]["yAxes"][1]["ticks"];
  if (type == "system") {
    left_y_ticks["stepSize"] = 60;
    right_y_ticks["stepSize"] = 3;
    if (name == "hourly") {
      left_y_ticks["stepSize"] = 180;
      right_y_ticks["stepSize"] = 1;
    }
  }
  if (type == "component") {
    left_y_ticks["stepSize"] = 10;
    right_y_ticks["stepSize"] = 1;
    if (name == "integration_test") {
      left_y_ticks["stepSize"] = 60;
      right_y_ticks["stepSize"] = 2;
    }
  }
  left_y_ticks["max"] = left_y_ticks["stepSize"] * 10;
  right_y_ticks["max"] = right_y_ticks["stepSize"] * 10;

  let config = {
    type: 'line',
    data: {
      labels: [],
      datasets: []
    },
    options: option
  };

  // config init
  config["data"]["labels"] = window.myData.x_axis;
  config["data"]["datasets"] = [{
    d_index: 0,
    d_name: "total_count",
    label: "total count",
    yAxisID: "count",
    backgroundColor: window.chartColors.blue,
    borderColor: window.chartColors.blue
  }, {
    d_index: 1,
    d_name: "passed_count",
    label: "passed count",
    yAxisID: "count",
    backgroundColor: window.chartColors.purple,
    borderColor: window.chartColors.purple
  }, {
    d_index: 2,
    d_name: "end2end_duration",
    label: "end2end wall-clock time",
    yAxisID: "duration",
    backgroundColor: window.chartColors.red,
    borderColor: window.chartColors.red
  }, {
    d_index: 3,
    d_name: "running_duration",
    label: "max testing duration + build duration",
    yAxisID: "duration",
    backgroundColor: window.chartColors.yellow,
    borderColor: window.chartColors.yellow
  }];
  if (type == "component") {
    config["data"]["datasets"].splice(3, 1);
  }
  config["data"]["datasets"].forEach(function(dataset) {
    dataset["data"] = [];
    dataset["fill"] = false;
    dataset["cubicInterpolationMode"] = "monotone";
    dataset["hidden"] = !dataset["d_name"].endsWith(legend);
    if (window.myLine[type][name] != undefined) {
      window.myLine[type][name].chart.getDatasetMeta(dataset["d_index"])["hidden"]
          = !dataset["d_name"].endsWith(legend);
    }
  });
  return config;
}

function getPipelineData(stepSize, start_t, end_t = undefined) {
  end_t = end_t == undefined ? moment() : moment(end_t);
  let promise_arr = [];
  window.myData.x_axis = [];
  for (let date_t = moment(start_t), next_t; date_t.diff(end_t) < 0; date_t = next_t) {
    next_t = moment(date_t).add(parseInt(stepSize), "day");
    if (next_t.diff(end_t) > 0) {
      next_t = end_t;
    }
    let date_t_str = date_t.format("YYYY-MM-DD");
    let next_t_str = next_t.format("YYYY-MM-DD");
    window.myData.x_axis.push([date_t.format("YY/MM/DD"), " | ", next_t.format("YY/MM/DD")]);
    let index = window.myData.x_axis.length - 1;

    for (let job_type of ["mit", "wip", "hourly"]) {
      promise_arr.push(new Promise((resolve, reject) => {
        getTimeCost(date_t_str, next_t_str, job_type, -1, true).then((data) => {
          window.myData[job_type][index] = data.result;
          resolve();
        }).catch((reason) => {
          console.log(reason);
          reject(reason);
        });
      }));
    }
  }
  return promise_arr;
}


function parseTimecost(timecost_line) {
  let time_label = timecost_line.split(" ")[0], timecost = timecost_line.split(" ")[2];
  if (timecost == "NaN") {
    timecost = 0;
  }
  timecost = +(parseFloat(timecost).toFixed(3));
  if (!time_label.endsWith("count") && timecost == 0) {
    return null;
  }
  return timecost;
}


function setPipelineChart(job_type, canvas_name, legend) {
  // init config for each chart
  let config = init_config(job_type, legend, "system");

  // set data for each chart
  for (let pipeline_data of window.myData[job_type]) {
    for (let line of pipeline_data.split("\n")) {
      if (line.startsWith("total_count")) {
        config["data"]["datasets"][0]["data"].push(parseTimecost(line));
      } else if (line.startsWith("passed_count")) {
        config["data"]["datasets"][1]["data"].push(parseTimecost(line));
      } else if (line.startsWith("avg_end2end_cost")) {
        config["data"]["datasets"][2]["data"].push(parseTimecost(line));
      } else if (line.startsWith("avg_running_cost")) {
        config["data"]["datasets"][3]["data"].push(parseTimecost(line));
      }
    }
  }

  let ctx = $('#' + canvas_name)[0].getContext("2d");
  if (window.myLine.system[job_type] == undefined) {
    window.myLine.system[job_type] = new Chart(ctx, config);
  } else {
    window.myLine.system[job_type].chart.config = config;
    window.myLine.system[job_type].chart.update();
  }
}


function getComponentData(div_name) {
  let len = window.myData.mit.length;
  for (let pipeline_index in window.myData.mit) {
    let cur_ut = "";
    for (let line of window.myData.mit[pipeline_index].split("\n")) {
      if (!line.includes(":")) continue;
      if (line.split(" ").length == 2) {
        cur_ut = line.split(" ")[0];
      } else if (line.includes("total integration test")) {
        cur_ut = "integration_test";
      }
      if (cur_ut != "" && (line.includes("count") || line.includes("min"))) {
        if (window.myData.unittest[cur_ut] == undefined) {
          window.myData.unittest[cur_ut] = {
            average: Array.from(Array(len), () => null),
            total_count: Array.from(Array(len), () => 0),
            passed_count: Array.from(Array(len), () => 0)
          };
        }
        if (line.startsWith("total")) {
          window.myData.unittest[cur_ut].total_count[pipeline_index] = parseTimecost(line);
        } else if (line.startsWith("passed")) {
          window.myData.unittest[cur_ut].passed_count[pipeline_index] = parseTimecost(line);
        } else if (line.startsWith("average")) {
          window.myData.unittest[cur_ut].average[pipeline_index] = parseTimecost(line);
        }
      }
    }
  }
  if ($('.' + div_name).children().length == 0) {
    $('.' + div_name).empty();
    for (let cur_ut in window.myData.unittest) {
      let canvas_name = "ut_" + cur_ut;
      $('.' + div_name).append("<canvas id='" + canvas_name + "'></canvas>");
    }
  }
}

function setComponentChart(ut_name, canvas_name, legend) {
  // init config for each chart
  //console.log(ut_name);
  let config = init_config(ut_name, legend, "component");

  config["data"]["datasets"][0]["data"] = window.myData.unittest[ut_name].total_count;
  config["data"]["datasets"][1]["data"] = window.myData.unittest[ut_name].passed_count;
  config["data"]["datasets"][2]["data"] = window.myData.unittest[ut_name].average;
  let ctx = $('#' + canvas_name)[0].getContext("2d");
  if (window.myLine.component[ut_name] == undefined) {
    window.myLine.component[ut_name] = new Chart(ctx, config);
  } else {
    window.myLine.component[ut_name].chart.config = config;
    window.myLine.component[ut_name].chart.update();
  }
}
