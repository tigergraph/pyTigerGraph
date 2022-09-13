/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

const yaml = require('js-yaml');
const fs = require('fs');
const send = require('koa-send');

const config = require('../../config/config.js');
const model = require('../model.js');
const constDef = require('../const.js');
const Errors = require('../catchError.js');
const log = require('../../config/log.js')();
const db = require('../db.js');

const spawnSync = constDef.spawnSync;

let PATHPREFIX = constDef.PATHPREFIX;
let exp = {};

exp['GET ' + PATHPREFIX + '/version'] = async (ctx, next) => {
  ctx.rest({
    version: config.version
  });
};

exp['GET ' + PATHPREFIX + '/timecost'] = async (ctx, next) => {
  try {
    log('Get timecost webpage API');
    await send(ctx, 'timecost.html', { root: __dirname + '/../../static' });
  } catch (err) {
    throw new Errors.APIError('Get timecost webpage API failed', err, 500);
  }
};

// exp['GET ' + PATHPREFIX + '/release/:branch'] = async (ctx, next) => {
//   let branch = ctx.params.branch;
//   try {
//     log('Get release branch API');
//     let pipelines = db.runGsqlQuery('getReleaseInfo', {"branch": branch});
//     log(pipelines);
//     let res = [];
//     for (let pipeline of pipelines) {
//       let obj = {};
//       obj['mit_id'] = pipeline['job_id'];
//       obj['user_name'] = pipeline['@user_name'][0];
//       obj['start_t'] = pipeline['start_t'];
//
//       res.push(obj);
//     }
//   } catch (err) {
//     throw new Errors.APIError('Get release branch API failed', err, 500);
//   }
// };

exp['GET ' + PATHPREFIX + '/files/:filename'] = async (ctx, next) => {
  let filename = ctx.params.filename;
  try {
    log('Get static file ' + filename + ' API');
    await send(ctx, filename, { root: __dirname + '/../../static' });
  } catch (err) {
    throw new Errors.APIError('Get static file failed', err, 500);
  }
};

exp['POST ' + PATHPREFIX + '/jenkins/:jobname'] = async (ctx, next) => {
  let jobname = ctx.params.jobname;
  let body = ctx.request.body;
  try {
    log('Trigger Jenkins ' + jobname + ' API:');
    if (config.debug == true) {
      log(body);
    }
    let base_refs = body['base_ref'].split("/")
    if (base_refs.length < 3) {
      throw new Errors.APIError('base ref format error: ' + body['base_ref'], err, 500);
    }
    let branch_name = base_refs[2];
    let job_url = 'http://'
        + config.jenkins_config['jenkins_account'] + ':' + config.jenkins_config['jenkins_pwd']
        + '@'
        + config.jenkins_config['jenkins_ip'] + ':' + config.jenkins_config['jenkins_port']
        + '/job/' + jobname + '/buildWithParameters'
        + '?branch_name=' + branch_name;
    await constDef.requestUrl(job_url, 'POST', {});
    ctx.rest("Trigger jenkins starts");
  } catch (err) {
    throw new Errors.APIError('Trigger Jenkins ' + jobname + ' API failed', err, 500);
  }
};

exp['GET ' + PATHPREFIX + '/detail'] = async (ctx, next) => {
  let start_t = ctx.query.start_t, end_t = ctx.query.end_t;
  let num_hours = ctx.query.num_hours, j_type = ctx.query.j_type;
  let inc_customized = ctx.query.inc_customized, num_of_nodes = ctx.query.num_of_nodes;
  let queryData = {"work_loc": "all"};

  // set default type
  if (j_type == undefined) {
    j_type = "mit,wip";
  }
  if (inc_customized != undefined) {
    queryData["inc_customized"] = inc_customized;
  }
  if (num_of_nodes != undefined) {
    queryData["num_of_nodes"] = num_of_nodes;
  }

  // the date range is specified
  if (start_t != undefined && end_t != undefined) {
    num_hours = 0;
    queryData["start_t"] = start_t;
    queryData["end_t"] = end_t;
  } else if (num_hours == undefined) {
    // default num_hours is two week
    num_hours = 14 * 24;
  }
  queryData["num_hours"] = num_hours;
  queryData["j_type"] = j_type;

  try {
    let res = "The max/min/avg duration of wip/mit:\n";
    res += "(The end2end cost is the user end2end duration)\n";
    res += "(The real cost is the end2end cost minus the time blocking by QA hourly ticket)\n";
    res += "(The running cost is the time : max testing duration + build duration)\n";
    let cost_summary = db.runGsqlQuery('getTimeCost', queryData);
    let total_count = 0, succeed_count = 0, ut_dict = {};
    let it_dict = {
      it_total_cost: 0.0,
      it_max_cost: 0.0,
      it_min_cost: Number.MAX_VALUE,
      it_count: 0,
      failed_it_count: 0
    };
    for (let cost of cost_summary) {
      let cost_name = Object.keys(cost)[0];
      if (cost["mwh_with_info"] == undefined && cost["failed_mwh"] == undefined) {
        res += cost_name.replace(/^@+/g, '') + " : " + cost[cost_name];
        if (!cost_name.includes("count")) {
          res += " min";
        }
        res += "\n";
      } else {
        await calComponentTimeCost(cost, ut_dict, it_dict);
      }
    }
    let it_avg_cost = 0;
    if (it_dict["it_count"] != 0) {
      it_avg_cost = + (it_dict["it_total_cost"] / it_dict["it_count"]).toFixed(1);
    }
    res += "\nThe avg/max/min duration of total integration test in mit/wip:\n";
    res += "average_integration_test : " + it_avg_cost + " min\n";
    res += "maximum_integration_test : " + it_dict["it_max_cost"].toFixed(1) + " min\n";
    res += "minimum_integration_test : " + it_dict["it_min_cost"].toFixed(1) + " min\n";
    res += "total_integration_count : " + (it_dict["it_count"] + it_dict["failed_it_count"]) + "\n";
    res += "passed_integration_count : " + it_dict["it_count"] + "\n";

    res += "\nThe avg/max/minduration of each unit test in mit/wip:\n";
    for (let ut_name in ut_dict) {
      let ut_avg_cost = 0.0, ut_min_cost = Number.MAX_VALUE, ut_max_cost = 0.0;
      for (let ut_cost of ut_dict[ut_name].test) {
        ut_max_cost = Math.max(ut_max_cost, ut_cost);
        ut_min_cost = Math.min(ut_min_cost, ut_cost);
        ut_avg_cost += ut_cost;
      }
      if (ut_dict[ut_name].test.length != 0) {
        ut_avg_cost = + (ut_avg_cost / ut_dict[ut_name].test.length).toFixed(1);
      }
      // no ut data found
      if (ut_min_cost == Number.MAX_VALUE) {
        ut_min_cost = 0;
      }
      res += ut_name + " :\n" + "average_cost : " + ut_avg_cost + " min\n"
            + "minimum_cost : " + ut_min_cost + " min\n"
            + "maximum_cost : " + ut_max_cost + " min\n"
            + "total_ut_count : " + (ut_dict[ut_name].test.length + ut_dict[ut_name].failed_num) + "\n"
            + "passed_ut_count : " + ut_dict[ut_name].test.length + "\n\n";
    }
    ctx.rest(res);
  } catch (err) {
    throw new Errors.APIError('Get system detail API failed', err, 500);
  }
};

async function readFileTotal(summary_file, ut_dict = undefined) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(summary_file)) {
      resolve([0.0, 0]);
      return;
    }
    fs.readFile(summary_file, 'utf8', (err, data) => {
      if (err) {
        log(err);
        reject();
      }
      let total = 0.0, failed_it_count = 0;
      for (let line of data.split('\n')) {
        if (!line.includes("min") || line.includes("uncompleted") || line.includes("running")) continue;
        let ut_name = line.split(' ')[0], timecost = parseFloat(line.split(' ')[1]);
        if (ut_name == "Total") {
          total += +(timecost.toFixed(1));
        } else if (ut_dict != undefined) {
          // it is integration test summary
          if (ut_dict[ut_name] == undefined) {
            ut_dict[ut_name] = {
              "test": [],
              "failed_num": 0
            };
          }
          if (line.includes("failed")) {
            ut_dict[ut_name].failed_num += 1;
          } else {
            ut_dict[ut_name].test.push(timecost);
          }
        }
      }
      total = +(total.toFixed(1));
      resolve([total, data.includes("(failed)")]);
    });
  });
}


async function calComponentTimeCost(cost, ut_dict, it_dict) {
  for (let new_job of cost) {
    if (new_job['attributes'] == undefined) continue;
    let job = new_job['attributes'];
    if (job["log_dir"] == "") continue;
    let log_dir = job["log_dir"];
    let ut_summary = log_dir + '/unit_test_summary';
    let it_summary = log_dir + '/integration_test_summary';
    if (!fs.existsSync(ut_summary) || !fs.existsSync(it_summary)) {
      spawnSync('bash', [config.jenkins_script + '/shell_script/conclude_summary.sh', log_dir]);
    }
    await readFileTotal(ut_summary, ut_dict);

    if (job["integrations"] == "default") {
      let it_arr = await readFileTotal(it_summary, undefined);
      if (cost["failed_mwh"] == undefined && it_arr[0] > 0) {
        it_dict["it_max_cost"] = Math.max(it_dict["it_max_cost"], it_arr[0]);
        it_dict["it_min_cost"] = Math.min(it_dict["it_min_cost"], it_arr[0]);
        it_dict["it_total_cost"] += it_arr[0];
        it_dict["it_count"] += 1;
      } else {
        it_dict["failed_it_count"] += it_arr[1];
      }
    }
  }
}

module.exports = exp;
