/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

const moment = require('moment');
const request = require('request');
const sps = require('child_process').spawnSync;

const config = require('../config/config.js');
const Errors = require('./catchError.js');

let exp = {};

exp.getCurrentTime = () => {
  return moment(new Date(Date.now())).format().substring(0, 19);
};

exp.nameLogWithTime = (log_name) => {
  return log_name.slice(0, -4) + '_' + exp.getCurrentTime() + '_' + '.log';
}

const log = require('../config/log.js')(config.log, exp.nameLogWithTime(config.log));

exp.PATHPREFIX = '/api';
exp.LONGLONG_AGO = '1994-01-01T01:01:01.111Z';
exp.LONGLONG_LATER = '2050-02-02T02:02:02.222Z';

exp.AllModels = ['Pipeline', 'User', 'Node'];

//online config
exp.type_table = {
  'mit': {
    'modelName': 'mwh_request',
    'jenkinsName': 'mit_test'
  },
  'wip': {
    'modelName': 'mwh_request',
    'jenkinsName': 'wip_test'
  },
  'hourly': {
    'modelName': 'mwh_request',
    'jenkinsName': 'hourly_test'
  },
  'build_job': {
    'modelName': 'build_job',
    'jenkinsName': 'build_test'
  },
  'test_job': {
    'modelName': 'test_job',
    'jenkinsName': 'parallel_test'
  }
}


exp.validateTimeRange = (start, end) => {
  start = start !== undefined ? start : exp.LONGLONG_AGO;
  end = end !== undefined ? end : exp.LONGLONG_LATER;
  if (!moment(start, 'YYYY-MM-DDTHH:mm:SS.sssZ', true).isValid() ||
      !moment(end, 'YYYY-MM-DDTHH:mm:SS.sssZ', true).isValid()) {
    throw new Errors.APIError('Invalid request parameters time range', {}, 400);
  }
  return { start: start, end: end };
};

exp.validateTrigger = (trigger) => {
  if (trigger !== 'true' && trigger !== 'false' && trigger !== '1' && trigger !== '0') {
    throw new Errors.APIError('Invalid request parameters bool trigger', {}, 400);
  }
  return trigger === 'true' || trigger === '1';
};

exp.spawnSync = (cmd, cmdOp) => {
  let log_str = 'spawnSync command: ' + cmd + ' ';
  for (let op of cmdOp) {
    log_str += ' ' + op;
  }
  log(log_str);
  let output = sps(cmd, cmdOp, { encoding: 'utf-8' }).output;
  return output[1];
}

exp.setPayload = (obj) => {
  for (let key in obj) {
    if (obj[key] == undefined || obj[key] == null) {
      delete obj[key];
    }
  }
  return obj;
}

exp.handelQueryOutput = (arr) => {
  if (!Array.isArray(arr)) {
    arr = [arr];
  }
  for (let obj of arr) {
    for (let key in obj) {
      if (key.startsWith("@")) {
        let new_key = key.replace(/^@+/g, '');
        obj[new_key] = obj[key];
        delete obj[key];
      }
    }
  }
  return arr;
}

exp.setAttrForRestpp = (obj) => {
  let new_obj = {}
  for (let key in obj) {
    new_obj[key] = { "value": obj[key] };
  }
  return new_obj;
}

exp.requestUrl = (request_url, method, data = undefined) => {
  request_url = encodeURI(request_url);
  log("Send requests. " + method + " url: " + request_url);
  let opt = {
    headers: {'content-type':'application/json'},
    method: method,
    url: request_url,
    json: true
  };
  if (data != undefined) {
    opt['body'] = data;
  }
  return new Promise((resolve, reject) => {
    request(opt, function(error, response, body) {
      if (error) {
        log(error);
        reject();
      } else {
        resolve(body);
      }
    });
  });
}

exp.requestServer = (urlBody, method, data = undefined) => {
  let request_url = 'http://localhost:' + config.serverPort + '/api' + urlBody;
  log("To requestServer. " + method + " url: " + request_url);
  return exp.requestUrl(request_url, method, data);
}
module.exports = exp;
