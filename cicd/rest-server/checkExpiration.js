/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/
const Errors = require('./app/catchError.js');
const db = require('./app/db.js');
const constDef = require('./app/const.js');
const config = require('./config/config.js');
const log = require('./config/log.js')();
const model = require('./app/model.js');

let type_table = constDef.type_table;
let time2add = 5;


async function checkExpiration() {
  try {
    let notify_time_arr = [10, 4, 1, 0], result = {};
    for (let notify_t of notify_time_arr) {
      result = db.runGsqlQuery('checkExpirationV2', {"time_remain": notify_t});
      for (let job of result) {
        let job_type = job['v_type'], job_id = job['job_id'], job_log = job['log_dir'];
        let user_name_list = job['@user_name']
        if (user_name_list == undefined || job_type == undefined || job_id == undefined || job_id == "") continue;
        let user_name = job['@user_name'][0];
        let mwh = (await constDef.requestServer('/' + job_type + '/' + job_id + '/mwh_request', 'GET'))['result'];
        if (mwh.length != 1 || mwh[0].job_id == undefined) continue;
        let job_url = 'http://' + config.jenkins_config['jenkins_ip'] + ':'
            + config.jenkins_config['jenkins_port']
            + '/job/' + type_table[job_type]['jenkinsName'] + '/' + job_id;
        let master_job_url = 'http://' + config.jenkins_config['jenkins_ip'] + ':'
          + config.jenkins_config['jenkins_port']
          + '/job/' + type_table[mwh[0].job_type]['jenkinsName'] + '/' + mwh[0].job_id;    
        let job_name = '[' + job_type + '#' + job_id + '](' + job_url + 
          ') of [' + mwh[0].job_type + '#' + mwh[0].job_id + '](' + master_job_url + ')';
        let renew_url = 'http://' + config.jenkins_config.rest_server_address
            + '/api/' + job_type + '/' + job_id + '/renew/' + time2add + 'h?user=' + user_name;
        let return_url = 'http://' + config.jenkins_config.rest_server_address
            + '/api/' + job_type + '/' + job_id + '/reclaim?user=' + user_name;
        let comment = 'The debugging job ' + job_type + '#' + job_id + ' will be expired after '
            + notify_t + ' hours. You can click [here](' + renew_url
            + ') to renew ' + time2add + ' hours.';
        comment += ' And you can click [here](' + return_url +
            ') to return the cluster(take it online) immediately.'
        if (notify_t == 0) {
          await constDef.requestServer('/' + job_type + '/' + job_id, 'PUT', {
            debug_status: false
          });
          await constDef.requestServer('/' + job_type + '/' + job_id + '/reclaim?force=true&user=' + user_name, 'GET');
          comment = 'The debugging job ' + job_type + '#' + job_id + ' is expired';
        }
        await model.User.NotifyUser(user_name, job_name, job_url, comment);
      }
    }
  } catch (err) {
    log('check expiration failed');
    log(err);
  }
}

setInterval(checkExpiration, config.checkExpirInterval);
