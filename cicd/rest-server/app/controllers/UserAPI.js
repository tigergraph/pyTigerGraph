/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

const model = require('../model.js');
const constDef = require('../const.js');
const Errors = require('../catchError.js');
const config = require('../../config/config.js');
const db = require('../db.js');
const log = require('../../config/log.js')();
const util = require('util');

let PATHPREFIX = constDef.PATHPREFIX;
let url = PATHPREFIX + '/users';

let instance = model.User;
let exp = {};


// basic internal data operation API

exp['DELETE ' + url + '/:user_name'] = async (ctx, next) => {
  let user_name = ctx.params.user_name;
  try {
    log('Delete user by name ' + user_name + ' API')
    ctx.rest(await instance.deletebyName(user_name));
  } catch (err) {
    throw new Errors.APIError('Delete user by name ' + user_name + 'API failed', err, 500);
  }
};


exp['POST ' + url] = async (ctx, next) => {
  let body = ctx.request.body;
  try {
    log('Add users API:');
    if (config.debug == true) {
      log(body);
    }
    ctx.rest(await instance.upsertUsers(body));
  } catch (err) {
    throw new Errors.APIError('Add users API failed', err, 500);
  }
};

exp['PUT ' + url + '/:user_name'] = async (ctx, next) => {
  let user_name = ctx.params.user_name, body = ctx.request.body;
  try {
    log('Update user ' + user_name + ' API:');
    if (config.debug == true) {
      log(body);
    }
    // update is not allowed if the vertice does not exist
    await instance.getByName(user_name);
    body['user_name'] = user_name;
    ctx.rest(await instance.upsertUsers(body));
  } catch (err) {
    throw new Errors.APIError('Update user ' + user_name + ' API failed', err, 500);
  }
};

exp['GET ' + url] = async (ctx, next) => {
  try {
    log('GET users API');
    ctx.rest(await instance.getUsers());
  } catch (err) {
    throw new Errors.APIError('GET users API failed', err, 500);
  }
};


exp['GET ' + url + '/:user_name'] = async (ctx, next) => {
  let user_name = ctx.params.user_name;
  try {
    log('GET user by name ' + user_name + ' API');
    ctx.rest(await instance.getByName(user_name));
  } catch (err) {
    throw new Errors.APIError('GET user by name ' + user_name + ' API failed', err, 500);
  }
};

// lookup API

exp['GET ' + url + '/:user_name/mwh_request'] = async (ctx, next) => {
  let modelName = 'user', user_name = ctx.params.user_name, payload = ctx.query;
  try {
    log('GET mwh_request for user ' + user_name + ' API');
    let requests = await db.getConnectedVertices(modelName, payload,{
      from_id: user_name,
      edge_name: 'user_request_info',
      to_name: 'mwh_request'
    });
    ctx.rest(requests);
  } catch (err) {
    throw new Errors.APIError('GET mwh_request for user ' + user_name + ' API failed', err, 500);
  }
};

exp['GET ' + url + '/:user_name/activity'] = async (ctx, next) => {
  let user_name = ctx.params.user_name, job_type = ctx.query.job_type, job_id=ctx.query.job_id;
  try {
    if (job_type == undefined) {
      job_type = 'all';
    }
    log('GET user ' + user_name + ' activity API');
    let query_res = db.runGsqlQuery('getUserAction', {"the_user": user_name, "j_type": job_type});
    let running_str = "Your running mit/wip :\n", debugging_str = "Your debugging testing jobs :\n";
    let specified_job_str = "";
    for (let v_set of query_res) {
      if (v_set['running_mwh'] != undefined) {
        for (let new_job of v_set['running_mwh']) {
          if (new_job['attributes'] == undefined) continue;
          let job = new_job['attributes'];
          let job_str = job["job_type"] + "#" + job["job_id"] + ":\n";
          let summary = (await constDef.requestServer('/' + job["job_type"] + '/' +
              job["job_id"] + '/summary?stages="running passed failed"'))['result'];
          job_str += summary;
          running_str += job_str;
          if (job_id != undefined && job_id == job["job_id"]) {
            specified_job_str = job_str;
          }
        }
      } else if (v_set['debugging_jobs_with_info'] != undefined) {
        for (let new_job of v_set['debugging_jobs_with_info']) {
          if (new_job['attributes'] == undefined) continue;
          let job = new_job['attributes'];
          let mwh = (await constDef.requestServer('/' + new_job['v_type'] + '/' + job['job_id'] + '/mwh_request', 'GET'))['result'];
          if (mwh.length != 1 || mwh[0].job_id == undefined) {
            throw new Errors.APIError('no mwh_request for ' + new_job['v_type'] + ' ' + job['job_id'], err, 500);
          }
          debugging_str += util.format("%s %s of %s %s on %s, will expire at %s\n", new_job['v_type'],
              job['job_id'], mwh[0].job_type, mwh[0].job_id, job['@node_name'].join(', '), job['debug_end']);
        }
      }
    }
    let result = job_id == undefined ? running_str + "\n" + debugging_str : specified_job_str;
    ctx.rest(result);
  } catch (err) {
    throw new Errors.APIError('GET user ' + user_name + ' activity API failed', err, 500);
  }
};


exp['GET ' + url + '/:user_name/reclaim'] = async (ctx, next) => {
  let user_name = ctx.params.user_name;
  try {
    log('GET user ' + user_name + ' activity API');
    let query_res = db.runGsqlQuery('getUserAction', {"the_user": user_name});
    for (let v_set of query_res) {
      if (v_set['debugging_jobs_with_info'] != undefined) {
        for (let new_job of v_set['debugging_jobs_with_info']) {
          if (new_job['attributes'] == undefined) continue;
          let job = new_job['attributes'];
          let url2req = '/' + new_job["v_type"] + '/' + job["job_id"] + '/reclaim?user=' + user_name;
          await constDef.requestServer(url2req, 'GET');
        }
      }
    }
    ctx.rest("All jobs are returned");
  } catch (err) {
    throw new Errors.APIError('GET user ' + user_name + ' activity API failed', err, 500);
  }
};

exp['GET ' + url + '/:user_name/register'] = async (ctx, next) => {
  let modelName = 'user', user_name = ctx.params.user_name, work_loc = ctx.query.loc;
  if (!work_loc) {
    work_loc = "US";
  }
  try {
    log('Register user ' + user_name + ' API');
    let user_vid='unknown';
    let query_res = db.runGsqlQuery('addUser', {"user_name": user_name, "email": user_name + '@tigergraph.com', "work_loc": work_loc});
    for (let v_set of query_res) {
      if (v_set['name'] != undefined) {
         for (let new_job of v_set['name']) {
            if (new_job['attributes'] == undefined) continue;
            user_vid=new_job['v_id'];
        }
      }
    }
    ctx.rest(user_vid);
  } catch (err) {
    throw new Errors.APIError('Register user ' + user_name + ' API failed', err, 500);
  }
};

exp['GET ' + url + '/:user_name/checkThrottle'] = async (ctx, next) => {
  let modelName = 'user', user_name = ctx.params.user_name;
  try {
    let job_type = "mit wip", opCount = 0;
    log('Check throttle for user ' + user_name + ' API');
    let query_res = db.runGsqlQuery('getUserAction', {"the_user": user_name, "j_type": job_type});
    for (let v_set of query_res) {
      if (v_set['running_mwh'] != undefined) {
         for (let new_job of v_set['running_mwh']) {
            if (new_job['attributes'] == undefined) continue;
            opCount += 1;
        }
      }
      if (v_set['debugging_jobs_with_info'] != undefined) {
        for (let new_job of v_set['debugging_jobs_with_info']) {
            if (new_job['attributes'] == undefined) continue;
            opCount += 1;
        }
      }
    }
    ctx.rest(opCount);
  } catch (err) {
    throw new Errors.APIError('Check throttle for user ' + user_name + ' API failed', err, 500);
  }
};


module.exports = exp;
