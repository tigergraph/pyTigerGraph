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
const db = require('../db.js');
const config = require('../../config/config.js');
const log = require('../../config/log.js')();
const spawnSync = constDef.spawnSync;

let PATHPREFIX = constDef.PATHPREFIX;
let url = PATHPREFIX + '/nodes';

let instance = model.Node;
let exp = {};

// basic internal data operation API

exp['DELETE ' + url] = async (ctx, next) => {
  let payload = ctx.query
  try {
    log('DELETE nodes API');
    ctx.rest(await instance.deleteNodes(payload));
  } catch (err) {
    throw new Errors.APIError('DELETE nodes API failed', err, 500);
  }
};

exp['DELETE ' + url + '/:nodeName'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName;
  try {
    log('Delete node by name ' + nodeName + ' API');
    ctx.rest(await instance.deletebyName(nodeName));
  } catch (err) {
    throw new Errors.APIError('Delete node by name ' + nodeName + ' API failed', err, 500);
  }
};

exp['POST ' + url] = async (ctx, next) => {
  let body = ctx.request.body;
  try {
    log('Add nodes API:');
    if (config.debug == true) {
     log(body);
    }
    ctx.rest(await instance.upsertNodes(body));
  } catch (err) {
    throw new Errors.APIError('Add nodes API failed', err, 500);
  }
};

exp['PUT ' + url + '/:nodeName'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName, body = ctx.request.body;
  try {
    log('Update node ' + nodeName + ' API:');
    if (config.debug == true) {
     log(body);
    }
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    body['node_name'] = nodeName;
    ctx.rest(await instance.upsertNodes(body));
  } catch (err) {
    throw new Errors.APIError('Update node ' + nodeName + ' API failed', err, 500);
  }
};

exp['GET ' + url] = async (ctx, next) => {
  let payload = ctx.query;
  try {
    log('GET nodes API');
    ctx.rest(await instance.getNodes(payload));
  } catch (err) {
    throw new Errors.APIError('GET nodes API failed', err, 500);
  }
};

exp['GET ' + url + '/:nodeName'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName;
  try {
    log('GET node by name/ip ' + nodeName + ' API');
    if (nodeName.indexOf("_") == -1) {
      ctx.rest(await instance.getByIp(nodeName));
    } else {
      ctx.rest(await instance.getByName(nodeName));
    }
  } catch (err) {
    throw new Errors.APIError('GET node by name/Ip ' + nodeName + ' API failed', err, 500);
  }
};

// lookup API


exp['GET ' + url + '/:nodeName/build_job'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName, modelName = 'slave_node', payload = ctx.query;
  try {
    log('GET build job for ' + nodeName + ' API');
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    let jobs = await db.getConnectedVertices(modelName, payload, {
      from_id: nodeName,
      edge_name: 'build_node_info',
      to_name: 'build_job'
    });
    ctx.rest(jobs);
  } catch (err) {
    throw new Errors.APIError('GET build job for ' + nodeName + ' API failed', err, 500);
  }
};


exp['GET ' + url + '/:nodeName/test_job'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName, modelName = 'slave_node', payload = ctx.query;
  try {
    log('GET test job for ' + nodeName + ' API');
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    let jobs = await db.getConnectedVertices(modelName, payload, {
      from_id: nodeName,
      edge_name: 'test_node_info',
      to_name: 'test_job'
    });
    ctx.rest(jobs);
  } catch (err) {
    throw new Errors.APIError('GET test job for ' + nodeName + ' API failed', err, 500);
  }
};

// action API

exp['PUT ' + url + '/:nodeName/takeOffline'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName, body = ctx.request.body;
  try {
    log('Take node ' + nodeName + ' offline API:');
    if (config.debug == true) {
      log(body);
     }
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    let msg = body['offline_message'], log_dir = body['log_dir'];
    ctx.rest(await instance.upsertNodes({
      'node_name': nodeName,
      'status': 'offline',
      'offline_message': msg
    }));
  } catch (err) {
    throw new Errors.APIError('Take node ' + nodeName + ' offline API failed', err, 500);
  }
};

exp['PUT ' + url + '/:nodeName/takeOnline'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName, body = ctx.request.body;
  try {
    log('Take node ' + nodeName + ' online API');
    if (config.debug == true) {
      log(body);
    }
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    let log_dir = body['log_dir'];
    let version = ctx.query.version == undefined ? 'TigerGraph' : ctx.query.version;
    // syntax for spawnSync is spawnSync(command[, comand_args][, spawnSync_options]).
    spawnSync('git', ['reset', 'HEAD', '--hard']);
    spawnSync('git', ['pull']);
    if (nodeName.startsWith('k8s-')) {
      let pool_tag = (nodeName.includes('-ext')) ? nodeName.split("-")[1] : 'default'
      spawnSync('python3', [config.python3_script + '/clean_k8s_pods.py', log_dir, version, pool_tag, nodeName]);
    }
    let node_status = nodeName.includes('k8s-') ? 'deleted': 'online'
    ctx.rest(await instance.upsertNodes({
      'node_name': nodeName,
      'status': node_status
    }));
  } catch (err) {
    throw new Errors.APIError('Take node ' + nodeName + ' online API failed', err, 500);
  }
};

exp['GET ' + url + '/:nodeName/renew/:time'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName, add_time = ctx.params.time;
  try {
    log('Renew node ' + nodeName + ' with time ' + add_time + ' API:');
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    let result = db.runGsqlQuery('getDebugJob4Node', {"node_name": nodeName});
    if (result.length != 1 || result[0]['debugging_jobs'][0]['attributes']['job_id'] == undefined) {
      throw new Errors.APIError('query getDebugJob4Node result err: ' + JSON.stringify(result), {}, 500);
    }
    let job_id = result[0]['debugging_jobs'][0]['attributes']['job_id'], job_type = result[0]['debugging_jobs'][0]['v_type'];
    let url2req = '/' + job_type + '/' + job_id + '/renew/' + add_time;
    if (ctx.query.user != undefined) {
      url2req += '?user=' + ctx.query.user;
    }
    ctx.rest(await constDef.requestServer(url2req, 'GET'));
  } catch (err) {
    throw new Errors.APIError('Renew node ' + nodeName + ' with time ' + add_time + ' API failed', err, 500);
  }
};

exp['GET ' + url + '/:nodeName/reclaim'] = async (ctx, next) => {
  let nodeName = ctx.params.nodeName;
  try {
    log('Reclaim node ' + nodeName + ' API:');
    // "nodeName" might be ip, get true nodeName
    nodeName = await getNameByIp(nodeName);
    let result = db.runGsqlQuery('getDebugJob4Node', {"node_name": nodeName});
    if (result.length != 1 || result[0]['debugging_jobs'].length == 0) {
      throw new Errors.APIError('query getDebugJob4Node result err: ' + JSON.stringify(result), {}, 500);
    }
    let job_id = result[0]['debugging_jobs'][0]['attributes']['job_id'], job_type = result[0]['debugging_jobs'][0]['v_type'];
    let url2req = '/' + job_type + '/' + job_id + '/reclaim';
    if (ctx.query.user != undefined) {
      url2req += '?user=' + ctx.query.user;
    }
    ctx.rest(await constDef.requestServer(url2req, 'GET'));
  } catch (err) {
    throw new Errors.APIError('Reclaim node ' + nodeName + ' API failed', err, 500);
  }
};

async function getNameByIp(nodeName) {
  if (nodeName.indexOf("_") == -1 && nodeName.indexOf("-") == -1) {
    // it is node Ip
    let this_node = await instance.getByIp(nodeName);
    if (this_node.length != 1 || this_node[0]['node_name'] == undefined) {
      throw new Errors.APIError('can not find the node by ip ' + nodeName, {}, 500);
    }
    return this_node[0]['node_name'];
  }
  return nodeName;
}

module.exports = exp;
