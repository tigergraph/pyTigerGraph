/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/
const format = require('util').format;

const config = require('../config/config.js');
const log = require('../config/log.js')();
const Errors = require('./catchError.js');
const constDef = require('./const.js');

const spawnSync = constDef.spawnSync;

let exp = {};
let restpp_address='http://' + config.restppServerIp + ':' + config.restppServerPort;

function checkUndefined(field) {
  return field == undefined ? '' : '/' + field;
};

function curlCmd(url, method, data = undefined) {
  try {
    if (method == 'POST' && data) {
      return spawnSync('curl', ['-X', method, '-d', JSON.stringify(data), url]);
    } else {
      return spawnSync('curl', ['-X', method, url]);
    }
  } catch (err) {
    throw new Errors.APIError('curl command failed', err, 500);
  }
}

function getRestppRes(url, method, data = undefined) {
  log('The url sent to restpp is: ' + url);
  log('The payload is:\n' + JSON.stringify(data, null, 2));
  let output = JSON.parse(curlCmd(url, method, data));
  if (config.debug == true) {
    log('The restpp output is: \n' + JSON.stringify(output, null, 2));
  } else {
    log('Got respp output with error:'+ output.error + '\n')
  }
  
  if (output.error == true) {
    throw new Errors.APIError('Restpp result has error: ' + output.message, {}, 500);
  }
  if (Array.isArray(output.results) && output.results.length > 0) {
    let result = [];
    for (let obj of output.results) {
      if (obj.attributes != undefined) {
        let new_obj = obj.attributes;
        if (obj['to_id'] != undefined) {
          new_obj['to_id'] = obj['to_id'];
        }
        result.push(new_obj);
      } else if (obj.v != undefined) {
        let new_obj = obj.v;
        new_obj['v_type'] = obj.v_type;
        new_obj['v_set'] = obj.v_set;
        result.push(new_obj);
      } else {
        result.push(obj);
      }
    }
    return result;
  } else if (output.results != undefined){
    return output.results;
  } else {
    return output;
  }
}

exp.runGsqlQuery = (queryName, payload) => {
  try {
    let url = restpp_address + '/query/' + config.graphName + '/' + queryName;
    if (payload && Object.keys(payload).length > 0) {
      url += '?';
      for (let key in payload) {
        url += key + '=' + payload[key] + '&';
      }
      url = url.slice(0, -1);
    }
    return getRestppRes(url, 'GET');
  } catch (err) {
    throw new Errors.APIError('Run Gsql Query ' + queryName + ' with '
        + JSON.stringify(payload) + ' failed', err, 500);
  }
}

exp.upsertData = (modelName, payload) => {
  try {
    let url = restpp_address + '/graph/' + config.graphName , data_obj = {};
    return getRestppRes(url, 'POST', payload);
  } catch (err) {
    throw new Errors.APIError('Upsert Data to ' + modelName + ' with '
        + JSON.stringify(payload) + ' failed', err, 500);
  }
}

exp.searchDB = (method, modelName, payload, type, edge_info = undefined, sort_way = undefined) => {
  try {
    let url = restpp_address + '/graph/' + config.graphName + '/' + type + '/' + modelName;
    if (type == 'edges') {
      url += '/' + edge_info['from_id'] + checkUndefined(edge_info['edge_name']) +
          checkUndefined(edge_info['to_name']) + checkUndefined(edge_info['to_id']);
    } else if (payload['primary_id'] != undefined) {
      url += checkUndefined(payload['primary_id']);
      delete payload['primary_id'];
    }
    if (payload && Object.keys(payload).length > 0) {
      url += '?filter=';
      for (let key in payload) {
        let val = '%22' + payload[key] + '%22';
        if (typeof payload[key] == 'boolean') {
          val = '' + (payload[key] ? 1 : 0);
        } else if (payload[key] == 'true' || payload[key] == 'false') {
          val = '' + (payload[key] == 'true' ? 1 : 0);
        } else if (typeof payload[key] == 'number' || /^\d+$/.test(payload[key])) {
          val = '' + payload[key];
        }
        url += key + '=' + val + ',';
      }
      url = url.slice(0, -1);
    }
    if (sort_way && Object.keys(sort_way).length > 0) {
      url += '&sort=' + sort_way
    }
    return getRestppRes(url, method);
  } catch (err) {
    throw new Errors.APIError(method + ' data from ' + modelName + ' with '
        + JSON.stringify(payload) + ' failed', err, 500);
  }
}

exp.selectData = (modelName, payload, type, edge_info = undefined, sort_way = undefined) => {
  return exp.searchDB('GET', modelName, payload, type, edge_info, sort_way);
}

exp.deleteData = (modelName, payload, type, edge_info = undefined, sort_way = undefined) => {
  return exp.searchDB('DELETE', modelName, payload, type, edge_info, sort_way);
}

exp.getConnectedVertices = (modelName, payload, edge_info, sort_way = undefined, edge_payload = {}) => {
  try {
    let which_edges = exp.selectData(modelName, edge_payload, 'edges', edge_info, sort_way);
    let connected_vertices = [];
    for (let edge of which_edges) {
      payload['primary_id'] = edge['to_id'];
      let vertices = exp.selectData(edge_info['to_name'], payload, 'vertices', undefined, sort_way);
      for (let vertice of vertices) {
        connected_vertices.push(vertice);
      }
    }
    return connected_vertices;
  } catch (err) {
    throw new Errors.APIError('Get all connected Vertices from '
        + modelName + ' with ' + JSON.stringify(edge_info) + ' failed', err, 500);
  }
}


exp.deleteConnectedVertices = (modelName, payload, edge_info, sort_way = undefined, edge_payload = {}) => {
  try {
    let which_edges = exp.selectData(modelName, edge_payload, 'edges', edge_info, sort_way);
    for (let edge of which_edges) {
      payload['primary_id'] = edge['to_id'];
      exp.deleteData(edge_info['to_name'], payload, 'vertices', undefined, sort_way);
    }
  } catch (err) {
    throw new Errors.APIError('Delete all connected Vertices from '
        + modelName + ' with ' + JSON.stringify(edge_info) + ' failed', err, 500);
  }
}

module.exports = exp;
