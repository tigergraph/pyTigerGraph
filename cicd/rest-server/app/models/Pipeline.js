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
const db = require('../db.js');
const constDef = require('../const.js');
const config = require('../../config/config.js');
const Errors = require('../catchError.js');
const log = require('../../config/log.js')();

const spawnSync = constDef.spawnSync;

let type_table = constDef.type_table;
let primary_key = 'job_id';

let exp = {};

function get_p_id(type, id) {
  const jenkins_id = config.jenkins_config['jenkins_id']
  return type_table[type]['modelName'] == 'mwh_request' ? type + id + '_' + jenkins_id : id + '_' + jenkins_id;
}

exp.upsertPipeline = async (type, data) => {
  let modelName = type_table[type]['modelName'];
  let payload = { 'vertices': { [modelName]: {} } };
  try {
    data = Array.isArray(data) && data.length > 0 ? data : [data];
    for (let obj of data) {
      let p_id = get_p_id(type, obj[primary_key]);
      payload['vertices'][modelName][p_id] = constDef.setAttrForRestpp(obj);
    }
    return db.upsertData(modelName, payload);
  } catch (err) {
    throw new Errors.APIError('Upsert ' + modelName + ' failed: ' + JSON.stringify(payload), err, 500);
  }
}

exp.upsertEdges = async (type, data) => {
  let modelName = type_table[type]['modelName'];
  let payload = "";
  try {
    data = Array.isArray(data) && data.length > 0 ? data : [data];
    for (let obj of data) {
      let edge_info = obj['edge_info'];
      delete obj['edge_info'];
      let from_name = edge_info['from_name'], from_id = edge_info['from_id'];
      let edge_name = edge_info['edge_name'];
      let to_name = edge_info['to_name'], to_id = edge_info['to_id'];
      payload = { 'edges': { [from_name]: {} } };
      payload['edges'][from_name][from_id] =
          { [edge_name]: { [to_name]: { [to_id]: constDef.setAttrForRestpp(obj) } } };
    }
    return db.upsertData(modelName, payload);
  } catch (err) {
    throw new Errors.APIError('Upsert edge for ' + modelName + ' failed: ' + JSON.stringify(payload), err, 500);
  }
}

exp.deletebyId = async (type, p_id) => {
  let modelName = type_table[type]['modelName'];
  try {
    return db.deleteData(modelName, { 'primary_id': p_id }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Delete ' + modelName + ' for ' + p_id + ' failed', err, 500);
  }
}

exp.getPipeline = async (type, payload) => {
  let modelName = type_table[type]['modelName'];
  try {
    if (modelName == 'mwh_request') {
      payload['job_type'] = type
    }
    return db.selectData(modelName, payload, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' failed', err, 500);
  }
};

exp.deletePipelines = async (type, payload) => {
  let modelName = type_table[type]['modelName'];
  try {
    if (modelName == 'mwh_request') {
      payload['job_type'] = type;
    }
    return db.deleteData(modelName, payload, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Delete ' + modelName + ' failed', err, 500);
  }
};

exp.getById = async (type, p_id) => {
  let modelName = type_table[type]['modelName'];
  try {
    return db.selectData(modelName, { 'primary_id': p_id }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' for ' + p_id + ' failed', err, 500);
  }
};

exp.getEdges = async (type, payload) => {
  let modelName = type_table[type]['modelName'];
  try {
    let edge_info = payload['edge_info'];
    delete payload['edge_info'];
    return db.selectData(modelName, payload, 'edges', edge_info);
  } catch (err) {
    throw new Errors.APIError('Get egde from ' + type + ' to Node for ' + edge_info['from_id'] + ' failed', err, 500);
  }
}

module.exports = exp;
