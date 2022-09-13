/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/
const db = require('../db.js');
const constDef = require('../const.js');
const config = require('../../config/config.js');
const Errors = require('../catchError.js');
const log = require('../../config/log.js')();

const spawnSync = constDef.spawnSync;

let modelName = 'slave_node';
let primary_key = 'node_name';

let exp = {};

exp.upsertNodes = async (data) => {
  let payload = { 'vertices': { [modelName]: {} } };
  try {
    data = Array.isArray(data) && data.length > 0 ? data : [data];
    for (let obj of data) {
      let name = obj[primary_key];
      payload['vertices'][modelName][name] = constDef.setAttrForRestpp(obj);
    }
    return db.upsertData(modelName, payload);
  } catch (err) {
    throw new Errors.APIError('Upsert ' + modelName + ' failed: ' + JSON.stringify(payload), err, 500);
  }
}

exp.getNodes = async (payload) => {
  try {
    return db.selectData(modelName, payload, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' failed: ' + JSON.stringify(payload), err, 500);
  }
};

exp.getByName = async (name) => {
  try {
    return db.selectData(modelName, { 'primary_id': name }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' for ' + name + ' failed', err, 500);
  }
};

exp.getByIp = async (name) => {
  try {
    return db.selectData(modelName, { 'ip': name }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' for ip ' + name + ' failed', err, 500);
  }
};


exp.deleteNodes = async (payload) => {
  try {
    return db.deleteData(modelName, payload, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Delete ' + modelName + ' failed: ' + JSON.stringify(payload), err, 500);
  }
};

exp.deletebyName = async (name) => {
  try {
    return db.deleteData(modelName, { 'primary_id': name }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Delete ' + modelName + ' for ' + name + ' failed', err, 500);
  }
}

module.exports = exp;
