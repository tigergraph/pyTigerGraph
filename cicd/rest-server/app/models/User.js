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

let modelName = 'user';
let primary_key = 'user_name';

let exp = {};

exp.upsertUsers = async (data) => {
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

exp.deletebyName = async (name) => {
  try {
    return db.deleteData(modelName, { 'primary_id': name }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Delete ' + modelName + ' for ' + name + ' failed', err, 500);
  }
}

exp.getUsers = async () => {
  try {
    return db.selectData(modelName, {}, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' failed', err, 500);
  }
};

exp.getByName = async (name) => {
  try {
    return db.selectData(modelName, { 'primary_id': name }, 'vertices');
  } catch (err) {
    throw new Errors.APIError('Get ' + modelName + ' for ' + name + ' failed', err, 500);
  }
};

exp.NotifyUser = async (name, job_name, job_url, comment) => {
  try {
    let users = await exp.getByName(name);
    if (users.length != 1 || users[0].email == undefined) {
      throw new Errors.APIError('Can not find user ' + name, {}, 500);
    }
    let email = users[0]['email'];
    let notify_data = {
      'name': job_name,
      'url': job_url,
      'comment': comment
    }
    spawnSync('python', [config.jenkins_script + '/python3_script/notification.py',
      '', 'STATUS', email, 'none', 'none', JSON.stringify(notify_data)]);
  } catch (err) {
    throw new Errors.APIError('Notify user ' + name + ' failed', err, 500);
  }
}

module.exports = exp;
