/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

const fs = require('fs');
const log = require('../config/log.js')();

let exp = {};

let folder = __dirname + '/models/';
fs.readdirSync(folder).filter((f) => {
  return f.endsWith('.js');
}).forEach((f) => {
  log(`import model from file ${f}...`);
  let name = f.substring(0, f.length - 3);
  exp[name] = require(folder + f);
})

module.exports = exp;
