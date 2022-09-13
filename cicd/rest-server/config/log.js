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
const util = require('util');
const winston = require('winston');
const sps = require('child_process').spawnSync;

const config = require('./config.js');

let exp = {};
let log = undefined;

exp = (logFileName = '', newlogFileName = '') => {
  if (log !== undefined) {
    return log;
  }

  if (logFileName != '') {
    winston.add(winston.transports.File, {
      filename: newlogFileName,
      maxsize: config.restlogSizeLimit * 1048576 * 1024,
      maxFiles: config.restlogFileLimit,
      colorize: true,
      showLevel: false,
      flag: 'w',
      json: false,
      timestamp: false,
      tailable: true
    });
    if (process.env.NODE_ENV !== 'dev') {
      winston.remove(winston.transports.Console);
    }

    sps('rm', ['-rf', logFileName], { encoding: 'utf-8' });
    fs.symlinkSync(newlogFileName, logFileName);

    log = function () {
      winston.info(require('../app/const.js').getCurrentTime() + ': ' + util.format.apply(null, arguments));
    }
  } else {
    const logStdout = process.stdout;
    log = function () {
      logStdout.write(util.format.apply(null, arguments) + '\n');
    }
  }
  return log
}

//console.error = console.log;

module.exports = exp;
