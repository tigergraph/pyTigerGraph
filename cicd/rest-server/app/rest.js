/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

const constDef = require('./const.js');
const log = require('../config/log.js')();

let PATHPREFIX = constDef.PATHPREFIX;
let exp = {};

function handleErrorMsg(err) {
  if (err == undefined || err.err === undefined) {
    return err instanceof Object ? JSON.stringify(err): String(err);
  } else {
    return err.message + '--->' + handleErrorMsg(err.err);
  }
}

exp.restify = () => {
  return async (ctx, next) => {
    if (ctx.request.path.startsWith(PATHPREFIX)) {
      log(`Process API ${ctx.request.method} ${ctx.request.url}...`);
      ctx.rest = (data) => {
        ctx.response.type = 'application/json';
        ctx.response.body = {
          error: false,
          message: '',
          result: data
        }
      }
      try {
        await next();
      } catch (err) {
        log('Process API failed...');
        log(err);
        ctx.response.type = 'application/json';
        ctx.response.status = err.code || 400;
        ctx.response.body = {
          error: true,
          message: handleErrorMsg(err) || "Process API failed",
          result: err.data
        };
      }
    } else {
      await next();
      ctx.response.type = 'application/json';
      ctx.response.status = 404;
      ctx.response.body = {
        error: true,
        message: 'Url not found',
        result: {}
      };
    }
  };
}

module.exports = exp;
