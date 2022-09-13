/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

const Koa = require('koa');
const bodyParser = require('koa-bodyparser');
const cors = require('koa2-cors');

const fs = require('fs');
const cp = require('child_process');

const config = require('./config/config.js');
const constDef = require('./app/const.js');
const log = require('./config/log.js')();

const controller = require('./app/controller.js');
const rest = require('./app/rest.js');

const app = new Koa();

app.use(async (ctx, next) => {
  log(`Process ${ctx.request.method} ${ctx.request.url}...`);
  await next();
});

app.use(cors());
app.use(bodyParser());
app.use(rest.restify());
app.use(controller());

// app.on('error', err => {
//   log('server error', );
// });

app.listen(config.serverPort);
log('app started at port: ' + config.serverPort);

const checkExpiration = require('./checkExpiration.js');
