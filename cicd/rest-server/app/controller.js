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
let router = require('koa-router')();

const log = require('../config/log.js')();

function addMapping(router, mapping) {
  for (let url in mapping) {
    if (url.startsWith('GET ')) {
      let path = url.substring(4);
      router.get(path, mapping[url]);
      log(`register URL mapping: GET ${path}`);
    } else if (url.startsWith('POST ')) {
      let path = url.substring(5);
      router.post(path, mapping[url]);
      log(`register URL mapping: POST ${path}`);
    } else if (url.startsWith('PUT ')) {
      let path = url.substring(4);
      router.put(path, mapping[url]);
      log(`register URL mapping: PUT ${path}`);
    } else if (url.startsWith('DELETE ')) {
      let path = url.substring(7);
      router.del(path, mapping[url]);
      log(`register URL mapping: DELETE ${path}`);
    } else {
      log(`invalid URL: ${url}`);
    }
  }
}

function addControllers(router) {
  let folder = __dirname + '/controllers/';
  fs.readdirSync(folder).filter((f) => {
    return f.endsWith('.js');
  }).forEach((f) => {
    log(`process controller: ${f}...`);
    let mapping = require(folder + f);
    addMapping(router, mapping);
  });
}

module.exports = (dir) => {
  addControllers(router);
  return router.routes();
};
