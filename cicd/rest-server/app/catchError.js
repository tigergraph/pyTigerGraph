/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/

let exp = {};

function APIError(message, err, code) {
  this.message = message;
  this.err = err;
  this.code = code;
};

exp.APIError = APIError;

module.exports = exp;
