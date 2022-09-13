/******************************************************************************
# Copyright (c) 2016-2017, TigerGraph Inc.
# All rights reserved.
#
# Project: MIT Restful API
# Authors: Yun Peng
# Created on: Aug 24, 2017
#
******************************************************************************/
const yaml = require('js-yaml');
const fs = require('fs');

const defaultConfig = './configDefault.json';
const overrideConfig = './configOverride.json';

let config = {
  gsqlCfg: process.env.HOME + '/.gsql/gsql.cfg'
}

config = Object.assign(config, require(defaultConfig));
config = Object.assign(config, require(overrideConfig));

//let gsqlCfg = yaml.safeLoad(fs.readFileSync(config.gsqlCfg));

//config.gsqlRoot = gsqlCfg['tigergraph.root.dir'];
//config.gsqlPath = process.env.HOME + '/.gium/gsql';

config.log_dir = process.env.HOME + '/mit_server_logs';
if (!fs.existsSync(config.log_dir)) {
  fs.mkdirSync(config.log_dir);
}
config.debug = process.env.DEBUG?true:false 
config.log = config.log_dir + '/mit_server.log';
//add log size max 3G file max 7
config.restlogSizeLimit = 3
config.restlogFileLimit = 7

config.mit_folder = __dirname + '/../../../mit';
config.jenkins_script = config.mit_folder + '/jenkins_script';
config.python3_script = config.jenkins_script + '/python3_script';

//load different configs as the jenkins_id
if (! process.env.JENKINS_ID || process.env.JENKINS_ID.includes('prod_sv4')) {
  config.jenkins_config = require(config.jenkins_script + '/config/config.json');
} else {
  config.jenkins_config = require(config.jenkins_script + '/config/config_' + process.env.JENKINS_ID + '.json')
  const restpp_server_ip = config.jenkins_config.mit_server_address.split(":")[0]
  const restpp_server_port = config.jenkins_config.mit_server_address.split(":")[1]
  config.restppServerIp = restpp_server_ip
  config.restppServerPort = restpp_server_port

}
config.jenkins_test_config = require(config.jenkins_script + '/config/test_config.json');

config.mnt_config_folder = config.jenkins_config.log_dir + '/config';
config.timecost_config_folder = config.mnt_config_folder + '/timecost_config';

config.ut_timecost_config = require(config.timecost_config_folder + '/unittest_timecost.json');

module.exports = config;
