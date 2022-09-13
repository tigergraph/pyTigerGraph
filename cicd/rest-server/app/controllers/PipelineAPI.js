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
const fs = require('fs');
const model = require('../model.js');
const constDef = require('../const.js');
const Errors = require('../catchError.js');
const db = require('../db.js');
const config = require('../../config/config.js');
const log = require('../../config/log.js')();
const spawnSync = constDef.spawnSync;

let PATHPREFIX = constDef.PATHPREFIX;
let url = PATHPREFIX;
let type_table = constDef.type_table;

let instance = model.Pipeline;
let exp = {};


function get_p_id(type, id) {
  const jenkins_id = config.jenkins_config['jenkins_id']
  return type_table[type]['modelName'] == 'mwh_request' ? type + id + '_' + jenkins_id : id + '_' + jenkins_id;
}

for (let type of Object.keys(type_table)) {

  // basic internal data operation API

  exp['DELETE ' + url + '/' + type] = async (ctx, next) => {
    let payload = ctx.query;
    try {
      log('DELETE ' + type + ' API');
      ctx.rest(await instance.deletePipelines(type, payload));
    } catch (err) {
      throw new Errors.APIError('DELETE ' + type + ' API failed', err, 500);
    }
  };


  exp['DELETE ' + url + '/' + type + '/:id'] = async (ctx, next) => {
    let id = ctx.params.id;
    try {
      log('Delete ' + type + ' by id ' + id + ' API');
      ctx.rest(await instance.deletebyId(type, get_p_id(type, id)));
    } catch (err) {
      throw new Errors.APIError('Delete ' + type + ' by id ' + id + ' API failed', err, 500);
    }
  };


  exp['POST ' + url + '/' + type] = async (ctx, next) => {
    let body = ctx.request.body;
    try {
      log('Add ' + type + ' API:');
      if (config.debug == true) {
        log(body);
      }
      ctx.rest(await instance.upsertPipeline(type, body));
    } catch (err) {
      throw new Errors.APIError('Add ' + type + ' API failed', err, 500);
    }
  };


  exp['PUT ' + url + '/' + type + '/:id'] = async (ctx, next) => {
    let id = ctx.params.id, body = ctx.request.body;
    try {
      log('Update ' + type + ' ' + id + ' API:');
      if (config.debug == true) {
        log(body);
      }
      // update is not allowed if the vertice does not exist
      await instance.getById(type, get_p_id(type, id));

      body['job_id'] = parseInt(id);
      ctx.rest(await instance.upsertPipeline(type, body));
    } catch (err) {
      throw new Errors.APIError('Update ' + type + ' ' + id + ' API failed', err, 500);
    }
  };


  exp['GET ' + url + '/' + type] = async (ctx, next) => {
    let payload = ctx.query;
    try {
      log('GET ' + type + ' API');
      ctx.rest(await instance.getPipeline(type, payload));
    } catch (err) {
      throw new Errors.APIError('GET ' + type + ' API failed', err, 500);
    }
  };

  exp['GET ' + url + '/' + type + '/:id'] = async (ctx, next) => {
    let id = ctx.params.id;
    try {
      log('GET ' + type + ' by id ' + id + ' API');
      ctx.rest(await instance.getById(type, get_p_id(type, id)));
    } catch (err) {
      throw new Errors.APIError('GET ' + type + ' by id ' + id + ' API failed', err, 500);
    }
  };

  // lookup API

  exp['GET ' + url + '/' + type + '/:id/log'] = async (ctx, next) => {
    let id = ctx.params.id;
    try {
      log('GET log for ' + type + ' ' + id + ' API');
      let log = "";
      let which_obj = (await instance.getById(type, get_p_id(type, id)));
      if (which_obj.length != 1 || which_obj[0]['log_dir'] == undefined)  {
        throw new Errors.APIError('can not find the log by job id ' + id, {}, 500);
      }
      log = 'http://' + config.jenkins_config['log_review_machine']
          + '/Log.php?directory=' + which_obj[0]['log_dir'];
      ctx.rest(log);
    } catch (err) {
      throw new Errors.APIError('GET log for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  exp['GET ' + url + '/' + type + '/:id/output'] = async (ctx, next) => {
    let id = ctx.params.id;
    try {
      log('GET ' + type + ' ' + id + ' jenkins output API')
      let jenkins_url = 'http://' +  config.jenkins_config['jenkins_ip'] + ':'
          + config.jenkins_config['jenkins_port']
          + '/job/' + type_table[type]['jenkinsName'] + '/' + id + '/console';
      ctx.rest(jenkins_url);
    } catch (err) {
      throw new Errors.APIError('GET ' + type + ' ' + id + ' jenkins output API failed', err, 500);
    }
  };

  // only for mwh_request and test_job
  exp['GET ' + url + '/' + type + '/:id/build_job'] = async (ctx, next) => {
    let id = ctx.params.id, payload = ctx.query;
    try {
      log('GET build_job for ' + type + ' ' + id + ' API');
      let modelName = type_table[type]['modelName'];
      if (modelName != 'mwh_request' && modelName != 'test_job') {
        throw new Errors.APIError('The type must be mwh_request/test_job', {}, 500);
      }
      let jobs = await db.getConnectedVertices(modelName, payload, {
        from_id: get_p_id(type, id),
        edge_name: modelName == 'mwh_request' ? 'mwh_build_info' : 'build_test_info',
        to_name: 'build_job'
      });
      ctx.rest(jobs);
    } catch (err) {
      throw new Errors.APIError('GET build_job for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only for mwh_request and build_job
  exp['GET ' + url + '/' + type + '/:id/test_job'] = async (ctx, next) => {
    let id = ctx.params.id, payload = ctx.query;
    try {
      log('GET test_job for ' + type + ' ' + id + ' API');
      let modelName = type_table[type]['modelName'];
      if (modelName != 'mwh_request' && modelName != 'build_job') {
        throw new Errors.APIError('The type must be mwh_request/build_job', {}, 500);
      }
      let jobs = await db.getConnectedVertices(modelName, payload,{
        from_id: get_p_id(type, id),
        edge_name: modelName == 'mwh_request' ? 'mwh_test_info' : 'build_test_info',
        to_name: 'test_job'
      });
      ctx.rest(jobs);
    } catch (err) {
      throw new Errors.APIError('GET test_job for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only for build_job and test_job
  exp['GET ' + url + '/' + type + '/:id/mwh_request'] = async (ctx, next) => {
    let id = ctx.params.id;
    try {
      log('GET mwh_request for ' + type + ' ' + id + ' API');
      let modelName = type_table[type]['modelName'];
      if (modelName == 'mwh_request') {
        throw new Errors.APIError('The type must be build_job/test_job', {}, 500);
      }
      let jobs = await db.getConnectedVertices(modelName, {},{
        from_id: get_p_id(type, id),
        edge_name: modelName == 'build_job' ? 'mwh_build_info' : 'mwh_test_info',
        to_name: 'mwh_request'
      });
      ctx.rest(jobs);
    } catch (err) {
      throw new Errors.APIError('GET mwh_request for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only for build_job and test_job
  exp['GET ' + url + '/' + type + '/:id/node'] = async (ctx, next) => {
    let id = ctx.params.id;
    try {
      log('GET node for ' + type + ' ' + id + ' API');
      let modelName = type_table[type]['modelName'];
      if (modelName == 'mwh_request') {
        throw new Errors.APIError('The type must be build_job/test_job', {}, 500);
      }
      let nodes = await db.getConnectedVertices(modelName, {},{
        from_id: get_p_id(type, id),
        edge_name: modelName == 'build_job' ? 'build_node_info' : 'test_node_info',
        to_name: 'slave_node'
      });
      ctx.rest(nodes);
    } catch (err) {
      throw new Errors.APIError('GET node for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only for mwh_request
  exp['GET ' + url + '/' + type + '/:id/user'] = async (ctx, next) => {
    let id = ctx.params.id, p_id = get_p_id(type, id);
    try {
      log('GET user for ' + type + ' ' + id + ' API');
      let modelName = type_table[type]['modelName'], mwh_id = p_id;
      if (modelName != 'mwh_request') {
        let mwh = await db.getConnectedVertices(modelName, {}, {
          from_id: p_id,
          edge_name: modelName == 'build_job' ? 'mwh_build_info' : 'mwh_test_info',
          to_name: 'mwh_request'
        });
        if (mwh.length != 1 || mwh[0].job_id == undefined) {
          throw new Errors.APIError('can not get mwh_request of job ' + id, {}, 500);
        }
        mwh_id = get_p_id(mwh[0].job_type, mwh[0].job_id);
      }
      let users = await db.getConnectedVertices('mwh_request', {}, {
        from_id: mwh_id,
        edge_name: 'user_request_info',
        to_name: 'user'
      });
      ctx.rest(users);
    } catch (err) {
      throw new Errors.APIError('GET user for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // // only for mwh_request to get all job and its node name
  // exp['GET ' + url + '/' + type + '/:id/alljob'] = async (ctx, next) => {
  //   let id = ctx.params.id, payload = ctx.query, p_id = get_p_id(type, id);
  //   try {
  //     log('GET build_job for ' + type + ' ' + id + ' API');
  //     let modelName = type_table[type]['modelName'];
  //     if (modelName != 'mwh_request' && modelName != 'test_job') {
  //       throw new Errors.APIError('The type must be mwh_request/test_job', {}, 500);
  //     }
  //
  //     let jobs = [];
  //     let build_jobs = await db.getConnectedVertices(modelName, payload, {
  //       from_id: p_id,
  //       edge_name: 'mwh_build_info',
  //       to_name: 'build_job'
  //     });
  //     let test_jobs = await db.getConnectedVertices(modelName, payload, {
  //       from_id: p_id,
  //       edge_name: 'mwh_test_info',
  //       to_name: 'test_job'
  //     });
  //     for (let job of test_jobs.concat(build_jobs)) {
  //       let job_type = 'test_job';
  //       if (job['unittests'] == undefined) {
  //         job_type = 'build_job';
  //       }
  //       let nodes = await db.getConnectedVertices(job_type, {}, {
  //         from_id: job['job_id'],
  //         edge_name: job_type == 'test_job' ? 'test_node_info' : 'build_node_info',
  //         to_name: 'slave_node'
  //       });
  //       jobs.push({
  //         'job_id': job['job_id'],
  //         'node_name': nodes[0]['node_name'],
  //         'status': job['status'],
  //         'debug_status': job['debug_status'],
  //         'os': job['os']
  //       })
  //     }
  //     ctx.rest(jobs);
  //   } catch (err) {
  //     throw new Errors.APIError('GET all job for ' + type + ' ' + id + ' API failed', err, 500);
  //   }
  // };

  // jenkins usage API

  // only for mwh_request
  exp['DELETE ' + url + '/' + type + '/:id/withEdge'] = async (ctx, next) => {
    let id = ctx.params.id, p_id = get_p_id(type, id);
    try {
      let modelName = type_table[type]['modelName'];
      if (modelName != 'mwh_request') {
          throw new Errors.APIError('Only MIT/WIP/HOURLY can be deleted with edge', {}, 500);
      }
      // delete is not allowed if the vertice does not exist
      await instance.getById(type, p_id);

      log('DELETE ' + type + ' withEdge by id ' + id + ' API');
      await db.deleteConnectedVertices(modelName, {}, {
        from_id: p_id,
        edge_name: 'mwh_test_info',
        to_name: 'test_job'
      });
      await db.deleteConnectedVertices(modelName, {}, {
        from_id: p_id,
        edge_name: 'mwh_build_info',
        to_name: 'build_job'
      });
      ctx.rest(await instance.deletebyId(type, p_id));
    } catch (err) {
      throw new Errors.APIError('DELETE ' + type + ' by id ' + id + ' API failed', err, 500);
    }
  };


  exp['POST ' + url + '/' + type + '/withEdge'] = async (ctx, next) => {
    let body = ctx.request.body;
    try {
      log('Add ' + type + ' withEdge API:');
      if (config.debug == true) {
        log(body);
      }
      let modelName = type_table[type]['modelName'];
      let edge_infos = body['edge_infos'], p_id = get_p_id(type, body['job_id']);
      delete body['edge_infos'];
      await instance.upsertPipeline(type, [body]);

      let mwh_p_id = "";
      if (modelName == "test_job") {
        mwh_p_id = edge_infos['mwh_request']['edge_data'][0]['from_id'];
      }
      for (let to_name in edge_infos) {
        let edge_name = edge_infos[to_name]['edge_name'];
        let from_id = "", from_name_i = "", to_id = "", to_name_i;
        for (let edge_data of edge_infos[to_name]['edge_data']) {
          //Add from id to api data so we know the direction of the edge
          //if the given MIT/WIP/HOURLY/build/test job_id is the given from_id
          //then the edge is from p_id (e.g. WIPxxx for MIT/WIP/HOURLY or xxx for build/test)
          //to edge_data['to_id']. Otherwise the edge is from edge['from_id'] to p_id.
          if (edge_data['from_id'] == body['job_id']) {
            from_id = p_id;
            //Set from_name to MIT/WIP/HOURLY/build/test job type so upsertEdges knows
            //we're going from p_id to edge_data['to_id']
            from_name_i = modelName;
            //use to_name_i instead of to_name to avoid overwrite if there are more
            //than 1 edges with the same name.
            to_name_i = to_name
            to_id = edge_data['to_id'];
          } else {
            from_id = edge_data['from_id'];
            //Set from_name and to name so upsertEdges knows the direction
            from_name_i = to_name;
            //use to_name_i instead of to_name to avoid overwrite if there are more
            //than 1 edges with the same name.
            to_name_i = modelName;
            to_id = p_id;
          }
          edge_data['edge_info'] = {
            'from_name': from_name_i,
            'from_id': from_id,
            'edge_name': edge_name,
            'to_name': to_name_i,
            'to_id': to_id
          }
          delete edge_data['from_id'];
          delete edge_data['to_id'];
          await instance.upsertEdges(type, [edge_data]);
        }
      }
      if (modelName == "test_job") {
        let mwh_type = mwh_p_id.match(/[a-zA-Z]+/)[0];
        let mwh_obj = (await instance.getById(mwh_type, mwh_p_id));
        if (!Array.isArray(mwh_obj) || mwh_obj.length != 1) {
          throw new Errors.APIError(mwh_type + ' does not exist', {}, 500);
        }
        let skip_build = mwh_obj[0]['skip_build'];
        log('skip_build is: ' + skip_build);
        if (skip_build != 'false') {
          let build_os = body['os'] == 'ubuntu16' ? 'ubuntu16' : 'centos6';

          let borrow_id = parseInt(skip_build.substring(skip_build.lastIndexOf('_') + 1));
          if (skip_build.startsWith('w')) skip_build = 'wip';
          else if (skip_build.startsWith('m')) skip_build = 'mit';
          else skip_build = 'hourly';

          let build_jobs = (await db.getConnectedVertices('mwh_request', {
            'os': build_os
          }, {
            'from_id': skip_build + borrow_id,
            'edge_name': 'mwh_build_info',
            'to_name': 'build_job'
          }));
          if (!Array.isArray(build_jobs) || build_jobs.length == 0) {
            throw new Errors.APIError('build job does not exist', {}, 500);
          }
          //build_test_info edge goes from build job to test job
          await instance.upsertEdges(type, [{'edge_info': {
            'from_name': 'build_job',
            'from_id': build_jobs[0]['job_id'],
            'edge_name': 'build_test_info',
            'to_name': modelName,
            'to_id': pid
          }}]);
        }
      }
      ctx.rest('Post succeed');
    } catch (err) {
      throw new Errors.APIError('Add ' + type + ' withEdge API failed', err, 500);
    }
  };


  // action API

  exp['GET ' + url + '/' + type + '/:id/abort'] = async (ctx, next) => {
    let id = ctx.params.id, p_id = get_p_id(type, id);
    try {
      log('Abort ' + type + ' ' + id + ' API');
      // update is not allowed if the vertice does not exist
      await instance.getById(type, p_id);
      await validateUser(type, id, ctx.query.user);

      // Use qa_build jenkins account for abort function since we no longer allow aynonmous abort
      let jenkins_url = 'http://' +  config.jenkins_config['jenkins_account'] + ":" + 
        config.jenkins_config['jenkins_pwd'] + "@" + config.jenkins_config['jenkins_ip']
          + ':' + config.jenkins_config['jenkins_port']
          + '/job/' + type_table[type]['jenkinsName'] + '/' + id + '/stop';
      spawnSync('curl', ['-X', 'POST', jenkins_url]);
      ctx.rest(await instance.upsertPipeline(type, {
        job_id: parseInt(id),
        status: 'ABORTED',
        end_t: constDef.getCurrentTime()
      }));
    } catch (err) {
      throw new Errors.APIError('Abort ' + type + ' ' + id + ' API failed', err, 500);
    }
  };


  exp['GET ' + url + '/' + type + '/:id/summary'] = async (ctx, next) => {
    let id = ctx.params.id, p_id = get_p_id(type, id), stages = ctx.query.stages;
    try {
      log('GET summary for ' + type + ' ' + id + ' API');
      let modelName = type_table[type]['modelName'];
      let jobs = await instance.getById(type, p_id);
      if (!Array.isArray(jobs) || jobs.length == 0) {
        throw new Errors.APIError(p_id + ' does not exist', {}, 500);
      }
      let log_dir = jobs[0]['log_dir'];
      let ut_summary = log_dir + '/unit_test_summary', it_summary = log_dir + '/integration_test_summary';

      if (modelName == "mwh_request") {
        spawnSync('bash', [config.jenkins_script + '/shell_script/conclude_summary.sh', log_dir]);
      }

      let summary = spawnSync('python3', [config.jenkins_script + '/python3_script/get_summary.py',
          ut_summary, it_summary, stages, config.timecost_config_folder + '/unittest_timecost.json',
          config.timecost_config_folder + '/integration_timecost.json']);
      log(summary);
      ctx.rest(summary);
    } catch (err) {
      throw new Errors.APIError('GET summary for ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  exp['GET ' + url + '/' + type + '/:id/nodeOnline'] = async (ctx, next) => {
    let modelName = type_table[type]['modelName'], id = ctx.params.id, p_id = get_p_id(type, id);
    try {
      log('Return machines ' + type + ' ' + id + ' API');
      // update is not allowed if the vertice does not exist
      await instance.getById(type, p_id);
      await validateUser(type, id, ctx.query.user);

      for (let theStatus of ['ABORTED', 'RUNNING']) {
        let jobs = (await db.getConnectedVertices(modelName, {
          'status': theStatus
        }, {
          from_id: p_id,
          edge_name: 'mwh_test_info',
          to_name: 'test_job'
        }));
        for (let job of jobs) {
          let machines = (await db.getConnectedVertices("test_job", {
            'status': 'offline'
          }, {
            from_id: job['job_id'],
            edge_name: 'test_node_info',
            to_name: 'slave_node'
          }));
          for (let machine of machines) {
            await constDef.requestServer('/nodes/' + machine['node_name'] + '/takeOnline', 'PUT', {
              log_dir: job['log_dir']
            });
          }
        }
      }

      ctx.rest('revert all succeed!');
    } catch (err) {
      throw new Errors.APIError('Revert ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only for failed mwh_request, to revert all its aborted test_job
  exp['GET ' + url + '/' + type + '/:id/revertAll'] = async (ctx, next) => {
    let modelName = type_table[type]['modelName'], id = ctx.params.id, p_id = get_p_id(type, id);
    try {
      log('Revert ' + type + ' ' + id + ' API');
      // update is not allowed if the vertice does not exist
      await instance.getById(type, p_id);
      await validateUser(type, id, ctx.query.user);

      // abort test job
      let jobs = (await db.getConnectedVertices(modelName, {
        'status': 'RUNNING'
      }, {
        from_id: p_id,
        edge_name: 'mwh_test_info',
        to_name: 'test_job'
      }));
      for (let job of jobs) {
        await instance.upsertPipeline('test_job', {
          job_id: parseInt(job['job_id']),
          status: 'ABORTED',
          end_t: constDef.getCurrentTime()
        });
      }

      // abort build job
      let b_jobs = (await db.getConnectedVertices(modelName, {
        'status': 'RUNNING'
      }, {
        from_id: p_id,
        edge_name: 'mwh_build_info',
        to_name: 'build_job'
      }));
      for (let job of b_jobs) {
        await instance.upsertPipeline('build_job', {
          job_id: parseInt(job['job_id']),
          status: 'ABORTED',
          end_t: constDef.getCurrentTime()
        });
      }

      ctx.rest('revert all succeed!');
    } catch (err) {
      throw new Errors.APIError('Revert ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only can renew test_job and build_job
  exp['GET ' + url + '/' + type + '/:id/renew/:time'] = async (ctx, next) => {
    let modelName = type_table[type]['modelName'], add_time = ctx.params.time;
    let id = ctx.params.id, p_id = get_p_id(type, id);
    try {
      log('Renew ' + type + ' ' + id + ' API');
      if (modelName == 'mwh_request') {
        throw new Errors.APIError('Can not renew ' + type +
            ' , only can renew build_job or test_job', {}, 500);
      }
      await validateUser(type, id, ctx.query.user);

      let cur_job = (await instance.getById(type, p_id));
      if (!Array.isArray(cur_job) || cur_job.length == 0) {
        throw new Errors.APIError(p_id + ' does not exist', {}, 500);
      }
      cur_job = cur_job[0];
      let old_t = moment(cur_job['debug_end']);
      if (moment().diff(old_t) > 0 && cur_job['debug_status'] == false) {
        if (ctx.query.force == undefined || ctx.query.force != "true") {
          throw new Errors.APIError('The job already expired, you can not renew an expired job', {}, 500);
        }
        old_t = moment();
        let nodes = await db.getConnectedVertices(modelName, {},{
          from_id: p_id,
          edge_name: modelName == 'build_job' ? 'build_node_info' : 'test_node_info',
          to_name: 'slave_node'
        });
        if (!Array.isArray(nodes) || nodes.length == 0) {
          throw new Errors.APIError('nodes for ' + modelName + ' ' + p_id + ' does not exist', {}, 500);
        }
        let mwh_obj = await db.getConnectedVertices(modelName, {},{
          from_id: p_id,
          edge_name: modelName == 'build_job' ? 'mwh_build_info' : 'mwh_test_info',
          to_name: 'mwh_request'
        });
        if (!Array.isArray(mwh_obj) || mwh_obj.length == 0) {
          throw new Errors.APIError('mwh_request for ' + modelName + ' ' + p_id + ' does not exist', {}, 500);
        }
        let users = await db.getConnectedVertices('mwh_request', {}, {
          from_id: get_p_id(mwh_obj[0]['job_type'], mwh_obj[0]['job_id']),
          edge_name: 'user_request_info',
          to_name: 'user'
        });
        if (!Array.isArray(users) || users.length == 0) {
          throw new Errors.APIError('users for ' + modelName + ' ' + p_id + ' does not exist', {}, 500);
        }
        for (let node of nodes) {
          let running_job = await db.getConnectedVertices('slave_node', {
            'status': 'RUNNING'
          }, {
            from_id: node['node_name'],
            edge_name: modelName == 'build_job' ? 'build_node_info' : 'test_node_info',
            to_name: modelName
          });
          let debugging_job = await db.getConnectedVertices('slave_node', {
            'debug_status': true
          }, {
            from_id: node['node_name'],
            edge_name: modelName == 'build_job' ? 'build_node_info' : 'test_node_info',
            to_name: modelName
          });
          if (running_job.length > 0 || debugging_job.length > 0) {
            throw new Errors.APIError('node ' + node['node_name'] +
                ' can not be takeOffline, it might be running or debugging by others', {}, 500);
          }
          await constDef.requestServer('/nodes/' + node['node_name'] + '/takeOffline', 'PUT', {
            "offline_message": 'for user ' + users[0]['user_name'] + ' to debug ' + type + ' ' + cur_job['job_id'],
            "log_dir": cur_job['log_dir']
          });
        }
      }
      let time_unit = 'hours';
      if (/^\d+$/g.test(add_time)) {
        add_time = add_time + 'h';
      }
      if (add_time.endsWith('h')) {
        time_unit = 'hours';
      } else if (add_time.endsWith('m')) {
        time_unit = 'minutes';
      } else if (add_time.endsWith('s')) {
        time_unit = 'seconds';
      } else if (add_time.endsWith('d')) {
        time_unit = 'days';
      } else {
        throw new Errors.APIError('parameter time format error, should end with h/m/s/d', {}, 500);
      }
      add_time = add_time.slice(0, -1);
      ctx.rest(await instance.upsertPipeline(type, {
        job_id: parseInt(id),
        debug_end: old_t.add(add_time, time_unit).format(),
        debug_status: true
      }));
    } catch (err) {
      throw new Errors.APIError('Renew ' + type + ' ' + id + ' API failed', err, 500);
    }
  };

  // only can reclaim test_job and build_job
  exp['GET ' + url + '/' + type + '/:id/reclaim'] = async (ctx, next) => {
    let modelName = type_table[type]['modelName'], id = ctx.params.id, p_id = get_p_id(type, id);
    let force = ctx.query.force;
    try {
      log('Reclaim ' + type + ' ' + id + ' API');
      if (modelName == 'mwh_request') {
        throw new Errors.APIError('Can not reclaim ' + type +
            ' , only can reclaim build_job or test_job', {}, 500);
      }
      await validateUser(type, id, ctx.query.user);

      let cur_job = (await instance.getById(type, p_id));
      if (!Array.isArray(cur_job) || cur_job.length == 0) {
        throw new Errors.APIError(type + ' ' + p_id + ' does not exist', {}, 500);
      }
      cur_job = cur_job[0];
      let old_t = moment(cur_job['debug_end']);
      if (moment().diff(old_t) < 0 || (force != undefined && force === "true")) {
        let nodes = (await db.getConnectedVertices(modelName, {}, {
          from_id: p_id,
          edge_name: modelName == 'build_job' ? 'build_node_info' : 'test_node_info',
          to_name: 'slave_node'
        }));
        if (!Array.isArray(nodes) || nodes.length == 0) {
          throw new Errors.APIError('nodes for ' + modelName + ' ' + p_id + ' does not exist', {}, 500);
        }
        for (let node of nodes) {
          await constDef.requestServer('/nodes/' + node['node_name'] + '/takeOnline', 'PUT', {
            log_dir: cur_job['log_dir']
          });
        }
        ctx.rest(await instance.upsertPipeline(type, {
          job_id: parseInt(id),
          debug_end: moment().format(),
          debug_status: false
        }));
      }
    } catch (err) {
      throw new Errors.APIError('Reclaim ' + type + ' ' + id + ' API failed', err, 500);
    }
  };
}

async function validateUser(type, id, user_name) {
  let users = (await constDef.requestServer('/' + type + '/' + id + '/user', 'GET'))['result'];
  if (users.length == 0 || (user_name != undefined && users[0].user_name != user_name)) {
    throw new Errors.APIError('User secure check error', {}, 500);
  }
}

module.exports = exp;
