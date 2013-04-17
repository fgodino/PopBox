/*
 Copyright 2012 Telefonica Investigación y Desarrollo, S.A.U

 This file is part of PopBox.

 PopBox is free software: you can redistribute it and/or modify it under the
 terms of the GNU Affero General Public License as published by the Free
 Software Foundation, either version 3 of the License, or (at your option) any
 later version.
 PopBox is distributed in the hope that it will be useful, but WITHOUT ANY
 WARRANTY; without even the implied warranty of MERCHANTABILITY or
 FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 License for more details.

 You should have received a copy of the GNU Affero General Public License
 along with PopBox. If not, seehttp://www.gnu.org/licenses/.

 For those usages not covered by the GNU Affero General Public License
 please contact with::dtc_support@tid.es
 */

//clustering and database management (object)

var redisModule = require('redis');
var config = require('./config.js');
var hashing = require('./consistentHashingServer.js');
var async = require('async');
var EventEmitter = require( "events" ).EventEmitter;

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();


var redisNodes = config.redisServers;

logger.prefix = path.basename(module.filename, '.js');


var addNode = function(name, host, port, cb){
  logger.info('Adding new node ', name + ' - ' + host + ':' + port);
  if(redisNodes.hasOwnProperty(name)){
    logger.warning('addNode()', 'Node ' + name + ' already exists, wont be added');
    cb('addNode()', 'Node ' + name + ' already exists, wont be added');
  }
  else {
    var redisClient = createClient(port, host, function(err){
      if(err){
        cb(err);
      } else {
        redisNodes[name] = {host: host, port: port, redisClient : redisClient};
        var redistributionObj = hashing.addNode(name);
        redistributeAdd(name, redistributionObj, function(err){
          logger.info('New node added', name + ' - ' + host + ':' + port);
          cb(err);
        });
      }
    });
  }
};

var removeNode = function(name, cb) {
  logger.info('Removing node', name);
  if(!redisNodes.hasOwnProperty(name)){
    logger.warning('removeNode()', 'Node ' + name + ' does not exist, wont be removed');
    if (cb && typeof(cb) === 'function') {
      cb('removeNode()', 'Node ' + name + ' does not exist, wont be removed');
    }
  } else {
    var redistributionObj = hashing.removeNode(name);
    console.log("distribuir");
    redistributeRemove(name, redistributionObj, function(err){
      if (!err){
        logger.info('Node \'' + name + '\' removed');
        redisNodes[name].redisClient.quit();
        delete redisNodes[name];
      }
      if (cb && typeof(cb) === 'function') {
        cb(err);
      }
    });
  }
};


var redistributeAdd = function(nodeTo, redistributionObj, cb){
  redistributionFunctions = [];

  for(var node in redistributionObj){
    redistributionFunctions.push(migrateFromOne(node, nodeTo, redistributionObj[node]));
  }

  async.parallel(redistributionFunctions, cb);
};

var redistributeRemove = function (nodeFrom, redistributionObj, cb) {
  redistributionFunctions = [];

  console.log(redistributionObj);

  for(var node in redistributionObj){
    console.log(redistributionObj[node]);
    redistributionFunctions.push(migrateFromOne(nodeFrom, node, redistributionObj[node]));
  }

  async.parallel(redistributionFunctions, cb);
};

var getAllKeys = function(node, hash, cb){

  var cPattern = "PB:[QT]|*{" + hash + "}";

  redisNodes[node].redisClient.keys(cPattern , function onGet(err, res){
    if (err){
      logger.error('getKeys()', err);
    }
    cb(err, res);
  });
};


var migrateFromOne = function(nodeFrom, nodeTo, keys){

  function _getAllKeys(node, hash){
    return function(callback){
      getAllKeys(node, hash, callback);
    }
  }

  return function(callback){
    async.waterfall([
      function(callback){
        var getAllKeysFunctions = [];
        for (var i=0; i < keys.length; i++){
          console.log(keys[i]);
          getAllKeysFunctions.push(_getAllKeys(nodeFrom, keys[i]));
        }
        async.parallel(getAllKeysFunctions, function (err, res){
          var allKeys = [];
          if(!err){
            for (var i=0; i < res.length; i++){
              allKeys = allKeys.concat(res[i]);
            }
          }
          callback(err, allKeys);
        });
      },
      function(allKeys, callback){
        console.log(allKeys);
        migrateKeys(nodeFrom, nodeTo, allKeys, callback);
      }
    ], callback);
  }
};

var migrateKeys = function(from, to, keys, cb) {
  var clientFrom = redisNodes[from].redisClient;
  var clientTo = redisNodes[to].redisClient;
  var clientToHost = redisNodes[to].host;
  var clientToPort = redisNodes[to].port;

  var multi = clientFrom.multi();
  for (var i = 0; i < keys.length; i++){
    multi.migrate(clientToHost, clientToPort, keys[i], config.selectedDB, config.migrationTimeout);
  }
  multi.exec(function(err, res){
    if(err){
      throw new Error(err);
    }
    else {
      for (var i = 0; i < res.length; i++){
        if (res[i] != 'OK'){
          err = 'ERR: Can not migrate from ' + from + ' to ' + to;
          break;
        }
      }
    }
    cb(err,res);
  });
};

function createClient(port, host, cb){
  var cli = redisModule.createClient(port, host);

  cli.on('error', function(err){
    logger.warning('createClient()', err);
    if (cb && typeof(cb) === 'function') {
      cb(err);
    }
    throw new Error(err);
  });

  cli.on('ready', function(){
    logger.debug('createClient()', 'ready');
    if (cb && typeof(cb) === 'function') {
      cb(null);
    }
  });

  return cli;
}

// Init Nodes and functions

var generateNodes = function(){
  for (var node in redisNodes) {
    var port = redisNodes[node].port || redisModule.DEFAULT_PORT;
    var host = redisNodes[node].host;
    var cli = createClient(port, host);
    redisNodes[node].redisClient = cli;

    require('../hookLogger.js').initRedisHook(cli, logger);

    logger.info('Connected to REDIS ', host + ':' + port);
    cli.select(config.selectedDB);
    cli.isOwn = false;

    hashing.addNode(node);
  }
};


//Bootstrapping clients and redistributing

var bootstrapMigration = function(callback){

  redistributionFunctions = [];

  /*calculateDistribution(function(err, items){
    if (!err){
      for (nodeFrom in items){
        var redistribution = {};
        var keys = items[nodeFrom];
        for(var id in keys){
          var redNode = hashing.getNode(id);
          if (redNode != nodeFrom){
            if (!redistribution.hasOwnProperty(redNode)) {
              redistribution[redNode] = [];
            }
            redistribution[redNode].push(keys[id]);
          }
        }
        if(Object.keys(redistribution).length > 0) {
          for(nodeDest in redistribution){
            redistributionFunctions.push(migrateAll(nodeFrom, nodeDest, redistribution[nodeDest]));
          }
        }
      }

      async.parallel(redistributionFunctions, callback);

    } else {
       callback(err);
    }
  });*/
  callback(null);

  function migrateAll(nodeName, nodeTo, keys){
    return function(callback){
      migrateKeys(nodeName, nodeTo, keys, callback);
    }
  }

};

var getNodes = function(){
  var redisHostPort = {};

  for(var node in redisNodes){
    var info = redisNodes[node];
    redisHostPort[node] = JSON.stringify({host : info.host, port : info.port}); //serialize
  }

  return redisHostPort;
};


/**
 *
 * @param {string} queu_id identifier.
 * @return {RedisClient} rc redis client for QUEUES.
 */

exports.generateNodes = generateNodes;

exports.addNode = addNode;

exports.removeNode = removeNode;

exports.bootstrapMigration = bootstrapMigration;

exports.getNodes = getNodes;
