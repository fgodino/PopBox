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
var haMon = require('./haMonitor.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();


var redisNodes = config.redisServers;
haMon.newSentinel(redisNodes);


logger.prefix = path.basename(module.filename, '.js');

var setNodes = function(nodes){
  redisNodes = nodes;
};

var getNodes = function(){
  return redisNodes;
};

var addNode = function(name, host, port, cb){
  logger.info('Adding new node ', name + ' - ' + host + ':' + port);
  if(redisNodes.hasOwnProperty(name)){
    logger.warning('Node ' + name + ' already exists, wont be added');
    cb('Node ' + name + ' already exists, wont be added');
  }
  else {
    var redisClient = redisModule.createClient(port, host);
    redisNodes[name] = {host: host, port: port, redisClient : redisClient};
    var redistributionObj = hashing.addNode(name);
    distributeBefore(name, function(err){
      if(err){
        cb(err);
      } else {
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
    logger.warning('Node ' + name + ' does not exist, wont be removed');
    if (cb && typeof(cb) === 'function') {
      cb('Node ' + name + ' does not exist, wont be removed');
    }
  } else {
    var redistributionObj = hashing.removeNode(name);
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

  for(var nodeTo in redistributionObj){
    redistributionFunctions.push(migrateFromOne(nodeFrom, nodeTo, redistributionObj[nodeTo]));
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
    };
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
  };
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

// Init Nodes and functions

var generateNodes = function(){
  for (var node in redisNodes) {
    var port = redisNodes[node].port || redisModule.DEFAULT_PORT;
    var host = redisNodes[node].host;
    var cli = redisModule.createClient(port, host);
    redisNodes[node].redisClient = cli;

    require('../hookLogger.js').initRedisHook(cli, logger);

    logger.info('Connected to REDIS ', host + ':' + port);
    cli.select(config.selectedDB);

    hashing.addNode(node);
  }
  console.log(redisNodes);
};

var calculateDistribution = function(){
  var continuum = hashing.getContinuum();
  var currentDist = {};

  for(var key in continuum){
    if(!currentDist.hasOwnProperty(continuum[key])){
      currentDist[continuum[key]] = [];
    }
    currentDist[continuum[key]].push(key);
  }

  return currentDist;
};

var distributeBefore = function(node, callback){
  var redistributionFunctions = [];
  var currentDist = calculateDistribution();

  for(var nodeTo in currentDist){
    redistributionFunctions.push(migrateFromOne(node, nodeTo, currentDist[nodeTo]));
  }

  async.parallel(redistributionFunctions, callback);
};

//Bootstrapping clients and redistributing

var bootstrapMigration = function(callback){

  var redistributionFunctions = [];
  var currentDist = calculateDistribution();

  for(var nodeTo in currentDist){
    for(var nodeFrom in currentDist){
      if(nodeFrom != nodeTo){
        redistributionFunctions.push(migrateFromOne(nodeFrom, nodeTo, currentDist[nodeTo]));
      }
    }
  }
  async.parallel(redistributionFunctions, callback);
};

var getNodesSerialized = function(){
  var redisHostPort = {};

  for(var node in redisNodes){
    var info = redisNodes[node];
    redisHostPort[node] = JSON.stringify({host : info.host, port : info.port}); //serialize
  }

  return redisHostPort;
};


var downloadConfig = function (rc, callback){
  var multi = rc.multi();

  multi.hgetall('NODES');
  multi.hgetall('CONTINUUM');
  multi.lrange('KEYS', 0, -1);

  multi.exec(function(err, res){
    if(err){
      callback(err);
      return;
    }
    var serializedNodes = res[0];
    var continuum = res[1];
    var keys = res[2];
    hashing.setKeys(keys);
    hashing.setContinuum(continuum);

    //deserialize nodes
    var deserializedNodes = {};
    for (var node in serializedNodes){
      var info = JSON.parse(serializedNodes[node]);
      deserializedNodes[node] = info;
    }



    for (var node in deserializedNodes){
      var port = deserializedNodes[node].port || redisModule.DEFAULT_PORT;
      var host = deserializedNodes[node].host;
      var cli = redisModule.createClient(port, host);

      redisNodes[node] = {host : host, port : port, redisClient : cli};
    }
    console.log(Object.keys(deserializedNodes));
    if(Object.keys(deserializedNodes).length != Object.keys(redisNodes).length){
      haMon.newSentinel(redisNodes);
    }
    callback(err);
  });
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

exports.getNodesSerialized = getNodesSerialized;

exports.getNodes = getNodes;

exports.setNodes = setNodes;

exports.downloadConfig = downloadConfig;
