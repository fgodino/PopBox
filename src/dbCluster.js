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
var net = require('net');
var config = require('./configProxy.js');
var hashing = require('./consistent_hashing.js');
var async = require('async');
var lua = require('./lua.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();


var redisNodes = config.redisServers;

logger.prefix = path.basename(module.filename, '.js');


var addNode = function(name, host, port){
  logger.info('Adding new node ', name + ' - ' + host + ':' + port);
  var redisClient = createClient(port, host);
  var netClient = net.connect({host: host,port: port});
  redisNodes[name] = {host: host, port: port, redisClient : client, netClient : netClient};
  redistributeRemove(name, function(err){
    hashing.addNode(name);
    redistributeAdd(name, function(err){
      logger.info('New node added', name + ' - ' + host + ':' + port);
    });
  });
};

var getDb = function(queueId) {
  'use strict';
  var node = hashing.getNode(queueId);
  return redisNodes[node].netClient;
};

var getOwnDb = function(queueId, callback) {
  'use strict';
  var pool = poolArray[hash];
  pool.get(queueId, callback);
};

var redistributeAdd = function(newNode, cb){
  calculateDistribution(function distribution(err, items){
    if (!err) {
      for (var node in items){
        if (node != newNode){
          var keys = items[node];
          for(var i = 0; i < keys.length; i++){
            var key = keys[i];
            if (hashing.getNode(key) != newNode){
              keys.splice(i, 1);
              i--;
            }
          }
          if(keys.length > 0){
            migrateKeys(node, newNode, keys, function(err){
              if (err){
                logger.error('migrateKeys()', err);
                if (cb && typeof(cb) === 'function') {
                  cb(err);
                }
              }
            });
          }
          else {
            if (cb && typeof(cb) === 'function') {
              cb(err);
            }
          }
        }
      }
    }
  });
};

var redistributeRemove = function (nodeName, cb) {
  redistributionObj = {}
  getAllKeys(redisNodes[nodeName], function(err, keys){
    if (err){
      logger.error('getAllKeys', err);
    }
    else {
      for (var i = 0; i < keys.length; i++){
        var nodeTo = hashing.getNode(keys[i]);
        console.log(nodeTo);
        if(!redistributionObj.hasOwnProperty(nodeTo)){
          redistributionObj[nodeTo] = [];
        }
        redistributionObj[nodeTo].push(keys[i]);
      }
      for (var nodeTo in redistributionObj){
        var keysToMigrate = redistributionObj[nodeTo];
        migrateKeys(nodeName, nodeTo, keysToMigrate, function (err){
          if (err){
            logger.error('migrateKeys()', err);
            if (cb && typeof(cb) === 'function') {
              cb(err);
            }
          }
          else {
            if (cb && typeof(cb) === 'function') {
              cb(null);
            }
          }
        });
      }
    }
  });
};



var migrateKeys = function(from, to, keys, cb) {
  var clientFrom = redisNodes[from].redisClient;
  var clientTo = redisNodes[to].redisClient;

  lua.getQueues(keys, clientFrom, function gotQueues (err, resGet){
    if (err){
      logger.error('getQueues()', 'Failed to get redis Queues : ' + err);
      cb(err);
    }
    else {
      lua.copyQueues(resGet, clientTo, function copied(err, resCopy){
        if (err){
          logger.error('copyQueues()', 'Failed to copy redis queues : + ' + err);
          if (cb && typeof(cb) === 'function') {
            cb(err);
          }
        }
        else {
          lua.deleteQueues(keys, clientFrom);
          if (cb && typeof(cb) === 'function') {
            cb(null);
          }
        }
      });
    }
  });
};

var calculateDistribution = function(cb){
  var nodeFunctions = {}; //Parallel functions as an object to receive results in an object.
  for(var node in redisNodes){
    nodeFunctions[node] = _getAllKeysParrallel(redisNodes[node]);
  }

  async.parallel(nodeFunctions, function(err, res){
    if (err){
      logger.error('getAllKeys()', err);
    }
    if (cb && typeof(cb) === 'function') {
      cb(err, res);
    }
  });

  function _getAllKeysParrallel (item){
    return function _getAllKeys (callback){
      getAllKeys(item, callback);
    }
  }
};

var getAllKeys = function(node, cb){

  node.redisClient.keys("PB:Q|*", function onGet(err, res){
    if (err){
      logger.error('getKeys()', err);
    }
    if (cb && typeof(cb) === 'function') {
      cb(err, res);
    }
  });
};

function createClient(port, host){
  var cli = redisModule.createClient(port, host);

  cli.on('error', function(err){
    logger.warning('createClient()', err);
  });

  return cli;
}


// Init Nodes and functions
for (var node in redisNodes) {
  var port = redisNodes[node].port || redisModule.DEFAULT_PORT;
  var host = redisNodes[node].host;
  var cli = createClient(port, host);
  var netCli = net.connect({port : port, host : host});

  redisNodes[node].redisClient = cli;
  redisNodes[node].netClient = netCli;

  require('./hookLogger.js').initRedisHook(cli, logger);

  if (config.slave) {
    slaveOf(cli, config.masterRedisServers[i].host,
        config.masterRedisServers[i].port);
  }

  logger.info('Connected to REDIS ', host + ':' + port);
  cli.select(config.selectedDB);
  cli.isOwn = false;

  hashing.addNode(node);
}

//Bootstrapping clients and redistributing

calculateDistribution(function(err, items){
  for (nodeFrom in items){
    var redistribution = {};
    var keys = items[nodeFrom];
    for(var i = 0; i < keys.length; i++){
      var key = keys[i];
      var redNode = hashing.getNode(key);
      if (redNode != nodeFrom){
        if (!redistribution.hasOwnProperty(redNode)) {
          redistribution[redNode] = [];
        }
        redistribution[redNode].push(key);
      }
    }
    for(nodeDest in redistribution){
      migrateKeys(nodeFrom, nodeDest, redistribution[nodeDest], function(err){
        if (err){
          logger.error('migrateKeys()', err);
        }
      });
    }
  }
});

var createPipes = function(socket){
  for(var node in redisNodes){
    redisNodes[node].netClient.pipe(socket);
  }
}

/**
 *
 * @param {string} queu_id identifier.
 * @return {RedisClient} rc redis client for QUEUES.
 */
exports.getDb = getDb;

exports.createPipes = createPipes;

require('./hookLogger.js').init(exports, logger);
