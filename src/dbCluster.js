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
var Pool = require('./pool.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();


var redisNodes = config.redisServers;

logger.prefix = path.basename(module.filename, '.js');


var addNode = function(name, host, port, cb){
  logger.info('Adding new node ', name + ' - ' + host + ':' + port);
  if(redisNodes.hasOwnProperty(name)){
    logger.warning('addNode()', 'Node ' + name + ' already exists, wont be added');
  }
  else {
    var redisClient = createClient(port, host, function(err){
      if(err){
        cb(err);
      } else {
        var pool = new Pool(host, port);
        redisNodes[name] = {host: host, port: port, redisClient : redisClient, pool : pool};
        redistributeRemove(name, function(err){
          hashing.addNode(name);
          redistributeAdd(name, function(err){
            logger.info('New node added', name + ' - ' + host + ':' + port);
          });
        });
      }
    });
  }
};

var getGlobalResponse = function (req, cb){
  var requestFunctions = [];

  for (var node in redisNodes){
    var netClient = redisNodes[node].pool.get();
    requestFunctions.push(getOneResponse(netClient, req));
  }
  async.parallel(requestFunctions, cb);

  function getOneResponse(client, req){
    return function _getOneResponse (callback){
      client.write(req);
      client.on('data', function(res){
        callback(null, res)
      });
      client.on('error', function(err){
        callback(err)
      });
    }
  };
};

var getAddressNodes = function() {
  var addresses = [];
  for(var key in redisNodes){
    addresses.push(redisNodes[key].host + ':' + redisNodes[key].port);
  }
  return addresses;
};

var removeNode = function(name, cb) {
  logger.info('Removing node', name);
  if(!redisNodes.hasOwnProperty(name)){
    logger.warning('removeNode()', 'Node ' + name + ' does not exist, wont be removed');
  } else {
    hashing.removeNode(name);
    redistributeRemove(name, function(err){
      if (err){
        cb(err);
      } else {
        logger.info('Node \'' + name + '\' removed');
        //redisNodes[name].netClient.end();
        redisNodes[name].redisClient.quit();
        cb(null);
      }
    });
  }
};

var getDb = function(queueId) {
  'use strict';
  logger.info('getDb()', node);
  var node = hashing.getNode(queueId);
  var pool = redisNodes[node].pool;
  return pool.get();
};

var free = function (db) {
  'use strict';
  db.pool.free(db);
};

var redistributeAdd = function(newNode, cb){
  redistributionFunctions = [];
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
            redistributionFunctions.push(migrateAll(node, newNode, keys));
          } else {
            if (cb && typeof(cb) === 'function') {
              cb(err);
            }
          }
        }
      }
      async.parallel(redistributionFunctions, function (err){
        if (err){
          logger.error('migrateKeys()', err);
        }
        if (cb && typeof(cb) === 'function') {
          cb(err);
        }
      });
    }
  });

  function migrateAll(nodeName, nodeTo, keys){
    return function(callback){
      migrateKeys(nodeName, nodeTo, keys, callback);
    }
  }
};

var redistributeRemove = function (nodeName, cb) {
  redistributionObj = {}, redistributionFunctions = [];

  getAllKeys(redisNodes[nodeName], function(err, keys){
    if (err){
      logger.error('getAllKeys', err);
    } else {
      for (var i = 0; i < keys.length; i++){
        var nodeTo = hashing.getNode(keys[i]);
        if(!redistributionObj.hasOwnProperty(nodeTo)){
          redistributionObj[nodeTo] = [];
        }
        redistributionObj[nodeTo].push(keys[i]);
      }
      for (var nodeTo in redistributionObj){
        var keysToMigrate = redistributionObj[nodeTo];
        redistributionFunctions.push(migrateAll(nodeName, nodeTo, keysToMigrate));
      }
      async.parallel(redistributionFunctions, function (err){
        if (err){
          logger.error('migrateKeys()', err);
          if (cb && typeof(cb) === 'function') {
            cb(err);
          }
        } else {
          if (cb && typeof(cb) === 'function') {
            cb(null);
          }
        }
      });
    }
  });

  function migrateAll(nodeName, nodeTo, keys){
    return function(callback){
      migrateKeys(nodeName, nodeTo, keys, callback);
    }
  }
};

var migrateKeys = function(from, to, keys, cb) {
  var clientFrom = redisNodes[from].redisClient;
  var clientTo = redisNodes[to].redisClient;
  var clientToHost = redisNodes[to].host;
  var clientToPort = redisNodes[to].port;

  var multi = clientFrom.multi();
  for (var i = 0; i < keys.length; i++){
    multi.migrate(clientToHost, clientToPort, keys[i], config.selectedDB, config.migrationTimeout)
  }
  multi.exec(cb);
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

  node.redisClient.keys("*", function onGet(err, res){
    if (err){
      logger.error('getKeys()', err);
    }
    if (cb && typeof(cb) === 'function') {
      cb(err, res);
    }
  });
};

function createClient(port, host, cb){
  var cli = redisModule.createClient(port, host);

  cli.on('error', function(err){
    logger.warning('createClient()', err);
    if (cb && typeof(cb) === 'function') {
      cb(err);
    }
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
for (var node in redisNodes) {
  var port = redisNodes[node].port || redisModule.DEFAULT_PORT;
  var host = redisNodes[node].host;
  var cli = createClient(port, host);
  var pool = new Pool(host,port);
  redisNodes[node].redisClient = cli;
  redisNodes[node].pool = pool;

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

/**
 *
 * @param {string} queu_id identifier.
 * @return {RedisClient} rc redis client for QUEUES.
 */
exports.getDb = getDb;

exports.free = free;

exports.addNode = addNode;

exports.removeNode = removeNode;

exports.getAddressNodes = getAddressNodes;

exports.getGlobalResponse = getGlobalResponse;

require('./hookLogger.js').init(exports, logger);
