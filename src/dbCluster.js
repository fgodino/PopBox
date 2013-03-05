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
var poolMod = require('./pool.js');
var hashing = require('./consistent_hashing.js');
var async = require('async');
var lua = require('./lua.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();


var redisNodes = config.redisServers;
/**
 *
 * @param rc
 * @param masterHost
 * @param masterPort
 */
var slaveOf = function(rc, masterHost, masterPort) {
  'use strict';
  if (! (masterHost && masterPort)) {
    logger.error('Masters must be defined in slave' +
        ' configuration. Look at configFile');
    throw 'fatalError';
  }

  rc.slaveof(masterHost, masterPort, function(err) {
    if (err) {
      logger.error('slaveOf(rc, masterHost, masterPort):: ' + err);
      throw 'fatalError';
    }
  });
};

logger.prefix = path.basename(module.filename, '.js');

var transactionDbClient = createClient(config.tranRedisServer.port ||
    redisModule.DEFAULT_PORT, config.tranRedisServer.host);
require('./hookLogger.js').initRedisHook(transactionDbClient, logger);
if (config.slave) {
  slaveOf(transactionDbClient, config.masterTranRedisServer.host,
      config.masterTranRedisServer.port);
}

transactionDbClient.select(config.selectedDB); //false pool for pushing

//Create the pool array - One pool for each server
/*var poolArray = [];
for (var i = 0; i < redisNodes.length; i++) {
  var pool = poolMod.Pool(i);
  poolArray.push(pool);
}*/

var addNode = function(name, host, port){
  logger.info('Adding new node ', name + ' - ' + host + ':' + port);
  hashing.addNode(name);
  var client = createClient(port, host);
  redisNodes[name] = {host: host, port: port, client : client};
  redistributeAdd(name);
};

var getDb = function(queueId) {
  'use strict';
  var node = hashing.getNode(queueId);
  return redisNodes[node].client;
};

var getOwnDb = function(queueId, callback) {
  'use strict';
  var hash = hashMe(queueId, redisNodes.length);
  //get the pool
  var pool = poolArray[hash];
  pool.get(queueId, callback);
};

var redistributeAdd = function(newNode){
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
              }
            });
          }
        }
      }
    }
  });
};

/* Cambiar todo esto, chapuza
var redistributeRemove = function (nodeName) {
  getAllKeys(redisNodes[nodeName], function(err, keys){
    if (err){
      logger.error('getAllKeys', err);
    }
    else {
      for (var i = 0; i < keys.length; i++){
        var nextRealNode, vNode = hashing.getVNode(keys[i]);
        console.log(vNode);
        while ((nextRealNode = hashing.getNextRealNode(vNode)) === nodeName){
          //console.log(vNode);
          vNode = hashing.getNextVNode(vNode);
        }
        console.log(nextRealNode);
      }
    }
  })
};
*/



var migrateKeys = function(from, to, keys, cb) {
  var clientFrom = redisNodes[from].client;
  var clientTo = redisNodes[to].client;
  var keysPrefix = [];

  for (var i = 0; i < keys.length; i++){
    var queueId = keys[i];
    var fullQueueIdHSec = config.dbKeyQueuePrefix + 'H:SEC:' + queueId;
    var fullQueueIdLSec = config.dbKeyQueuePrefix + 'L:SEC:' + queueId;
    var fullQueueIdHUnsec = config.dbKeyQueuePrefix + 'H:UNSEC:' + queueId;
    var fullQueueIdLUnsec = config.dbKeyQueuePrefix + 'L:UNSEC:' + queueId;
    keysPrefix.push(fullQueueIdLUnsec,fullQueueIdHUnsec, fullQueueIdLSec, fullQueueIdHSec);
  }

  lua.getQueues(keysPrefix, clientFrom, function gotQueues (err, resGet){
    if (err){
      logger.error('getQueues()', 'Failed to get redis Queues : ' + err);
      cb(err);
    }
    else {
      lua.copyQueues(resGet, clientTo, function copied(err, resCopy){
        if (err){
          logger.error('copyQueues()', 'Failed to copy redis queues : + ' + err);
          cb(err);
        }
        else {
          lua.deleteQueues(keysPrefix, clientFrom);
          cb(null);
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
    cb(err, res);
  });

  function _getAllKeysParrallel (item){
    return function _getAllKeys (callback){
      getAllKeys(item, callback);
    }
  }
};

var getAllKeys = function(node, cb){

  var pattern=/\w+$/;
  node.client.keys("PB:Q|*", function onGet(err, res){
    if (err){
      logger.error('getKeys()', err);
      cb(err);
    }
    else {
      async.map(res, function transform(item, callback){
        callback(null, pattern.exec(item)[0]);
      }, cb);
    }
  });
};

var getTransactionDb = function(transactionId) {
  'use strict';
  //return a client for transactions
  return transactionDbClient;

};

var free = function (db) {
  'use strict';
  //return to the pool TechDebt
  if (db.isOwn) {
    db.pool.free(db);
  }
};

var promoteMaster = function() {
  'use strict';
  transactionDbClient.slaveof('NO', 'ONE');
  queuesDbArray.forEach(function(db) {
    db.slaveof('NO', 'ONE');
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

  redisNodes[node].client = cli;

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

  redistributeRemove('redis1');

/**
 *
 * @param {string} queu_id identifier.
 * @return {RedisClient} rc redis client for QUEUES.
 */
exports.getDb = getDb;

/**
 *
 * @param {string} queuw_id identifier.
 * @return {RedisClient} rc redis client for QUEUES.
 */
exports.getOwnDb = getOwnDb;
/**
 *
 * @param {string} transaction_id valid uuid identifier.
 * @return {RedisClient}  rc redis Client for Transactions.
 */
exports.getTransactionDb = getTransactionDb;

/**
 *
 * @param {RedisClient} db Redis DB to be closed.
 */
exports.free = free;

/**
 *
 * @type {Function}
 */
exports.promoteMaster = promoteMaster;

require('./hookLogger.js').init(exports, logger);
