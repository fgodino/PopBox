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
var hashing = require('./consistent_hashing');
var async = require('async');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();

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

var transactionDbClient = redisModule.createClient(config.tranRedisServer.port ||
    redisModule.DEFAULT_PORT, config.tranRedisServer.host);
require('./hookLogger.js').initRedisHook(transactionDbClient, logger);
if (config.slave) {
  slaveOf(transactionDbClient, config.masterTranRedisServer.host,
      config.masterTranRedisServer.port);
}

transactionDbClient.select(config.selectedDB); //false pool for pushing

for (var node in config.redisServers) {
  var port = config.redisServers[node].port || redisModule.DEFAULT_PORT;
  var host = config.redisServers[node].host;
  var cli = redisModule.createClient(port, host);

  config.redisServers[node].client = cli;

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

//Create the pool array - One pool for each server
var poolArray = [];
for (var i = 0; i < config.redisServers.length; i++) {
  var pool = poolMod.Pool(i);
  poolArray.push(pool);
}

var getDb = function(queueId) {
  'use strict';
  var node = hashing.getNode(queueId);
  return config.redisServers[node].client;
};

var getOwnDb = function(queueId, callback) {
  'use strict';
  var hash = hashMe(queueId, config.redisServers.length);
  //get the pool
  var pool = poolArray[hash];
  pool.get(queueId, callback);
};

var redistributeAdd = function(newNode){
  var newNodeCli = redisModule.createClient(newNode.port, newNode.host);

  calculateDistribution(function distribution(err, items){
    for (var node in items){
      for(var i = 0; i < items[node].length; i++){
        var key = items[node][i];
        if(hashing.getNode(key) != newNode.name){
          items[node].splice(i, 1);
        }
      }
    }
    migrateKeys()
  });
};


var migrateKeys = function(from, to, keys) {
  var clientFrom = config.redisServers[from].client;
  var clientTo = config.redisServers[to].client;


}

var calculateDistribution = function(cb){
  var nodeFunctions = {}; //Parallel functions as an object to receive results in an object.
  for(var node in config.redisServers){
    nodeFunctions[node] = _getAllKeysParrallel(config.redisServers[node]);
  }

  async.parallel(nodeFunctions, cb);

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
      cb(err);
    }
    else {
    async.map(res, function transform(item, cb){
      cb(null, pattern.exec(item)[0]);
    }, cb);
  }
  });
};

hashing.addNode('redis3');
redistributeAdd({name : 'redis3', host : 'localhost', port : 7777});

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
