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
var hashing = require('./consistentHashingClient.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();

logger.prefix = path.basename(module.filename, '.js');

var redisNodes = {};


var getDb = function(hash) {
  'use strict';
  logger.debug('getDb()', node);
  var node = hashing.getNode(hash);

  return redisNodes[node].pool.get();
};

var free = function (db) {
  'use strict';
  db.pool.free(db);
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
    console.log(Object.keys(deserializedNodes));
    for (var node in deserializedNodes){
      console.log(node);
      var port = deserializedNodes[node].port || redisModule.DEFAULT_PORT;
      var host = deserializedNodes[node].host;


      var Pool = poolMod.Pool;

      redisNodes[node] = {host : host, port : port};
      redisNodes[node].pool =  new Pool(host, port);

      logger.info('Connected to REDIS ', host + ':' + port);
    }
    callback(err);
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
 * @param {RedisClient} db Redis DB to be closed.
 */
exports.free = free;

exports.downloadConfig = downloadConfig;

require('./hookLogger.js').init(exports, logger);
