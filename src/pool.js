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

//Pool modeled via Connection array
var config = require('./config.js');
var net = require('net');
var redis = require('redis');


var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();
logger.prefix = path.basename(module.filename, '.js');


var Pool = function Pool(host, port) {
  'use strict';
  this.maxElems = config.pool.maxElems || 1000;
  this.connections = [];
  this.currentConnections = 0;
  this.host = host;
  this.port = port;
}

Pool.prototype.get = function() {
  if (this.connections.length > 0) {
    var con = this.connections.pop();
    return con;
  }
  else if (!con && this.currentConnections < this.maxElems) {
    con = redis.createClient(this.port, this.host);
    con.pool = this; //add pool reference
    this.currentConnections++;
    con.on('error', function(err) {
      console.log('error - redis', err);
    });
    return con;
  } else {
    return null;
  }
}

Pool.prototype.free = function (con) {
  this.connections.push(con);
};

exports.Pool = Pool;

require('./hookLogger.js').init(exports.Pool, logger);
