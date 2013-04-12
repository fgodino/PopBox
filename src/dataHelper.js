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


//Require Area
var config = require('./config.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();
var cs = require('./consistentHashingClient.js');

var getHKey = cs.getKey;

logger.prefix = path.basename(module.filename, '.js');

var setKey = function(db, extqueueId, id, value, callback) {
  'use strict';
  var idKey = id + '{' + getHKey(extqueueId) + '}';
  db.set(idKey, value, function onSet(err) {
    if (err) {
      //error pushing
      logger.warning(err);
    }

    if (callback) {
      callback(err);
    }
  });
};

var getKey = function(db, extqueueId, id, callback) {
  'use strict';
  var idKey = id + '{' + getHKey(id) + '}';
  db.get(idKey, function(err, value) {
    if (err) {
      //error pushing
      logger.warning(err);
    }

    if (callback) {
      callback(err, value);
    }
  });
};

var exists = function(db, id, callback) {
  'use strict';
  var idKey = id + '{' + getHKey(id) + '}';
  db.exists(idKey, function(err, value) {
    if (err) {
      //error pushing
      logger.warning(err);
    }
    if (callback) {
      callback(err, value === 1);
    }
  });
};

var pushParallel = function(db, queueid, queue, priority, transaction_id) {
  'use strict';
  return function asyncPushParallel(callback) {
    var fullQueueId = config.dbKeyQueuePrefix + priority + queue.id + '{' + getHKey(queueid) + '}';
    db.rpush(fullQueueId, transaction_id, function onLpushed(err) {
      if (err) {
        //error pushing
        logger.warning(err);
      }
      if (callback) {
        callback(err);
      }
    });
  };
};

var hsetHashParallel = function(dbTr, queue, extTransactionId, transactionId, datastr) {
  'use strict';

  console.log(queue, extTransactionId, transactionId, datastr);
  var idKey = transactionId + '{' + getHKey(extTransactionId) + '}';
  console.log(idKey);
  return function asyncHsetHashParallel(callback) {
    dbTr.hmset(idKey, queue, datastr, function(err) {
      if (err) {
        //error pushing
        logger.warning(err);
      }

      if (callback) {
        callback(err);
      }
    });
  };
};

var hsetMetaHashParallel = function(dbTr, extTransactionId, transaction_id, provision) {
  'use strict';

  var idKey = transaction_id + '{' + getHKey(extTransactionId) + '}';
  return function asyncHsetMetaHash(callback) {
    /*var meta =
     {
     'payload': provision.payload,
     'priority': provision.priority,
     'callback': provision.callback,
     'expirationDate': provision.expirationDate
     };
     */
    var meta = {};
    for (var p in provision) {
      if (provision.hasOwnProperty(p) && provision[p] !== null &&
          provision[p] !== undefined && p !== 'queue') {
        meta[p] = provision[p];
      }
    }

    dbTr.hmset(idKey, meta, function onHmset(err) {
      if (err) {
        //error pushing
        logger.warning('onHmset', err);
      }
      callback(err);
    });
  };
};

var setExpirationDate = function(dbTr, extTransactionId, key, provision, callback) {
  'use strict';
  var idKey = key + '{' + getHKey(extTransactionId) + '}';
  if (provision.expirationDate) {
    dbTr.expireat(idKey, provision.expirationDate, function onExpireat(err) {
      if (err) {
        //error setting expiration date
        logger.warning('onExpireat', err);
      }
      if (callback) {
        callback(err);
      }

    });
  }
  else {
    if (callback) {
      callback(null);
    }
  }
};

//Public area

/**
 *
 * @param {RedisClient} db valid redis client.
 * @param {PopBox.Queue} queue object.
 * @param {string} priority enum type 'H' || 'L' for high low priority.
 * @param {string} transaction_id valid transaction identifier.
 * @return {function(function)} asyncPushParallel ready for async module.
 */
exports.pushParallel = pushParallel;
/**
 *
 * @param {RedisClient} dbTr valid redis client.
 * @param {PopBox.Queue} queue object with a valid id.
 * @param {string} transactionId valid uuid.
 * @param {string} sufix for redis key.
 * @param {string} datastr to be kept.
 * @return {function(function)} asyncHsetHashParallel function ready for async.
 */
exports.hsetHashParallel = hsetHashParallel;
/**
 *
 * @param {RedisClient} dbTr  valid redis client.
 * @param {string} transaction_id valid transaction identifier.
 * @param {string} sufix for the key, usually ':meta' or ':state'.
 * @param {Provision} provision object.
 * @return {function(function)} asyncHsetMetaHash function ready for async.
 */
exports.hsetMetaHashParallel = hsetMetaHashParallel;
/**
 *
 * @param {Object} dbTr valid redis Client.
 * @param {string} key collection Key to expire.
 * @param {Provision} provision provision object to extract expiration times.
 * @param {function(Object)} callback with error param.
 */
exports.setExpirationDate = setExpirationDate;


/**
 *
 * @param db
 * @param id
 * @param value
 * @param callback
 */
exports.setKey = setKey;

exports.getKey = getKey;

exports.exists = exists;

require('./hookLogger.js').init(exports, logger);
