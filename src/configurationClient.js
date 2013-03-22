
var redis = require('redis');
var uuid = require('node-uuid');

var agentId = uuid.v1();

var migrating = false;

exports.checkMigrating = function(req, res, next){
    if (migrating){
        res.send('Migrating', 500);
    } else {
        next();
    }
};

var publisher = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
var subscriber = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

publisher.pusblish('agent:new', agentId);
