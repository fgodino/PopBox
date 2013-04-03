
var config = require('./config.js');
var redis = require('redis');
var uuid = require('node-uuid');
var hooker = require('hooker');

var agentId = uuid.v1();

var migrating = false;
var numberActive = 0;

exports.checkMigrating = function(req, res, next){
    if (migrating){
        res.send('Migrating', 500);
    } else {
        next();
    }
};

exports.init = function(exp){
  hooker.hook(exp, {
    passName: true,
    pre: function() {
      numberActive++;
      console.log('antes');
    },
    post: function() {
      numberActive--;
      console.log('despues');
    }
  });
};

var publisher = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
var subscriber = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

publisher.publish('agent:new', agentId);
subscriber.subscribe('migration:new');

subscriber.on('message', function(channel, message){
  switch(message){
    case ''
  }
});

var initMigrationProcess = function(){

}
