
var config = require('./config.js');
var redis = require('redis');
var uuid = require('node-uuid');
var hooker = require('hooker');
var dbCluster = require('./dbCluster');

var agentId = uuid.v1();

var migrating = true;
var numberActive = 0;

var migChecker = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
migChecker.get('MIGRATING', function(err, res){
  if (err){
    throw new Error(err);
  } else {
    migrating = (res === 'true'); //deserialize response, boolean
    if (migrating){
      initMigrationProcess();
    } else {
      dbCluster.downloadConfig(function onDownloaded(err) {
        if(err){
          throw new Error(err);
        }
      });
    }
  }
});

exports.checkMigrating = function(req, res, next){
  if (migrating){
      res.send('Migrating', 500);
  } else {
      next();
  }
};


var incActive = function() {
  numberActive++;
  console.log('antes: ' + numberActive);
};

var decActive = function(){
  numberActive--;
  console.log('despues ' + numberActive);
};

exports.init = function(exp){
  hooker.hook(exp, {
    passName: true,
    pre: function() {
      incActive();
    }
  });
};

var publisher = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
var subscriber = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

setInterval(function(){
  publisher.publish('agent:new', agentId);
},2000);

subscriber.subscribe('migration:new');

subscriber.on('message', function(channel, message){
  if (message === 'NEW') { //Ignore it otherwise
    if(!migrating) initMigrationProcess();
    console.log('new');
  }
});


var initMigrationProcess = function(){
  migrating = true;
  var checkReady = setInterval(function checkAgain(argument) {
          console.log('check');
    if (numberActive === 0){
      publisher.publish('agent:ok', agentId);
      clearTimeout(checkReady);
    }
  }, 2000);
  subscriber.on('message', function onMessage(channel, message){
    if (message === 'OK') { //Ignore it otherwise
      subscriber.removeListener('message', onMessage);
      dbCluster.downloadConfig(function onDownloaded(err) {
        if(err){
          throw new Error(err);
        } else {
          migrating = false;
        }
      });
    }
    else if (message === 'FAIL') { //Ignore it otherwise
      subscriber.removeListener('message', onMessage);
      migrating = false;
    }
  });
};

exports.decActive = decActive;
