
var config = require('./config.js');
var redis = require('redis');
var uuid = require('node-uuid');
var hooker = require('hooker');
var dbCluster = require('./dbCluster');
var haMon = require('./haMonitor.js');

var agentId = uuid.v1();

var migrating = true;
var numberActive = 0;

var persistenceClient;
var rc, publisher, subscriber;

var createClient = function(port, host){
  var redisCli = redis.createClient(port, host);
  var reference = {cli : redisCli, emitter : haMon.masterChanged};
  haMon.masterChanged.on('switched', function(newMaster){
    redisCli.end();
    persistenceClient = newMaster;
    redisCli = redis.createClient(newMaster.port, newMaster.host);
    reference.cli = redisCli;
  });

  return reference;
};

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


haMon.getMaster(function(err,master){
  console.log(master);
  rc = createClient(master.port, master.host);
  publisher = createClient(master.port, master.host);
  subscriber = createClient(master.port, master.host);

  rc.cli.get('MIGRATING', function(err, res){
    if (err){
      throw new Error(err);
    } else {
      migrating = (res === 'true'); //deserialize response, boolean
      if (migrating){
        initMigrationProcess();
      } else {
        dbCluster.downloadConfig(rc.cli, function onDownloaded(err) {
          if(err){
            throw new Error(err);
          }
        });
      }
    }
  });


  setInterval(function(){
    publisher.cli.publish('agent:new', agentId);
  },2000);

  subscriber.emitter.on('changed', function(){
    subscriber.cli.subscribe("migration:new");
    subscriber.cli.on("message", function onMessage(channel, message){
      if (message === 'NEW') { //Ignore it otherwise
        if(!migrating) initMigrationProcess();
        console.log('new');
      }
    });
  });

  subscriber.emitter.emit('changed');
});


var initMigrationProcess = function(){
  migrating = true;

  var checkReady = setInterval(function checkAgain(argument) {
    if (numberActive === 0){
      publisher.cli.publish('agent:ok', agentId);
      clearInterval(checkReady);
    }
  }, 2000);

  subscriber.emitter.on('changed', function(){
    subscriber.cli.subscribe("migration:new");
    subscriber.cli.on("message", function onMessage(channel, message){
      if (message === 'OK') { //Ignore it otherwise
        subscriber.cli.removeListener('message', onMessage);
        dbCluster.downloadConfig(rc.cli, function onDownloaded(err) {
          if(err){
            throw new Error(err);
          } else {
            migrating = false;
          }
        });
      }
      else if (message === 'FAIL') { //Ignore it otherwise
        subscriber.cli.removeListener('message', onMessage);
        migrating = false;
      }
    });
  });

  subscriber.emitter.emit('changed');

};

exports.decActive = decActive;
