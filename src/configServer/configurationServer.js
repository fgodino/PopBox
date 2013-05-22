var repHelper = require('./replicationHelper.js');
var hashHelper = require('./consistentHashingServer.js');
var redis = require('redis');
var config = require('./config.js');
var haMon = require('./haMonitor.js');
var uuid = require("node-uuid");
var events = require('events');

var serverId = uuid.v1();
var count = 0;
var agents = {};
var available = false;
var persistenceClient;
var rc;

var subscriberAgent;
var subscriberConfig;

var createClient = function(port, host){
  var redisCli = redis.createClient(port, host);
  var eventEmitter = new events.EventEmitter();
  var reference = {cli : redisCli, emitter : eventEmitter};
  haMon.masterChanged.on('switched', function(newMaster){
    redisCli.end();
    persistenceClient = newMaster;
    redisCli = redis.createClient(newMaster.port, newMaster.host);
    reference.cli = redisCli;
    reference.emitter.emit('changed');
  });

  return reference;
};

exports.checkMigrating = function(req, res, next){
  if(available){
    next();
  }
  else {
    res.send('Not available', 500);
  }
};

var migrator = function(cb){

  rc.cli.set('MIGRATING', true);
  available = false;

  var subscriber = createClient(persistenceClient.port, persistenceClient.host);
  var publisher = createClient(persistenceClient.port, persistenceClient.host);

  setTimeout(delayedMigrate, 4000);

  function delayedMigrate(){

    var countAux = count;

    if (countAux === 0){
      migrated();
    } else {

      publisher.cli.publish("migration:new", "NEW");

      subscriber.cli.subscribe("agent:ok");
      subscriber.cli.on("message", function(channel, message){
        if(agents.hasOwnProperty(message) && agents[message].ok){
          agents[message].ready = true;
          countAux--;
        }
      });

      var checkInterval = setInterval(function checkCounter(){
        if(countAux === 0){
          subscriber.cli.unsubscribe("agent:ok");
          clearInterval(checkInterval);
          migrated();
        }
      }, 2000);
    }


    function migrated(){
      cb(function(err){
        if(!err){
          uploadRing(function(err){
            if(!err){
              rc.cli.set('MIGRATING', false);
              available = true;
              publisher.cli.publish("migration:new", "OK");
            } else {
              throw new Error(err);
            }
          });
        } else {
          publisher.cli.publish("migration:new", "FAIL");
          rc.cli.set('MIGRATING', false);
          available = true;
        }
        for (var agent in agents){
          agents[agent].ready = false;
        }
      });
    }
  }
};

setInterval(function(){
  console.log(count);
},3000);

var monitorAgent = function(monitor, id){
  var num_tries = 0;

  function newMessage(channel, message){
    if(message === id){
      num_tries = 0;
    }
  }

  monitor.cli.on('message', newMessage);

  var monitorInterval = setInterval(function(){
    num_tries++;
    if(num_tries > 4){
      console.log('peto agente');
      monitor.cli.removeListener('message', newMessage);
      agents[id].ok = false;
      count--;
      clearInterval(monitorInterval);
    }
  }, 3000);
};

var uploadRing = function(cb){
  var continuum = hashHelper.getContinuum();
  var keys = hashHelper.getKeys();
  var nodes = repHelper.getNodesSerialized();

  var multi = rc.cli.multi();

  multi.del('CONTINUUM');
  multi.del('KEYS');
  multi.del('NODES');
  multi.hmset('CONTINUUM', continuum);
  multi.rpush('KEYS', keys);
  multi.hmset('NODES', nodes);

  multi.exec(cb);
};

repHelper.generateNodes();

haMon.getMaster(function(err,master){
  console.log(master);
  if(err){
    return;
  }
  persistenceClient = master;
  rc = createClient(persistenceClient.port, persistenceClient.host);
  rc.cli.get('MIGRATING', function(err, migrating){
    if(err){
      available = false;
    } else {
      if (migrating=="true"){
        available = false;
      } else {
        available = true;
      }
    }
  });
  repHelper.bootstrapMigration(function(err){
    if(err){
      console.log(err);
    }
    else {
      var subscriberAgent = createClient(persistenceClient.port,persistenceClient.host);
      var subscriberConfig = createClient(persistenceClient.port, persistenceClient.host);

      subscriberAgent.emitter.on('changed', function(){
        subscriberAgent.cli.subscribe("agent:new");
        subscriberAgent.cli.on("message", function(channel, message){
          if(!agents.hasOwnProperty(message) || !agents[message].ok){
            monitorAgent(subscriberAgent, message);
            count++;
            agents[message] = {ready : false, ok : true};
          }
        });
      });

      subscriberConfig.emitter.on('changed', function(){
        subscriberConfig.cli.subscribe("migration:new");
        subscriberConfig.cli.on("message", function(channel, message){
          if(message === "OK"){
            available = false;
            repHelper.downloadConfig(rc.cli,function(err){
              if (!err){
                available = true;
              }
            });
          }
        });
      });

      subscriberConfig.emitter.emit('changed');
      subscriberAgent.emitter.emit('changed');

      migrator(function(cb){
        cb();
      });
    }
  });
});

var addNode = function(req, res){
  var host = req.body.host || 'localhost', port = req.body.port, name = req.body.name;
  var errors = [];

  console.log(host, port, name);

  if (!req.headers['content-type'] || req.headers['content-type'] !== 'application/json') {
    errors.push('Invalid content-type');
  }
  if (!host || !port || !name){
    errors.push('missing fields');
  }
  if(errors.length > 0){
    res.send({errors: errors}, 400);
    return;
  }
  migrator(function(cb){
    repHelper.addNode(name, host, port, function(err){
      if(err){
        res.send({errors: [err]}, 400);
      } else {
        res.send({ok: true}, 200);
      }
      cb(err);
    });
  });
};

var delNode = function(req, res){
  var name = req.body.name;
  var errors = [];

  if (!req.headers['content-type'] || req.headers['content-type'] !== 'application/json') {
    errors.push('Invalid content-type');
  }
  if (!name){
    errors.push('missing fields');
  }
  if(errors.length > 0){
    res.send({errors: errors}, 400);
    return;
  }
  migrator(function(cb){
    repHelper.removeNode(name, function(err){
      if(err){
        res.send({errors: [err]}, 400);
      } else {
        res.send({ok: true}, 200);
      }
      cb(err);
    });
  });
};

var getAgents = function(req, res){
  res.send(agents,200);
};



exports.delNode = delNode;
exports.addNode = addNode;
exports.getAgents = getAgents;
exports.migrator = migrator;

