var repHelper = require('./replicationHelper.js');
var hashHelper = require('./consistentHashingServer.js');
var redis = require('redis');
var config = require('./config.js');
var uuid = require("node-uuid");
var rc = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

var serverId = uuid.v1();
var count = 0;
var agents = {};
var available = false;

rc.get('MIGRATING', function(err, migrating){
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

exports.checkMigrating = function(req, res, next){
  if(available){
    next();
  }
  else {
    res.send('Not available', 500);
  }
};

var migrator = function(cb){

  rc.set('MIGRATING', true);
  available = false;

  var subscriber = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
  var publisher = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

  setTimeout(delayedMigrate, 4000);

  function delayedMigrate(){
    if (count === 0){
      migrated();
    } else {

      publisher.publish("migration:new", "NEW");

      subscriber.subscribe("agent:ok");
      subscriber.on("message", function(channel, message){
        if(agents.hasOwnProperty(message) && agents[message].ok){
          agents[message].ready = true;
          count--;
          if(count === 0){
            subscriber.unsubscribe("agent:ok");
            migrated();
          }
        }
      });
    }

    function migrated(){
      cb(function(err){
        if(!err){
          uploadRing(function(err){
            if(!err){
              rc.set('MIGRATING', false);
              available = true;
              publisher.publish("migration:new", "OK");
            } else {
              throw new Error(err);
            }
          });
        } else {
          publisher.publish("migration:new", "FAIL");
          rc.set('MIGRATING', false);
          available = true;
        }
        count = Object.keys(agents).length;
        for (var agent in agents){
          agents[agent].ready = false;
        }
      });
    };
  };
};

var monitorAgent = function(monitor, id){
  var num_tries = 0;

  function newMessage(channel, message){
    if(message === id){
      num_tries = 0;
    }
  }

  monitor.on('message', newMessage);

  var monitorInterval = setInterval(function(){
    num_tries++;
    if(num_tries > 3){
      console.log('peto agente');
      monitor.removeListener('connection', newMessage);
      agents[id].ok = false;
      count--;
      clearTimeout(monitorInterval);
    }
  }, 2000);
};

var uploadRing = function(cb){
  var continuum = hashHelper.getContinuum();
  var keys = hashHelper.getKeys();
  var nodes = repHelper.getNodes();

  var multi = rc.multi();

  multi.del('CONTINUUM');
  multi.del('KEYS');
  multi.del('NODES');
  multi.hmset('CONTINUUM', continuum);
  multi.rpush('KEYS', keys);
  multi.hmset('NODES', nodes);

  multi.exec(cb);
};

repHelper.generateNodes();

repHelper.bootstrapMigration(function(err){
  if(err){
    console.log(err);
  }
  else {
    var subscriberAgent = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
    var subscriberConfig = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
    subscriberAgent.subscribe("agent:new");
    subscriberConfig.subscribe("migration:new");
    subscriberAgent.on("message", function(channel, message){
      if(!agents.hasOwnProperty(message)){
        monitorAgent(subscriberAgent, message);
        count++;
        agents[message] = {ready : false, ok : true};
      }
    });
    subscriberConfig.on("message", function(channel, message){
      if(message === "OK"){
        available = false;
        repHelper.downloadConfig(function(err){
          if (!err) available = true;
        })
      }
    });
    migrator(function(cb){
      cb();
    });
  }
});

var addNode = function(req, res){
  var host = req.body.host || 'localhost', port = req.body.port, name = req.body.name;
  var errors = []

  console.log(host, port, name);

  if (!req.headers['content-type'] || req.headers['content-type'] !== 'application/json') {
    errors.push('Invalid content-type')
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
    errors.push('Invalid content-type')
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
}

exports.delNode = delNode;
exports.addNode = addNode;
exports.getAgents = getAgents;


