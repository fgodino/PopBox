var repHelper = require('./replicationHelper.js');
var hashHelper = require('./consistentHashingServer.js');
var redis = require('redis');
var config = require('./config.js');
var rc = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

var count = 0;
var agents = {};

var migrator = function(cb){

    rc.set('MIGRATING', true);

    var subscriber = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
    var publisher = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);

    if (count === 0){
      migrated();
    } else {

      publisher.publish("migration:new", "NEW");

      subscriber.subscribe("agent:ok");
      subscriber.on("message", function(channel, message){
        if(agents.hasOwnProperty(message)){
          agents[message] = true;
          count--;
        }
        if(count === 0){
          migrated();
        }
      });
    }

    function migrated(){
      cb(function(err){
        console.log('se ha migrado');
        if(!err){
          count = Object.keys(agents).length;
          for (var agent in agents){
            agents[agent] = false;
          }
          uploadRing(function(err){
            if(!err){
              rc.set('MIGRATING', false);
              publisher.publish("migration:new", "OK");
            } else {
              throw new Error(err);
            }
          });
        } else {
          throw new Error(err);
        }
      });
    };
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
    uploadRing();
    rc.set('MIGRATING', false);
    var subscriber = redis.createClient(config.persistenceRedis.port, config.persistenceRedis.host);
    subscriber.subscribe("agent:new");
    subscriber.on("message", function(channel, message){
        console.log('hola');
        if(!agents.hasOwnProperty(message)){
          count++;
          agents[message] = false;
        }
    });
  }
});

setTimeout(function(){
      migrator(function(cb){
        repHelper.addNode('redis3', 'localhost', 9999, cb);
      });
    }, 10000);

setTimeout(function(){
      migrator(function(cb){
        repHelper.removeNode('redis3', cb);
      });
    }, 20000);



exports.migrator = migrator;

