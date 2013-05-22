var config = require('./config.js');
var redis = require('redis');
var events = require('events');
var fs = require('fs');
var util = require('util');
var uuid = require('node-uuid');
var portchecker = require('portchecker');
var childProcess = require('child_process');
var configServer = require('./configurationServer');
var repHelper = require('./replicationHelper.js');

var redisSent = redis.createClient(config.redisSentinel.port, config.redisSentinel.host);
var redisSentMon = redis.createClient(config.redisSentinel.port, config.redisSentinel.host);

var sentinelCli;

redisSentMon.subscribe('+switch-master');


var getMaster = function(callback){
  redisSent.send_command('SENTINEL', ['get-master-addr-by-name', config.masterName], function(err, res){
    if(!err){
      var result = {};
      result.host = res[0];
      result.port = res[1];
    }
    callback(err, result);
  });
};

var sentinelConfig = "sentinel monitor %s %s %d %d\n" +
"sentinel down-after-milliseconds %s 5000\n" +
"sentinel failover-timeout %s 900000\n" +
"sentinel can-failover %s yes\n" +
"sentinel parallel-syncs %s 1\n\n";

var newSentinel = function(nodes){
  var newConfig = "";
  for(var node in nodes){
    var nodeInf = nodes[node];
    newConfig = newConfig.concat(util.format(sentinelConfig, node, nodeInf.host, nodeInf.port, 1, node, node, node, node));
  }
  var configId = uuid.v1();
  fs.writeFileSync(configId, newConfig);
  portchecker.getFirstAvailable(5000, 6000, 'localhost', function(port, host) {
    console.log("new sentinel", port);

    var sentinelProc = childProcess.spawn('redis-server', [configId, '--sentinel', '--port', port], {
      detached: false
    });

    sentinelCli = redis.createClient(port, 'localhost');

    sentinelCli.on('ready', function(){
      persistenceMasterMonitor(sentinelCli);
    });
  });
};

var persistenceMasterMonitor = function(cli){
  console.log('message');
  cli.subscribe('+switch-master');
  cli.subscribe('+odown');
  cli.on('message', function(channel, message){
    console.log(message);
    switch(channel){
      case '+switch-master' :
        var lastNodes = repHelper.getNodes();
        var splitted = message.split(' ');
        var name = splitted[0], host = splitted[3], port = splitted[4];
        var cli = redis.createClient(port, host);
        lastNodes[name].redisClient.end();
        lastNodes[name] = {host : host, port : port, redisClient : cli};
        configServer.migrator(function(cb){
          repHelper.setNodes(lastNodes);
          cb();
        });
        break;
      case '+odown' :

    }
  });
};


var masterChanged = new events.EventEmitter();

redisSentMon.on('message', function(channel, message){
  var data = message.split(' ');
  var results = {host : data[3], port : data[4]};
  masterChanged.emit('switched', results);
});

exports.masterChanged = masterChanged;
exports.getMaster = getMaster;
exports.newSentinel = newSentinel;
