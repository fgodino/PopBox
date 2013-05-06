var config = require('./config.js');
var redis = require('redis');
var events = require('events');
var redisSent = redis.createClient(config.redisSentinel.port, config.redisSentinel.host);
var redisSentMon = redis.createClient(config.redisSentinel.port, config.redisSentinel.host);

redisSentMon.psubscribe('*');


var getMaster = function(callback){
  redisSent.send_command('SENTINEL', ['get-master-addr-by-name', config.masterName], function(err, res){
    if(!err){
      var result = {};
      result.host = res[0];
      result.port = res[1];
    }
    callback(err, result);
  })
};

var masterChanged = new events.EventEmitter();

redisSentMon.on('pmessage', function(pattern, channel, message){
  console.log(channel);
  if(channel === "+switch-master"){
    var data = message.split(' ');
    var results = {host : data[3], port : data[4]};
    masterChanged.emit('switched', results);
  }
});

exports.masterChanged = masterChanged;
exports.getMaster = getMaster;
