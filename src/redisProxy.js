var net = require('net');
var Parser = require('./hiredis.js').Parser;

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();
var commands = require('./proxyCommands.js');
var dbCluster = require('./dbCluster.js');

logger.prefix = path.basename(module.filename, '.js');

var clientInterface = require('./clientInterface.js');


var server = net.createServer(function(c) { //'connection' listener

  var parser = new Parser();

  c.on('data', function(data){
    try {
      parser.execute(data);
    } catch (err) {
      console.log(err);
    }
  });

  parser.on('error', function() {
    throw new Error('bad');
  });

  parser.on('reply', function(reply, data) {
    if (commands.indexOf(reply[0].toLowerCase()) < 0){
      c.write('-ERROR\r\n');
    }
    else {
      var id = reply[1];
      var client = dbCluster.getDb(id);
      client.write(data);
      client.on('data', function(data){
        c.write(data);
      });
    }
  });
});





server.listen(8124, function() { //'listening' listener
  logger.info('server bound');
});


