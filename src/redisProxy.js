var net = require('net');
var hiredis = require("hiredis"),
    reader = new hiredis.Reader();

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();

logger.prefix = path.basename(module.filename, '.js');

var clientInterface = require('./clientInterface.js');

var pipeMngr = require('./pipeMngr.js')
var dbCluster = require('./dbCluster.js');

var server = net.createServer(function(c) { //'connection' listener

  pipeMngr.newClientSocket(c);

  c.on('end', function() {
    pipeMngr.removeClientSocket(c);
  });

  c.on('data', function(data){
    reader.feed(data);
    var id = reader.get()[1];
    if(id){
      var client = dbCluster.getDb(id);
      client.write(data);
    }
  });
});

server.listen(8124, function() { //'listening' listener
  logger.info('server bound');
});


