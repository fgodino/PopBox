var net = require('net');
var hiredis = require('hiredis');
var reader = new hiredis.Reader();

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();
var commands = require('./proxyCommands.js');
var dbCluster = require('./dbCluster.js');
var pipeMgr = require('./pipeMgr.js');

process.setMaxListeners(20);

logger.prefix = path.basename(module.filename, '.js');

var clientInterface = require('./clientInterface.js');


var server = net.createServer(function(c) { //'connection' listener

  console.log('conectado');
  pipeMgr.newClientSocket(c);
  c.on('data', function(data){
    try {
      reader.feed(data);
      var reply = reader.get();
      console.log(reply);
      if (commands.indexOf(reply[0].toLowerCase()) < 0 || reply.length < 2){
        dbCluster.getGlobalResponse(data, function(err, res){
          c.write(createMultiBulk(res));
        });
      }
      else {
        var id = reply[1];
        var client = dbCluster.getDb(id);
        client.write(data, function(){
          dbCluster.free(client);
        });
      }
    } catch (err) {
      console.log(err);
    }
  });
});

var createMultiBulk = function (array){
  var res = '*' + array.length + '\r\n';
  for (var i = 0; i < array.length; i++) {
    res += array[i];
  }
  return res;
};


server.listen(8124, function() { //'listening' listener
  logger.info('server bound');
});
