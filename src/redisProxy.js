var net = require('net');
var hiredis = require("hiredis"),
    reader = new hiredis.Reader();

var pipeMngr = require('./pipeMngr.js')
var dbCluster = require('./dbCluster.js');

var sockets = [];

var server = net.createServer(function(c) { //'connection' listener
  console.log('server connected');

  sockets.push(c);

  dbCluster.createPipes(c);

  c.on('end', function() {
    sockets.splice(sockets.indexOf(c));
    console.log('server disconnected');
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
  console.log('server bound');
});


