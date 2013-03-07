var net = require('net');
var hiredis = require("hiredis"),
    reader = new hiredis.Reader();

var dbCluster = require('./dbCluster.js');


var server = net.createServer(function(c) { //'connection' listener
  console.log('server connected');

  c.on('end', function() {
    console.log('server disconnected');
  });
  c.on('data', function(data){
    reader.feed(data);
    var client = dbCluster.getDb(reader.get()[1]);
    client.write(data);
    client.pipe(c);
  });
});


server.listen(8124, function() { //'listening' listener
  console.log('server bound');
});


