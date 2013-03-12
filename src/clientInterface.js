var net = require('net'),
    config = require('./configProxy.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();

var dbCluster = require('./dbCluster.js');

logger.prefix = path.basename(module.filename, '.js');

var handler = function(socket){
    console.log("New admin client: " + socket.remoteAddress + ":" + socket.remotePort);

    socket.setEncoding('utf8');

    socket.write('Welcome to the Telnet server!\n');

    socket.on('end', function() {
        logger.info('Admin server closed');
    });

    socket.on('data', function (data) {
        options(cleanInput(data), socket);
    });

    socket.write('> ');

}

var server = net.createServer(handler);

server.listen(config.adminPort, function() { //'listening' listener
  logger.info('Admin server connected - port: ' + config.adminPort);
});

function cleanInput(data) {
    return data.toString().replace(/(\r\n|\n|\r)/gm,"");
}

var options = function(data, socket){

    var commands = data.toLowerCase().split(' ');

    switch (commands[0]) {
        case 'quit' :
            socket.end('Bye!\n');
            break;
        case 'addserver' :
            dbCluster.addNode(commands[1], commands[2], commands[3], function(err){
                console.log(err);
            });
            break;
        case 'delserver' :
            dbCluster.removeNode(commands[1], function(err){
                console.log(err);
            });
            break;
        default :
            socket.write('Invalid Command\n');
    }

    socket.write('> ');
}


