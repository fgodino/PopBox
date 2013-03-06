var net = require('net'),
    config = require('./config.js');

var path = require('path');
var log = require('PDITCLogger');
var logger = log.newLogger();

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

server.listen(config.agent.adminPort, function() { //'listening' listener
  logger.info('Admin server connected - port: ' + config.agent.adminPort);
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
            break;
        case 'delserver' :
            break;
        default :
            socket.write('Invalid Command\n');
    }

    socket.write('> ');
}


