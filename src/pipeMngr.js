

var clientSockets = [];
var redisSockets = [];

var addClientSocket = function (socket){
    clientSockets.push(socket);
};

var addRedisSocket = function (socket){
    redisSockets.push(socket);
};

var removeClientSocket = function (socket){
    clientSockets.splice(clientSockets.indexOf(socket), 1);
};

var removeRedisSocket = function (socket){
    redisSockets.splice(redisSockets.indexOf(socket), 1);
}

var newRedisSocket = function (socket){
    redisSockets.push(socket);
    for (var i = 0; i < clientSockets.length; i++) {
        socket.pipe(clientSockets[i]);
    }
}

var newClientSocket = function(socket) {
    clientSockets.push(socket);
    for (var i = 0; i < redisSockets.length; i++) {
        redisSockets[i].pipe(socket);
    }
}


exports.addClientSocket = addClientSocket;

exports.addRedisSocket = addRedisSocket;

exports.newClientSocket = newClientSocket;

exports.newRedisSocket = newRedisSocket;

exports.removeRedisSocket = removeRedisSocket;

exports.removeClientSocket = removeClientSocket;
