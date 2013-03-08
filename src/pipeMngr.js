

var clientSockets = [];
var redisSockets = [];

var createPipes = function (from, to){
    for(var i = 0; i < from.length; i++){
        for(var j = 0; j < to.length; j++){
            from[i].pipe(j);
        }
    }
};

exports.createPipes = createPipes;
