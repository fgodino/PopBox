var crypto = require('crypto'),
config = require('./config.js');


var replicas = config.hashing.replicas;
algorithm = config.hashing.algorithm;

var continuum  = {}, keys = [];

var addNode = function(node) {

  for (var i = 0; i < replicas; i++) {
    var key = createHash(node + ':' + i);

    keys.push(key);
    continuum[key] = node;
  }
  keys.sort();
};

var getContinuum = function(){
  return continuum;
};

var getKeys = function(){
  return keys;
}

var removeNode = function(node) {

  for (var i = 0; i < replicas; i++) {
    var key = createHash(node + ':' + i);
    delete continuum[key];

    var keyIndex = keys.indexOf(key);
    keys.splice(keyIndex, 1);
  }
};


var getNode = function(key) {
  if (keys.length === 0) return 0;

  var hash = createHash(key);
  var pos  = getNodePosition(hash);

  return continuum[keys[pos]];
};

//binary search

var getNodePosition = function(hash) {
  var down = 0, up = keys.length - 1;
  while(down <= up){
    var center = Math.floor((up + down) / 2);
    var centerValue = keys[center];
    if (centerValue === hash) { return center; }
    else if (hash < centerValue){
      up = center - 1;
    }
    else{
      down = center + 1;
    }
  }
  if (up < 0) {
    up = keys.length - 1;
  }
  return up;
};

var createHash = function(str) {
  return crypto.createHash(algorithm).update(str).digest('hex');
};



exports.getNode = getNode;
exports.addNode = addNode;
exports.removeNode = removeNode;
exports.getContinuum = getContinuum;
exports.getKeys = getKeys;
