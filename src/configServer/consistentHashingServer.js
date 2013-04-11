var crypto = require('crypto'),
config = require('./config.js');


var replicas = config.hashing.replicas;
algorithm = config.hashing.algorithm;

var continuum  = {};
var partitions = Math.pow(2,5); //Must be a power of two
var md5spacelength = Math.pow(2,128);
var keyspacelength = md5spacelength / partitions;

var keys = new Array(partitions);
var vNodeToNode = {}, keyToVNode = {}, nodes = [];

for(i = 0; i < partitions; i++){
  keys[i] = (keyspacelength * i).toString(16);
}

var addNode = function(node) {

  nodes.push(node);
  var numNodes = nodes.length;
  var replicas = Math.floor(partitions / numNodes);

  for (var i=(numNodes-1), j=0; j < replicas; i = (i + numNodes), j++) {
    var key = keys[i];
    continuum[key] = node;
  }
    console.log(continuum);
};

var getContinuum = function(){
  return continuum;
};

var getKeys = function(){
  return keys;
}

var removeNode = function(node) {

  var remIndex = nodes.indexOf(node);
  nodes.splice(remIndex, 1);

  for (var i = 0, j = 0; i < keys.length; i++){
    var key = keys[i];
    if(continuum[key] === node){
      continuum[key] = nodes[(j % nodes.length)];
      j++;
    }
  }
  console.log(continuum);
};


var getNode = function(key) {
  if (keys.length === 0) return 0;

  var hash = createHash(key);
  var pos  = getNodePosition(hash);

  return continuum[keys[pos]];
};

var getKey = function(key){
  var hash = createHash(key);
  var pos  = getNodePosition(hash);
  return keys[pos];
}

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
