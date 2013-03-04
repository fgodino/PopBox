var crypto = require('crypto'),
config = require('./config.js');


var replicas = config.hashing.replicas;
algorithm = config.hashing.algorithm;

var continuum  = {}, keys = [], nodes = [];

var addNode = function(node) {
  nodes.push(node);

  for (var i = 0; i < replicas; i++) {
    var key = createHash(node + ':' + i);

    keys.push(key);
    continuum[key] = node;
  }


  keys.sort();
};


var removeNode = function(node) {
  var nodeIndex = nodes.indexOf(node);
  nodes = nodes.splice(nodeIndex, 1);

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

var getVNodePosition = function(vNode){
  return keys.indexOf(createHash(vNode));
}

var getKeySpace = function(vNode){;
  var posNode = getVNodePosition(vNode);
  var init, end;
  if(posNode === 0){
    init = keys[keys.length - 1];
  }
  else {
    init = keys[posNode - 1];
  }
  end = keys[posNode];
  return [init, end];
};

var getNextVNode = function (vNode){
  var posNode = getVNodePosition(vNode);
  if (posNode === keys.length - 1){
    return keys[0]; //Next would be the first (ring structure)
  }
  else {
    return keys[posNode + 1];
  }
};

var getNextRealNode = function(vNode){
  return continuum[getNextVNode(vNode)];
};

var inRange = function(key, range){
  return (key >= range[0] && key < range[1]);
}


exports.getNode = getNode;
exports.addNode = addNode;
exports.removeNode = removeNode;
exports.getKeySpace = getKeySpace;
exports.getNextRealNode = getNextRealNode;
