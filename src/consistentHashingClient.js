var crypto = require('crypto'),
config = require('./config.js');

var algorithm = 'md5';

var continuum  = {}, keys = [];

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

var setKeys = function(_keys){
  keys = _keys;
}

var setContinuum = function(_continuum){
  continuum = _continuum;
}

var createHash = function(str) {
  return crypto.createHash(algorithm).update(str).digest('hex');
};



exports.getNode = getNode;
exports.getKey = getKey;
exports.setKeys = setKeys;
exports.setContinuum = setContinuum;
