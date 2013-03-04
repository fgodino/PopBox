/**
 * Created with JetBrains WebStorm.
 * User: mru
 * Date: 24/10/12
 * Time: 09:12
 * To change this template use File | Settings | File Templates.
 */

var redisModule = require('redis');
var config = require('./config.js');

var deleteScript = "\
for j, k in pairs(KEYS) do\n\
    redis.call('del', k)\n\
end\n\
return 1\n\
";

var peekScript = "\
local err = nil\n\
local result={}\n\
for j, k in ipairs(KEYS) do\n\
    local data = redis.pcall('lrange', k, 0, -1)\n\
    if (data.err) then return {err = data.err} end\n\
    table.insert(result, k)\n\
    table.insert(result, data)\n\
end\n\
return result\n\
";

var insertList = "\
local rollback = function(rollbackData)\n\
  redis.call('del', rollbackData)\n\
end\n\
local lpush = redis.pcall('lpush', ARGV[1], unpack(KEYS))\n\
if (type(lpush) == 'table') then\n\
  rollback(ARGV[1]);\n\
  return {err = lpush.err}\n\
end\n\
return lpush\n\
"


//TODO: Without multi, array aproach.
var copyQueues = function (transaction, redis, cb){
  var content = 0;
  var multi = redis.multi();
  for (var i = 0; i < transaction.length; i +=2){
    redis.watch(transaction[i]);
    var keys = transaction[i+1];
    if (keys.length > 0){
      content++;
      var join = keys.concat(transaction[i]);
      multi.eval(insertList, keys.length, join);
    }
  };
  multi.exec(cb);
};

var getQueues = function(queuesIds, redis, cb){
  var keys = [];
  for (var i = 0; i < queuesIds.length; i++){
    var queueId = queuesIds[i];
    var fullQueueIdHSec = config.dbKeyQueuePrefix + 'H:SEC:' + queueId;
    var fullQueueIdLSec = config.dbKeyQueuePrefix + 'L:SEC:' + queueId;
    var fullQueueIdHUnsec = config.dbKeyQueuePrefix + 'H:UNSEC:' + queueId;
    var fullQueueIdLUnsec = config.dbKeyQueuePrefix + 'L:UNSEC:' + queueId;
    keys.push(fullQueueIdLUnsec,fullQueueIdHUnsec, fullQueueIdLSec, fullQueueIdHSec);
  }
  redis.eval(peekScript, keys.length, keys, cb);
}

var deleteQueues = function(queuesIds, redis){
  redis.eval(deleteScript, queuesIds.length, queuesIds);
}

exports.copyQueues = copyQueues;
exports.getQueues = getQueues;
exports.deleteQueues = deleteQueues;
