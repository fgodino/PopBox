/**
 * Created with JetBrains WebStorm.
 * User: mru
 * Date: 24/10/12
 * Time: 09:12
 * To change this template use File | Settings | File Templates.
 */

var redisModule = require('redis');
var rc = redisModule.createClient(6379, 'localhost');
var keys = ['PB:Q|H:UNSEC:Bx', 'dos', 'tres'];

var deleteScript = "\
local rollback = function(data, key)\n\
  for i, elem in pairs(data) do\n\
    redis.call('lpush',key, elem)\n\
  end\n\
end\n\
local rollbackAll = function(rollbackData)\n\
  for k,d in pairs(rollbackData) do\n\
    rollback(d, k)\n\
  end\n\
end\n\
local err = nil\n\
local result={}\n\
local rollbackData = {}\n\
for j, k in pairs(KEYS) do\n\
    local data = redis.pcall('lrange', k, 0, -1)\n\
    if (data.err) then\n\
      rollbackAll(rollbackData);\n\
      return err = data.err;\n\
    end\n\
    rollbackData[k] = data\n\
    local del = redis.pcall('del', k)\n\
    if (trimerr.err) then\n\
      rollbackAll(rollbackData)\n\
      return {err = trimerr.err}\n\
    end\n\
end\n\
return 1\n\
";

var peekScript = "\
local err = nil\n\
local result={}\n\
for j, k in ipairs(KEYS) do\n\
    local data = redis.pcall('lrange', k, 0, 1)\n\
    if data.err then return {err = data.err} end\n\
    table.insert(result, k)\n\
    table.insert(result, data)\n\
end\n\
return result\n\
";

var insertTrans = "\
local rollback = function(rollbackData)\n\
  redis.call('del', rollbackData)\n\
end\n\
for j, k in ipairs(KEYS) do\n\
  redis.call('lpush', ARGV[1], k)\n\
end\n\
"

var copyTrans = function (transaction, redis){
  for (var i = 0; i < transaction.length; i +=2){
    redis.watch(transaction[i]);
  };
  for (var i = 0; i < transaction.length; i +=2){
    var keys = transaction[i+1];
    var join = keys.concat(transaction[i]);
    console.log(join)
    redis.multi().eval(insertTrans, keys.length, join);
  };
  redis.multi().exec(function(err, res){
    console.log(res);
  });
}

var keys = ['key1', ['value1', 'value2'], 'key2', ['value1', 'value2', 'value3']];
copyTrans(keys, rc);
/*rc.eval(peekScript, keys.length, keys, function(err, data) {
  'use strict';
  console.dir(err);
  console.dir(data);
});*/

exports.peekScript = peekScript;
