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

var copyScript = ""

rc.eval(peekScript, keys.length, keys, function(err, data) {
  'use strict';
  console.dir(err);
  console.dir(data);
});

exports.peekScript = peekScript;
