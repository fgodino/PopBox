
var dir_prefix = './';
if (process.env.POPBOX_DIR_PREFIX) {
  dir_prefix = process.env.POPBOX_DIR_PREFIX;
}
/**
 * Level for logger
 * debug
 * warning
 * error
 *
 * @type {String}
 */
exports.logger = {};
exports.logger.logLevel = 'debug';
exports.logger.inspectDepth = 1;
exports.logger.Console = {
  level: 'info', timestamp: true
};
exports.logger.File = {
  level: 'debug', filename: dir_prefix +
      '/popbox.log', timestamp: true, json: false,
  maxsize: 10 * 1024 * 1024,
  maxFiles: 3
};

/**
 *
 * @type {Array} ex. [{host:'localhost'}, {host:'localhost', port:'6789'}]
 */
//exports.redisServers = [{host:'localhost'}, {host:'localhost', port:'6789'}];
exports.redisServers = {
  redis1 : {host: 'localhost', port: 6379},
  redis2 : {host: 'localhost', port: 8888}
};

exports.hashing = {};
exports.hashing.replicas = 10;
exports.hashing.algorithm = 'md5';

/**
 *
 * @type {Number}
 */

exports.selectedDB = 0; //0..15 for   0 ->pre-production 1->test
