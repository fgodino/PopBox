var express = require('express');
var fs = require('fs');
var configSrv = require('./configurationServer.js');
var config = require('./config.js');
var path = require('path');

var dirModule = path.dirname(module.filename);

var log = require('PDITCLogger');
log.setConfig(config.logger);
var logger = log.newLogger();
logger.prefix = path.basename(module.filename, '.js');

var optionsDir;

if (!config.crtPath) {
  optionsDir = {
    key: path.resolve(dirModule, '../../utils/server.key'),
    cert: path.resolve(dirModule, '../../utils/server.crt')
  };
} else {
  optionsDir = {
    key: path.resolve(config.agent.crtPath, 'server.key'),
    cert: path.resolve(config.agent.crtPath, 'server.crt')
  };
}

/*checks whether the cert files exist or not
 and starts the appSec server*/

if (fs.existsSync(optionsDir.key) &&
    fs.existsSync(optionsDir.cert) &&
    fs.statSync(optionsDir.key).isFile() &&
    fs.statSync(optionsDir.cert).isFile()) {

  var options = {
    key: fs.readFileSync(optionsDir.key),
    cert: fs.readFileSync(optionsDir.cert)
  };
  logger.info('valid certificates');
} else {
  logger.warning('certs not found', optionsDir);
  throw new Error('No valid certificates were found in the given path');
}

var app = express.createServer(options);
app.port = config.adminPort;

app.use(express.query());
app.use(express.bodyParser());
app.post('/add', configSrv.checkMigrating, configSrv.addNode);
app.post('/del', configSrv.checkMigrating, configSrv.delNode);
app.get('/get', configSrv.getAgents);

app.listen(app.port);

process.on('uncaughtException', function onUncaughtException(err) {
  'use strict';
  logger.warning('onUncaughtException', err);
});
