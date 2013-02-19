var should = require('should');
var async = require('async');
var config = require('./config.js');
var utils = require('./utils.js');
var redis = require('redis'), rc = redis.createClient(6379, 'localhost');

var HOST = config.hostname;
var PORT = config.port;

var trans, trans1 = {};

describe('Invalid Data in JSON', function() {

  var executeTest = function(trans, expectedError, done) {
    var heads = {};
    heads['content-type'] = 'application/json';
    var options = { host: HOST, port: PORT,
      path: '/trans/', method: 'POST', headers: heads};

    utils.makeRequest(options, trans, function(error, response, data) {
      response.statusCode.should.be.equal(400);
      should.not.exist(error);

      data.should.have.property('errors');
      data.errors[0].should.be.equal(expectedError);

      done();

    });
  }

  it('Invalid Priority', function(done) {

    var trans = {
      'payload': '{\"spanish\": \"hola\", \"english\": ' +
          '\"hello\", \"to\": \"Mr Lopez\"}',
      'priority': 'M',
      'callback': 'http' + '://foo.bar',
      'queue': [
        { 'id': 'q1' },
        { 'id': 'q2' }
      ],
      'expirationDate': Math.round(new Date().getTime() / 1000 + 2)
    };

    executeTest(trans, 'invalid priority', done);
  });

  it('Undefined Priority', function(done) {

    var trans = {
      'payload': '{\"spanish\": \"hola\", \"english\": ' +
          '\"hello\", \"to\": \"Mr Lopez\"}',
      'callback': 'http' + '://foo.bar',
      'queue': [
        { 'id': 'q1' },
        { 'id': 'q2' }
      ],
      'expirationDate': Math.round(new Date().getTime() / 1000 + 2)
    };

    executeTest(trans, 'undefined priority', done);
  });

  it('Invalid queue', function(done) {

    var trans = {
      'payload': '{\"spanish\": \"hola\", \"english\": ' +
          '\"hello\", \"to\": \"Mr Lopez\"}',
      'callback': 'http' + '://foo.bar',
      'priority': 'H',
      'queue': {
         'id': 'q1'
      },
      'expirationDate': Math.round(new Date().getTime() / 1000 + 2)
    };

    executeTest(trans, 'invalid queue type', done);
  });

  it('Undefined queue', function(done) {

    var trans = {
      'payload': '{\"spanish\": \"hola\", \"english\": ' +
          '\"hello\", \"to\": \"Mr Lopez\"}',
      'callback': 'http' + '://foo.bar',
      'priority': 'H',
      'expirationDate': Math.round(new Date().getTime() / 1000 + 2)
    };

    executeTest(trans, 'undefined queue', done);
  });

  it('Invalid Queue Element', function(done) {

    var trans = {
      'payload': '{\"spanish\": \"hola\", \"english\": ' +
          '\"hello\", \"to\": \"Mr Lopez\"}',
      'callback': 'http' + '://foo.bar',
      'priority': 'H',
      'queue': [
        { 'identifier': 'q1' },
        { 'id': 'q2' }
      ],
      'expirationDate': Math.round(new Date().getTime() / 1000 + 2)
    };

    executeTest(trans, 'invalid queue element', done);
  });

  it('too many queues', function(done) {

    var queues = [];
    for (var i = 0; i < 10001; i++){
      queues.push({id: 'q' + i});
    }

    var trans = {
      'payload': '{\"spanish\": \"hola\", \"english\": ' +
          '\"hello\", \"to\": \"Mr Lopez\"}',
      'callback': 'http' + '://foo.bar',
      'priority': 'H',
      'queue': queues,
      'expirationDate': Math.round(new Date().getTime() / 1000 + 2)
    };

    executeTest(trans, 'too many queues: maximum 10000', done);
  });
});

