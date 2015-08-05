var test = require('tape');
var util = require('./util');
var queue = require('queue-async');
var Kine = require('../');
var AWS = require('aws-sdk');
var Dyno = require('dyno');
var _ = require('lodash');

test('init', util.init);

var kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568'
};

var kinesis = new AWS.Kinesis(kinesisOptions);

var kine;

test('createStream', function(t) {
  kinesis.createStream({ShardCount:4, StreamName: 'teststream'}, function(err, resp){
    t.error(err);
    t.end();
  });
});

test('', function(t){


});

test('teardown', util.teardown);
