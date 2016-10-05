var test = require('tape');
var util = require('./util');
var queue = require('queue-async');
var Kine = require('../');
var AWS = require('aws-sdk');
var Dyno = require('dyno');
var _ = require('lodash');
var sinon = require('sinon');
var events = require('events');

test('init', util.init);

var kinesisOptions = {
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568',
  table: 'kine-kcl-test'
};

var kinesis = new AWS.Kinesis(kinesisOptions);

var kine;

test('createStream', function(t) {
  kinesis.createStream({ShardCount:4, StreamName: 'teststream'}, function(err){
    t.error(err);
    t.end();
  });
});

test('start kcl', function(t){
  // mock cloudwatch
  var send = sinon.spy(function() {
    t.ok(this.options);
    t.equal(this.options.Namespace, 'test');
    t.equal(this.options.MetricData.length, 1);
    t.equal(this.options.MetricData[0].MetricName, 'ShardIteratorAgeInMs');
    t.ok(this.options.MetricData[0].Value);
    t.equal(this.options.MetricData[0].Unit, 'Milliseconds');
    t.equal(this.options.MetricData[0].Dimensions.length, 3);
    t.equal(this.options.MetricData[0].Dimensions[0].Name, 'DeliveryStream');
    t.ok(this.options.MetricData[0].Dimensions[0].Value);
    t.equal(this.options.MetricData[0].Dimensions[1].Name, 'ShardId');
    t.ok(this.options.MetricData[0].Dimensions[1].Value);
    t.equal(this.options.MetricData[0].Dimensions[2].Name, 'Stack');
    t.ok(this.options.MetricData[0].Dimensions[1].Value);
  });

  var cloudwatch = {
    putMetricData: function(options) {
      var e = new events.EventEmitter();
      e.options = options;
      e.send = send;
      return e;
    }
  };

  kine = Kine(
    _.extend(kinesisOptions, {
      dynamoEndpoint: 'http://localhost:4567',
      shardIteratorType: 'TRIM_HORIZON',
      streamName: 'teststream',
      cloudwatchNamespace: 'test',
      cloudwatchStackname: 'test',
      cloudwatch: cloudwatch,
      _leaseTimeout: 5000,
      init: function(done) {
        console.log('init');
        done();
      },
      processRecords: function(records, done) {
        // check value of this.
        console.log(records);
        t.equal(records.length, 1, 'got record');
        t.equal(records[0].PartitionKey, 'a', 'has paritionKey');
        t.equal(records[0].Data.toString(), 'hello', 'has data');
        sinon.assert.calledOnce(send);
        done(null, true);
        t.end();
      }
    })
  );

  kinesis.putRecord(
    { Data: 'hello', PartitionKey: 'a', StreamName: 'teststream' },
    function(err) {
      t.error(err);
    }
  );
});

test('kcl - checkpointed', function(t){

  // check if it got checkpointed in dynamo
  var dyno = Dyno({
    endpoint: 'http://localhost:4567',
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    region: 'us-east-1',
    table: kine.config.table
  });

  function checkpointed() {
    dyno.query({type:{EQ:'shard'}}, function(err, shards) {

      shards.forEach(function(s) {
        t.equal(s.status, 'leased', 'leased');
        t.equal(s.instance, kine.config.instanceId, 'this instance leased');
        t.ok(s.hashKeyStart);
        t.ok(s.hashKeyEnd);
      });

      var checkpointed = _(shards).filter(function(s){ return !!s.checkpoint;}).value();
      t.equal(checkpointed.length, 1, 'one checkpointed');
      t.end();
    });
  }
  setTimeout(checkpointed, 7000);
});

test('stop kcl', function(t){
  // stop the kcl, somehow
  kine.stop();
  setTimeout(t.end, 6000);
});

test('add more records', function(t) {
  var q = queue();
  for(var i=0; i< 3; i++){
    q.defer(kinesis.putRecord.bind(kinesis), { Data: 'hello'+i, PartitionKey: 'a'+i, StreamName: 'teststream' });
  }
  q.awaitAll(function(err) {
    t.error(err);
    t.end();
  });
});

var kine2;
test('start 2nd kcl', function(t) {
  kine2 = Kine(
    _.extend(kinesisOptions, {
      dynamoEndpoint: 'http://localhost:4567',
      shardIteratorType: 'TRIM_HORIZON',
      streamName: 'teststream',
      table: kine.config.table,
      cloudwatchNamespace: null,
      cloudwatchStackname: null,
      _leaseTimeout: 5000,
      cloudwatch: null,
      init: function(done) {
        console.log('init');
        done();
      },
      processRecords: function(records, done) {
        // check value of this.
        console.log(records);
        t.equal(records.length, 2, 'got record');
        t.equal(records[0].PartitionKey, 'a0', 'has paritionKey');
        t.equal(records[0].Data.toString(), 'hello0', 'has data');
        done(null, true);
        t.end();
      }
    })
  );
});

test('stop kcl2', function(t){
  // stop the kcl, somehow
  kine2.stop();
  setTimeout(t.end, 6000);
});

test('query instanceInfo', function (t) {
  kine.instanceInfo('a', function (err, info) {
    t.error(err, 'no error querying instance info');
    t.equal(info.instance, kine.config.instanceId, 'finds the instance');
    t.ok(info.hashKeyStart, 'info has hashKeyStart');
    t.ok(info.hashKeyEnd, 'info has hashKeyEnd');
    t.ok(info.shardId, 'info has shardId');
    t.end();
  });
});

test('teardown', util.teardown);
