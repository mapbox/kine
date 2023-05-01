var test = require('tape');
var util = require('./util');
var queue = require('d3-queue').queue;
var Kine = require('../');
var AWS = require('aws-sdk');
var Dyno = require('@mapbox/dyno');
var _ = require('lodash');
var sinon = require('sinon');
var events = require('events');

const DYNAMO_ENDPOINT = 'http://localhost:4567';

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

var dyno = Dyno({
  endpoint: DYNAMO_ENDPOINT,
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  table: 'kine-kcl-test'
});

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
      dynamoEndpoint: DYNAMO_ENDPOINT,
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
        t.equal(this.hasOwnProperty('checkpointFunc'), true, 'has checkpointFunc function');
        t.equal(records.length, 1, 'got record');
        t.equal(records[0].PartitionKey, 'a', 'has paritionKey');
        t.equal(records[0].Data.toString(), 'hello', 'has data');
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
  function checkpointed() {
    dyno.query({KeyConditions:{type:{ComparisonOperator:'EQ',AttributeValueList: ['shard']}}}, function(err, response) {
      var shards = response.Items;
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
      dynamoEndpoint: DYNAMO_ENDPOINT,
      shardIteratorType: 'TRIM_HORIZON',
      streamName: 'teststream',
      table: kine.config.table,
      cloudwatchNamespace: null,
      cloudwatchStackname: null,
      _leaseTimeout: 5000,
      cloudwatch: null,
      verbose: true,
      minProcessTime: 5000,
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

var kineManualCheckpoint;
test('start manual checkpoint kcl', function(t) {
  kineManualCheckpoint = Kine(
    _.extend(kinesisOptions, {
      dynamoEndpoint: DYNAMO_ENDPOINT,
      shardIteratorType: 'TRIM_HORIZON',
      streamName: 'teststream',
      table: kine.config.table,
      _leaseTimeout: 10000,
      cloudwatch: null,
      verbose: false,
      init: function(done) {
        console.log('init manual checkpoint');
        done(null, false);
      },
      processRecords: function(records, done) {
        done(null, false);
        this.checkpointFunc('012345', function(){
          t.end();
        });
      }
    })
  );
});

test('kcl - has manually checkpointed', function(t){
  function cp() {
    dyno.query({KeyConditions:{type:{ComparisonOperator:'EQ',AttributeValueList: ['shard']}}}, function(err, response) {
      t.equal(response.Items[2].checkpoint, '012345', 'sequenceNumber matches what we checkpointed manually');
      t.end();
    });
  }
  setTimeout(cp, 5000);
});

test('stop kcl', function(t){
  kineManualCheckpoint.stop();
  setTimeout(t.end, 6000);
});

var timerChecking;
test('start getRecords kcl', function (t) {
  var i = 0;
  var startTime;
  timerChecking = Kine(
    _.extend(kinesisOptions, {
      dynamoEndpoint: DYNAMO_ENDPOINT,
      shardIteratorType: 'LATEST',
      streamName: 'teststream',
      table: kine.config.table,
      cloudwatchNamespace: null,
      cloudwatchStackname: null,
      _leaseTimeout: 10000,
      cloudwatch: null,
      verbose: true,
      maxShards: 1,
      minProcessTime: 7000,
      init: function (done) {
        console.log('init');
        // trigger the first getRecords call
        kinesis.putRecord({Data: 'hello', PartitionKey: 'a', StreamName: 'teststream'}, function () {
          startTime = +new Date();
          done();
        });
      },
      processRecords: function (records, done) {
        var a = 0;
        timerChecking.kinesis.getRecords = function () {
          if (i === 0) {
            var diff = +new Date() - startTime;
            t.true(diff > 7000, 'Time between consecutive getRecords call > minProcessTime');
          }

          return {
            on: function (action, cb) {
              if (action === 'error') {
                // these errors should return a timeout function with different intervals based on the error type
                if (i === 0) {
                  var resultError1 = cb({code: 'ServiceUnavailable'});
                  t.true(resultError1._idleTimeout >= 500 && resultError1._idleTimeout <= 6000, 'ServiceUnavailable is between 500ms and 6000ms for retry 1');
                }
                if (i === 1) {
                  var resultError2 = cb({code: 'SyntaxError'});
                  t.equal(resultError2._idleTimeout, 500, 'SyntaxError is 0.5 second wait for retry');
                }
                if (i === 2) {
                  var resultError3 = cb({code: 'ProvisionedThroughputExceededException'});
                  t.true(resultError3._idleTimeout >= 500 && resultError3._idleTimeout <= 9000, 'ProvisionedThroughputExceededException is between 3500ms and 9000ms for retry 3');
                }
                i++;
              }
              if (action === 'success') {
                if (i === 1) {
                  var resultError4 = cb({
                    data: {
                      Records: [],
                      NextShardIterator: 'next1'
                    }
                  });
                  t.true(resultError4._idleTimeout, 2500, 'Norecords wait is 2500ms');
                }
              }
            },
            abort: function () {
              a++;
              if (a === 5) t.end();
            },
            send: function () {
            }
          };
        };
        done(null, false);
      }
    })
  );
});

test('stop error checking kcl', function (t) {
  timerChecking.stop();
  setTimeout(t.end, 10000);
});

var closeShard;
test('start shard closing test', function (t) {
  process.exit = function(code){
    t.ok('has been called');
    t.equal(code, 0, 'exited properly');
    console.log('Exited with code', code);
  };
  closeShard = Kine(
    _.extend(kinesisOptions, {
      dynamoEndpoint: DYNAMO_ENDPOINT,
      shardIteratorType: 'LATEST',
      streamName: 'teststream',
      table: kine.config.table,
      cloudwatchNamespace: null,
      cloudwatchStackname: null,
      _leaseTimeout: 10000,
      cloudwatch: null,
      verbose: true,
      maxShards: 1,
      init: function (done) {
        console.log('init');
        // trigger the first getRecords call
        kinesis.putRecord({Data: 'hello', PartitionKey: 'a', StreamName: 'teststream'}, function () {
          done();
        });
      },
      onShardClosed: function(done){
        t.equal(this.id, 'shardId-000000000000', 'shard closed is the first one');
        t.equal(this.status, 'leased', 'status is not closed yet');
        done();
      },
      processRecords: function (records, done) {
        closeShard.kinesis.getRecords = function () {
          return {
            on: function (action, cb) {
              if (action === 'success') {
                // valid Records, but no NextShardIterator -> this should close the shard
                cb({
                  data: {
                    Records: []
                  }
                });
                setTimeout(function () {
                  dyno.query({
                    KeyConditions: {
                      type: {
                        ComparisonOperator: 'EQ',
                        AttributeValueList: ['shard']
                      }
                    }
                  }, function (err, response) {
                    var shards = response.Items;
                    t.equal(shards.length, 4, 'shard count is the same');
                    t.equal(shards[0].status, 'complete', 'shard marked as complete');
                    t.end();
                  });
                }, 1000);
              }
            },
            abort: function () {
            },
            send: function () {
            }
          };
        };
        done(null, false);
      }
    })
  );
});

test('stop closed shard kcl', function (t) {
  closeShard.stop();
  setTimeout(t.end, 6000);
});

test('teardown', util.teardown);