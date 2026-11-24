var test = require('tape');
var util = require('./util');
var queue = require('d3-queue').queue;
var AWS = require('aws-sdk');
var Dyno = require('@mapbox/dyno');
var Kine = require('../');
var tableDef = require('../lib/table');
var Db = require('../lib/db');

// Each instance takes ceil(shards / instances) shards. Rows of drained shards stay in the lease table
// with status 'complete' after a reshard. If an instance counts them, it takes too many shards and
// the other instances have nothing to read.
//
// Setup: 4 open shards, 20 'complete' rows, and 3 other live instances, so each instance must take 1 shard.

var creds = {accessKeyId: 'fake', secretAccessKey: 'fake', region: 'us-east-1'};
var kinesis = new AWS.Kinesis(Object.assign({endpoint: 'http://localhost:5568'}, creds));

function setup(name, cb) {
  var dyno = Dyno(Object.assign({endpoint: 'http://localhost:4567', table: name}, creds));
  dyno.createTable(tableDef(name), function(err) {
    if (err) return cb(err);
    kinesis.createStream({StreamName: name, ShardCount: 4}, function(err) {
      if (err) return cb(err);
      var q = queue(1);
      for (var i = 0; i < 20; i++) {
        q.defer(dyno.putItem, {Item: {type: 'shard', id: 'shardId-old-' + i, status: 'complete', expiresAt: 0}});
      }
      // Other live instances. Ids start with 'm', so 'a-...' sorts first and 'z-...' sorts last.
      for (var j = 1; j <= 3; j++) {
        q.defer(dyno.putItem, {Item: {type: 'instance', id: 'm-other-' + j, counter: 1, expiresAt: Date.now() + 36e5}});
      }
      q.defer(Db(dyno, kinesis, {streamName: name}).updateShards);
      q.awaitAll(function(err) {
        cb(err, dyno);
      });
    });
  });
}

function leasedBy(dyno, instanceId, cb) {
  dyno.query({KeyConditions: {type: {ComparisonOperator: 'EQ', AttributeValueList: ['shard']}}}, function(err, response) {
    if (err) return cb(err);
    cb(null, response.Items.filter(function(s) { return s.status === 'leased' && s.instance === instanceId; }).length);
  });
}

function checkBalance(name, instanceId) {
  return function(t) {
    setup(name, function(err, dyno) {
      t.error(err, 'setup');
      var kine = Kine(Object.assign({}, creds, {
        endpoint: 'http://localhost:5568',
        dynamoEndpoint: 'http://localhost:4567',
        table: name,
        streamName: name,
        instanceId: instanceId,
        shardIteratorType: 'TRIM_HORIZON',
        _leaseTimeout: 60000,
        init: function(done) { done(); },
        processRecords: function(records, done) { done(null, true); }
      }));
      // One lease takes about 2 seconds. Without the fix the instance takes all 4 shards in this time.
      setTimeout(function() {
        leasedBy(dyno, instanceId, function(err, count) {
          t.error(err);
          t.equal(count, 1, instanceId + ' leases 1 of 4 shards');
          kine.stop();
          setTimeout(t.end, 3000);
        });
      }, 12000);
    });
  };
}

test('init', util.init);
test('instance that refreshes the shard list takes its share', checkBalance('kine-balance-top', 'a-kine'));
test('instance that only reads the lease table takes its share', checkBalance('kine-balance-other', 'z-kine'));
test('teardown', util.teardown);
