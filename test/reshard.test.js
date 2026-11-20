var test = require('tape');
var util = require('./util');
var AWS = require('aws-sdk');
var Dyno = require('@mapbox/dyno');
var BigNumber = require('bignumber.js');
var http = require('http');
var tableDef = require('../lib/table');
var Db = require('../lib/db');

// Reproduces the slow lease registration after a 32 -> 16 -> 32 reshard.
// DescribeStream then lists 48 closed shards before the 32 open ones.

var streamName = 'reshardstream';
var tableName = 'kine-reshard-test';
var creds = {accessKeyId: 'fake', secretAccessKey: 'fake', region: 'us-east-1'};
var kinesis = new AWS.Kinesis(Object.assign({endpoint: 'http://localhost:5568'}, creds));

// aws-sdk v2 uses at most 50 sockets for https (real AWS). The local endpoint is http, which has no limit,
// so set the same limit here. Requests that wait for a socket are not sent yet, and the old code aborted them
// on the first ConditionalCheckFailedException.
var agent = new http.Agent({keepAlive: true, maxSockets: 50});
var dyno = Dyno(Object.assign({endpoint: 'http://localhost:4567', table: tableName, httpOptions: {agent: agent}}, creds));

function describe(cb) {
  kinesis.describeStream({StreamName: streamName}, function(err, d) {
    if (err) return cb(err);
    cb(null, d.StreamDescription);
  });
}

function waitActive(cb) {
  describe(function(err, d) {
    if (err) return cb(err);
    if (d.StreamStatus === 'ACTIVE') return cb(null, d.Shards);
    setTimeout(waitActive, 20, cb);
  });
}

function openShards(shards) {
  return shards.filter(function(s) { return !s.SequenceNumberRange.EndingSequenceNumber; }).sort(function(a, b) {
    return new BigNumber(a.HashKeyRange.StartingHashKey).cmp(b.HashKeyRange.StartingHashKey);
  });
}

// Run fn on each item in order, waiting for the stream to be ACTIVE after each call.
function series(items, fn, cb) {
  if (!items.length) return cb();
  fn(items[0], function(err) {
    if (err) return cb(err);
    waitActive(function(err) {
      if (err) return cb(err);
      series(items.slice(1), fn, cb);
    });
  });
}

var initialShards;

test('init', util.init);

test('create stream and lease rows for 32 shards', function(t) {
  dyno.createTable(tableDef(tableName), function(err) {
    t.error(err);
    kinesis.createStream({StreamName: streamName, ShardCount: 32}, function(err) {
      t.error(err);
      waitActive(function(err, shards) {
        t.error(err);
        initialShards = shards;
        series(shards, function(s, cb) {
          dyno.putItem({Item: {type: 'shard', id: s.ShardId, status: 'complete', expiresAt: 0}}, cb);
        }, function(err) {
          t.error(err);
          t.end();
        });
      });
    });
  });
});

test('reshard 32 -> 16 -> 32', function(t) {
  var pairs = [];
  var open = openShards(initialShards);
  for (var i = 0; i < open.length; i += 2) pairs.push([open[i].ShardId, open[i + 1].ShardId]);
  series(pairs, function(pair, cb) {
    kinesis.mergeShards({StreamName: streamName, ShardToMerge: pair[0], AdjacentShardToMerge: pair[1]}, cb);
  }, function(err) {
    t.error(err);
    describe(function(err, d) {
      t.error(err);
      var merged = openShards(d.Shards);
      t.equal(merged.length, 16, '16 open shards after merge');
      series(merged, function(s, cb) {
        var mid = new BigNumber(s.HashKeyRange.StartingHashKey).plus(s.HashKeyRange.EndingHashKey).dividedToIntegerBy(2).plus(1).toFixed();
        kinesis.splitShard({StreamName: streamName, ShardToSplit: s.ShardId, NewStartingHashKey: mid}, cb);
      }, function(err) {
        t.error(err);
        describe(function(err, d) {
          t.error(err);
          t.equal(d.Shards.length, 80, 'DescribeStream lists 80 shards');
          t.equal(openShards(d.Shards).length, 32, '32 open shards');
          t.end();
        });
      });
    });
  });
});

test('updateShards registers all open shards in one cycle', function(t) {
  var db = Db(dyno, kinesis, {streamName: streamName});
  db.updateShards(function(err) {
    t.error(err);
    describe(function(err, d) {
      t.error(err);
      db.getShardList(function(err, items) {
        t.error(err);
        var ids = items.map(function(i) { return i.id; });
        var registered = openShards(d.Shards).filter(function(s) { return ids.indexOf(s.ShardId) !== -1; });
        t.equal(registered.length, 32, 'all 32 open shards have a lease row');
        t.end();
      });
    });
  });
});

test('teardown', function(t) {
  agent.destroy();
  util.teardown(t);
});
