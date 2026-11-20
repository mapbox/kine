var test = require('tape');
var DB = require('../lib/db');

function shard(id) {
  return {ShardId: id, HashKeyRange: {StartingHashKey: '0', EndingHashKey: '1'}};
}

// Fake dyno: rows in `existing` fail the condition at once, like DynamoDB does for a row that already exists.
// New rows succeed a little later. Like an AWS request, each call returns an object with abort().
function fakeDyno(existing) {
  var dyno = {created: [], aborted: 0};
  dyno.updateItem = function(params, cb) {
    var aborted = false;
    var id = params.Key.id;
    setTimeout(function() {
      if (aborted) return;
      if (existing.indexOf(id) !== -1) {
        var err = new Error('The conditional request failed');
        err.code = 'ConditionalCheckFailedException';
        return cb(err);
      }
      setTimeout(function() {
        if (aborted) return;
        dyno.created.push(id);
        cb(null, {Attributes: {id: id}});
      }, 5);
    }, 0);
    return {abort: function() { aborted = true; dyno.aborted++; }};
  };
  return dyno;
}

function fakeKinesis(pages) {
  var kinesis = {calls: []};
  kinesis.describeStream = function(params, cb) {
    kinesis.calls.push(params);
    var page = pages[kinesis.calls.length - 1];
    setTimeout(cb.bind(null, null, {StreamDescription: page}), 0);
  };
  return kinesis;
}

test('updateShards creates rows for all new shards when old shard rows exist', function(t) {
  // After a 32 -> 16 -> 32 reshard the stream lists 80 shards: 48 closed shards that already have rows, then 32 new shards.
  var oldIds = [];
  var newIds = [];
  var shards = [];
  for (var i = 0; i < 48; i++) { oldIds.push('old-' + i); shards.push(shard('old-' + i)); }
  for (var j = 0; j < 32; j++) { newIds.push('new-' + j); shards.push(shard('new-' + j)); }

  var dyno = fakeDyno(oldIds);
  var kinesis = fakeKinesis([{Shards: shards, HasMoreShards: false}]);
  var db = DB(dyno, kinesis, {streamName: 'teststream'});

  db.updateShards(function(err, listed) {
    t.error(err, 'no error');
    t.equal(listed.length, 80, 'returns all listed shards');
    t.equal(dyno.aborted, 0, 'no request aborted');
    t.deepEqual(dyno.created.slice().sort(), newIds.slice().sort(), 'a row is created for every new shard');
    t.end();
  });
});

test('updateShards follows HasMoreShards pages', function(t) {
  var dyno = fakeDyno([]);
  var kinesis = fakeKinesis([
    {Shards: [shard('a'), shard('b')], HasMoreShards: true},
    {Shards: [shard('c')], HasMoreShards: false}
  ]);
  var db = DB(dyno, kinesis, {streamName: 'teststream'});

  db.updateShards(function(err, listed) {
    t.error(err, 'no error');
    t.equal(kinesis.calls.length, 2, 'two DescribeStream calls');
    t.equal(kinesis.calls[0].ExclusiveStartShardId, undefined, 'first call has no start shard');
    t.equal(kinesis.calls[1].ExclusiveStartShardId, 'b', 'second call starts after the last shard of page one');
    t.deepEqual(listed.map(function(s) { return s.ShardId; }), ['a', 'b', 'c'], 'returns shards from all pages');
    t.deepEqual(dyno.created.slice().sort(), ['a', 'b', 'c'], 'a row is created for every shard');
    t.end();
  });
});

test('updateShards returns other DynamoDB errors', function(t) {
  var dyno = {updateItem: function(params, cb) {
    var err = new Error('throttled');
    err.code = 'ProvisionedThroughputExceededException';
    setTimeout(cb.bind(null, err), 0);
  }};
  var kinesis = fakeKinesis([{Shards: [shard('a')], HasMoreShards: false}]);
  var db = DB(dyno, kinesis, {streamName: 'teststream'});

  db.updateShards(function(err) {
    t.equal(err && err.code, 'ProvisionedThroughputExceededException', 'error is returned');
    t.end();
  });
});
