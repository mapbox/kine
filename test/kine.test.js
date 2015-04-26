var test = require('tape');
var util = require('./util');
var Kine = require('../');

test('init', util.init);

var kine = Kine({
  streamName:'fakestream2',
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  endpoint: 'http://localhost:5568'
});

test('createStream', function(t) {
  kine.createStream({shardCount:2}, function(err, resp) {
    t.error(err);
    console.log(JSON.stringify(resp));
    t.end();
  });
});

var desc;

test('describeStream', function(t) {

  kine.describeStream(function(err, resp) {
    t.error(err);
    console.log(JSON.stringify(resp, null, 2));
    desc = resp;
    t.end();
  });
});

test('getShardIterator', function(t) {

  kine.getShardIterator(
    { shardId: desc.StreamDescription.Shards[0].ShardId},
    function(err, iterator) {
      t.error(err);
      t.notEqual(iterator.ShardIterator, undefined, 'there is an iterator');
      t.end();
    }
  );
});

test('putRecord', function(t) {

  kine.putRecord(
    { data: 'hello', partitionKey: 'a' },
    function(err, resp) {
      t.error(err);
      console.log(resp);
      t.end();
    }
  );
});

test('teardown', util.teardown);
