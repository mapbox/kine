var test = require('tape');
var util = require('./util');
var queue = require('queue-async');
var Kine = require('../');
var Dyno = require('dyno');
var _ = require('lodash');

test('init', util.init);

var kine = Kine({
  streamName:'fakestream3',
  accessKeyId: 'fake',
  secretAccessKey: 'fake',
  region: 'us-east-1',
  endpoint: 'http://localhost:5568',
  dynamoEndpoint: 'http://localhost:4567'
});
var kcl;

test('createStream', function(t) {
  kine.createStream({shardCount:4}, function(err, resp) {
    t.error(err);
    console.log(JSON.stringify(resp));
    t.end();
  });
});

test('start kcl', function(t) {

  var kclprocess = {
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
      done(null, true);
      t.end();
    }
  };

  kcl = kine.kcl(kclprocess, {shardIteratorType: 'TRIM_HORIZON'});
  kcl.start();

  kine.putRecord(
    { data: 'hello', partitionKey: 'a' },
    function(err, resp) {
      t.error(err);
      console.log(resp);
    }
  );

});

test('kcl - checkpointed', function(t) {

  // check if it got checkpointed in dynamo

  var dyno = Dyno({
    endpoint: 'http://localhost:4567',
    accessKeyId: 'fake',
    secretAccessKey: 'fake',
    region: 'us-east-1',
    table: kcl.config.table
  });

  function checkpointed() {
    dyno.query({type:{EQ:'shard'}}, function(err, shards) {

      shards.forEach(function(s) {
        t.equal(s.status, 'leased', 'leased');
        t.equal(s.instance, kcl.config.instanceId, 'this instance leased');
      });

      var checkpointed = _(shards).filter(function(s) { return !!s.checkpoint;}).value();
      t.equal(checkpointed.length, 1, 'one checkpointed');
      t.end();
    });
  }
  setTimeout(checkpointed, 7000);
});

test('stop kcl', function(t) {
  // stop the kcl, somehow
  kcl.stop();
  setTimeout(t.end, 6000);
});

test('add more records', function(t) {
  var q = queue();
  for (var i = 0; i < 3; i++) {
    q.defer(kine.putRecord, { data: 'hello' + i, partitionKey: 'a' + i });
  }
  q.awaitAll(function(err, resp) {
    t.error(err);
    t.end();
  });
});

var kcl2;
test('start 2nd kcl', function(t) {
  var kclprocess = {
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
  };
  kcl2 = kine.kcl(kclprocess, {shardIteratorType: 'TRIM_HORIZON', table: kcl.config.table});
  kcl2.start();

});

test('stop kcl2', function(t) {
  // stop the kcl, somehow
  kcl2.stop();
  setTimeout(t.end, 6000);
});

test('teardown', util.teardown);
