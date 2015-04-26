var os = require('os');
var Dyno = require('dyno');
var hat = require('hat');
var table = require('./table');
var queue = require('queue-async');
var _ = require('lodash');

module.exports = function(config, mod, opts, kine) {

  config.table = opts.table || 'kinesis-client-library-' + hat(6);
  config.instanceId = [os.hostname(), process.pid, +new Date()].join('-');

  if (!opts) opts = {};

  var dyno = Dyno({
    table: config.table,
    region: config.region,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    endpoint: config.dynamoEndpoint
  });

  var shardList = [];
  var instanceList = [];
  var instanceShardList = {}; // the shards this instance is reading.
  var kcl = {config: config};
  var stop = false;

  kcl.start = function() {
    // make sure that our dynamo table exists.
    dyno.createTable(table(config.table), function(err) {
      if (err) throw err;

      var  q = queue();
      q.defer(updateInstances);
      q.await(function(err, updatedInstances) {
        heartbeat();
        shards();
      });
    });
  };

  kcl.status = function(opts, cb) {

  };

  kcl.stop = function() {
    stop = true;
  };

  // get the shard list from kinesis. Update them in dynamo.
  function updateShards(cb) {

    kine.describeStream(function(err, stream) {
      if (err) throw err;
      var q = queue();

      stream.StreamDescription.Shards.forEach(function(shard) {

        q.defer(dyno.updateItem,
          {type: 'shard', id: shard.ShardId},
          {put: {status: 'available', updated: +new Date(), expiresAt: 0}},
          {expected: {id: {NULL: []}}}
        );

      });

      // look at the list in dynamo, remove any that arent in stream description.
      // maybe possible to do this when getRecords fails because the shard isnt there?

      q.awaitAll(function(err, resp) {
        if (err && err.code !== 'ConditionalCheckFailedException') return cb(err);
        cb(null, stream.StreamDescription.Shards);
      });

    });

  }

  // figure out the shards that this instance is responsible for and the checkpoints of those shards
  function shards() {
    if (stop) return;

    var howMany = 0;

    updateShards(function(err, updatedShards) {
      if (err) throw err;
      shardList = updatedShards;
      howMany = Math.ceil(shardList.length / instanceList.length);
      needMore();
    });

    function findShardToLease() {

      dyno.query({type:{EQ:'shard'}}, function(err, shardsFromDynamo) {
        for (var s = 0; s < shardsFromDynamo.length; s++) {
          if (shardsFromDynamo[s].status === 'available') {
            return leaseShard(shardsFromDynamo[s].id,
              {
                status: {EQ: 'available'}
              }, checkDone);
          } else if (shardsFromDynamo[s].expiresAt < +new Date()) {
            return leaseShard(shardsFromDynamo[s].id,
              {
                status: {EQ: 'leased'},
                expiresAt: {LE: +new Date()}
              }, checkDone);
          }
        }
        needMore();
      });
    }

    function checkDone(err, shard) {
      if (err && err.code === 'ConditionalCheckFailedException') {
        console.log(err);
        return setTimeout(findShardToLease, 1000).unref();
      } else if (err) {
        throw err;
      }

      if (shard.instance == config.instanceId) {
        var getIteratorOpts = {
          shardId: shard.id,
          shardIteratorType: opts.shardIteratorType
        };

        if (shard.checkpoint) {
          getIteratorOpts.shardIteratorType = 'AFTER_SEQUENCE_NUMBER';
          getIteratorOpts.startingSequenceNumber = shard.checkpoint;
        }
        kine.getShardIterator(
          getIteratorOpts,
          function(err, iterator) {
            if (err) throw err;
            shard.iterator = iterator.ShardIterator;
            instanceShardList[shard.id] = shard;
            startGettingRecords();
            return needMore();
          }
        );
      } else {
        needMore();
      }

    }

    function needMore() {
      howMany = Math.ceil(shardList.length / instanceList.length);
      console.log('leased shards', Object.keys(instanceShardList).length, '/', howMany);
      if (Object.keys(instanceShardList).length < howMany) {
        setTimeout(findShardToLease, 2000).unref();
      } else {
        setTimeout(shards, 5000).unref();
      }
    }

  }

  // get a lease on a shard in dynamo
  function leaseShard(id, expected, cb) {
    dyno.updateItem(
      {type: 'shard', id: id},
      {put: {
        status: 'leased',
        updated: +new Date(),
        instance: config.instanceId,
        expiresAt: +new Date() + (1000 * 20)
      }},
      {
        conidtionalOperator: 'AND',
        expected: expected
      },
      cb);
  }

  // figure out how many shard there are

  // create  / update items in dynamo per shard

  function getRecords(shard) {
    if (stop) return;
    //make sure we are still responsible for this shard.
    if (!instanceShardList[shard.id]) return;
    var _this = {};

    kine.getRecords(
      {id: shard.id, shardIterator: shard.iterator},
      function(err, resp) {
        if (err) throw err;

        shard.iterator = resp.NextShardIterator;

        if (resp.Records.length > 0) {
          shard.SequenceNumber = resp.Records[resp.Records.length - 1].SequenceNumber;
          mod.processRecords.call(shard, resp.Records, processRecordsDone);
        } else {
          setTimeout(getRecords.bind(this, shard), 2500).unref();
        }
      }
    );

    function processRecordsDone(err, checkpointShard) {
      if (err) throw err;

      if (checkpointShard) {
        // save checkpoint to dynamo.
        checkpoint(shard, checkpointSaved);
      } else {
        setTimeout(getRecords.bind(this, shard), 500).unref();
      }
    }
    function checkpointSaved(err, resp) {
      if (err) throw err;
      setTimeout(getRecords.bind(this, shard), 500).unref();
    }
  }

  function startGettingRecords() {
    var shardIds = Object.keys(instanceShardList);
    console.log(shardIds);
    shardIds.forEach(function(s) {
      if (!instanceShardList[s].getRecords) {
        mod.init.call(instanceShardList[s], function() {
          getRecords(instanceShardList[s]);
        });
        instanceShardList[s].getRecords = true;
      }
    });
  }

  function checkpoint(shard, cb) {

    dyno.updateItem(
      {type: 'shard', id: shard.id},
      {put: {
        updated: +new Date(),
        expiresAt: +new Date() + (1000 * 20),
        checkpoint: shard.SequenceNumber
      }},
      {
        conidtionalOperator: 'AND',
        expected: {
          instance: {EQ: config.instanceId},
          expiresAt: {GE: +new Date()}
        }
      },
      cb);
  }

  // add this instance to the list of instances in dynamo (keep that up to date with heartbeat)
  function heartbeat() {
    if (stop) return;
    var q = queue(1);
    // clean up out of date instances.
    q.defer(updateInstances);

    Object.keys(instanceShardList).forEach(function(s) {
      q.defer(dyno.updateItem,
        {type:'shard', id: s},
        {add: {counter: 1}, put: {expiresAt: +new Date() + (1000 * 20), updated: +new Date()}},
        {
          conidtionalOperator: 'AND',
          expected: {
            expiresAt: {GE: +new Date()},
            status: {EQ: 'leased'},
            instance: {EQ: config.instanceId}
          }
        }
      );
    });

    q.awaitAll(function(err, resp) {
      setTimeout(heartbeat, 5000);
    });
  }

  function updateInstances(cb) {

    dyno.updateItem(
      {type:'instance', id: config.instanceId},
      {add: {counter: 1}, put: {expiresAt: +new Date() + (1000 * 20)}},
      cleanupInstances
    );

    function cleanupInstances(err) {
      if (err) throw err;
      dyno.query({type:{EQ:'instance'}}, {pages:0}, function(err, instances) {
        if (err) throw err;
        var q = queue();
        var deleteInstances = [];
        instances.forEach(function(i) {
          if (i.expiresAt < +new Date()) {
            deleteInstances.push(i.id);
            q.defer(dyno.deleteItem, {type:'instance', id: i.id});
          }
        });

        instances = _(instances).filter(function(ins) { return deleteInstances.indexOf(ins.id) == -1;}).value();
        q.awaitAll(function(err, resp) {
          if (err) return cb(err);
          instanceList = instances;
          cb(null, instances);
        });
      });
    }
  }
  return kcl;
};
