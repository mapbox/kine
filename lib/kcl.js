var os = require('os');
var Dyno = require('dyno');
var hat = require('hat');
var table = require('./table');
var queue = require('queue-async');
var _ = require('lodash');
var DB = require('./db');

module.exports = function(config, kinesis) {

  config.instanceId = [os.hostname(), process.pid, +new Date()].join('-');
  config.maxShards = config.maxShards || 10;

  var dyno = Dyno({
    table: config.table,
    region: config.region,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    sessionToken: config.sessionToken,
    endpoint: config.dynamoEndpoint
  });
  var db = DB(dyno, kinesis, config);

  var shardList = [];
  var instanceList = [];
  var instanceShardList = {}; // the shards this instance is reading.
  var kcl = {config: config};
  var stop = false;


  // make sure that our dynamo table exists.
  dyno.createTable(table(config.table), function(err) {
    if(err) throw err;

    var  q = queue(1);
    q.defer(db.updateInstance, config.instanceId);
    q.defer(db.cleanupInstances);
    q.await(function(err, instance, updatedInstances) {
      instanceList = updatedInstances;
      heartbeat();
      shards();
    });
  });

  kcl.stop = function() {
    stop = true;
  };

  // figure out the shards that this instance is responsible for and the checkpoints of those shards
  function shards() {
    if (stop) return;

    var howMany = 0;

    db.updateShards(function(err, updatedShards) {
      if (err) throw err;
      shardList = updatedShards;
      needMore();
    });

    function foundShardToLease(err, shard) {
      if (err) throw err;
      if (!shard) return needMore();
      return db.leaseShard(shard.id, checkDone);
    }

    function checkDone(err, shard) {
      if (err && err.code === 'ConditionalCheckFailedException') {
        // attempted to lease a shard that was already leased.  Lets try again.
        console.log(err);
        return setTimeout(db.availableShard.bind(this,foundShardToLease), 1000).unref();
      } else if (err) {
        throw err;
      }
      db.getIterator(shard, function(err, iterator) {
        if (err) throw err;
        shard.iterator = iterator.ShardIterator;
        instanceShardList[shard.id] = shard;
        startGettingRecords();
        needMore();
      });
    }

    function needMore() {
      if (stop) return;
      howMany = Math.ceil(shardList.length / instanceList.length);
      howMany = (howMany < config.maxShards ? howMany : config.maxShards);
      if (Object.keys(instanceShardList).length < howMany) {
        setTimeout(db.availableShard.bind(this, foundShardToLease), 2000).unref();
      } else {
        setTimeout(shards, 10000).unref();
      }
    }
  }

  function getRecords(shard) {
    if (stop) return;

    //make sure we are still responsible for this shard.
    if (!instanceShardList[shard.id]) return;
    if (instanceShardList[shard.id].expiresAt < (+new Date()-5000)) {
      return delete instanceShardList[shard.id];
    }

    // expose the checkpoint function to the procerssRecords, so they can checkpoint
    // with requesting more records.
    shard.checkpoint = db.checkpoint.bind(null, shard);

    kinesis.getRecords(
      { ShardIterator: shard.iterator, Limit: 10000 },
      function(err, resp) {
        if(err) throw err;

        shard.iterator = resp.NextShardIterator;

        if(resp.Records.length > 0) {
          shard.sequenceNumber = resp.Records[resp.Records.length -1].SequenceNumber;
          config.processRecords.call(shard, resp.Records, processRecordsDone);
        } else {
          if (!shard.iterator) {
            db.shardComplete(shard, function(err){
              if (err) throw err;
            });
          } else {
            setTimeout(getRecords.bind(this, shard), 2500).unref();
          }
        }
      }
    );

    function processRecordsDone(err, checkpointShard) {
      if (err) throw err;
      if (checkpointShard) {
        // save checkpoint to dynamo.
        db.checkpoint(shard, checkpointSaved);
      } else {
        setImmediate(getRecords.bind(this, shard));
      }
    }

    function checkpointSaved(err) {
      if (err) throw err;
      if (!shard.iterator) {
        // we are done with this shard, its complete.  lets mark it as complete.
        db.shardComplete(shard, function(err){
          if (err) throw err;
        });
      } else {
        setImmediate(getRecords.bind(this, shard));
      }
    }
  }

  function startGettingRecords() {
    var shardIds = Object.keys(instanceShardList);
    shardIds.forEach(function(s) {
      if (!instanceShardList[s].getRecords) {
        config.init.call(instanceShardList[s], function() {
          getRecords(instanceShardList[s]);
        });
        instanceShardList[s].getRecords = true;
      }
    });
  }

  // add this instance to the list of instances in dynamo (keep that up to date with heartbeat)
  function heartbeat() {
    if (stop) return;
    var q = queue(1);
    // clean up out of date instances.
    q.defer(db.updateInstance, config.instanceId);
    q.defer(db.cleanupInstances);
    Object.keys(instanceShardList).forEach(function(s) {
      q.defer(db.updateLease, instanceShardList[s]);
    });

    q.awaitAll(function(err, resp) {
      if(err) throw err;
      instanceList = resp[1];
      for(var i = 2; i< resp.length; i++) {
        if(instanceShardList[resp[i].id]) _.extend(instanceShardList[resp[i].id],resp[i]);
      }
      setTimeout(heartbeat, 5000);
    });
  }

  return kcl;
};
