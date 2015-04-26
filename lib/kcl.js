var os = require('os');
var Dyno = require('dyno');
var hat = require('hat');
var table = require('./table');
var queue = require('queue-async');
var AWS = require('aws-sdk');
var Processor = require('./processor');

module.exports = function(config, kclParameters, backendOptions) {
  if (!backendOptions) backendOptions = {};
  config.table = backendOptions.table || 'kinesis-client-library-' + hat(6);
  config.instanceId = [os.hostname(), process.pid, +new Date()].join('-');

  if (!config.streamName) throw new Error('streamName must be configured');

  var kinesis = new AWS.Kinesis({
    region: config.region,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    endpoint: config.kinesisEndpoint
  });

  var dyno = Dyno({
    table: config.table,
    region: config.region,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    endpoint: config.dynamoEndpoint
  });

  var db = require('./db')(dyno, kinesis);

  var shardList = [];
  var instanceList = [];
  var instanceShardList = {}; // the shards this instance is reading.
  var stop = false;

  var kcl = {
    config: config,

    start: function() {
      db.insureTable(table(config.table), function(err) {
        if (err) throw err;

        var  q = queue();
        q.defer(updateInstances);
        q.await(function(err) {
          if (err) throw err;
          heartbeat();
          shards();
        });
      });
    },

    stop: function() {
      stop = true;
    }
  };

  // Updates the instanceList in memory and in dynamodb
  function updateInstances(callback) {
    db.addInstance(config.instanceId, function(err) {
      if (err) return callback(err);

      db.listInstances(function(err, instances) {
        if (err) return callback(err);

        instanceList = instances;
        callback();
      });
    });
  }

  // Maintain this instance's leases in dynamodb
  function heartbeat() {
    if (stop) return;

    var q = queue(1);

    q.defer(updateInstances);

    Object.keys(instanceShardList).forEach(function(shardId) {
      q.defer(db.heartbeat, shardId);
    });

    q.awaitAll(function(err) {
      if (err) throw err;
      setTimeout(heartbeat, 5000);
    });
  }

  // Keep instanceShardList up-to-date in memory and take out leases
  function shards() {
    if (stop) return;

    db.listShards(config.streamName, function(err, shards) {
      if (err) throw err;
      shardList = shards;
      getLease();
    });

    function getLease() {
      var shardsPerInstance = Math.ceil(shardList.length / instanceList.length);

      if (Object.keys(instanceShardList).length < shardsPerInstance)
        setTimeout(findShardToLease, 2000).unref();
      else setTimeout(shards, 5000).unref();
    }

    function findShardToLease() {
      db.lease(config.instanceId, function(err, shard) {
        if (err) throw err;
        if (!shard) return getLease();

        instanceShardList[shard.id] = shard;
        readRecords();
        getLease();
      });
    }
  }

  // Read records from shards in the instanceShardList
  function readRecords() {
    Object.keys(instanceShardList).forEach(function(shardId) {
      var shard = instanceShardList[shardId];

      kclParameters.init.call(shard, function() {
        var options = {
          name: config.streamName,
          shardId: shardId,
          limit: backendOptions.limit || 100,
          region: config.region,
          accessKeyId: config.accessKeyId,
          secretAccessKey: config.secretAccessKey,
          endpoint: config.kinesisEndpoint
        };

        if (shard.checkpoint) options.lastCheckpoint = shard.checkpoint;

        var processor = Processor(options, function(records, callback) {
          if (stop) return processor.close();
          if (!instanceShardList[shardId]) return processor.close();

          kclParameters.processRecords.call(shard, records, function(err, checkpoint) {
            if (err) throw err;

            if (checkpoint) {
              var last = records.slice(-1)[0].SequenceNumber;
              return db.checkpoint(config.instanceId, shardId, last, callback);
            }

            callback();
          });
        });
      });
    });
  }

  return kcl;
};
