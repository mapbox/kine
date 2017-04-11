var os = require('os');
var Dyno = require('dyno');
var table = require('./table');
var queue = require('queue-async');
var _ = require('lodash');
var AWS = require('aws-sdk');
var DB = require('./db');
var BN = require('bignumber.js');
var crypto = require('crypto');

module.exports = function(config, kinesis) {

  config.instanceId = config.instanceId || [os.hostname(), process.pid, +new Date()].join('-');
  config.maxShards = config.maxShards || 10;
  config.Limit = config.limit || 10000;

  config._leaseTimeout = config._leaseTimeout || (12e4); // 2min default
  if (config.cloudwatchNamespace) {
    var cw = config.cloudwatch || new AWS.CloudWatch({region: config.region});
    var throttledPutToCloudwatch = _.throttle(putToCloudwatch, 30000);
  }

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

  kcl.instanceInfo = function (partitionKey, callback) {
    var md5 = crypto.createHash('md5').update(partitionKey).digest('hex');
    var hashKey = new BN(md5, 16);

    db.getShardList(function (err, shards) {
      if (err) return callback(err);

      for (var s = 0; s < shards.length; s++) {
        if (hashKey.cmp(shards[s].hashKeyStart) >= 0 && hashKey.cmp(shards[s].hashKeyEnd) <= 0) {
          return callback(null, {
            instance: shards[s].instance,
            shardId: shards[s].id,
            hashKeyStart: shards[s].hashKeyStart,
            hashKeyEnd: shards[s].hashKeyEnd
          });
        }
      }

      return callback(new Error('No shard found for ' + partitionKey));
    });
  };

  function instancePosition() {
    for (var i =0; i < instanceList.length; i++) {
      if (instanceList.id == config.instanceId) return i;
    }
    return -1;
  }

  // figure out the shards that this instance is responsible for and the checkpoints of those shards
  function shards() {
    if (stop) return;

    var howMany = 0;

    // only have the top few instances update the list of shards.  This reduce the number of
    // calls to kinesis, and thus we get throttled less.
    if(instancePosition() < 3) {
      db.updateShards(function(err, updatedShards) {
        if (err && err.code === 'LimitExceededException') {
          return setTimeout(shards, 10000).unref();
        }
        if (err) throw err;
        shardList = updatedShards;
        needMore();
      });
    } else {
      db.getShardList(function(err, updatedShards){
        if (err) throw err;
        shardList = updatedShards;
        needMore();
      });
    }

    function foundShardToLease(err, shard) {
      if (err) throw err;
      if (!shard) return needMore();
      return db.leaseShard(shard.id, checkDone);
    }

    function checkDone(err, shard) {
      if (err && err.code === 'ConditionalCheckFailedException') {
        // attempted to lease a shard that was already leased.  Lets try again.
        return setTimeout(db.availableShard.bind(this,foundShardToLease), 5000).unref();
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
    if (instanceShardList[shard.id].expiresAt < (+new Date() - 5000)) {
      return delete instanceShardList[shard.id];
    }

    // expose the checkpoint function to the procerssRecords, so they can checkpoint
    // without requesting more records.
    shard.checkpointFunc = db.checkpoint.bind(null, shard);

    kinesis.getRecords(
      { ShardIterator: shard.iterator, Limit: config.limit },
      function(err, resp) {
        if (err && err.code === 'SyntaxError') return setTimeout(getRecords.bind(this, shard), 100);
        if (err) throw err;

        if (config.cloudwatchNamespace &&
          resp.Records &&
          resp.Records[0] &&
          resp.Records[0].ApproximateArrivalTimestamp) {
          throttledPutToCloudwatch('ShardIteratorAgeInMs', (+new Date()) - resp.Records[0].ApproximateArrivalTimestamp, 'Milliseconds', shard.id);
        }

        // malformed responses will have an undefined NextShardIterator. In that case, we keep the existing one.
        shard.iterator = resp.NextShardIterator === undefined ? shard.iterator : resp.NextShardIterator;
        shard.lastGetRecords = +new Date();

        if(resp.Records && resp.Records.length > 0) {
          shard.sequenceNumber = resp.Records[resp.Records.length -1].SequenceNumber;
          config.processRecords.call(shard, resp.Records, processRecordsDone);
        } else {
          if (shard.iterator === null) {
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
      if (shard.iterator === null) {
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


      // if we havent called getrecords recently, we consider this process a
      // zombie and shut it down.
      if (instanceShardList[s].lastGetRecords < (+ new Date() - config.maxProcessTime)) {
        if(config.debugFunction && typeof config.debugFunction === 'function') config.debugFunction();
        throw Error('Max processing time reached, ' + config.maxProcessTime);
      }

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

  function putToCloudwatch(metricname, metric, unit, shardId) {
    var request = cw.putMetricData({
      MetricData:[{
        MetricName: metricname,
        Dimensions: [{
          Name: 'DeliveryStream',
          Value: config.streamName
        }, {
          Name: 'ShardId',
          Value: shardId
        }, {
          Name: 'Stack',
          Value: config.cloudwatchStackname
        }],
        Value: metric,
        Unit: unit
      }],
      Namespace: config.cloudwatchNamespace
    });
    request.on('error', function(err) {
      console.log('Cloudwatch metric %s not put for shard %s on %s.',
        metricname, shardId, config.streamName, err);
    });
    request.send();
  }

  return kcl;
};
