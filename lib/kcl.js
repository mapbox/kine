var os = require('os');
var Dyno = require('dyno');
var table = require('./table');
var queue = require('queue-async');
var _ = require('lodash');
var AWS = require('aws-sdk');
var DB = require('./db');
var BN = require('bignumber.js');
var crypto = require('crypto');
var logger = require('fastlog')('kine');

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

  function getBackoffWait(attempts){
    // random number between 500ms and 5000ms + (attempts*1000)ms
    return Math.round(Math.random() * (5000 - 500) + 500 + (attempts*1000));
  }

  function getRecords(shard, attempts) {
    attempts+=1;
    if (stop) return;
    if(config.verbose) logger.info('[ getRecords ] attempt', attempts);
    if(attempts >= 10) throw new Error('Too many getRecords attempts.');

    //make sure we are still responsible for this shard.
    if (!instanceShardList[shard.id]) return;
    if (instanceShardList[shard.id].expiresAt < (+new Date() - 5000)) {
      return delete instanceShardList[shard.id];
    }

    // expose the checkpoint function to the processRecords, so they can checkpoint
    // without requesting more records.
    shard.checkpointFunc = db.checkpoint.bind(null, shard);

    if (config.verbose) logger.info('[ getRecords ] params:', JSON.stringify({
      ShardIterator: shard.iterator,
      Limit: config.limit
    }));

    var req = kinesis.getRecords({ShardIterator: shard.iterator, Limit: config.limit});
    var requestTimeOut = setTimeout(req.abort.bind(req), 5000);

    req.on('error', function (err) {
      if (err.code === 'SyntaxError' || err.code === 'RequestAbortedError') {
        if (config.verbose) logger.info('[ getRecords ] Found SyntaxError', err);
        return setTimeout(getRecords.bind(this, shard, attempts), 500);
      }
      if (err.code === 'ProvisionedThroughputExceededException') {
        if (config.verbose) logger.info('[ getRecords ] Found ProvisionedThroughputExceededException', err);
        return setTimeout(getRecords.bind(this, shard, attempts), getBackoffWait(attempts));
      }
      if (err.code === 'ServiceUnavailable' || err.code === 'InternalFailure' || err.code === 'NetworkingError') {
        if (config.verbose) logger.info('[ getRecords ] Found serviceUnavailable', err);
        return setTimeout(getRecords.bind(this, shard, attempts), getBackoffWait(attempts));
      }
      throw new Error(err);
    });
    req.on('extractData', function (response) {
      if (config.verbose) logger.info('[ getRecords ] extractData', response.data.Records.length, 'items');
    });
    req.on('extractError', function (response) {
      if (config.verbose) logger.info('[ getRecords ] extractError', response);
    });
    req.on('httpError', function (response) {
      if (config.verbose) logger.info('[ getRecords ] httpError', response);
    });
    req.on('retry', function (resp) {
      if (config.verbose) logger.info('[ getRecords retry ] retry count: ', resp.retryCount);
      var nonRetryableErrors = ['ServiceUnavailable', 'InternalFailure', 'NetworkingError', 'ProvisionedThroughputExceededException', 'SyntaxError', 'RequestAbortedError'];
      if (nonRetryableErrors.indexOf(resp.error.code) >= 0) {
        resp.error.retryable = false; // disable retries entirely for these error codes since we are handling them ourselves
      }
    });
    req.on('success', function (resp) {
      if (config.verbose) logger.info('[ getRecords ] success Found', resp.data.Records.length, 'items');

      if (config.cloudwatchNamespace &&
        resp.Records &&
        resp.Records[0] &&
        resp.Records[0].ApproximateArrivalTimestamp) {
        throttledPutToCloudwatch('ShardIteratorAgeInMs', (+new Date()) - resp.Records[0].ApproximateArrivalTimestamp, 'Milliseconds', shard.id);
      }

      // malformed responses will have an undefined NextShardIterator. In that case, we keep the existing one.
      shard.iterator = resp.data.NextShardIterator === undefined ? shard.iterator : resp.data.NextShardIterator;
      shard.lastGetRecords = +new Date();

      if (resp.data.Records && resp.data.Records.length > 0) {
        shard.sequenceNumber = resp.data.Records[resp.data.Records.length - 1].SequenceNumber;
        config.processRecords.call(shard, resp.data.Records, function(err, checkpointShard) {
          if (err) throw err;

          if (checkpointShard) {
            // save checkpoint to dynamo.
            db.checkpoint(shard, checkpointSaved);
          } else {
            var diff = +new Date() - shard.lastGetRecords;
            var delay = 0;
            // if operation takes less than minProcessTime, slow it down to minProcessTime
            if (diff < config.minProcessTime) delay = config.minProcessTime - diff;
            logger.info('Processing records took ' + diff + ' ms, delaying ' + delay + ' ms');
            return setTimeout(getRecords.bind(this, shard, 0), delay);
          }
        });
      } else {
        if (shard.iterator === null) {
          db.shardComplete(shard, function (err) {
            if (err) throw err;
          });
        } else {
          return setTimeout(getRecords.bind(this, shard, 0), 2500).unref();
        }
      }
    });
    req.send(function () {
      clearTimeout(requestTimeOut);
    });


    function checkpointSaved(err) {
      if (err) throw err;
      if (shard.iterator === null) {
        // we are done with this shard, its complete.  lets mark it as complete.
        db.shardComplete(shard, function(err){
          if (err) throw err;
        });
      } else {
        setImmediate(getRecords.bind(this, shard, 0));
      }
    }
  }

  function startGettingRecords() {
    var shardIds = Object.keys(instanceShardList);
    shardIds.forEach(function(s) {
      if (!instanceShardList[s].getRecords) {
        config.init.call(instanceShardList[s], function() {
          getRecords(instanceShardList[s], 0);
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
