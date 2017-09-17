'use strict';

const os = require('os');
const queue = require('d3-queue').queue;
const Db = require('../lib/db.js');
const EventEmitter = require('events').EventEmitter;
const _ = require('lodash');
let logger = require('fastlog')();

module.exports = function (config, dynoClient, kinesisClient) {

  // TODO use loglevels instead of emitEvents
  // TODO docs -> readme
  // TODO test resharding /w app
  // TODO don't read if shard has parent not done yet
  // TODO CFN test app /w resources (2 kcl lambdas + 1 sync + 1 producer)
  // TODO resharding testing

  config.leaseTimeout = config.leaseTimeout || (12e4);
  config.instanceId = [os.hostname(), process.pid, +new Date()].join('-');
  config.shardIteratorType = config.shardIteratorType || 'LATEST';
  config.limit = config.limit || 10000;
  config.minProcessTime = config.minProcessTime || 1e3;
  config.maxProcessTime = config.maxProcessTime || 3e5;
  config.leaseShardDelay = config.leaseShardDelay || 5000;
  config.abortKinesisRequestTimeout = config.abortKinesisRequestTimeout || 5000;

  let db = Db(config, dynoClient, kinesisClient);

  let kcl = {
    shard: {
      updated: +new Date(),
      id: null
    },
    stop: false,
    events: new EventEmitter(),
    heartbeatTimeoutId: null,
    db: db
  };

  if (config.cloudwatchNamespace) kcl.throttledPutToCloudwatch = _.throttle(putToCloudWatch, 30000);

  /***
   * Initialize kine. Check for available shards, lease a shard if one is found, update our local shard state,
   * start the heartbeat cycle and get a shard iterator. Once the iterator has been grabbed, we start pulling records
   * in a loop. The callback is optional, it is mostly used for testing.
   * @param cb
   */
  kcl.init = function (cb) {
    logger = require('fastlog')();
    kcl.emitEvent('init', null);
    kcl.availableShard(function (err, availableShard) {
      if (err) throw err;
      kcl.leaseShard(availableShard, function (err, shard) {
        if (err) throw err;
        logger = require('fastlog')(shard.id);
        kcl.updateShardState(shard); // update local shard state
        kcl.heartbeat(); // start heartbeat cycle
        kcl.getIterator(shard); // get a first iterator. Once it gets it, kine will start pulling automatically
        if (cb) cb(null, shard); // mostly a hook for testing
      });
    })
  };

  /***
   * Check if a shard is available. If so, return it (we'll try to lease it). If not, keep checking in a loop.
   * @param cb
   */
  kcl.availableShard = function (cb) {
    kcl.db.availableShard(function (err, availableShard) {
      if (err) throw err;
      if (availableShard) {
        kcl.emitEvent('availableShard', availableShard, availableShard.id);
        cb(null, availableShard);
      } else {
        kcl.emitEvent('noShardsAvailable', null);
        setTimeout(kcl.availableShard, config.leaseShardDelay, cb);
      }
    })
  };

  /***
   * Try to lease an availableShard passed in the params. If the conditional check fails, we'll try again in a loop.
   * For any other error, we kill this process.
   * @param availableShard
   * @param cb
   */
  kcl.leaseShard = function (availableShard, cb) {
    kcl.db.leaseShard(availableShard.id, function (err, result) {
      if (err && err.code === 'ConditionalCheckFailedException') {
        kcl.emitEvent('noShardsAvailable', null);
        return setTimeout(kcl.leaseShard, config.leaseShardDelay, availableShard, cb);
      } else if (err) {
        cb(err);
      }
      kcl.emitEvent('shardLeased', null);
      cb(null, result);
    })
  };

  /***
   * Update our internal kcl.shard object
   * @param newState
   */
  kcl.updateShardState = function (newState) {
    kcl.shard = Object.assign(kcl.shard, newState);
  };

  /***
   * Update our configuration (mostly useful for testing)
   * @param newConfig
   */
  kcl.updateConfig = function (newConfig) {
    config = Object.assign(config, newConfig);
  };

  /***
   * Get a shard iterator for our shard. The type depends on our settings – if we've checkpointed in the past, keep
   * going after our sequence number. Once we have our shard iterator, start pulling records immediately.
   */
  kcl.getIterator = function () {
    const params = {
      StreamName: config.streamName,
      ShardId: kcl.shard.id,
      ShardIteratorType: config.shardIteratorType
    };

    if (kcl.shard.checkpoint) {
      params.ShardIteratorType = 'AFTER_SEQUENCE_NUMBER';
      params.StartingSequenceNumber = kcl.shard.checkpoint;
    } else if (params.ShardIteratorType === 'AT_TIMESTAMP') {
      params.Timestamp = config.timestamp;
    }

    kinesisClient.getShardIterator(params, function (err, iterator) {
      if (err) throw err;
      kcl.startGettingRecords(iterator);
    });

  };

  /***
   * Wrapper for getting records function. Before we start, call the users' init function and wait for the callback
   * to return
   * @param iterator
   */
  kcl.startGettingRecords = function (iterator) {
    config.init.call(kcl.shard, function () {
      kcl.emitEvent('userInitCalled', null);
      kcl.getRecords(kcl.shard, 0, iterator.ShardIterator, kcl.successfulGetRecords)
    })
  };

  /***
   * The heartbeat ensures that don't have processes that are stuck or not pulling from a shard they have leased.
   * If we haven't had a heartbeat from a process within 5 minutes, we'll consider it dead and throw a hard error.
   * Once we have updated the lease with a new timestamp, merge the return value with our local state.
   * The callback is optional, it's mostly useful for testing (instead of waiting for a timeout, we can test things
   * right after a heartbeat).
   *
   * @param cb
   */
  kcl.heartbeat = function (cb) {
    if (kcl.stop) return;
    kcl.emitEvent('heartbeat');

    // if we haven't called getRecords recently, we consider this process a zombie and shut it down.
    if (kcl.shard.updated < (+new Date() - config.maxProcessTime)) {
      throw Error(`Max processing time reached: ${config.maxProcessTime}. Last updated was ${kcl.shard.updated}`);
    }
    if (kcl.shard.status !== 'complete') {
      kcl.db.updateLease(kcl.shard, function (err, result) {
        if (err) throw err;
        const timeoutId = setTimeout(kcl.heartbeat, 5000);
        // merge result values with our current shard object
        _.extend(kcl.shard, result);
        kcl.heartbeatTimeoutId = timeoutId; // expose the timeoutId in case we want to cancel it
        if (cb) cb(null, kcl.shard); // mostly for testing
      })
    } else {
      kcl.emitEvent('heartbeat', 'shard status is complete, not updating lease');
    }
  };

  /**
   * Wrapper function to call our db's checkpoint function
   * @param sequenceNumber
   * @param cb
   */
  kcl.checkpoint = function (sequenceNumber, cb) {
    kcl.emitEvent('externalCheckpointCalled', null);
    kcl.db.checkpoint(kcl.shard, sequenceNumber, cb)
  };

  /***
   * Get the backoff wait time when getRecords hits errors, based on attempts. Min backoff time is 1.5 seconds
   * and maximum is 14 seconds.
   * @param attempts
   * @returns {number}
   */
  kcl.getBackoffWait = function (attempts) {
    return Math.round(Math.random() * (5000 - 500) + 500 + (attempts * 1000));
  };

  /***
   * Build a request object from our kinesis client, and manually call the send() method from it. We've built our own
   * retry logic which mimicks' the SDK's behavior, but affords more fine-grained control over timeouts and retries.
   *
   * We had API calls for getRecords that did not return a response, nor an error and never timed out. For this reason
   * we added setTimeout(req.abort.bind(req), config.abortKinesisRequestTimeout) to force kill the request (and retry)
   * after a certain time.
   * @param shard
   * @param attempts
   * @param iterator
   * @param cb
   */
  kcl.getRecords = function (shard, attempts, iterator, cb) {
    attempts += 1;
    if (kcl.stop) return;
    kcl.emitEvent('[ getRecords ] attempts', null, attempts);
    kcl.emitEvent('[ getRecords ] params', null, JSON.stringify({
      ShardIterator: iterator,
      Limit: config.limit
    }));
    if (attempts >= 10) throw new Error('Too many getRecords attempts.');

    let req = kinesisClient.getRecords({ShardIterator: iterator, Limit: config.limit});
    let requestTimeOut = setTimeout(req.abort.bind(req), config.abortKinesisRequestTimeout);

    req.on('error', function (err) {
      if (err.code === 'SyntaxError' || err.code === 'RequestAbortedError') {
        kcl.emitEvent('[ getRecords ] SyntaxError', null, err);
        return setTimeout(kcl.getRecords, 500, shard, attempts, iterator, kcl.successfulGetRecords);
      }
      if (err.code === 'ProvisionedThroughputExceededException') {
        kcl.emitEvent('[ getRecords ] ProvisionedThroughputExceededException', null, err);
        return setTimeout(kcl.getRecords, getBackoffWait(attempts), shard, attempts, iterator, kcl.successfulGetRecords);
      }
      if (err.code === 'ServiceUnavailable' || err.code === 'InternalFailure' || err.code === 'NetworkingError') {
        kcl.emitEvent('[ getRecords ] serviceUnavailable', null, err);
        return setTimeout(kcl.getRecords, getBackoffWait(attempts), shard, attempts, iterator, kcl.successfulGetRecords);
      }
      throw new Error(err);
    });
    req.on('extractData', function (response) {
      kcl.emitEvent('[ getRecords ] extractData items', null, response.data.Records.length);
    });
    req.on('extractError', function (response) {
      kcl.emitEvent('[ getRecords ] extractError', null, response);
    });
    req.on('httpError', function (response) {
      kcl.emitEvent('[ getRecords ] httpError', null, response);
    });
    req.on('retry', function (response) {
      kcl.emitEvent('[ getRecords ] retry count', null, response._retryCount);
      let nonRetryableErrors = ['ServiceUnavailable', 'InternalFailure', 'NetworkingError', 'ProvisionedThroughputExceededException', 'SyntaxError', 'RequestAbortedError'];
      if (nonRetryableErrors.indexOf(response.error.code) >= 0) {
        response.error.retryable = false; // disable retries entirely for these error codes since we are handling them ourselves
      }
    });
    req.on('success', function (resp) {
      const nbItems = resp.data.Records ? resp.data.Records.length : 'n/a';
      kcl.emitEvent('[ getRecords ] success found', null, nbItems);
      cb(resp, iterator, shard)
    });
    req.send(function () {
      clearTimeout(requestTimeOut);
    });
  };

  /***
   * Once we've gotten a successful getRecords response, decide what to do with our shardIterator, and how to
   * continue processing, based on the response. Malformed responses will have an undefined NextShardIterator
   * and undefined Records key. We only want to replace the iterator if we have a nextShardIterator.
   * Otherwise we'll keep using the same one.
   * @param resp
   * @param iterator
   * @param shard
   */
  kcl.successfulGetRecords = function (resp, iterator, shard) {
    if (resp.data.NextShardIterator) iterator = resp.data.NextShardIterator;
    kcl.lastGetRecords = +new Date();

    if (resp.data.Records && resp.data.Records.length > 0) {
      kcl.postProcessWithRecords(resp, iterator, shard);
    } else {
      kcl.postProcessWithoutRecords(resp, iterator, shard);
    }
  };

  /***
   * Figure out how much delay to avoid pulling from a shard too often. config.minProcessTime can be configured to
   * pull more or less often.
   * @param startTime
   * @returns {number}
   */
  kcl.getDelay = function (startTime) {
    let diff = +new Date() - startTime;
    let delay = 0;
    // if operation takes less than minProcessTime, slow it down to minProcessTime
    if (diff < config.minProcessTime) delay = config.minProcessTime - diff;
    kcl.emitEvent('ProcessRecords', null, `took ${diff}, delaying ${delay} ms`);
    return delay;
  };

  /***
   * If our response has records, call the users' processRecords function on them, and checkpoint if it was
   * requested in the callback. Checkpointing happens in dynamo using the sequenceNumber of the last record in our
   * array. Put stats to CloudWatch for this instance, if possible.
   * @param resp
   * @param iterator
   * @param shard
   */
  kcl.postProcessWithRecords = function (resp, iterator, shard) {
    if (config.cloudwatchNamespace && config.cloudwatch && resp.data.Records[0].ApproximateArrivalTimestamp) {
      kcl.throttledPutToCloudwatch('ShardIteratorAgeInMs', (+new Date()) - resp.data.Records[0].ApproximateArrivalTimestamp, 'Milliseconds', kcl.shard.id);
    }
    // grab the last sequence number, we'll use it to checkpoint
    const sequenceNumber = resp.data.Records[resp.data.Records.length - 1].SequenceNumber;
    config.processRecords.call(kcl.shard, resp.data.Records, function (err, checkpointShard) {
      if (err) throw err;
      if (checkpointShard) {
        kcl.emitEvent('[ getRecords ] user processing done', null);
        kcl.db.checkpoint(shard, sequenceNumber, function (err) {
          if (err) throw err;
          kcl.checkpointSaved(shard, iterator);
        });
      } else {
        let delay = kcl.getDelay(kcl.lastGetRecords);
        setTimeout(kcl.getRecords, delay, shard, 0, iterator, kcl.successfulGetRecords);
      }
    });
  };

  /***
   * If there are no records from the getRecords call, check whether the shard is complete and should be marked as such,
   * or if we just got an invalid response and should be retrying. If the shard iterator is undefined (or null...) and
   * the response is valid (not a malformed payload, with undefined Records key), we'll mark the shard as complete. Run
   * the user's onShardClosed function beforehand (if it exists) and wait for the callback.
   * @param resp
   * @param iterator
   * @param shard
   */
  kcl.postProcessWithoutRecords = function (resp, iterator, shard) {
    if (!resp.data.NextShardIterator && resp.data.Records) {
      if (config.onShardClosed) { // if there's a function to be called before closing things off, wait for it to finish
        config.onShardClosed.call(shard, function (err) {
          kcl.emitEvent('[ getRecords ] user onShardClosed done', null);
          if (err) throw err;
          kcl.markShardAsComplete(shard);
        });
      } else {
        kcl.markShardAsComplete(shard);
      }
    } else {
      // try again with the same iterator
      setTimeout(kcl.getRecords, 2500, shard, 0, iterator, kcl.successfulGetRecords);
    }
  };

  /***
   * Mark our shard as complete, and exit the process.
   * @param shard
   */
  kcl.markShardAsComplete = function (shard) {
    kcl.shard.status = 'complete';
    kcl.db.shardComplete(shard, function (err) {
      if (err) throw err;
      kcl.emitEvent('shardMarkedComplete', null);
      process.exit(0);
    });
  };

  /***
   * Check if the value for our iterator is explicitly null. If so, close the shard.
   * @param shard
   * @param iterator
   */
  kcl.checkpointSaved = function (shard, iterator) {
    kcl.emitEvent('checkpointSaved');
    if (iterator === null) {
      kcl.markShardAsComplete(shard)
    } else {
      let delay = kcl.getDelay(kcl.lastGetRecords);
      setTimeout(kcl.getRecords, delay, shard, 0, iterator, kcl.successfulGetRecords);
    }
  };

  /***
   * Helper function to centralize what to log and events to emit
   * @param event
   * @param data
   * @param metadata
   */
  kcl.emitEvent = function (event, data, metadata) {
    if (metadata === null || metadata === undefined) metadata = ''; // we want to let 0 through
    if (config.verbose) logger.info(`[ Kcl ] ${event} ${metadata}`);
    kcl.events.emit(event, data);
  };

  /***
   * Send stats about this instance to CloudWatch. We use a throttled version of this function.
   * @param metricName
   * @param metric
   * @param unit
   * @param shardId
   */
  function putToCloudWatch(metricName, metric, unit, shardId) {
    let request = config.cloudwatch.putMetricData({
      MetricData: [{
        MetricName: metricName,
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
    request.on('error', function (err) {
      console.log(`CloudWatch metric ${metricName} not put for shard ${shardId} on ${config.streamName}: ${err}.`);
    });
    request.send();
  }

  return kcl;
};