var AWS = require('aws-sdk');
var Kcl = require('./lib/kcl');

module.exports = function(config) {
  if (!config) config = {};

  if (!config.streamName) throw new Error('streamName must be configured');

  var kinesisOpts = {
    apiVersion: '2013-12-02',
    region: config.region || 'us-east-1',
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey
  };

  if (config.endpoint && config.endpoint !== '') kinesisOpts.endpoint = new AWS.Endpoint(config.endpoint);
  var kinesis = new AWS.Kinesis(kinesisOpts);

  var kine = {};
  var kcl;

  kine.createStream = function(opts, cb) {
    if (!opts.shardCount) throw new Error('shardCount not set');
    var params = {
      ShardCount: opts.shardCount,
      StreamName: config.streamName
    };
    kinesis.createStream(params, cb);
  };

  kine.describeStream = function(opts, cb) {
    if (!cb) cb = opts;
    // add paging.
    var params = {
      StreamName: config.streamName,
      Limit: opts.limit || 100
    };
    kinesis.describeStream(params, cb);
  };

  kine.getShardIterator =  function(opts, cb) {
    if (!opts.shardId) throw new Error('shardId not set');
    var params = {
      ShardId: opts.shardId,
      ShardIteratorType: (opts.shardIteratorType ||  'LATEST').toUpperCase(),
      StreamName: config.streamName
    };
    if (opts.startingSequenceNumber) params.StartingSequenceNumber = opts.startingSequenceNumber;

    kinesis.getShardIterator(params, cb);
  };

  kine.getRecords = function(opts, cb) {
    if (!opts.shardIterator) throw new Error('shardIterator not set');
    var params = {
      ShardIterator: opts.shardIterator,
      Limit: opts.limit || 100
    };
    kinesis.getRecords(params, cb);
  };

  kine.putRecord = function(opts, cb) {
    var params = {
      Data: opts.data,
      PartitionKey: opts.partitionKey,
      StreamName: config.streamName
    };
    if (opts.explicitHashKey) params.ExplicitHashKey = opts.explicitHashKey;
    if (opts.sequenceNumberForOrdering) params.SequenceNumberForOrdering = opts.sequenceNumberForOrdering;

    kinesis.putRecord(params, cb);
  };

  kine.kcl = function(mod, opts) {
    kcl = Kcl(config, mod, opts, kine);
    return kcl;
  };
  return kine;
};
