'use strict';

let Kcl = require('./lib/kcl');
let Sync = require('./lib/sync');

/**
 * Creates a kine client. You must provide a certain number of mandatory configuuration options, a dyno client instance
 * and a kinesis client instance.
 *
 * @param {object} config - configuration parameters
 * @param {string} config.streamName - the name of the kinesis stream to consume
 * @param {string} config.shardIteratorType - where to start in the stream. `LATEST`, `TRIM_HORIZON` or `AT_TIMESTAMP`
 * @param {function} config.init - function that is called when a new lease of a shard is started
 * @param {function} config.processRecords - function is that called when new records are fetches from the kinesis shard.
 * @param {function} [config.onShardClosed] - function that is called when a shard is closed
 * @param {number} [config.leaseTimeout] - time for a lease to be considered expired (default 2 minutes)
 * @param {number} [config.leaseShardDelay] - if a shard cannot be leased, try again using this delay (default 5 seconds)
 * @param {number} [config.abortKinesisRequestTimeout] - abort getRecords kinesis requests after this delay (default 5 seconds)
 * @param {string} [config.limit] - limit used for requests to kinesis for the number of records.  This is a max, you might get few records on process records
 * @param {string} [config.maxProcessTime] - max number of milliseconds between getting records before considering a process a zombie . defaults to 300000 (5mins)
 * @param {string} [config.minProcessTime] - min number of milliseconds between getting records. defaults to 0
 * @param {string} [config.cloudwatchNamespace] - namespace to use for custom cloudwatch reporting of shard ages. required if `cloudwatchStackname` is set
 * @param {string} [config.cloudwatch] - cloudwatch instance. required if `cloudwatchNamespace` is set
 * @param {string} [config.cloudwatchStackname] - stack name to use as a dimension on custom cloudwatch reporting of shard ages. required if `cloudwatchNamespace` is set
 * @param {boolean} [config.verbose] - verbose output
 * @param {object} dynoClient - a pre-configured dyno client to communicate with our dynamo table
 * @param {object} kinesisClient - a pre-configured kinesis client to communicate with our kinesis stream

 * @returns {client} a kine client
 * @example
 * const Kine = require('@mapbox/kine').kcl;
 * const Sync = require('@mapbox/kine').sync;
 * const Dyno = require('@mapbox/dyno');

 * const kinesisClient = new AWS.Kinesis();
 * const dynoClient = Dyno({ region: 'us-east-1', table: 'myTableName'});

 * // start a sync instance to keep your stream state in sync with dynamo
 * // you probably want this code in a separate process/worker to keep it isolated from worker failures
 * const sync = Sync(syncConfig, dynoClient, kinesisClient);
 * sync.init(function () { // init dynamo table
 *    sync.shards(); // start syncing in a loop
 * })

 * // start a consumer worker for a single shard
 * // kine will start looking for available shards, pick one if possible and start pulling records immediately.
 * Kine({
 *   streamName: 'my-kinesis-stream',
 *   shardIteratorType: 'TRIM_HORIZON',
 *   init: function (cb) {
 *       cb(null, null);
 *   },
 *   processRecords: function (records, cb) {
 *       // records is an array of records from kinesis.
 *       records.forEach(function (r) {
 *         console.log(r.Data.toString());
 *       });
 *    // cb(err) will throw. Restart with a process manager like upstart
 *    // cb(null, false) with fetch more of the Kinesis stream and not checkpoint
 *    // cb(null, true) will checkpoint, then fetch more off the Kinesis stream.
 *    cb(null, true);
 *   }
 * }, dynoClient, kinesisClient);
 *
 * // see /examples folder for more
 */

module.exports.kcl = function (config, dynoClient, kinesisClient) {
  if (!config) config = {};
  if (!dynoClient) throw new Error('dynoClient must be configured');
  if (!kinesisClient) throw new Error('kinesisClient must be configured');
  if (!config.streamName) throw new Error('streamName must be configured');
  if (!config.shardIteratorType) throw new Error('shardIteratorType must be configured');
  if (config.shardIteratorType === 'AT_TIMESTAMP' && !config.timestamp) throw new Error('config.timestamp must be configured for an AT_TIMESTAMP shard iterator type');
  if (!config.init) throw new Error('an init function must be configured');
  if (!config.processRecords) throw new Error('a processRecords function must be configured');
  if ((config.cloudwatchNamespace && !config.cloudwatchStackname) || (!config.cloudwatchNamespace && config.cloudwatchStackname) || (!config.cloudwatchNamespace && config.cloudwatch)) {
    throw new Error('cloudwatchNamespace, cloudwatchStackname and cloudwatch must be configured');
  }

  const kcl = Kcl(config, dynoClient, kinesisClient);
  kcl.init();
};

module.exports.sync = Sync;