var AWS = require('aws-sdk');
var Kcl = require('./lib/kcl');

/**
 * Creates a kine client. You must provide a stream name and the region where the
 * stream resides. You can also provide the name of the dynamodb table that kine will
 * use for tracking shard leases and checkpointing progess.
 *
 * If you do not explicitly pass credentials when creating a kine client, the
 * aws-sdk will look for credentials in a variety of places. See [the configuration guide](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html)
 * for details.
 *
 * @param {object} options - configuration parameters
 * @param {string} options.streamName - the name of the kinesis stream to consume
 * @param {string} options.region - the region in which the default stream resides
 * @param {string} options.shardIteratorType - where to start in the stream. `LATEST` or `TRIM_HORIZON`
 * @param {string} options.table - the dynamodb table to use for tracking shard leases.
 * @param {function} options.init - function that is called when a new lease of a shard is started
 * @param {function} options.processRecords - function is that called when new records are fetches from the kinesis shard.
 * @param {string} [options.maxShards] - max number of shards to track per process. defaults to 10
 * @param {string} [options.limit] - limit used for requests to kinesis for the number of records.  This is a max, you might get few records on process records
 * @param {string} [options.maxProcessTime] - max number of millseconds between getting records before considering a process a zombie . defaults to 300000 (5mins)
 * @param {string} [options.endpoint] - the kinesis endpoint url
 * @param {string} [options.dynamoEndpoint] - the dynamodb endpoint url
 * @param {string} [options.sessionToken] - credentials for the client to utilize
 * @param {string} [options.accessKeyId] - credentials for the client to utilize
 * @param {string} [options.secretAccessKey] - credentials for the client to utilize
 * @param {string} [options.sessionToken] - credentials for the client to utilize
 * @param {string} [options.cloudwatchNamespace] - namespace to use for custom cloudwatch reporting of shard ages. required if `cloudwatchStackname` is set
 * @param {string} [options.cloudwatchStackname] - stack name to use as a dimension on custom cloudwatch reporting of shard ages. required if `cloudwatchNamespace` is set

 * @returns {client} a kine client
 * @example
 * var Kine = require('kine');
 * var kine = Kine({
 *   streamName: 'my-kinesis-stream',
 *   region: 'us-east-1'
 * });
 */

module.exports = function(config) {
  if (!config) config = {};

  if (!config.streamName) throw new Error('streamName must be configured');
  if (!config.region) throw new Error('region must be configured');
  if (!config.shardIteratorType) throw new Error('shardIteratorType must be configured');
  if (!config.table) throw new Error('table must be configured');
  if (!config.init) throw new Error('an init function must be configured');
  if (!config.processRecords) throw new Error('a processRecords function must be configured');
  if ((config.cloudwatchNamespace && !config.cloudwatchStackname) ||
    (!config.cloudwatchNamespace && config.cloudwatchStackname)) throw new Error('both cloudwatchNamespace and cloudwatchStackname must be configured');

  config.maxProcessTime = config.maxProcessTime || 3e5;

  var kinesisOpts = {
    region: config.region,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    sessionToken: config.sessionToken,
    maxRetries: 10
  };

  if (config.endpoint && config.endpoint !== '') kinesisOpts.endpoint = new AWS.Endpoint(config.endpoint);
  var kinesis = new AWS.Kinesis(kinesisOpts);

  var kcl = Kcl(config, kinesis);
  kcl.kinesis = kinesis;
  return kcl;
};
